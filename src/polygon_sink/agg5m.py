from __future__ import annotations

import asyncio
import contextlib
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from .logging_setup import get_logger
from .redis_sink import BaseSink


logger = get_logger()


@dataclass
class _SymbolState:
    day_key: str
    buckets: List[Tuple[int, float]] = field(default_factory=list)
    current_bucket_end: Optional[int] = None
    current_close: Optional[float] = None
    dirty: bool = False


class Agg5mCollector:
    def __init__(
        self,
        sink: BaseSink,
        *,
        flush_interval_sec: int,
        ttl_sec: Optional[int],
        timezone_name: str,
        max_bars: int,
    ) -> None:
        self._sink = sink
        self._flush_interval = max(1, flush_interval_sec)
        self._ttl = ttl_sec if ttl_sec and ttl_sec > 0 else None
        self._max_bars = max(1, max_bars)
        self._states: Dict[str, _SymbolState] = {}
        self._lock = asyncio.Lock()
        self._flush_task: Optional[asyncio.Task[None]] = None
        self._closed = False
        try:
            self._tz = ZoneInfo(timezone_name)
        except ZoneInfoNotFoundError:
            logger.warning("agg5m_timezone_fallback", timezone=timezone_name)
            self._tz = timezone.utc

    def start(self) -> None:
        if self._flush_task is not None:
            return
        self._flush_task = asyncio.create_task(self._run_flush_loop())

    async def _run_flush_loop(self) -> None:
        try:
            while not self._closed:
                await asyncio.sleep(self._flush_interval)
                try:
                    await self.flush()
                except Exception as exc:  # noqa: BLE001
                    logger.error("agg5m_flush_loop_error", error=str(exc))
        except asyncio.CancelledError:
            raise

    async def close(self) -> None:
        self._closed = True
        if self._flush_task is not None:
            self._flush_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._flush_task
        await self.flush(force=True, finalize_open=True)
        async with self._lock:
            self._states.clear()

    async def on_minute_bar(self, symbol: str, bar: Dict[str, Any]) -> None:
        close_val = bar.get("close")
        if close_val is None:
            return
        try:
            close = float(close_val)
        except (TypeError, ValueError):
            return

        start_ms = self._extract_start_ms(bar)
        end_ms = self._extract_end_ms(bar, start_ms)
        if start_ms is None or end_ms is None:
            logger.debug("agg5m_missing_timestamps", symbol=symbol)
            return

        bucket_end_ms = self._bucket_end_ms(start_ms)
        day_key = self._day_key(end_ms)

        writes: List[Tuple[str, Dict[str, Any]]] = []
        async with self._lock:
            state = self._states.get(symbol)
            if state is None:
                state = _SymbolState(day_key=day_key)
                self._states[symbol] = state
                state.dirty = True
            elif state.day_key != day_key:
                self._finalize_current(state, force=True)
                payload_prev = self._build_payload(state)
                writes.append((symbol, payload_prev))
                state.day_key = day_key
                state.buckets = []
                state.current_bucket_end = None
                state.current_close = None
                state.dirty = True

            if state.current_bucket_end is None:
                state.current_bucket_end = bucket_end_ms
                state.current_close = close
            elif bucket_end_ms == state.current_bucket_end:
                state.current_close = close
            elif bucket_end_ms > state.current_bucket_end:
                if state.current_close is not None:
                    self._append_bucket(state, state.current_bucket_end, state.current_close)
                state.current_bucket_end = bucket_end_ms
                state.current_close = close
            else:
                # Late/out-of-order bucket; store directly.
                self._append_bucket(state, bucket_end_ms, close)

        for sym, payload in writes:
            await self._sink.set_agg5m_snapshot(sym, payload["day"], payload["bars"], ttl=self._ttl)

    async def flush(self, *, force: bool = False, finalize_open: bool = True) -> None:
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        writes: List[Tuple[str, Dict[str, Any]]] = []
        async with self._lock:
            for symbol, state in self._states.items():
                changed = False
                if finalize_open:
                    changed = self._finalize_current(state, now_ms=now_ms)
                if state.dirty or force or changed:
                    payload = self._build_payload(state)
                    writes.append((symbol, payload))
                    state.dirty = False
        for symbol, payload in writes:
            await self._sink.set_agg5m_snapshot(symbol, payload["day"], payload["bars"], ttl=self._ttl)

    def _finalize_current(self, state: _SymbolState, now_ms: Optional[int] = None, force: bool = False) -> bool:
        if state.current_bucket_end is None or state.current_close is None:
            return False
        if not force:
            if now_ms is None:
                return False
            if now_ms < state.current_bucket_end:
                return False
        self._append_bucket(state, state.current_bucket_end, state.current_close)
        state.current_bucket_end = None
        state.current_close = None
        return True

    def _append_bucket(self, state: _SymbolState, bucket_end_ms: int, close: Optional[float]) -> None:
        if close is None:
            return
        updated = False
        if state.buckets and state.buckets[-1][0] == bucket_end_ms:
            if state.buckets[-1][1] != close:
                state.buckets[-1] = (bucket_end_ms, close)
                updated = True
        else:
            state.buckets.append((bucket_end_ms, close))
            updated = True
        if len(state.buckets) > self._max_bars:
            overflow = len(state.buckets) - self._max_bars
            if overflow > 0:
                state.buckets = state.buckets[overflow:]
                updated = True
        if updated:
            state.dirty = True

    def _build_payload(self, state: _SymbolState) -> Dict[str, Any]:
        return {
            "day": state.day_key,
            "bars": [{"ts": ts, "close": close} for ts, close in state.buckets],
        }

    def _extract_start_ms(self, bar: Dict[str, Any]) -> Optional[int]:
        start = bar.get("start")
        if isinstance(start, (int, float)):
            return int(start)
        end = bar.get("end")
        if isinstance(end, (int, float)):
            approx_start = int(end) - 60_000
            return approx_start if approx_start >= 0 else 0
        return None

    def _extract_end_ms(self, bar: Dict[str, Any], start_ms: Optional[int]) -> Optional[int]:
        end = bar.get("end")
        if isinstance(end, (int, float)):
            return int(end)
        if start_ms is not None:
            return start_ms + 60_000
        return None

    def _bucket_end_ms(self, start_ms: int) -> int:
        start_seconds = start_ms // 1000
        bucket_start_seconds = (start_seconds // 300) * 300
        bucket_end_seconds = bucket_start_seconds + 300
        return bucket_end_seconds * 1000

    def _day_key(self, end_ms: int) -> str:
        dt = datetime.fromtimestamp(end_ms / 1000, tz=timezone.utc).astimezone(self._tz)
        return dt.date().isoformat()
