from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, Optional

from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from .logging_setup import get_logger
from .redis_sink import BaseSink


logger = get_logger()


@dataclass
class _QuoteState:
    day: Optional[str] = None
    prev_close: Optional[float] = None
    last_price: Optional[float] = None
    pending_close_price: Optional[float] = None


class QuoteTracker:
    def __init__(self, sink: BaseSink, timezone_name: str, close_hour: int, close_minute: int, prev_close_ttl: int) -> None:
        self._sink = sink
        try:
            self._tz = ZoneInfo(timezone_name)
        except ZoneInfoNotFoundError:
            logger.warning("quote_tracker_timezone_fallback", timezone=timezone_name)
            self._tz = timezone.utc
        self._close_hour = close_hour
        self._close_minute = close_minute
        self._prev_close_ttl = prev_close_ttl
        self._states: Dict[str, _QuoteState] = {}
        self._lock = asyncio.Lock()

    def seed_prev_close(self, symbol: str, prev_close: float) -> None:
        state = self._states.setdefault(symbol, _QuoteState())
        state.prev_close = prev_close
        logger.info("quote_prev_close_seeded", symbol=symbol, prevClose=prev_close)

    async def process_quote(
        self,
        symbol: str,
        *,
        bid: Optional[float],
        ask: Optional[float],
        ts: Optional[int],
    ) -> Dict[str, Optional[float]]:
        async with self._lock:
            dt_local = self._resolve_dt(ts)
            day_key = dt_local.date().isoformat()

            state = self._states.setdefault(symbol, _QuoteState())
            if state.day is None:
                state.day = day_key

            if state.prev_close is None:
                loaded = await self._sink.get_prev_close(symbol)
                if loaded is not None:
                    state.prev_close = loaded
                    logger.info("quote_prev_close_loaded", symbol=symbol, prevClose=loaded)

            if state.day != day_key:
                prev_close_candidate = state.pending_close_price or state.last_price
                if prev_close_candidate is not None:
                    state.prev_close = prev_close_candidate
                    await self._persist_prev_close(symbol, prev_close_candidate)
                    logger.info(
                        "quote_prev_close_rolled",
                        symbol=symbol,
                        prevClose=state.prev_close,
                        fromDay=state.day,
                        toDay=day_key,
                    )
                else:
                    logger.warning("quote_prev_close_missing", symbol=symbol, fromDay=state.day, toDay=day_key)
                state.day = day_key
                state.pending_close_price = None

            price = self._compute_price(bid, ask)
            if price is not None:
                state.last_price = price
                if self._is_after_close(dt_local):
                    state.pending_close_price = price
                    await self._persist_prev_close(symbol, price)
                    logger.debug("quote_close_price_recorded", symbol=symbol, price=price, day=state.day)

            prev_close = state.prev_close
            daily_change = None
            daily_change_pct = None
            if price is not None and prev_close is not None:
                daily_change = price - prev_close
                if prev_close != 0:
                    daily_change_pct = (daily_change / prev_close) * 100

            return {
                "price": price,
                "prevClose": prev_close,
                "dailyChange": daily_change,
                "dailyChangePct": daily_change_pct,
            }

    def _is_after_close(self, dt_local: datetime) -> bool:
        return (dt_local.hour, dt_local.minute) >= (self._close_hour, self._close_minute)

    def _compute_price(self, bid: Optional[float], ask: Optional[float]) -> Optional[float]:
        bid_valid = isinstance(bid, (float, int))
        ask_valid = isinstance(ask, (float, int))
        if bid_valid and ask_valid:
            return (float(bid) + float(ask)) / 2.0
        if ask_valid:
            return float(ask)
        if bid_valid:
            return float(bid)
        return None

    def _resolve_dt(self, ts: Optional[int]) -> datetime:
        if ts is None:
            return datetime.now(timezone.utc).astimezone(self._tz)
        seconds = self._normalize_seconds(ts)
        return datetime.fromtimestamp(seconds, tz=timezone.utc).astimezone(self._tz)

    @staticmethod
    def _normalize_seconds(ts: int) -> float:
        if ts >= 10**18:
            # nanoseconds
            return ts / 10**9
        if ts >= 10**15:
            # microseconds
            return ts / 10**6
        if ts >= 10**12:
            # milliseconds
            return ts / 10**3
        return float(ts)

    async def _persist_prev_close(self, symbol: str, price: float) -> None:
        try:
            await self._sink.set_prev_close(symbol, price, ttl=self._prev_close_ttl)
        except Exception as exc:  # noqa: BLE001
            logger.error("quote_prev_close_persist_error", symbol=symbol, error=str(exc))
