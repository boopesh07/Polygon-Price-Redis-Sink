from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Dict, Optional

from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from .logging_setup import get_logger
from .redis_sink import BaseSink

logger = get_logger()


class PrevCloseRecorder:
    """Stores the first FMV price observed at/after the configured market close as prev close."""

    def __init__(
        self,
        sink: BaseSink,
        *,
        timezone_name: str,
        close_hour: int,
        close_minute: int,
        prev_close_ttl: int,
    ) -> None:
        self._sink = sink
        try:
            self._tz = ZoneInfo(timezone_name)
        except ZoneInfoNotFoundError:
            logger.warning("prev_close_timezone_fallback", timezone=timezone_name)
            self._tz = timezone.utc
        self._close_hour = close_hour
        self._close_minute = close_minute
        self._prev_close_ttl = prev_close_ttl
        self._written_day: Dict[str, str] = {}
        self._lock = asyncio.Lock()

    async def maybe_record_from_fmv(self, symbol: str, price: Optional[float], ts: Optional[int]) -> None:
        if price is None or ts is None:
            return
        dt_local = self._resolve_dt(ts)
        day_key = dt_local.date().isoformat()
        if not self._is_at_or_after_close(dt_local):
            async with self._lock:
                if self._written_day.get(symbol) and self._written_day[symbol] != day_key:
                    self._written_day.pop(symbol, None)
            return

        async with self._lock:
            if self._written_day.get(symbol) == day_key:
                return
            self._written_day[symbol] = day_key

        try:
            await self._sink.set_prev_close(symbol, price, ttl=self._prev_close_ttl)
            logger.info("prev_close_recorded", symbol=symbol, day=day_key, price=price)
        except Exception as exc:  # noqa: BLE001
            logger.error("prev_close_record_error", symbol=symbol, error=str(exc))

    async def get_prev_close(self, symbol: str) -> Optional[float]:
        try:
            value = await self._sink.get_prev_close(symbol)
        except Exception as exc:  # noqa: BLE001
            logger.error("prev_close_fetch_error", symbol=symbol, error=str(exc))
            return None
        return value

    def _is_at_or_after_close(self, dt_local: datetime) -> bool:
        return (dt_local.hour, dt_local.minute) >= (self._close_hour, self._close_minute)

    def _resolve_dt(self, ts: int) -> datetime:
        return datetime.fromtimestamp(self._normalize_seconds(ts), tz=timezone.utc).astimezone(self._tz)

    @staticmethod
    def _normalize_seconds(ts: int) -> float:
        if ts >= 10**18:
            return ts / 10**9
        if ts >= 10**15:
            return ts / 10**6
        if ts >= 10**12:
            return ts / 10**3
        return float(ts)
