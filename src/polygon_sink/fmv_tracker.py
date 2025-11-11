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
class _FmvState:
    day: Optional[str] = None
    prev_close: Optional[float] = None
    last_price: Optional[float] = None
    pending_close_price: Optional[float] = None


class FmvTracker:
    """
    Tracks Fair Market Value (FMV) prices and calculates daily P/L metrics.
    
    Features:
    - Computes dailyChange and dailyChangePct based on previous close
    - Captures closing price after market close time (default 16:00 ET)
    - Handles day rollovers and persists prev_close to Redis
    - Loads prev_close from Redis on first FMV event per symbol
    """

    def __init__(self, sink: BaseSink, timezone_name: str, close_hour: int, close_minute: int, prev_close_ttl: int) -> None:
        self._sink = sink
        try:
            self._tz = ZoneInfo(timezone_name)
        except ZoneInfoNotFoundError:
            logger.warning("fmv_tracker_timezone_fallback", timezone=timezone_name, fallback="UTC")
            self._tz = timezone.utc
        self._close_hour = close_hour
        self._close_minute = close_minute
        self._prev_close_ttl = prev_close_ttl
        self._states: Dict[str, _FmvState] = {}
        self._lock = asyncio.Lock()
        logger.info(
            "fmv_tracker_initialized",
            timezone=timezone_name,
            close_time=f"{close_hour:02d}:{close_minute:02d}",
            prev_close_ttl_sec=prev_close_ttl,
        )

    def seed_prev_close(self, symbol: str, prev_close: float) -> None:
        """Manually seed a previous close price for a symbol (useful for testing)."""
        state = self._states.setdefault(symbol, _FmvState())
        state.prev_close = prev_close
        logger.info("fmv_prev_close_seeded", symbol=symbol, prevClose=prev_close)

    async def process_fmv(
        self,
        symbol: str,
        *,
        price: float,
        ts: Optional[int],
    ) -> Dict[str, Optional[float]]:
        """
        Process an FMV event and calculate daily P/L metrics.
        
        Args:
            symbol: Stock ticker symbol
            price: FMV price
            ts: Timestamp (nanoseconds, microseconds, milliseconds, or seconds)
        
        Returns:
            Dict with: price, prevClose, dailyChange, dailyChangePct
        """
        async with self._lock:
            dt_local = self._resolve_dt(ts)
            day_key = dt_local.date().isoformat()

            state = self._states.setdefault(symbol, _FmvState())
            
            # Initialize day on first event for this symbol
            if state.day is None:
                state.day = day_key
                logger.debug("fmv_state_initialized", symbol=symbol, day=day_key)

            # Load prev_close from Redis on first event if not in memory
            if state.prev_close is None:
                loaded = await self._sink.get_prev_close(symbol)
                if loaded is not None:
                    state.prev_close = loaded
                    logger.info("fmv_prev_close_loaded_from_redis", symbol=symbol, prevClose=loaded, day=day_key)
                else:
                    logger.debug("fmv_prev_close_not_found", symbol=symbol, day=day_key, note="Will populate after market close")

            # Handle day rollover
            if state.day != day_key:
                prev_close_candidate = state.pending_close_price or state.last_price
                if prev_close_candidate is not None:
                    state.prev_close = prev_close_candidate
                    await self._persist_prev_close(symbol, prev_close_candidate)
                    logger.info(
                        "fmv_day_rollover",
                        symbol=symbol,
                        prevClose=state.prev_close,
                        fromDay=state.day,
                        toDay=day_key,
                        source="pending_close" if state.pending_close_price else "last_price",
                    )
                else:
                    logger.warning("fmv_day_rollover_no_prev_close", symbol=symbol, fromDay=state.day, toDay=day_key)
                
                # Reset for new day
                state.day = day_key
                state.pending_close_price = None

            # Update last price for this day
            state.last_price = price

            # Check if we're after market close and should capture closing price
            if self._is_after_close(dt_local):
                if state.pending_close_price != price:
                    state.pending_close_price = price
                    await self._persist_prev_close(symbol, price)
                    logger.debug(
                        "fmv_close_price_captured",
                        symbol=symbol,
                        price=price,
                        day=state.day,
                        time=dt_local.strftime("%H:%M:%S %Z"),
                    )

            # Calculate daily P/L metrics
            prev_close = state.prev_close
            daily_change = None
            daily_change_pct = None
            
            if price is not None and prev_close is not None:
                daily_change = price - prev_close
                if prev_close != 0:
                    daily_change_pct = (daily_change / prev_close) * 100
                    
                logger.debug(
                    "fmv_pl_calculated",
                    symbol=symbol,
                    price=price,
                    prevClose=prev_close,
                    dailyChange=round(daily_change, 4),
                    dailyChangePct=round(daily_change_pct, 4),
                )

            return {
                "price": price,
                "prevClose": prev_close,
                "dailyChange": daily_change,
                "dailyChangePct": daily_change_pct,
            }

    def _is_after_close(self, dt_local: datetime) -> bool:
        """Check if the given time is at or after market close time."""
        return (dt_local.hour, dt_local.minute) >= (self._close_hour, self._close_minute)

    def _resolve_dt(self, ts: Optional[int]) -> datetime:
        """Convert timestamp to timezone-aware datetime."""
        if ts is None:
            return datetime.now(timezone.utc).astimezone(self._tz)
        seconds = self._normalize_seconds(ts)
        return datetime.fromtimestamp(seconds, tz=timezone.utc).astimezone(self._tz)

    @staticmethod
    def _normalize_seconds(ts: int) -> float:
        """Normalize various timestamp formats to seconds."""
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
        """Persist previous close price to Redis with TTL."""
        try:
            await self._sink.set_prev_close(symbol, price, ttl=self._prev_close_ttl)
            logger.debug("fmv_prev_close_persisted", symbol=symbol, price=price, ttl=self._prev_close_ttl)
        except Exception as exc:  # noqa: BLE001
            logger.error("fmv_prev_close_persist_error", symbol=symbol, error=str(exc))

