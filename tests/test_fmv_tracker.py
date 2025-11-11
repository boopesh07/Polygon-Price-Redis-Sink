from __future__ import annotations

import unittest
from datetime import datetime, timezone
from typing import Any, Dict, Optional, List

from zoneinfo import ZoneInfo

import sys
from pathlib import Path
import asyncio

sys.path.append(str(Path(__file__).resolve().parents[1] / "src"))

from polygon_sink.fmv_tracker import FmvTracker
from polygon_sink.redis_sink import BaseSink


class _DummySink(BaseSink):
    def __init__(self) -> None:
        self.prev: Dict[str, float] = {}

    async def set_prev_close(self, symbol: str, price: float, ttl: Optional[int] = None) -> None:
        self.prev[symbol] = price

    async def get_prev_close(self, symbol: str) -> Optional[float]:
        return self.prev.get(symbol)

    # Unused abstract methods
    async def set_latest_price(self, symbol: str, payload: Dict[str, Any]) -> None:
        raise NotImplementedError

    async def set_latest_agg1m(self, symbol: str, payload: Dict[str, Any]) -> None:
        raise NotImplementedError

    async def set_trade(self, symbol: str, payload: Dict[str, Any]) -> None:
        raise NotImplementedError

    async def set_quote(self, symbol: str, payload: Dict[str, Any]) -> None:
        raise NotImplementedError

    async def set_fmv(self, symbol: str, payload: Dict[str, Any]) -> None:
        raise NotImplementedError

    async def set_agg5m_snapshot(self, symbol: str, day: str, entries: List[Dict[str, Any]], ttl: Optional[int] = None) -> None:
        raise NotImplementedError

    async def ping(self) -> bool:
        return True

    async def close(self) -> None:
        return


def _ts(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


class FmvTrackerTest(unittest.TestCase):
    def test_price_and_daily_change(self) -> None:
        sink = _DummySink()
        tracker = FmvTracker(sink, "America/New_York", 16, 0, 604800)
        tracker.seed_prev_close("AAPL", 100.0)

        tz = ZoneInfo("America/New_York")
        ts_midday = _ts(datetime(2024, 5, 27, 11, 30, tzinfo=tz))

        metrics = asyncio.run(tracker.process_fmv("AAPL", price=101.0, ts=ts_midday))

        self.assertAlmostEqual(metrics["price"], 101.0)
        self.assertAlmostEqual(metrics["dailyChange"], 1.0)
        self.assertAlmostEqual(metrics["dailyChangePct"], 1.0)
        self.assertEqual(metrics["prevClose"], 100.0)

    def test_day_rollover_uses_pending_close(self) -> None:
        sink = _DummySink()
        tracker = FmvTracker(sink, "America/New_York", 16, 0, 604800)
        tz = ZoneInfo("America/New_York")

        # Day 1 midday FMV to establish state
        sink.prev["MSFT"] = 200.0
        asyncio.run(tracker.process_fmv("MSFT", price=202.0, ts=_ts(datetime(2024, 5, 27, 12, 0, tzinfo=tz))))

        # FMV after close to capture pending close price
        ts_close = _ts(datetime(2024, 5, 27, 16, 5, tzinfo=tz))
        metrics_close = asyncio.run(tracker.process_fmv("MSFT", price=206.0, ts=ts_close))
        self.assertAlmostEqual(metrics_close["price"], 206.0)
        # Prev close should still reference prior day until rollover
        self.assertAlmostEqual(metrics_close["prevClose"], 200.0)

        # Next trading day open FMV should roll prev close to previous pending value
        metrics_next = asyncio.run(tracker.process_fmv("MSFT", price=209.0, ts=_ts(datetime(2024, 5, 28, 9, 35, tzinfo=tz))))

        self.assertAlmostEqual(metrics_next["prevClose"], 206.0)
        self.assertAlmostEqual(metrics_next["price"], 209.0)
        self.assertAlmostEqual(metrics_next["dailyChange"], 3.0)
        self.assertAlmostEqual(metrics_next["dailyChangePct"], 3.0 / 206.0 * 100)

    def test_no_prev_close_returns_none(self) -> None:
        sink = _DummySink()
        tracker = FmvTracker(sink, "America/New_York", 16, 0, 604800)
        tz = ZoneInfo("America/New_York")
        ts_midday = _ts(datetime(2024, 5, 27, 10, 0, tzinfo=tz))

        metrics = asyncio.run(tracker.process_fmv("TSLA", price=180.5, ts=ts_midday))
        self.assertAlmostEqual(metrics["price"], 180.5)
        self.assertIsNone(metrics["prevClose"])
        self.assertIsNone(metrics["dailyChange"])
        self.assertIsNone(metrics["dailyChangePct"])

    def test_prev_close_loaded_from_redis(self) -> None:
        sink = _DummySink()
        tracker = FmvTracker(sink, "America/New_York", 16, 0, 604800)
        tz = ZoneInfo("America/New_York")
        
        # Pre-populate Redis with prev_close
        sink.prev["NVDA"] = 450.0
        
        ts_midday = _ts(datetime(2024, 5, 27, 14, 0, tzinfo=tz))
        metrics = asyncio.run(tracker.process_fmv("NVDA", price=455.0, ts=ts_midday))
        
        self.assertAlmostEqual(metrics["price"], 455.0)
        self.assertAlmostEqual(metrics["prevClose"], 450.0)
        self.assertAlmostEqual(metrics["dailyChange"], 5.0)
        self.assertAlmostEqual(metrics["dailyChangePct"], (5.0 / 450.0) * 100)

    def test_close_price_captured_after_market_close(self) -> None:
        sink = _DummySink()
        tracker = FmvTracker(sink, "America/New_York", 16, 0, 604800)
        tz = ZoneInfo("America/New_York")
        
        tracker.seed_prev_close("GOOG", 2800.0)
        
        # Before market close
        ts_before = _ts(datetime(2024, 5, 27, 15, 55, tzinfo=tz))
        asyncio.run(tracker.process_fmv("GOOG", price=2850.0, ts=ts_before))
        
        # After market close - should capture as pending close
        ts_after = _ts(datetime(2024, 5, 27, 16, 10, tzinfo=tz))
        asyncio.run(tracker.process_fmv("GOOG", price=2875.0, ts=ts_after))
        
        # Verify it was persisted to Redis
        self.assertAlmostEqual(sink.prev["GOOG"], 2875.0)


if __name__ == "__main__":
    unittest.main()

