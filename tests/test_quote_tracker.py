from __future__ import annotations

import unittest
from datetime import datetime, timezone
from typing import Any, Dict, Optional, List

from zoneinfo import ZoneInfo

import sys
from pathlib import Path
import asyncio

sys.path.append(str(Path(__file__).resolve().parents[1] / "src"))

from polygon_sink.quote_tracker import QuoteTracker
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


class QuoteTrackerTest(unittest.TestCase):
    def test_price_and_daily_change(self) -> None:
        sink = _DummySink()
        tracker = QuoteTracker(sink, "America/New_York", 16, 0, 604800)
        tracker.seed_prev_close("AAPL", 100.0)

        tz = ZoneInfo("America/New_York")
        ts_midday = _ts(datetime(2024, 5, 27, 11, 30, tzinfo=tz))

        metrics = asyncio.run(tracker.process_quote("AAPL", bid=100.0, ask=102.0, ts=ts_midday))

        self.assertAlmostEqual(metrics["price"], 101.0)
        self.assertAlmostEqual(metrics["dailyChange"], 1.0)
        self.assertAlmostEqual(metrics["dailyChangePct"], 1.0)
        self.assertEqual(metrics["prevClose"], 100.0)

    def test_day_rollover_uses_pending_close(self) -> None:
        sink = _DummySink()
        tracker = QuoteTracker(sink, "America/New_York", 16, 0, 604800)
        tz = ZoneInfo("America/New_York")

        # Day 1 midday quote to establish state
        sink.prev["MSFT"] = 200.0
        asyncio.run(tracker.process_quote("MSFT", bid=201.0, ask=203.0, ts=_ts(datetime(2024, 5, 27, 12, 0, tzinfo=tz))))

        # Quote after close to capture pending close price
        ts_close = _ts(datetime(2024, 5, 27, 16, 5, tzinfo=tz))
        metrics_close = asyncio.run(tracker.process_quote("MSFT", bid=205.0, ask=207.0, ts=ts_close))
        self.assertAlmostEqual(metrics_close["price"], 206.0)
        # Prev close should still reference prior day until rollover
        self.assertAlmostEqual(metrics_close["prevClose"], 200.0)

        # Next trading day open quote should roll prev close to previous pending value
        metrics_next = asyncio.run(tracker.process_quote("MSFT", bid=208.0, ask=210.0, ts=_ts(datetime(2024, 5, 28, 9, 35, tzinfo=tz))))

        self.assertAlmostEqual(metrics_next["prevClose"], 206.0)
        self.assertAlmostEqual(metrics_next["price"], 209.0)
        self.assertAlmostEqual(metrics_next["dailyChange"], 3.0)
        self.assertAlmostEqual(metrics_next["dailyChangePct"], 3.0 / 206.0 * 100)

    def test_price_fallback_to_single_side(self) -> None:
        sink = _DummySink()
        tracker = QuoteTracker(sink, "America/New_York", 16, 0, 604800)
        tz = ZoneInfo("America/New_York")
        ts_midday = _ts(datetime(2024, 5, 27, 10, 0, tzinfo=tz))

        metrics = asyncio.run(tracker.process_quote("TSLA", bid=None, ask=180.5, ts=ts_midday))
        self.assertAlmostEqual(metrics["price"], 180.5)

        metrics_bid_only = asyncio.run(tracker.process_quote("TSLA", bid=179.0, ask=None, ts=ts_midday))
        self.assertAlmostEqual(metrics_bid_only["price"], 179.0)


if __name__ == "__main__":
    unittest.main()
