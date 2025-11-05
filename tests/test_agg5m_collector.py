from __future__ import annotations

import asyncio
import unittest
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional
import sys
from pathlib import Path

from zoneinfo import ZoneInfo

sys.path.append(str(Path(__file__).resolve().parents[1] / "src"))

from polygon_sink.agg5m import Agg5mCollector
from polygon_sink.redis_sink import BaseSink


class DummySink(BaseSink):
    def __init__(self) -> None:
        self.calls: List[Dict[str, Any]] = []

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
        self.calls.append({"symbol": symbol, "day": day, "bars": entries, "ttl": ttl})

    async def ping(self) -> bool:
        return True


def _make_bar(start_dt: datetime, close: float) -> Dict[str, Any]:
    end_dt = start_dt + timedelta(minutes=1)
    return {
        "symbol": "AAPL",
        "start": int(start_dt.timestamp() * 1000),
        "end": int(end_dt.timestamp() * 1000),
        "close": close,
    }


class Agg5mCollectorTest(unittest.TestCase):
    def test_flush_accumulates_five_minute_bucket(self) -> None:
        sink = DummySink()
        collector = Agg5mCollector(
            sink,
            flush_interval_sec=900,
            ttl_sec=120,
            timezone_name="America/New_York",
            max_bars=20,
        )

        async def _scenario() -> None:
            base = (datetime.now(timezone.utc) - timedelta(hours=4)).replace(second=0, microsecond=0)
            minute_delta = base.minute % 5
            base -= timedelta(minutes=minute_delta)
            for idx in range(5):
                await collector.on_minute_bar("AAPL", _make_bar(base + timedelta(minutes=idx), 100.0 + idx))
            await collector.flush()
            await collector.close()

        asyncio.run(_scenario())

        self.assertTrue(sink.calls, "Expected at least one Redis write")
        payload = sink.calls[-1]
        self.assertEqual(payload["symbol"], "AAPL")
        tz = ZoneInfo("America/New_York")
        expected_day = datetime.fromtimestamp(payload["bars"][-1]["ts"] / 1000, tz=timezone.utc).astimezone(tz).date().isoformat()
        self.assertEqual(payload["day"], expected_day)
        self.assertEqual(len(payload["bars"]), 1)
        self.assertAlmostEqual(payload["bars"][0]["close"], 104.0)
        self.assertEqual(payload["ttl"], 120)

    def test_day_rollover_resets_state(self) -> None:
        sink = DummySink()
        collector = Agg5mCollector(
            sink,
            flush_interval_sec=900,
            ttl_sec=300,
            timezone_name="America/New_York",
            max_bars=20,
        )

        async def _scenario() -> None:
            day_one_start = datetime(2024, 5, 27, 13, 30, tzinfo=timezone.utc)
            for idx in range(5):
                await collector.on_minute_bar("AAPL", _make_bar(day_one_start + timedelta(minutes=idx), 150.0 + idx))
            await collector.flush()

            day_two_start = day_one_start + timedelta(days=1)
            await collector.on_minute_bar("AAPL", _make_bar(day_two_start, 200.0))
            await collector.flush()
            await collector.close()

        asyncio.run(_scenario())

        self.assertGreaterEqual(len(sink.calls), 2)
        day_one_payload = sink.calls[0]
        self.assertEqual(day_one_payload["day"], "2024-05-27")
        self.assertTrue(day_one_payload["bars"])
        day_two_payload = sink.calls[-1]
        self.assertEqual(day_two_payload["day"], "2024-05-28")
        self.assertTrue(day_two_payload["bars"])
        self.assertAlmostEqual(day_two_payload["bars"][0]["close"], 200.0)


if __name__ == "__main__":
    unittest.main()
