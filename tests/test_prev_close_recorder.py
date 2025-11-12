from __future__ import annotations

import asyncio
import unittest
from datetime import datetime
from typing import Any, Dict, List, Optional

from zoneinfo import ZoneInfo

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1] / "src"))

from polygon_sink.prev_close_recorder import PrevCloseRecorder
from polygon_sink.redis_sink import BaseSink


class _DummySink(BaseSink):
    def __init__(self) -> None:
        self.records: List[Dict[str, Any]] = []
        self.prev_values: Dict[str, float] = {}

    async def set_prev_close(self, symbol: str, price: float, ttl: Optional[int] = None) -> None:
        self.records.append({"symbol": symbol, "price": price, "ttl": ttl})
        self.prev_values[symbol] = price

    async def get_prev_close(self, symbol: str) -> Optional[float]:
        return self.prev_values.get(symbol)

    async def ping(self) -> bool:
        return True

    # Remaining BaseSink methods are unused for this test.
    async def set_latest_price(self, symbol: str, payload: Dict[str, Any]) -> None:  # pragma: no cover
        raise NotImplementedError

    async def set_latest_agg1m(self, symbol: str, payload: Dict[str, Any]) -> None:  # pragma: no cover
        raise NotImplementedError

    async def set_trade(self, symbol: str, payload: Dict[str, Any]) -> None:  # pragma: no cover
        raise NotImplementedError

    async def set_quote(self, symbol: str, payload: Dict[str, Any]) -> None:  # pragma: no cover
        raise NotImplementedError

    async def set_fmv(self, symbol: str, payload: Dict[str, Any]) -> None:  # pragma: no cover
        raise NotImplementedError

    async def set_agg5m_snapshot(self, symbol: str, day: str, entries: List[Dict[str, Any]], ttl: Optional[int] = None) -> None:  # pragma: no cover
        raise NotImplementedError


def _ts_local(dt_local: datetime) -> int:
    dt_utc = dt_local.astimezone(ZoneInfo("UTC"))
    return int(dt_utc.timestamp() * 1000)


class PrevCloseRecorderTest(unittest.TestCase):
    def test_records_once_after_close(self) -> None:
        sink = _DummySink()
        recorder = PrevCloseRecorder(
            sink,
            timezone_name="America/New_York",
            close_hour=16,
            close_minute=0,
            prev_close_ttl=600,
        )
        tz = ZoneInfo("America/New_York")
        before_close = datetime(2024, 5, 27, 15, 55, tzinfo=tz)
        after_close = datetime(2024, 5, 27, 16, 5, tzinfo=tz)

        asyncio.run(recorder.maybe_record_from_fmv("AAPL", 100.0, _ts_local(before_close)))
        self.assertFalse(sink.records)

        asyncio.run(recorder.maybe_record_from_fmv("AAPL", 101.0, _ts_local(after_close)))
        self.assertEqual(len(sink.records), 1)

        asyncio.run(recorder.maybe_record_from_fmv("AAPL", 102.0, _ts_local(after_close)))
        self.assertEqual(len(sink.records), 1)

    def test_records_next_day(self) -> None:
        sink = _DummySink()
        recorder = PrevCloseRecorder(
            sink,
            timezone_name="America/New_York",
            close_hour=16,
            close_minute=0,
            prev_close_ttl=600,
        )
        tz = ZoneInfo("America/New_York")
        day1 = datetime(2024, 5, 27, 16, 1, tzinfo=tz)
        day2 = datetime(2024, 5, 28, 16, 2, tzinfo=tz)

        asyncio.run(recorder.maybe_record_from_fmv("MSFT", 200.0, _ts_local(day1)))
        asyncio.run(recorder.maybe_record_from_fmv("MSFT", 210.0, _ts_local(day2)))
        self.assertEqual(len(sink.records), 2)
        self.assertEqual(sink.records[-1]["price"], 210.0)

    def test_get_prev_close_reads_sink_value(self) -> None:
        sink = _DummySink()
        sink.prev_values["GOOG"] = 300.0
        recorder = PrevCloseRecorder(
            sink,
            timezone_name="America/New_York",
            close_hour=16,
            close_minute=0,
            prev_close_ttl=600,
        )
        first = asyncio.run(recorder.get_prev_close("GOOG"))
        self.assertAlmostEqual(first or 0, 300.0)

        tz = ZoneInfo("America/New_York")
        asyncio.run(recorder.maybe_record_from_fmv("GOOG", 350.0, _ts_local(datetime(2024, 5, 27, 16, 1, tzinfo=tz))))
        # simulate external deletion + new value
        sink.prev_values["GOOG"] = 360.0
        second = asyncio.run(recorder.get_prev_close("GOOG"))
        self.assertAlmostEqual(second or 0, 360.0)


if __name__ == "__main__":
    unittest.main()
