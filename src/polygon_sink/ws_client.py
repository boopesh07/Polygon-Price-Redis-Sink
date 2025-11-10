from __future__ import annotations

import asyncio
import json
from typing import Any, Dict, List, Optional

import websockets
from websockets.client import WebSocketClientProtocol

from .config import Settings, mask_api_key
from .logging_setup import get_logger
from .redis_sink import BaseSink
from .agg5m import Agg5mCollector
from .quote_tracker import QuoteTracker


logger = get_logger()


class PolygonWsClient:
    def __init__(
        self,
        settings: Settings,
        sink: BaseSink,
        channels: Optional[List[str]] = None,
        host_override: Optional[str] = None,
        symbols_override: Optional[List[str]] = None,
        agg5m_collector: Optional[Agg5mCollector] = None,
        quote_tracker: Optional[QuoteTracker] = None,
    ):
        self._settings = settings
        self._sink = sink
        self._stop_event = asyncio.Event()
        self._is_authenticated: bool = False
        self._trade_counts: Dict[str, Dict[str, int]] = {}
        self._agg_counts: Dict[str, Dict[str, int]] = {}
        self._debug = settings.ws_debug
        self._channels = [c.upper() for c in (channels or ["T", "AM"])]
        self._host_override = host_override
        self._symbols = [s.upper() for s in (symbols_override if symbols_override is not None else settings.symbols)]
        self._agg5m = agg5m_collector
        self._quote_tracker = quote_tracker

    def _debug_log(self, direction: str, raw: str) -> None:
        if self._debug:
            snippet = raw[:1000]
            logger.info(
                "ws_debug",
                direction=direction,
                payload=snippet,
            )

    async def run_forever(self) -> None:
        backoff = self._settings.backoff_initial_ms / 1000.0
        while not self._stop_event.is_set():
            try:
                await self._connect_and_run()
                backoff = self._settings.backoff_initial_ms / 1000.0
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                logger.error("ws_loop_error", error=str(exc))
                await asyncio.sleep(backoff)
                backoff = min(backoff * self._settings.backoff_factor, self._settings.backoff_max_ms / 1000.0)

    async def stop(self) -> None:
        self._stop_event.set()

    async def _connect_and_run(self) -> None:
        # If running split hosts, choose the URL based on the first requested channel
        primary = self._channels[0] if self._channels else "AM"
        url = self._host_override or self._settings.normalized_ws_url_for(primary)
        logger.info("ws_connecting", url=url, symbols=self._symbols, channels=self._channels)
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                await self._handle_connection(ws)
        except websockets.ConnectionClosedOK:
            logger.info("ws_closed_ok")
        except websockets.ConnectionClosedError as exc:  # type: ignore[attr-defined]
            logger.warning("ws_closed_error", code=getattr(exc, "code", None))
        except Exception as exc:  # noqa: BLE001
            logger.error("ws_conn_error", error=str(exc))

    async def _handle_connection(self, ws: WebSocketClientProtocol) -> None:
        self._is_authenticated = False
        # Receive and handle messages; auth and subscribe on status events
        async def sender() -> None:
            # no-op: we send only in response to status events
            await self._stop_event.wait()

        async def receiver() -> None:
            async for raw in ws:
                if isinstance(raw, (bytes, bytearray)):
                    try:
                        raw_text = raw.decode("utf-8")
                    except Exception:
                        continue
                else:
                    raw_text = str(raw)
                self._debug_log("in", raw_text)
                await self._on_message(ws, raw_text)

        done, pending = await asyncio.wait(
            [asyncio.create_task(sender()), asyncio.create_task(receiver())],
            return_when=asyncio.FIRST_COMPLETED,
        )
        for task in pending:
            task.cancel()

    async def _send_json_str(self, ws: WebSocketClientProtocol, obj: Dict[str, Any]) -> None:
        payload = json.dumps(obj)
        self._debug_log("out", payload)
        await ws.send(payload)

    async def _on_message(self, ws: WebSocketClientProtocol, raw: str) -> None:
        try:
            parsed = json.loads(raw)
        except Exception as exc:  # noqa: BLE001
            logger.error("ws_parse_error", error=str(exc))
            return

        events: List[Dict[str, Any]] = parsed if isinstance(parsed, list) else [parsed]
        for evt in events:
            await self._handle_event(ws, evt)

    async def _handle_event(self, ws: WebSocketClientProtocol, evt: Dict[str, Any]) -> None:
        if not evt:
            return
        if evt.get("ev") == "status":
            status = evt.get("status")
            logger.info("ws_status", status=status, message=evt.get("message"))
            if status == "connected":
                # send auth
                await self._send_json_str(ws, {"action": "auth", "params": self._settings.polygon_api_key})
            elif status == "auth_success":
                self._is_authenticated = True
                # Batch subscriptions to avoid overly large payloads
                channel_prefixes: List[str] = []
                if "T" in self._channels:
                    channel_prefixes.append("T")
                if "Q" in self._channels:
                    channel_prefixes.append("Q")
                if "AM" in self._channels:
                    channel_prefixes.append("AM")
                if "FMV" in self._channels:
                    channel_prefixes.append("FMV")
                batch_size = max(1, int(self._settings.subscribe_batch_size))
                for i in range(0, len(self._symbols), batch_size):
                    batch = self._symbols[i : i + batch_size]
                    parts: List[str] = []
                    for s in batch:
                        for p in channel_prefixes:
                            parts.append(f"{p}.{s}")
                    sub = ",".join(parts)
                    logger.info("ws_subscribing", batchStart=i, batchCount=len(batch))
                    await self._send_json_str(ws, {"action": "subscribe", "params": sub})
            elif status == "error":
                logger.error("ws_status_error", message=evt.get("message"))
            return

        if evt.get("ev") == "T" and evt.get("sym") and isinstance(evt.get("p"), (int, float)):
            sym = str(evt["sym"]).upper()
            # Write trade-specific key only (no stock:latest)
            # Richer record when available
            trade_payload = {
                "symbol": sym,
                "price": float(evt["p"]),
                "ts": int(evt["t"]) if isinstance(evt.get("t"), (int, float)) else None,
                "size": int(evt["s"]) if isinstance(evt.get("s"), (int, float)) else None,
                "conditions": evt.get("c"),
                "updatedAt": __import__("datetime").datetime.utcnow().isoformat() + "Z",
            }
            await self._sink.set_trade(sym, trade_payload)
            # Emit raw NDJSON for S3 cold path
            if hasattr(self._sink, "write_raw_event"):
                try:
                    await getattr(self._sink, "write_raw_event")("T", sym, evt)
                except Exception:
                    pass
            c = self._trade_counts.get(sym) or {"count": 0}
            c["count"] += 1
            if isinstance(evt.get("t"), (int, float)):
                c["lastTs"] = int(evt["t"])  # type: ignore[index]
            self._trade_counts[sym] = c
            return

        if evt.get("ev") == "AM" and evt.get("sym"):
            sym = str(evt["sym"]).upper()
            bar = {
                "symbol": sym,
                "start": int(evt.get("s") or 0),
                "end": int(evt.get("e") or 0),
                "open": float(evt.get("o") or 0),
                "high": float(evt.get("h") or 0),
                "low": float(evt.get("l") or 0),
                "close": float(evt.get("c") or 0),
                "volume": int(evt.get("v") or 0),
                "vwap": float(evt.get("a")) if isinstance(evt.get("a"), (int, float)) else None,
                "updatedAt": __import__("datetime").datetime.utcnow().isoformat() + "Z",
            }
            await self._sink.set_latest_agg1m(sym, bar)
            if self._agg5m:
                await self._agg5m.on_minute_bar(sym, bar)
            if hasattr(self._sink, "write_raw_event"):
                try:
                    await getattr(self._sink, "write_raw_event")("AM", sym, evt)
                except Exception:
                    pass
            c = self._agg_counts.get(sym) or {"count": 0}
            c["count"] += 1
            if isinstance(evt.get("e"), (int, float)):
                c["lastTs"] = int(evt["e"])  # type: ignore[index]
            self._agg_counts[sym] = c
            return

        # Quotes: Event type on Polygon is typically "Q" with fields like bid/ask
        if evt.get("ev") == "Q" and evt.get("sym"):
            sym = str(evt["sym"]).upper()
            bid = float(evt.get("bp")) if isinstance(evt.get("bp"), (int, float)) else None
            ask = float(evt.get("ap")) if isinstance(evt.get("ap"), (int, float)) else None
            ts_raw = int(evt.get("t")) if isinstance(evt.get("t"), (int, float)) else None
            quote = {
                "symbol": sym,
                "bid": bid,
                "bidSize": int(evt.get("bs")) if isinstance(evt.get("bs"), (int, float)) else None,
                "ask": ask,
                "askSize": int(evt.get("as")) if isinstance(evt.get("as"), (int, float)) else None,
                "ts": ts_raw,
                "updatedAt": __import__("datetime").datetime.utcnow().isoformat() + "Z",
            }
            if self._quote_tracker:
                metrics = await self._quote_tracker.process_quote(sym, bid=bid, ask=ask, ts=ts_raw)
                quote.update(metrics)
            else:
                price = None
                if bid is not None and ask is not None:
                    price = (bid + ask) / 2.0
                elif bid is not None:
                    price = bid
                elif ask is not None:
                    price = ask
                quote["price"] = price
                quote["prevClose"] = None
                quote["dailyChange"] = None
                quote["dailyChangePct"] = None
            await self._sink.set_quote(sym, quote)
            if hasattr(self._sink, "write_raw_event"):
                try:
                    await getattr(self._sink, "write_raw_event")("Q", sym, evt)
                except Exception:
                    pass
            return

        # FMV: Fair Market Value per-second indicative price; assume ev == "FMV" and fields p (price), t (ts)
        if evt.get("ev") == "FMV" and evt.get("sym") and isinstance(evt.get("fmv"), (int, float)):
            sym = str(evt["sym"]).upper()
            fmv = {
                "symbol": sym,
                "price": float(evt["p"]),
                "ts": int(evt.get("t")) if isinstance(evt.get("t"), (int, float)) else None,
                "updatedAt": __import__("datetime").datetime.utcnow().isoformat() + "Z",
            }
            await self._sink.set_fmv(sym, fmv)
            if hasattr(self._sink, "write_raw_event"):
                try:
                    await getattr(self._sink, "write_raw_event")("FMV", sym, evt)
                except Exception:
                    pass
            return

    def health(self) -> Dict[str, Any]:
        trades = {s: self._trade_counts.get(s, {"count": 0}) for s in self._symbols}
        aggs = {s: self._agg_counts.get(s, {"count": 0}) for s in self._symbols}
        return {
            "authenticated": self._is_authenticated,
            "subscribedSymbols": len(self._symbols),
            "tradesPerSymbol": trades,
            "aggsPerSymbol": aggs,
        }
