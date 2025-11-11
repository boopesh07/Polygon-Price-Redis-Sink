from __future__ import annotations

import asyncio
import json
from typing import Any, Dict, List, Optional

from massive import WebSocketClient
from massive.websocket.models import WebSocketMessage, Feed, Market

from .agg5m import Agg5mCollector
from .config import Settings, mask_api_key
from .logging_setup import get_logger
from .quote_tracker import QuoteTracker
from .redis_sink import BaseSink

logger = get_logger()


def _to_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_int(value: Any) -> Optional[int]:
    try:
        if value is None:
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


class MassiveWsClient:
    """Massive WebSocket client that subscribes to AM, T, Q, and FMV channels."""

    def __init__(
        self,
        settings: Settings,
        sink: BaseSink,
        channels: List[str],
        agg5m_collector: Optional[Agg5mCollector] = None,
        quote_tracker: Optional[QuoteTracker] = None,
    ) -> None:
        self._settings = settings
        self._sink = sink
        self._channels = [c.upper() for c in channels]
        self._agg5m = agg5m_collector
        self._quote_tracker = quote_tracker
        self._stop_event = asyncio.Event()
        self._is_running = False
        self._client: Optional[WebSocketClient] = None
        self._event_loop: Optional[asyncio.AbstractEventLoop] = None
        self._debug = settings.ws_debug

        is_realtime = any(ch in ("AM", "FMV") for ch in self._channels)
        self._feed = Feed.Business if is_realtime else Feed.DelayedBusiness
        logger.info(
            "massive_ws_client_init",
            channels=self._channels,
            feed=str(self._feed),
            ws_debug=self._debug,
        )

    def _log_ws_request(self, action: str, params: str) -> None:
        payload = mask_api_key(params) if action == "auth" else params
        logger.info("ws_request", action=action, params=payload)

    def _log_ws_response(self, msgs: List[Dict[str, Any]]) -> None:
        if not msgs:
            return
        try:
            preview = json.dumps(msgs)
        except Exception:
            preview = str(msgs)
        snippet = preview[:1000]
        logger.info("ws_response", payload_preview=snippet, payload_length=len(preview))

    async def run_forever(self) -> None:
        backoff = max(0.1, self._settings.backoff_initial_ms / 1000.0)
        while not self._stop_event.is_set():
            try:
                await self._connect_and_run()
                backoff = max(0.1, self._settings.backoff_initial_ms / 1000.0)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                logger.error("ws_loop_error", error=str(exc), exc_info=True)
                await asyncio.sleep(backoff)
                backoff = min(
                    backoff * max(1.0, self._settings.backoff_factor),
                    max(0.1, self._settings.backoff_max_ms / 1000.0),
                )

    async def stop(self) -> None:
        logger.info("ws_stopping")
        self._stop_event.set()
        if self._client and hasattr(self._client, "close"):
            close_method = getattr(self._client, "close")
            try:
                if asyncio.iscoroutinefunction(close_method):
                    await close_method()
                else:
                    close_method()
            except Exception as exc:  # noqa: BLE001
                logger.error("ws_close_error", error=str(exc))

    async def _connect_and_run(self) -> None:
        logger.info(
            "ws_connecting",
            channels=self._channels,
            feed=str(self._feed),
            api_key_masked=mask_api_key(self._settings.polygon_api_key),
        )

        self._event_loop = asyncio.get_event_loop()
        self._client = WebSocketClient(
            api_key=self._settings.polygon_api_key,
            feed=self._feed,
            market=Market.Stocks,
        )

        subscriptions: List[str] = []
        for ch in self._channels:
            topic = f"{ch}.*"
            subscriptions.append(topic)
            self._log_ws_request("subscribe", topic)
            self._client.subscribe(topic)

        logger.info("ws_subscribed", count=len(subscriptions), subscriptions=subscriptions)
        self._is_running = True

        import concurrent.futures

        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(self._client.run, handle_msg=self._handle_message_sync)
                while not self._stop_event.is_set():
                    if future.done():
                        exc = future.exception()
                        if exc:
                            raise exc
                        break
                    await asyncio.sleep(0.1)
        finally:
            self._is_running = False

    def _handle_message_sync(self, msgs: List[WebSocketMessage]) -> None:
        if not msgs:
            return
        try:
            loop = self._event_loop or asyncio.get_event_loop()
            asyncio.run_coroutine_threadsafe(self._handle_message(msgs), loop)
        except RuntimeError:
            logger.warning("ws_no_event_loop", msg_count=len(msgs))
            asyncio.run(self._handle_message(msgs))

    async def _handle_message(self, msgs: List[WebSocketMessage]) -> None:
        payloads: List[Dict[str, Any]] = []
        for msg in msgs:
            if hasattr(msg, "dict"):
                payloads.append(msg.dict())
            elif hasattr(msg, "__dict__"):
                payloads.append(msg.__dict__)
            else:
                payloads.append({"raw": str(msg)})
        self._log_ws_response(payloads)

        for evt in payloads:
            try:
                await self._process_event(evt)
            except Exception as exc:  # noqa: BLE001
                logger.error("ws_event_processing_error", error=str(exc), raw_event=evt, exc_info=True)

    async def _process_event(self, evt: Dict[str, Any]) -> None:
        event_type = evt.get("event_type") or evt.get("ev")
        if not event_type:
            logger.debug("ws_event_missing_ev", raw_event=evt)
            return

        if event_type.lower() == "status":
            logger.info("ws_status_event", payload=evt)
            return

        symbol_raw = evt.get("symbol") or evt.get("sym") or evt.get("ticker")
        if not isinstance(symbol_raw, str):
            logger.debug("ws_event_missing_symbol", event_type=event_type, raw_event=evt)
            return
        symbol = symbol_raw.upper()

        logger.info("ws_data_event", event_type=event_type, symbol=symbol, full_event=evt)

        if event_type == "T":
            await self._handle_trade(symbol, evt)
        elif event_type == "Q":
            await self._handle_quote(symbol, evt)
        elif event_type == "AM":
            await self._handle_aggregate(symbol, evt)
        elif event_type == "FMV":
            await self._handle_fmv(symbol, evt)
        else:
            logger.warning("ws_event_unknown_type", event_type=event_type, symbol=symbol)

    async def _handle_trade(self, symbol: str, evt: Dict[str, Any]) -> None:
        price = _to_float(evt.get("price") or evt.get("p"))
        size = _to_int(evt.get("size") or evt.get("s"))
        ts = _to_int(evt.get("timestamp") or evt.get("t"))
        trade_payload = {
            "symbol": symbol,
            "price": price,
            "ts": ts,
            "size": size,
            "conditions": evt.get("conditions") or evt.get("c"),
            "updatedAt": __import__("datetime").datetime.utcnow().isoformat() + "Z",
        }
        await self._sink.set_trade(symbol, trade_payload)
        if hasattr(self._sink, "write_raw_event"):
            try:
                await getattr(self._sink, "write_raw_event")("T", symbol, evt)
            except Exception:
                pass
        logger.info("trade_processed", symbol=symbol, price=price, size=size, ts=ts)

    async def _handle_quote(self, symbol: str, evt: Dict[str, Any]) -> None:
        bid = _to_float(evt.get("bid_price") or evt.get("bp"))
        ask = _to_float(evt.get("ask_price") or evt.get("ap"))
        bid_size = _to_int(evt.get("bid_size") or evt.get("bs"))
        ask_size = _to_int(evt.get("ask_size") or evt.get("as"))
        ts = _to_int(evt.get("timestamp") or evt.get("t"))
        quote = {
            "symbol": symbol,
            "bid": bid,
            "bidSize": bid_size,
            "ask": ask,
            "askSize": ask_size,
            "ts": ts,
            "updatedAt": __import__("datetime").datetime.utcnow().isoformat() + "Z",
        }
        if self._quote_tracker:
            metrics = await self._quote_tracker.process_quote(symbol, bid=bid, ask=ask, ts=ts)
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

        await self._sink.set_quote(symbol, quote)
        if hasattr(self._sink, "write_raw_event"):
            try:
                await getattr(self._sink, "write_raw_event")("Q", symbol, evt)
            except Exception:
                pass
        logger.info("quote_processed", symbol=symbol, bid=bid, ask=ask, ts=ts)

    async def _handle_aggregate(self, symbol: str, evt: Dict[str, Any]) -> None:
        start = _to_int(evt.get("start_timestamp") or evt.get("s"))
        end = _to_int(evt.get("end_timestamp") or evt.get("e"))
        vwap_value = _to_float(evt.get("vwap") or evt.get("aggregate_vwap") or evt.get("vw"))
        bar = {
            "symbol": symbol,
            "start": start,
            "end": end,
            "open": _to_float(evt.get("open") or evt.get("o")),
            "high": _to_float(evt.get("high") or evt.get("h")),
            "low": _to_float(evt.get("low") or evt.get("l")),
            "close": _to_float(evt.get("close") or evt.get("c")),
            "volume": _to_int(evt.get("volume") or evt.get("v")),
            "vwap": vwap_value,
            "updatedAt": __import__("datetime").datetime.utcnow().isoformat() + "Z",
        }
        await self._sink.set_latest_agg1m(symbol, bar)
        if self._agg5m:
            await self._agg5m.on_minute_bar(symbol, bar)
        if hasattr(self._sink, "write_raw_event"):
            try:
                await getattr(self._sink, "write_raw_event")("AM", symbol, evt)
            except Exception:
                pass
        logger.info("aggregate_processed", symbol=symbol, close=bar["close"], ts=bar["end"])

    async def _handle_fmv(self, symbol: str, evt: Dict[str, Any]) -> None:
        price = _to_float(evt.get("fmv") or evt.get("price"))
        ts = _to_int(evt.get("timestamp") or evt.get("t"))
        fmv = {
            "symbol": symbol,
            "price": price,
            "ts": ts,
            "updatedAt": __import__("datetime").datetime.utcnow().isoformat() + "Z",
        }
        await self._sink.set_fmv(symbol, fmv)
        if hasattr(self._sink, "write_raw_event"):
            try:
                await getattr(self._sink, "write_raw_event")("FMV", symbol, evt)
            except Exception:
                pass
        logger.info("fmv_processed", symbol=symbol, price=price, ts=ts)

    def health(self) -> Dict[str, Any]:
        return {
            "is_running": self._is_running,
            "channels": self._channels,
            "subscriptions": [f"{c}.*" for c in self._channels],
        }
