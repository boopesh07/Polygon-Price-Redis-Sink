from __future__ import annotations

import asyncio
import json
from typing import Any, Dict, List, Optional

from massive import WebSocketClient
from massive.websocket.models import WebSocketMessage, Feed, Market

from .agg5m import Agg5mCollector
from .config import Settings, mask_api_key
from .logging_setup import get_logger
from .prev_close_recorder import PrevCloseRecorder
from .redis_sink import BaseSink
from .agg5m import Agg5mCollector
from .fmv_tracker import FmvTracker


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
        channels: Optional[List[str]] = None,
        host_override: Optional[str] = None,
        agg5m_collector: Optional[Agg5mCollector] = None,
        fmv_tracker: Optional[FmvTracker] = None,
    ):
        self._settings = settings
        self._sink = sink
        self._channels = [c.upper() for c in channels]
        self._agg5m = agg5m_collector
        self._prev_close_recorder = prev_close_recorder
        self._stop_event = asyncio.Event()
        self._is_authenticated: bool = False
        self._trade_counts: Dict[str, Dict[str, int]] = {}
        self._agg_counts: Dict[str, Dict[str, int]] = {}
        self._fmv_counts: Dict[str, Dict[str, int]] = {}
        self._quote_counts: Dict[str, Dict[str, int]] = {}
        self._debug = settings.ws_debug
        self._channels = [c.upper() for c in (channels or ["T", "AM"])]
        self._host_override = host_override
        self._agg5m = agg5m_collector
        self._fmv_tracker = fmv_tracker

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
        # If running split hosts, choose the URL based on the first requested channel
        primary = self._channels[0] if self._channels else "AM"
        url = self._host_override or self._settings.normalized_ws_url_for(primary)
        logger.info("ws_connecting", url=url, channels=self._channels, subscription_mode="wildcard")
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
                # Subscribe to all tickers using wildcard subscriptions
                for channel in self._channels:
                    wildcard_sub = f"{channel}.*"
                    logger.info("ws_subscribing", channel=channel, params=wildcard_sub)
                    await self._send_json_str(ws, {"action": "subscribe", "params": wildcard_sub})
            elif status == "error":
                logger.error("ws_status_error", message=evt.get("message"))
            return

    async def _process_event(self, evt: Dict[str, Any]) -> None:
        event_type = evt.get("event_type") or evt.get("ev")
        if not event_type:
            logger.debug("ws_event_missing_ev", raw_event=evt)
            return

        if event_type.lower() == "status":
            logger.info("ws_status_event", payload=evt)
            return

        # Quotes: Raw bid/ask data (NO P/L calculations - that's handled by FMV now)
        if evt.get("ev") == "Q" and evt.get("sym"):
            sym = str(evt["sym"]).upper()
            bid = float(evt.get("bp")) if isinstance(evt.get("bp"), (int, float)) else None
            ask = float(evt.get("ap")) if isinstance(evt.get("ap"), (int, float)) else None
            ts_raw = int(evt.get("t")) if isinstance(evt.get("t"), (int, float)) else None
            
            # Compute mid-price from bid/ask
            price = None
            if bid is not None and ask is not None:
                price = (bid + ask) / 2.0
            elif bid is not None:
                price = bid
            elif ask is not None:
                price = ask
            
            quote = {
                "symbol": sym,
                "bid": bid,
                "bidSize": int(evt.get("bs")) if isinstance(evt.get("bs"), (int, float)) else None,
                "ask": ask,
                "askSize": int(evt.get("as")) if isinstance(evt.get("as"), (int, float)) else None,
                "price": price,
                "ts": ts_raw,
                "updatedAt": __import__("datetime").datetime.utcnow().isoformat() + "Z",
            }
            
            if self._debug:
                logger.info("quote_processed", symbol=sym, bid=bid, ask=ask, price=price)
            
            await self._sink.set_quote(sym, quote)
            if hasattr(self._sink, "write_raw_event"):
                try:
                    await getattr(self._sink, "write_raw_event")("Q", sym, evt)
                except Exception:
                    pass
            c = self._quote_counts.get(sym) or {"count": 0}
            c["count"] += 1
            if isinstance(evt.get("t"), (int, float)):
                c["lastTs"] = int(evt["t"])  # type: ignore[index]
            self._quote_counts[sym] = c
            return
        symbol = symbol_raw.upper()

        # FMV: Fair Market Value with P/L calculations
        if evt.get("ev") == "FMV" and evt.get("sym") and isinstance(evt.get("fmv"), (int, float)):
            sym = str(evt["sym"]).upper()
            price = float(evt["fmv"])
            ts = int(evt.get("t")) if isinstance(evt.get("t"), (int, float)) else None
            
            # Process through FmvTracker to get P/L metrics
            if self._fmv_tracker:
                metrics = await self._fmv_tracker.process_fmv(sym, price=price, ts=ts)
                # metrics = {price, prevClose, dailyChange, dailyChangePct}
            else:
                # Fallback if no tracker configured
                metrics = {
                    "price": price,
                    "prevClose": None,
                    "dailyChange": None,
                    "dailyChangePct": None,
                }
            
            fmv = {
                "symbol": sym,
                "ts": ts,
                "updatedAt": __import__("datetime").datetime.utcnow().isoformat() + "Z",
                **metrics,  # Spread all P/L metrics
            }
            
            if self._debug:
                logger.info(
                    "fmv_processed",
                    symbol=sym,
                    price=price,
                    prevClose=metrics.get("prevClose"),
                    dailyChange=metrics.get("dailyChange"),
                    dailyChangePct=metrics.get("dailyChangePct"),
                )
            
            await self._sink.set_fmv(sym, fmv)
            if hasattr(self._sink, "write_raw_event"):
                try:
                    await getattr(self._sink, "write_raw_event")("FMV", sym, evt)
                except Exception:
                    pass
            c = self._fmv_counts.get(sym) or {"count": 0}
            c["count"] += 1
            if isinstance(evt.get("t"), (int, float)):
                c["lastTs"] = int(evt["t"])  # type: ignore[index]
            self._fmv_counts[sym] = c
            return
        try:
            await writer(channel, symbol, evt)
        except Exception as exc:  # noqa: BLE001
            logger.warning("raw_event_write_failed", channel=channel, symbol=symbol, error=str(exc))

    def health(self) -> Dict[str, Any]:
        return {
            "authenticated": self._is_authenticated,
            "channels": self._channels,
            "subscription_mode": "wildcard",
            "unique_trade_symbols": len(self._trade_counts),
            "unique_agg_symbols": len(self._agg_counts),
            "unique_fmv_symbols": len(self._fmv_counts),
            "unique_quote_symbols": len(self._quote_counts),
            "total_trade_events": sum(c.get("count", 0) for c in self._trade_counts.values()),
            "total_agg_events": sum(c.get("count", 0) for c in self._agg_counts.values()),
            "total_fmv_events": sum(c.get("count", 0) for c in self._fmv_counts.values()),
            "total_quote_events": sum(c.get("count", 0) for c in self._quote_counts.values()),
        }
