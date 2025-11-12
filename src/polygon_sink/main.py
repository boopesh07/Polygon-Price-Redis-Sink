from __future__ import annotations

import asyncio
import contextlib
import os
import signal
from dotenv import load_dotenv
import json
import socket

import httpx

from .config import Settings, mask_api_key
from .logging_setup import configure_logging, get_logger
from .redis_sink import build_sink
from .agg5m import Agg5mCollector
from .fmv_tracker import FmvTracker
from .ws_client import PolygonWsClient


async def _health_loop(client: MassiveWsClient, interval_sec: int) -> None:
    logger = get_logger()
    while True:
        await asyncio.sleep(interval_sec)
        logger.info("health", **client.health())


async def _validate_redis_rw(settings: Settings) -> bool:
    logger = get_logger()
    key = f"stock:startup:{socket.gethostname()}"
    value = json.dumps({"hello": "world"})
    ttl = 60
    url = settings.redis_url or ""
    try:
        if url.lower().startswith("http"):
            if not settings.redis_token:
                logger.error("startup_redis_validate_missing_token")
                return False
            headers = {
                "Authorization": f"Bearer {settings.redis_token}",
                "Content-Type": "application/json",
            }
            async with httpx.AsyncClient(timeout=httpx.Timeout(10.0, read=15.0)) as client:
                set_body = ["SETEX", key, str(ttl), value]
                set_resp = await client.post(url, headers=headers, json=set_body)
                if not set_resp.is_success:
                    logger.error("startup_redis_validate_set_failed", status=set_resp.status_code)
                    return False
                get_body = ["GET", key]
                get_resp = await client.post(url, headers=headers, json=get_body)
                if not get_resp.is_success:
                    logger.error("startup_redis_validate_get_failed", status=get_resp.status_code)
                    return False
                data = get_resp.json()
                ok = data.get("result") == value
                logger.info("startup_redis_validate_result", ok=ok)
                return bool(ok)
        else:
            client = Redis.from_url(url, encoding="utf-8", decode_responses=True)
            try:
                await client.setex(key, ttl, value)
                res = await client.get(key)
                ok = res == value
                logger.info("startup_redis_validate_result", ok=ok)
                return bool(ok)
            finally:
                try:
                    await client.close()
                except Exception:
                    pass
    except Exception as exc:  # noqa: BLE001
        logger.error("startup_redis_validate_exception", error=str(exc))
        return False


async def _main_async() -> None:
    load_dotenv()
    settings = Settings()
    configure_logging(debug=settings.ws_debug)
    logger = get_logger()
    logger.info(
        "starting_polygon_sink",
        realtime_host=settings.polygon_ws_host_realtime,
        delayed_host=settings.polygon_ws_host_delayed,
        subscription_mode="wildcard",
    )

    sink = build_sink(settings)
    agg5m_collector = Agg5mCollector(
        sink,
        flush_interval_sec=settings.agg5m_flush_interval_sec,
        ttl_sec=settings.agg5m_ttl_sec,
        timezone_name=settings.agg5m_timezone,
        max_bars=settings.agg5m_max_bars,
    )
    agg5m_collector.start()
    fmv_tracker = FmvTracker(
        sink,
        timezone_name=settings.quote_pl_timezone,
        close_hour=settings.quote_pl_market_close_hour,
        close_minute=settings.quote_pl_market_close_minute,
        prev_close_ttl=settings.quote_prev_close_ttl_sec,
    )
    ok = await sink.ping()
    logger.info("redis_health", ok=ok)
    # Startup write/read validation; exit if it fails
    ok_rw = await _validate_redis_rw(settings)
    if not ok_rw:
        raise RuntimeError("startup_redis_validation_failed")

    # Build two clients: one for real-time channels (AM+FMV), one for delayed (T+Q)
    # Both use wildcard subscriptions (e.g., "AM.*", "T.*") to receive all tickers
    clients = []
    # Real-time (business) host: AM + FMV (with P/L tracking)
    rt_url = settings.normalized_ws_url_for("AM")
    clients.append(
        MassiveWsClient(
            settings,
            sink,
            channels=["AM", "FMV"],
            host_override=rt_url,
            agg5m_collector=agg5m_collector,
            fmv_tracker=fmv_tracker,
        )
    )

    # Delayed (delayed-business) host: T + Q (raw data only, no trackers)
    delayed_url = settings.normalized_ws_url_for("T")
    clients.append(
        MassiveWsClient(
            settings,
            sink,
            channels=["T", "Q"],
            host_override=delayed_url,
            agg5m_collector=agg5m_collector,
            fmv_tracker=None,
        )
    )
    
    logger.info("massive_clients_created", client_count=len(clients))

    stop_event = asyncio.Event()

    def _handle_sig(*_: int) -> None:
        logger.info("signal_received", signal="SIGTERM/SIGINT")
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, _handle_sig)
        except NotImplementedError:
            pass

    ws_tasks = [asyncio.create_task(c.run_forever()) for c in clients]
    health_task = asyncio.create_task(_health_loop(clients[0], settings.ws_health_interval_sec))

    await stop_event.wait()
    for c in clients:
        await c.stop()
    for t in [*ws_tasks, health_task]:
        t.cancel()
        with contextlib.suppress(Exception):
            await t
    await agg5m_collector.close()
    await sink.close()
    logger.info("stopped_massive_sink")


def main() -> None:
    try:
        import uvloop  # type: ignore
        uvloop.install()
    except Exception:
        pass
    asyncio.run(_main_async())


if __name__ == "__main__":
    main()
