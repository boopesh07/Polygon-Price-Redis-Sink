from __future__ import annotations

import asyncio
import contextlib
import os
import signal
from dotenv import load_dotenv
import json
import socket

import httpx
from redis.asyncio import Redis

from .config import Settings, mask_api_key
from .logging_setup import configure_logging, get_logger
from .redis_sink import build_sink
from .ws_client import PolygonWsClient
from .tickers import fetch_all_active_stock_tickers


async def _health_loop(client: PolygonWsClient, interval_sec: int) -> None:
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
    # Discover all active stock tickers
    if settings.polygon_discover_tickers:
        all_symbols = await fetch_all_active_stock_tickers(settings.polygon_api_key)
    else:
        all_symbols = settings.symbols
    if settings.polygon_ticker_limit and settings.polygon_ticker_limit > 0:
        all_symbols = all_symbols[: settings.polygon_ticker_limit]
    logger.info("discovered_symbols", count=len(all_symbols))
    logger.info(
        "starting_polygon_sink",
        realtime_host=settings.polygon_ws_host_realtime,
        delayed_host=settings.polygon_ws_host_delayed,
        symbols=all_symbols[:20],
    )

    sink = build_sink(settings)
    ok = await sink.ping()
    logger.info("redis_health", ok=ok)
    # Startup write/read validation; exit if it fails
    ok_rw = await _validate_redis_rw(settings)
    if not ok_rw:
        raise RuntimeError("startup_redis_validation_failed")

    # Build one or two clients depending on env overrides
    # One client per host. Real-time host handles AM+FMV. Delayed host handles T+Q.
    clients = []
    # Real-time (business) host: AM + FMV
    rt_url = settings.normalized_ws_url_for("AM")
    clients.append(PolygonWsClient(settings, sink, channels=["AM", "FMV"], host_override=rt_url, symbols_override=all_symbols))

    # Delayed (delayed-business) host: T + Q
    delayed_url = settings.normalized_ws_url_for("T")
    clients.append(PolygonWsClient(settings, sink, channels=["T", "Q"], host_override=delayed_url, symbols_override=all_symbols))

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
    await sink.close()
    logger.info("stopped_polygon_sink")


def main() -> None:
    try:
        import uvloop  # type: ignore
        uvloop.install()
    except Exception:
        pass
    asyncio.run(_main_async())


if __name__ == "__main__":
    main()


