from __future__ import annotations

import asyncio
import json
from typing import Any, Dict, Optional, List

import httpx
from redis.asyncio import Redis

from .config import Settings
from .logging_setup import get_logger
from .s3_writer import S3RawMultiWriter


logger = get_logger()


def _latest_price_key(symbol: str) -> str:
    return f"stock:latest:{symbol}"


def _agg1m_key(symbol: str) -> str:
    return f"stock:agg1m:{symbol}"


def _trade_key(symbol: str) -> str:
    return f"stock:trade:{symbol}"


def _quote_key(symbol: str) -> str:
    return f"stock:quote:{symbol}"


def _fmv_key(symbol: str) -> str:
    return f"stock:fmv:{symbol}"


def _prev_close_key(symbol: str) -> str:
    return f"stock:prev_close:{symbol}"


class BaseSink:
    async def set_latest_price(self, symbol: str, payload: Dict[str, Any]) -> None:  # pragma: no cover - interface
        raise NotImplementedError

    async def set_latest_agg1m(self, symbol: str, payload: Dict[str, Any]) -> None:  # pragma: no cover - interface
        raise NotImplementedError

    async def set_trade(self, symbol: str, payload: Dict[str, Any]) -> None:  # pragma: no cover - interface
        raise NotImplementedError

    async def set_quote(self, symbol: str, payload: Dict[str, Any]) -> None:  # pragma: no cover - interface
        raise NotImplementedError

    async def set_fmv(self, symbol: str, payload: Dict[str, Any]) -> None:  # pragma: no cover - interface
        raise NotImplementedError

    async def set_prev_close(self, symbol: str, price: float, ttl: Optional[int] = None) -> None:  # pragma: no cover - interface
        raise NotImplementedError

    async def get_prev_close(self, symbol: str) -> Optional[float]:  # pragma: no cover - interface
        raise NotImplementedError

    async def set_agg5m_snapshot(self, symbol: str, day: str, entries: List[Dict[str, Any]], ttl: Optional[int] = None) -> None:  # pragma: no cover - interface
        raise NotImplementedError

    async def ping(self) -> bool:  # pragma: no cover - interface
        raise NotImplementedError

    async def close(self) -> None:  # pragma: no cover - interface
        pass


class MultiSink(BaseSink):
    def __init__(self, sinks: List[BaseSink], s3_raw: Optional[S3RawMultiWriter] = None):
        self._sinks = list(sinks)
        self._s3_raw = s3_raw

    async def set_latest_price(self, symbol: str, payload: Dict[str, Any]) -> None:
        for s in self._sinks:
            try:
                await s.set_latest_price(symbol, payload)
            except Exception as exc:  # noqa: BLE001
                logger.error("sink_error", action="set_latest_price", error=str(exc))

    async def set_latest_agg1m(self, symbol: str, payload: Dict[str, Any]) -> None:
        for s in self._sinks:
            try:
                await s.set_latest_agg1m(symbol, payload)
            except Exception as exc:  # noqa: BLE001
                logger.error("sink_error", action="set_latest_agg1m", error=str(exc))

    async def set_trade(self, symbol: str, payload: Dict[str, Any]) -> None:
        for s in self._sinks:
            try:
                await s.set_trade(symbol, payload)
            except Exception as exc:  # noqa: BLE001
                logger.error("sink_error", action="set_trade", error=str(exc))

    async def set_quote(self, symbol: str, payload: Dict[str, Any]) -> None:
        for s in self._sinks:
            try:
                await s.set_quote(symbol, payload)
            except Exception as exc:  # noqa: BLE001
                logger.error("sink_error", action="set_quote", error=str(exc))

    async def set_fmv(self, symbol: str, payload: Dict[str, Any]) -> None:
        for s in self._sinks:
            try:
                await s.set_fmv(symbol, payload)
            except Exception as exc:  # noqa: BLE001
                logger.error("sink_error", action="set_fmv", error=str(exc))

    async def set_agg5m_snapshot(self, symbol: str, day: str, entries: List[Dict[str, Any]], ttl: Optional[int] = None) -> None:
        for s in self._sinks:
            try:
                await s.set_agg5m_snapshot(symbol, day, entries, ttl)
            except Exception as exc:  # noqa: BLE001
                logger.error("sink_error", action="set_agg5m_snapshot", error=str(exc))

    async def set_prev_close(self, symbol: str, price: float, ttl: Optional[int] = None) -> None:
        for s in self._sinks:
            try:
                await s.set_prev_close(symbol, price, ttl)
            except Exception as exc:  # noqa: BLE001
                logger.error("sink_error", action="set_prev_close", error=str(exc))

    async def get_prev_close(self, symbol: str) -> Optional[float]:
        for s in self._sinks:
            try:
                val = await s.get_prev_close(symbol)
                if val is not None:
                    return val
            except Exception as exc:  # noqa: BLE001
                logger.error("sink_error", action="get_prev_close", error=str(exc))
        return None

    async def write_raw_event(self, channel: str, symbol: str, raw_event: Dict[str, Any]) -> None:
        if not self._s3_raw:
            return
        try:
            # Defensive: minimal envelope; preserve raw_event fields
            obj: Dict[str, Any] = {"channel": channel.upper(), "symbol": symbol.upper(), "ingestTs": __import__("datetime").datetime.utcnow().isoformat() + "Z"}
            obj.update(raw_event)
            line = __import__("json").dumps(obj)
            await self._s3_raw.write(channel.upper(), line)
        except Exception as exc:  # noqa: BLE001
            logger.error("s3_raw_write_error", error=str(exc))

    async def ping(self) -> bool:
        ok_any = False
        for s in self._sinks:
            try:
                ok_any = await s.ping() or ok_any
            except Exception:
                pass
        return ok_any

    async def close(self) -> None:
        for s in self._sinks:
            try:
                await s.close()
            except Exception:
                pass
        if self._s3_raw:
            try:
                await self._s3_raw.close()
            except Exception:
                pass


class StandardRedisSink(BaseSink):
    def __init__(self, url: str, debug: bool):
        self._client: Redis = Redis.from_url(url, encoding="utf-8", decode_responses=True)
        self._debug = debug

    async def set_latest_price(self, symbol: str, payload: Dict[str, Any]) -> None:
        key = _latest_price_key(symbol)
        value = json.dumps(payload)
        if self._debug:
            logger.info("redis_write", key=key, size=len(value), preview=value[:200])
        await self._client.set(key, value)

    async def set_latest_agg1m(self, symbol: str, payload: Dict[str, Any]) -> None:
        key = _agg1m_key(symbol)
        value = json.dumps(payload)
        if self._debug:
            logger.info("redis_write", key=key, size=len(value), preview=value[:200])
        await self._client.set(key, value)

    async def set_trade(self, symbol: str, payload: Dict[str, Any]) -> None:
        key = _trade_key(symbol)
        value = json.dumps(payload)
        if self._debug:
            logger.info("redis_write", key=key, size=len(value), preview=value[:200])
        await self._client.set(key, value)

    async def set_quote(self, symbol: str, payload: Dict[str, Any]) -> None:
        key = _quote_key(symbol)
        value = json.dumps(payload)
        if self._debug:
            logger.info("redis_write", key=key, size=len(value), preview=value[:200])
        await self._client.set(key, value)

    async def set_fmv(self, symbol: str, payload: Dict[str, Any]) -> None:
        key = _fmv_key(symbol)
        value = json.dumps(payload)
        if self._debug:
            logger.info("redis_write", key=key, size=len(value), preview=value[:200])
        await self._client.set(key, value)

    async def set_agg5m_snapshot(self, symbol: str, day: str, entries: List[Dict[str, Any]], ttl: Optional[int] = None) -> None:
        key = f"stock:agg5m:{symbol}"
        value = json.dumps({"day": day, "bars": entries})
        if self._debug:
            logger.info("redis_write", key=key, size=len(value), preview=value[:200])
        await self._client.set(key, value, ex=ttl)

    async def set_prev_close(self, symbol: str, price: float, ttl: Optional[int] = None) -> None:
        key = _prev_close_key(symbol)
        value = f"{float(price):.6f}"
        if self._debug:
            logger.info("redis_write", key=key, value=value, ttl=ttl)
        await self._client.set(key, value, ex=ttl)

    async def get_prev_close(self, symbol: str) -> Optional[float]:
        key = _prev_close_key(symbol)
        try:
            value = await self._client.get(key)
        except Exception as exc:  # noqa: BLE001
            logger.error("redis_get_prev_close_error", key=key, error=str(exc))
            return None
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    async def ping(self) -> bool:
        try:
            pong = await self._client.ping()
            return bool(pong)
        except Exception as exc:  # noqa: BLE001
            logger.error("redis_ping_error", error=str(exc))
            return False

    async def close(self) -> None:
        try:
            await self._client.close()
        except Exception:
            pass


class UpstashRestSink(BaseSink):
    def __init__(self, url: str, token: str, debug: bool):
        self._url = url.rstrip("/")
        self._token = token
        self._client = httpx.AsyncClient(timeout=httpx.Timeout(10.0, read=15.0))
        self._debug = debug

    async def _command(self, *args: str) -> httpx.Response:
        headers = {
            "Authorization": f"Bearer {self._token}",
            "Content-Type": "application/json",
        }
        body = list(args)
        return await self._client.post(self._url, headers=headers, json=body)

    async def set_latest_price(self, symbol: str, payload: Dict[str, Any]) -> None:
        key = _latest_price_key(symbol)
        value = json.dumps(payload)
        if self._debug:
            logger.info("redis_write", key=key, size=len(value), preview=value[:200])
        await self._command("SET", key, value)

    async def set_latest_agg1m(self, symbol: str, payload: Dict[str, Any]) -> None:
        key = _agg1m_key(symbol)
        value = json.dumps(payload)
        if self._debug:
            logger.info("redis_write", key=key, size=len(value), preview=value[:200])
        await self._command("SET", key, value)

    async def set_trade(self, symbol: str, payload: Dict[str, Any]) -> None:
        key = _trade_key(symbol)
        value = json.dumps(payload)
        if self._debug:
            logger.info("redis_write", key=key, size=len(value), preview=value[:200])
        await self._command("SET", key, value)

    async def set_quote(self, symbol: str, payload: Dict[str, Any]) -> None:
        key = _quote_key(symbol)
        value = json.dumps(payload)
        if self._debug:
            logger.info("redis_write", key=key, size=len(value), preview=value[:200])
        await self._command("SET", key, value)

    async def set_fmv(self, symbol: str, payload: Dict[str, Any]) -> None:
        key = _fmv_key(symbol)
        value = json.dumps(payload)
        if self._debug:
            logger.info("redis_write", key=key, size=len(value), preview=value[:200])
        await self._command("SET", key, value)

    async def set_agg5m_snapshot(self, symbol: str, day: str, entries: List[Dict[str, Any]], ttl: Optional[int] = None) -> None:
        key = f"stock:agg5m:{symbol}"
        value = json.dumps({"day": day, "bars": entries})
        if self._debug:
            logger.info("redis_write", key=key, size=len(value), preview=value[:200])
        if ttl and ttl > 0:
            await self._command("SET", key, value, "EX", str(ttl))
        else:
            await self._command("SET", key, value)

    async def set_prev_close(self, symbol: str, price: float, ttl: Optional[int] = None) -> None:
        key = _prev_close_key(symbol)
        value = f"{float(price):.6f}"
        if self._debug:
            logger.info("redis_write", key=key, value=value, ttl=ttl)
        if ttl and ttl > 0:
            await self._command("SET", key, value, "EX", str(ttl))
        else:
            await self._command("SET", key, value)

    async def get_prev_close(self, symbol: str) -> Optional[float]:
        key = _prev_close_key(symbol)
        try:
            resp = await self._command("GET", key)
            data = resp.json()
            result = data.get("result")
            if result is None:
                return None
            return float(result)
        except Exception as exc:  # noqa: BLE001
            logger.error("upstash_prev_close_error", key=key, error=str(exc))
            return None

    async def ping(self) -> bool:
        try:
            resp = await self._command("PING")
            return resp.is_success
        except Exception as exc:  # noqa: BLE001
            logger.error("upstash_ping_error", error=str(exc))
            return False

    async def close(self) -> None:
        try:
            await self._client.aclose()
        except Exception:
            pass


def build_sink(settings: Settings) -> BaseSink:
    # Build the primary Redis sink
    primary: BaseSink
    if settings.redis_url and settings.redis_url.lower().startswith("http"):
        if not settings.redis_token:
            raise ValueError("REDIS_TOKEN is required when REDIS_URL is an Upstash REST URL")
        logger.info("redis_sink_selected", kind="upstash_rest")
        primary = UpstashRestSink(settings.redis_url, settings.redis_token, settings.ws_debug)
    elif not settings.redis_url:
        raise ValueError("REDIS_URL is required")
    else:
        logger.info("redis_sink_selected", kind="standard_redis")
        primary = StandardRedisSink(settings.redis_url, settings.ws_debug)

    # Optionally build the S3 raw writer and return a MultiSink
    if settings.s3_enabled:
        if not settings.s3_bucket or not settings.s3_prefix:
            raise ValueError("S3_BUCKET and S3_PREFIX are required when S3_ENABLED=true")
        s3_raw = S3RawMultiWriter(
            bucket=settings.s3_bucket,
            base_prefix=settings.s3_prefix,
            aws_region_name=settings.aws_region,
            window_minutes=settings.s3_window_minutes,
            max_object_bytes=settings.s3_max_object_bytes,
            part_size_bytes=settings.s3_part_size_bytes,
            use_marker=settings.s3_use_marker,
        )
        logger.info("s3_sink_enabled", bucket=settings.s3_bucket, prefix=settings.s3_prefix)
        return MultiSink([primary], s3_raw=s3_raw)

    return primary

