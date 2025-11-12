from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

try:
    from upstash_redis.asyncio import Redis as UpstashRedis
except ImportError:  # pragma: no cover - optional for unit tests without dependency installed
    UpstashRedis = None  # type: ignore[assignment]

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


class UpstashRedisSink(BaseSink):
    def __init__(self, url: str, token: str, debug: bool, s3_raw: Optional[S3RawMultiWriter] = None) -> None:
        if UpstashRedis is None:
            raise RuntimeError("upstash-redis package is required. Install it via 'pip install upstash-redis'.")
        self._client = UpstashRedis(url=url, token=token, allow_telemetry=False)
        self._debug = debug
        self._s3_raw = s3_raw

    async def set_latest_price(self, symbol: str, payload: Dict[str, Any]) -> None:
        await self._set_json("set_latest_price", _latest_price_key(symbol), payload)

    async def set_latest_agg1m(self, symbol: str, payload: Dict[str, Any]) -> None:
        await self._set_json("set_latest_agg1m", _agg1m_key(symbol), payload)

    async def set_trade(self, symbol: str, payload: Dict[str, Any]) -> None:
        await self._set_json("set_trade", _trade_key(symbol), payload)

    async def set_quote(self, symbol: str, payload: Dict[str, Any]) -> None:
        await self._set_json("set_quote", _quote_key(symbol), payload)

    async def set_fmv(self, symbol: str, payload: Dict[str, Any]) -> None:
        await self._set_json("set_fmv", _fmv_key(symbol), payload)

    async def set_agg5m_snapshot(self, symbol: str, day: str, entries: List[Dict[str, Any]], ttl: Optional[int] = None) -> None:
        value = {"day": day, "bars": entries}
        await self._set_json("set_agg5m_snapshot", f"stock:agg5m:{symbol}", value, ttl=ttl)

    async def set_prev_close(self, symbol: str, price: float, ttl: Optional[int] = None) -> None:
        await self._set_value("set_prev_close", _prev_close_key(symbol), f"{float(price):.6f}", ttl=ttl)

    async def get_prev_close(self, symbol: str) -> Optional[float]:
        key = _prev_close_key(symbol)
        try:
            result = await self._client.get(key)
        except Exception as exc:  # noqa: BLE001
            logger.exception("redis_command_failed", action="get_prev_close", key=key, error=str(exc))
            raise
        if result is None:
            return None
        try:
            return float(result)
        except (TypeError, ValueError) as exc:  # pragma: no cover
            logger.warning("redis_prev_close_parse_error", key=key, raw_value=result, error=str(exc))
            return None

    async def ping(self) -> bool:
        try:
            pong = await self._client.ping()
            return bool(pong)
        except Exception as exc:  # noqa: BLE001
            logger.exception("redis_ping_failed", error=str(exc))
            raise

    async def close(self) -> None:
        try:
            await self._client.close()
        except Exception:
            pass
        if self._s3_raw:
            try:
                await self._s3_raw.close()
            except Exception:
                pass

    async def write_raw_event(self, channel: str, symbol: str, raw_event: Dict[str, Any]) -> None:
        if not self._s3_raw:
            return
        try:
            obj = {
                "channel": channel.upper(),
                "symbol": symbol.upper(),
                "ingestTs": __import__("datetime").datetime.utcnow().isoformat() + "Z",
            }
            obj.update(raw_event)
            await self._s3_raw.write(channel.upper(), json.dumps(obj))
        except Exception as exc:  # noqa: BLE001
            logger.error("s3_raw_write_error", error=str(exc))

    async def _set_json(self, action: str, key: str, payload: Dict[str, Any], ttl: Optional[int] = None) -> None:
        await self._set_value(action, key, json.dumps(payload), ttl=ttl)

    async def _set_value(self, action: str, key: str, value: str, ttl: Optional[int] = None) -> None:
        if self._debug:
            logger.info("redis_write", key=key, size=len(value), preview=value[:200], ttl=ttl)
        try:
            if ttl and ttl > 0:
                await self._client.set(key, value, ex=ttl)
            else:
                await self._client.set(key, value)
        except Exception as exc:  # noqa: BLE001
            logger.exception("redis_write_failed", action=action, key=key, error=str(exc))
            raise


def build_sink(settings: Settings) -> BaseSink:
    if not settings.redis_url or not settings.redis_url.lower().startswith("http"):
        raise ValueError("REDIS_URL must be an Upstash REST URL (https://...)")
    if not settings.redis_token:
        raise ValueError("REDIS_TOKEN is required for Upstash Redis")

    s3_raw: Optional[S3RawMultiWriter] = None
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

    logger.info("redis_sink_selected", kind="upstash")
    return UpstashRedisSink(settings.redis_url, settings.redis_token, settings.ws_debug, s3_raw=s3_raw)
