from __future__ import annotations

from typing import List
from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", case_sensitive=False, extra="ignore")

    polygon_api_key: str = Field(..., alias="POLYGON_API_KEY")
    # Feed code is auto-selected based on channel type (Feed.Business for AM/FMV, Feed.DelayedBusiness for T/Q)
    polygon_ws_host_realtime: str | None = Field(None, alias="POLYGON_WS_HOST_REALTIME")  # Deprecated: SDK handles internally
    polygon_ws_host_delayed: str | None = Field(None, alias="POLYGON_WS_HOST_DELAYED")  # Deprecated: SDK handles internally
    polygon_ws_symbols: str = Field("", alias="POLYGON_WS_SYMBOLS")  # Deprecated: using wildcards now

    redis_url: str | None = Field(None, alias="REDIS_URL")
    redis_token: str | None = Field(None, alias="REDIS_TOKEN")

    ws_debug: bool = Field(False, alias="WS_DEBUG")
    ws_health_interval_sec: int = Field(30, alias="WS_HEALTH_INTERVAL_SEC")

    backoff_initial_ms: int = Field(1000, alias="BACKOFF_INITIAL_MS")
    backoff_factor: float = Field(2.0, alias="BACKOFF_FACTOR")
    backoff_max_ms: int = Field(30000, alias="BACKOFF_MAX_MS")

    # Ticker discovery - DEPRECATED: Using wildcard subscriptions now
    # Keeping for backward compatibility but not used
    polygon_discover_tickers: bool = Field(False, alias="POLYGON_DISCOVER_TICKERS")
    polygon_ticker_limit: int = Field(0, alias="POLYGON_TICKER_LIMIT")
    subscribe_batch_size: int = Field(500, alias="POLYGON_SUBSCRIBE_BATCH")

    # S3 cold-path configuration
    s3_enabled: bool = Field(False, alias="S3_ENABLED")
    s3_bucket: str | None = Field(None, alias="S3_BUCKET")
    s3_prefix: str | None = Field(None, alias="S3_PREFIX")
    aws_region: str | None = Field(None, alias="AWS_REGION")
    s3_window_minutes: int = Field(30, alias="S3_WINDOW_MINUTES")
    s3_max_object_bytes: int = Field(512_000_000, alias="S3_MAX_OBJECT_BYTES")
    s3_part_size_bytes: int = Field(33_554_432, alias="S3_PART_SIZE_BYTES")
    s3_use_marker: bool = Field(True, alias="S3_USE_MARKER")

    # 5-minute aggregate settings
    agg5m_flush_interval_sec: int = Field(900, alias="AGG5M_FLUSH_INTERVAL_SEC")
    agg5m_ttl_sec: int = Field(172_800, alias="AGG5M_TTL_SEC")
    agg5m_timezone: str = Field("America/New_York", alias="AGG5M_TIMEZONE")
    agg5m_max_bars: int = Field(120, alias="AGG5M_MAX_BARS")

    # Quote daily P/L settings
    quote_pl_timezone: str = Field("America/New_York", alias="QUOTE_PL_TIMEZONE")
    quote_pl_market_close_hour: int = Field(16, alias="QUOTE_PL_MARKET_CLOSE_HOUR")
    quote_pl_market_close_minute: int = Field(0, alias="QUOTE_PL_MARKET_CLOSE_MINUTE")
    quote_prev_close_ttl_sec: int = Field(604_800, alias="QUOTE_PREV_CLOSE_TTL_SEC")

    @property
    def symbols(self) -> List[str]:
        raw = self.polygon_ws_symbols or ""
        return [s.strip().upper() for s in raw.split(",") if s.strip()]

    def normalized_ws_url_for(self, channel: str) -> str:
        # AM and FMV -> realtime host; T and Q -> delayed host
        if channel.upper() in ("AM", "FMV"):
            base = self.polygon_ws_host_realtime
        else:
            base = self.polygon_ws_host_delayed
        host = (base or "").rstrip("/")
        if host.lower().endswith("/stocks"):
            host = host[: -len("/stocks")]
        return f"{host}/stocks"

    @model_validator(mode="after")
    def _validate_hosts(self):
        # Hosts are optional now - Massive SDK handles connection internally
        if self.agg5m_flush_interval_sec <= 0:
            raise ValueError("AGG5M_FLUSH_INTERVAL_SEC must be > 0")
        if self.agg5m_max_bars <= 0:
            raise ValueError("AGG5M_MAX_BARS must be > 0")
        if not 0 <= self.quote_pl_market_close_hour <= 23:
            raise ValueError("QUOTE_PL_MARKET_CLOSE_HOUR must be between 0 and 23")
        if not 0 <= self.quote_pl_market_close_minute <= 59:
            raise ValueError("QUOTE_PL_MARKET_CLOSE_MINUTE must be between 0 and 59")
        if self.quote_prev_close_ttl_sec <= 0:
            raise ValueError("QUOTE_PREV_CLOSE_TTL_SEC must be > 0")
        return self


def mask_api_key(key: str) -> str:
    if not key:
        return ""
    if len(key) <= 6:
        return "***"
    return f"{key[:3]}***{key[-3:]}"
