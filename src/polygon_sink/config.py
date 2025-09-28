from __future__ import annotations

from typing import List
from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", case_sensitive=False, extra="ignore")

    polygon_api_key: str = Field(..., alias="POLYGON_API_KEY")
    polygon_ws_host_realtime: str = Field(..., alias="POLYGON_WS_HOST_REALTIME")
    polygon_ws_host_delayed: str = Field(..., alias="POLYGON_WS_HOST_DELAYED")
    polygon_ws_symbols: str = Field("AAPL,MSFT,TSLA", alias="POLYGON_WS_SYMBOLS")

    redis_url: str | None = Field(None, alias="REDIS_URL")
    redis_token: str | None = Field(None, alias="REDIS_TOKEN")

    ws_debug: bool = Field(False, alias="WS_DEBUG")
    ws_health_interval_sec: int = Field(30, alias="WS_HEALTH_INTERVAL_SEC")

    backoff_initial_ms: int = Field(1000, alias="BACKOFF_INITIAL_MS")
    backoff_factor: float = Field(2.0, alias="BACKOFF_FACTOR")
    backoff_max_ms: int = Field(30000, alias="BACKOFF_MAX_MS")

    # Ticker discovery and subscription behavior
    polygon_discover_tickers: bool = Field(True, alias="POLYGON_DISCOVER_TICKERS")
    polygon_ticker_limit: int = Field(0, alias="POLYGON_TICKER_LIMIT")  # 0 means unlimited
    subscribe_batch_size: int = Field(500, alias="POLYGON_SUBSCRIBE_BATCH")

    # S3 cold-path configuration
    s3_enabled: bool = Field(False, alias="S3_ENABLED")
    s3_bucket: str | None = Field(None, alias="S3_BUCKET")
    s3_prefix: str | None = Field(None, alias="S3_PREFIX")
    aws_region: str | None = Field(None, alias="AWS_REGION")
    s3_window_minutes: int = Field(30, alias="S3_WINDOW_MINUTES")
    s3_max_object_bytes: int = Field(512_000_000, alias="S3_MAX_OBJECT_BYTES")
    s3_part_size_bytes: int = Field(16_777_216, alias="S3_PART_SIZE_BYTES")
    s3_use_marker: bool = Field(True, alias="S3_USE_MARKER")

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
        if not self.polygon_ws_host_realtime or not self.polygon_ws_host_delayed:
            raise ValueError("POLYGON_WS_HOST_REALTIME and POLYGON_WS_HOST_DELAYED are required")
        return self


def mask_api_key(key: str) -> str:
    if not key:
        return ""
    if len(key) <= 6:
        return "***"
    return f"{key[:3]}***{key[-3:]}"


