Polygon WebSocket → Redis Sink (Python)

Overview

This service (polygon-sink) maintains exactly two WebSocket connections to Polygon stocks (one per host) and sinks AM, FMV, T, and Q to Redis with strict key schemas:

- stock:agg1m:{SYMBOL}
- stock:fmv:{SYMBOL}
- stock:trade:{SYMBOL}
- stock:quote:{SYMBOL}

It is designed to run as a single replica and is suitable for AWS ECS Fargate. One WS client connects to the real-time host (AM+FMV). A second WS client connects to the delayed host (T+Q).

References

- Polygon WebSocket quickstart: `https://polygon.io/docs/websocket/quickstart`
- Polygon Python client repo (reference only): `https://github.com/polygon-io/client-python`

Environment Variables (required)

- POLYGON_API_KEY: Polygon API key
- POLYGON_WS_SYMBOLS: Comma-separated symbols (e.g., AAPL,MSFT,TSLA)
- POLYGON_WS_HOST_REALTIME: Real-time host (e.g., wss://business.polygon.io). Do NOT include /stocks; the service appends it.
- POLYGON_WS_HOST_DELAYED: Delayed host (e.g., wss://delayed-business.polygon.io). Do NOT include /stocks.
- REDIS_URL: Standard Redis URL (redis://host:port/db) OR Upstash REST URL (https://...)
- REDIS_TOKEN: Required if using Upstash REST URL

Optional

- WS_DEBUG: true|false for verbose logs (default false)
- WS_HEALTH_INTERVAL_SEC: Health log cadence (default 30)
- AGG1M_TTL_SEC: TTL for stock:agg1m (default 120)
- FMV_TTL_SEC: TTL for stock:fmv (default 60)
- TRADE_TTL_SEC: TTL for stock:trade (default 60)
- QUOTE_TTL_SEC: TTL for stock:quote (default 60)
- POLYGON_DISCOVER_TICKERS: true|false to discover tickers from Polygon API (default true)
- POLYGON_TICKER_LIMIT: Limit the number of discovered tickers (default 0 for unlimited)
- POLYGON_SUBSCRIBE_BATCH: Batch size for subscribing to tickers (default 500)
- BACKOFF_INITIAL_MS: Initial backoff for WebSocket reconnect (default 1000)
- BACKOFF_FACTOR: Backoff factor for WebSocket reconnect (default 2.0)
- BACKOFF_MAX_MS: Maximum backoff for WebSocket reconnect (default 30000)
- AGG5M_FLUSH_INTERVAL_SEC: Flush cadence for 5-minute snapshots (default 900)
- AGG5M_TTL_SEC: TTL for stock:agg5m keys (default 172800)
- AGG5M_TIMEZONE: Timezone for day boundaries (default America/New_York)
- AGG5M_MAX_BARS: Maximum 5-minute buckets stored per day (default 120)
- QUOTE_PL_TIMEZONE: Timezone for daily P/L calculations (default America/New_York)
- QUOTE_PL_MARKET_CLOSE_HOUR: Hour of market close in configured timezone (default 16)
- QUOTE_PL_MARKET_CLOSE_MINUTE: Minute of market close (default 0)
- QUOTE_PREV_CLOSE_TTL_SEC: TTL for cached previous close prices (default 604800)

S3 Cold Path (optional)

- S3_ENABLED: true|false to enable S3 raw NDJSON sink (default false)
- S3_BUCKET: Target S3 bucket
- S3_PREFIX: Base prefix within the bucket (e.g., polygon/raw)
- AWS_REGION: AWS region for S3 client
- S3_WINDOW_MINUTES: Rotation window minutes (default 30)
- S3_MAX_OBJECT_BYTES: Max object size before rotate (default 512000000)
- S3_PART_SIZE_BYTES: Multipart upload part size (default 16777216)
- S3_USE_MARKER: Create uploading.marker during MPU (default true)

Local Development

1) Create and activate a virtualenv and install dependencies:

   python -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt

2) Copy env template and set values:

   cp .env.example .env

   Ensure both POLYGON_WS_HOST_REALTIME and POLYGON_WS_HOST_DELAYED are set.

3) Run the service:

   export PYTHONPATH=src
   python -m polygon_sink.main

Redis Key Schema and Contracts

- stock:agg1m:{SYMBOL} (TTL default 120s)
  {
    "symbol": "AAPL",
    "start": 1715097600000,
    "end": 1715097659999,
    "open": 191.9,
    "high": 192.7,
    "low": 191.8,
    "close": 192.3,
    "volume": 21834,
    "vwap": 192.1,
    "updatedAt": "2025-05-07T15:04:05.000Z"
  }
- stock:agg5m:{SYMBOL} (TTL default 172800s)
  {
    "day": "2025-05-07",
    "bars": [
      { "ts": 1715099400000, "close": 192.30 },
      { "ts": 1715099700000, "close": 192.45 },
      { "ts": 1715100000000, "close": 192.55 }
    ]
  }

- stock:fmv:{SYMBOL} (TTL default 60s)
  {
    "symbol": "AAPL",
    "price": 192.34,
    "ts": 1715097600000,
    "updatedAt": "2025-05-07T15:04:05.000Z"
  }

- stock:trade:{SYMBOL} (TTL default 60s)
  {
    "symbol": "AAPL",
    "price": 192.34,
    "ts": 1715097600000,
    "size": 100,
    "conditions": [
      12
    ],
    "updatedAt": "2025-05-07T15:04:05.000Z"
  }

- stock:quote:{SYMBOL} (TTL default 60s)
  {
    "symbol": "AAPL",
    "bid": 192.3,
    "bidSize": 200,
    "ask": 192.4,
    "askSize": 180,
    "price": 192.35,
    "prevClose": 191.2,
    "dailyChange": 1.15,
    "dailyChangePct": 0.601,
    "ts": 1715097600000,
    "updatedAt": "2025-05-07T15:04:05.000Z"
  }

Operational Notes

- The service authenticates first, then subscribes:
  - Real-time host: AM and FMV per configured symbols
  - Delayed host: T and Q per configured symbols
- Health logs appear every WS_HEALTH_INTERVAL_SEC.
- With WS_DEBUG=true, outbound/inbound frames (truncated) and `redis_write` events are logged.
- Maintain a single ECS replica to keep one WS connection per host.

Validate Redis

- Quick check using the built-in unit test:

  export PYTHONPATH=src
  python -m polygon_sink.redis_unit_test

- This writes `stock:test` with TTL 60s and reads it back over your configured REDIS_URL (and REDIS_TOKEN for Upstash REST).

Keys to validate in Redis (by metric)

- Aggregates per Minute (AM): key `stock:agg1m:{SYMBOL}` (TTL default 120s)
  - Example: redis-cli --raw GET stock:agg1m:AAPL
- Fair Market Value (FMV): key `stock:fmv:{SYMBOL}` (TTL default 60s)
  - Example: redis-cli --raw GET stock:fmv:AAPL
- Trades (T): key `stock:trade:{SYMBOL}` (TTL default 60s)
  - Example: redis-cli --raw GET stock:trade:AAPL
- Quotes (Q): key `stock:quote:{SYMBOL}` (TTL default 60s)
  - Example: redis-cli --raw GET stock:quote:AAPL

Notes

- Keys should refresh as live data arrives; verify TTL > 0 and values update over time.
- With WS_DEBUG=true, you will see `redis_write` logs for each write, including key and TTL.

Docker

- See Dockerfile and .dockerignore. Build with:

  docker build -t polygon-sink:latest .

AWS ECS

- See `ecs/task-def.json` and `ecs/service.json` for example task and service definitions (Fargate, awslogs). Provide secrets via AWS SSM/Secrets Manager. Ensure desiredCount=1.

S3 layout and Athena

- Objects are written as NDJSON, partitioned by channel and time:
  - s3://$S3_BUCKET/$S3_PREFIX/channel={AM|FMV|T|Q}/ingest_dt=YYYY/MM/DD/hour=HH/part=YYYYMMDD_HHMM-YYYYMMDD_HHMM/{hostname}-seq=000001.ndjson
- Example Athena table for Trades (projection enabled; adjust bucket/prefix):

  CREATE EXTERNAL TABLE IF NOT EXISTS polygon_trades_raw (
    symbol string,
    p double,
    s bigint,
    c array<int>,
    t bigint,
    q bigint,
    ingestTs string
  )
  PARTITIONED BY (
    ingest_dt string,
    hour string
  )
  ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
  WITH SERDEPROPERTIES ('ignore.malformed.json'='true')
  LOCATION 's3://S3_BUCKET/S3_PREFIX/channel=T/'
  TBLPROPERTIES (
    'projection.enabled'='true',
    'projection.ingest_dt.type'='date',
    'projection.ingest_dt.range'='2024/01/01,NOW',
    'projection.ingest_dt.format'='yyyy/MM/dd',
    'projection.hour.type'='integer',
    'projection.hour.range'='0,23',
    'projection.hour.digits'='2',
    'storage.location.template'='s3://S3_BUCKET/S3_PREFIX/channel=T/ingest_dt=${ingest_dt}/hour=${hour}/'
  );
