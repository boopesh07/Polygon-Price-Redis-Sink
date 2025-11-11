Polygon WebSocket → Redis Sink (Python)

## Overview
`polygon-sink` keeps exactly two WebSocket connections to Polygon’s Stocks feed by way of the `massive` SDK:

| Client | Feed | Channels | Purpose |
| ------ | ---- | -------- | ------- |
| realtime | `Feed.Business` | `AM`, `FMV` | minute aggregates + Fair Market Value |
| delayed  | `Feed.DelayedBusiness` | `T`, `Q` | trades + quotes (fallback for non-real‑time data) |

Every event moves straight to Redis using deterministic keys and json payloads, and—if `S3_ENABLED=true`—raw frames are mirrored to S3 as NDJSON for cold-storage/auditing.

Key schemas (TTL defaults in seconds):

| Key | TTL | Payload Highlights |
| --- | --- | ------------------ |
| `stock:agg1m:{SYMBOL}` | none (latest only) | Polygon minute bar + metadata |
| `stock:agg5m:{SYMBOL}` | `AGG5M_TTL_SEC` (default 172800) | {`day`, `bars`[]} produced by `Agg5mCollector` |
| `stock:fmv:{SYMBOL}` | none (latest only) | FMV price + `prevClose`, `dailyChange`, `dailyChangePct` |
| `stock:trade:{SYMBOL}` | none (latest only) | Latest trade |
| `stock:quote:{SYMBOL}` | none (latest only) | Raw quote payload (bid/ask/size, timestamps, etc.) |
| `stock:prev_close:{SYMBOL}` | `QUOTE_PREV_CLOSE_TTL_SEC` (default 604800) | Stored at 4 PM from FMV via `PrevCloseRecorder` |

Quotes remain untouched; FMV owns all daily P/L calculations. At (or just after) 4 PM in the configured timezone, the first FMV price per symbol becomes the next session’s `stock:prev_close:*`, guaranteeing tomorrow morning’s FMV deltas are accurate. Any FMV error automatically logs at `warning` or `error` level, so CloudWatch surfaces issues immediately.

## Configuration
All configuration is managed through Pydantic settings (`src/polygon_sink/config.py`). Required vars:

- `POLYGON_API_KEY`
- `REDIS_URL` – must be Upstash REST (https://…)
- `REDIS_TOKEN`
- `QUOTE_PL_TIMEZONE`, `QUOTE_PL_MARKET_CLOSE_HOUR`, `QUOTE_PL_MARKET_CLOSE_MINUTE`, `QUOTE_PREV_CLOSE_TTL_SEC` – control prev-close behavior

Optional highlights:

- `WS_DEBUG` – emit verbose `redis_write` logs
- `WS_HEALTH_INTERVAL_SEC` – periodic `health` heartbeat
- `BACKOFF_INITIAL_MS`, `BACKOFF_FACTOR`, `BACKOFF_MAX_MS`
- `AGG5M_*` knobs for the 5‑minute collector
- `S3_ENABLED` + `S3_BUCKET` + `S3_PREFIX` + `AWS_REGION` + `S3_WINDOW_MINUTES` + `S3_MAX_OBJECT_BYTES` + `S3_PART_SIZE_BYTES` + `S3_USE_MARKER`

See `.env.example` for the exhaustive list.

## Runtime Flow
1. `polygon_sink.main` loads settings, configures structured logging (`structlog`), builds the Redis sink (Upstash only) and optional S3 writers.
2. `Agg5mCollector` accumulates minute bars into 5‑minute buckets and flushes asynchronously based on `AGG5M_FLUSH_INTERVAL_SEC`.
3. `PrevCloseRecorder` watches FMV events and writes a single `stock:prev_close:*` per symbol per day.
4. Two `MassiveWsClient` instances start, each with exponential backoff. Every handler writes to Redis and optionally calls `write_raw_event` so S3 mirrors stay in lockstep. Any failure writing to Redis or S3 is logged with `logger.error`/`logger.warning` so CloudWatch shows red immediately.
5. A lightweight `_health_loop` emits `{"is_running": …}` snapshots at `WS_HEALTH_INTERVAL_SEC`.

## S3 Cold Path
`S3RawMultiWriter` spins up one rolling writer per channel. Each writer:

- rotates files by `S3_WINDOW_MINUTES` **or** `S3_MAX_OBJECT_BYTES` (whichever comes first)
- uploads streaming multipart parts (`S3_PART_SIZE_BYTES`, ≥16 MB) and deletes bytes from memory immediately, keeping resident memory at ~one part per channel
- writes/removes `uploading.marker` files (warnings logged if marker I/O fails)

Layout: `s3://$S3_BUCKET/$S3_PREFIX/channel={AM|FMV|T|Q}/ingest_dt=YYYY/MM/DD/hour=HH/part=YYYYMMDD_HHMM-YYYYMMDD_HHMM/{hostname}-seq=######.ndjson`

## Local Development
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env   # fill in secrets
export PYTHONPATH=src
python -m polygon_sink.main
```

Unit tests target the collectors/recorders:
```bash
export PYTHONPATH=src
pytest
```

To verify Redis connectivity independently:
```bash
export PYTHONPATH=src
python -m polygon_sink.redis_unit_test
```

## Deployment
- Build container: `docker build -t polygon-sink:latest .`
- ECS/Fargate: use `ecs/task-def.json` as a template. Only **one** task should run at a time to avoid duplicate Polygon subscriptions.
- Structured logs (JSON via `structlog`) ship straight to CloudWatch through the awslogs driver; all Redis/S3/Polygon failures are logged at `error` or `warning` and therefore visible in dashboards/alarms.

## Operational Tips
- Set `WS_DEBUG=true` temporarily when troubleshooting Redis writes.
- Watch `prev_close_recorded` logs around 4 PM to ensure FMV → prev-close writes succeed; if a ticker stops publishing FMV, the recorder simply skips logging and the key will expire, allowing the next valid FMV to backfill.
- If S3 throttles or fails, you’ll see `s3_finalize_failed`, `s3_marker_*`, or `raw_event_write_failed` logs. Those events do **not** stop Redis writes but should be acted on quickly.
