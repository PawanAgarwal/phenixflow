# Backfill Runtime Parameters

This is the canonical run-time parameter guide for ClickHouse historical backfill.

Use this instead of rediscovering knobs from code.

## Scope

Applies to:

- `scripts/backfill/backfill-clickhouse-historical-days.js`
- `scripts/backfill/backfill-clickhouse-historical-days-parallel.sh`

## Required Prerequisites

- ThetaTerminal is already running locally.
- `THETADATA_BASE_URL` is set and reachable (usually `http://127.0.0.1:25503`).
- ClickHouse is running and schema is initialized.

## Canonical Startup

```bash
cd /Users/pawanagarwal/github/phenixflow
set -a; source .env.mon79.local; set +a
```

## Recommended Profiles

### 1) Stable default (recommended)

Use for most remediation runs:

```bash
BACKFILL_MODE=download \
BACKFILL_FORCE=1 \
BACKFILL_WORKERS=2 \
BACKFILL_RAM_BUDGET_MB=10240 \
BACKFILL_NODE_MAX_OLD_SPACE_MB=1024 \
CLICKHOUSE_DELETE_MUTATION_SYNC=0 \
THETADATA_HISTORICAL_OPTION_FORMAT=ndjson \
THETADATA_OPTION_QUOTE_FORMAT=ndjson \
THETADATA_STREAM_HEARTBEAT_EVERY_ROWS=250000 \
THETADATA_STREAM_IDLE_TIMEOUT_MS=1800000 \
bash scripts/backfill/backfill-clickhouse-historical-days-parallel.sh
```

### 2) Max Theta concurrency (when server is healthy)

Use only when Theta is stable and you want to saturate 4 allowed connections:

```bash
BACKFILL_MODE=download \
BACKFILL_FORCE=1 \
BACKFILL_WORKERS=4 \
BACKFILL_RAM_BUDGET_MB=10240 \
BACKFILL_NODE_MAX_OLD_SPACE_MB=1024 \
CLICKHOUSE_DELETE_MUTATION_SYNC=0 \
THETADATA_HISTORICAL_OPTION_FORMAT=ndjson \
THETADATA_OPTION_QUOTE_FORMAT=ndjson \
THETADATA_STREAM_HEARTBEAT_EVERY_ROWS=250000 \
THETADATA_STREAM_IDLE_TIMEOUT_MS=1800000 \
bash scripts/backfill/backfill-clickhouse-historical-days-parallel.sh
```

If retries/500s spike, immediately drop back to 2 workers.

### 3) Targeted raw-component remediation (no unnecessary downloads)

Quote-only remediation:

```bash
BACKFILL_MODE=download \
BACKFILL_FORCE=1 \
BACKFILL_RAW_COMPONENTS=quote \
BACKFILL_SYMBOL_DAY_LIST_PATH=artifacts/reports/missing-quote-symbol-days-<ts>.tsv \
BACKFILL_WORKERS=4 \
BACKFILL_RAM_BUDGET_MB=10240 \
CLICKHOUSE_DELETE_MUTATION_SYNC=0 \
bash scripts/backfill/backfill-clickhouse-historical-days-parallel.sh
```

Stock-only remediation:

```bash
BACKFILL_MODE=download \
BACKFILL_FORCE=1 \
BACKFILL_RAW_COMPONENTS=stock \
BACKFILL_SYMBOL_DAY_LIST_PATH=artifacts/reports/missing-stock-symbol-days-<ts>.tsv \
BACKFILL_WORKERS=4 \
BACKFILL_RAM_BUDGET_MB=10240 \
CLICKHOUSE_DELETE_MUTATION_SYNC=0 \
bash scripts/backfill/backfill-clickhouse-historical-days-parallel.sh
```

### 4) Enrichment-only

```bash
BACKFILL_MODE=enrich \
BACKFILL_FORCE=1 \
BACKFILL_WORKERS=2 \
BACKFILL_RAM_BUDGET_MB=10240 \
CLICKHOUSE_ENRICH_STREAM_READ=1 \
CLICKHOUSE_ENRICH_STREAM_WRITE=1 \
CLICKHOUSE_ENRICH_STREAM_CHUNK_SIZE=5000 \
CLICKHOUSE_ENRICH_PROGRESS_BATCH_MINUTES=10 \
bash scripts/backfill/backfill-clickhouse-historical-days-parallel.sh
```

## Parameter Reference

### Core execution

- `BACKFILL_MODE`: `full | download | enrich`.
- `BACKFILL_FORCE`: `1` to re-run even if cache says complete.
- `BACKFILL_SYMBOL_DAY_LIST_PATH`: TSV (`YYYY-MM-DD<TAB>SYMBOL`) for targeted jobs.
- `BACKFILL_WORKERS`: parallel worker count (keep `<=4` for Theta cap alignment).
- `BACKFILL_REPORT_INCLUDE_JOBS`: `1` for detailed per-job output.

### Memory and worker sizing

- `BACKFILL_RAM_BUDGET_MB`: total allowed backfill memory budget (use `10240` for 10 GB budget).
- `BACKFILL_NODE_MAX_OLD_SPACE_MB`: per-worker V8 heap cap.
- `BACKFILL_WORKER_OVERHEAD_MB`: non-heap estimate per worker.
- `BACKFILL_MEMORY_RESERVE_MB`: system reserve not used by workers.
- `BACKFILL_MEMORY_PER_WORKER_MB`: heuristic cap for worker count.

### Theta stream behavior

- `THETADATA_HISTORICAL_OPTION_FORMAT=ndjson`: stream trade_quote.
- `THETADATA_OPTION_QUOTE_FORMAT=ndjson`: stream option quotes.
- `THETADATA_STREAM_HEARTBEAT_EVERY_ROWS`: progress heartbeat cadence.
- `THETADATA_STREAM_IDLE_TIMEOUT_MS`: client idle timeout for NDJSON stream.
  - `1800000` (30 min) is safe default.
  - `0` disables client idle timeout (use only if external watchdog exists).
- `THETADATA_LARGE_SYMBOLS`: comma list or `all`.
- `THETADATA_LARGE_SYMBOL_WINDOW_MINUTES`: window split for heavy symbols (default 60).

### ClickHouse safety/performance

- `CLICKHOUSE_DELETE_MUTATION_SYNC=0`: async delete mutations; avoid blocking stream ingestion.
- `CLICKHOUSE_CONNECT_TIMEOUT_SEC`: connection timeout.
- `CLICKHOUSE_SEND_TIMEOUT_SEC`: send timeout.
- `CLICKHOUSE_RECEIVE_TIMEOUT_SEC`: receive timeout.
- `CLICKHOUSE_ENRICH_STREAM_READ=1`: stream read trade rows for enrichment.
- `CLICKHOUSE_ENRICH_STREAM_WRITE=1`: stream write enriched rows.
- `CLICKHOUSE_ENRICH_STREAM_CHUNK_SIZE`: insert chunk size (recommended 2000-5000).
- `CLICKHOUSE_ENRICH_PROGRESS_BATCH_MINUTES`: enrich progress log interval (minutes).

### Raw-hydration selectors

- `BACKFILL_RAW_COMPONENTS=all|stock|quote|oi|greeks` (comma-separated supported).
- Use this to avoid unnecessary downloads during remediation.

## Guardrails

- Keep total Theta concurrent streams `<= 4`.
- Prefer 2 workers when Theta is unstable.
- Use targeted missing lists before any broad rerun.
- Keep `CLICKHOUSE_DELETE_MUTATION_SYNC=0` unless debugging mutations.
- For retries, keep exponential backoff and do not use short request timeouts.

## Expected Artifacts

Parallel runner writes run directories under:

- `artifacts/reports/clickhouse-last-week-backfill-<timestamp>/`

Common files:

- `worker<N>.log`
- `worker<N>.json`
- `summary.json`
- `summary.tsv`
- `failures.tsv`

