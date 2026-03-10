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

## Coverage Analysis (Post-Run Verification)

Use the analyzer to compute month/symbol/day slot coverage from raw or chunk sources:

```bash
node scripts/backfill/analyze-1m-coverage.js \
  --symbol-days <symbol_days.tsv> \
  --calendar <calendar_detailed.tsv> \
  --from YYYY-MM-DD \
  --to YYYY-MM-DD \
  --source raw \
  --out-dir artifacts/reports \
  --tag <timestamp>
```

Defaults:
- `--attempted-only 1` (default): count only attempted symbol-days as missing.
- `--source raw` (default): authoritative but slower.
- `--source chunk`: faster, only valid if chunk status is fully rebuilt for range.

## Recommended Profiles

### 1) Stable default (recommended)

Use for most remediation runs:

```bash
BACKFILL_MODE=download \
BACKFILL_FORCE=1 \
THETADATA_MAX_CONCURRENT_CONNECTIONS=4 \
BACKFILL_WORKERS=4 \
BACKFILL_RAM_BUDGET_MB=10240 \
BACKFILL_NODE_MAX_OLD_SPACE_MB=1024 \
CLICKHOUSE_DELETE_MUTATION_SYNC=1 \
THETADATA_HISTORICAL_OPTION_FORMAT=ndjson \
THETADATA_OPTION_QUOTE_FORMAT=ndjson \
THETADATA_STREAM_HEARTBEAT_EVERY_ROWS=250000 \
THETADATA_STREAM_IDLE_TIMEOUT_MS=1800000 \
bash scripts/backfill/backfill-clickhouse-historical-days-parallel.sh
```

### 2) Max Theta concurrency (when server is healthy)

Use when Theta is stable and you want max download throughput:

```bash
BACKFILL_MODE=download \
BACKFILL_FORCE=1 \
THETADATA_MAX_CONCURRENT_CONNECTIONS=4 \
BACKFILL_WORKERS=4 \
BACKFILL_RAM_BUDGET_MB=10240 \
BACKFILL_NODE_MAX_OLD_SPACE_MB=1024 \
CLICKHOUSE_DELETE_MUTATION_SYNC=1 \
THETADATA_HISTORICAL_OPTION_FORMAT=ndjson \
THETADATA_OPTION_QUOTE_FORMAT=ndjson \
THETADATA_STREAM_HEARTBEAT_EVERY_ROWS=250000 \
THETADATA_STREAM_IDLE_TIMEOUT_MS=1800000 \
bash scripts/backfill/backfill-clickhouse-historical-days-parallel.sh
```

If retries/500s spike, immediately drop to 3 workers, then 2 workers.

### 3) Targeted raw-component remediation (no unnecessary downloads)

Quote-only remediation:

```bash
BACKFILL_MODE=download \
BACKFILL_FORCE=1 \
BACKFILL_RAW_COMPONENTS=quote \
BACKFILL_FORCE_QUOTE_FULL=1 \
BACKFILL_GAP_TELEMETRY=1 \
BACKFILL_SYMBOL_DAY_LIST_PATH=artifacts/reports/missing-quote-symbol-days-<ts>.tsv \
THETADATA_MAX_CONCURRENT_CONNECTIONS=4 \
BACKFILL_WORKERS=4 \
BACKFILL_RAM_BUDGET_MB=10240 \
CLICKHOUSE_DELETE_MUTATION_SYNC=1 \
bash scripts/backfill/backfill-clickhouse-historical-days-parallel.sh
```

Stock-only remediation:

```bash
BACKFILL_MODE=download \
BACKFILL_FORCE=1 \
BACKFILL_RAW_COMPONENTS=stock \
BACKFILL_GAP_TELEMETRY=1 \
BACKFILL_SYMBOL_DAY_LIST_PATH=artifacts/reports/missing-stock-symbol-days-<ts>.tsv \
THETADATA_MAX_CONCURRENT_CONNECTIONS=4 \
BACKFILL_WORKERS=4 \
BACKFILL_RAM_BUDGET_MB=10240 \
CLICKHOUSE_DELETE_MUTATION_SYNC=1 \
bash scripts/backfill/backfill-clickhouse-historical-days-parallel.sh
```

### 4) Date-range setup + pipeline run (example: Nov 1-30, 2025)

```bash
START_DATE=2025-11-01 \
END_DATE=2025-11-30 \
SYMBOL_LIMIT=100 \
THETADATA_MAX_CONCURRENT_CONNECTIONS=4 \
DOWNLOAD_WORKERS=4 \
ENRICH_WORKERS=4 \
BACKFILL_RAM_BUDGET_MB=10240 \
BACKFILL_NODE_MAX_OLD_SPACE_MB=1024 \
PIPELINE_STAGE_OVERLAP=1 \
bash scripts/backfill/run-clickhouse-backfill-range.sh
```

### 5) Enrichment-only

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
  - This now propagates end-to-end into `materializeHistoricalDayInClickHouse(..., forceRecompute=true)` so trade/day-cache sync is not silently skipped.
- `BACKFILL_FORCE_QUOTE_FULL`: quote force mode scope; default `0` (minute-resume scope), set `1` for full-day quote rewrites.
- `BACKFILL_GAP_TELEMETRY`: `1` to emit per-job expected/actual minute-slot coverage (stock/quote/trade/enrich + missing deltas) into worker logs and job JSON.
  - Coverage fields now include both padded-session and core-session expectations:
    - `expectedPaddedSlots` / `missingStockSlots` (stock vs padded session)
    - `expectedCoreSlots` / `missingQuoteCoreSlots` / `missingTradeCoreSlots` (quote/trade vs core session)
    - `missingEnrichVsTradeSlots` (enrich parity with trade stream)
- `BACKFILL_SYMBOL_DAY_LIST_PATH`: TSV (`YYYY-MM-DD<TAB>SYMBOL`) for targeted jobs.
- `BACKFILL_WORKERS`: parallel worker count (if unset and `BACKFILL_MODE=download`, defaults to `THETADATA_DOWNLOAD_CONCURRENCY`).
- `BACKFILL_SHARD_STRATEGY`: worker shard strategy (`balanced` default, `hash` for legacy modulo hash sharding).
- `THETADATA_MAX_CONCURRENT_CONNECTIONS`: hard cap for Theta concurrent download workers (default `4`).
- `THETADATA_DOWNLOAD_CONCURRENCY`: target worker concurrency for download mode when `BACKFILL_WORKERS` is unset (default `THETADATA_MAX_CONCURRENT_CONNECTIONS`).
- `BACKFILL_REPORT_INCLUDE_JOBS`: `1` for detailed per-job output.
- Coverage analyzer:
  - `--attempted-only`: `1` (default) excludes unattempted days from missing counts.
  - `--attempted-only 0`: legacy behavior (treat all expected days as in-scope).

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

- `CLICKHOUSE_DELETE_MUTATION_SYNC=1`: synchronous single-replica mutation wait; deterministic delete+rewrite behavior.
- `CLICKHOUSE_INSERT_ONLY_STOCK_QUOTE=1` (default): skip day-scope delete mutations for `stock_ohlc_minute_raw` and `option_quote_minute_raw` and rely on ReplacingMergeTree latest-row semantics.
  - Set `0` only when you explicitly need delete+rewrite semantics for a controlled run.
- `CLICKHOUSE_QUOTE_INCLUDE_RAW_PAYLOAD=0` (default): stores `{}` for quote payloads to reduce insert size and query pressure.
- `CLICKHOUSE_INSERT_MAX_BYTES` default is `33554432` (32 MiB) to reduce insert chunk/process overhead.
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
- Keep `CLICKHOUSE_DELETE_MUTATION_SYNC=1` for deterministic backfills. Use `0` only for controlled throughput experiments.
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

Coverage analyzer artifacts:
- `coverage-1m-<from>-<to>-<tag>-month-summary.tsv`
- `coverage-1m-<from>-<to>-<tag>-symbol-summary.tsv`
- `coverage-1m-<from>-<to>-<tag>-anomalies.tsv`

## Operational Learnings

Failure signatures, remediation loops, and anti-rerun guardrails are captured in:

- `docs/BACKFILL_OPERATIONAL_LEARNINGS.md`
