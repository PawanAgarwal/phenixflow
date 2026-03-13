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
BACKFILL_NODE_MAX_OLD_SPACE_MB=1536 \
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
BACKFILL_NODE_MAX_OLD_SPACE_MB=1536 \
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
BACKFILL_NODE_MAX_OLD_SPACE_MB=1536 \
PIPELINE_STAGE_OVERLAP=1 \
bash scripts/backfill/run-clickhouse-backfill-range.sh
```

### 4b) Max Theta throughput within 10GB overlap budget (recommended for full-month waves)

Use asymmetric overlap so download saturates Theta cap (`4`) while enrichment stays within memory guardrails:

```bash
START_DATE=2025-10-01 \
END_DATE=2025-10-31 \
SYMBOL_LIMIT=100 \
THETADATA_MAX_CONCURRENT_CONNECTIONS=4 \
DOWNLOAD_WORKERS=4 \
ENRICH_WORKERS=2 \
BACKFILL_RAM_BUDGET_MB=10240 \
BACKFILL_NODE_MAX_OLD_SPACE_MB=1536 \
PIPELINE_STAGE_OVERLAP=1 \
THETADATA_STREAM_IDLE_TIMEOUT_MS=1800000 \
bash scripts/backfill/run-clickhouse-backfill-range.sh
```

If startup logs show `Capping overlapping workers ... download 4->3, enrich 4->3`, relaunch with `DOWNLOAD_WORKERS=4 ENRICH_WORKERS=2`.

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
- `BACKFILL_FORCE_QUOTE_FULL`: quote force mode scope; default `0` (minute-resume scope).
  - When `1`, quote remediation now prefers contiguous missing-minute windows first (when gap planning is enabled) and falls back to full-day windows only if needed (for example, too many gap windows).
- `BACKFILL_FORCE_QUOTE_GAP_WINDOWS`: `1` (default) enables missing-minute contiguous window planning even when `BACKFILL_FORCE_QUOTE_FULL=1`.
  - Set `0` only when you explicitly want strict full-day quote rewrites in force mode.
- `BACKFILL_FORCE_GREEKS_FULL`: greeks force mode scope; default `0` (minute-targeted scope).
  - When `1`, force mode may rewrite broader greek windows if gap planning cannot be applied.
- `BACKFILL_GREEKS_GAP_FILL`: `1` (default) enables greeks missing-minute contiguous window planning using trade-minute baseline.
  - Applied for remediation/top-up when greek rows already exist for the symbol-day; fresh symbol-days use adaptive broader windows.
- `BACKFILL_FORCE_GREEKS_GAP_WINDOWS`: `1` (default) keeps gap-window planning enabled even when `BACKFILL_FORCE_GREEKS_FULL=1`.
- `BACKFILL_GREEKS_GAP_MAX_WINDOWS`: max contiguous gap windows to request before falling back to broader windows (default `24`).
- `BACKFILL_TRADE_SYNC_MODE`: `auto | skip | force`.
  - Default when unset: `auto`.
  - Special default: when `BACKFILL_RAW_COMPONENTS` is explicitly set and does **not** include `tradequote`, trade sync is auto-forced to `skip` to avoid unnecessary trade mutations.
  - Use `BACKFILL_TRADE_SYNC_MODE=force` only for explicit trade-stream repair cases.
- `BACKFILL_FORCE_TRADE_SYNC`: legacy force flag.
  - Still supported, but ignored by default when explicit raw-component selection excludes `tradequote`, unless you set `BACKFILL_TRADE_SYNC_MODE=force`.
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
- Download worker startup guard (enabled by default):
  - `BACKFILL_DOWNLOAD_WORKER_GUARD=1`
  - `BACKFILL_DOWNLOAD_WORKER_GUARD_MIN_JOBS=200`
  - `BACKFILL_DOWNLOAD_WORKER_GUARD_TARGET=4` (clamped to `THETADATA_MAX_CONCURRENT_CONNECTIONS`)
  - For large runs (`jobs >= min`), launch fails fast if effective download workers are below target.
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
- `THETADATA_DOWNLOAD_TRACE=1` (default): emit `[THETA_DOWNLOAD]` for every Theta request with rows and `bytesDownloaded`.
- `THETADATA_LARGE_SYMBOLS`: comma list or `all`.
- `THETADATA_LARGE_SYMBOL_WINDOW_MINUTES`: window split for heavy symbols (default 60).
- SOFR reference source:
  - `npm run clickhouse:sofr:sync` ingests New York Fed SOFR into `options.reference_sofr_daily`.
  - First run seeds ~3 years; subsequent runs refresh incrementally with overlap.
- Greeks window sizing:
  - `THETADATA_GREEKS_WINDOW_MINUTES`: base greek window size (defaults to `THETADATA_LARGE_SYMBOL_WINDOW_MINUTES`).
  - `THETADATA_GREEKS_ADAPTIVE_WINDOWS=1` (default): enable adaptive greek window sizing by expiration count.
  - `THETADATA_GREEKS_WINDOW_MIN_MINUTES=15`, `THETADATA_GREEKS_WINDOW_MAX_MINUTES=391`.
  - `THETADATA_GREEKS_ADAPTIVE_LOW_EXPIRATIONS=120`
  - `THETADATA_GREEKS_ADAPTIVE_HIGH_EXPIRATIONS=400`
  - `THETADATA_GREEKS_ADAPTIVE_VERY_HIGH_EXPIRATIONS=800`
  - Optional Greek model inputs (passed to Theta Greeks history endpoint):
    - `THETADATA_GREEKS_RATE_TYPE=sofr|bond|federal_funds|zero` (default `sofr`).
    - `THETADATA_GREEKS_RATE_VALUE=<float>` (optional explicit annualized rate override).
    - `THETADATA_GREEKS_ANNUAL_DIVIDEND=<float>` (optional global annualized dividend amount).
    - `THETADATA_GREEKS_DIVIDEND_OVERRIDES=<SYM=DIV,...>` (optional symbol-level overrides; takes precedence over global dividend).
    - `THETADATA_GREEKS_VERSION=<int>` (optional Theta model version).
  - Source coverage and remaining gaps for local IV/Greeks reconstruction are documented in `docs/THETADATA_GREEKS_INPUT_SOURCES.md`.

### ClickHouse safety/performance

- `CLICKHOUSE_DELETE_MUTATION_SYNC=1`: synchronous single-replica mutation wait; deterministic delete+rewrite behavior.
- `CLICKHOUSE_INSERT_ONLY_STOCK_QUOTE=1` (default): skip day-scope delete mutations for `stock_ohlc_minute_raw` and `option_quote_minute_raw` and rely on ReplacingMergeTree latest-row semantics.
  - Set `0` only when you explicitly need delete+rewrite semantics for a controlled run.
- `CLICKHOUSE_INSERT_ONLY_CHUNK_STATUS=1` (default): skip delete-before-insert for `option_download_chunk_status` and `option_enrich_chunk_status` (ReplacingMergeTree handles latest row version).
- `CLICKHOUSE_INSERT_ONLY_ENRICH_SUPPORT=0` (default): when set to `1`, skip delete-before-insert for:
  - `option_symbol_minute_derived`
  - `option_contract_minute_derived`
  - `contract_stats_intraday`
  - `symbol_stats_intraday`
  This reduces mutation overhead significantly during backfills, but can temporarily create duplicate versions until merges complete. Enable only when downstream reads are dedupe-safe (`argMax`/`FINAL`) or you will run a cleanup rewrite pass afterward.
- `CLICKHOUSE_TRACK_DELETES=1` (default): emit one audit event for every `ALTER ... DELETE` scope used by the pipeline.
- `CLICKHOUSE_TRACK_DELETE_COUNTS=0` (default): include scoped row counts before/after each delete in audit events when set to `1`.
- `CLICKHOUSE_TRACK_DELETE_RECHECK_MS=0` (default): optional delayed second count check; use when testing async mutation behavior (`CLICKHOUSE_DELETE_MUTATION_SYNC=0`).
- `CLICKHOUSE_DELETE_AUDIT_PATH`: optional path for delete audit JSONL log (default `artifacts/reports/clickhouse-delete-audit.jsonl`).
- `CLICKHOUSE_DELETE_BUDGET_PROTECTION=1` (default): runtime mutation-budget guard for delete-heavy backfills.
- `CLICKHOUSE_DELETE_BUDGET_SPIKE_MS=5000` (default): a delete taking longer than this is counted as a spike.
- `CLICKHOUSE_DELETE_BUDGET_SPIKE_COUNT=2` (default): number of spikes before table-level auto-downgrade.
- `CLICKHOUSE_DELETE_BUDGET_DOWNGRADE_TABLES`: comma list of tables eligible for auto-downgrade to insert-only when spikes persist.
  - Default:
    - `stock_ohlc_minute_raw`
    - `option_quote_minute_raw`
    - `option_open_interest_raw`
    - `option_download_chunk_status`
    - `option_enrich_chunk_status`
    - `option_symbol_minute_derived`
    - `option_contract_minute_derived`
    - `contract_stats_intraday`
    - `symbol_stats_intraday`
  - Special values: `default`, `none`, `all`.
  - Safety note: tables that require strict delete semantics (for example `option_trades` and `option_trade_enriched`) are intentionally excluded by default.
- `CLICKHOUSE_QUOTE_INCLUDE_RAW_PAYLOAD=0` (default): stores `{}` for quote payloads to reduce insert size and query pressure.
- `CLICKHOUSE_INSERT_MAX_BYTES` default is `67108864` (64 MiB) to reduce insert chunk/process overhead.
- `CLICKHOUSE_ENRICH_STREAM_CHUNK_SIZE` default is `20000` rows (bounded by `CLICKHOUSE_INSERT_MAX_BYTES`).
- `CLICKHOUSE_CONNECT_TIMEOUT_SEC`: connection timeout.
- `CLICKHOUSE_SEND_TIMEOUT_SEC`: send timeout.
- `CLICKHOUSE_RECEIVE_TIMEOUT_SEC`: receive timeout.
- Pipeline runner (`scripts/backfill/backfill-clickhouse-historical-days-pipeline.sh`) passes the above timeout vars plus `CLICKHOUSE_DELETE_MUTATION_SYNC` to every worker process.
- `CLICKHOUSE_ENRICH_STREAM_READ=1`: stream read trade rows for enrichment.
- `CLICKHOUSE_ENRICH_STREAM_WRITE=1`: stream write enriched rows.
- `CLICKHOUSE_ENRICH_STREAM_CHUNK_SIZE`: insert chunk size (recommended 2000-5000).
- `CLICKHOUSE_ENRICH_PROGRESS_BATCH_MINUTES`: enrich progress log interval (minutes).
- `CLICKHOUSE_FORCE_COVERAGE_PROJECTION=0` (default): optional strict mode to force projection (`p_cov_day_symbol_minute`) for minute-coverage/count queries. Keep `0` during projection catch-up to avoid fallback retries.
- `CLICKHOUSE_COVERAGE_OPTIMIZE_IN_ORDER=1` (default): enables `optimize_aggregation_in_order` for minute-coverage/count queries.

### ClickHouse query acceleration for 1m coverage scans

- Use projection-backed minute rollups for coverage queries (avoids wide `uniqExact` scans on raw quote rows):

```bash
node scripts/clickhouse/optimize-clickhouse-query-paths.js --materialize 1 --partitions 202511,202512,202601,202602,202603
```

- Check projection materialization progress:

```bash
node scripts/clickhouse/optimize-clickhouse-query-paths.js --status-only 1
```

- For immediate sync materialization of a small partition, add `--wait 1`:

```bash
node scripts/clickhouse/optimize-clickhouse-query-paths.js --materialize 1 --partitions 202603 --wait 1
```

- `scripts/backfill/analyze-1m-coverage.js --source raw` uses a projection-friendly grouped minute subquery path (instead of direct `uniqExact` on the raw quote table).
- Coverage analyzer flags:
  - `--force-projection 0` (default): force projection path (`p_cov_day_symbol_minute`) only when explicitly enabled.
  - `--optimize-in-order 1` (default): add `optimize_aggregation_in_order=1`.

### Raw-hydration selectors

- `BACKFILL_RAW_COMPONENTS=all|tradequote|stock|quote|oi|greeks` (comma-separated supported).
- Use this to avoid unnecessary downloads during remediation.
- Trade sync behavior:
  - Include `tradequote` when you want trade/trade-quote stream sync to run as part of that wave.
  - If `BACKFILL_RAW_COMPONENTS` is explicitly set and excludes `tradequote`, trade sync defaults to `skip`.
  - Explicit-selection policy telemetry is emitted as `[BACKFILL_RAW_COMPONENTS_TRADE_SYNC_POLICY]`.
  - Ignored legacy force telemetry is emitted as `[BACKFILL_RAW_COMPONENTS_FORCE_TRADE_SYNC_IGNORED]`.
- Quote gap/window telemetry:
  - `[QUOTE_GAP_FILL]` reports expected/missing minutes and strategy (`gap_windows`, `force_gap_windows`, `force_fallback_full_day`, `already_complete`).
  - `[QUOTE_WINDOW_PLAN]` reports final request-window strategy and window count.
- Greeks gap/window telemetry:
  - `[GREEKS_GAP_FILL]` reports expected/missing minutes and strategy (`gap_windows`, `force_gap_windows`, `force_fallback_full_day`, `already_complete`).
  - `[GREEKS_WINDOW_PLAN]` reports request-window strategy, adaptive window mode/size, and window count.
- ClickHouse mutation-budget telemetry:
  - `[CLICKHOUSE_DELETE_BUDGET_SPIKE]` logs spike detections.
  - `[CLICKHOUSE_DELETE_BUDGET_DOWNGRADE]` logs table-level auto-downgrade activation.
  - `[CLICKHOUSE_DELETE_BUDGET_SKIP]` logs when a downgraded table skip is first applied.
  - `[CLICKHOUSE_DELETE_AUDIT]` now includes budget fields (`budgetProtectedMode`, `budgetIsSpike`, `budgetDowngradedNow`, `budgetDowngradedActive`).

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
