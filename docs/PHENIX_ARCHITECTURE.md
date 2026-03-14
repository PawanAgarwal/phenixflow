# Phenix Architecture (Current Implementation)

Last updated: 2026-03-11

## 1. Architecture Purpose
Support a cache-first, deterministic options-flow backend where `sigScore` is computed from versioned rules and enriched market context, with strict quality accounting and explainability.

## 2. End-to-End Flow
```mermaid
flowchart LR
  A[Theta data and historical pulls] --> B[Raw option trades cache]
  B --> C[Historical enrichment engine]
  C --> D[Enriched rows and minute aggregates]
  D --> E[Rule-resolved scoring and chip evaluation]
  E --> F[/api/flow + /api/flow/historical + summary/facets/catalog]
```

## 3. Core Runtime Components
1. **Raw trade persistence and day cache**
   - Source rows are stored in `option_trades`.
   - Symbol/day completeness is tracked in `option_trade_day_cache`.
2. **Enrichment pipeline**
   - `queryHistoricalFlow` ensures raw day availability, then calls `ensureEnrichedForDay`.
   - Enrichment computes row-level features, score, quality, chips, and minute rollups.
3. **Rule/config resolution**
   - Active rule comes from `filter_rule_versions`.
   - `FLOW_SIGSCORE_MODEL` can force model selection; mismatched active-rule weights are ignored to prevent cross-model pollution.
4. **Flow API layer**
   - Read APIs (`/api/flow`, `/api/flow/historical`, `/summary`, `/facets`, `/filters/catalog`) serve enriched/cached data.
   - Score-dependent chips are quality- and direction-gated.

## 4. Scoring Architecture
### 4.1 Models
1. `v1_baseline`
2. `v4_expanded`
3. `v5_swing` (current mission model)

### 4.2 `v5_swing` Components
`valueShockNorm`, `volOiNorm`, `repeatNorm`, `otmNorm`, `dteSwingNorm`, `flowImbalanceNorm`, `deltaPressureNorm`, `cpOiPressureNorm`, `ivSkewSurfaceNorm`, `ivTermSlopeNorm`, `underlyingTrendConfirmNorm`, `liquidityQualityNorm`, `sweepNorm`, `multilegPenaltyNorm`.

### 4.3 Availability-Aware Scoring
1. Components missing for a row are excluded from weighted sum.
2. Score is renormalized by sum of absolute weights of available components.
3. Score details are persisted (`sig_score_components_json`) with unavailable-component list.

## 5. Data Reuse and Caching Strategy
1. **Raw day cache**: `option_trade_day_cache` prevents redundant historical trade downloads.
2. **Metric day cache**: `option_trade_metric_day_cache` tracks per-metric enrichment completeness.
3. **Supplemental cache**: `supplemental_metric_cache` stores stock 1m/OI/greeks responses for reuse.
4. **Feature baseline cache**: `feature_baseline_intraday` stores rolling intraday normalization baselines.
5. **Historical immutability**: past-day supplemental cache entries are effectively long-lived.
6. **Parallel supplemental fetches**: bounded queue with `THETADATA_SUPPLEMENTAL_CONCURRENCY` (default `18`).

## 6. Data Model (Implemented)
Primary tables:
1. `option_trades`
2. `option_trade_day_cache`
3. `option_trade_metric_day_cache`
4. `option_trade_enriched`
5. `contract_stats_intraday`
6. `symbol_stats_intraday`
7. `option_symbol_minute_derived`
8. `option_contract_minute_derived`
9. `filter_rule_versions`
10. `supplemental_metric_cache`
11. `feature_baseline_intraday`
12. `saved_queries`
13. `ingest_checkpoints`

## 7. Quality, Explainability, and Gating
1. Each enriched row includes:
   - `sig_score`
   - `score_quality`
   - `missing_metrics_json`
   - `sig_score_components_json`
2. Score-dependent chips (`high-sig`, `unusual`, `urgent`) require:
   - non-degraded mode,
   - quality eligibility,
   - directional sentiment eligibility.
3. API payloads expose score metadata so clients can avoid acting on low-quality signals.

## 8. API Contract (Current)
1. `/api/flow` and `/api/flow/historical` return score, quality, missing metrics, rule version, and score components.
2. `/api/flow/summary` and `/api/flow/facets` are computed from same enriched/rule-resolved logic.
3. `/api/flow/filters/catalog` exposes active thresholds and scoring model context.

## 9. Determinism and Reliability Controls
1. Rule-versioned scoring for replay consistency.
2. Explicit cache completeness states (`full`/`partial`) for day and metric caches.
3. Retry/backoff on Theta pulls plus bounded concurrency.
4. Readiness/health endpoints (`/ready`, `/health`).

## 10. Current Architectural Focus
1. Keep `v5_swing` deterministic and high-quality under real cache-reuse workloads.
2. Promote calibrated rule versions only after walk-forward performance gates pass.
3. Avoid architectural drift: docs/spec/runtime/seed must remain aligned to active scoring behavior.

## 11. Historical Backfill and Remediation Subsystem
### 11.1 Purpose
Provide deterministic, resumable symbol-day hydration and enrichment in ClickHouse with explicit attempt tracking and gap verification.

### 11.2 Orchestration Components
1. Symbol-day list generator (`scripts/backfill/generate-symbol-days-topn-range.js`) with Theta calendar checks.
2. Worker orchestrator (`scripts/backfill/backfill-clickhouse-historical-days-parallel.sh`) for bounded concurrency and memory budgets.
3. Worker runtime (`scripts/backfill/backfill-clickhouse-historical-days.js`) for per-symbol/day execution, retries, and job-level reporting.
4. Core materialization engine (`materializeHistoricalDayInClickHouse` in `src/historical-flow.js`) with download/enrich modes.

### 11.3 Canonical Backfill Data Planes
1. Raw planes:
   - `option_trades`
   - `option_quote_minute_raw`
   - `stock_ohlc_minute_raw`
2. Plane role split:
   - `option_trades` is the base day-cache stream (trade/trade-quote sync gate).
   - `BACKFILL_RAW_COMPONENTS` supports `tradequote` plus supplemental hydration tokens (`stock|quote|oi|greeks`).
   - For explicit component selections that exclude `tradequote`, trade sync defaults to skip unless explicitly overridden.
3. Enriched plane:
   - `option_trade_enriched`
4. Calculated-greeks plane:
   - `option_calculated_greeks_minute`
   - Enrichment resolves greeks from this table first by default (`CLICKHOUSE_ENRICH_GREEKS_SOURCE=calculated_first`), with optional fallback/override modes.
5. Status/attempt planes:
   - `option_download_chunk_status`
   - `option_enrich_chunk_status`
   - `option_trade_day_cache`
   - `option_trade_metric_day_cache`
6. Reference factors:
   - `reference_sofr_daily` (Fed SOFR daily rates for reproducible rate assumptions)

### 11.4 Session and Expectation Model
1. Calendar `open` and `early_close` days are tradable and must be included in symbol-day generation.
2. Coverage verification uses:
   - padded-session expectation for stock (`open -> close+pad`),
   - core-session expectation for quote/trade (`open -> close`, close minute excluded).
3. Closed days are explicitly excluded from failure accounting.

### 11.5 Attempted vs Missing Semantics
1. A symbol-day is `attempted` if status exists in chunk-status tables for that stream.
2. Default missing analysis must count only `attempted_missing`.
3. Unattempted symbol-days are tracked separately for scheduling, not as failures.

### 11.6 Observability and Telemetry
1. Endpoint logs: `[THETA_DOWNLOAD]` with API, status, duration, rows, and `bytesDownloaded`.
2. Stream liveness: `[THETA_STREAM_HEARTBEAT]` with parsed/inserted row counters.
3. Per-job slot coverage (optional): `BACKFILL_GAP_TELEMETRY=1` emits expected/actual/missing slot summaries.
4. Batch enrichment progress: `[ENRICH_BATCH_PROGRESS]` for minute-bucket throughput.
5. Enrichment greeks source telemetry: `[ENRICH_GREEKS_SOURCE]` with mode (`calculated_first|calculated|raw`), selected source, and rows loaded.
6. Raw-component trade-sync policy telemetry:
   - `[BACKFILL_RAW_COMPONENTS_TRADE_SYNC_POLICY]` is emitted for explicit `BACKFILL_RAW_COMPONENTS` runs.
   - `[BACKFILL_RAW_COMPONENTS_FORCE_TRADE_SYNC_IGNORED]` is emitted when legacy force is ignored because `tradequote` was not selected.
   - Operational policy: monitor non-skip `tradequote` override usage; if it remains zero across backfill waves, remove the non-skip override path.
7. Greeks gap/window telemetry:
   - `[GREEKS_GAP_FILL]` reports expected/missing trade-minute slots and chosen strategy.
   - `[GREEKS_WINDOW_PLAN]` reports final request-window strategy, adaptive window mode, and window count.
8. ClickHouse mutation-budget telemetry:
   - `[CLICKHOUSE_DELETE_BUDGET_SPIKE]` marks delete-latency spikes.
   - `[CLICKHOUSE_DELETE_BUDGET_DOWNGRADE]` marks table-level downgrade to insert-only mode.
   - `[CLICKHOUSE_DELETE_BUDGET_SKIP]` marks subsequent skipped deletes for downgraded tables.
   - `[CLICKHOUSE_DELETE_AUDIT]` carries mutation outcome plus budget-state fields.

### 11.7 Operational Loop Contract
1. Download -> verify -> enrich -> verify for the same day/batch before moving forward.
2. Requeue only failed symbol-days/workers; avoid restarting healthy workers.
3. Quote-hole pattern (`quoteSlots == tradeSlots`, both below quote expectation) should trigger quote-only full refresh mode.
4. Benchmark each wave by normalized throughput and slot closure before scaling to broader ranges.
5. Quote remediation strategy:
   - Build missing-minute contiguous windows from existing quote-minute coverage.
   - In force quote mode (`BACKFILL_FORCE_QUOTE_FULL=1`), prefer these gap windows first to reduce transfer volume and runtime.
   - Fall back to full-day windows only when gap planning cannot be applied efficiently (for example, excessive window fragmentation).
6. Greeks remediation strategy:
   - Build missing-minute contiguous windows from trade-minute baseline vs existing greeks minute coverage.
   - Apply gap windows for top-up/remediation waves (existing greek coverage present); use adaptive broader windows for fresh symbol-days.
   - In force mode (`BACKFILL_FORCE_GREEKS_FULL=1`), prefer gap windows first when `BACKFILL_FORCE_GREEKS_GAP_WINDOWS=1`.
   - Fall back to adaptive split windows only when gap-window planning is not efficient (for example, excessive fragmentation).
7. Download worker startup guard:
   - For large waves, launch must fail fast if effective download workers are below target (`BACKFILL_DOWNLOAD_WORKER_GUARD*`).
   - This prevents accidental under-cap runs (for example, overlap/memory auto-caps silently dropping download workers below 4).
8. Mutation budget protection strategy:
   - Keep proactive insert-only defaults enabled for known-safe ReplacingMergeTree paths.
   - For delete-required paths, monitor delete latency and auto-downgrade only allowlisted safe tables after repeated spikes.
   - Keep strict delete semantics for core MergeTree fact tables (`option_trades`, `option_trade_enriched`) unless explicitly overridden.

### 11.8 Greeks Model Input Sources
1. Local IV/Greeks reconstruction requires explicit assumptions for risk-free rate and dividends in addition to raw market data.
2. Runtime can now pass Greek model controls directly to Theta endpoints:
   - `THETADATA_GREEKS_RATE_TYPE`
   - `THETADATA_GREEKS_RATE_VALUE`
   - `THETADATA_GREEKS_ANNUAL_DIVIDEND`
   - `THETADATA_GREEKS_DIVIDEND_OVERRIDES`
   - `THETADATA_GREEKS_VERSION`
3. Canonical source-coverage matrix (available vs missing factor tables) is maintained in `docs/THETADATA_GREEKS_INPUT_SOURCES.md`.
