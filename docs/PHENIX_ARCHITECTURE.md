# Phenix Core Quant V1 Architecture

## 1) Purpose and Scope
This document defines the target architecture to deliver the Core Quant V1 goals in `docs/PHENIX_PROJECT_GOALS.md`, using ThetaData as the only market-data source and respecting the entitlement/runtime constraints in `docs/THETADATA_DEV_GUIDE.md`.

Primary outcomes:
- Real-time options flow ingestion + enrichment for top-200 symbols.
- Deterministic chip/filter engine that drives API and UI behavior.
- Backward-compatible saved filters/alerts while expanding to V2 state.
- Operational reliability for latency, shadow rollout, and graceful degradation.

## 2) Inputs Used
- Product goals and formulas: `docs/PHENIX_PROJECT_GOALS.md`
- ThetaData entitlement/runtime constraints: `docs/THETADATA_DEV_GUIDE.md`
- Current implementation baseline:
  - API + flow query scaffold: `src/app.js`, `src/flow.js`
  - Threshold/execution chips: `src/flow-filter-definitions.js`, `src/activity/filters.js`
  - Saved filter compatibility: `src/saved-filters-alerts.js`
  - Shadow rollout baseline: `src/shadow/*`, `scripts/mon77-shadow-rollout.js`
  - Theta preflight/bootstrap tooling: `scripts/mon79_*`, `scripts/mon86_thetadata_bootstrap.py`

## 3) Current State vs Target State

### 3.1 What exists now
- Express API scaffold for flow list/facets/stream/detail and saved presets/alerts.
- Deterministic parsing for execution filters (`Calls`, `Puts`, `Bid`, `Ask`, `AA`, `Sweeps`).
- Configurable threshold filters (`100k+`, `Sizable`, `Whales`, `Large Size`) via env vars.
- In-memory + artifact-backed query mode (`source=real-ingest`) for integration-style tests.
- Compatibility layer between legacy saved payloads and DSL V2 clauses.
- Shadow diff workflow + artifacts for multi-session comparisons.
- ThetaData config-check, smoke, and bootstrap preflight scripts.

### 3.2 Gaps to close
- No production ingestion worker consuming Theta stream into durable storage.
- No persistent data model for raw trades, enriched rows, or rolling aggregates.
- No full metric engine (`dte`, `otmPct`, `volOiRatio`, `repeat3m`, `sigScore`) at runtime scale.
- No V1 endpoints for `/api/flow/summary` and `/api/flow/filters/catalog`.
- No full top-200 universe management, lag-aware fallback, or p95 observability pipeline.
- No integrated feature-flag + shadow-mode reporting in live runtime.

## 4) Architecture Principles
1. Deterministic first: metric/rule outcomes are reproducible from stored raw inputs.
2. Config-driven rules: thresholds and chip formulas are versioned data, not hardcoded behavior.
3. Read path isolation: query API never blocks on live Theta calls.
4. Graceful degradation: if enrichment lags, API can serve explicit raw-mode status.
5. Backward compatibility: legacy saved filters/alerts continue to read/write safely.
6. Entitlement safety: endpoint use is bounded by Options Standard + Stocks Value access.

## 5) Target System Architecture

```mermaid
flowchart LR
  A[ThetaTerminal v3<br/>local service] --> B[Theta Ingestion Worker]
  B --> C[(option_trades raw)]
  B --> D[(ingest checkpoints)]
  C --> E[Enrichment + Metrics Engine]
  E --> F[(option_trade_enriched)]
  E --> G[(contract_stats_intraday)]
  E --> H[(symbol_stats_intraday)]
  E --> I[(filter_rule_versions)]
  F --> J[Flow Query API]
  G --> J
  H --> J
  I --> J
  J --> K[/api/flow]
  J --> L[/api/flow/summary]
  J --> M[/api/flow/filters/catalog]
  J --> N[/api/flow/stream]
  O[Saved Presets/Alerts API] --> P[(saved_queries)]
  P --> J
  Q[Feature Flags + Shadow Comparator] --> J
  R[Observability + SLO Dashboards] --> B
  R --> E
  R --> J
```

## 6) Component Design and Implementation Pieces

### 6.1 Theta runtime and preflight layer
Purpose: ensure local/hosted ThetaTerminal is reachable and configured before ingest or smoke.

Pieces to implement:
- Promote MON-79/MON-86 scripts into a reusable runtime module for app startup checks.
- Standard env contract:
  - `THETADATA_BASE_URL`
  - `THETADATA_DOWNLOAD_PATH`
  - `THETADATA_INGEST_PATH` (explicit ingest endpoint)
  - `THETADATA_HEALTH_PATH`
  - timeout/retry vars
- Failure taxonomy passthrough in logs and metrics:
  - `MON86_ERR_MISSING_CREDS`, `MON86_ERR_PORT_IN_USE`, `MON86_ERR_DOWNLOAD_FAILED`, `MON86_ERR_HEALTH_TIMEOUT`, `MON86_ERR_CONFIG`

### 6.2 Ingestion worker
Purpose: continuously pull/stream option flow events and persist immutable raw records.

Pieces to implement:
- Worker process (`src/ingest/worker.js`) separate from request-serving process.
- Theta connector client (`src/thetadata/client.js`) with:
  - reconnect loop
  - request timeout
  - exponential retry with jitter
  - watermark/checkpoint resume (similar semantics to `src/flow-live.js` watermark handling)
- Event parser + normalizer (`src/ingest/normalize.js`) mapping Theta payloads to canonical raw schema.
- Dedup/idempotency strategy:
  - deterministic trade identity hash (`symbol+expiration+strike+right+ts+price+size+condition`)
  - upsert-ignore semantics for replays
- Dead-letter capture (`src/ingest/dead-letter.js`) for parse failures with reason code + raw snippet.
- Backpressure controls:
  - bounded in-memory queue
  - batch flush to DB
  - dropped-event counters if queue overflows

### 6.3 Enrichment and metrics engine
Purpose: compute all V1 derived fields and chip-relevant metrics from raw flow + reference data.

Pieces to implement:
- Enrichment pipeline (`src/enrichment/pipeline.js`) triggered by new raw rows.
- Quote/spot resolver (`src/enrichment/quote-join.js`): nearest underlying quote at/before trade time.
- Contract/day stats updater (`src/enrichment/contract-stats.js`):
  - `dayVolume`
  - latest `oi` snapshot
  - `volOiRatio`
- Symbol/window stats updater (`src/enrichment/symbol-stats.js`):
  - rolling 1m and 15m baselines
  - open-window baseline for AM spike
- Formula engine (`src/enrichment/formulas.js`) implementing project-goal formulas:
  - `value = price * size * 100`
  - `dte = ceil((expirationDateET - tradeTsET)/86400)`
  - `otmPct` by right/strike/spot
  - `repeat3m` trailing 180s same contract+side count
  - sentiment map and `sigScore` weighted calculation
- Timezone/session service (`src/market/session-clock.js`) pinned to `America/New_York`.

### 6.4 Rule/chip engine
Purpose: evaluate deterministic chip flags with versioned thresholds/heuristics.

Pieces to implement:
- Rule registry (`src/rules/catalog.js`) listing all chips:
  - Execution chips: `Calls`, `Puts`, `Bid`, `Ask`, `AA`
  - Value/size chips: `100k+`, `Whales`, `Sizable`, `Large Size`
  - Advanced chips: `OTM`, `LEAPS`, `Weeklies`, `Vol>OI`, `Repeat Flow`, `Rising Vol`, `AM Spike`, `High Sig`, `Unusual`, `Urgent`, `Bullflow`, `Position Builders`, `Grenade`
- Rule evaluator (`src/rules/engine.js`) consuming enriched row + aggregate context.
- Versioned config loader (`src/rules/config-store.js`):
  - active version pointer
  - checksum
  - rollout metadata
- Persist evaluated chip flags into enriched store for fast query filtering.

### 6.5 Query API layer
Purpose: serve UI-ready rows and filtering semantics within latency target.

Pieces to implement:
- Replace fixture-driven query path in `src/flow.js` with repository-backed execution.
- Endpoint contracts:
  - Extend `GET /api/flow` response fields:
    - `spot`, `dte`, `otmPct`, `dayVolume`, `oi`, `volOiRatio`, `repeat3m`, `sigScore`, `sentiment`, `chips`
  - Extend `GET /api/flow` query params:
    - `chips`, `side`, `type`, `sentiment`, `minSigScore`, `maxSigScore`, `minDte`, `maxDte`, `minOtmPct`, `maxOtmPct`, `minVolOi`, `minRepeat3m`
    - preserve existing `minValue`, `maxValue`, `right`, `expiration`, pagination
  - Add `GET /api/flow/summary`
  - Add `GET /api/flow/filters/catalog`
- Query planning/index-aware filtering:
  - pre-filter by high-selectivity columns
  - cursor pagination stability on `(sort_key, id)` composite
- Fallback mode contract:
  - when enrichment lag > 30s, return raw rows + explicit `meta.degraded=true` and reason.

### 6.6 Live stream delivery
Purpose: low-latency incremental updates to UI.

Pieces to implement:
- Server SSE endpoint (`/api/flow/stream`) backed by enriched row updates.
- Sequence + watermark emitted in every payload for client dedupe/replay.
- Reconnect behavior compatible with existing client controller in `src/flow-live.js`.

### 6.7 Saved filters and alerts
Purpose: maintain backward compatibility while adopting V2 DSL.

Pieces to implement:
- Persisted store (`saved_queries`) replacing in-memory maps.
- Continue `legacy <-> queryDslV2` conversion path in `src/saved-filters-alerts.js`.
- Add catalog-aware validation to reject unknown fields/chips at write time.

### 6.8 Feature flags and shadow rollout
Purpose: safe migration with measurable diffs.

Pieces to implement:
- Feature flag key: `FLOW_FILTERS_V2`.
- Shadow comparator job (`src/shadow/live-compare.js`):
  - compute old/new chip outputs in parallel
  - write per-session diff artifacts
- Rollout phases:
  1. shadow-only for 3+ sessions
  2. enable core chips
  3. progressive enable advanced chips

### 6.9 Observability and operations
Purpose: enforce SLOs and diagnose failures quickly.

Pieces to implement:
- Metrics namespace:
  - `ingest_events_total`, `ingest_parse_failures_total`, `ingest_lag_seconds`
  - `enrichment_latency_ms`, `enrichment_backlog_size`
  - `api_flow_latency_ms`, `api_flow_error_total`
  - `filter_hits_total{chip=...}`
- Structured logs with request id + watermark + filter version.
- Health/readiness:
  - `/health`: process alive
  - `/ready`: Theta connectivity, DB ready, enrichment backlog below threshold
- Alerting:
  - ingest lag p95 > 5s
  - query p95 > 350ms for benchmark profile
  - parse failure rate threshold

## 7) Data Model (Target)

### 7.1 `option_trades` (raw source)
Columns (minimum):
- `trade_id` (PK, deterministic hash)
- `trade_ts_utc`, `trade_ts_et`
- `symbol`, `expiration`, `strike`, `right`
- `price`, `size`, `bid`, `ask`
- `condition_code`, `exchange`, `raw_payload_json`
- `ingested_at_utc`, `watermark`

Indexes:
- `(trade_ts_utc DESC)`
- `(symbol, trade_ts_utc DESC)`
- `(symbol, expiration, strike, right, trade_ts_utc DESC)`

### 7.2 `option_trade_enriched`
Columns:
- `trade_id` (PK/FK -> option_trades)
- computed: `value`, `dte`, `spot`, `otm_pct`, `day_volume`, `oi`, `vol_oi_ratio`, `repeat3m`, `sig_score`, `sentiment`
- chip booleans/materialized array
- `rule_version`, `enriched_at_utc`

Indexes:
- `(enriched_at_utc DESC)`
- `(symbol, enriched_at_utc DESC)`
- `(sig_score DESC, trade_id)`
- partial/composite indexes for common filter combinations (e.g. `vol_oi_ratio`, `repeat3m`, chip flags)

### 7.3 `contract_stats_intraday`
Key: `(symbol, expiration, strike, right, session_date)`

Columns:
- `day_volume`
- `oi`
- `last_trade_ts`
- `updated_at`

### 7.4 `symbol_stats_intraday`
Key: `(symbol, minute_bucket_et)`

Columns:
- `vol_1m`
- `vol_baseline_15m`
- `open_window_baseline`
- `bullish_ratio_15m`
- `updated_at`

### 7.5 `filter_rule_versions`
Columns:
- `version_id` (PK)
- `config_json`
- `checksum`
- `is_active`
- `created_at`, `activated_at`

### 7.6 `ingest_checkpoints`
Columns:
- `stream_name` (PK)
- `watermark`
- `updated_at`

### 7.7 `saved_queries`
Columns:
- `id` (PK)
- `kind` (`preset|alert`)
- `name`
- `payload_version`
- `query_dsl_v2_json`
- `created_at`, `updated_at`

## 8) ThetaData API Mapping (Entitlement-Aware)

### 8.1 Confirmed safe endpoints from current guide
- `/v3/stock/list/symbols?format=json`
- `/v3/stock/snapshot/quote?symbol=...&format=json`
- `/v3/option/list/roots?format=json`

### 8.2 Endpoint classes to validate in M0 before implementation
- Option trade ingestion endpoint(s) for live/historical flow used by worker.
- OI/reference endpoint(s) needed for `oi` and `volOiRatio`.
- Underlying quote lookup endpoint(s) for `spot` alignment at trade timestamp.

Rule: each endpoint added to production path must pass the MON-79/MON-86 preflight checklist and be documented with:
- exact URL shape
- expected response contract
- entitlement behavior (success vs permission error)

### 8.3 Known likely entitlement boundaries
Based on current guide, plan for failure/denial on higher-tier APIs (e.g., advanced greeks/NBBO/full tick history). The architecture must not depend on those for V1 critical path.

## 9) Performance and Reliability Design
- Ingest-to-UI p95 <= 5s:
  - async ingestion + bounded enrichment backlog
  - precomputed chip flags + indexed query path
- `/api/flow` p95 <= 350ms (limit=50, 3 filters):
  - avoid live Theta calls on read path
  - enforce query plans via index hints/EXPLAIN checks in CI
- Deterministic behavior:
  - formula engine pure functions covered by high unit-test density
  - version pinning for chip configs
- Degradation mode:
  - explicit raw-mode response when enrichment lag threshold breached

## 10) Security and Configuration
- Secrets are not embedded in app code; ThetaTerminal credentials remain in local creds file.
- All Theta host/paths come from env parsing module with strict validation.
- Request validation on API filters to prevent malformed/unbounded queries.
- Limit/timeout guards for ingestion and API handlers.

## 11) Test Strategy (Mapped to Goals)

### 11.1 Unit
- Metric formulas (`value`, `dte`, `otmPct`, `repeat3m`, sentiment, `sigScore`).
- Rule boundaries (`AA` threshold, `Vol>OI`, `Urgent`, `Grenade`, etc.).
- Saved payload compatibility (`legacy <-> v2`).

### 11.2 Integration
- fixture/replay ingest -> enrichment -> `/api/flow` expected IDs/chips.
- repeat-flow `20 in 3 mins` scenario.
- cursor pagination stability under sort/filter combinations.

### 11.3 End-to-end and operational
- Theta preflight + smoke (`MON-79`, `MON-86`) before runtime tests.
- top-200 stream simulation for ingest lag and API p95.
- shadow comparison for 3 market sessions minimum.

## 12) Milestone Implementation Plan (M0-M5)

### M0 Spec and contracts
Deliver:
- Endpoint matrix for Theta APIs actually used in V1.
- Final chip dictionary + config schema.
- Fixture data contract for deterministic tests.

### M1 Ingestion and storage
Deliver:
- ingestion worker + checkpoint resume.
- raw trade persistence.
- aggregate tables and update jobs.

### M2 Metrics and rule engine
Deliver:
- formula engine + enrichment pipeline.
- configurable/versioned rule evaluator.
- chip persistence on enriched rows.

### M3 API and query layer
Deliver:
- upgraded `GET /api/flow` fields + params.
- `GET /api/flow/summary`.
- `GET /api/flow/filters/catalog`.

### M4 UI integration
Deliver:
- API-backed chip behavior.
- full filter drawer contract.
- saved preset state parity with server DSL.

### M5 hardening
Deliver:
- replay/perf validation.
- observability dashboards and alerting.
- staged rollout from shadow to enabled.

## 13) Proposed Repository Structure Changes

New backend modules:
- `src/config/env.js`
- `src/thetadata/client.js`
- `src/ingest/worker.js`
- `src/ingest/normalize.js`
- `src/ingest/checkpoint-store.js`
- `src/enrichment/pipeline.js`
- `src/enrichment/formulas.js`
- `src/enrichment/contract-stats.js`
- `src/enrichment/symbol-stats.js`
- `src/rules/catalog.js`
- `src/rules/engine.js`
- `src/rules/config-store.js`
- `src/data/repositories/*`
- `src/flow/queries.js`
- `src/flow/summary.js`
- `src/flow/catalog.js`
- `src/shadow/live-compare.js`

Operational scripts:
- keep `scripts/mon79_*` and `scripts/mon86_thetadata_bootstrap.py` as preflight gates
- add `scripts/flow-replay-benchmark.js` for p95 checks

## 14) Definition of Done Checklist
A release candidate is ready only when all are true:
- ingest-to-UI lag <= 5s p95 during market hours simulation.
- `GET /api/flow` p95 <= 350ms (`limit=50`, 3 filters).
- deterministic formula/rule unit coverage >= 95%.
- repeat-flow integration scenario passes.
- UI chips + drawer produce server-consistent filtered rows.
- shadow rollout report covers >=3 sessions with acceptable deltas.
- fallback mode tested for enrichment lag breach.

## 15) Open Decisions to Lock Early
1. Exact Theta endpoint(s) and payload schema for live option trade ingest under current entitlement.
2. Database baseline:
   - local/dev: SQLite at `data/phenixflow.sqlite` (selected)
   - production: engine selection remains open.
3. Rule config storage authority (DB-backed only vs DB + file bootstrap).
4. Whether `/api/flow/stream` is true SSE server push in V1 or poll-compatible transitional API.

These decisions should be finalized in M0 to avoid rework in M1-M3.
