# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
npm start                   # Start flow-api server (port 3000, override with PORT env var)
npm test                    # Run Vitest tests
npm run test:watch          # Watch mode
npm run lint                # ESLint

# Data operations
npm run backfill:*          # Historical backfill operations
npm run enrich:*            # Enrichment pipelines
npm run clickhouse:*        # ClickHouse DB management (start/stop/status/schema/import)
npm run sigscore:calibrate  # Calibrate scoring model
npm run rules:activate      # Activate rule versions
npm run oi:sync             # Open Interest sync

# Research / sub-projects
npm run vixregime:check
npm run vixregime:backtest
```

## Monorepo Structure

```
apps/flow-api/      # Core options flow backend (Express API + scoring engine)
packages/
  clickhouse-core/  # Shared ClickHouse client wrapper
  theta-client/     # Shared ThetaData HTTP client
infra/clickhouse/   # ClickHouse install scripts, schema init, data migration
projects/
  vixregime/        # VIX regime detection and backtesting
  podcast-prediction/
  yieldmax/
scripts/            # Root-level operational scripts
```

## Architecture

Phenixflow is a **deterministic, cache-first options flow backend** that computes `sigScore` (unusual options activity signal) using versioned rules and enriched market context, designed for 1–5 day swing trading signals.

**Data flow:**
```
ThetaData (historical + live)
  → option_trades (raw cache)
  → enrichment engine (Greeks, IV, aggregates)
  → option_trade_enriched (scored rows)
  → REST API (/api/flow/*)
```

### Key Source Files (under `apps/flow-api/`)

| File | Purpose |
|------|---------|
| `app.js` | Express route handlers |
| `server.js` | Server entry point |
| `historical-flow.js` | Core enrichment + scoring engine (~12k LOC) |
| `flow.js` | Query filtering, chip logic, sentiment |
| `historical-formulas.js` | Metric computation formulas |
| `storage/` | ClickHouse + SQLite abstraction |
| `ingest/` | Continuous ingestion worker + checkpoint logic |
| `scoring/` | Scoring sub-modules |
| `thetadata/` | ThetaData API integration |

### Scoring Models

Three models: `v1_baseline`, `v4_expanded`, `v5_swing`. The active production target is **v5_swing** with 14 component norms:

`valueShockNorm`, `volOiNorm`, `repeatNorm`, `otmNorm`, `dteSwingNorm`, `flowImbalanceNorm`, `deltaPressureNorm`, `cpOiPressureNorm`, `ivSkewSurfaceNorm`, `ivTermSlopeNorm`, `underlyingTrendConfirmNorm`, `liquidityQualityNorm`, `sweepNorm`, `multilegPenaltyNorm`

- Score range: `[0, 1]`; quality: `complete` (all metrics) or `partial` (some missing)
- Missing components are excluded and remaining weights renormalized
- Explainability via `sigScoreComponents` JSON on each enriched row

### Primary Database Tables

`option_trades`, `option_quote_minute_raw`, `stock_ohlc_minute_raw`, `option_trade_enriched`, `option_calculated_greeks_minute`, `contract_stats_intraday`, `symbol_stats_intraday`, `filter_rule_versions`, `saved_queries`, `ingest_checkpoints`

Chunk status tables: `option_download_chunk_status`, `option_enrich_chunk_status`

### Operational Constraints

- **ThetaData concurrency**: max 4 concurrent connections
- **Backfill**: gate-based — complete day D (download + enrich + verify) before advancing to D+1
- **Memory**: target ≤10GB RSS, use streaming reads/writes with bounded queues
- **Idempotency**: resume from last completed minute; delete only the resumed minute's scope
- **Gap analysis**: distinguish `unattempted` vs `attempted_missing` (only the latter counts as failure)

### API Conventions

- Both `/api` and `/api/v1` paths are supported (backward compatible)
- Timestamps: ISO-8601 UTC
- Pagination: `limit` (default 25 for `/flow`, 100 for `/historical`, max 100/1000)
- Comma-separated filter lists: e.g., `chips=calls,100k+`
- SSE: `transport=sse` or `Accept: text/event-stream`
- Filter versioning: `filterVersion=legacy|candidate`

### Code Conventions

- CommonJS modules (`module.exports`) throughout
- Unused variable prefix convention: `_varName` (enforced by ESLint)
- Tests live in `apps/flow-api/test/`

### Agent Workflow

See `AGENT_PROTOCOL.md` for the PM-led agent coordination contract and `AGENTS.md` for the operational backfill runbook. Task queue is tracked in `PM_QUEUE.md` (git-backed).

### Documentation

- `docs/PHENIX_ARCHITECTURE.md` — complete end-to-end system design
- `docs/PHENIX_API_SPEC.md` — endpoint matrix and response schemas
- `docs/PHENIX_PROJECT_GOALS.md` — v5_swing mission and calibration goals
- `docs/BACKFILL_RUNTIME_PARAMETERS.md` — canonical backfill runtime settings
- `docs/BACKFILL_OPERATIONAL_LEARNINGS.md` — failure signatures and remediation patterns
