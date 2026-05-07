# CLAUDE.md

This file provides guidance to Claude Code when working with code in this repository.

## Current Policy

All new market and strategy analysis uses Massive data only. Treat the root `README.md`,
`AGENTS.md`, and `projects/spy-intraday-prediction/PLAN.md` as the active guidance.

Do not route new analysis through legacy database backfills, `options.*` table names, or
Theta-derived historical tables.

## Commands

```bash
npm start                   # Start flow-api server (port 3000, override with PORT env var)
npm test                    # Run Vitest tests
npm run test:watch          # Watch mode
npm run lint                # ESLint

# Massive flat files
npm run parquet:massive:options:1m

# SPY intraday research
npm run spy-intraday:coverage
npm run spy-intraday:build-dataset
npm run spy-intraday:research
npm run spy-intraday:phase2
npm run spy-intraday:validate-signals
npm run spy-intraday:best-signal-full-history
npm run spy-intraday:python-export
npm run spy-intraday:python-research
npm run spy-intraday:backtest

# Other repo projects
npm run vixregime:check
npm run vixregime:backtest
```

## Monorepo Structure

```text
apps/flow-api/                    # Core options flow backend
packages/
  theta-client/                   # Shared ThetaData HTTP client for legacy app flows
projects/
  spy-intraday-prediction/        # Massive-only SPY intraday research
  vixregime/                      # VIX regime detection and backtesting
  podcast-prediction/
  yieldmax/
scripts/                          # Root-level operational scripts
```

## SPY Intraday Research

The Massive-only project predicts and backtests SPY intraday movement using local flat-file/parquet
data. See `projects/spy-intraday-prediction/PLAN.md` for the sealed protocol, data roots, feature
universe, validation gates, and current results.

Key rules:

- Coverage checks inspect Massive files/manifests only.
- Feature rows must be causal: use only data at or before the prediction minute.
- Official results train on January 2026 and test February, March, and April through `2026-04-27`.
- Sensitivity tracks using longer history must be reported separately from official results.
- Guardrail tests should fail if project code imports forbidden data-source helpers or embeds
  forbidden schema table references.

## Flow API

The flow API remains the live backend for options-flow endpoints and scoring. Prefer project-level
Massive data paths for new research work unless the user explicitly asks for live API maintenance.

Key source files under `apps/flow-api/`:

| File | Purpose |
|------|---------|
| `app.js` | Express route handlers |
| `server.js` | Server entry point |
| `historical-flow.js` | Historical enrichment and scoring engine |
| `flow.js` | Query filtering, chip logic, sentiment |
| `historical-formulas.js` | Metric computation formulas |
| `ingest/` | Continuous ingestion worker and checkpoint logic |
| `scoring/` | Scoring sub-modules |
| `thetadata/` | ThetaData API integration for legacy app flows |

## Code Conventions

- CommonJS modules (`module.exports`) throughout.
- Unused variable prefix convention: `_varName` (enforced by ESLint).
- Tests live near the relevant app or project.
