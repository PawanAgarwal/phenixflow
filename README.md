# phenixflow

Monorepo for Project phenixflow apps, market-data research, and strategy experiments.

## Master Data Source Policy

All new market and strategy analysis uses Massive data only. The active SPY intraday research plan
is [projects/spy-intraday-prediction/PLAN.md](/Users/pawanagarwal/github/phenixflow/projects/spy-intraday-prediction/PLAN.md).

Canonical local data roots:

- Historical Massive cache: `/Volumes/SEC4TB/massive-data/massive/`
- Intraday/live Massive parquet: `/Volumes/SEC4TB/massive-data/massive-live/parquet/massive/`
- Calendar cache: `/Volumes/SEC4TB/massive-data/calendar/us-equities-options-calendar.json`

New analysis should not use legacy database backfills, `options.*` table names, or Theta-derived
historical tables.

## Layout

- `apps/flow-api`: the main options-flow backend app and operational scripts.
- `projects/spy-intraday-prediction`: Massive-only SPY intraday prediction research.
- `projects/vixregime`: SPX/VIX regime research project.
- `projects/podcast-prediction`: podcast extraction and prediction workflows.
- `projects/yieldmax`: YieldMax-related screening and analysis outputs.
- `docs`: shared architecture and operational documentation.
- `docs/strategies`: live strategy-service documentation for each tracked study.
- `artifacts`, `data`, `output`: repo-level runtime assets and generated outputs.

## Common Commands

```bash
npm start
npm test
npm run lint
npm run parquet:massive:options:1m
npm run spy-intraday:coverage -- --start-date 2026-01-02 --end-date 2026-04-27
npm run spy-intraday:build-dataset -- --start-date 2026-01-02 --end-date 2026-04-27
npm run spy-intraday:research -- --dataset <dataset-jsonl>
npm run spy-intraday:phase2 -- --dataset <dataset-jsonl>
npm run spy-intraday:validate-signals
```

The root `package.json` keeps repo-level entry points stable while app and project code live under
their own folders.
