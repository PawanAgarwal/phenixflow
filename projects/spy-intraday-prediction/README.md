# SPY Intraday Prediction

Massive-only SPY intraday prediction and backtesting project.

The master plan is [PLAN.md](/Users/pawanagarwal/github/phenixflow/projects/spy-intraday-prediction/PLAN.md).
Use that file for the official protocol, data roots, feature universe, validation gates, and current
results.

## Data Policy

Use only Massive local files/manifests for this project:

- Historical Massive cache: `/Volumes/SEC4TB/massive-data/massive/`
- Intraday/live Massive parquet: `/Volumes/SEC4TB/massive-data/massive-live/parquet/massive/`
- Calendar cache: `/Volumes/SEC4TB/massive-data/calendar/us-equities-options-calendar.json`

Do not read legacy databases, `options.*` table names, or Theta-derived historical tables for new
analysis.

## Common Commands

```bash
npm run spy-intraday:coverage -- --start-date 2026-01-02 --end-date 2026-04-27
npm run spy-intraday:build-dataset -- --start-date 2026-01-02 --end-date 2026-04-27
npm run spy-intraday:research -- --dataset <dataset-jsonl>
npm run spy-intraday:phase2 -- --dataset <dataset-jsonl>
npm run spy-intraday:validate-signals
npm run spy-intraday:best-signal-full-history
```
