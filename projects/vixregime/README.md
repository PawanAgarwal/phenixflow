# VIXRegime

This workspace contains the SPX/VIX-family regime classifier and ETF allocation baseline.

## What It Does

- Validates coverage for the required symbols:
  - `SPX`, `SPY`, `SPXL`, `SPXS`
  - `VIX`, `VIX1D`, `VIX3M`, `VIX9D`
- Builds day-level and causal minute-level features from `options.stock_ohlc_minute_raw`
- Classifies each bar into:
  - `Calm`
  - `Normal`
  - `Stress`
  - `Crash`
- Maps regime exposure into `SPY` / `SPXL` / `SPXS` / cash
- Runs next-bar backtests for:
  - daily execution
  - minute execution

## Files

- `src/vix-regime.js`: feature, regime, coverage, and backtest helpers
- `scripts/check-required-data.js`: live ClickHouse coverage gate
- `scripts/backtest-spx-vix-regime.js`: feature build + classification + backtest artifact
- `config/vix-regime-thresholds.json`: rule thresholds and execution defaults
- `config/vix-regime-required-universe.json`: required symbol universe
- `test/vix-regime.test.js`: unit coverage for the helper layer

## Run

From repo root:

```bash
node projects/vixregime/scripts/check-required-data.js
node projects/vixregime/scripts/backtest-spx-vix-regime.js
```

Or use:

```bash
npm run vixregime:check
npm run vixregime:backtest
```

## Output

Default artifacts are written under:

```bash
projects/vixregime/artifacts/reports/
```

The backtest artifact contains:

- `coverage`
- `summaries.daily`
- `summaries.minute`
- `daily.features`
- `daily.observations`
- `minute.features`
- `minute.observations`

## Current Baseline Result

Latest successful run on the current dataset produced:

- Daily:
  - ending equity `0.8887`
  - benchmark ending equity `1.1108`
  - max drawdown `-17.55%`
- Minute:
  - ending equity `0.9243`
  - benchmark ending equity `1.1008`
  - max drawdown `-13.51%`

These numbers are baseline outputs from the current ruleset, not tuned production targets.
