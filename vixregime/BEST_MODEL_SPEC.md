# Best VIXRegime Model Spec

Last updated: 2026-03-21

## Purpose

This file captures the current best-performing `vixregime` model and action policy so we do not lose the exact setup while iterating.

## Model Family

- Type: ordinal logistic classifier
- Resolution: daily
- Target: classify `SPY close X -> close X+1` into:
  - `Crash`
  - `Stress`
  - `Normal`
  - `Calm`

The model is trained in 3 stages:

1. `Calm` vs rest
2. `Stress/Crash` vs rest
3. `Crash` vs `Stress`

## Best Label Definition

- `Crash <= -2.5%`
- `Stress (-2.5%, -0.75%]`
- `Normal (-0.75%, +0.75%)`
- `Calm > +0.75%`

## Best Feature Set

Selected feature groups:

- `volCore`
- `priceTrend`

This means the live model uses only:

- `vix`
- `vixPctRank`
- `delta5`
- `delta10`
- `ts9d30`
- `ts30d90d`
- `ts1d9d`
- `vix1dOverVix`
- `spyRet1`
- `spyRet5`
- `spyRet10`
- `spyReturn1d`
- `spyReturn3d`
- `spyReturn5d`
- `spyReturn10d`
- `spyMaGap5`
- `spyMaGap10`
- `spyMaGap20`

Important note:
- Event-day features and larger feature stacks were tested, but the smaller `volCore + priceTrend` model generalized better on forward accuracy.

## Best Hyperparameters

- `featureMode = base`
- `learningRate = 0.015`
- `reg = 0.0005`
- `epochs = 350`
- `upPosMultiplier = 1`
- `downPosMultiplier = 1`
- `crashPosMultiplier = 1.5`
- `upThreshold = 0.45`
- `downThreshold = 0.6`
- `crashThreshold = 0.5`

## Current Chosen Policy

Policy name:

- `conservativeCash`

Mapping:

- `Calm -> SPXL`
- `Normal -> SPY`
- `Stress -> CASH`
- `Crash -> CASH`

## Training / Evaluation Windows Used For Current Best Spec

From `vixregime-feature-subset-ordinal-model-search.json`:

- Train: `2025-03-19` to `2025-06-17`
- Selection: `2025-06-18` to `2025-09-17`
- Holdout 1: `2025-09-18` to `2025-12-16`
- Holdout 2: `2025-12-17` to `2026-03-19`

## Forward Classification Performance

Current best feature-subset ordinal model:

- Selection accuracy: `77.36%`
- Holdout 1 accuracy: `52.83%`
- Holdout 2 accuracy: `50.94%`

## Forward Strategy Performance

Using `conservativeCash`:

- Combined forward windows:
  - ending equity: `1.1289`
  - total return: `+12.89%`
  - `SPY` benchmark: `+1.30%`

- Holdout 1:
  - strategy: `+0.27%`
  - `SPY`: `+0.57%`

- Holdout 2:
  - strategy: `+5.79%`
  - `SPY`: `-4.90%`
  - relative edge: `+10.69%`

## Principal Factors

Highest overall drivers in the trained model:

1. `ts1d9d`
2. `vix1dOverVix`
3. `vixPctRank`
4. `delta10`
5. `spyRet5` / `spyReturn5d`
6. `spyReturn3d`
7. `spyRet1` / `spyReturn1d`
8. `spyMaGap5`

Interpretation:

- Short-end volatility term structure is the dominant signal.
- Recent SPY trend and distance from short moving averages are the main price-state context.
- Raw event flags were not part of the final best spec.

## Key Files

- Model search: [scripts/search-feature-subset-ordinal-models.js](/Users/pawanagarwal/github/phenixflow/vixregime/scripts/search-feature-subset-ordinal-models.js)
- Policy backtest: [scripts/backtest-feature-subset-ordinal-policies.js](/Users/pawanagarwal/github/phenixflow/vixregime/scripts/backtest-feature-subset-ordinal-policies.js)
- Next-day predictor: [scripts/predict-next-day-feature-subset-ordinal.js](/Users/pawanagarwal/github/phenixflow/vixregime/scripts/predict-next-day-feature-subset-ordinal.js)
- Best model artifact: [vixregime-feature-subset-ordinal-model-search.json](/Users/pawanagarwal/github/phenixflow/vixregime/artifacts/reports/vixregime-feature-subset-ordinal-model-search.json)
- Policy artifact: [vixregime-feature-subset-ordinal-policy-backtest.json](/Users/pawanagarwal/github/phenixflow/vixregime/artifacts/reports/vixregime-feature-subset-ordinal-policy-backtest.json)

## Current Operational Note

- The local ClickHouse dataset currently covers `2025-01-02` onward for this workflow.
- A true `2024` month-by-month validation will require backfilling the required symbols for `2024` into `options.stock_ohlc_minute_raw`.
