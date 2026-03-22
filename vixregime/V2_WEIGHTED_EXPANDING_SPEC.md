# V2 Weighted Expanding Walk-Forward Spec

Last updated: 2026-03-21

## Purpose

This document captures Version 2 of the `vixregime` model:

- expanding walk-forward training
- all prior labeled data retained
- recency weighting
- extra emphasis on `Stress` and `Crash`

This is separate from Version 1 and does not replace it.

## Version Definition

- Version name: `v2_weighted_expanding_walkforward`
- Base model family: ordinal logistic classifier
- Policy: `conservativeCash`

Mapping:

- `Calm -> SPXL`
- `Normal -> SPY`
- `Stress -> CASH`
- `Crash -> CASH`

## Base Label Definition

- `Crash <= -2.5%`
- `Stress (-2.5%, -0.75%]`
- `Normal (-0.75%, +0.75%)`
- `Calm > +0.75%`

## Base Feature Set

Selected groups:

- `volCore`
- `priceTrend`

Feature list:

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

## Training Method

For each test month:

1. Use **all prior labeled rows** as the training set
2. Weight older rows down exponentially
3. Weight `Stress` and `Crash` rows up
4. Retrain monthly
5. Test only on the next calendar month

## Best Tested Weighting So Far

- `minTrainRows = 63`
- `halfLifeRows = 189`
- class weights:
  - `Calm = 1.0`
  - `Normal = 1.0`
  - `Stress = 2.5`
  - `Crash = 5.0`

## Base Model Hyperparameters

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

## Current V2 Results

Artifact:

- [vixregime-monthly-walkforward-v2-weighted-expanding.json](/Users/pawanagarwal/github/phenixflow/vixregime/artifacts/reports/vixregime-monthly-walkforward-v2-weighted-expanding.json)

Aggregate monthly walk-forward:

- classification accuracy: `60.81%`
- strategy return: `+51.24%`
- `SPY` benchmark: `+16.70%`
- relative edge: `+34.54%`

## 2026 Monthly Results

- `2026-01`
  - accuracy: `70.0%`
  - strategy: `+0.39%`
  - `SPY`: `+1.78%`
  - relative edge: `-1.40%`

- `2026-02`
  - accuracy: `31.6%`
  - strategy: `-4.91%`
  - `SPY`: `-1.39%`
  - relative edge: `-3.52%`

- `2026-03`
  - accuracy: `21.4%`
  - strategy: `-1.84%`
  - `SPY`: `-5.25%`
  - relative edge: `+3.41%`

## Comparison To Version 1

Version 1 remains the chosen operational model because:

- it is simpler
- it produced a stronger frozen-holdout March 2026 result
- it avoids monthly retrain drift

Version 2 is valuable because:

- it uses all past data instead of throwing older history away
- it is much better than the naive monthly 63-row rolling retrain
- it provides a more realistic path if we want recurring model refreshes

## Files

- Evaluator: [evaluate-monthly-walkforward-v2-weighted-expanding.js](/Users/pawanagarwal/github/phenixflow/vixregime/scripts/evaluate-monthly-walkforward-v2-weighted-expanding.js)
- Version 1 spec: [BEST_MODEL_SPEC.md](/Users/pawanagarwal/github/phenixflow/vixregime/BEST_MODEL_SPEC.md)
