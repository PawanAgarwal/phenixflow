# SPY Intraday Prediction: Massive-Only Research Plan

## Summary

This project predicts and backtests SPY intraday movement using only Massive data. The official
protocol remains fixed:

- Train: January 2026.
- Test: February 2026, March 2026, and April 2026 through `2026-04-27`.
- Treat `2026-04-28` as intraday/provisional only.

Hard rule: use Massive only. Do not read legacy database helpers, query `options.*`, or use
Theta-derived tables.

The research suite also has a sensitivity track that trains on `2025-01-02` through `2026-01-30`
to test whether stronger techniques need more history. Sensitivity results must be reported
separately from official January-only results.

## Data Roots

Use:

- Historical Massive cache: `/Volumes/SEC4TB/massive-data/massive/`
- Intraday/live Massive parquet: `/Volumes/SEC4TB/massive-data/massive-live/parquet/massive/`

Historical datasets:

- `stock_quotes_1m`: SPY, market ETFs, sector ETFs, risk/factor ETFs, and Mag7 1m bars.
- `indices_1m`: SPX/VIX-family 1m bars.
- `option_quotes_1m`: OPRA option 1m aggregates.
- `option_trades_all`: OPRA option trades.

## Research Roadmap

Evidence-ranked implementation order:

1. Cross-sectional 5m/60m models.
2. Last-hour/EOD intraday momentum.
3. Opening option-flow score.
4. 0DTE/gamma-regime filters.
5. Triple-barrier labels + meta-labeling.
6. Volatility/magnitude prediction for abstention and sizing.

The first profitability gate is SPY underlying long/cash/short backtests. Option strategy
backtests are deferred until a signal passes robustness checks on SPY.

### Phase 1 Status

Completed on the Massive-only dataset through `2026-04-27`:

- Built the full 2025-plus dataset with option-derived features.
- Ran cross-sectional, EOD momentum, opening option-flow proxy, gamma-regime, meta-labeling, and
  volatility/magnitude experiments in the Node research suite.
- Added horizon-aware policy accounting so 5m/60m/EOD backtests do not overcount overlapping
  minute predictions.

Result: no strategy passed the formal "promising" gate. The strongest evidence was not directional;
it was magnitude/volatility prediction, especially `high_abs_return_5m`, `high_abs_return_30m`,
`high_abs_return_60m`, and `high_abs_return_eod`.

Not completed in Phase 1:

- The optional Python sklearn track did not run real models because local Python dependencies were
  missing.
- Volatility-gated directional trading, regime diagnostics, validation-selected thresholds, enhanced
  meta-labeling, and walk-forward retraining were not part of the first full report.

### Phase 2 Follow-Up Tracks

Implement and report these as separate Phase 2 artifacts so the original Jan-only protocol remains
readable:

1. Volatility-gated directional trading.
   - Train a directional model and a separate magnitude model.
   - Take SPY long/cash/short trades only when predicted move magnitude clears a validation-selected
     threshold.
   - Test 5m, 60m, last-30m, and EOD variants.
2. Regime diagnostics.
   - Break prediction and policy results down by VIX/VIX1D/VIX9D levels, realized volatility,
     time-of-day, opening-range direction, option-flow imbalance, and gamma proxy buckets.
   - Use diagnostics to find where edge exists instead of averaging all regimes together.
3. Enhanced meta-labeling.
   - Rebuild meta-labeling as a take/skip model for a base directional signal.
   - Include base confidence, base predicted return, magnitude probability, VIX regime, realized
     volatility, opening range, time of day, and option/gamma proxy features.
4. Validation-selected policy thresholds.
   - Select confidence and magnitude thresholds using training-window validation only.
   - Report the selected thresholds next to February, March, and April holdout performance.
5. Python sklearn track.
   - Create a compact feature export to avoid loading the full 2.5 GB JSONL into pandas.
   - Use a local runtime virtual environment with `numpy`, `pandas`, and `scikit-learn` when
     available.
   - Run logistic regression, random forest, and histogram gradient boosting on the same formal
     splits.
6. Walk-forward training.
   - Keep Jan-only official results separate.
   - Add an expanding walk-forward sensitivity track:
     - train through January, test February
     - train through February, test March
     - train through March, test April through `2026-04-27`

### Phase 2 Status

Completed:

- Added `spy-intraday:phase2` and ran the full Phase 2 suite on the full Massive-only dataset.
- Added volatility-gated directional policies with validation-selected magnitude thresholds.
- Added confidence-threshold policy sweeps using training-window validation only.
- Added regime diagnostics to Phase 2 result records.
- Added enhanced meta-labeling that uses base signal confidence, predicted return, and magnitude
  probability to make take/skip decisions.
- Added expanding walk-forward train/test runs.
- Added compact Python export and ran the sklearn research track in a local runtime virtualenv.

Phase 2 artifacts:

- Node Phase 2 report: `artifacts/full-phase2-suite-2025-01-02-2026-04-27.json`
- Node Phase 2 predictions: `runtime/full-phase2-predictions-2025-01-02-2026-04-27.jsonl`
- Python compact dataset: `runtime/spy-intraday-python-compact-2025-01-02-2026-04-27.jsonl`
- Python sklearn report: `artifacts/full-python-sklearn-research-2025-01-02-2026-04-27.json`

Most promising Phase 2 result:

- Walk-forward volatility-gated gamma-regime `next_60m`:
  - February: `+1.508%` net, `-0.645%` max drawdown.
  - March: `+1.623%` net, `-3.192%` max drawdown.
  - April through `2026-04-27`: `+1.005%` net, `-0.368%` max drawdown.
  - Combined simple monthly sum: `+4.136%` net.
  - Beat buy-and-hold in two of three holdout months.

Other interesting Phase 2 result:

- History-plus volatility-gated opening option-flow EOD proxy:
  - Positive in February, March, and April.
  - Combined simple monthly sum: `+2.902%` net.
  - This remains a proxy signal; Massive data here does not identify true buyer/seller initiation.

Current caution:

- These are research results, not production-trading approval. The best results need follow-up
  robustness checks for threshold stability, single-day contribution, execution sensitivity, and
  delayed/slippage-stressed fills.

### Phase 3 Validation Plan

Validate the Phase 2 "promising" set before trusting any signal. The Phase 2 screen produced 15
promising train/method variants, which collapse into these signal families:

1. Volatility-gated gamma-regime `next_60m`.
2. Volatility-gated opening option-flow EOD proxy.
3. Volatility-gated cross-sectional `next_60m`.
4. Confidence-threshold gamma-regime `next_60m`.
5. Enhanced-meta gamma-regime `next_60m`.
6. EOD momentum threshold/volatility-gated EOD variants.
7. EOD momentum threshold last-30m variants.
8. Walk-forward duplicates of the above where available.

Validation checks:

- Single-day contribution:
  - report best/worst day,
  - remove the best day and recompute total return,
  - flag signals dominated by one day.
- Execution stress:
  - default cost/slippage,
  - 2 bps cost + 2 bps slippage,
  - 5 bps cost + 5 bps slippage.
- Position policy stress:
  - original long/cash/short,
  - long/cash only.
- Delayed-entry stress:
  - for minute-horizon signals where rows are available, enter one minute later and reuse the
    same horizon label from the delayed row.
- Threshold stability:
  - keep the selected validation threshold,
  - rerun stricter thresholds at `+0.05` and `+0.10` where possible.

Validation promotion rule:

- A signal can move to paper-trading only if it is positive in at least two of three holdout months,
  remains positive after removing its best day, survives 2 bps + 2 bps execution stress, and does
  not collapse under a nearby stricter threshold.

### Phase 3 Validation Status

Completed:

- Added `spy-intraday:validate-signals`.
- Validated all 15 Phase 2 promising variants across 8 signal families.
- Validation report: `artifacts/phase2-signal-validation-2025-01-02-2026-04-27.json`.

Survivors:

- 2 variants passed the promotion rule.
- Both survivors are the same family: volatility-gated gamma-regime `next_60m`.

Best survivor:

- Walk-forward volatility-gated gamma-regime `next_60m`:
  - default total return: `+4.192%`
  - 2 bps + 2 bps stress: `+2.313%`
  - 5 bps + 5 bps stress: `-3.124%`
  - long/cash only: `+4.464%`
  - delayed one minute: `+3.532%`
  - return after removing best day: `+1.639%`
  - stricter threshold `+0.05`: `+1.916%`

Rejected despite being interesting:

- Opening option-flow EOD proxy stayed profitable under cost stress, but failed the single-best-day
  concentration check.
- Cross-sectional `next_60m` variants had positive pockets but failed cost, concentration, or
  stability checks.
- Threshold-only and enhanced-meta variants did not survive the full validation layer.

### Phase 4 Monthly Full-History Signal Validation Plan

Goal: test the best surviving signal family across every available month from the Massive-only
dataset beginning `2025-01-01`.

Signal to test:

- Best survivor family: volatility-gated gamma-regime `next_60m`.
- Run two clearly separated protocols:
  - Causal walk-forward: train expanding models using only rows before the month being tested.
  - Retrospective frozen-model sweep: train one model on a chosen anchor window and score every
    month, including months before the anchor window. This tests whether the learned pattern works
    across regimes, but it is not a claim that the model could have traded those earlier months
    live without lookahead.
- Select the magnitude gate threshold on a validation split inside the training window.
- Score each test month.

Monthly reporting:

- active prediction count after horizon sampling and gating,
- success count and failure count,
- success percentage and failure percentage,
- abstained/cash count,
- long count and short count,
- monthly net return and max drawdown,
- selected magnitude threshold and training row count.

Policy notes:

- January 2025 has data and is included in the retrospective frozen-model sweep.
- January 2025 has no prior training data inside this dataset for causal walk-forward evaluation
  and is reported as skipped in that protocol unless earlier training data is added.
- Count prediction success only on active long/short predictions. Cash/abstain observations are
  reported separately and do not count as either success or failure.
- Also report a long/cash-only view because the Phase 3 survivor improved under long-only stress.

### Phase 4 Monthly Full-History Signal Validation Status

Completed:

- Added `spy-intraday:best-signal-full-history`.
- Ran full-history tests from January 2025 through April 2026 using the Massive-only dataset.
- Report: `artifacts/best-signal-full-history-2025-01-02-2026-04-27.json`.

Protocols tested:

- Frozen history-plus-Jan anchor:
  - Train once on `2025-01-02` through `2026-01-30`.
  - Score every month from January 2025 through April 2026.
  - Retrospective regime-stability test, not causal for months before the train end.
- Frozen walk-forward-final anchor:
  - Train once on `2025-01-02` through `2026-03-31`.
  - Score every month from January 2025 through April 2026.
  - Retrospective regime-stability test, not causal for months before the train end.
- Daily expanding selected-threshold retrain:
  - Start scoring on `2025-02-03` after using January 2025 as initial history.
  - Retrain each day using only prior trading days.
  - Threshold reselected from prior-history validation.
  - Training rows capped deterministically at `25,000` for runtime.
- Daily expanding fixed `0.60` threshold retrain:
  - Same daily prior-day-only retrain protocol.
  - Gate probability threshold fixed at `0.60`.
  - Training rows capped deterministically at `25,000` for runtime.

Headline result:

- Frozen history-plus-Jan anchor:
  - Active predictions: `228`
  - Success/failure: `130` / `98`
  - Success rate: `57.02%`
  - Positive months: `10` of `16`
  - Simple monthly return sum: `+11.895%`
- Frozen walk-forward-final anchor:
  - Active predictions: `234`
  - Success/failure: `134` / `100`
  - Success rate: `57.26%`
  - Positive months: `10` of `16`
  - Simple monthly return sum: `+6.474%`
- Daily expanding selected-threshold retrain:
  - Active predictions: `170`
  - Success/failure: `79` / `91`
  - Success rate: `46.47%`
  - Positive months: `7` of `15`
  - Simple monthly return sum: `-13.197%`
- Daily expanding fixed `0.60` threshold retrain:
  - Active predictions: `195`
  - Success/failure: `92` / `103`
  - Success rate: `47.18%`
  - Positive months: `7` of `15`
  - Simple monthly return sum: `-13.463%`

Interpretation:

- The frozen survivor pattern appears to generalize across many months in a retrospective sweep.
- The daily-retrained variant does not work as implemented. It likely over-adapts or destabilizes
  the gate/model relationship, and should not be promoted.

## Universe

Target:

- `SPY`

Stock and ETF features:

- Market ETFs: `QQQ`, `IWM`, `DIA`
- Sector ETFs: `XLK`, `XLF`, `XLE`, `XLV`, `XLY`, `XLP`, `XLI`, `XLU`, `XLB`, `XLRE`, `XLC`
- Risk/factor proxies: `TLT`, `HYG`, `LQD`, `GLD`, `SLV`, `USO`, `SMH`, `SOXX`, `ARKK`
- Mag7: `NVDA`, `TSLA`, `GOOGL`, `MSFT`, `META`, `AAPL`, `AMZN`

Index/vol features:

- `I:SPX`, `I:VIX`, `I:VIX1D`, `I:VIX9D`, `I:VIX3M`, `I:VIX1Y`, `I:VVIX`

Options:

- SPY, SPX/SPXW, VIX/VIXW, and Mag7 option roots where present in Massive OPRA files.

## Modeling And Labels

Fixed forward targets:

- next 1m
- next 5m
- next 60m
- EOD close

Research targets:

- `last_30m_return`: enter at 15:30 ET, exit at the regular-session close.
- `abs_return_5m`, `abs_return_30m`, `abs_return_60m`, `abs_return_eod`: magnitude labels.
- `tb_5m`, `tb_30m`, `tb_60m`: triple-barrier event labels using rolling realized-volatility-scaled
  take-profit and stop-loss barriers.

Rank by prediction quality first:

- directional accuracy
- balanced accuracy
- confusion matrix
- confidence-bucket accuracy
- Brier score and log loss where probabilities exist
- return MAE/RMSE

Secondary checks:

- SPY long/cash/short backtest with costs, slippage, drawdown, turnover, exposure share, and
  buy-and-hold comparison.
- A result is promising only if it improves balanced accuracy by at least 2 percentage points in
  at least two holdout months, or shows a clearly useful high-confidence bucket edge, and has
  positive net return in at least two holdout months without relying on one outlier day.

## Guardrails

- All feature rows must be causal: only data at or before prediction minute.
- Coverage checks must inspect Massive files/manifests only.
- Tests must fail if project code imports legacy database helpers or references `options.*`.
- Formal claims must separate sealed historical results from intraday/provisional results.

## Commands

- `npm run spy-intraday:coverage -- --start-date 2026-01-02 --end-date 2026-04-27`
- `npm run spy-intraday:build-dataset -- --start-date 2026-01-02 --end-date 2026-04-27`
- `npm run spy-intraday:experiments -- --dataset projects/spy-intraday-prediction/runtime/spy-intraday-dataset-2026-01-02-2026-04-27-with-option-features.jsonl`
- `npm run spy-intraday:research -- --dataset <dataset-jsonl>`
- `npm run spy-intraday:phase2 -- --dataset <dataset-jsonl>`
- `npm run spy-intraday:python-export -- --input <dataset-jsonl> --output <compact-jsonl>`
- `npm run spy-intraday:python-research -- --dataset <dataset-jsonl>`
- `npm run spy-intraday:backtest -- --predictions <predictions-jsonl> --horizon next_1m`
