# Option 1m Filter Backtest Report (2026-03-14)

## Scope
- Base entry logic: `scripts/backfill/backtest-30m-flag-ema8.js` (30m flag + EMA8 pullback long setup).
- Option features: prior-1m join from `options.option_symbol_minute_derived`.
- Date range tested: `2025-09-01` to `2026-03-12`.
- Symbol scopes:
  - `all_symbols` (top 100 universe in this run)
  - `robust20` (subset that previously showed stronger baseline behavior)

## Important Data Note
- `option_trade_quote_1m` is treated as a stream/chunk label, not a physical fact table.
- Physical option rows are in `options.option_trades`.
- The strategy-filter work here used `options.option_symbol_minute_derived` for 1m option features.

## Code Added/Updated
- Added richer trade fields to backtest trade output:
  - `scripts/backfill/backtest-30m-flag-ema8.js`
- Added option-filter search script:
  - `scripts/backfill/analyze-option-1m-filters.js`
- Added strict walk-forward script:
  - `scripts/backfill/walkforward-option-1m-filters.js`

## Main Runs

### 1) In-sample option-filter search
Backtest input:
- `artifacts/reports/flag30-ema8-backtest-2025-09-01-2026-03-12-20260314T124920456Z.json`

Key reports:
- `artifacts/reports/option-1m-filter-analysis-robust20-20260314T125650911Z.json` (targetR=4)
- `artifacts/reports/option-1m-filter-analysis-robust20-20260314T125808579Z.json` (targetR=5)
- `artifacts/reports/option-1m-filter-analysis-robust20-20260314T125947997Z.json` (targetR=6)
- `artifacts/reports/option-1m-filter-analysis-robust20-20260314T125948003Z.json` (targetR=9)
- `artifacts/reports/option-1m-filter-analysis-all-symbols-20260314T125725910Z.json` (targetR=4)
- `artifacts/reports/option-1m-filter-analysis-all-symbols-20260314T125847204Z.json` (targetR=5)
- `artifacts/reports/option-1m-filter-analysis-all-symbols-20260314T130052291Z.json` (targetR=6)
- `artifacts/reports/option-1m-filter-analysis-all-symbols-20260314T130052288Z.json` (targetR=9)

Highlights:
- Robust20 improved strongly with option filters at 4R/5R/6R/9R target-hit framing.
- All-symbol runs improved hit rates materially, but expectancy remained mixed at 5R/6R under stricter trade-count constraints.

### 2) Strict walk-forward validation + exit-model comparison
Train/Test split:
- Train: `2025-09-01` to `2025-12-31`
- Test: `2026-01-01` to `2026-03-12`

Reports:
- `artifacts/reports/walkforward-option-1m-filter-all-symbols-r5-20260314T130737158Z.json` (`targetR=5`, `minTrades=40`)
- `artifacts/reports/walkforward-option-1m-filter-all-symbols-r5-20260314T130737149Z.json` (`targetR=5`, `minTrades=25`, looser/overfit check)
- `artifacts/reports/walkforward-option-1m-filter-robust20-r5-20260314T130759604Z.json` (`targetR=5`, robust20)
- `artifacts/reports/walkforward-option-1m-filter-all-symbols-r4-20260314T130830868Z.json` (`targetR=4`, `minTrades=40`)

## Results Summary

### A) All symbols, targetR=5, strict (`minTrades=40`)
Selected filter (from train):
- `putSize >= 110 AND callPutSizeRatio <= 1`

Test-period avgExitR by exit:
- `closeOrStopR`: `-0.1257 -> +0.0080` (baseline -> filtered)
- `hodOrStopR`: `-0.0518 -> -0.0026`
- `scaleout1r2rBeR`: `+0.0088 -> -0.3067`

Interpretation:
- Filtering improved `close_or_stop` from negative to slightly positive OOS.
- Scaleout model degraded materially on this filtered subset.

### B) Robust20, targetR=5 (`minTrades=25`)
Selected filter (from train):
- `ivSpread >= 0 AND callSize >= 163.2`

Test-period avgExitR by exit:
- `closeOrStopR`: `+0.3518 -> +1.0783`
- `hodOrStopR`: `+0.4038 -> +1.2066`
- `scaleout1r2rBeR`: `+0.3044 -> +0.7016`

Interpretation:
- Strong OOS lift on robust20 for all three exits.
- Best on this subset: `hod_or_stop`.

### C) Overfit/robustness checks
- Looser all-symbol selection (`targetR=5`, `minTrades=25`) looked strong in train but failed OOS.
- All-symbol `targetR=4` strict run also failed OOS after filtering.
- This supports keeping stricter sample-size constraints and forward validation.

## 4R Average Exit Question
- No tested strategy achieved `avgExitR >= 4`.
- Highest observed `avgExitR` in these runs was `2.3149` (small in-sample pocket, robust20).
- Best strict OOS filtered result observed was `1.2066` (`hodOrStopR`, robust20 test subset).

## Repro Commands (Representative)
```bash
# Baseline backtest with full trade rows
node scripts/backfill/backtest-30m-flag-ema8.js \
  --from 2025-09-01 \
  --symbolsPath artifacts/reports/top100-symbols-20250901-20260312.json \
  --exitModel close_or_stop \
  --includeTrades 1

# In-sample option-filter scans
node scripts/backfill/analyze-option-1m-filters.js \
  --backtest artifacts/reports/flag30-ema8-backtest-2025-09-01-2026-03-12-20260314T124920456Z.json \
  --targetR 5 \
  --minTrades 40 \
  --includeAllSymbols 1

# Strict walk-forward with exit comparison
node scripts/backfill/walkforward-option-1m-filters.js \
  --backtest artifacts/reports/flag30-ema8-backtest-2025-09-01-2026-03-12-20260314T124920456Z.json \
  --targetR 5 \
  --minTrades 40 \
  --includeAllSymbols 1
```
