# Wheel Option Income Research

Generated on 2026-05-10 from Massive-only stock and OPRA option 1-minute aggregates.

## Research Inputs

External work suggested that option income needs more than "sell high premium":

- Cboe/Bondarenko put-write research: the durable edge is volatility risk premium, but it comes with left-tail risk and drawdowns.
- arXiv option-writing sizing research: sizing and regime control matter because option-selling returns are skewed.
- arXiv option risk-premium research: short-dated and longer-dated options can carry different premia, so weekly and monthly variants should be tested separately.
- GitHub/open-source option backtest patterns: include commissions, slippage, IV rank/proxy filters, profit-taking, stop losses, and drawdown ranking.
- Reddit/thetagang practitioner notes: high IV helps only when the underlying is acceptable to own; otherwise the wheel becomes forced bag-holding.

## Implementation

Added a Massive-only wheel backtester:

- `projects/spy-intraday-prediction/src/wheel-backtest.js`
- `projects/spy-intraday-prediction/scripts/backtest-wheel-strategy.js`
- `projects/spy-intraday-prediction/test/wheel-backtest.test.js`

The implementation uses:

- Massive `stock_quotes_1m` for underlying marks and trend/realized-volatility features.
- Massive `option_quotes_1m` for OPRA option aggregate entry/mark prices.
- OPRA ticker parsing for root, expiration, right, and strike.
- Black-Scholes IV and delta estimated from the Massive 1-minute option aggregate close.
- Explicit premium haircut, commissions, collateral utilization, assignment handling, and daily mark-to-market equity.

Important assumptions:

- Assignment is modeled at expiration only.
- Early assignment, dividends, borrow, taxes, margin interest, and bid/ask reconstruction are not modeled.
- Entry uses the option aggregate close inside the entry window, with a premium haircut.
- Missing option marks fall back to max intrinsic value or prior mark.

## Experiment Slate

Baseline strategies:

- Weekly 5-10 DTE cash-secured put, 5% OTM.
- Weekly 5-10 DTE cash-secured put, 10% OTM.
- Weekly 5-10 DTE wheel, 5% OTM put and 5% OTM covered call.
- Weekly 5-10 DTE wheel, 10% OTM put and 5% OTM covered call.
- Monthly 25-45 DTE cash-secured put, 10% OTM.
- Monthly 25-45 DTE wheel, 10% OTM put and 5% OTM covered call.

Expanded filters:

- Annualized premium-yield filters.
- Derived IV filters.
- IV/RV filters using prior 20-day realized volatility.
- Rolling IV-rank-style filter.
- Delta-band put selection.
- Prior uptrend filters: above SMA20 and positive 20-day return.
- 50% profit-taking buy-to-close.
- 2x stop-loss buy-to-close.

## Backtest Window

- Window: `2026-01-02` through `2026-04-27`.
- Open trading days: 79.
- Universe: local liquid top-100 proxy, 96 equity/ETF symbols after excluding index roots.
- Coverage: 0 missing Massive files, 0 provider-sparse days.
- Initial capital: `$1,000,000`.

Artifacts:

- `projects/spy-intraday-prediction/artifacts/wheel-strategy-backtest-2026-01-02-2026-04-27.json`
- `projects/spy-intraday-prediction/artifacts/wheel-expanded-backtest-2026-01-02-2026-04-27.json`

Artifacts are intentionally ignored by git. Regenerate them with:

```bash
node projects/spy-intraday-prediction/scripts/backtest-wheel-strategy.js \
  --suite expanded \
  --start-date 2026-01-02 \
  --end-date 2026-04-27 \
  --limit 100 \
  --output projects/spy-intraday-prediction/artifacts/wheel-expanded-backtest-2026-01-02-2026-04-27.json
```

## What Worked

Best raw-return strategy:

| Strategy | Return | Max drawdown | Trades | Assignments |
| --- | ---: | ---: | ---: | ---: |
| `wheel_monthly_10otm_put_5otm_call` | `+3.49%` | `-2.67%` | 79 | 21 |

This did not beat SPY or QQQ on raw return, but drawdown was much lower:

| Benchmark | Return | Max drawdown |
| --- | ---: | ---: |
| SPY | `+4.68%` | `-9.13%` |
| QQQ | `+8.35%` | `-11.80%` |

Best low-drawdown strategy:

| Strategy | Return | Max drawdown | STO | BTC | Assignments |
| --- | ---: | ---: | ---: | ---: | ---: |
| `wheel_weekly_10otm_trend_ivrv_profit50` | `+0.55%` | `-0.06%` | 388 | 356 | 2 |

That strategy is now exposed in the strategy-service UI as:

- `option-income-wheel-trend-ivrv`
- Display name: `Option Income Wheel`

Rules:

- Sell weekly 5-10 DTE puts roughly 10% OTM.
- Require prior uptrend, positive 20-day return, and derived IV/RV >= 1.10.
- Close short options at 50% profit.
- If assigned, hold shares and sell covered calls when eligible.

## What Did Not Work

High premium alone did not work well:

- Weekly 5% OTM cash puts collected `$40,722.70` in premium but returned `-0.13%`.
- High annualized-yield weekly cash puts collected `$32,430.15` but returned only `+0.34%` with many assignments.
- Monthly yield-plus-profit-taking variants collected large premium but had worse drawdowns than the simpler monthly wheel.

Stop-loss logic did not help in this version:

| Strategy | Return | Max drawdown | Buybacks |
| --- | ---: | ---: | ---: |
| `wheel_monthly_10otm_profit50_stop2x` | `-1.51%` | `-2.24%` | `$71,618.40` |

Rolling IV rank did not work in this short official window:

- `cash_put_weekly_10otm_ivrank50` produced no trades.
- The reason is insufficient warmup history inside the sealed window.

Delta-band selection was not enough:

- `cash_put_weekly_10otm_delta10_25` returned `+0.29%` with `-1.39%` max drawdown and 33 assignments.

## Current Recommendation

The best candidate for UI/paper monitoring is not the highest-return strategy. It is:

`wheel_weekly_10otm_trend_ivrv_profit50`

Reason:

- Positive every holdout month after January warmup.
- Very small drawdown in this window.
- Many trades, so it is less likely to be a one-trade fluke.
- Low assignment count.

Caution:

- Absolute return is small.
- Derived IV is estimated from aggregate prices, not provider Greeks.
- This needs a longer warmup and wider history before production consideration.

## Next Experiments

1. Add a 6-12 month warmup before the official test window for IV rank and IV percentile.
2. Add earnings-date exclusion for single-name short puts.
3. Add liquidity ranking by option volume and transaction count.
4. Try portfolio-level caps by sector/theme so tech/growth does not dominate exposure.
5. Compare 30%, 50%, and 70% profit-taking thresholds.
6. Test lower collateral utilization and per-symbol limits.
7. Add VIX/VIX1D regime filters from the existing Massive index dataset.
8. Test put spreads as defined-risk variants for high-IV names.
9. Stress fills with larger premium haircuts and higher commissions.
10. Run monthly walk-forward selection between the simple monthly wheel and the low-drawdown weekly IV/RV wheel.
