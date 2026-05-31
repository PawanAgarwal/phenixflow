# Cross-Asset Trend Strategy — beating SPY's Sharpe

**Goal:** use established trend/momentum techniques to identify which asset classes (from the
`research/asset-universe` taxonomy) are in an uptrend across various time horizons, rotate into
them, and beat **SPY's Sharpe ratio** out-of-sample — walking the approach forward and learning
from each mistake (see [`NOTES.md`](NOTES.md) for the full iteration log).

## Result

| Period | Strategy Sharpe | SPY Sharpe | Strategy CAGR | SPY CAGR | Strategy MaxDD | SPY MaxDD |
|---|---|---|---|---|---|---|
| **Walk-forward OOS** (2019-07 → 2026-05) | **0.91** | 0.84 | 18.9% | 16.4% | -20.0% | -23.9% |
| Full sample (2017-07 → 2026-05) | **0.85** | 0.83 | 15.5% | 15.4% | -20.7% | -23.9% |

The walk-forward result uses **no hindsight** — each month it picks parameters only from prior
data. Sortino (1.53 vs 1.31) and drawdown also beat SPY. The strategy's edge comes from
**avoiding crashes and catching cross-asset trends**: it beat SPY's Sharpe in 5/10 calendar
years, decisively in 2020 (+34% vs +18%), 2022 (-4% vs -18%), and the 2025–26 commodity /
uranium / semiconductor / crypto rallies, while lagging in calm US-equity bull years.

## The strategy (established techniques)

- **Universe:** 245 cross-asset ETF/proxy sleeves (equities, bonds, commodities, real assets,
  FX, crypto, themes) from `research/asset-universe`, daily total-return prices 2016–2026.
- **Uptrend signal:** **12-month (252-day) total-return momentum** — the classic, most robust
  momentum horizon. (Blending shorter 1–6-month lookbacks measurably *hurt* — see NOTES.)
- **Trend filter:** keep only sleeves with positive 12-month momentum **and** price above their
  200-day moving average; the rest of the book sits in cash (BIL). Downside protection.
- **Selection & weighting:** hold the **top 20** by momentum (diversification kills idiosyncratic
  risk — the single biggest Sharpe lever found), **inverse-volatility** weighted, 30% cap.
- **Rebalance:** monthly, 10 bps turnover cost, risk-free = realized cash (BIL) for fair Sharpe.

## What beat SPY, and the mistakes that didn't (learned forward)

1. ❌ *Concentrated (top-5), inverse-vol, no filter* → Sharpe 0.70: vol exploded to 28%.
2. ❌ *Aggressive 12% vol targeting* → capped returns below SPY's own ~16% vol; 0% beat SPY.
3. ❌ *Blending 1–6-month lookbacks into the 12-month signal* → diluted it (0.39 vs 0.72).
4. ✅ *Pure 12-month momentum + diversify across top ~20 + inverse-vol + 200d trend filter* →
   Sharpe rises monotonically with breadth; 33/48 grid variants beat SPY → robust, not luck.

## Reproduce

```bash
pip install pandas numpy
python3 fetch_prices.py        # cache daily adj-close for the universe + SPY (Yahoo)
python3 backtest.py --recompute  # build candidate-return cache + walk-forward grid
python3 meta.py                # fast meta-layer comparison of walk-forward schemes
python3 strategy.py            # FINAL rule: full-sample + walk-forward + yearly + holdings
python3 daily_holdings.py 10   # target holdings for EVERY trading day (+ turnover)
python3 daily_backtest.py      # validate daily/weekly/monthly rebalance vs cost
```

Artifacts: `strategy_results.json`, `equity_curve.csv`, `equity_curve.png`, `data/cand_rets.csv`,
`daily_weights.csv`.

## Rebalance frequency — why monthly, not daily

The signal is 12-month momentum (slow-moving), so trading it daily mostly adds turnover. On an
identical daily return stream (OOS 2019–2026), recomputing/trading holdings every day at EOD+10min:

| Rebalance | turnover/yr | Sharpe @0bps | @3bps | @5bps | @10bps |
|---|---|---|---|---|---|
| **Monthly (published)** | **11.3×** | **0.96** | 0.95 | 0.94 | 0.92 |
| Weekly | 25.4× | 0.80 | 0.77 | 0.75 | 0.69 |
| Daily | 63.4× | 0.84 | 0.76 | 0.70 | 0.56 |
| SPY buy & hold | — | 0.72 | — | — | — |

Daily is **worse even before costs** (0.84 < 0.96 — it whipsaws names around the top-20 cutoff),
and at realistic 3–5 bps ETF cost it erodes to ~SPY parity; at 10 bps it underperforms SPY.
Compute holdings daily for **monitoring**, but rebalance **monthly** (weekly at most). The
`EOD+10min` fill assumption is fine — the problem is frequency, not timing. See `NOTES.md` §5.
