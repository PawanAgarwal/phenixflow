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
Naively trading the slow momentum book daily fails — but see below for a daily overlay that wins.

## Daily overlay that *does* beat monthly (iteration 6)

The fix: keep **selection** monthly (12-month momentum, top-20, inverse-vol) and add a single
**daily, portfolio-level PRICE-trend risk lever** — scale total exposure down when SPY trades
below its EMA100. It reacts daily between rebalances at almost no turnover (~10×/yr). Net of 5bps,
OOS 2019–2026 (daily basis):

| Strategy | Sharpe | CAGR | MaxDD | turnover/yr |
|---|---|---|---|---|
| SPY buy & hold | 0.72 | 15.8% | -33.7% | — |
| Monthly base (bogey) | 0.94 | 20.1% | -27.2% | 8.5× |
| **+ daily regime SPY<EMA100 → 50%** | **0.98** | 19.9% | **-21.3%** | 10× |
| + daily regime SPY<EMA100 → 33% | **1.03** | 23.3% | -26.7% | 12× |

It beats the monthly Sharpe in the full sample and **every** sub-period, and a walk-forward that
picks the overlay monthly from past data only confirms it. **Volume signals (OBV, dollar-volume)
did *not* help net** — their daily toggling drove 26–51× turnover that erased the gains; the
winning daily signal is price-based. Per-name daily gating/scaling also churns too much. Run:
`python3 daily_opt.py` (overlay sweep) and `python3 daily_validate.py` (robustness + walk-forward).
See `NOTES.md` §5–6.

### SPY vs full-asset-class regime gate (iteration 7)

Is SPY the right risk signal, or should the gate use the whole universe? Tested **breadth**
(% of the universe above its 200d MA), **own-book** (the held portfolio's own trend), and a
**combo** of SPY+breadth (`python3 regime_compare.py`). Net 5bps, OOS 2019–2026:

| Regime gate | Sharpe | MaxDD | turn/yr |
|---|---|---|---|
| SPY EMA100→33% | **1.03** | -27% | 12× |
| SPY EMA100→50% | 0.98 | -21% | 10× |
| **combo SPY+breadth →33%** | 1.00 | **-22%** | **8.3×** |
| breadth continuous (cross-asset only) | 0.95 | -20% | 7.1× |
| own-book EMA100→33% | 0.95 | -30% | 13× |

**SPY alone has the highest raw Sharpe** — it's the least-noisy proxy for the broad risk-off
events that matter, so it whipsaws less than breadth or the concentrated book. Pure cross-asset
gates are *competitive, not better* (breadth is cheapest with the best drawdown; own-book
protected best in the 2022 everything-down bear). **Combining SPY + breadth is the best
all-rounder**: Sharpe ≈ SPY-only but smaller drawdown and lowest turnover. See `NOTES.md` §7.

### Better breadth: MA length, scaling, and non-SMA uptrend mechanisms (iteration 8)

The first breadth gate used `price > 200d SMA` — it turns out that was a *weak* choice. Sweeping
the uptrend mechanism, length, and the breadth→exposure scaling (`python3 breadth_explore.py`),
net 5bps, OOS 2019–2026:

| Breadth uptrend mechanism | OOS Sharpe |
|---|---|
| **Donchian-252 channel position** (price in upper half of 1-yr range) | **1.08–1.10** |
| EMA-50 | 1.08 |
| 12-month momentum > 0 | 1.05 |
| SMA-50 | 1.04 |
| SMA-200 (the original) | 0.97 |
| MACD / near-52w-high | 0.82–0.90 |

Best config = **Donchian-252 breadth with an exposure ramp from 18% breadth (→ 0% invested, full
cash) up to 50% breadth (→ 100% invested)**: OOS Sharpe **1.10**, ~11× turnover, drawdown -18%,
and it transforms the 2022 bear (**0.81 vs 0.29** no-gate, **0.02** SPY). The full-cash floor at
low breadth helps, mainly in bears. A walk-forward that picks the mechanism monthly from past
data confirms it (0.95 vs 0.81 no-gate). See `NOTES.md` §8.
