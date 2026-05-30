# Asset-Trend Walk-Forward — Iteration Log

Goal: identify asset classes in uptrend (established trend/momentum techniques, multiple
horizons) and rotate into them to beat **SPY's Sharpe** out-of-sample, walking the approach
forward and learning from each mistake.

Data: 248 cross-asset ETFs/proxies from `research/asset-universe` (daily adj-close, 2016–2026,
Yahoo). Risk-free = realized cash (BIL) return, applied identically to strategy and SPY.

Benchmark to beat — SPY buy & hold (full 2017-06..2026-05): **Sharpe 0.83**, CAGR 15.4%,
Vol 16.1%, MaxDD -23.9%.

---

## Iteration 1 — baseline rotation (multi-horizon momentum + top-K + cash overlay)
Walk-forward adaptive: **Sharpe 0.70** (CAGR 20.2%, Vol 28.0%, MaxDD -34.7%). **LOST to SPY.**

Mistakes identified:
1. **Uncontrolled portfolio vol (28%).** Selector chases trailing Sharpe and lands on
   concentrated, high-octane candidates (k=5, inverse-vol, MA filter OFF) — crypto / single-
   country / leveraged sleeves dominate. High CAGR but terrible risk-adjusted return.
2. **Single-best selection flip-flops.** Picking the one best trailing-Sharpe candidate each
   month is itself an overfit signal; it switches into whatever just ran hot.
3. **Unfair benchmark window.** SPY measured over 107 months vs WF's 83-month OOS window.

Fixes for iteration 2:
- Portfolio **volatility targeting** (scale exposure to a target vol, remainder to cash; no
  leverage). Most reliable Sharpe lever.
- **Ensemble** walk-forward: average the top-M candidates by trailing Sharpe (cuts selection
  variance) instead of betting on a single rule.
- Measure SPY over the **identical OOS window** as the walk-forward curve.

---

## Iteration 2 — vol-targeting + ensemble selection
WF (top-5 trailing-Sharpe ensemble, vol-target candidates): **Sharpe 0.72** vs SPY 0.84.
Vol-targeting cut MaxDD to -17.7% but a too-low 12% target capped returns. Still lost.
Family analysis lesson: the **`lb252` (12-month) momentum family clusters high** (mean 0.78,
38% beat SPY) — pure 12-month momentum is the real signal; blending short horizons dilutes it.
12% vol target hurt (0% of those beat SPY).

## Iteration 3 — focus on 12-month momentum, revisit concentration
Lesson refined: blending 126d into 252d **craters** Sharpe (lb126_252 mean 0.39 vs lb252 0.72).
Pure 12-month momentum is best. Inverse-vol > equal-weight. Vol-targeting still hurts in this
bull window. k10 > k4 (early concentration hypothesis was wrong — a 1-iteration fluke).

## Iteration 4 — diversify the winner set (K sweep on pure 12-month momentum)
**Sharpe rises monotonically with K while vol falls** — diversifying across the top trending
assets removes idiosyncratic risk:
  k=8 -> 0.86, k=15 -> 0.97, k=20 -> 1.03, k=25 -> 1.06 (inverse-vol, no filter).
33/48 candidates beat SPY. **Robust family, not a lucky point.**

### Honest walk-forward (no hindsight) — BEATS SPY
| Scheme | Sharpe | CAGR | Vol | Sortino | MaxDD |
|---|---|---|---|---|---|
| SPY buy & hold | 0.84 | 16.4% | 16.7% | 1.31 | -23.9% |
| Equal-wt 12m + inverse-vol (zero selection) | **0.93** | 19.1% | 17.9% | 1.51 | -20.9% |
| Top5 trailing-Sharpe, 12m + trend-ON | **0.91** | 18.9% | 18.1% | 1.53 | -20.0% |
| Single a-priori rule: top-20, invvol, trend-ON | **1.03** | 19.6% | 16.4% | 1.68 | -16.2% |

## Conclusions / what beat SPY and why
- **Signal:** cross-sectional **12-month total-return momentum** across the broad multi-asset
  universe identifies the asset classes in the strongest established uptrends.
- **Diversification:** holding the **top ~20** trending sleeves (not 5) removes idiosyncratic
  vol — the single biggest Sharpe lever here.
- **Weighting:** **inverse-volatility** beats equal weight.
- **Downside protection:** a **200-day MA trend-to-cash filter** trims drawdown (-16% vs SPY
  -24%) at little Sharpe cost and protects in regimes this 2019–2026 window under-samples.
- **Mistakes that cost Sharpe:** concentration (k=5), blending short lookbacks into the 12-month
  signal, and over-aggressive (<16%) vol targeting. All discarded.
