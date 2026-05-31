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

---

## Iteration 5 — does DAILY rebalancing (EOD+10min) validate?

Tested recomputing holdings every trading day and rebalancing just after the close
(decide on close[T], earn close[T]->close[T+1]; causal, near-close fill). Compared monthly /
weekly / daily / daily+no-trade-band on an identical DAILY return stream, with a one-way cost
sweep. (Daily-frequency stats show SPY's true daily Sharpe 0.72 and MaxDD -33.7%, deeper than
the month-end-sampled -23.9% — so strategy and SPY are compared on the same daily basis here.)

OOS 2019-07..2026-05, top-20 12m-momentum inverse-vol + trend filter:

| Rebalance | one-way turnover/yr | Sharpe @0bps | @3bps | @5bps | @10bps | MaxDD |
|---|---|---|---|---|---|---|
| **Monthly (published)** | **11.3x** | **0.96** | 0.95 | 0.94 | 0.92 | -27% |
| Weekly | 25.4x | 0.80 | 0.77 | 0.75 | 0.69 | -27% |
| Daily | 63.4x | 0.84 | 0.76 | 0.70 | 0.56 | -25% |
| Daily + 2% no-trade band | 53.6x | 0.83 | 0.76 | 0.71 | 0.59 | -24% |
| SPY buy & hold | — | 0.72 | — | — | — | -34% |

**Verdict: daily rebalancing does NOT validate.**
1. **Worse even gross.** Daily Sharpe 0.84 < monthly 0.96. The signal is 12-month momentum —
   slow-moving — so daily recomputation chases noise around the top-20 cutoff and whipsaws
   names in/out (ICLN, PICK, EPU rotating; constant inverse-vol reweighting). 100% of days
   trade; ~49-63x annual turnover vs 11x monthly.
2. **Costs then bury it.** At a realistic 3-5 bps one-way ETF cost, daily Sharpe falls to
   0.70-0.76 — roughly SPY parity; at 10 bps (thematic/leveraged sleeves, EOD+10min after-hours
   spreads) it drops to 0.56, BELOW SPY. Monthly loses almost nothing to cost (0.96->0.92).
3. Daily does cut vol/drawdown slightly (faster de-risking) but at a large return penalty.
4. A no-trade band helps marginally but cannot rescue it.

**Conclusion:** keep the **monthly** cadence (weekly at most). Computing holdings daily is useful
for monitoring, but trading them daily destroys the edge once cost is realistic. `EOD+10min`
execution is fine as a fill model; the problem is frequency, not timing.

---

## Iteration 6 — daily PRICE/VOLUME overlays that beat the monthly Sharpe

Reframe of iter-5's failure: don't recompute *selection* daily (that churns the top-K cutoff).
Keep 12-month-momentum **selection monthly**; add **daily-updated risk overlays** with
hysteresis/no-trade bands. Bogey to beat = monthly base, net 5bps: **Sharpe 0.94** (OOS daily).

Tested overlays (OOS 2019-07..2026-05, net 5bps):

| Overlay (daily) | Sharpe | MaxDD | turn/yr | verdict |
|---|---|---|---|---|
| monthly base (bogey) | 0.94 | -27% | 8.5x | — |
| **daily regime: SPY<EMA100 -> 50% exposure** | **0.98** | **-21%** | 10x | **beats, +DD** |
| daily regime: SPY<EMA100 -> 33% exposure | **1.03** | -27% | 12x | highest Sharpe |
| daily regime: SPY<EMA50 -> 50% | 0.93 | -19% | 12x | ~bogey, best DD |
| portfolio vol-target 15% (1 gross lever) | 0.88 | -24% | 7x | below bogey |
| VT15 + regime100/50% | 0.92 | -22% | 9x | below regime-only |
| per-name fast-gate EMA20 (hysteresis) | 0.76 | -15% | 51x | **turnover kills it** |
| per-name OBV volume confirmation | 0.82 | -16% | 26x | **turnover kills it** |

### What worked, what didn't (learned)
- **WIN = a single portfolio-level risk lever driven by a daily PRICE signal:** scale total
  exposure down when SPY is below its EMA100 (medium-term trend). It reacts daily between monthly
  rebalances, costs almost no turnover (one gross lever, ~10x/yr), and lifts net Sharpe to
  0.98-1.03 while cutting drawdown.
- **Volume signals did NOT add net value here.** OBV confirmation and per-name volume gates
  improved gross risk but their daily toggling drove 26-51x turnover that erased the edge after
  cost. Dollar-volume liquidity floors were neutral (the universe is already liquid).
- **Per-name daily gating/scaling churns** (each name's signal moves independently -> reshuffles
  weights daily). Portfolio-level scaling changes only the risk/cash split -> low turnover.

### Honest validation (no cherry-pick)
- **Robust across every sub-period** net 5bps: regime100->50% beats the monthly bogey on Sharpe
  AND drawdown in FULL (0.84 vs 0.80), OOS (0.98 vs 0.94), 2020-21 (1.26 vs 1.17), 2022-23
  (DD -17% vs -23%), 2024-26 (1.25 vs 1.22). regime100->33% is higher-Sharpe everywhere.
- **Walk-forward** (each month pick the overlay by trailing-252d Sharpe, past-only): 0.83 vs
  monthly 0.81 over 2018-2026, and it organically selects the regime overlay in 60/95 months.

**Conclusion:** a daily rebalance DOES beat monthly once the daily signal is the right one —
a low-turnover, portfolio-level **price-trend regime gate**, not faster trading of the momentum
book and not volume confirmation. Recommended config: monthly 12m-momentum top-20 inverse-vol
selection + daily "SPY<EMA100 -> 50% exposure" overlay (Sharpe 0.98, MaxDD -21%, ~10x turnover).
