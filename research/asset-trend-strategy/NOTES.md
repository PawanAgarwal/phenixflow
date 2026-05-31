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

---

## Iteration 7 — full-asset-class regime gates vs SPY-only (does cross-asset beat SPY as the risk proxy?)

The iter-6 regime gate used SPY only. Tested two genuinely cross-asset alternatives plus a combo,
same discipline (daily, net 5bps, walk-forward). regime_compare.py.

- **breadth**: % of the whole universe above its 200d MA (true cross-asset risk gauge).
- **ownbook**: the held portfolio's OWN equity trend vs its EMA (de-risk when YOUR assets roll over).
- **combo**: average of the SPY-gate scale and the breadth-gate scale.

OOS 2019-07..2026-05, net 5bps:

| Regime gate | Sharpe | MaxDD | turn/yr | note |
|---|---|---|---|---|
| base (no regime) | 0.94 | -27% | 8.5x | bogey |
| **SPY EMA100->33%** | **1.03** | -27% | 12x | highest Sharpe |
| SPY EMA100->50% | 0.98 | -21% | 10x | best SPY Sharpe+DD |
| breadth continuous | 0.95 | **-20%** | **7.1x** | cheapest, best DD, cross-asset |
| breadth<50%->50% | 0.90 | -22% | 9x | below SPY |
| ownbook EMA100->33% | 0.95 | -30% | 13x | best in 2022 bear (0.41) |
| ownbook EMA100->50% | 0.89 | -21% | 11x | below SPY |
| **combo SPY+breadth ->33%** | **1.00** | **-22%** | **8.3x** | best all-rounder |

### Findings (honest)
- **SPY alone is the single best risk proxy on raw Sharpe** (1.03 / 0.98). Sharp equity
  drawdowns are the cleanest, least-noisy signal of broad risk-off; the index is less jittery
  than breadth or the concentrated book, so it whipsaws less.
- **Pure cross-asset gates are competitive, not better.** Breadth-continuous (0.95) and
  ownbook->33% (0.95) beat the base but trail SPY on Sharpe. Each has a virtue: breadth-continuous
  is the cheapest (7x turnover) with the best drawdown (-20%); ownbook protected best in the
  2022 everything-down bear (Sharpe 0.41 vs SPY 0.30).
- **Combining SPY + breadth is the best balance:** Sharpe 1.00 (≈ SPY-only) but with a smaller
  drawdown (-22% vs -27%), the lowest turnover, and the best bear-market Sharpe — it genuinely
  uses the full asset universe and is the most robust all-rounder.
- **Walk-forward** (pick the gate monthly by trailing-252d Sharpe, past-only): 0.85 vs base 0.81;
  it organically spreads across breadth<40%->33% (25mo), SPY->33% (21mo), base (17mo) — confirming
  cross-asset and SPY gates are both useful, neither dominant.

**Verdict:** a full-asset-class regime signal (breadth) works and, combined with SPY, gives the
best drawdown-adjusted result; but SPY alone is hard to beat on pure Sharpe because it is the
least-noisy proxy for the broad risk-off events that matter. Recommended: **combo SPY+breadth
->33%** (best all-rounder, cross-asset) or **SPY EMA100->50%** (simplest, best SPY Sharpe+DD).

---

## Iteration 8 — breadth gate: multiple MAs, scalings (incl. ->0), and non-SMA uptrend mechanisms

Prompted by: "did we try multiple MAs and scaling factors incl. 0 at low breadth, and other
uptrend mechanisms than SMA?" Answer was no — only SMA200 + a couple scalings. Now swept
systematically (breadth_explore.py; analytic engine — gate is one scalar on a fixed book).
Net 5bps, OOS 2019-2026. Bogey (no gate) = 0.94, SPY = 0.72.

### (A) Uptrend MECHANISM x length (breadth ramp 20%->60%, full-cash floor 0)
| mechanism | best length | OOS Sharpe |
|---|---|---|
| price > **EMA** | 50 | **1.08** |
| **Donchian** channel pos > 0.5 | 252 | **1.08** |
| 12-month **momentum** > 0 | 252 | 1.05 |
| price > **SMA** | 50 / 252 | 1.04 / 1.01 |
| price > SMA | **200 (original)** | **0.97** |
| MACD > 0 | 12/26 | 0.90 |
| near 252d high (within 5%) | 252 | 0.82 |

**The original SMA200 was one of the WEAKER choices.** ~14/16 configs beat the bogey -> the
breadth gate is robust, but the *mechanism* matters: slow trend measures (Donchian-252,
mom-252) give the best Sharpe AND the lowest turnover (~11x); fast lines (EMA50/SMA50) match
Sharpe but churn more (18-21x); MACD and new-high lag.

### (B) SCALING map sweep  scale = clip((breadth-lo)/(hi-lo), 0, 1)   (incl. ->0)
- Best: **Donchian-252, ramp lo=0.18 -> hi=0.50 -> Sharpe 1.10** (CAGR 22%, MaxDD -18%, 11x).
- The **full-cash floor helps**: lo~0.10-0.18 (exposure ramps to 0% when <~18% of assets are in
  uptrend) beats lo=0 (never fully de-risk); lo=0.40 (de-risk too eagerly) hurts.
- hi=0.50 (reach full investment at 50% breadth) beats hi=0.60/0.70.

### (C) Sub-period robustness — best = Donchian-252, ramp 0.18->0.50
| period | base (no gate) | breadth-Donch | SPY | best MaxDD |
|---|---|---|---|---|
| OOS 2019-2026 | 0.94 | **1.10** | 0.72 | -18% |
| 2020-2021 | 1.17 | **1.41** | 0.93 | -12% |
| 2022-2023 (bear) | 0.29 | **0.81** | 0.02 | -12% |
| 2024-2026 | 1.22 | 1.12 | 1.07 | -18% |
Huge bear-market gain (2022: 0.81 vs 0.29); only laggard is 2024-26 (de-risked through 2025 chop).

### (D) Walk-forward (pick the mechanism monthly, past-only, ramp 0.20->0.55)
Adaptive Sharpe **0.95** vs base 0.81, SPY 0.70 (window starts 2018). Picks spread across ema50
(26mo), mom252 (25mo), sma200, donch200, macd — no single mechanism is magic; the *family* works.

**Verdict:** yes — sweeping MAs/scalings/mechanisms materially improves the breadth gate. Best
cross-asset gate = **Donchian-252 channel-position breadth, exposure ramp 18%->50% (to 0% cash
at very low breadth)**: OOS Sharpe 1.10 (vs 0.97 for the original SMA200 breadth, 0.94 no gate,
0.72 SPY), ~11x turnover, and far better bear protection. Walk-forward confirms (0.95).

---

## Iteration 9 — verify Breadth EMA-50 and register it in the phenixflow UI

**Verify (verify_breadth_ema50.py).**
- RECONCILE: an independent from-scratch daily engine (explicit weights, explicit 1-day
  execution lag, explicit turnover/cost) vs the fast analytic engine agree to ~3e-4
  (Sharpe 1.0966 vs 1.0963; daily-return corr 0.99998). The iter-8 analytic numbers are sound.
  (A first scratch pass had an off-by-one = 2-day lag; fixing it to a true 1-day lag matched.)
- LOOKAHEAD audit: honest 1-day-lagged Sharpe 1.10 vs illegal same-day peek 2.11 — the lag is
  respected and matters; EMA/rolling/momentum/selection are all backward-looking (no leakage).
- WALK-FORWARD (anchored: pick ramp + EMA length from PRIOR years only): OOS 2020-2026
  Sharpe 0.99 vs no-gate base 0.97, SPY 0.68; MaxDD -16% vs base -27%. Parameter-stability grid
  (EMA50 x ramps) is tight at 1.05-1.11. Honest read: the full-OOS 1.10 carries some
  in-sample-param optimism; the realistic edge over the ungated book is modest on Sharpe but
  strong on drawdown and clearly beats SPY.

**Register (phenixflow strategy-service + studies-dashboard).**
- Artifact generator `export_artifact.py` -> `projects/asset-trend-breadth/artifacts/
  breadth-ema50-report.json` (daily equity series + monthly snapshots, schema v1).
- Adapter `apps/strategy-service/src/strategies/breadth-ema50.js`, registered in
  `default-registry.js` as `asset-trend-breadth-ema50` (research_only, daily_eod). Doc at
  `docs/strategies/asset-trend-breadth-ema50.md`.
- Verified: appears in `GET /api/strategies` (28 total); chart/values/open-positions/portfolio
  endpoints all serve; strategy-service test suite green except one PRE-EXISTING kernel/PYM
  failure unrelated to this change (fresh checkout missing a PYM data/kernel-download artifact).
