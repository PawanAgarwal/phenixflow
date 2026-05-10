# PYM Extension Strategies — Research Notes

This document captures every idea tested as a candidate add-on to base PYM V5,
what worked, what failed, and the methodology lessons. The single registered
output of this research is `pym-v5-sleeve-meta-21d-cap25` in the strategy
service.

The intended audience is the next person (or AI) extending PYM. Read this
before adding new strategies.

## TL;DR

- Base PYM V5: ret `+80.77%` / DD `-12.87%` / Sharpe `2.38`
  (2025-01-02 → 2026-05-07, 2 bps cost, live-service file).
- Winner: `pym-v5-sleeve-meta-21d-cap25`: ret `+118.38%` / DD `-11.20%` /
  Sharpe `2.82`. Same drawdown out of sample, much higher return.
- The mechanism: equal-weighting the eight Composer sub-strategies is suboptimal.
  Reweight them daily by trailing 21d Sharpe with a 25% per-sleeve cap.
- A floor-based variant (`floor=5%`) was tried first and works less well than
  a cap. **Caps beat floors** — see "Cap vs floor" below.
- Multiple other ideas (credit overlay, sector momentum, RSI-horizon gate,
  breadth filter, sleeve-meta with floor) all looked promising at first but
  failed once the methodology bug was fixed.

## Methodology lessons (read this first)

### 1. Wilder RSI is path-dependent — pick the right warmup file

The Composer tree uses Wilder RSI (path-dependent smoothing). The repo has
multiple Massive adjusted EOD bars files with different start dates:

- `pym-v5-massive-eod-adjusted-daily-bars-2015-01-01-2026-05-08.jsonl`
  (long warmup)
- `pym-v5-massive-eod-adjusted-daily-bars-2024-01-01-2026-05-08.jsonl`
  (short warmup)

`findLatestMassiveEodBarsPath()` sorts by `(endDate, startDate)` ascending and
returns the last entry. With the same `endDate`, the file with the **later
startDate wins** — i.e. the **2024 file** is what the live strategy service
uses.

This caught me out badly. My first round of backtests used the 2015 file
(longer warmup) because that's what I copied into the worktree first. Base
PYM showed `+55.56%` and several "winners" looked great. The dashboard at the
same time showed base PYM at `+80.77%`. Same Composer tree, same start date,
same end date — different bars file. Wilder RSI with 9 extra years of warmup
converges to different smoothed values, and the tree picks different `if`
branches in early 2025.

**Rule of thumb**: when you want to compare against the live service number,
verify you're reading the same bars file with `findLatestMassiveEodBarsPath()`.
Don't trust a backtest that disagrees with the dashboard until you've ruled
out warmup mismatch.

### 2. Always retest with at least two windows (full + OOS)

The "full window" 2025-01-02 → 2026-05-07 is the in-sample period the
Composer tree was built against. The "OOS 2026" window (2026-01-02 onward)
is the only stretch the tree authors couldn't have peeked at.

Several variants beat base PYM in the full window but failed in OOS — those
are likely overfit to the strong tech-bull stretches of mid-2025.

### 3. Apples-to-apples cost: 2 bps total turnover cost

All extension backtests use `2 bps` total cost (1 bp commission + 1 bp
slippage) applied to portfolio-weight turnover, matching the option overlay
suite and ML harness.

### 4. Docker container needs a rebuild to pick up code changes

The strategy-service runs in a Docker container with `container_name:
phenixflow-strategy-service`. To make a new strategy show up in the dashboard:

```bash
docker compose -f docker-compose.strategy-service.yml -p phenixflow up -d --build strategy-service
```

The `-p phenixflow` matches the project name so the existing container is
recreated rather than a new one in a different project.

## Ideas tested

The base PYM tree is a `wt-cash-equal` over **8 named Composer sleeves**:

1. Safe Sectors or Bonds
2. Gold
3. Emerging Markets
4. Volatility Short & Long
5. Rain's best signals
6. DereckN Hedge System
7. Zoop's QQQ FTLT
8. Simple Algos

Five overlays were tried — all implemented in
`projects/pym-v5-replication/src/extension-strategies-suite.js`:

| # | Idea | Implementation function | Verdict |
|---|---|---|---|
| 1 | Credit-spread risk-off (HYG/LQD, JNK/LQD, HYG/TLT) | `strategyCreditSpread` | Loses on live-service file |
| 2 | Sector momentum overlay (XLE/XLB/XLI/XLF/...) | `strategySectorMomentum` | Marginal/negative |
| 3 | RSI horizon gate (SPY RSI(2)/RSI(50)) | `strategyRsiHorizonGate` | Marginal |
| 4 | Breadth filter (% sector ETFs above SMA50) | `strategyBreadthFilter` | Cuts DD but gives up too much return |
| 5 | Sleeve meta-reweight by trailing Sharpe | `strategySleeveMeta`, `strategySleeveMetaCap`, `strategySleeveMetaDispersion`, `strategySleeveMetaAutoFloor` | **Winner (cap variant)** |

To run the full suite locally:

```bash
npm run pym-v5:extension-strategies -- --start 2025-01-02 --label your-label
```

To rebuild the supporting bars file for HYG/LQD/JNK and the sectors not in
the base PYM universe:

```bash
npm run pym-v5:build-extra-eod -- --start 2016-05-12 --end 2026-05-08
```

## Sleeve-meta deep dive

This is the only winning family, so it gets its own section.

### Concept

Each day, evaluate each of the 8 Composer sleeves independently. Score each
sleeve by its own trailing 21-day annualized Sharpe (using its own next-session
realized returns from the precomputed dataset). Allocate proportional to
`max(0, Sharpe)` — winning sleeves get more, losing sleeves go to zero (or to
the floor, depending on the variant).

Final weights are obtained by multiplying each sleeve's internal holdings by
its meta-weight and summing across sleeves, then renormalizing.

### Cap vs floor

Two ways to enforce diversification:

- **Floor**: every sleeve gets `≥ N%`. Forces capital into sleeves that are
  bleeding money — protects against the meta layer being wrong, at the cost
  of dragging on returns.
- **Cap**: no sleeve gets `> N%`. Prevents over-concentration without funding
  losers — if one sleeve has all the positive Sharpe, the cap forces it to
  share with the second/third best.

Backtested grid (live-service file, 2 bps, 2025-01-02 → 2026-05-07):

| Variant | Return | MaxDD | Sharpe | Turnover |
|---|---:|---:|---:|---:|
| `floor=0%` (no floor) | 117.85% | -12.90% | 2.257 | 53.6% |
| `floor=2.5%` | 110.81% | -11.29% | 2.374 | 48.8% |
| `floor=5%` | 103.54% | -10.97% | 2.474 | 44.2% |
| `floor=7.5%` | 96.08% | -10.77% | 2.530 | 39.7% |
| `floor=12.5%` (= base PYM) | 80.77% | -12.87% | 2.383 | 31.3% |
| `cap=25%` | **118.38%** | -11.20% | **2.824** | 49.4% |
| `cap=30%` | 121.29% | -11.24% | 2.764 | 50.3% |
| `cap=35%` | 122.44% | -11.26% | 2.686 | 51.2% |
| `cap=50%` | 125.71% | -11.71% | 2.509 | 52.7% |
| Dispersion-aware floor | 118.79% | -12.52% | 2.342 | 51.3% |
| Auto-floor (walk-forward, 63d) | 88.15% | -11.99% | 2.682 | 38.8% |

Across both windows (full + OOS 2026), `cap=25%` strictly dominates every
floor variant on Sharpe. The cap family monotonically improves return as the
cap rises (more concentration allowed) but Sharpe peaks at cap=25%.

OOS 2026 only (87 trading days):

| Variant | Return | MaxDD | Sharpe |
|---|---:|---:|---:|
| Base PYM | 20.07% | -5.99% | 3.148 |
| `floor=5%` | 30.96% | -9.86% | 2.932 |
| `cap=25%` | **43.56%** | **-5.92%** | **4.206** |

Cap=25% gives **2× the OOS return at the same drawdown** as base PYM.

The floor study runner is at `scripts/run-sleeve-meta-floor-study.js`.

### Why cap > floor (intuition)

- Floor: "every sleeve must hold at least N% — protect against picker error".
  Pays 5–10% per sleeve to losers regardless of evidence.
- Cap: "no sleeve over N% — prevent monoculture risk". Losers get 0%, winners
  must share with other positive-Sharpe sleeves. Forces diversification at
  the *top* of the distribution rather than at the bottom.

Cap=25% effectively requires at least four sleeves to hold positive weight.
That's the same diversification floor as `floor=12.5%` (8 × 12.5% = 100%, no
residual to allocate) but using only the four currently-best sleeves rather
than all eight.

### Caveat: ticker concentration is not capped

The 25% cap is **per sleeve**, not per ticker. If two sleeves both rotate to
VIXY on the same day (e.g. Volatility sleeve + Rain's signals both go
risk-off), the combined VIXY weight can exceed 25%. On 2026-05-08 the
strategy held VIXY at ~52%. That's expected behavior, not a bug. If you want
a per-ticker cap, that's a follow-up.

## Other ideas — why they failed

### Credit overlay (HYG/LQD)

When HYG/LQD 5-day return drops more than 1σ below its 21-day mean, scale
PYM to 50% and add BIL. Looked great on the **wrong** bars file (longer
warmup): cut DD from -12.87% to -5.26% with only -5pp return loss.

On the live-service file: gave up `-16pp` of return for the same DD reduction
that base PYM already had on its own. Net negative.

Interpretation: the credit signal was firing on the same calendar days the
short-warmup tree was already in defensive mode. Adding it on top didn't help.

### Sector momentum

Top-3 sectors by trailing 63-day return, blended 20-50% with base PYM. Won
OOS 2026 strongly (top 3 by Sharpe), but marginal in the full window. Not
robust enough — likely picking up the Q1 2026 mega-cap rotation specifically.

### RSI horizon gate

Defensive scaling when SPY RSI(50) is low; capped when SPY RSI(2) is very
high (overbought). Adds at most `+0.04 Sharpe` for `-12pp` return. The base
PYM tree is already RSI-driven, so this is mostly redundant.

### Breadth filter

Risk-off when fewer than `N%` of sector ETFs are above SMA(50). Cut DD
roughly in half (-12.87% → -5.25%) but gave up `33pp` of return. Different
risk profile rather than strictly better. Worth keeping as a researcher's
note: if you ever want a more conservative version of base PYM, breadth is
the right defensive overlay.

### Sleeve-meta with floor (vs cap)

Monotone in floor value: more floor → less return → better Sharpe up to ~7.5%
floor, then declining. Cap variant strictly dominates on Sharpe in both
windows. Dropped.

### Sleeve-meta auto-floor (walk-forward floor selection)

Each day pick the floor (from `{0, 2.5%, 5%, 7.5%, 10%}`) that would have
maximized trailing N-day Sharpe on prior data. Best variant (`autoLookback=63`)
hits Sharpe 2.682 — beats most static floors but still worse than `cap=25%`.
The walk-forward selector overfits on the small dataset.

## Implementation map

- **Strategy interpreter** for the Composer tree (per-sleeve evaluation
  reuses this): `src/symphony.js`
- **Indicators** (RSI Wilder, SMA, etc.): `src/indicators.js`
- **Base evaluator** (one strategy at a time, snapshots-shaped):
  `src/rebalance-report.js → buildDailyRebalanceReport`
- **Extension suite**: `src/extension-strategies-suite.js`
  - `precomputeContext` — evaluates base + each of the 8 sleeves once for the
    full date range, computes per-sleeve next-session realized returns
  - `runExtensionStrategiesSuite` — runs many strategies through the same
    daily loop with the same cost model
  - `buildExtensionRebalanceReport` — emits a snapshots/equity-series report
    in the same shape as `buildDailyRebalanceReport`, used by the service
    adapter
  - `strategySleeveMeta`, `strategySleeveMetaCap`,
    `strategySleeveMetaDispersion`, `strategySleeveMetaAutoFloor` — the four
    variants of the sleeve-meta family
  - `strategyCreditSpread`, `strategySectorMomentum`,
    `strategyRsiHorizonGate`, `strategyBreadthFilter` — the four other ideas
- **Service adapter**: `apps/strategy-service/src/strategies/pym-v5-extension.js`
- **Suite runner**: `scripts/run-extension-strategies-suite.js`
- **Floor study runner**: `scripts/run-sleeve-meta-floor-study.js`
- **Extra ticker fetcher** (HYG/LQD/JNK + missing sector ETFs):
  `scripts/build-extra-eod-daily-bars.js`

## NPM scripts added

- `npm run pym-v5:build-extra-eod` — fetch HYG, LQD, JNK and the sector ETFs
  (XLE, XLB, XLI, XLY, XLC, XLRE) from Massive REST. Run once with a long
  warmup window; the file is referenced as a sidecar by the credit and breadth
  strategies.
- `npm run pym-v5:extension-strategies` — run the full backtest suite. Output
  goes to `artifacts/pym-v5-extension-strategies-{label}.json`.

## Methodology checklist (next AI: read this)

Before declaring a new PYM extension strategy a "winner":

- [ ] Confirm `findLatestMassiveEodBarsPath()` picks the same file the live
      service uses (the file with the **later startDate** among tied endDate
      entries — typically the 2024-warmup file).
- [ ] Reproduce the live dashboard's base PYM number with your script. If it
      doesn't match, you're using a different bars file.
- [ ] Test on at least two windows: full (2025-01-02 onward) and OOS 2026
      only. Don't trust a strategy that wins one and loses the other.
- [ ] Be skeptical of "the only variant in a 5-variant grid that wins" —
      survivorship bias is real on small datasets.
- [ ] Cap is preferable to Floor for sleeve-meta-style strategies: it
      preserves diversification without funding losers.
- [ ] Verify the new strategy actually serves through the strategy service
      registry (`createDefaultRegistry().getStrategy(id).getReport()`) before
      declaring it done.
- [ ] If you want it visible in the dashboard, rebuild the Docker container
      with `docker compose -p phenixflow ... up -d --build strategy-service`.
- [ ] Write a research note like this one with what you tried, what you
      ruled out, and why. Future-you (or future-AI) will thank you.

## What was kept

- One strategy registered: `pym-v5-sleeve-meta-21d-cap25`.
- The full extension suite, all 5 strategy families, and the floor-study
  runner are kept in source so the next person can grid-search further or
  re-test against new data without rebuilding from scratch.
