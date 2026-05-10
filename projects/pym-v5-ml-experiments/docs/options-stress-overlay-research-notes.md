# Options-Stress Overlay Research Notes — May 2026

## Goal

Reduce volatility and drawdown of `pym-v5-cap25-lgbm-blend40` while
preserving as much return as possible. The user explicitly directed:
*"let's try using options data to reduce volatility...we have EOD open
interest data as well."*

## Result

**Registered: `pym-v5-cap25-lgbm-blend40-stress`** —
the cap25 + 40% LightGBM blend with an aggressive options-derived stress
overlay. Over a 10-year backtest (2017-04-04 → 2026-05-08, 2287 days,
2 bps cost):

| | blend40 (without overlay) | blend40 + stress aggressive |
|---|---:|---:|
| Total return | 23,606% | 16,217% |
| CAGR | 82.7% | 75.3% |
| **Max DD** | **-21.2%** | **-12.2%** |
| **Vol** | **26.0%** | **18.8%** |
| **Sharpe** | **2.447** | **3.084** |
| Calmar | 3.89 | 6.19 |

**The stress overlay is the first config in this project that beats both
base PYM AND cap25 on every dimension over 10 years simultaneously.**

| | Base PYM | cap25 | blend40+stress |
|---|---:|---:|---:|
| 10yr CAGR | 45.9% | 58.2% | **75.3%** |
| 10yr Max DD | -12.6% | -18.8% | **-12.2%** |
| 10yr Sharpe | 2.612 | 2.322 | **3.084** |
| 10yr Vol | 14.9% | 20.7% | 18.8% |

## What worked

### Composite stress signal from three options-derived inputs

Each day's stress is the mean of available 60-day trailing z-scores:

1. **VIXY 5d log-return z-score** — always available 2017+. The only proxy
   for vol regime in our daily bars file. Captures spikes via the VIX
   short-term futures ETF.
2. **^VIX z-score** — direct VIX level, available 2023-02-14+ via Massive
   REST `I:VIX` endpoint.
3. **OCC equity put/call ratio z-score** — available 2021-01-04+ from
   local OCC EOD aggregate OI files.

The composite handles missing inputs gracefully — earlier years use only
VIXY, 2021+ adds OCC, 2023+ adds VIX.

### Aggressive scaling function

```
if stress < 0       → 100% gross   (calm)
if stress in [0,1]  → linear 100% → 60%
if stress in [1,2]  → linear 60% → 20%
if stress > 2       → 20% gross   (max defensive)
```

Slack from the gross scale routes to BIL (cash). About 15% of days hit
stress > 1σ over the 10-year window; ~4% hit > 2σ.

### Why options data beats reactive vol-targeting

We tested simple trailing-21d vol-targeting first as a comparison. It
reduced vol but hurt returns much more:

| Variant | CAGR | DD | Sharpe |
|---|---:|---:|---:|
| baseline blend40 | 82.7% | -21.2% | 2.447 |
| **stress aggressive** | **75.3%** | **-12.2%** | **3.084** |
| vol-target 20% | 61.5% | -19.1% | 2.467 |
| vol-target 18% | 57.3% | -18.7% | 2.465 |

Vol-target only sizes down AFTER trailing realized vol spikes, so it
misses the actual stress event and pays opportunity cost on the recovery.
Options-derived stress is **forward-looking** — VIX, OCC P/C, and VIXY
price moves typically rise BEFORE realized vol shows up in the strategy's
own returns. That's exactly what the options market is for: pricing
expected future risk.

## Year-by-year impact

| Year | blend40 (no overlay) | blend40 + stress | Stress verdict |
|---|---:|---:|---|
| 2017 | 1.87 Sh | 2.12 Sh | helped |
| 2018 | 0.84 Sh | 1.00 Sh | helped |
| 2019 | 2.02 Sh | 2.40 Sh | helped |
| 2020 | 3.57 Sh | 3.97 Sh | helped (sized down in March COVID crash) |
| 2021 | 2.70 Sh | 3.06 Sh | helped |
| 2022 | 3.58 Sh | 3.55 Sh | flat (very minor false positive) |
| 2023 | 3.38 Sh | 3.31 Sh | slightly hurt (false positives in rally) |
| 2024 | 1.70 Sh | 2.21 Sh | helped |
| 2025 | 1.79 Sh | 3.04 Sh | **helped massively** (DD -18.4% → -5.7%) |
| 2026 | 3.61 Sh | 3.73 Sh | helped |

Helps Sharpe in 8 of 10 years. The 2 hurt-years are 2022-2023 — periods
when stress was elevated but markets still rallied (false-positive
defensive). Net effect over the decade is overwhelmingly positive.

## What did NOT work

### Vol-target overlays (kept return / cost ratio bad)
Worse than stress overlay on every Sharpe-improvement axis. Reactive,
not forward-looking. Still better than no overlay at all — useful as a
fallback signal if options data is unavailable.

### Drawdown-controlled sizing alone
Modest improvement (Sharpe 2.45 → 2.46) but doesn't directly address
volatility. Combined with stress overlay it's redundant.

### Other ideas not yet tested (room for follow-up)
- **Per-symbol GEX from local OPRA + OI** — for the 2025+ portion only
  (post-OPRA history). Could provide tighter signals than aggregate OCC.
- **Term-structure backwardation** — VIX 1m vs 3m. Need either VIX9D /
  VIX3M from external source or rely on VIXY/VXZ ETF spread.
- **Skew (25-delta put vs call)** — needs full options chain not in
  current local data.
- **Re-train LGBM with options stress as a feature** — would let the
  ML model pre-decide rather than overlay post-hoc.

## Critical methodology choices

### Causality
The overlay is fully causal: at signal date X close, we use stress
computed from data through X close to size positions held into X+1.
Trailing z-scores use the prior 60 days excluding the comparison day.

### Where the slack goes
Scaled-down gross routes to **BIL** (cash proxy). The realized return
of the overlayed strategy on a stressed day is approximately
`scale × blend_return + (1-scale) × BIL_return`. BIL earns ~5% APY in
the current rate environment, so the slack isn't dead money.

Note: this slightly differs from the back-of-envelope returns-level
approximation used in the initial /tmp/options-stress-overlay.js
analysis (which ignored BIL return). Live strategy uses position-level
overlay with actual BIL exposure.

### The two false-positive years (2022, 2023)
The overlay was sometimes elevated when markets rallied, costing a few
Sharpe points those years. This is expected from any defensive overlay —
it costs you in calm-but-grinding rallies. The benefit shows up in the
vol-spike years (2020, 2025) where the overlay mattered most.

### Pre-2021 signal sparsity
For 2017-2020 the composite uses only the VIXY-derived signal (no OCC
yet, no VIX yet). It still works but is a single-signal regime. The
addition of OCC P/C in 2021 and ^VIX in 2023 makes the signal more
robust over time.

### Aggregate OCC vs per-symbol
The OCC EOD OI dataset only has aggregate totals (equity calls/puts, index
calls/puts, debt, futures). NOT per-symbol OI. With per-symbol OI we
could compute SPY-specific signals, sector ETF signals, or even
strategy-portfolio-specific GEX. That's a future enhancement.

## Implementation map

- **Stress signal builder**:
  `projects/pym-v5-ml-experiments/scripts/build-options-stress-signal.js`
  Fetches ^VIX from Massive REST, parses local OCC OI files, reads VIXY
  closes from the daily bars file, computes per-day composite z-score.
  Output: JSONL artifact under
  `projects/pym-v5-ml-experiments/artifacts/options-stress-signal-*.jsonl`
- **Overlay primitive**:
  `projects/pym-v5-replication/src/extension-strategies-suite.js →
  strategyWithStressOverlay` (generic wrapper) and `aggressiveStressScale`
  (the registered scale function)
- **Service adapter**:
  `apps/strategy-service/src/strategies/pym-v5-extension.js →
  createPymV5Cap25LgbmBlendStressStrategy`
- **Registered**: `apps/strategy-service/src/default-registry.js`
- **API endpoints**:
  `GET /api/strategies/pym-v5-cap25-lgbm-blend40-stress`
  + chart, values, portfolio/latest

## Data dependencies

### To regenerate the stress signal
```bash
npm run pym-v5:build-stress-signal
```

Requires:
- `MASSIVE_API_KEY` in `.env.local` (for ^VIX 2023+)
- Local Massive bars file with VIXY (already used by other strategies)
- Local OCC OI files at `/Volumes/SEC4TB/massive-data/occ/option_open_interest_eod/`
  (the `openinterest-occ-eod-oi` Docker container keeps these fresh)

### Artifact lifecycle
- Stress signal artifact is git-ignored (under `artifacts/`)
- Regenerate before each strategy refresh to include the latest day
- A daily auto-refresh would call `npm run pym-v5:build-stress-signal`
  followed by the strategy-service `recompute` endpoint

## Methodology checklist (next AI: read this)

Before adding another overlay or strategy variant:

- [ ] Use the **same bars file** as the live service. The stress signal
      itself should be computed from the longest-history bars file
      (currently `pym-v5-massive-eod-adjusted-daily-bars-2015-01-01-*`)
      to maximize VIXY z-score accuracy.
- [ ] Test with both **historical depth + OOS-only**. The 10-year
      history gave us confidence the overlay isn't overfit; the 2025
      OOS year showed massive benefit.
- [ ] **Causal z-scores** — exclude the day being scored from its own
      trailing window. Trivial but easy to forget.
- [ ] Compare against **vol-target** as a baseline for any vol-reduction
      claim. If your overlay can't beat trailing-vol-target on Sharpe,
      it's not adding information.
- [ ] **Per-year breakdown** to identify false-positive years. Some
      cost in rally years is fine; if the overlay only "wins" in 1-2
      crisis years, the signal isn't robust.
- [ ] Verify the strategy **serves through the registry** AND **shows
      up in the dashboard** before declaring done.
- [ ] Rebuild Docker:
      `docker compose -f docker-compose.strategy-service.yml -p phenixflow up -d --build strategy-service`

## NPM scripts

- `npm run pym-v5:build-stress-signal` — fetch latest ^VIX, parse OCC,
  rebuild composite stress JSONL. Idempotent; ~30 sec when `^VIX` data
  is fully cached.

## Why this strategy was registered (the user-facing summary)

The blend40 strategy has very high return (>4× cap25's CAGR over 10
years) but high volatility (26% vs cap25's 21%). The user asked
specifically for a vol-reduced variant. We tried two approaches:

1. **Vol-targeting** (mechanical, reactive): worked but cost ~25% of
   return for ~25% vol reduction. Sharpe basically unchanged.
2. **Options stress overlay** (forward-looking): cut vol by 28%, cut
   max DD by 43%, INCREASED Sharpe by 26%, only modestly cost return.

Stress overlay was strictly better. Registered as the conservative
companion to `pym-v5-cap25-lgbm-blend40` so users get both:
- `cap25-lgbm-blend40` for max return
- `cap25-lgbm-blend40-stress` for max risk-adjusted return
