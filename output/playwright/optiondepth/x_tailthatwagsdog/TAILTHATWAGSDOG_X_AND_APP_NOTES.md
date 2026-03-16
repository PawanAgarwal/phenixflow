# TailThatWagsDog X And nextSignals App Notes

Captured on 2026-03-15 from X posts by `@TailThatWagsDog` and the linked public Netlify app.

Purpose: document what this account adds to our understanding of what can realistically be built from `GEX`-style positioning data, how such data can be used, and what this means for our own OptionDepth replication and backtest plans.

## Executive Summary

Yes, this added meaningful new information.

The biggest additions are:

- a credible public/commercial benchmark exists for a useful `OD`-adjacent product, even if it is not a literal OptionDepth clone
- the highest-value use case is `regime classification`, not just static support and resistance
- `gamma` alone is not the main object; the user repeatedly emphasizes `gamma + vanna`, spot/vol interaction, and sign disagreement versus naive dealer assumptions
- `limit order book` or other stronger directional classification appears to matter a lot; one post says `43.6%` of `LOB`-traded contracts disagreed with the naive dealer assumption, and the linked app shows disagreement above `50%`
- the model is being used for:
  - `pinning vs squeeze` classification
  - `amplifying vs dampening` regime labeling
  - downside crash-fragility analysis
  - risk-neutral forecast conditioning
  - intraday band trading in short-dated products

My read is that this is one of the strongest public hints yet that a useful public-data or commercial-data positioning product is very feasible, but that the biggest jump over naive `OI * gamma` comes from better sign classification and adding `vanna` and `IV-shock` structure.

## Source Set

Primary X posts reviewed:

- [2026-03-15 major update with public app](https://x.com/TailThatWagsDog/status/2033161694441013310)
- [2026-03-04 risk surface using gamma, vanna, and LOB transactions](https://x.com/TailThatWagsDog/status/2029200688748605453)
- [2026-03-02 post on naive dealer-assumption disagreement vs LOB data](https://x.com/TailThatWagsDog/status/2028664372260663609)
- [2026-03-02 post on live GEX-based signals and regime-change model](https://x.com/TailThatWagsDog/status/2028470121123963194)
- [2026-03-01 intraday gamma-band backtest post](https://x.com/TailThatWagsDog/status/2028230160894263507)
- [2026-02-03 high pinning vs low squeezes post](https://x.com/TailThatWagsDog/status/2018760964993134789)

Linked public app reviewed:

- [nextSignals Directional Index Crash Risk Monitor](https://snazzy-concha-bb8adb.netlify.app/)

Local evidence captured:

- [heatmap.png](/Users/pawanagarwal/github/phenixflow/output/playwright/optiondepth/x_tailthatwagsdog/screenshots/heatmap.png)
- [risk_analysis.png](/Users/pawanagarwal/github/phenixflow/output/playwright/optiondepth/x_tailthatwagsdog/screenshots/risk_analysis.png)
- [crash_risk.png](/Users/pawanagarwal/github/phenixflow/output/playwright/optiondepth/x_tailthatwagsdog/screenshots/crash_risk.png)
- [forecast.png](/Users/pawanagarwal/github/phenixflow/output/playwright/optiondepth/x_tailthatwagsdog/screenshots/forecast.png)

## What He Appears To Be Building

This does not look like a literal OptionDepth clone.

It looks more like a `public/commercial-data positioning stack` built around:

- contract-level or chain-level `GEX`
- `vanna`
- a stronger-than-naive directional sign model
- `LOB`-aware or trade-classification-aware overrides
- spot/vol stress testing
- probability forecasts derived from options prices

The linked app is especially important because it is public and explicit about outputs:

- `GEX (DIR IDX)`
- `VEX (DIR IDX)`
- `GEX+`
- `VGR`
- `ZERO-Γ`
- a `spot move × IV shock` risk surface
- `crash risk` tables at drawdown levels
- `1-day`, `1-week`, and expiry-horizon forecasts

That means the target is broader than a simple heatmap. It is a `market-structure dashboard` that tries to turn options positioning into regime labels and conditional forecasts.

## How He Uses GEX-Style Positioning Data

### 1. Regime classification first

His repeated framing is not "where is one gamma wall?" but:

- `high pinning vs low squeezes`
- `amplifying vs dampening`
- when a market is structurally fragile versus structurally absorbing flow

That is a regime model.

This is important for us because it suggests the most defensible predictive claim is not precise point forecasting, but identifying:

- whether moves should self-correct or self-reinforce
- whether downside should cascade
- whether realized volatility should compress or expand

### 2. Naive dealer assumptions are treated as a serious failure mode

This was one of the clearest new findings.

The account explicitly says that `43.6%` of `LOB`-traded contracts disagreed with the naive dealer assumption for one `SPX` expiration, and the public app shows disagreement above `50%` in all three reviewed months.

That implies:

- raw `OI * gamma` is not just noisy
- it can be sign-wrong on a very large fraction of contracts
- any serious build should plan for a `sign-disagreement audit`

This lines up with what Alma says about naive `OI` inference, but here it is tied to a concrete public benchmark app and an explicit `LOB` comparison.

### 3. Gamma should be combined with vanna and IV-shock logic

The app's most important visualization is not a time-by-price heatmap. It is a `spot move × IV shock` surface.

That says the intended use is:

- ask what happens if spot drops and IV rises
- ask what happens if spot rallies and IV compresses
- map those joint shocks into dealer dampening or amplification

This is a more general risk view than a plain gamma map.

The strongest practical takeaway is:

- if we only model `gamma` as a function of spot and time, we miss a major part of the framework
- we also need `vanna` and an explicit spot/vol coupling layer

### 4. The map is used for downside asymmetry and crash fragility

The public app repeatedly frames the downside as the key asymmetry:

- soft floor
- hard ceiling
- downside breach that becomes self-reinforcing
- crash amplification when spot declines coincide with IV expansion

This is not just "support and resistance."

It is a claim that options positioning can identify when the market is vulnerable to `nonlinear downside follow-through`.

### 5. He uses GEX for trading signals, not just background context

The account makes several explicit use claims:

- a live `pinning vs squeeze` signal stream
- a regime-change model
- potentially a `long-short SPX algorithm`
- intraday `gamma exposure bands` for short-dated trading

The `SPY 1DTE ATM` gamma-band post is especially concrete:

- he frames it like Bollinger-style bands driven by GEX
- the backtest intentionally waits until `10:00 AM`
- trades are flattened before the close

That gives us a very practical backtest template:

- avoid open volatility
- test same-day short-dated band mean-reversion or breakout logic
- flatten by end of day

### 6. Risk-neutral forecasts should be adjusted by regime

The app's `FORECAST` tab is unusually explicit.

It says:

- the probability tables are `risk-neutral`
- they come from `Breeden-Litzenberger` density extraction and `Cornish-Fisher` adjustment
- then they are `qualitatively conditioned` by the `GEX+` regime

This is a useful modeling split:

- option prices provide the market-implied distribution
- positioning data changes how much we trust different parts of that distribution

That is a strong idea we should keep:

- use options prices for the baseline distribution
- use positioning data for `regime-conditioned interpretation`

## What Appears Public Or Commercial Versus Proprietary

Publicly or commercially observable inputs mentioned by the account:

- `SPX options chain data`
- `limit order book (LOB)` data
- `GEX` data from `@SqueezeMetrics`
- `GEX` data from `@Signal_Sigma`

Outputs that still appear proprietary:

- `Directional Index`
- `Complexity Index`
- `GEX+`
- `VGR`
- exact disagreement logic and weighting

So the lesson is not that the entire model is public.

The lesson is that a strong public/commercial benchmark can be built from non-exchange-secret inputs, while the final sign model and overlays remain proprietary.

## What This Adds To Our OptionDepth Knowledge

Relative to our earlier OptionDepth notes, the new useful additions are:

- a strong public benchmark exists for turning positioning data into a `usable dashboard`
- a serious build should support both:
  - `time × price` surfaces like `OD`
  - `spot move × IV shock` surfaces like `nextSignals`
- `LOB` or materially better trade-direction classification is probably one of the highest-value upgrades over naive public models
- predictive value is strongest when expressed as:
  - regime
  - asymmetry
  - pinning vs squeeze
  - crash fragility
  - conditional interpretation of risk-neutral probabilities

This makes me more confident that we should not think of the end goal as "just recreate the chart."

The better target is:

- reconstruct the surface
- derive regime labels
- derive conditional forecast commentary
- backtest regime-dependent outcomes

## Data And Feed Implications For Us

### Minimum useful stack

- underlying index prices such as `SPX`
- full listed options chain
- open interest
- first- and higher-order greeks
- a fitted IV surface

This is enough for:

- broad `GEX`
- `vanna`
- zero-gamma and related levels
- public-style heatmaps
- baseline risk-neutral distributions

### Better stack

- quote-aware trades
- aggressor-side classification
- spread-leg filtering
- `LOB` data or another richer directional-classification feed

This is the most important upgrade if we want to get closer to the `Directional Index` idea.

### Best non-proprietary benchmark target

- build a public `gamma + vanna` regime dashboard
- include sign-disagreement diagnostics versus naive assumptions
- add `spot × IV shock` surfaces
- add `Breeden-Litzenberger` forecast extraction
- use positioning only as a regime-conditioning overlay, not as a fake point-forecast engine

## What I Would Backtest Based On This

These are the most concrete testable ideas from the posts and app:

- `pinning vs squeeze` classification and subsequent realized range / breakout behavior
- naive-sign versus improved-sign model performance
- downside `spot down + IV up` zones and subsequent realized downside acceleration
- whether negative `gamma + vanna` regimes understate downside in the raw risk-neutral distribution
- intraday `GEX band` strategies on `SPY` or `SPX`, starting after `10:00 AM` and flattening by the close

## Caveats

- The posts include strong performance language, but I did not independently verify those claims.
- The public app exposes results and some methodology, not the full production recipe.
- The account blends public data, commercial vendor data, and proprietary overlays.
- This is best treated as a `high-signal public benchmark`, not proof that a full OptionDepth clone is trivial.
