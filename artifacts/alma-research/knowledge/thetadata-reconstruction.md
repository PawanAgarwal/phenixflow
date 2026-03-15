# ThetaData Reconstruction

## Goal

Recreate an OptionDepth-like structural map inside PhenixFlow using ThetaData-backed option, quote, OI, and greek data so we can make the same class of deductions Alma makes about:

- liquidity zones
- realized-volatility expansion or suppression
- pivots and centroids
- vanna/speed flip rejection levels
- hidden spread structure
- mechanical versus organic trend behavior

This is not a literal order-book depth reconstruction. It is a synthetic options-positioning and greek-surface reconstruction.

## What data we already have

From the current schema and docs:

- `options.option_quote_minute_raw`
  - contract-level bid/ask/last by minute
- `options.option_open_interest_raw`
  - contract-level OI by day
- `options.option_greeks_minute_raw`
  - delta, gamma, theta, vega, rho, IV, underlying price
- `options.option_calculated_greeks_minute`
  - locally solved IV and first-order greeks with explicit rate/dividend inputs
- `options.stock_ohlc_minute_raw`
  - underlying minute bars
- `options.reference_sofr_daily`
  - risk-free rate source

Repo notes that matter:

- we have first-order greek reconstruction already
- rate and dividend assumptions are explicit and configurable
- we do not have true market depth or the proprietary OptionDepth feed

## What ThetaData does not give us directly

At least from the current repo design and entitlement docs, we should assume we do not have:

- proprietary OptionDepth heatmaps
- full live order-book depth for SPX options
- direct higher-order greek surfaces like speed, color, zomma, vomma, or charm in stored tables

So we need to compute the higher-order layer ourselves.

## Translation from Alma's product to PhenixFlow

### Alma concept -> local proxy

- OptionsDepth heatmap -> end-of-day exposure surface across hypothetical spot levels
- speed profile -> finite-difference slope of aggregated gamma profile
- color -> finite-difference time decay of aggregated gamma profile
- vanna flips -> zero-crossings of aggregated vanna surface
- speed flips -> zero-crossings of aggregated speed surface
- centroid -> center of mass of weighted net speed or realized-volatility pressure surface
- supportive/suppressive clusters -> local maxima/minima in exposure density and finite-difference pressure

## Recommended reconstruction pipeline

### 1. Build an end-of-day snapshot

For each symbol-day:

- use the last liquid minute before the close or a fixed EOD snapshot minute
- join:
  - quote midpoint
  - OI
  - spot
  - IV
  - first-order greeks
  - risk-free rate
  - dividend assumption

This should be the canonical next-session planning snapshot, because Alma explicitly prefers end-of-day structure over intraday updates.

### 2. Build a hypothetical spot grid

For each symbol-day, define a spot grid around the EOD close:

- example: `spot_close * (1 + pct_move)` for pct moves from `-10%` to `+10%`
- use coarse canary grids first, then refine after validation
- for heavy names, keep the grid bounded and chunked to stay inside the repo's memory guardrails

The point is to ask:

- if spot moved here tomorrow, what would aggregated gamma, vanna, and related pressure look like?

### 3. Reprice each contract on that grid

For every contract in the EOD chain:

- use current IV as the base state
- compute first-order greeks on each hypothetical spot node
- compute time-forward versions for next-session decay
- optionally compute IV-shocked versions for `vol up` and `vol down` scenarios

We already have the ingredients for this from:

- quote midpoint
- spot
- strike
- expiry
- IV
- rate/dividend

### 4. Aggregate to a surface

Aggregate by spot node, weighted by:

- OI
- and optionally a hybrid weight that includes recent volume or trade activity

Suggested outputs per spot node:

- net delta exposure
- net gamma exposure
- net vega exposure
- skewed call/put contribution
- upside/downside asymmetry

Then derive second-order and mixed terms from the aggregated surface.

## Finite-difference greek layer

These are the key local derivatives we need.

### Speed

Definition:

- `speed = dGamma / dS`

Finite-difference approximation:

```text
speed(S) ~= [Gamma(S + dS) - Gamma(S - dS)] / (2 * dS)
```

Use:

- proxy for expected local liquidity change
- identifies where realized volatility should compress or accelerate

### Color

Definition:

- `color = dGamma / dt`

Finite-difference approximation:

```text
color(t) ~= [Gamma(t + dt) - Gamma(t)] / dt
```

For our use case, `dt` should represent the next session decay horizon.

Use:

- gamma decay map
- forward pressure on next-session realized volatility
- higher priority than charm in Alma's framework

### Charm

Definition:

- `charm = dDelta / dt`

Finite-difference approximation:

```text
charm(t) ~= [Delta(t + dt) - Delta(t)] / dt
```

Use:

- lower priority than color for this methodology
- mostly useful around later-session flows and power-hour behavior

### Vanna

Equivalent views:

- `dDelta / dSigma`
- `dVega / dS`

Finite-difference approximation:

```text
vanna(S) ~= [Vega(S + dS) - Vega(S - dS)] / (2 * dS)
```

or

```text
vanna(sigma) ~= [Delta(sigma + dVol) - Delta(sigma - dVol)] / (2 * dVol)
```

Use:

- identify vanna flip lines
- reason about spot/IV reflexivity and skew behavior

### Zomma

Definition:

- `zomma = dGamma / dSigma`

Finite-difference approximation:

```text
zomma ~= [Gamma(sigma + dVol) - Gamma(sigma - dVol)] / (2 * dVol)
```

Use:

- understand how gamma changes when IV moves
- important for rejection/absorption around flip zones

### Vomma

Definition:

- `vomma = dVega / dSigma`

Finite-difference approximation:

```text
vomma ~= [Vega(sigma + dVol) - Vega(sigma - dVol)] / (2 * dVol)
```

Use:

- map whether vol sellers or buyers are likely to get trapped
- estimate whether IV moves should reinforce or fade

## Synthetic OptionDepth view

The synthetic view should not try to mimic a literal proprietary UI. It should try to preserve the decision-useful content.

Recommended panels:

### 1. Spot-node heatmap

Rows or x-axis:

- hypothetical spot levels

Columns or layers:

- net gamma
- speed
- color
- vanna
- zomma
- vomma

### 2. Cluster map

Detect:

- local supportive clusters
- local suppressive clusters
- pinning zones
- low-liquidity gaps
- zero-vanna and zero-speed crossings

### 3. Distribution summary

Produce:

- centroid
- upside pivot
- downside pivot
- expected supportive and unstable zones
- left/right skew of the implied structure

## How to infer hidden structure from the surface

This is the most important modeling step.

We are not just measuring greek magnitudes. We are inferring pattern geometry.

Examples:

- center-heavy short-vol with stabilizing wings can imply rangebound behavior
- asymmetric downside long-speed or long-zomma can imply fragile downside liquidity
- positive speed profile with more upside liquidity can imply drift-up with failed downside breaks
- negative speed profile can imply upside instability and stronger reversion pressure

The practical detector should therefore look for:

- sign changes
- asymmetry between upside and downside nodes
- concentration versus dispersion of exposure
- how the profile changes under small time decay and small IV shocks

## Intraday validation layer

Once the EOD structure exists, the intraday validator should compare expectation versus realized behavior.

Signals to compare:

- actual intraday realized volatility versus expected speed/color regime
- actual IV change versus vomma/zomma setup
- actual skew move versus vanna expectation
- spot/RV beta versus expected long-speed or short-speed state
- whether price stabilizes near the projected supportive cluster
- whether price accelerates through a zone that should have absorbed flow

This is how Alma uses the framework in practice:

- the premarket structure gives the map
- intraday RV and IV tell you if the map is still valid

## Suggested tables

If we later implement this, a clean first cut would be:

- `options.option_surface_profile_eod`
  - one row per symbol-day-spot-node
- `options.option_surface_profile_minute`
  - optional intraday refresh version
- `options.option_surface_regime_signal`
  - centroid, pivots, flips, skew, regime flags, validation state

Suggested fields:

- `symbol`
- `trade_date_utc`
- `snapshot_ts_utc`
- `spot_node`
- `net_gamma`
- `net_speed`
- `net_color`
- `net_vanna`
- `net_zomma`
- `net_vomma`
- `centroid_flag`
- `pivot_type`
- `supportive_score`
- `suppressive_score`
- `expected_rv_regime`
- `expected_iv_regime`

## Expected limitations

We should be explicit about what this reconstruction can and cannot do.

### What it can do well

- recreate the structural, next-session map Alma actually relies on
- identify pivots, flips, and asymmetric risk zones
- compare expected versus realized intraday behavior
- produce the same class of directional/liquidity deductions

### What it cannot do one-to-one

- replicate the proprietary OptionDepth product exactly
- reconstruct true live book depth from ThetaData alone
- know customer intent with certainty from static chain snapshots

### Main modeling risks

- OI is end-of-day, so same-day opening repositioning is not visible in advance
- higher-order derivatives can be noisy if spot or IV steps are too coarse
- weighting choice matters: OI-only and volume-adjusted views may disagree
- dealer/customer sign inference is partly heuristic unless trade-direction context is added

## Implementation posture for this repo

Follow the repo guardrails:

- use bounded spot grids and chunked processing
- keep the first implementation end-of-day only
- validate on one symbol-day canary before wider runs
- do not block on a full backfill before testing the surface logic
- store intermediate outputs in idempotent tables keyed by symbol-day-snapshot

## Recommended first milestone

The best first milestone is not a full UI. It is a daily research artifact for one symbol.

Deliverable:

- EOD chain snapshot
- spot grid
- aggregated gamma/speed/color/vanna/zomma/vomma surface
- centroid and pivots
- one-day intraday validation against realized volatility

If that works on a canary name like `SPY` or `QQQ`, we can then decide whether to formalize it into a table and UI layer.
