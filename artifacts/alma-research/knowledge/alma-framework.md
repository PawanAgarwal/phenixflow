# Alma Framework

## What she is trying to predict

Alma is trying to forecast the next session's:

- likely liquidity map
- realized volatility regime
- direction of mechanical dealer pressure
- support/resistance zones where the market is most likely to pin, reject, or accelerate
- conditions under which the priced-in structure is being confirmed or broken

Her repeated point is that raw put/call ratios, volume ratios, and intraday flow changes are lagging. She prefers to reverse-engineer the structure already priced into the option chain, then use intraday behavior only as confirmation or invalidation.

## Daily workflow she describes

From `A Guide to Reading My Daily Posts`, the daily package is:

1. Premarket OptionsDepth heatmap
2. Script input lines with assigned probabilities
3. Speed profile description
4. Vanna flip and speed flip levels

That implies a two-step process:

- premarket: estimate the structure
- intraday: test whether spot, IV, and RV behave as that structure implies

## OptionDepth meaning in her framework

What matters to her is the end-of-day summary view, not intraday refresh noise.

Key deductions from the post set:

- The heatmap is meant to reveal the structure traders priced for the next session.
- She uses it to decode hidden spread structures such as butterflies, risk reversals, condors, ratio spreads, and related local clusters.
- Those structures are then turned into centroids, pivots, targets, pinning zones, and supportive/suppressive areas.
- She explicitly says intraday updates are mostly sentiment information and are not needed to plan the session.

So the proprietary edge is not "live depth" in the order-book sense. It is a forward structural map inferred from option positioning.

## Core concepts

### 1. Volatility as a liquidity proxy

From `What is volatility?` and `Liquidity structure`:

- realized volatility is treated as an indirect read on local liquidity
- low volatility means the market is spending time in a liquid, accepted area
- high volatility means price is moving quickly through thin or poorly accepted territory

This is a central bridge in her work:

- gamma is treated as realized-volatility management
- vega is treated as implied-volatility management
- liquidity and realized volatility are inverse to each other

### 2. Speed

Her definition:

- speed is the slope of the gamma curve with respect to spot
- it is effectively a "realized-volatility smirk"
- it tells her where the market expects liquidity to increase or decrease

Why it matters:

- it gives context for where dealer hedging should compress or expand realized volatility
- it is the main environmental map in the daily letters
- it can be used as a proxy for expected liquidity

Interpretation she repeats:

- compare expected speed-driven RV behavior with actual intraday RV behavior
- if the market behaves opposite to the profile, liquidity migrated and the structure is repricing

### 3. Color

Her definition:

- color is gamma decay
- equivalently, the slope of the charm curve or time change in gamma

Importance:

- she says color is more important than charm for most daily work
- charm matters mainly near the power hour
- color directly affects local and daily realized volatility because it tells you how gamma is changing over time

This means her framework is not only spatial across spot levels. It is also temporal across the session and across the next day.

### 4. Vanna, zomma, vomma

She uses these to interpret how the spot/IV relationship should behave:

- vanna maps how vega changes with spot, and therefore how skewed the volatility response should be
- zomma links gamma to volatility changes
- vomma maps how vega responds to IV changes

She repeatedly uses them to infer:

- whether IV sellers will get trapped
- whether a move is likely to stabilize or keep accelerating
- whether downside or upside vol should be sold or bought

### 5. Centroid and pivots

Her centroid is the center of mass of the net realized-volatility skew or speed surface.

Meaning:

- around the centroid, dealer hedging effects are close to balanced
- customer flow dominates the immediate behavior there
- centroid is therefore the main daily pivot

Her upside and downside pivots are local centroids where supply-demand dynamics should change. She uses them as the points where the market tests whether the priced-in structure still holds.

### 6. Hidden patterns

She interprets the chain as hidden spread structure.

Repeated examples:

- long or short iron butterflies
- risk reversals
- ratio call spreads
- ratio put spreads

The point of these structures in her framework is not naming them for their own sake. It is using their greek geometry to infer:

- where realized volatility should expand
- where it should compress
- where the market expects more liquidity
- which side is more likely to trend versus revert

## Her prediction loop

This is the most reusable part of the methodology.

### Premarket

- infer hidden structure from option positioning
- estimate centroid, pivots, targets, and distribution shape
- determine whether the environment is net long-speed or short-speed, long-color or short-color, etc.

### During the session

Watch whether:

- realized volatility behaves as expected near the pivots
- spot/RV beta behaves as expected
- IV and skew react the way the structure implies
- the market stabilizes where it was supposed to stabilize
- the market accelerates where it was supposed to accelerate

### If it matches

- structure is being validated
- keep trading with the expected regime

### If it diverges

- liquidity migrated
- sentiment changed
- the priced-in structure is being repriced
- reversion or trend continuation probabilities must be revised

## Concrete examples from the reviewed posts

### January OpEx week quant breakdown

This free post shows her framework in action:

- net speed profile negative
- centroid around the balanced zone
- explicit downside and upside pivots
- dealer gamma, color, zomma, vega, vomma, vanna read together
- sentiment projection by day
- "vomma projected zones" used as expected spot-volatility travel bands

This is a strong indication that her product is really a structured greek/liquidity forecast layer rather than a simple exposure chart.

### OpEx, VIXpery and PCE week

This post shows how she uses:

- net speed profile sign
- local gamma and color sign
- local net vega/veta/vomma structure
- vanna sign within and outside the range

to describe whether the market expects:

- downside stabilization
- false breakdowns
- upside pain trade
- mechanical rangebound behavior with weak organic conviction

### Window of Risk Pt. 2

This post shows the higher-level use case:

- she uses flows, RV suppression, zomma/vomma supply, and SPX/VIX/VVIX behavior to identify when the market is being held together mechanically
- she explicitly treats those mechanical flows as a reason volatility is not squeezing despite visible fragility

## What matters most for implementation

The strongest method rules from the reviewed sources are:

- Use end-of-day structure for next-session planning.
- Treat intraday chain changes as secondary and mostly sentiment-oriented.
- Model speed and color, not just gamma and delta.
- Convert exposure geometry into centroid, pivots, supportive clusters, and projected zones.
- Use actual intraday RV and spot/IV behavior to confirm or reject the premarket map.
- Focus on hidden spread structure and distribution shape, not isolated greek values.

## What this implies for us

If we want to emulate the method with ThetaData, the minimum viable product is:

- an end-of-day structural surface
- local reconstruction of higher-order greek behavior
- derived centroid/pivot/flip logic
- an intraday validator that compares actual RV and IV behavior with the expected profile

That is enough to get the same style of deductions even without the proprietary OptionDepth subscription.
