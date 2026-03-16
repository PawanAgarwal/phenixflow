# Alma Calculation Method Notes

Captured on 2026-03-15 from `artifacts/alma-research`.

Purpose: summarize how Alma says she calculates the main concepts she uses on top of OptionDepth, what appears to come directly from OptionDepth or related positioning data, what she derives herself, and what parts of the full recipe remain intentionally undisclosed.

## Executive Summary

Alma does not appear to use OptionDepth as a finished signal package.

She starts from the positioning and greek structure, then derives her own layer on top of it:

- `speed`
- `color`
- `centroid`
- `upside/downside pivots`
- `vanna flips`
- `speed flips`
- `hidden pattern` labels such as flies, risk reversals, condors, and ratio spreads
- probability bands and scenario levels

She is fairly explicit about the mathematical objects:

- `speed` is the slope of gamma with respect to spot
- `color` is gamma decay, or the time-slope of gamma
- `centroid` is the center of mass of the realized-volatility skew / speed profile
- `pivots` are local centroids of `ATM` vs `22-delta` skews
- `vanna flips` are zero-vanna points whose location moves with IV

But she also withholds parts of the implementation:

- exact weighting
- exact tenor aggregation
- exact numerical reduction from her `3D speed surface` to the 2D levels she publishes
- exact mapping from higher-order derivatives to some of her local pivot lines

So the archive gives us a strong conceptual and partial mathematical specification, but not a literal full formula sheet.

## What Comes From OptionDepth Versus Alma

What she appears to take from OptionDepth or equivalent directional positioning data:

- gamma / charm / vanna structure
- strike-by-strike and expiry structure
- directional dealer/customer positioning read
- the base heatmap and exposure geometry

What she says she derives on top:

- speed profile
- color interpretation
- 3D speed-surface centroids
- local pivots and targets
- flip levels
- hidden spread classification
- probability lines
- scenario ranking and narrative

## 1. Speed

### Explicit

Alma repeatedly says:

- `speed` is the derivative of gamma with respect to spot
- it is the slope of the gamma curve
- it is a proxy for expected liquidity because gamma hedging affects realized volatility

In normal notation:

```text
speed = dGamma / dS
```

She also describes it as a `realized-volatility smirk`.

### What she says she does with it

She says she measures:

- the shape of the speed profile
- its sign
- its skew
- how realized volatility behaves in each segment relative to what the speed profile implied

She also explicitly says:

- she uses speed for short-term trades
- centroid and pivots come out of the speed / realized-volatility structure

### Important extra detail

In `A Guide to Reading My Daily Posts`, she says:

- she is "measuring the fourth and fifth derivatives"
- she does this while taking into account time decay and volatility changes
- this produces slopes and hidden-pattern classifications such as a `net hidden customer short butterfly spread`

That suggests her published "speed profile" is not just a naive first finite difference of net gamma. It is a higher-order, adjusted structural object.

## 2. Color

### Explicit

Alma defines `color` as:

- gamma decay
- the slope of the charm curve
- the time-change of gamma

In normal notation:

```text
color = dGamma / dt
```

She also references `Schwarz theorem` and describes color as `charm convexity`.

### What she says she does with it

- She prioritizes color over charm for daily work.
- She uses color to judge whether gamma should increase or decay as time passes.
- She ties color directly to local and daily realized-volatility behavior.

She also infers color from charm convexity in some chat explanations:

- if charm changes from negative to positive as spot rises, she calls that positive charm convexity
- from that she infers positive color and rising gamma

So in practice, she seems to use both:

- direct color reasoning
- indirect inference from how charm changes across spot

## 3. Centroid

### Explicit

Alma defines the centroid as:

- the center of mass of the entire realized-volatility skew
- the balance point of the whole speed profile
- the area where aggregated dealer hedging flow is nearly balanced

In her own words, it "cuts the whole profile in half" at the balance point of vol-expansion and vol-suppression bets.

### What she says she calculates

She explicitly says that:

- after accounting for gamma decay and net changes to expected volatilities
- she calculates the `net centroids of the three-dimensional speed surface`
- then reduces them to two dimensions

This is one of the clearest places where she signals a real internal model while withholding the exact implementation.

### Practical meaning

For Alma, centroid is:

- the most important daily pivot
- the place where mechanical flows are weakest
- the zone where customer flow gives the clearest sentiment read

## 4. Upside And Downside Pivots

### Explicit

She says the upside and downside pivots are:

- local centroids
- mathematically tied to `ATM` and `22-delta` skews
- derived from the fourth and fifth derivatives, then netted

So her pivots are not arbitrary support/resistance lines. They are derived local balance points in the skew / speed structure.

### Practical meaning

She uses the pivots as the levels where the market must prove whether the expected dynamics are actually materializing.

If the realized volatility and skew behavior at the pivot do not match the structural expectation, she treats that as repricing.

## 5. Vanna And Vanna Flips

### Explicit

She describes vanna as:

- the change in vega with respect to spot
- tightly connected to skew
- highly conditional across maturities

She is very explicit that:

- vanna cannot be simply aggregated the way "furu services" do
- dealers measure vega in maturity buckets
- the hedging impact differs by tenor and covariance structure

For the base case, she says:

- the zero-vanna point sits around the ATM strike where the vega function peaks
- when volatility changes, the zero-vanna point moves

### How she uses flips

Her `vanna flip` is a zero-vanna point where the hedging flow reverses beyond the line.

She explains the mechanism explicitly:

- as spot approaches a zero-vanna line, IV changes can shift the line itself
- if IV rises, the zero-vanna line can slide upward toward OTM calls
- in live use, she notes that local zero-vanna lines can oscillate around a small range

This is why some of her notes say things like:

- low vol needed to break a given vanna flip
- high vol can move the flip itself

So her vanna flips are not static chart lines in the naive sense. They are regime-conditional thresholds.

## 6. Speed Flips And "Zero Speed-Gamma"

### Explicit

Alma repeatedly pairs:

- `zero vanna lines`
- `zero speed lines`

and says they have similar hedging-flow reversal effects.

She also says:

- zero-speed lines always coincide with zero-vanna areas
- the converse is not always true

In later chats she refers to certain critical local pivots as:

- `zero speed-gamma`
- `the zeros of the 4th derivative of delta to spot`

### What this likely means

There are two layers here:

1. a simple published intuition:
   - speed flip as a reversal line in the speed profile
2. a deeper internal implementation:
   - some local pivots appear to be tied to a higher-order "speed-gamma" or fourth-derivative condition

### Best reading

The archive supports this conclusion:

- a naive local `speed = 0` line is probably not the whole story
- Alma seems to use an additional higher-order convexity condition for some of the most important local flip or pin levels

That is one of the clearest places where we know the object exists but do not have the full exact formula.

## 7. Hidden Spread / Hidden Pattern Inference

This is one of the most important parts of her method.

She does not merely label a chart "bullish" or "bearish." She tries to infer the hidden options structure behind the surface.

### Explicit clues she uses

She repeatedly says she compares:

- where gamma is long or short
- how speed changes above and below the center
- the relative steepness of `GEX` and `DEX`
- local charm/color behavior
- vanna and skew behavior

### Examples she gives

#### Short fly

She explicitly describes a local `MM short fly pattern` as:

- long `GEX` in the middle
- negative speed above
- positive speed below

#### Long / short iron fly mapping

She gives a direct mapping:

- long fly: long premium, long vega, short theta, positive net color
- short fly: short premium, short vega, long theta, negative net color

She then connects those profiles to whether the market expects:

- rangebound behavior
- reversion toward the center
- momentum toward the wings
- more liquidity on one side than the other

#### Ratio spreads from GEX/DEX geometry

In one detailed example, she says the relative steepness of `GEX` and `DEX` across the wings and center lets her infer:

- a long ratio put spread to the downside
- a long ratio call spread to the upside

She even gives example ratios from the geometry.

#### Risk reversal

She also uses combinations like:

- long-gamma and short-gamma separated in space
- negative speed skew

to describe `short risk reversal` style environments where one side becomes more fragile than the other beyond the centroid.

### Best summary

Her hidden-structure inference appears to work like this:

1. read the sign and steepness of net gamma, speed, color, vanna, and delta exposure
2. identify the center and wings
3. infer whether the geometry matches a fly, risk reversal, condor, or ratio spread
4. translate that structure into an expected realized-volatility and liquidity regime

## 8. Probability Lines And Scenario Bands

This is slightly adjacent to your question, but it belongs to the same stack.

She says her script input lines are derived from:

- premarket volatility positioning
- a Gaussian distribution assumption
- the day's implied volatility
- expected realized volatility
- implied and realized vol of vol

So her published level set is not only surface geometry. It is also a probability model layered on top of the structural read.

## 9. What She Has Explicitly Not Disclosed

There are several places where she stops short of giving the full recipe.

What still appears intentionally undisclosed:

- exact weighting of different expirations / tenors
- exact 3D surface construction
- exact numerical reduction from 3D speed surface to 2D published levels
- exact netting logic across the fourth and fifth derivatives
- exact algorithm for local speed-flip / zero-speed-gamma pivots
- full covariance-aware maturity aggregation for vanna
- exact hidden-pattern classifier

So we know the inputs, the mathematical objects, and the interpretation logic much better than the exact production formula.

## 10. Best Practical Reconstruction Of Her Method

Based on the archive, the closest local reconstruction recipe would be:

1. build an `EOD` directional positioning snapshot
2. compute aggregated delta / gamma / vega surfaces over spot, time, and vol grids
3. derive:
   - speed
   - color
   - vanna
   - zomma
   - vomma
4. locate:
   - centroids
   - local centroids / pivots
   - zero-vanna lines
   - zero-speed or speed-convexity lines
5. compare `GEX` and `DEX` geometry to infer likely hidden structures
6. convert the structure into scenario branches and probability bands
7. validate it intraday using realized volatility, skew, and spot/vol behavior

## Bottom Line

The archive gives a surprisingly strong answer to "how does Alma calculate this?"

What is explicit:

- the main derivative objects
- the center-of-mass logic for centroid
- the local-centroid logic for pivots
- the zero-vanna logic for flips
- the structural use of `GEX` and `DEX` geometry to infer hidden spreads

What remains hidden:

- the full weighting and numerical implementation
- the exact higher-order netting scheme
- the exact local flip / pivot algorithm

So we can document and reconstruct the framework with decent fidelity, but we should not pretend we have her full proprietary formula sheet.

## Source Pointers

Primary sources used for this note:

- `artifacts/alma-research/posts/2025-11-12_a-guide-to-reading-my-daily-posts/content.txt`
- `artifacts/alma-research/posts/2026-01-08_liquidity-structure-lets-put-speed/content.txt`
- `artifacts/alma-research/posts/2025-03-24_intraday-post-24march/content.txt`
- `artifacts/alma-research/posts/2025-02-04_intraday-post-04feb/content.txt`
- `artifacts/alma-research/posts/2025-02-05_intraday-post-05feb/content.txt`
- `artifacts/alma-research/posts/2025-06-30_july-outlook-short-weekly-post-30june/content.txt`
- `artifacts/alma-research/posts/2025-10-08_fomc-minutes-gold-btceth-intraday/content.txt`
- `artifacts/alma-research/chats/2025-02-27_thursday-chat-27-feb/content.txt`
- `artifacts/alma-research/chats/2025-03-11_tuesday-chat-11-mar/content.txt`
- `artifacts/alma-research/chats/2025-12-12_friday-chat-12-dec/content.txt`
- `artifacts/alma-research/chats/2025-12-16_tuesday-chat-16-dec/content.txt`
- `artifacts/alma-research/chats/2026-02-23_monday-chat-23-feb/content.txt`
- `artifacts/alma-research/knowledge/alma-framework.md`
- `artifacts/alma-research/knowledge/thetadata-reconstruction.md`
