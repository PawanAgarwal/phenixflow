# Alma OptionDepth Usage Notes

Captured on 2026-03-15 from the full `artifacts/alma-research` archive, combining archive-wide term scans with detailed reads of the highest-signal posts and chats.

Purpose: document how Alma actually advises people to use OptionDepth, what was meaningfully new relative to the earlier product-focused notes, and what we should incorporate into our own OptionDepth understanding.

## Executive Summary

Yes, there were meaningful new learnings.

The biggest additions are:

- Alma uses the `EOD summary` OptionDepth heatmap as the main map for the `next session`.
- She treats most intraday OptionDepth refreshes as `lagging sentiment`, not as the core planning tool.
- She prioritizes `speed`, `color`, `centroid`, and `vanna/speed flips` above ordinary gamma-wall interpretation.
- She uses OptionDepth to decode `hidden spread structure`, not just to find large exposure bars.
- She treats the intraday job as `validate or reject the premarket map` using realized volatility, skew, and spot/vol behavior.
- She explicitly splits horizons:
  - `speed` for short-term trading
  - `vanna` for midterm structure

These points materially refine how we should think about OptionDepth and how we should reproduce it locally.

## What Alma's Daily Workflow Looks Like

From `A Guide to Reading My Daily Posts`, her standard package is:

1. `Premarket OptionsDepth heatmap`
2. `Script input lines` with probabilities
3. `Speed profile` description
4. `Vanna flip` and `speed flip` levels

The workflow behind that package is:

1. Before the open, infer the structural map the market priced for the coming session.
2. Reduce that structure into a handful of operational levels and scenarios.
3. During the day, watch whether realized behavior confirms that structure or invalidates it.

This is much more structured than "watch the live gamma map."

## How She Uses OptionDepth

### 1. Premarket EOD map first

- She explicitly says the heatmap she shares is the one created from `EOD summary` data.
- Her reason is that the EOD positioning reveals the structure traders priced for the next session.
- She says intraday updates are mostly lagging noise for planning and are useful mainly for sentiment.

What this means:

- The main planning artifact is yesterday's completed structural map.
- Same-day heatmap refreshes are secondary unless they show sentiment or a material repricing.

### 2. Speed is the main short-term signal

- She calls `speed` the most important part of the daily letters.
- She defines it as the slope of the gamma curve with respect to spot.
- She interprets it as a proxy for expected liquidity and realized-volatility behavior.

What this means:

- Ordinary gamma is not enough.
- The slope and shape of the gamma surface matter more than isolated gamma peaks.

### 3. Color matters more than charm

- She says she usually does not attach a charm chart because it is unnecessary most of the time.
- Her priority is `color`, which she describes as gamma decay or the time-slope of gamma.
- She says charm matters mainly later in the session, especially near the power hour.

What this means:

- A serious OptionDepth-like model should not stop at gamma and charm.
- `Color` is a first-class feature in the actual operator workflow.

### 4. Centroid and pivots are the key levels

- She calls the `centroid` the most important daily pivot.
- She describes upside and downside pivots as local centroids or checkpoints where the market tests whether the priced-in structure is holding.
- She repeatedly uses centroid, pivots, local pins, and targets to convert the structural map into an actual trade plan.

What this means:

- The heatmap is an input.
- The decision-ready output is a small set of derived levels and scenario branches.

### 5. Vanna and speed flips are exceptional levels

- She treats zero-vanna lines and zero-speed lines as very strong support and resistance.
- She suggests vanna flips may be the strongest of all.
- She also ties the strength of flip rejection to local `zomma` and the spot/IV relationship.

What this means:

- Flip detection belongs in our local replication and backtest plan.
- Simple gamma-sign regime tests are only part of the story.

### 6. Hidden spread structure is the real "reading"

- She uses the heatmap and greek geometry to infer patterns such as flies, risk reversals, condors, and ratio spreads.
- She then interprets those structures in terms of:
  - rangebound versus momentum
  - where liquidity should be
  - where realized volatility should expand
  - where the market should pin, reject, or accelerate

What this means:

- In her workflow, OptionDepth is not just a charting product.
- It is a way to reverse-engineer the aggregate structure embedded in the chain.

## How She Uses The Data Intraday

She does not appear to use intraday OptionDepth updates as a new forecasting engine.

Instead, she uses intraday behavior to check whether the premarket structure is being confirmed.

The repeated validation signals are:

- realized volatility near centroid and pivot zones
- whether `VIX` or IV "catches up"
- fixed-strike skew changes
- spot / realized-volatility beta
- whether volatility is being sold or bought at key levels
- whether PM-session pinning develops near centroid or local magnets

Her repeated idea is:

- if the market behaves as the structure implies, stay with the regime
- if realized behavior diverges, liquidity migrated and the structure is being repriced

## Horizon Split

One of the clearest operator rules in the chats is:

- `speed profile` for short-term trades
- `vanna profile` for midterm trades
- beyond roughly the quarterly horizon, options are much less useful for this style of analysis

This is a meaningful addition to the earlier notes because it makes the horizon model much more concrete.

## What She Explicitly Downplays

- raw put/call ratios
- raw volume ratios
- intraday flow changes as a primary planning tool
- naive `GEX` interpretation
- `OI`-only directional inference

She is especially clear that `OI` by itself does not tell you whether the trade was customer-to-dealer, dealer-to-customer, or dealer-to-dealer.

So her framework is much less "watch the flow dashboard" than many options tools imply.

## What We Should Incorporate Into Our OptionDepth Knowledge

These items should now be treated as part of our core understanding:

- OptionDepth is most useful as a `next-session structural map` built from the prior close.
- `Speed` is a first-class feature, not a side calculation.
- `Color` should be modeled before we spend too much time on charm-heavy interpretation.
- `Centroid`, `pivots`, `pins`, and `flip` lines are the decision-ready levels.
- Predictive value is `conditional`: the map is only as good as the intraday validation.
- The real edge is decoding `hidden structure` from the chain, not just reading a gamma wall.
- `Vanna` belongs more in the midterm structure layer than in pure 0DTE navigation.
- `Naive GEX` and `OI * gamma` style shortcuts are not enough for a serious reproduction.

## Implications For Local Replication

If we want a faithful PhenixFlow version of this methodology, the minimum useful build is:

1. an `EOD` structural surface for next-session planning
2. higher-order derivatives including `speed`, `color`, `vanna`, `zomma`, and `vomma`
3. derived `centroid`, `pivot`, `target`, and `flip` levels
4. an intraday validator for realized-volatility, skew, and spot/vol behavior
5. backtests that judge not only direction, but also pinning, rejection, false breaks, and liquidity migration

## Source Pointers

Primary sources used for this note:

- `artifacts/alma-research/posts/2025-11-12_a-guide-to-reading-my-daily-posts/content.txt`
- `artifacts/alma-research/posts/2026-01-08_liquidity-structure-lets-put-speed/content.txt`
- `artifacts/alma-research/posts/2026-01-11_january-opex-week-quant-breakdown/content.txt`
- `artifacts/alma-research/posts/2026-02-17_opex-vixpery-and-pce-week-weekly/content.txt`
- `artifacts/alma-research/posts/2025-09-22_intraday-post-22sept/content.txt`
- `artifacts/alma-research/posts/2025-08-06_intraday-prognostications-and-tidings/content.txt`
- `artifacts/alma-research/chats/2025-05-29_thursday-chat-29-may/content.txt`
- `artifacts/alma-research/chats/2025-11-01_weekly-chat-01-09-nov/content.txt`
- `artifacts/alma-research/chats/2025-12-16_tuesday-chat-16-dec/content.txt`
- `artifacts/alma-research/knowledge/alma-framework.md`
- `artifacts/alma-research/knowledge/thetadata-reconstruction.md`
