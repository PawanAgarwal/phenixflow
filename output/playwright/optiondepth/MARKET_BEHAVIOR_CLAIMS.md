# OptionDepth Market Behavior Claims

Captured on 2026-03-15 from the logged-in OptionDepth app, its help modals, and the Knowledge Base PDFs saved under `references/`.

Purpose: summarize what OptionDepth says its core features are showing, how it claims those features can be used to anticipate market behavior, and what time horizon those claims appear to target.

## Executive Summary

- OptionDepth's core thesis is that customer options positioning forces market makers to hedge, and those hedging flows can shape intraday price behavior.
- The platform most clearly claims it can help anticipate same-day market structure: support, resistance, price rejection, path of least resistance, volatility compression/expansion, drift, pinning, and trend acceleration/deceleration.
- The strongest explicit horizon is the current trading session, not a clean next-day or multi-week forecast.
- The platform does expose multiple expirations, including weekly and farther-dated contracts, but that appears to be presented more as structural context than as an explicit "predict next week/month direction" promise.

## Time Horizon

- `Gamma` and `Charm` are explicitly described as projections "throughout the trading session."
- The API and plan copy explicitly expose both `daily` and `intraday` models for the heatmaps.
- `Positional Insights` is explicitly described as updating every 10 minutes for Pro Max users.
- `Breakdown by Expiration` and `Depth View` expose cross-expiration structure, so a trader can inspect weekly/monthly positioning, but the strongest marketing language still points to intraday and same-session use.
- I did not find strong explicit wording that the product is designed as a direct next-day, next-week, or next-month directional forecaster.

Primary evidence:

- `notes/09-data-shop-history-snapshot.md` lines 165-215
- `notes/08-api-docs-snapshot.md` lines 126-161
- `notes/api-units.md` lines 122-128
- `notes/15b-positional-help-open.md` line 229
- `references/interpreting_gamma_heatmaps.txt` lines 25-40, 87-137
- `references/charm_projection.txt` lines 32-48

## What The Product Is Trying To Show

### 1. Market Makers' Gamma Exposure

What they say it shows:

- the entire market maker gamma exposure surface across strikes, expirations, price levels, and time
- how market maker hedging pressure should evolve as spot moves during the session
- gamma peaks, troughs, and zero/flip areas

How they claim traders can use it:

- identify support and resistance zones
- anticipate price rejection or slowdown in high positive-gamma areas
- identify "paths of least resistance" in lower-gamma areas
- anticipate whether volatility should be dampened or amplified
- anticipate changes in behavior when price moves from negative gamma to positive gamma, or the reverse

What market behavior they claim follows:

- positive gamma: market makers hedge against the move, which stabilizes the market and dampens volatility
- negative gamma: market makers hedge with the move, which extracts liquidity and increases volatility
- high gamma peaks: stronger resistance/support and slower price movement
- low gamma troughs: easier travel zones and, in negative gamma, potentially higher volatility
- gamma regime flips: can create support/resistance or accelerate an existing trend

Claimed time horizon:

- explicitly same-day and intraday
- daily model and intraday model are both supported
- projected throughout the trading session

Evidence:

- `notes/09-data-shop-history-snapshot.md` lines 165-188
- `references/interpreting_gamma_heatmaps.txt` lines 25-40
- `references/interpreting_gamma_heatmaps.txt` lines 47-82
- `references/interpreting_gamma_heatmaps.txt` lines 87-137

Related screenshots:

- `screenshots/04-market-makers.png`
- `screenshots/04-market-makers-full.png`
- `screenshots/04-3d.png`
- `screenshots/04i-market-makers-how-it-works.png`

### 2. Market Makers' Charm Exposure

What they say it shows:

- the full market maker charm exposure surface across strikes, expirations, price levels, and time
- how dealer delta changes passively as time passes
- a time-driven force that builds through the session, especially as 0DTE approaches expiration

How they claim traders can use it:

- anticipate whether time decay should create supportive or suppressive dealer flows
- estimate whether passive dealer buying/selling may reinforce or lean against the tape
- identify charm flip or equilibrium areas where the directional force becomes neutral
- think about pinning behavior near expiration

What market behavior they claim follows:

- positive charm: suppressive, with market makers gradually selling the underlying over time
- negative charm: supportive, with market makers gradually buying the underlying over time
- elevated charm can contribute to drift and can reinforce broader trends
- charm can contribute to pinning as price converges toward a charm equilibrium point

Claimed time horizon:

- explicitly same-session and intraday
- projected throughout the trading session
- described as a persistent force that accumulates over time during the day

Evidence:

- `references/charm_projection.txt` lines 5-13
- `references/charm_projection.txt` lines 32-48
- `references/charm_projection.txt` lines 66-108
- `references/delta_hedging_fundamentals.txt` lines 175-188

Related screenshots:

- `screenshots/04-charm.png`
- `screenshots/04-market-makers-full.png`

### 3. Vanna Exposure

What they say it shows:

- implied-volatility-driven changes in dealer delta exposure
- another second-order Greek that matters for delta hedging dynamics

How they appear to position it:

- as a meaningful part of hedging analysis, especially for understanding how IV changes affect dealer delta
- as a more advanced and less visually intuitive signal than gamma or charm

What market behavior they suggest:

- IV changes can materially affect dealer hedging needs
- farther-dated contracts can still matter here

Important limitation:

- their explicit predictive marketing is much lighter here than for gamma and charm
- I did not find equally strong feature copy claiming specific support/resistance or volatility behaviors for Vanna inside the captured app views

Claimed time horizon:

- available in the same market-maker exposure surface and API family as the other metrics
- likely intended as both daily and intraday context, but the strongest direct wording is in the hedging fundamentals guide rather than in the product UI

Evidence:

- `references/delta_hedging_fundamentals.txt` lines 199-213
- `notes/04-vanna-snapshot.md` lines 113-138

Related screenshots:

- `screenshots/04-vanna.png`

### 4. Breakdown by Strike

What they say it shows:

- net customer positions by strike
- customer delta exposure (`DEX`)
- market maker gamma exposure (`MM GEX`)
- how positions are distributed across strikes and what that implies for directional bias and hedging pressure

How they claim traders can use it:

- infer directional bias from customer positioning
- identify strike levels that matter most for the trading day
- understand where customer supply/demand imbalance is concentrated
- infer likely market maker buy/sell behavior as price approaches specific levels
- anticipate support/resistance and volatility from MM GEX at those levels

What market behavior they claim follows:

- customer positioning gives a read on sentiment and directional bias
- the overall imbalance can act as a "blueprint" for session price action
- positive MM gamma acts as support/resistance and dampens volatility
- negative MM gamma can act like a magnet and increase volatility
- 0DTE positions held through the day can give useful insight into sentiment and possible price movement

Claimed time horizon:

- strongest framing is for the trading day and current session
- they also say intraday data adds precision

Evidence:

- `references/analyzing_breakdown_by_strike.txt` lines 5-10
- `references/analyzing_breakdown_by_strike.txt` lines 16-31
- `references/analyzing_breakdown_by_strike.txt` lines 100-110
- `references/analyzing_breakdown_by_strike.txt` lines 186-190
- `references/analyzing_breakdown_by_strike.txt` lines 222-289

Related screenshots:

- `screenshots/05-positional-insight.png`
- `screenshots/05-positional-insight-full.png`

### 5. Breakdown by Expiration

What they say it shows:

- market insights and outlooks aggregated by expiration date
- two modes:
- `Net` as a snapshot of current positioning
- `Flow` as the change in positioning between two selected moments

How they claim traders can use it:

- compare how positioning differs across expiration buckets
- track how positioning shifts over time
- stay up to date with market behavior as flows evolve
- filter the view to specific expirations and strikes

What market behavior they imply:

- short-dated and 0DTE structure can matter a lot intraday
- changes in expiration-bucket positioning can reveal shifts in market stance and dynamics

Claimed time horizon:

- explicit intraday utility via updates every 10 minutes for Pro Max
- also useful for examining weekly/monthly structure across expirations
- still framed more as a positioning monitor than as a standalone multi-week directional forecaster

Evidence:

- `notes/15b-positional-help-open.md` line 229

Related screenshots:

- `screenshots/05-positional-insight.png`
- `screenshots/15b-positional-help-open.png`

### 6. Depth View

What it appears to show:

- a strike-by-expiration matrix for exposure and positioning
- exact values in table mode and concentration zones in heatmap mode
- metrics including `Position`, `DEX`, `GEX`, `CEX`, and `VEX`

How it appears intended to be used:

- inspect where exposure is concentrated across the expiration surface
- locate clusters by strike and expiry
- compare net versus flow style views in a matrix format

Interpretation caution:

- I did not find strong explicit wording in the captured help text that Depth View itself predicts price in the same direct way the gamma/charm guides do
- it reads more like a structural inspection tool that supports the larger dealer-positioning thesis
- in practice, it likely helps traders find the strike/expiry zones that matter most, but that is partly inference from the UI rather than a direct sales claim

Claimed time horizon:

- consistent with the rest of the platform's daily and intraday data model
- also useful for seeing farther-dated structure across expirations

Evidence:

- `FEATURE_CATALOG.md` lines 307-385
- `notes/16b-depth-help-open.md` lines 116-140

Related screenshots:

- `screenshots/06-depth-view.png`
- `screenshots/06-depth-view-full.png`
- `screenshots/06b-depth-view-heatmap.png`

## How OptionDepth Says The Signals Predict Market Movement

Across the captured materials, the platform repeatedly claims these are the main ways the data can be used:

- find likely support and resistance levels
- anticipate where price may reject or slow down
- identify lower-resistance travel zones
- anticipate whether volatility should be suppressed or amplified
- anticipate whether trend continuation is likely to accelerate or decelerate
- infer whether passive time-decay flows may support or suppress price
- identify conditions that may contribute to pinning
- read customer positioning as directional sentiment
- monitor whether intraday flow changes are strengthening or weakening the current market stance
- stay aware of how 0DTE positioning can reshape the session as expiration approaches

## Important Caveats From Their Own Material

- Their own delta-hedging guide says observed options flow can lag the hedge response, so flow often reflects sentiment more than it predicts fresh dealer hedging.
- Their guides also note that a large customer order can quickly change the positioning landscape.
- Their charm guide says charm can contribute to pinning, but is not the sole determinant.
- Overall, the strongest claims are about market structure and probable hedging pressure, not deterministic point forecasts.

Evidence:

- `references/delta_hedging_fundamentals.txt` lines 86-107
- `references/interpreting_gamma_heatmaps.txt` lines 69-82
- `references/charm_projection.txt` lines 95-108

## Bottom Line

OptionDepth is primarily selling a dealer-positioning framework for predicting how the market may behave during the current session. Its clearest claims are:

- intraday positioning matters
- same-day dealer hedging flows can influence price action
- gamma helps frame support/resistance and volatility regime
- charm helps frame intraday drift, suppression/support, and pinning
- strike and expiration breakdowns help identify sentiment, key levels, and where those forces are concentrated

The weakest explicit claim is that it predicts the next day, next week, or next month in a direct directional sense. The platform shows longer-dated positioning, but the strongest language and workflows are built around same-day and intraday market behavior.
