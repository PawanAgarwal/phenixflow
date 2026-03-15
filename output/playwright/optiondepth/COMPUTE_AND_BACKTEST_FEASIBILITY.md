# Compute And Backtest Feasibility For OptionDepth-Style Signals

Captured on 2026-03-15.

Purpose: summarize whether we can reproduce OptionDepth-style signals from the ThetaData-backed warehouse we already have, what gaps remain, and how OptionDepth can show time-varying intraday exposure before the session starts.

## Executive Summary

- We can reproduce a strong proxy version of OptionDepth for a subset of the product's claims.
- We cannot build an exact clone of their proprietary "entire MM inventory" model because we do not have true dealer inventory.
- We do have enough data to build and backtest:
  - signed customer flow by strike and expiration
  - proxy customer positioning
  - proxy dealer gamma pressure
  - projected gamma/charm/vanna surfaces over spot and time grids
  - intraday tests for support/resistance, volatility suppression/amplification, drift, and pinning
- The best initial symbols are likely `SPY` and `QQQ`, not `SPX`, because `SPX/SPXW` is not currently in the active warehouse.

## Short Answer

### Can we calculate these ourselves?

Yes, mostly.

We already have:

- option trades
- option quote minutes
- stock minute bars
- daily open interest
- raw greeks
- calculated greeks
- enriched option trade rows
- minute-derived symbol and contract summaries

That is enough to build a practical dealer-positioning proxy framework.

### Can we backtest the claims?

Yes, many of the same-session claims are testable.

The best first backtests are:

- positive gamma vs negative gamma regime and realized intraday volatility
- distance to gamma peaks/troughs vs bounce, rejection, breakout, and travel distance
- charm sign at the open vs open-to-close drift
- concentrated strike/expiration positioning vs late-day pinning behavior

### Can we exactly reproduce OptionDepth?

No, not exactly.

The main reason is that OptionDepth claims to estimate the entire market maker inventory, while we only observe public prints, daily OI, quotes, and greeks. That means we can build a strong public-data proxy, not prove we matched their proprietary inventory model.

## Data We Already Have

Relevant schema and architecture references:

- `docs/PHENIX_ARCHITECTURE.md`
- `scripts/clickhouse/init-options-schema.sql`
- `docs/THETADATA_GREEKS_INPUT_SOURCES.md`

Core tables already present:

- `options.option_trades`
- `options.option_quote_minute_raw`
- `options.stock_ohlc_minute_raw`
- `options.option_open_interest_raw`
- `options.option_greeks_minute_raw`
- `options.option_calculated_greeks_minute`
- `options.option_trade_enriched`
- `options.option_symbol_minute_derived`
- `options.option_contract_minute_derived`

Observed live table sizes at the time of capture:

- `option_quote_minute_raw`: `14.33B` rows
- `option_greeks_minute_raw`: `2.01B` rows
- `option_calculated_greeks_minute`: `170.19M` rows
- `option_trades`: `993.85M` rows
- `option_trade_enriched`: `573.88M` rows
- `option_open_interest_raw`: `30.97M` rows

Observed date coverage:

- `option_trades`: `2024-11-04` to `2026-03-12`
- `option_quote_minute_raw`: `2024-11-04` to `2026-03-12`
- `stock_ohlc_minute_raw`: `2024-11-07` to `2026-03-12`
- `option_open_interest_raw`: `2025-01-02` to `2026-03-12`
- `option_greeks_minute_raw`: `2025-09-02` to `2026-03-10`
- `option_calculated_greeks_minute`: `2025-09-02` to `2026-03-12`
- `option_trade_enriched`: `2025-08-01` to `2026-03-12`
- `option_symbol_minute_derived`: `2025-08-01` to `2026-03-12`
- `option_contract_minute_derived`: `2025-08-01` to `2026-03-12`

## What We Can Compute Reasonably Well

### 1. Signed Customer Flow

We already infer aggressor side and directional sentiment heuristically from trade price vs bid/ask and option side.

Relevant logic:

- `src/historical-formulas.js`
- `computeExecutionFlags(...)`
- `computeSentiment(...)`

This lets us build:

- buy-ask / sell-bid style flow proxies
- bullish vs bearish flow imbalance
- strike-by-strike signed flow maps
- expiration-bucket flow changes over time

### 2. Positioning By Strike And Expiration

Using:

- daily OI
- current-day signed flow
- trade size and premium
- contract metadata

we can build:

- net customer flow by strike
- net customer flow by expiration
- approximate running same-day positioning change
- proxy customer `DEX`

This is not exact open-position inventory, but it is enough to test many "flow and positioning" claims.

### 3. Gamma-Based Dealer Pressure Proxy

Using:

- calculated gamma
- inferred customer direction
- the assumption that dealers are typically the opposite side of net customer positioning

we can approximate:

- proxy dealer `GEX`
- gamma peaks, troughs, and flips
- spot-grid gamma surfaces
- expected stabilizing vs destabilizing regimes

This is the strongest near-term replication target.

### 4. Charm And Vanna

We do not currently persist charm or vanna directly, but we have the required ingredients for most contracts:

- spot
- strike
- expiration / time to expiry
- implied vol
- risk-free rate assumption
- dividend assumption

Our calculated-greek pipeline already computes first-order greeks with these inputs.

Current stored outputs include:

- delta
- gamma
- theta
- vega
- rho

Charm and vanna are not stored today, but they can be derived from the same model inputs.

### 5. Surface Projection Through The Session

Even without minute-by-minute inventory updates, we can project exposures over:

- future time buckets during the same session
- hypothetical spot grids above/below current price

That is enough to build:

- gamma surfaces
- charm drift maps
- vanna sensitivity maps
- strike/expiration heatmaps

## What We Cannot Reproduce Exactly

### 1. True Market Maker Inventory

This is the biggest difference.

OptionDepth claims to use the entire market maker inventory. We do not have:

- direct dealer positions
- position transfer between customers/dealers/firms
- open vs close intent on every trade
- hidden/internalized inventory changes

So our version must be a proxy based on:

- public prints
- OI
- quote state
- greek state

### 2. Exact Intraday Position Changes

Open interest is daily, not intraday.

That means intraday changes in "positioning" must be inferred from tape and quotes rather than directly observed. This is good enough for research, but it is not the same as a privileged dealer book.

### 3. Exact Opening Inventory For Every Contract

We can estimate opening inventory from:

- prior OI
- recent history
- current-day flow

but we do not know:

- who is long vs short with certainty
- whether same-day prints are opening, closing, rolling, or spread adjustments

## Important Data Gaps

### 1. `SPX/SPXW` Gap

OptionDepth's flagship examples are mostly `SPX/SPXW`.

Observed in our live warehouse at the time of capture:

- `SPY` and `QQQ` are present
- `SPX` and `SPXW` were not present in the queried options tables

Practical implication:

- we can backtest the framework on `SPY` and `QQQ`
- we cannot directly replicate their SPX screenshots and examples until SPX options are included

### 2. Raw Greeks Coverage Breaks After 2026-03-06

This is a meaningful gap.

Observed recent raw-greek coverage:

- `2026-03-05`: `93` symbols
- `2026-03-06`: `98` symbols
- `2026-03-09`: only `1` symbol (`TGT`)
- `2026-03-10`: only `1` symbol (`AAPL`)

Meanwhile:

- quotes still had about `100` symbols
- stock bars had `100` symbols
- calculated greeks still had `100` symbols through `2026-03-12`

Practical implication:

- for current research, calculated greeks should be treated as the primary greek source
- raw greeks are currently not reliable enough for broad recent-date backtests

### 3. Calculated Greeks Are Good But Not Perfect

Recent calculated-greek quality looked usable:

- recent non-`ok` row rates were roughly `0.8%` to `1.1%` on most recent days
- somewhat worse on a couple of days, around `2%`

Practical implication:

- good enough for first-pass research
- should still be monitored or filtered in backtests

### 4. Rates And Dividends Are Still Partly Assumption-Driven

The repo explicitly documents that greek reconstruction still depends on:

- risk-free rate assumption
- dividend assumption

Practical implication:

- less of an issue for index-like proxies
- more relevant for single-name equities with meaningful dividend yields

### 5. Stock 1m Chunk Gaps Still Exist

The recent chunk-status snapshot showed:

- `stock_price_1m` had `1042` chunks in `missing` status in the last 30 days

Practical implication:

- targeted cleanup may be needed before broad multi-symbol backtests
- not necessarily fatal for focused `SPY`/`QQQ` canary work

## Best Current Research Window

The cleanest first pass appears to be:

- symbols: `SPY`, `QQQ`
- period: `2025-09-02` to `2026-03-12`
- greek source: `option_calculated_greeks_minute`

Reasons:

- continuous options, quote, stock, OI, and calculated-greek coverage
- calculated greeks remain present when raw greeks are sparse
- these are highly liquid ETF options with more stable modeling assumptions than many single names

## What Claims We Can Backtest First

### Gamma Claims

Testable:

- positive gamma suppresses volatility
- negative gamma amplifies volatility
- gamma peaks behave more like support/resistance
- gamma troughs behave more like travel zones / paths of least resistance
- gamma flips coincide with changes in price behavior

Example test ideas:

- compare realized 15m/30m/60m volatility by gamma-regime sign
- measure bounce/rejection frequency near large positive gamma peaks
- measure breakout continuation frequency after moving into lower-gamma zones

### Charm Claims

Testable:

- positive charm aligns with suppressive drift
- negative charm aligns with supportive drift
- charm sign near the open influences open-to-close drift
- charm flip or equilibrium areas coincide with late-day stabilization or pinning

Example test ideas:

- bucket days by opening charm sign and compare open-to-close return
- compare pinning frequency near large same-day charm equilibrium zones

### Positioning Claims

Testable:

- concentrated strike positioning marks important intraday levels
- expiration concentration changes the session's behavior
- 0DTE-heavy days show stronger same-session structure effects

Example test ideas:

- identify top concentration strikes and test whether price touches, rejects, or pins near them
- compare 0DTE-heavy sessions vs low-0DTE sessions

## Why OptionDepth Can Show Time-Varying Exposure Before The Open

This is the key conceptual point.

They do not need future minute-by-minute position data to show a time-varying surface.

### The Short Version

They are projecting, not observing future minute states.

If they have an estimate of the current inventory before the session starts, then they can revalue that inventory over:

- future time buckets during the day
- hypothetical spot levels

That automatically produces time-varying gamma/charm/vanna even before any new trades occur.

### Why Exposure Changes Even If Positions Do Not

Greeks change as a function of:

- spot
- time to expiry
- implied volatility
- rates/dividends

So even with frozen positions:

- gamma changes as price and time move
- charm changes because time only moves forward
- vanna changes because IV shifts change delta

In other words:

- the inventory may be static for the projection
- the sensitivity of that inventory is not static

### Why 0DTE Makes This More Noticeable

As expiration gets closer, greek convexity increases.

That means:

- small changes in time and spot create larger changes in exposure
- same-day projections become more dramatic
- 0DTE contracts can dominate the intraday surface

### How They Can Do This Premarket

A reasonable premarket workflow is:

1. Estimate the starting inventory before the open.
2. Build a spot grid around current/reference price.
3. Build a time grid for the session, for example every 5 minutes.
4. Reprice every contract on that `(spot, time)` grid.
5. Aggregate portfolio gamma/charm/vanna at each grid point.

That creates a projected surface for the whole day before the session begins.

### Why The Surface Can Still Change During The Day

Once the market opens, they can update the inventory estimate using:

- new same-day flow
- new spot
- new IV state
- possibly refreshed assumptions around customer/dealer imbalance

So the displayed surface can evolve intraday even though official OI is only daily.

### Important Clarification

The missing intraday piece is not greek math.

The missing intraday piece is exact inventory.

OptionDepth appears to solve this by:

- estimating inventory
- projecting greek sensitivities of that inventory over time and price
- updating the estimate as new information arrives

## Practical Interpretation For Us

We do not need minute-by-minute OI updates to build the same style of projection.

We only need:

- a reasonable opening position proxy
- an update rule for same-day net flow
- a pricing model to recompute exposures across time and spot grids

That is feasible with the data we already have.

## Suggested First Implementation Order

1. Build a contract-level signed-flow and position-proxy table.
2. Add charm and vanna calculations from the existing calculated-greek inputs.
3. Build a spot/time surface projector for `SPY` and `QQQ`.
4. Backtest four claims first:
   - gamma sign vs realized intraday volatility
   - distance to gamma peak vs bounce/rejection
   - opening charm sign vs open-to-close drift
   - concentration zones vs late-day pinning
5. Only after the canary works, widen to more symbols.

## Bottom Line

We can build and backtest a serious public-data proxy for OptionDepth.

What is realistic now:

- same-day dealer-pressure proxy research
- intraday support/resistance and volatility-regime tests
- charm/drift and pinning tests
- strike/expiration concentration studies

What is not realistic now:

- an exact replication of their proprietary dealer inventory model
- direct reproduction of their `SPX/SPXW` examples without adding SPX coverage

The biggest technical takeaway is that OptionDepth does not need minute-by-minute official position data to project intraday exposure. Once you estimate the starting inventory, the exposures can change minute by minute because the option math changes minute by minute.
