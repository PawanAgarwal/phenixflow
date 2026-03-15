# QQQ Regime Model Research

Date: 2026-03-14

## Objective

Build a model that detects regime changes early enough to drive instrument selection across:

- `QQQ` for clean bullish regimes
- `TQQQ` for strong bullish regimes with low-to-moderate realized volatility and high trend persistence
- `SQQQ` for bearish regimes
- `JEPQ` for sideways or mildly bullish, volatility-rich, income-friendly regimes

The research conclusion is that this should not start as a single-label "which ETF do I buy?" model. It should start as:

1. a regime-state model
2. a transition-probability model
3. an action layer that maps regime probabilities into the best instrument after costs and path-dependency penalties

## Short Answer

To build this well, we need data from 10 families:

1. `QQQ` and Nasdaq price state
2. Nasdaq options surface state
3. options flow and positioning
4. breadth and constituent leadership
5. cross-asset macro and stress variables
6. ETF-wrapper-specific data for `TQQQ`, `SQQQ`, and `JEPQ`
7. calendar and event risk
8. sentiment and narrative data, including X
9. execution and financing frictions
10. forward outcome labels for regime and instrument utility

If we skip any of the first 6, the model will be materially weaker.

## Why This Is Hard

The regime we care about is not just "up or down." The action set mixes:

- an unlevered beta ETF (`QQQ`)
- daily reset leveraged/inverse ETFs (`TQQQ`, `SQQQ`)
- a covered-call income ETF (`JEPQ`)

Those wrappers behave differently even under the same market direction. A regime model therefore needs to understand:

- direction
- volatility
- trend persistence
- skew and crash demand
- breadth quality
- path dependency
- option-premium richness
- financing and implementation cost

## Recommended Regime Taxonomy

Use 5 states instead of 3:

1. `bull_trend_low_vol`
2. `bull_trend_high_vol`
3. `bear_trend_persistent`
4. `choppy_rangebound`
5. `transition_or_breakout_risk`

Why 5 states:

- `TQQQ` wants a different environment than `QQQ`
- `JEPQ` wants a different environment than either bullish or bearish beta
- transition periods are where the biggest mistakes happen

## Data Inventory

### 1. Underlying Price State

This is the non-negotiable base layer.

Need:

- `QQQ` 1-minute OHLCV
- `QQQ` daily OHLCV
- Nasdaq-100 proxy data: `NDX` cash, `NQ` futures if available
- overnight gap, opening drive, intraday trend slope, VWAP distance
- rolling returns over `5m`, `15m`, `30m`, `1d`, `3d`, `5d`, `10d`, `20d`
- realized volatility over the same horizons
- drawdown from rolling highs
- realized skewness and kurtosis
- jump proxies and gap frequency

Why:

- regime shifts usually appear first through changes in trend persistence, gap behavior, and realized volatility
- `TQQQ` and `SQQQ` are very sensitive to realized-volatility path, not just endpoint return

Priority: `P0`

Possible sources:

- existing `options.stock_ohlc_minute_raw`
- daily bars from existing market data vendors
- futures or index reference feeds if we add them

### 2. Nasdaq Options Surface State

The most important missing layer for early regime detection is the state of the `QQQ` or `NDX` volatility surface.

Need:

- ATM implied volatility by tenor
- term structure slopes: `0DTE/1D`, `1W/1M`, `1M/3M`
- put-call skew by delta bucket
- 25-delta risk reversal
- butterfly or convexity measures
- implied-volatility percentile and z-score
- implied vs realized volatility spread
- vol-of-vol proxies using `VXN`, `VIX`, `VVIX` if available
- expected move into next day, week, and event

Why:

- bullish-to-bearish transitions often appear as skew steepening before price fully breaks
- choppy or covered-call-friendly regimes often show rich implied volatility without clean directional follow-through
- `JEPQ` is more attractive when call-selling income is rich and upside is less clean

Priority: `P0`

Possible sources:

- existing `options.option_quote_minute_raw`
- existing Greek and IV infrastructure
- Cboe indices such as `VXN`, `VIX1D`, `VIX3M`, and `SKEW`

### 3. Options Flow And Positioning

Need:

- call vs put premium imbalance
- call vs put volume imbalance
- opening vs closing flow if inferable
- sweep, block, and large-lot activity
- `0DTE` share of total volume
- net delta, gamma, vega, and theta proxies
- strike clustering around spot
- OTM put demand and downside hedge intensity
- open-interest changes by strike and expiry
- dealer gamma proxy and gamma-flip zones

Why:

- panic hedging shows up in puts and skew before broad price confirmation
- bullish breakouts with strong upside call participation behave differently from low-quality melt-ups
- dealer positioning helps distinguish trend acceleration from pin-and-chop

Priority: `P0`

Possible sources:

- existing `options.option_trades`
- existing `options.option_trade_enriched`
- existing `options.option_quote_minute_raw`
- open-interest history if already available or added
- derived surface and positioning tables we can materialize in ClickHouse

Important note on OI:

- official listed-options OI is a daily field, not a true intraday field
- there is no official `1m` OPRA OI stream to collect
- for dealer-gamma and wall-style features, the standard approach is prior-day OI plus live spot/IV and intraday trade-flow proxies

### 4. Breadth And Leadership

This is the most important confirmation layer after price and options.

Need:

- Nasdaq advance/decline counts
- `% above 20DMA`, `% above 50DMA`, `% above 200DMA`
- new highs / new lows
- equal-weight vs cap-weight Nasdaq performance
- `SOXX` or semiconductor leadership vs `QQQ`
- megacap contribution concentration
- constituent dispersion
- rolling correlation across Nasdaq leaders
- sector-relative strength inside the Nasdaq complex

Why:

- many failed bull regimes are actually narrow leadership episodes
- true bearish transitions often begin with breadth deterioration before index breakdown
- choppy regimes often show mixed breadth with poor trend follow-through

Priority: `P0`

Possible sources:

- constituent-level price histories
- Nasdaq market activity and breadth data
- internally derived breadth tables once constituent coverage exists

### 5. Cross-Asset Macro And Stress State

Need:

- `2Y`, `10Y`, and curve changes
- SOFR / policy-rate path
- high-yield OAS / credit spreads
- financial conditions indices
- `DXY`
- `TLT` or Treasury ETF returns as risk-off confirmation
- crude, gold, and optionally BTC as cross-risk gauges
- `VIX`, `VXN`, `VIX1D`, `VIX3M`, `SKEW`
- put/call ratios

Why:

- Nasdaq regime shifts are very rate-sensitive
- bearish tech transitions often coincide with rising real-rate pressure, tighter financial conditions, or widening credit spreads
- transition regimes often show divergence between equity price and macro-stress indicators

Priority: `P0`

Possible sources:

- `options.reference_sofr_daily` already exists
- FRED series for yields, spreads, and financial conditions
- Cboe volatility indices and sentiment series

### 6. ETF Wrapper And Instrument-Specific Data

This is required because the action set includes wrappers with non-linear behavior.

Need for `TQQQ` and `SQQQ`:

- their own minute and daily bars
- realized decay and path-dependency diagnostics
- rolling tracking difference vs `3x` target
- volatility-drag estimate
- liquidity, spread, and volume
- share-split history if relevant

Need for `JEPQ`:

- fund flow data
- distribution yield history
- premium/discount if relevant
- option-income sensitivity proxies
- call-overwrite or ELN-related income context
- relative upside capture vs downside capture

Why:

- a correct bear call with the wrong wrapper can still underperform
- `TQQQ` and `SQQQ` lose information if modeled as "just 3x QQQ"
- `JEPQ` is not a pure choppy-market bet; it is a covered-call-like income wrapper whose attractiveness depends on IV richness and capped-upside tradeoff

Priority: `P0`

Possible sources:

- official fund pages and fact sheets
- ETF OHLCV feeds
- ETF flow datasets

### 7. Calendar And Event Data

Need:

- FOMC, CPI, NFP, PPI, GDP, ISM, retail sales
- large-cap tech earnings calendar
- OpEx, monthly OpEx, quarterly OpEx
- index rebalance dates
- month-end, quarter-end, and holiday effects

Why:

- many regime transitions are event-driven
- options surface and gamma behavior are event-sensitive
- choppy regimes often cluster around high-event-density periods

Priority: `P1`

Possible sources:

- macro calendar providers
- earnings calendar providers
- internally maintained event tables

### 8. Sentiment And Narrative Data

Need:

- major news headline sentiment
- earnings transcript sentiment and topic shifts
- X sentiment for tech, rates, volatility, AI narrative, and risk appetite
- crowd attention metrics by ticker and theme
- disagreement or dispersion in sentiment, not just average tone

Why:

- social and narrative data are usually weak alone, but useful around transition states
- sentiment spikes can help separate trend continuation from late crowded extremes

Priority: `P1`

Possible sources:

- X
- news APIs
- transcripts and earnings-call text
- theme/topic embeddings built locally

High-value X-native features to persist:

- daily counts of posts mentioning `QQQ`, `TQQQ`, `SQQQ`, `JEPQ`
- daily counts of key regime phrases: `breadth`, `advance decline`, `gamma`, `gamma flip`, `call wall`, `put wall`, `skew`, `VXN`, `volatility decay`, `covered call`
- curated-account panel features for a fixed list of regime-relevant accounts
- narrative disagreement index: bullish vs bearish post share on the same day
- post velocity around macro events, CPI, FOMC, and large-cap tech earnings
- topic clusters for `rates`, `AI`, `semis`, `hedging`, `dealer gamma`, and `income ETFs`
- cross-feature joins between X narrative shifts and same-day moves in `VXN`, breadth, and options-flow imbalance

### 9. Execution And Frictions

Need:

- bid-ask spread
- depth and slippage proxies
- overnight gap risk estimates

Why:

- action selection should maximize net utility, not gross return
- some regimes favor lower-turnover or lower-drag exposure choices even when the directional view is similar

Priority: `P1`

### 10. Label And Outcome Data

This is the most overlooked requirement.

Need:

- forward returns for `QQQ`, `TQQQ`, `SQQQ`, and `JEPQ`
- forward realized volatility
- forward drawdown and max adverse excursion
- forward Sharpe / Sortino / Calmar-like utility
- turnover and transaction-cost-adjusted return
- explicit transition labels: stable state vs entering state vs exiting state

Why:

- the model should learn both state and tradeability
- the same `bullish` label can map to `QQQ` or `TQQQ` depending on volatility and expected persistence

Priority: `P0`

## Data We Already Have In PhenixFlow

Confirmed from repo docs:

- `options.option_trades`
- `options.option_quote_minute_raw`
- `options.stock_ohlc_minute_raw`
- `options.option_trade_enriched`
- `options.reference_sofr_daily`

This means we already have a strong base for:

- `QQQ` intraday price state
- `QQQ` or other ETF options flow
- intraday implied-volatility and skew summaries
- realized-vol and flow-derived feature engineering

The main missing datasets appear to be:

- breadth and constituent leadership
- cross-asset reference factors beyond SOFR
- ETF-wrapper-specific data for `TQQQ`, `SQQQ`, `JEPQ`
- event calendars
- sentiment and X-derived features
- borrow and financing data

## Access Status

This section reflects both repo-confirmed data and user-confirmed availability from the current discussion.

### What We Have Now (Good Enough)

Accessible now:

- `QQQ` 1-minute underlying bars in `options.stock_ohlc_minute_raw`
- option trades / flow in `options.option_trades`
- option-enriched rows in `options.option_trade_enriched`
- daily contract OI in `options.option_open_interest_raw`
- `QQQ` and `SPY` option surface at `1m`
- top-100 stock option surface at `1m`
- Greeks / IV support from raw or calculated Greeks tables
- SOFR daily rates in `options.reference_sofr_daily`

This is already good enough for:

- `QQQ` trend / realized-volatility features
- `QQQ` / `SPY` vol-surface regime features
- single-name cross-sectional option dispersion features
- flow imbalance, vol/OI, and skew-style features
- daily and `1m` repriced dealer-gamma proxy
- a serious `v1` regime model for `QQQ`, `TQQQ`, `SQQQ`, and `JEPQ`

Decision:

- no additional paid vendor is required to start `v1`
- ThetaData plus our own feature engineering is sufficient for the first build

### What I Can Get Easily (Good Enough)

Public or already-researchable data I can source directly:

- Treasury yields, credit spreads, and financial conditions from FRED
- public rate data such as SOFR from New York Fed
- daily `VIX`, `VXN`, `VIX1D`, `VIX3M`, and `SKEW` reference data from public Cboe sources
- ETF wrapper metadata, distributions, and fact sheets from issuer sites for `QQQ`, `TQQQ`, `SQQQ`, and `JEPQ`
- macro event calendars: FOMC, CPI, NFP, PPI, GDP, ISM
- public earnings calendars and company IR event dates
- ongoing qualitative X research through Playwright when needed
- OCC daily OI files as a public cross-check if we ever want them

Good candidate tables I can help build next from public data:

- `reference_macro_daily`
- `reference_vol_indices_daily`
- `reference_event_calendar`
- `etf_wrapper_state_daily`

### What I Can Probably Get (Still Good Enough For `v1`)

These are feasible on the free/public path and should be good enough for the first model, even if they are not perfect:

- Nasdaq breadth built from constituent price histories
- equal-weight vs cap-weight Nasdaq proxies
- historical constituent membership for Nasdaq-100 using public sources
- daily X phrase counts and narrative clustering

Caveats:

- breadth is straightforward if we accept a practical research-grade implementation rather than a perfect institutional back-history
- X is good enough for research features, but not production-grade ingestion

### What I Truly Need Your Help With

Only a few things are actually blocking or require your decision:

1. If you want automation instead of research-only X usage
   - manual Playwright-driven X research is fine now
   - only if you want automated daily ingestion do we need API or scraping decisions

### What I Do Not Need From You Yet

Not required for `v1`:

- another paid dealer-gamma vendor
- `NDX` options
- `NQ` futures or futures options
- institutional-grade breadth vendor data
- paid X data

### Recommendation By Phase

Start immediately with:

1. `QQQ` / `SPY` / top-100 surface
2. option trades and daily OI
3. `QQQ` 1m stock bars
4. FRED macro data
5. public Cboe vol-index data
6. event calendars
7. ETF wrapper metadata
8. our own daily and `1m` dealer-gamma proxy

Ask for your help only when we reach:

1. automated X ingestion
2. any later decision to expand beyond the current `v1` scope

## Dealer Gamma Methodology

Yes, we can build our own dealer-gamma proxy from the data we already have.

The key idea from both practitioner GEX frameworks and academic papers is the same:

- compute option gamma at the contract level
- weight it by contract open interest
- aggregate across strikes and expirations
- use the sign and concentration of that exposure to infer whether hedging should dampen or amplify moves

Important caveat:

- this is a proxy for dealer positioning, not a direct observation of dealer books
- official OI is daily, so any intraday update is model-driven, not exchange-published truth

### Research Basis

Academic support:

- papers such as `Option gamma and stock returns` proxy net gamma exposure as the gamma-weighted sum of open interest across listed options
- the recent `0DTEs: Trading, Gamma Risk and Volatility Propagation` literature also studies market-maker inventory gamma and its relation to intraday volatility

Industry support:

- modern GEX frameworks use chain gamma, open interest, and spot to estimate strike-level and aggregate hedging pressure
- more advanced vendor models then add same-day flow adjustments because official OI does not update intraday

### What We Can Compute Now

With current PhenixFlow data we can compute:

1. static daily GEX
2. intraday repriced GEX using fixed daily OI
3. flow-adjusted intraday GEX as an experimental layer
4. strike-level wall metrics such as `call_wall`, `put_wall`, and `gamma_flip`

### Inputs

From current tables:

- underlying spot from `options.stock_ohlc_minute_raw`
- option quotes / IV surface from `options.option_quote_minute_raw`
- option trades from `options.option_trades`
- daily OI from `options.option_open_interest_raw`
- Greeks / IV from raw or calculated Greeks tables

### Recommended Calculation Levels

#### 1. Daily Static GEX

This should be the first implementation.

Method:

- take previous official OI for every listed contract
- compute contract gamma using current or opening IV and spot
- aggregate by strike and expiration
- derive:
  - `net_gex_daily`
  - `gex_by_strike`
  - `gex_by_expiry`
  - `call_wall`
  - `put_wall`
  - `gamma_flip_level`

Use:

- overnight map of structural support / resistance
- regime feature for next-day classification

#### 2. 1-Minute Repriced GEX

This is the most useful intraday version for `v1`.

Method:

- hold OI fixed at prior-day official values
- every minute, reprice contract gamma using current spot and current minute IV inputs
- recompute strike and aggregate GEX

Why this works:

- gamma itself changes strongly with spot, moneyness, and time-to-expiry
- even without intraday OI updates, the hedging pressure map changes during the session as spot moves

This gives us:

- `net_gex_1m`
- `gamma_flip_distance_to_spot_1m`
- `call_wall_distance_to_spot_1m`
- `put_wall_distance_to_spot_1m`
- `front_expiry_gex_share_1m`
- `zero_dte_gex_proxy_1m`

#### 3. Flow-Adjusted Intraday GEX

This should be an optional `v2` layer.

Method:

- start with daily OI baseline
- classify intraday option trades by aggressor side and opening-pressure proxy
- maintain an `effective_oi` or `effective_gamma_weight` adjustment for front expiries
- use that adjusted weight to build a more responsive intraday GEX estimate

This is where our trade+quote data adds real value beyond static OI.

### Practical Formula Conventions

Use two parallel conventions:

1. share-based gamma pressure per `$1` move
2. dollar gamma pressure per `1%` move

Implementation idea:

- contract share gamma per `$1` move:
  - `contract_gamma_shares = gamma * oi * 100`
- contract dollar gamma per `1%` move:
  - `contract_gamma_dollars_1pct = gamma * oi * 100 * spot * 0.01`

For strike-level concentration maps, we should also compute:

- `abs_gamma_weight = abs(gamma) * oi * 100`

Why:

- the exact sign convention is the noisiest part
- absolute concentration still helps identify likely pinning / wall levels

### Sign Convention

This is the hardest part and should be handled carefully.

For `v1`, I recommend computing three variants:

1. `unsigned_gamma_density`
   - no dealer-side assumption
   - best for walls and concentration

2. `naive_signed_gex`
   - simple convention for directional interpretation

3. `flow_adjusted_signed_gex`
   - use signed trade-flow and opening-pressure proxies to refine the sign

This gives us robustness instead of overcommitting to one fragile dealer-sign assumption.

### What Makes Sense For This Project

Recommended production order:

1. build `daily_static_gex`
2. build `1m_repriced_gex`
3. derive `call_wall`, `put_wall`, and `gamma_flip_distance_to_spot`
4. validate whether these improve regime classification
5. only then build `flow_adjusted_intraday_gex`

### What I Recommend We Store

New derived tables:

- `dealer_gamma_daily`
- `dealer_gamma_strike_daily`
- `dealer_gamma_minute`
- `dealer_gamma_strike_minute`

Core fields:

- `symbol`
- `trade_date_utc`
- `minute_bucket_utc` for intraday table
- `expiration`
- `strike`
- `option_right`
- `spot`
- `gamma`
- `oi`
- `abs_gamma_weight`
- `signed_gamma_weight`
- `gex_regime`
- `call_wall_flag`
- `put_wall_flag`
- `gamma_flip_distance_to_spot`

### Bottom Line

We do not need to buy another vendor to start dealer-gamma modeling.

The sensible path is:

1. use official daily OI
2. combine it with minute-level spot and IV
3. compute daily and minute repriced GEX maps
4. add trade-flow adjustments later only if they improve predictive performance

## What The Internet And Papers Suggest

### Main Lessons From The Research Sweep

1. Do not rely on price-only trend filters.
2. Volatility regime variables are central, not secondary.
3. Cross-sectional structure matters: breadth, concentration, and correlation are regime information.
4. Options-flow imbalance can add predictive power beyond underlying returns.
5. Social sentiment can help near transitions, but should be treated as a noisy auxiliary signal.

### ArXiv And Related Papers Worth Using

#### 1. `RegimeFolio: A Regime Aware ML System for Sectoral Portfolio Optimization in Dynamic Markets`

Why it matters:

- the paper finds regime-aware allocation improves results over static benchmarks
- it explicitly uses a VIX-based regime classifier and interpretable features
- this supports including volatility-regime features in the first model version

What to borrow:

- interpretable baseline before deep models
- explicit regime probabilities
- sector and asset-specific models instead of a single universal signal

#### 2. `Representation Learning for Regime Detection in Block-Hierarchical Financial Markets`

Why it matters:

- it argues regime information lives in changing market structure and relationships, not only in single-series momentum
- supports using correlation, concentration, and constituent network features

What to borrow:

- include constituent co-movement features
- measure concentration and block behavior in Nasdaq leaders

#### 3. `Returns-Driven Macro Regimes and Characteristic Lead-Lag Behaviour between Asset Classes`

Why it matters:

- shows that macro regimes can be inferred from return interactions across assets
- strongly supports rates, credit, dollar, and cross-risk inputs

What to borrow:

- cross-asset lead-lag features
- joint regime definitions instead of equity-only tagging

#### 4. `Option Volume Imbalance as a Predictor for Equity Market Returns`

Why it matters:

- directly supports using options volume imbalance as a predictive input
- especially relevant given our existing options data architecture

What to borrow:

- call/put imbalance features
- separate treatment of moneyness buckets and tenor buckets

#### 5. `A Hybrid Learning Approach to Detecting Regime Switches in Financial Markets`

Why it matters:

- directly relevant to the problem of regime classification under non-stationary financial data
- supports using hybrid feature sets instead of a single model family

What to borrow:

- combine statistical and machine-learning views of state transitions
- compare interpretable baselines against more flexible models

#### 6. `Sentiment Analysis of Twitter Data for Predicting Stock Market Movements`

Why it matters:

- supports keeping X or Twitter-derived features as a secondary input family
- useful mainly as an augmentation to market state, not as the core model

What to borrow:

- topic-filtered sentiment
- event-centered sentiment windows
- avoid naive global average sentiment

## Practitioner Signals Repeatedly Mentioned Online

Across practitioner commentary, recurring regime markers are:

- `VXN` and Nasdaq-specific implied-volatility regime
- put-skew steepening
- breadth deterioration under index strength
- semis relative weakness as early warning for Nasdaq trend damage
- yield spikes and real-rate pressure
- dealer gamma pinning vs negative-gamma acceleration
- rich covered-call premium during sideways markets

These reinforce the feature inventory above.

## X Research

Live X search was requested and a headed Playwright session was opened for this work.

Session used:

- browser session: `regime-x`
- user-authenticated search completed on 2026-03-14 local time

Queries run:

- `dealer gamma qqq`
- `nasdaq breadth qqq`
- `JEPQ covered call`
- `JEPQ sideways`
- `TQQQ volatility decay`
- `qqq skew vxn`
- `JEPQ upside capped`

### What X Added

The useful signal from X was not raw sentiment. It was repeated practitioner emphasis on a small set of concrete market-state variables.

Recurring themes:

1. Dealer gamma regime matters a lot for `QQQ`.
2. Breadth deterioration often leads or contradicts index price.
3. Equal-weight Nasdaq vs cap-weight Nasdaq is a recurring confirmation tool.
4. `TQQQ` and `SQQQ` are consistently discussed as path-dependent vehicles, not simple `3x` directional bets.
5. `JEPQ` is repeatedly framed as an income or sideways-market tool whose relative attractiveness rises when option premium is rich and upside is less clean.
6. Practitioners watch `VXN`, `SKEW`, bonds, and event catalysts together rather than in isolation.

### Practitioner-Derived Features To Add

The X pass makes these derived features worth adding explicitly:

- `dealer_gamma_sign`
- `gamma_flip_distance_to_spot`
- `call_wall_distance_to_spot`
- `put_wall_distance_to_spot`
- `positive_gamma_pin_score`
- `negative_gamma_acceleration_score`
- `qqq_equal_weight_relative_strength`
- `nasdaq_breadth_50dma_pct`
- `nasdaq_advance_decline_line_slope`
- `breadth_momentum_divergence`
- `vxn_level`
- `vxn_term_structure_slope`
- `skew_level`
- `skew_vs_vix_divergence`
- `covered_call_premium_richness`
- `jepq_upside_cap_cost_proxy`
- `leveraged_etf_volatility_decay_proxy`
- `tqqq_realized_drag_vs_3x_target`
- `sqqq_realized_drag_vs_-3x_target`

### Representative X Findings

Dealer gamma / pin vs acceleration:

- [The Foot on call wall / put wall / gamma flip interpretation](https://x.com/TheFoot_/status/2032991364322136186)
- [The Foot on long-gamma conditions creating choppy price action](https://x.com/TheFoot_/status/2032808233664909417)
- [CJ Cameron on positive dealer gamma pinning `QQQ`](https://x.com/Gammamap/status/2032317333222420789)

Breadth / participation:

- [kautious on `QQQ` at critical support with stretched breadth and two-way options flow](https://x.com/kautiousCo/status/2032609391392817622)
- [TradersLab on equal-weight Nasdaq participation collapse and breadth deterioration](https://x.com/TradersLab_/status/2030332725647851815)
- [Terry R Danish on Nasdaq-100 advance/decline breadth as a participation check](https://x.com/OnTrackCapital/status/2029658535504601533)

Volatility / skew / cross-asset stress:

- [quantedOptions on `VIX` / `VXN` / `VVIX` / `SKEW` as a joint volatility dashboard](https://x.com/quantedOptions/status/2025191816341999734)
- [Rasool on high `SKEW` with moderate `VIX` / `VXN` as quiet institutional hedging](https://x.com/RasoolUFT/status/1932546385913811123)

Leveraged ETF path dependency:

- [My Weekly Stock on `TQQQ` daily reset, volatility decay, and choppy-market downside asymmetry](https://x.com/MyWeeklyStock/status/2032924949598327189)

Covered-call / `JEPQ` framing:

- [Dividend-focused discussion highlighting `JEPQ` as covered-call tech income](https://x.com/DevotedDividend/status/2032241478123286743)
- [Dan - Accrue Returns on `JEPQ` trading upside for income](https://x.com/AccrueReturns/status/2030241014447837240)
- [Dividend King Pro on `JEPQ` as capped-upside income exposure](https://x.com/DividendKingPro/status/2027554517391200683)

### X Conclusion

The X sweep reinforced four design decisions:

1. Build the regime model around state variables, not end-of-day ETF returns.
2. Treat dealer gamma, breadth, and Nasdaq-specific vol/skew as first-class data families.
3. Model `TQQQ` and `SQQQ` with explicit path-dependency and realized-drag features.
4. Treat `JEPQ` as a volatility-income wrapper, not simply a "safe tech ETF."

## Recommended Acquisition Order

### Phase 1: MVP Dataset

Acquire first:

- `QQQ`, `TQQQ`, `SQQQ`, `JEPQ` minute and daily bars
- `QQQ` options surface summaries derived from existing raw quotes
- options flow imbalance and moneyness-tenor buckets from existing trade data
- `VIX`, `VXN`, `VIX1D`, `VIX3M`, `SKEW`, put/call ratios
- yields, SOFR, high-yield OAS, financial conditions, `DXY`
- Nasdaq breadth basics
- macro and earnings event calendars

This is enough to build:

- a first daily regime classifier
- a first transition model
- an action-selection backtest

### Phase 2: Strong Upgrade

Add:

- Nasdaq constituent-level bars
- correlation, dispersion, and concentration features
- ETF flow datasets
- borrow and financing data
- dealer gamma proxies
- transcript and headline embeddings

### Phase 3: Expensive But High-Upside

Add:

- richer X embeddings and account graph features
- order-book or depth data
- alternative sentiment feeds
- fund positioning and dark-pool style proxies if obtainable

## Modeling Recommendations

### Architecture

Start with a 3-layer approach:

1. `regime_state_model`
2. `regime_transition_model`
3. `action_policy_model`

Possible first stack:

- gradient-boosted trees or multinomial logistic regression for regime state
- hidden Markov model or transition-probability classifier on top
- separate utility model for each action

Do not start with a deep end-to-end transformer unless the structured baseline is already strong.

### Labels

Use both:

- human-readable states
- utility-maximizing forward labels

Examples:

- future `5d` and `10d` return
- future `5d` realized vol
- future `5d` max drawdown
- net utility of holding each instrument for `1d`, `3d`, `5d`

### Validation

Use:

- walk-forward splits
- purged time-series cross-validation
- regime-conditioned performance reporting
- transition recall, not just accuracy

The key metric is not classification accuracy. The key metric is whether the system improves net action selection around transition periods.

## Initial ClickHouse Tables To Add

Suggested first additions:

- `reference_macro_daily`
- `reference_vol_indices_daily`
- `reference_breadth_daily`
- `etf_wrapper_state_daily`
- `regime_feature_daily`
- `regime_feature_intraday`
- `regime_label_daily`
- `x_sentiment_daily`

## My Recommendation For The First Build

If we want the fastest path to a useful model, build this first:

1. `QQQ` price state
2. Nasdaq options surface state
3. options flow imbalance
4. `VXN`/`VIX`/`SKEW`/put-call state
5. basic breadth
6. rates/credit/dollar
7. action-wrapper adjustments for `TQQQ`/`SQQQ`/`JEPQ`

That is the minimum serious dataset.

## Sources

Official / market structure:

- [Invesco QQQ ETF](https://www.invesco.com/qqq-etf/en/home.html)
- [ProShares UltraPro QQQ (TQQQ)](https://www.proshares.com/our-etfs/leveraged-and-inverse/tqqq)
- [ProShares UltraPro Short QQQ (SQQQ)](https://www.proshares.com/our-etfs/leveraged-and-inverse/sqqq)
- [J.P. Morgan JEPQ Fund Story](https://am.jpmorgan.com/us/en/asset-management/adv/products/jpmorgan-nasdaq-equity-premium-income-etf-etf-shares-46654q203#/story)
- [J.P. Morgan JEPQ Fact Sheet](https://am.jpmorgan.com/us/en/asset-management/adv/products/jpmorgan-nasdaq-equity-premium-income-etf-etf-shares-46654q203#/documents)
- [Cboe VXN Index](https://www.cboe.com/us/indices/dashboard/vxn/)
- [Cboe VIX1D](https://www.cboe.com/us/indices/dashboard/vix1d/)
- [Cboe VIX3M](https://www.cboe.com/us/indices/dashboard/vix3m/)
- [Cboe SKEW Index](https://www.cboe.com/us/indices/dashboard/skew/)
- [Cboe Put/Call Ratio Overview](https://www.cboe.com/us/options/market_statistics/)
- [Cboe Nasdaq-100 BuyWrite V2 Index (BXNT)](https://www.cboe.com/us/indices/dashboard/bxnt/)
- [FRED: 2-Year Treasury Constant Maturity](https://fred.stlouisfed.org/series/DGS2)
- [FRED: 10-Year Treasury Constant Maturity](https://fred.stlouisfed.org/series/DGS10)
- [FRED: ICE BofA US High Yield OAS](https://fred.stlouisfed.org/series/BAMLH0A0HYM2)
- [FRED: Chicago Fed National Financial Conditions Index](https://fred.stlouisfed.org/series/NFCI)
- [New York Fed SOFR](https://www.newyorkfed.org/markets/reference-rates/sofr)
- [Nasdaq market activity / breadth references](https://www.nasdaq.com/glossary/a/advance-decline-line)

Research:

- [RegimeFolio: A Regime Aware ML System for Sectoral Portfolio Optimization in Dynamic Markets](https://arxiv.org/abs/2510.14986)
- [Representation Learning for Regime Detection in Block-Hierarchical Financial Markets](https://arxiv.org/abs/2410.22346)
- [Returns-Driven Macro Regimes and Characteristic Lead-Lag Behaviour between Asset Classes](https://arxiv.org/abs/2209.00268)
- [Option Volume Imbalance as a Predictor for Equity Market Returns](https://arxiv.org/abs/2201.09319)
- [A Hybrid Learning Approach to Detecting Regime Switches in Financial Markets](https://arxiv.org/abs/2108.05801)
- [Sentiment Analysis of Twitter Data for Predicting Stock Market Movements](https://arxiv.org/abs/1610.09225)
- [Tactical Asset Allocation with Macroeconomic Regime Detection](https://arxiv.org/abs/2503.11499)
- [Explainable Regime Aware Investing](https://arxiv.org/abs/2603.04441)

Repo context:

- [docs/PHENIX_ARCHITECTURE.md](/Users/pawanagarwal/github/phenixflow/docs/PHENIX_ARCHITECTURE.md)
- [docs/THETADATA_GREEKS_INPUT_SOURCES.md](/Users/pawanagarwal/github/phenixflow/docs/THETADATA_GREEKS_INPUT_SOURCES.md)
