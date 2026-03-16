# Replicating OptionsDepth: Internet Research on Required Data Feeds

## Executive Answer

There are really two different replication targets:

1. A **public-data replica** of the OD heatmap and broad gamma/charm/vanna regions.
2. A **close-to-OD dealer-book replica** that tries to match OD's claimed "exchange-tagged" market-maker positioning model.

The public-data version is very feasible with a strong OPRA/index vendor like ThetaData plus daily open interest.

The close-to-OD version is much harder. Based on the sources below, I do **not** see a single public consolidated feed that gives:

- all-exchange options quotes and trades,
- participant type,
- buy/sell,
- opening vs closing classification,
- and enough structure to recover an actual dealer inventory book intraday.

My inference from the sources is:

- **Broad OD-style heatmaps are replicable from public feeds.**
- **Exact OD-style dealer-position overlays likely require stitching multiple venue-specific proprietary open/close products, or some proprietary normalized feed/model on top of them.**

## What OD Says It Uses

OptionsDepth makes two very strong public claims:

- It says its data is sourced from **"exchange-tagged information"** and gives "clarity on market participant positions."  
  Source: [optionsdepth.com home page](https://www.optionsdepth.com/) and specifically the statement that their data is "sourced directly from exchange-tagged information."
- It says the old approach of simply doing `OI * gamma` is obsolete because:
  - OI does not reflect market makers' positions,
  - gamma is cumulative across the whole portfolio,
  - gamma is dynamic and convex across price and time.  
  Source: [Market Makers' Gamma Exposure Projection](https://www.optionsdepth.com/resouce/market-makers-gamma-exposure-projection)
- It also explicitly says the solution is to use the kind of detailed exchange data that can distinguish market makers from customers, rather than relying only on OI or naive bid/ask aggressor inference.  
  Source: [The Pitfalls of Open Interest: Unlocking True Market Positions](https://www.optionsdepth.com/resouce/the-pitfalls-of-open-interest-unlocking-true-market-positions)

That means OD is publicly positioning itself as something **more informative than OPRA + daily OI alone**.

## What You Need To Reproduce The Broad Heatmap

This is the minimum stack to reproduce most of the **Gamma / (Δ / 2.5 pts)** region map:

### 1. Full listed options trade + quote feed

You need consolidated options trades and NBBO quotes across U.S. listed options.

- ThetaData states it receives every NBBO quote and trade from the OPRA feed in real time.  
  Source: [ThetaData SIPs page](https://docs.thetadata.us/Articles/Data-And-Requests/The-SIPs.html)
- ThetaData `trade_quote` returns every trade reported by OPRA paired with the last NBBO quote at the time of trade.  
  Source: [ThetaData option history trade_quote](https://docs.thetadata.us/operations/option_history_trade_quote.html)
- ThetaData `full trade stream` also states it returns every U.S. option trade reported on OPRA, with quote context.  
  Source: [ThetaData full trade stream](https://http-docs.thetadata.us/Streaming/US-Options/Full-Trade-Stream.html)

Practical requirement:

- **Feed needed:** OPRA-consolidated options quotes and trades.
- **Concrete vendor option:** ThetaData options feed.

### 2. Underlying/index price feed

For SPX/VIX-style charts you also need the underlying index price history.

- ThetaData has official index OHLC history, and their docs note that exchanges typically generate a price report every second for popular indices like `SPX`.  
  Source: [ThetaData index history OHLC](https://docs.thetadata.us/operations/index_history_ohlc.html)
- ThetaData subscriptions page also states index data resolution depends on the reporting exchange and calls out `SPX`.  
  Source: [ThetaData subscriptions](https://docs.thetadata.us/Articles/Getting-Started/Subscriptions.html)

Practical requirement:

- **Feed needed:** cash index ticks or OHLC for `SPX`, `VIX`, etc.
- **Concrete vendor option:** ThetaData Index Data.

### 3. Daily open interest

You need official contract-level open interest as the starting inventory baseline.

- OCC is the official open-interest source. Their market-data pages and DDS guide describe open-interest messages created for each tradable instrument.  
  Sources:
  - [OCC Open Interest](https://www.theocc.com/market-data/market-data-reports/volume-and-open-interest/open-interest)
  - [OCC Ovation DDS guide](https://www.theocc.com/getmedia/adb82faf-1b16-4ed0-a01f-be12cf2777f5/OV_DDS_Market_Data_Output_Guide.pdf)
- ThetaData also exposes historical and snapshot open-interest endpoints and lists open interest in its options endpoint catalog.  
  Source: [ThetaData subscriptions](https://docs.thetadata.us/Articles/Getting-Started/Subscriptions.html)

Practical requirement:

- **Feed needed:** daily official contract-level OI.
- **Concrete vendor options:** OCC batch/DDS or ThetaData open interest.

### 4. Intraday Greeks, including higher-order Greeks

To reproduce OD-style price-time surfaces and overlays, you need more than delta and gamma.

- ThetaData `all greeks` provides:
  - gamma
  - vanna
  - charm
  - vomma
  - veta
  - vera
  - speed
  - zomma
  - color
  - ultima  
  Source: [ThetaData all greeks](https://docs.thetadata.us/operations/option_history_greeks_all.html)
- ThetaData also exposes **third-order greeks** historically.  
  Source: [ThetaData third-order greeks](https://docs.thetadata.us/operations/option_history_greeks_third_order.html)

Practical requirement:

- **Feed needed:** either
  - vendor-supplied intraday first/second/third-order greeks, or
  - quotes + IV + your own Greeks engine.
- **Concrete vendor option:** ThetaData Pro is the cleanest current fit.

### 5. Rates/dividends and precise EOD methodology

If you compute greeks yourself, you also need model inputs like rates and dividends.

- ThetaData's Greeks endpoints explicitly expose `rate_type`, `rate_value`, `annual_dividend`, and `version`, including special handling for 0DTE time-to-expiry.  
  Sources:
  - [ThetaData EOD Greeks](https://docs.thetadata.us/operations/option_history_greeks_eod.html)
  - [ThetaData all greeks](https://docs.thetadata.us/operations/option_history_greeks_all.html)

Practical requirement:

- **Feed needed:** risk-free curve plus dividend assumptions, unless you use vendor-computed Greeks consistently.

## What You Need To Get Closer To OD's Claimed Dealer Inventory Model

This is where the feed stack changes materially.

OD explicitly argues that OI alone is not enough and that exchange-tagged participant data matters.

### 6. Exchange-tagged participant + open/close data

This is the most important extra layer if you want to move beyond a public `OI + greeks` proxy.

#### Cboe

- Cboe Open-Close Volume Summary categorizes every trade by:
  - participant type
  - buy/sell
  - open/close
  - and in some cases size bucket
- It is available as EOD and intraday snapshots, including `1-minute` and `10-minute` feeds.  
  Source: [Cboe Open-Close Volume Summary](https://datashop.cboe.com/cboe-options-open-close-volume-summary)

#### Nasdaq ISE / GEMX

- ISE/GEMX Open/Close Trade Profile offers proprietary exchange data about:
  - participant category
  - opening/closing activity
  - and intraday snapshots every 10 minutes
- Nasdaq explicitly says the report **does not include trade data from other exchanges**.  
  Sources:
  - [ISE/GEMX Trade Profile FAQ](https://www.nasdaqtrader.com/content/ProductsServices/DATAPRODUCTS/ISE/ISE-GEMX%20Consolidated%20Trade%20Profile%20FAQs%20v2.pdF)
  - [ISE/GEMX field description](https://www.nasdaqtrader.com/content/ProductsServices/DATAPRODUCTS/ISE/ISE-GEMX%20Consolidated%20Field%20Description%20-%20May%202017%20v2.pdf)
  - [Nasdaq filing on ISE Open/Close Trade Profile](https://listingcenter.nasdaq.com/assets/rulebook/ise/filings/SR-ISE-2024-32.pdf)

#### NYSE Arca / American

- NYSE Options Open-Close Volume Summary provides:
  - origin / participant type
  - buy/sell
  - opening/closing
  - intraday 10-minute updates
- NYSE states it is trading activity **on the Exchange**, meaning venue-specific, not a full-market consolidation.  
  Sources:
  - [NYSE product page](https://www.nyse.com/data-products/catalog/open-close-volume-summary)
  - [NYSE client specification PDF](https://www.nyse.com/publicdocs/nyse/data/NYSE_Options_Exchange_Open-Close_Client_Specification_v1.0c.pdf)

#### MIAX

- MIAX rules describe an Open-Close Report summarizing volume by:
  - origin
  - trade size
  - opening/closing
- It is available on both intraday and end-of-day bases.  
  Sources:
  - [MIAX market data policies](https://www.miaxglobal.com/sites/default/files/job-files/MIAX_Exchange_Group_Market_Data_Policies_01292024.pdf)
  - [MIAX Options rules](https://www.miaxglobal.com/sites/default/files/page-files/MIAX_Options_Exchange_Rules_01262023.pdf)

## Important Inference From Those Venue Products

I did **not** find a public single all-exchange feed that combines:

- all OPRA trades/quotes,
- participant type,
- buy/sell,
- opening/closing,
- and all venues in one standardized stream.

What I found instead is:

- OPRA-style consolidated trade/quote feeds for the whole market, and
- **venue-specific** proprietary open/close sentiment/profile products.

So my inference is:

- **To get closer to OD's claimed exchange-tagged position model, you probably need to stitch multiple exchange-specific open/close products on top of OPRA.**
- **A single public feed is probably not enough.**

## What Reddit And Practitioner Threads Add

Reddit was useful here, not as a primary authority, but as a cross-check on how practitioners actually build and use these models.

The highest-signal threads aligned with the formal feed documentation surprisingly well.

### 1. Public GEX models are widely understood as approximations

Several Reddit threads make the same point as OD's own marketing copy: `OI * gamma` style models can be useful, but they are still models, not direct measurements of a dealer book.

- In a long-running `r/options` discussion, one commenter lays out the standard public formula of summing call gamma and subtracting put gamma weighted by open interest, while another points out that this cannot recover exact market-maker inventory from anonymous public data. The rebuttal is that the result can still be useful as a simplified trading model even if it is not literally the true dealer book.  
  Source: [The chart moderators don't want you to see](https://www.reddit.com/r/options/comments/ep7yrs/the_chart_moderators_dont_want_you_to_see/)
- Another Reddit thread explicitly questions the common assumption that dealer gamma can be signed just from call-vs-put open interest, noting that the assumption can look unstable in real data.  
  Source: [Gamma Exposure is it any Good?](https://www.reddit.com/r/options/comments/1kdmdg0/gamma_exposure_is_it_any_good/)

This is consistent with the main conclusion above:

- **public data can produce a useful gamma map**
- **public data alone does not identify exact dealer inventory**

### 2. Intraday "positioning" is generally inferred, not officially observed

One of the most useful Reddit confirmations was operational:

- traders repeatedly note that official open interest is known only at the open from the previous close, and that intraday changes have to be inferred from flow rather than read from a true minute-by-minute OI field.  
  Source: [Is anybody using big changes in open interest to find trades??](https://www.reddit.com/r/swingtrading/comments/19bzvoh/is_anybody_using_big_changes_in_open_interest_to/)

This directly supports our earlier inference about OD:

- if OD shows changing exposure throughout the session, they are almost certainly **projecting and updating a model**, not reading a true intraday open-interest feed

### 3. Traders describe these products as strongest for pre-open and short-term regime mapping

Reddit discussions of SpotGamma-style tools repeatedly describe them as most useful for:

- pre-open support/resistance
- short-term `SPX` / `ES` regime context
- identifying squeeze or breakout conditions

Examples:

- a user who had used SpotGamma for years called it most helpful for `SPX / ES` and especially for shorter-term trading; another described the product as mainly static pre-open levels.  
  Source: [SpotGamma](https://www.reddit.com/r/options/comments/tbnq0i/spotgamma/)
- another thread shows practitioners converting strike-level OI, gamma, delta, and vanna into estimated hedging notional for `SPX` and treating those as support/resistance or pinning walls rather than precise inventory truth.  
  Source: [HEADING INTO OPEX WEEK: SPX GEX](https://www.reddit.com/r/options/comments/1lza9th/heading_into_opex_week_spx_gex/)

That lines up with what we have already observed from OD and Alma:

- strongest use case = **session map / regime map**
- weaker claim = **exact live dealer book**

### 4. Real-time or intraday updates are usually vendor/model overlays, not raw feed fields

Practitioner threads also point to a clear industry split:

- `EOD` and pre-open gamma maps are commonly built off open interest plus Greeks
- "real-time gamma changes" are sold as higher-end vendor/model products

In the SpotGamma discussion, a commenter distinguishes the static pre-open levels from "real-time gamma changes" tools that try to detect same-day squeezes.  
Source: [SpotGamma](https://www.reddit.com/r/options/comments/tbnq0i/spotgamma/)

This supports the feed-stack distinction in this note:

- **public replica:** OPRA + index + OI + Greeks
- **closer intraday dealer model:** add venue-specific open/close participant feeds and/or a stronger inventory inference engine

### 5. Common public formulas use "1% move" or "hedging notional" scaling

Reddit discussions around `GEX` and `VEX` regularly express exposure in terms of:

- `OI * gamma * contract multiplier * spot * 1% move`
- or an equivalent "hedging notional for a 1% move" framing

Examples:

- `r/options` discussion of `VEX` starts from the common `GEX` scaling of notional times gamma times a 1% underlying move.  
  Source: [Vanna exposure (VEX) calculation](https://www.reddit.com/r/options/comments/16yvjte/vanna_exposure_vex_calculation/)
- a Moontower-on-Gamma thread walks through the percent-move formulation that underlies the classic "dollar gamma" scaling traders use.  
  Source: [Moontower on Gamma](https://www.reddit.com/r/options/comments/14jogpn/moontower_on_gamma/)

That matters for OD replication because OD's legend uses:

- `Gamma / (Δ / 2.5 pts)`

which is still a **scaled hedging-impact framing**, not raw model gamma straight from Black-Scholes.

## What Reddit Changed In My View

Reddit did **not** change the main feed conclusion, but it did sharpen the practical interpretation:

- Most practitioners already treat these maps as **useful structural models**, not literal position truth.
- Intraday OD-style changes should be understood as **model-updated inventory/exposure estimates**, not official intraday OI.
- The most realistic public target is a **strong pre-open and same-session regime map**, not a perfect dealer-book reconstruction.

## What TailThatWagsDog And The Public nextSignals App Add

I also reviewed a recent public X thread and linked app from `@TailThatWagsDog`, because it is one of the clearest public examples of a practitioner trying to turn `GEX`-style positioning into a real decision product:

- X post: [major update with public interactive app](https://x.com/TailThatWagsDog/status/2033161694441013310)
- X post: [gamma + vanna + LOB risk-surface snippet](https://x.com/TailThatWagsDog/status/2029200688748605453)
- X post: [naive dealer-assumption disagreement versus LOB](https://x.com/TailThatWagsDog/status/2028664372260663609)
- Public app: [nextSignals Directional Index Crash Risk Monitor](https://snazzy-concha-bb8adb.netlify.app/)

This did not overturn the feed conclusions above, but it did add a very useful public benchmark.

### 1. A useful public/commercial benchmark is clearly possible

The app exposes a real positioning dashboard with:

- `GEX`
- `VEX`
- `GEX+`
- `ZERO-gamma`
- a `spot move × IV shock` risk surface
- crash-risk tables
- and a probability-forecast layer

That strongly supports the idea that a meaningful public-facing positioning product can be built from public or commercial market data without needing OD's exact proprietary feed stack.

### 2. Better sign classification appears to be one of the biggest upgrades

The most important extra clue from that account is the repeated emphasis on disagreement with naive dealer-sign assumptions:

- one post claims `43.6%` of `LOB`-traded contracts disagreed with the naive dealer assumption
- the public app claims disagreement above `50%` for all three reviewed expiries

So the evidence-based lesson is:

- **plain `OI * gamma` is enough for a rough heatmap**
- **LOB-aware or better microstructure-aware sign classification may be the biggest public/commercial step up from the rough heatmap**

### 3. Gamma alone is not the whole public benchmark

The app is not just a gamma map. It explicitly mixes:

- gamma
- vanna
- spot shocks
- IV shocks
- and risk-neutral distribution extraction

That means a stronger public benchmark stack should include:

- full chain quotes
- higher-order Greeks
- a fitted IV surface
- and ideally data that improves trade-direction or position-direction inference

### 4. This supports a layered feed conclusion

The public benchmark suggests three practical layers:

- **Layer 1:** OPRA + underlying + OI + Greeks -> broad heatmaps and zero/peak/trough structure
- **Layer 2:** add better sign classification such as `LOB`, aggressor-side, spread-leg filters, or venue-level open/close products -> much better directional-position proxy
- **Layer 3:** add forecast extraction and spot/vol stress surfaces -> a more decision-ready regime dashboard

That still does **not** mean OD itself is fully replicable from a single public feed.

But it does mean:

- a very useful `OD`-adjacent product is realistic with `public/commercial options data + better sign modeling`

## Optional But Valuable Additional Feeds

### 7. Aggressor side / spread-leg trade classification

If you do not have full venue-level participant data, you still want better trade classification than raw OPRA prints.

- dxFeed documents `aggressorSide` and `spreadLeg` in its options sale/time-and-sale model.  
  Source: [dxFeed QD model of market events](https://kb.dxfeed.com/en/data-model/market-events/qd-model-of-market-events.html)
- ThetaData also documents option trade conditions, which helps filter special prints and non-standard trades.  
  Source: [ThetaData trade conditions](https://docs.thetadata.us/Articles/Errors-Exchanges-Conditions/Trade-Conditions.html)

Practical requirement:

- **Feed needed:** something that helps separate true directional prints from spread legs, corrections, or noisy prints.
- **Concrete options:** dxFeed options sale feed, or ThetaData trade+quote plus your own classifier.

### 8. Overnight / GTH coverage for SPX and VIX

If you want OD-like pre-open maps or more faithful morning state estimates for SPX/VIX, overnight options data matters.

- ThetaData notes OPRA Global Trading Hours for `SPX`, `VIX`, and `XSP`.  
  Source: [ThetaData SIPs page](https://docs.thetadata.us/Articles/Data-And-Requests/The-SIPs.html)
- ThetaData also documents historical ETH/GTH availability limitations and later coverage.  
  Source: [ThetaData making requests](https://docs.thetadata.us/Articles/Data-And-Requests/Making-Requests.html)

Practical requirement:

- **Feed needed:** overnight SPX/VIX options trades and quotes if you want the morning book to reflect overnight activity.

### 9. ES futures data for realized hedge validation

OD's own gamma article says SPX-option dealers typically hedge using **ES futures**.

- Source: [OptionsDepth gamma article](https://www.optionsdepth.com/resouce/market-makers-gamma-exposure-projection)

Practical implication:

- If you want to study how dealer hedging actually hit the market, add:
  - **CME E-mini S&P futures feed**
- If you only want to reconstruct the map itself, this is optional.

## Recommended Feed Stack By Goal

## A. Cheapest workable public replica

Goal:

- recreate most of the heatmap,
- broad positive/negative gamma regions,
- a main zero line,
- rough peak/trough structure.

Feeds:

- ThetaData options history:
  - trade_quote
  - open_interest
  - all greeks / EOD greeks
- ThetaData index history for `SPX` / `VIX`

Result:

- Good for broad OD-style heatmaps.
- Not enough for exact OD branch geometry or participant-position fidelity.

## B. Best public approximation

Goal:

- get much closer to OD's participant-aware model.

Feeds:

- ThetaData OPRA + index stack
- OCC daily open interest
- Cboe Open-Close Volume Summary
- Nasdaq ISE/GEMX Open-Close Trade Profile
- NYSE Options Open-Close Volume Summary
- MIAX Open-Close Report
- Optional: dxFeed aggressor/spread-leg classification
- Optional: CME ES futures

Result:

- Much better estimate of who opened/closed and which participant bucket was involved.
- Still requires a lot of stitching and normalization.
- Still may not fully equal OD.

## C. Closest plausible non-proprietary setup

Goal:

- approach OD as closely as possible without private exchange relationships or OD's own model.

Feeds:

- Everything in B
- Your own inventory engine that rolls:
  - prior OI,
  - participant-tagged opening/closing,
  - buy/sell direction,
  - multi-venue aggregation,
  - greeks repricing across the full price-time surface.

Result:

- Likely the closest you can get publicly.
- Still not guaranteed to match OD's exact line topology or intraday updates.

## What Seems Non-Replicable Or Very Hard Publicly

- A fully consolidated all-venue dealer inventory book in real time.
- Exact OD peak/trough/zero branching.
- Any hidden normalization or netting logic OD applies to exchange-tagged participant data.
- A true minute-by-minute "open interest" feed for options.

That last point is especially important:

- official open interest is a **daily** concept,
- exchange intraday open/close products help estimate book changes,
- but they do not turn OI into a true official intraday field.

## Bottom Line

If the question is:

### "Can we replicate the OD chart broadly?"

Yes.

Use:

- **ThetaData Options + Index**
- daily OI
- intraday/all-contract Greeks

### "Can we replicate what OD claims is the actual dealer-position model?"

Not from OPRA alone.

You also need:

- **exchange-tagged open/close participant feeds**
- from **Cboe + Nasdaq venue products + NYSE + MIAX**
- and a custom multi-venue inventory engine

### "Is there one single public feed that does all of this?"

I did not find one.

My best evidence-based conclusion is:

- **ThetaData is enough for the public heatmap replica.**
- **A close OD-style dealer-position replica needs multiple proprietary exchange sentiment/open-close feeds.**
- **An exact OD clone is still unlikely without OD's own proprietary aggregation/modeling layer.**
