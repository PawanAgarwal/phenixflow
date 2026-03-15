# OptionDepth Feature Catalog

Observed on 2026-03-15 in a logged-in `app.optionsdepth.com` session.

This catalog is based on live exploration of the authenticated product using Playwright. It focuses on features that were directly visible in the UI during this session. Where a capability is described as "observed in controls" or "observed in DOM", that means the feature was present in the live interface even if I did not execute the final mutating step.

I avoided irreversible actions such as logout, purchases, subscription checkout, or saving dashboard changes.

## Artifact Layout

- `screenshots/`: UI captures used for this catalog
- `notes/`: Playwright accessibility snapshots and extracted UI structure
- `browser-profile/optiondepth-auth-state.json`: saved storage state for follow-up crawling

## Global App Structure

### Main navigation

Observed persistent left navigation entries:

- `Home`
- `My Dashboard`
- `Market Maker's Exposure`
- `Positional Insights`
- `Depth View`
- `Knowledge Base`
- `Data Shop`
- `API Units`

Observed global user/account entry points:

- profile menu under initials button (`PA`)
- `Settings`
- `Membership`
- `Logout`

Observed global context controls reused across analytics pages:

- date selector
- underlying selector
- chart/help/settings controls

Screenshots:

- [Home viewport](screenshots/02-home-refresh.png)
- [Profile menu](screenshots/10-profile-menu-open.png)

## 1. Home

Route observed:

- `/home`

What it appears to do:

- acts as the landing dashboard/home hub for the application
- mixes market context, educational content, and external embedded market/news widgets

Observed sections:

- `Economic Calendar`
  - embedded TradingView-style calendar widget
  - shows dated macro events, timing, and importance labels
- `Articles Spotlight`
  - featured PDF/article cards
  - examples observed:
    - `Market Makers' Charm Exposure Projection`
    - `Delta Hedging Fundamentals`
    - `Interpreting Market Makers' Gamma Exposure Heatmaps`
    - `Analyzing Breakdown by Strike Chart`
- `Featured Video`
  - embedded YouTube content
  - observed example: `Full Market Makers Delta Hedging Course For Traders (100% Free)`
- `Daily stock news`
  - embedded TradingView top stories/news area

Notable behavior:

- Home is more content hub than control surface
- it provides context and education around the analytics tools rather than being the analytics tool itself

Screenshots:

- [Home viewport](screenshots/02-home-refresh.png)
- [Home full page](screenshots/02-home-full.png)

## 2. My Dashboard

Routes observed:

- `/dashboard`
- `/dashboard/edit`

What it appears to do:

- provides a user-customizable workspace built from reusable market widgets
- starts with an empty-state onboarding flow if no dashboard is configured

Observed empty-state experience:

- heading: `Get Started with Your Dashboard!`
- guidance text encouraging the user to click edit and personalize the dashboard
- `Edit Dashboard` button

Observed edit/builder mode:

- `Cancel`
- `Save`
- drag-and-drop builder area
- widget palette with observed widget groups:
  - `MM Exposure`
    - `Gamma`
    - `Charm`
    - `Vanna`
  - `Positional Insights`
    - `Expirations Breakdown`
    - `Strikes Breakdown`
  - `Depth View`
    - two `Depth View` entries were listed in the builder surface
    - likely separate widget variants, though both labels rendered the same in the captured UI

What this means functionally:

- OptionDepth supports user-defined workspace composition instead of forcing a single fixed dashboard
- the builder is tied to the same date/ticker context controls as the analytics pages

Screenshots:

- [Dashboard empty state](screenshots/13-dashboard-builder-base.png)
- [Dashboard edit mode](screenshots/13b-dashboard-edit-open.png)

## 3. Market Maker's Exposure

Routes observed:

- `/market-makers?chart-type=gamma&chart-sub-type=Gradient&time-frame=1T`
- `/market-makers?chart-type=charm&chart-sub-type=Gradient&time-frame=1T`
- `/market-makers?chart-type=vanna&chart-sub-type=Gradient&time-frame=1T`
- `/market-makers?chart-type=gamma&chart-sub-type=3D&time-frame=1T`

What it appears to do:

- visualize dealer/market-maker greek exposure over price and time
- support multiple greek models, multiple visualizations, and detailed chart styling

Observed top-level controls:

- date selector
- underlying selector
- greek tabs:
  - `Gamma`
  - `Charm`
  - `Vanna`
- chart-style selector:
  - `Gradient`
  - `3D`
- time aggregation selector:
  - `1 min`
  - `2 min`
  - `3 min`
  - `5 min`
  - `15 min`
  - `65 min`
- help entry: `How it works?`
- settings/config button

Observed underlying choices:

- `SPX`
- `VIX`

Observed chart traces/legend items on the gamma view:

- `Gamma / (∆ / 2.5 pts)`
- `Gamma Peak`
- `Gamma Trough`
- `Gamma Zero`
- `OHLC`

Observed settings/configuration surface:

- section controls:
  - `Expand all`
  - `Collapse all`
  - `Color Scale`
  - `Colors`
  - `Zero Line`
  - `Grid`
  - `Xaxis`
  - `Yaxis`
  - `Legend`
  - `Colorbar`
  - `Cross Hair`
- color scale modes:
  - `OD Score`
  - `Absolute`
- line/marker style controls:
  - peak marker style
  - trough marker style
  - zero-line/grid line style
  - color pickers
  - numeric width/size inputs
- axis/legend controls:
  - font size
  - left/right legend placement
  - top/bottom legend placement
  - horizontal/vertical orientation
  - left/bottom colorbar placement
- reset/apply workflow:
  - `Reset`
  - `Apply`

Observed behavior notes:

- the current state is deep-linkable through query parameters
- the `3D` mode is a first-class option, not just a marketing label
- the page is the deepest chart-customization surface in the app

Screenshots:

- [Market Makers base view](screenshots/04-market-makers-state.png)
- [Symbol selector open](screenshots/04a-market-makers-symbol-open.png)
- [Chart-style selector open](screenshots/04b-market-makers-chartstyle-open.png)
- [Settings panel](screenshots/04g-market-makers-settings-open.png)
- [Charm tab](screenshots/04-charm.png)
- [Vanna tab](screenshots/04-vanna.png)
- [3D mode](screenshots/04-3d.png)

## 4. Positional Insights

Route observed:

- `/positional-insight`

What it appears to do:

- break down positioning across expirations and strikes
- compare current positioning vs flows
- segment by participant type and greek/metric family

Observed major sub-panels:

- `Exposure by Expiration`
- `Breakdown By Strike`

Observed participant / ownership toggles:

- `Customers`
- `MM`

Observed metric families:

- `Position - #`
- `DEX - δ`
- `GEX - $M/pt`
- `CEX - $M/5min`
- `VEX - $M/σ%`

Observed mode/filtering controls:

- `Net`
- `Flow`
- `All`
- `Range`
- `Specific`
- `ODTE` appeared as an additional expiration mode in the strike breakdown controls
- two date pickers
- reset/apply controls for filters

Observed directional/group toggles in rendered UI text:

- `Calls`
- `Puts`
- `Net`

Observed settings controls in DOM:

- `Positive Bars`
- `Negative Bars`
- `Xaxis`
- `Yaxis`
- `Legend`
- `Cross Hair`
- `Reset`
- `Apply`

Observed help/explainer content:

- the inline help modal explicitly describes `Breakdown by Expiration`
- explains `Net` as current positioning snapshot
- explains `Flow` as change between two selected moments
- notes that expirations and strikes can be filtered
- says Pro Max updates every 10 minutes
- points users toward future PDF/video walkthroughs and the knowledge base

What this means functionally:

- this page is closer to a factor decomposition / positioning explorer than a single chart
- it combines participant segmentation, greek selection, and temporal comparison

Screenshots:

- [Positional Insights main page](screenshots/05-positional-insight.png)
- [Positional Insights full page](screenshots/05-positional-insight-full.png)
- [Positional help modal](screenshots/15b-positional-help-open.png)

## 5. Depth View

Routes observed:

- `/depth-view?tab=table`
- `/depth-view?tab=heatmap`

What it appears to do:

- show strike-by-expiration matrix views of exposure/positioning
- support both tabular and heatmap-style representations

Observed main tabs:

- `Table`
- `Heatmap`

Observed participant / metric controls:

- `Customers`
- `MM`
- `Position - #`
- `DEX - δ`
- `GEX - $M/pt`
- `CEX - $M/5min`
- `VEX - $M/σ%`

Observed option-side controls:

- `All`
- `Calls`
- `Puts`

Observed analytical modes:

- `Net`
- `Flow`
- `Range`
- `Specific`

Observed date filter behavior:

- the heatmap route exposed two concrete date buttons in the captured state:
  - `06 Mar, 2026`
  - `15 Apr, 2026`

Observed table-mode behavior:

- renders a grid with strikes as rows and expirations as columns
- captured live values for `SPX/SPXW` expirations in March 2026
- this is the most spreadsheet-like page in the product

Observed heatmap-mode settings:

- `Customization`
- `Xaxis`
- `Yaxis`
- `Gradient Coloring`
  - `Log`
  - `Linear`
- `Heatmap Colors`
- `Heatmap Values`
  - `Absolute`
  - `Relative`
- `Cross Hair`
- `Reset`
- `Apply`

What this means functionally:

- Depth View is the matrix-analysis tool in the suite
- table mode is best for exact numeric inspection
- heatmap mode is best for fast visual scanning of concentration zones

Screenshots:

- [Depth View table](screenshots/06-depth-view.png)
- [Depth View full page](screenshots/06-depth-view-full.png)
- [Depth View heatmap](screenshots/06b-depth-view-heatmap.png)

## 6. Knowledge Base

Routes observed:

- `/knowledge-base`
- `/knowledge-base?id=1`

What it appears to do:

- provide in-app educational/tutorial documentation
- combine topic navigation with a long-form reading surface

Observed topic families:

- `Options 101`
- `Options Pricing 101`
- `First Order Greeks`
- `Market Mechanics`
- `Delta Hedging`
- `Second Order Greeks`
- `How Charts Work`
- `Custom Dashboard`
- `Breakdown by Strike`
- `Breakdown by Expiration`
- `DepthView`
- `Gamma Exposure Heatmap`
- `Charm Exposure Heatmap`
- `Vanna Exposure Heatmap`

Observed article-reader behavior:

- the route opened directly to `id=1`
- the loaded article was under `Options 101`
- observed article sections/subheadings included:
  - `Introduction to Options`
  - `Understanding Options Contracts`
  - `Call Option Analogy`
  - `Put Option Analogy`
  - `Selling Options: Selling Calls and Puts`
  - `Understanding Short Options`
  - `Long vs. Short Options: Benefits and Risks`
  - `Recap of Key Concepts`
  - `Appendix`

What this means functionally:

- the product includes a serious educational layer, not just UI tooltips
- the docs cover both foundations and platform-specific chart interpretation

Screenshots:

- [Knowledge Base page](screenshots/07-knowledge-base.png)
- [Knowledge Base full page](screenshots/07-knowledge-base-full.png)

## 7. Data Shop

Routes observed:

- `/data-shop?tab=purchase`
- `/data-shop?tab=buying-history`

What it appears to do:

- sell downloadable historical datasets separate from the in-app visualization experience
- support both package selection and order-history review

Observed tabs:

- `Purchase`
- `Buying History`

Observed purchase products/pricing in the captured state:

- `Gamma` at `$149/month`
- `Charm` at `$149/month`
- `Positional` at `$159/month`
- `Bundle` at `$249/month`

Observed purchase controls:

- `Select DTE Range`
- `Pay Now`
- `Checkout`

Observed content sections:

- `Choose Your Data Package`
- `General Description`
- `Technical Description`
- `Data Structure & Resolution`
- `Checkout (0)`

Observed sample-data support:

- `Download Sample File` link

Observed explanatory copy:

- the Gamma dataset description explains positive vs negative gamma regimes
- frames the output as a `gamma exposure surface`
- emphasizes full-strike/full-expiration inventory coverage and intraday projection

Observed buying-history behavior:

- empty state text:
  - `No purchases found.`
  - `Once you make a purchase your billing details will appear here.`

What this means functionally:

- OptionDepth monetizes raw datasets separately from subscriptions
- the data store is productized with descriptions, pricing, and sample-file inspection

Screenshots:

- [Data Shop purchase tab](screenshots/data-shop.png)
- [Data Shop purchase tab full page](screenshots/data-shop-full.png)
- [Data Shop buying history empty state](screenshots/09b-data-shop-history-clicked.png)

## 8. API Units

Routes observed:

- `/api-units`
- `/api-units-docs`

What it appears to do:

- sell developer/API access on a unit-based pricing model
- provide separate documentation for programmatic access to the same underlying datasets

Observed API Units commercial page:

- heading: `Api Units`
- displayed plan price: `$99`
- `Purchase Now` CTA
- `View Documentation` link
- `Overage Charges Information`

Observed commercial copy:

- `Basic Plan includes 2K API units`
- claims access to:
  - daily and intraday data
  - all metrics
  - Gamma and Charm heatmaps
  - DepthView visualizations
  - strike-specific breakdowns
  - expiration-based analytics
- states output is JSON formatted
- overage charge shown as `0.05 per API unit`

Observed API docs sections:

- `Heatmap`
- `Breakdown by Strike`
- `Breakdown by Expiration`
- `Depthview`
- `Intraday slots`

Observed documentation content on the Heatmap page:

- endpoint shown:
  - `GET options-depth-api/v1/heatmap/?key=YOUR_API_KEY`
- request parameter examples:
  - `date`
  - `ticker`
  - `model`
  - `type`
  - `min_price`
  - `max_price`
- model values called out:
  - `daily`
  - `intraday`
- code example language tabs observed in text:
  - `Shell`
  - `Python`
  - `Javascript`
  - `Node`
  - `Php`
  - `Ruby`
  - `Swift`
- sample response shows `price`, `value`, and `effectiveDatetime`
- docs note that API access requires `Pro Max Membership and API Unit subscription`

What this means functionally:

- the API is not a side note; it is a documented product surface with examples and response schemas
- the docs map directly onto the platform's visual modules

Screenshots:

- [API Units commercial page](screenshots/api-units.png)
- [API Units commercial page full page](screenshots/api-units-full.png)
- [API docs page](screenshots/08-api-docs.png)
- [API docs page full page](screenshots/08-api-docs-full.png)

## 9. Settings

Route observed:

- `/settings`

What it appears to do:

- manage user identity details, UI preferences, email preferences, and local reset actions

Observed sections:

- `Personal Info`
  - editable full name
  - editable email address
  - profile image/file input
- `Interface Theme`
  - theme controls are present, though I did not toggle them
- `Email Preferences`
  - `Email List`
  - `Important Notices`
  - `Daily Overview`
- `Current Plan`
  - captured UI text said:
    - `You are on a FREE plan`
    - `You have access to basic features. Upgrade to unlock advanced tools and insights.`
  - `Buy Subscription`
- `Reset Settings/Cache`
  - `Reset Settings`
  - `Reset Cache`

What this means functionally:

- the app exposes both account preferences and product-environment reset tools
- local state reset is a first-class user feature, which is notable for a data-heavy app

Screenshots:

- [Settings page](screenshots/11-settings-page.png)
- [Settings page full page](screenshots/11-settings-page-full.png)

## 10. Membership

Route observed:

- `/membership`

What it appears to do:

- present subscription plans and upgrade choices

Observed billing-period toggle:

- `Monthly`
- `Yearly`

Observed plans in the captured state:

- `Pro Max`
  - `$249.00 /per month`
  - claims updates every 10 minutes intraday
  - Discord server access
  - latest + historical dealer exposure
  - sentiment insight
  - priority support
- `TWI x OD Combo`
  - `$375.00 /per month`
  - combines TWI Pro and OD Pro Max
  - claims real-time gamma data with pro trade signals
  - references live streams and 24/7 support
  - explicitly mentions `3D gamma maps`
- `Pro`
  - `$199.00 /per month`
  - described as `Legacy Model`
  - latest daily data updates
  - Discord access

Observed CTAs:

- `Get Started` on each plan card

What this means functionally:

- subscriptions are tiered by data freshness and feature sophistication
- `Pro Max` is positioned as the most advanced/intraday plan

Screenshots:

- [Membership page](screenshots/12-membership-page.png)
- [Membership page full page](screenshots/12-membership-page-full.png)

## 11. Profile Menu / Account Access

Observed from the global `PA` profile button:

- `Settings`
- `Membership`
- `Logout`

What this means functionally:

- account administration is lightweight and centralized in the profile dropdown
- membership upsell/management is reachable from both the dropdown and the settings page

Screenshot:

- [Profile menu](screenshots/10-profile-menu-open.png)

## High-Level Product Map

If we group the product by function, the live app currently breaks down like this:

- Analytics/visualization:
  - Market Maker's Exposure
  - Positional Insights
  - Depth View
- Workspace/personalization:
  - My Dashboard
  - Settings
- Education:
  - Home content hub
  - Knowledge Base
  - inline `How it works?` explainers
- Commerce:
  - Membership
  - Data Shop
  - API Units
- Developer platform:
  - API Units docs

## Notable Implementation/UX Observations

- Market Maker's Exposure is the deepest visualization/customization module.
- Positional Insights is the most multi-dimensional filter-driven explorer.
- Depth View is the clearest matrix/table tool for exact strike-by-expiry inspection.
- Knowledge Base is much deeper than a tooltip library; it functions like a real course/reference area.
- Data Shop and API Units are separate commercial surfaces, suggesting different monetization paths for raw data vs developer access.
- Query params are meaningful in several places, especially Market Maker's Exposure and Depth View.
- Some nav links appeared duplicated in the DOM because the app renders both a desktop sidebar and another nav structure, but the feature set itself was consistent.

## Gaps / Follow-Up Candidates

Areas worth deeper follow-up in a second pass:

- exact behavior of Market Maker's Exposure `How it works?` on the Gamma page
- whether the two `Depth View` widgets in dashboard edit mode map to table vs heatmap variants
- topic-to-topic navigation patterns inside the Knowledge Base
- whether more underlying symbols become available under other plans or dates
- purchase/checkout modals and flows, if you want a commerce-specific catalog later
