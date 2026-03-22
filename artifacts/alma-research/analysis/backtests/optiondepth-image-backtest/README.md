# OptionDepth Image Backtest Strategy

This folder is for backtesting the `OptionsDepth Heatmap` image Alma includes in daily posts.

Current archive coverage from [inventory.json](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/analysis/backtests/optiondepth-image-backtest/inventory.json):

- `225` archived posts contain an `OptionsDepth Heatmap`
- `222` of those are daily/intraday posts
- `29` daily/intraday posts fall in `2026-02` and `2026-03`
- `28` of those `29` also include later `SCRIPT INPUTS`

## Key Design Choice

We should split this into two backtests, not one:

1. `image-native` backtest
   - uses only what is visibly present in the heatmap
   - safest first step
2. `Alma-derived` backtest
   - uses the heatmap plus Alma's nearby text to infer her derived levels
   - closer to her real workflow, but less purely mechanical

This matters because the image itself does **not** directly print all of Alma's published levels such as:

- centroid
- upside/downside pivot
- target
- vanna flip
- speed flip

Those are Alma's reduction layer on top of the structural map.

The raw image does visibly encode:

- `Gamma Peak` lines
- `Gamma Trough` lines
- `Gamma Zero` lines
- their slope over intraday time
- line spacing / compression
- late-day convergence
- hand-drawn or chart-rendered circled zones in some posts

So the first backtest should judge the predictive value of the image's raw geometry before we ask it to reproduce Alma's full derived map.

## What To Extract From The Image

For each heatmap image:

1. Calibrate axes
   - x-axis: market time in `ET`
   - y-axis: price
2. Segment the visual objects
   - green `Gamma Peak`
   - yellow `Gamma Trough`
   - dotted `Gamma Zero`
   - optional dotted oval highlight zones
3. Convert pixels to coordinates
   - each curve becomes a time series of `(time, price)`
4. Build derived geometric features
   - nearest line to open
   - line slope by segment
   - distance from spot to nearest zero/peak/trough
   - peak-trough corridor width
   - corridor expansion / compression through time
   - late-day convergence into a pin zone
   - number of disjoint corridors above/below spot

## Recommended Backtest Phases

### Phase 1: Inventory + Calibration

Already in place:

- [inventory.json](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/analysis/backtests/optiondepth-image-backtest/inventory.json)
- [inventory.md](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/analysis/backtests/optiondepth-image-backtest/inventory.md)
- [inventory-optiondepth-heatmaps.js](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/tools/inventory-optiondepth-heatmaps.js)

This gives us a reliable mapping from the post to the exact archived OD image file.

### Phase 2: Image-Native Signal Backtest

This should be the first real POC.

Hypotheses to test directly from the image:

- `zero-line breakout`: crossing and holding beyond `Gamma Zero` should predict larger travel and higher realized volatility than rejection at the same line.
- `corridor fade`: when spot reaches the outer `Peak/Trough` corridor and quickly rejects, price should mean-revert toward the interior corridor.
- `compression day`: narrow `Peak/Trough` spacing near the open should predict lower realized volatility and more pinning.
- `expansion day`: widening spacing and strong line slope should predict larger range and poorer pin behavior.
- `late pin`: if lines converge after `14:00 ET`, the final 60-90 minutes should pin toward the converged zone.

This phase is clean because it does not require us to guess Alma's private reduction logic.

### Phase 3: Alma-Derived Level Backtest

After Phase 2 works, use the nearby OD commentary as supervision to infer:

- centroid
- upside/downside pivot
- targets
- flip zones
- sticky support/resistance zones

The adjacent text is useful here because Alma often writes things like:

- `Centroid is 6674.13`
- `Downside pivot is at 6594.81`
- `target is 6588.49`
- `Upside pivot is at 6772.38`

That gives us historical labels for learning the mapping from image geometry to Alma's derived level set.

Recommended use of text in this phase:

- use text as `training / labeling`
- do not use text to decide whether the trade worked
- judge trade outcome only from market data

## Trade Families To Test First

These are the most defensible first rules.

### 1. Zero-Line Breakout

Entry:

- market reaches the nearest `Gamma Zero` line
- then closes beyond it for `2` consecutive 1-minute bars

Direction:

- trade in the direction of the break

Exit:

- target = next visible structural line or fixed time stop
- stop = back through the broken `Gamma Zero` band

Why first:

- easiest image-native rule
- most deterministic

### 2. Corridor Reversion

Entry:

- market touches outer `Gamma Peak` or `Gamma Trough` corridor
- then rejects back inside within `N` minutes

Direction:

- fade back toward corridor midpoint or nearest interior line

Exit:

- target = corridor midpoint / nearest opposite line
- stop = break through the outer corridor plus tolerance

Why:

- closest to Alma's repeated reversion / sticky-zone language

### 3. Late-Day Pin

Entry:

- after `14:00 ET`, `Peak/Trough/Zero` lines converge into a narrow zone
- spot remains within a small distance of that zone

Direction:

- trade toward the converged zone

Exit:

- `15:55 ET` or on arrival into the pin zone

Why:

- Alma repeatedly ties OD structure to PM-session pinning

### 4. Derived Pivot Trade

This belongs to the later `Alma-derived` phase.

Entry:

- use extracted `centroid` / `pivot` / `target` labels
- trade reclaim, rejection, or continuation exactly as with the script-rule backtests

Why:

- closest to her published intraday plans
- but requires the derivation layer first

## Data For Outcome Measurement

For now:

- use Yahoo `^GSPC` minute bars
- keep all signal times in `ET` because the chart is drawn in market time

Later:

- switch to ClickHouse minute data
- then add realized-volatility, VWAP, and SPX/SPY cross-checks

## Metrics

For each strategy:

- trade count
- gross profit / gross loss / net profit
- win rate
- max drawdown
- stop-loss count
- MAE / MFE
- profit factor

For structural quality, also track:

- touch rate of predicted zones
- average reversal distance after a touch
- breakout continuation distance after a confirmed zero-line break
- realized-volatility percentile conditioned on corridor width
- pin accuracy into `15:30-16:00 ET`

## Recommended Immediate POC

The best next build is:

1. vectorize the `Gamma Peak`, `Gamma Trough`, and `Gamma Zero` lines from the `2026-02` and `2026-03` daily images
2. create an `image-native` dataset with one row per day
3. backtest only:
   - `zero-line breakout`
   - `corridor reversion`
   - `late-day pin`

That will tell us if the image geometry itself has predictive value before we spend effort learning Alma's full centroid/pivot reduction logic.

## Practical Reading Of March Example

On [2026-03-13](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-03-13_pce-breakdown-intraday-post-13march/content.txt), the OD image at [4da2f5fc-21e1-4d05-a0d9-9e3bd6221230_1881x770.jpg](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-03-13_pce-breakdown-intraday-post-13march/images/4da2f5fc-21e1-4d05-a0d9-9e3bd6221230_1881x770.jpg) visibly shows:

- a lower corridor around the mid-`6600s`
- an upper corridor around the `6740-6780` area
- a late-day upper cluster near `6845-6860`
- dotted `Gamma Zero` paths separating those zones

That is enough for an image-native test of:

- corridor reversion
- zero-line break / reject
- PM pin or migration

It is **not** enough by itself to claim the exact `Centroid is 6674.13` number unless we build the Alma-derived layer.
