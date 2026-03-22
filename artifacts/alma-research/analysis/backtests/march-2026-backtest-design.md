# March 2026 Backtest Design

This is the working design for turning Alma's March 2026 SPX predictions into deterministic trades.
It is intentionally focused on a practical research loop, not a production system.

Related artifacts:

- [POC report](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/analysis/backtests/march-2026-poc/README.md)
- [POC trades](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/analysis/backtests/march-2026-poc/trades.json)
- [POC driver](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/tools/backtest-march-pocs.js)
- [March daily ledger](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/analysis/predictions/daily/2026-03.md)

## March inventory

- March daily rows reviewed: `231`
- March SPX rows reviewed: `112`
- Price-action rows that were parseable into candidate trades: `41`
- After deduping near-identical carry rows: `31`
- Trades triggered under the first POC rules: `6`

That tells us the main problem is not minute-data simulation. The main problem is converting Alma rows into a trade schema cleanly enough that the simulator is fair.

## What looks directly backtestable

These are the families that already behave like trade candidates:

| Family | What Alma usually gives | Backtest readiness | First action |
| --- | --- | --- | --- |
| `breakout_long` / `breakout_short` | pivot + confirmation + target | High | Backtest first |
| `rejection_short` / `rejection_long` | upside or downside reversion zone | Medium-high | Backtest first |
| `support_bounce_long` / `resistance_fade_short` | support band or stabilization zone | Medium | Backtest after trigger refinement |
| `centroid_mean_reversion` | centroid / pin / balance level | Medium-low | Separate module |
| `vol_regime`, `macro`, `cross_asset` | color, pressure, oil, yields, VIX context | Low for direct trade PnL | Score as context, not direct trade |
| `optiondepth_heatmap` | image zones, pins, rejection bands | Medium, but extraction-dependent | Treat as its own module |

## Proposed research pipeline

### 1. Normalize each prediction into a trade intent

Do not simulate directly off the raw ledger row.
First convert each row into a structured intent:

| Field | Purpose |
| --- | --- |
| `trade_family` | breakout, rejection, support_bounce, centroid_reversion, vol_regime |
| `direction` | long, short, both, none |
| `activation_window` | same_day, this_week, pm_only, next_session_open |
| `trigger_type` | cross_above, cross_below, first_touch_zone, reject_zone, recapture_level |
| `trigger_levels` | one or more numeric levels |
| `invalidation_type` | fixed_level, opposite_zone_edge, close_back_inside, time_stop |
| `invalidation_levels` | one or more numeric levels |
| `target_type` | fixed_level, next_level, centroid, 1R, close |
| `target_levels` | one or more numeric levels |
| `confidence` | high, medium, low |
| `notes` | explanation of how the row was mapped |

This can be done with a hybrid flow:

1. Deterministic pre-parser for obvious pivot/target rows.
2. LLM mapping only for commentary or ambiguous rows that still look tradeable.
3. Human review for the small set of rows where the model is uncertain.

The LLM should classify and map rows into schema fields. It should not decide whether the trade made money.

### 2. Build family-specific entry rules

Different Alma prediction families need different entry logic.
Using one universal "first touch equals entry" rule will distort results.

#### Breakout

Use for rows like:

- "above the upside pivot at X, expect extension toward Y"
- "if downside pivot at X gives way, expect move toward Y"

Entry:

- Long: first 1-minute bar after `madeAt` whose `high >= pivot`
- Short: first 1-minute bar after `madeAt` whose `low <= pivot`

Stop:

- Preferred: opposite pivot if Alma gave one in nearby text
- Fallback: `max(0.5 * distance_to_target, 6 SPX points)`

Exit:

- Primary: explicit target `Y`
- Fallback: end of day

This was the cleanest March family in the POC.

#### Rejection / fade

Use for rows like:

- "reversion zone"
- "momentum breakers"
- "expect rejection on test of X-Y"

Entry:

- Do not short the first print into the zone.
- Require `touch zone` plus `failure to hold above zone low/high for N minutes`.
- Better first POC: enter on the first bar that trades into the zone and then closes back below the lower edge for short setups.

Stop:

- Zone high plus buffer

Exit:

- Explicit target if Alma gave one
- Otherwise next centroid / support / 1R
- Fallback end of day

The first POC used a simpler touch rule and still produced useful trades, but this family clearly needs confirmation logic.

#### Support bounce

Use for rows like:

- "support/stall zone"
- "selling to slow or stabilize in X-Y"
- "short-term bounce"

Entry:

- Require `touch support zone` plus a `reclaim` confirmation.
- Good rule to test first: enter long on the first bar after touch that closes back above the top of the zone.

Stop:

- Zone low minus buffer

Exit:

- 1R
- centroid
- upside pivot if present
- end of day

The initial touch-only POC produced no triggered trades, which suggests the entry rule is too naive or the zone interpretation needs confirmation.

#### Centroid mean reversion

Use for rows like:

- "centroid is X"
- "pin"
- "main balance / reaction level"

This needs a different structure entirely.
Centroid is usually not a directional breakout level. It is closer to a magnet / balance concept.

Suggested first centroid policy:

- Only activate if opening price is at least `0.35%` away from centroid.
- Wait for a reversal condition toward centroid, not just any touch.
- Enter toward centroid after the first bar that rejects the extension.
- Exit at centroid or at close.

This should be backtested separately from breakout/rejection trades.

### 3. Respect prediction timing

Every trade must start after the prediction was made, not at the open unless the prediction was pre-open.

Rules:

- If `madeAt <= 09:30 ET`, session opens normally at `09:30 ET`
- If `madeAt > 09:30 ET`, the first eligible bar is the first 1-minute bar after `madeAt`
- If Alma says `into PM`, `afternoon`, or similar, restrict activation to that session window
- Weekly carry rows should remain active day by day until invalidated or the week ends

This is especially important for chat and intraday posts.

### 4. Separate direct-trade rows from context rows

Not every accurate Alma prediction should become a buy/sell trade.

For example:

- "tape is defensive"
- "headline risk remains high"
- "larger intraday whipsaw"
- "late afternoon rebound attempt"

These should be kept, but scored differently:

- as a regime filter
- as a volatility expectation
- or as a trade modifier

Example:

- if a row says `defensive tape`, reduce breakout-long size
- if a row says `late afternoon rebound attempt`, only allow bounce entries after `13:00 ET`

That gives us two layers:

1. `trade trigger rows`
2. `context modifier rows`

### 5. Backtest in passes, not all at once

Recommended order:

1. `breakout_long` and `breakout_short`
2. `rejection_short` and `rejection_long`
3. `support_bounce_long` and `resistance_fade_short`
4. `centroid_mean_reversion`
5. `optiondepth_heatmap`
6. Add commentary-based context filters on top

This keeps the early results understandable.

## POC findings

The first March POC already tells us something useful:

| Family | Raw signals | Consolidated | Triggered trades | Read |
| --- | ---: | ---: | ---: | --- |
| breakout_long | 10 | 10 | 3 | Cleanest family |
| breakout_short | 2 | 1 | 0 | Too few samples so far |
| rejection_short | 7 | 5 | 3 | Promising with better confirmation |
| support_bounce_long | 12 | 5 | 0 | Needs better reclaim rule |
| centroid_mean_reversion | 10 | 10 | 0 | Needs separate policy |

The important point is not the raw PnL.
The important point is that breakout and rejection rows already produce deterministic trades without much interpretive stretch.

## Recommended final backtest framework

For each normalized trade intent:

1. Determine whether the row is `trade_trigger`, `context_modifier`, or `non_tradeable`.
2. Fetch 1-minute bars for the target session.
3. Build a family-specific state machine:
   - waiting
   - triggered
   - entered
   - stopped
   - targeted
   - timed_out
4. Simulate the trade from `madeAt` onward.
5. Record:
   - `entry_ts`
   - `entry_price`
   - `exit_ts`
   - `exit_price`
   - `exit_reason`
   - `max_favorable_excursion`
   - `max_adverse_excursion`
   - `pnl_points`
   - `pnl_r`
6. Aggregate by:
   - family
   - basis (`commentary`, `script_levels`, `optiondepth_heatmap`)
   - source type (`post`, `chat`)
   - prediction timing (`pre_open`, `intraday`, `weekly_carry`)

## Where LLMs help

LLMs are useful in one narrow place:

- mapping Alma language into the normalized trade schema

LLMs should not be used for:

- fetching price data
- deciding if a trigger happened
- deciding PnL
- deciding if a stop or target was hit

Recommended hybrid rule:

- deterministic parser first
- LLM only when a row is likely tradeable but under-specified
- save the LLM output alongside a confidence score and explanation
- require human review for low-confidence mappings

## Next POCs to run

1. Improve rejection entries by requiring zone failure confirmation.
2. Rebuild support-bounce entries with reclaim logic instead of first touch.
3. Add a centroid-only POC with distance-from-centroid activation.
4. Add a lightweight regime-filter experiment using commentary rows such as `defensive tape` or `late afternoon rebound attempt`.
5. Keep the execution engine the same and later swap Yahoo `^GSPC` bars for ClickHouse SPX minute bars.

## Bottom line

The right design is not "one backtest for all Alma rows."
The right design is:

1. capture every prediction
2. normalize only the tradeable ones into family-specific trade intents
3. simulate each family with its own entry and exit logic
4. use commentary and chat as context modifiers on top

That gives us a path to answer the real question: when Alma implies "buy SPX" or "sell SPX," was there a trade with a deterministic entry, a deterministic exit, and positive expectancy.
