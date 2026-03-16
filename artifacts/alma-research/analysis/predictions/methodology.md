# Methodology

## Goal

This pass is focused on capturing Alma predictions cleanly before evaluating them.

## What changed

- One row now means one prediction, not one source item.
- The main daily ledgers are keyed by the **target date** of the prediction.
- Each row also records **when the prediction was made** in PT.
- Each row now also carries an instrument tag when the text supports it.
- Each row now also tries to capture the closest available underlying value at the prediction timestamp.
- Script-section rows are intentionally narrowed to SPX only; ES, NQ, VIX, and stock-specific script levels are excluded.
- Commentary extraction uses cached OpenAI structured extraction first. Chat extraction is stricter: Alma-authored messages are heuristic-filtered first, then only those candidate snippets are passed to the LLM, with the deterministic parser as fallback when LLM extraction returns nothing.
- Runtime chat mode is configurable with `ALMA_CHAT_MODE=disabled|on|chat-only`.
- Incremental rebuilds are configurable with `ALMA_PREDICTION_INCREMENTAL=1`; they reuse stored source hashes and rebuild only new or changed Alma sources plus the recent overlap window.
- A `chat-only` rebuild can optionally replace stale chat-derived rows in the main ledger with `ALMA_PREDICTION_MERGE_CHAT_ONLY_INTO_MAIN=1`.
- OptionsDepth heatmap images are also extracted and converted into separate `optiondepth_heatmap` prediction rows when a post includes that image block.
- Commentary-driven calls, level/script-driven calls, and heatmap-driven calls are separated with a `basis` field.
- `Expected result` is now a normalized action statement so we can backtest it more deterministically later.
- `Proxy actual value` and `Aligned?` are intentionally left blank for now.
- The weekly ledger now captures all week-duration predictions, not just weekly-post rows.

## Daily capture rules

- Weekly notes can generate many daily rows:
  - `weekly_day_specific` when Alma gives explicit day lines like `Tuesday:` or `Friday:`
  - `weekly_generic_carry` when a broader weekly comment is applied to every trading day of that week
- Daily posts generate one row per prediction sentence or clause that looks forecast-like.
- For commentary, the extractor asks the model for one row per distinct forward-looking claim, including pivot/centroid/reversion style commentary that is easy to miss with simple regex parsing.
- For chats, Alma-authored messages are reduced by heuristics first and only those candidate prediction snippets are sent to the model.
- When a post contains an OptionsDepth heatmap, the image plus nearby text are processed as a separate prediction source.
- Chats use Alma-authored messages only, and can generate:
  - `chat_same_day_prediction`
  - `chat_forward_prediction`

## Weekly capture rules

- Weekly rows are created from:
  - weekly posts with generic week-long calls
  - weekly posts with day-specific calls like `Thursday:`
  - non-weekly posts that clearly point to the coming week or a future weekday
  - chats where Alma makes a forward-looking weekly or weekday call
- Each weekly row records:
  - `targetWeek`
  - `targetScope` such as `whole_week` or `Thursday (2026-02-19)`
  - `madeAt`
  - `origin`
  - `basis`

## Basis rules

- `commentary`: broader narrative forecasts, sentiment calls, regime calls, and directional/volatility commentary
- `script_levels`: pivot/target/centroid/pin/confirmation style calls that are clearly tied to her level framework
- `optiondepth_heatmap`: image-derived support, pin, reversion, and rejection zones extracted from the heatmap plus nearby context
- Chat rows are kept even when they mention level-style calls, because they reflect Alma's direct chat commentary rather than the standalone script tables
- `instrument`: exact symbol when explicit in the text; family labels such as `SPX_or_ES` are resolved down to one market when the prediction levels fit one candidate better than the other
- `instrumentFamily`: broader grouping such as `SPX_complex` or `NDX_complex`
- `referenceSymbol`: the actual ThetaData or Yahoo Finance symbol used to fetch the nearest available price, such as `SPY`, `^GSPC`, or `ES=F`
- `referenceValue` / `referenceAt`: the closest available market value near the prediction-made timestamp
- `referenceQuality`: `exact` for direct symbol lookups and `family_inferred` when an ambiguous family tag was resolved using the prediction levels themselves
- `expected`: a heuristic action summary derived from the quote plus nearby paragraph context, intended to be easier to evaluate than the raw quote alone

## Current output size

- Daily prediction rows: 4665
- Weekly prediction rows: 457
- Daily heatmap rows: 772

## Next step

The next pass should focus on evaluation logic only after we review whether the captured predictions themselves look right.
