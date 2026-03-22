# March 2026 Alma Backtest POC

Backtest window: 2026-03-01 to 2026-03-13 using Yahoo Finance `^GSPC` 1-minute bars.

## Why This Is A POC

- It only uses SPX-tagged March rows whose `expected` text can be converted into deterministic triggers.
- It intentionally skips broad macro, cross-asset, and ambiguous commentary.
- Entry/exit rules are simple placeholders designed to reveal which signal families look testable at all.

## Signal Funnel

- March rows reviewed: 231
- March SPX rows reviewed: 112
- Parseable raw signals: 41
- Consolidated signals: 31

| Family | Raw signals | Consolidated | Triggered trades | Win rate | Avg pnl (pts) | Total pnl (pts) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| breakout_long | 10 | 10 | 3 | 100.0% | 26.18 | 78.54 |
| breakout_short | 2 | 1 | 0 | 0.0% | 0.00 | 0.00 |
| centroid_mean_reversion | 10 | 10 | 0 | 0.0% | 0.00 | 0.00 |
| rejection_short | 7 | 5 | 3 | 66.7% | 6.96 | 20.87 |
| support_bounce_long | 12 | 5 | 0 | 0.0% | 0.00 | 0.00 |

## Initial Read

- Breakout-style rows are the cleanest first target because they already contain trigger + direction + target.
- Support and rejection rows are testable, but target/invalidation still depend on a trade-construction choice rather than Alma explicitly stating both levels every time.
- Centroid rows are promising, but they need a separate mean-reversion policy before they can be judged fairly.

## Recommended Backtest Design

1. Convert each prediction into a trade schema before simulation.
2. Separate tradeable price-action rows from non-price rows.
3. Backtest families independently before combining them.
4. Use same-day intraday exits first, then add multi-day versions later.

Proposed schema fields:
- `family`: breakout, support_reversion, rejection_reversion, centroid_mean_reversion, vol_regime, macro
- `direction`: long, short, both, none
- `trigger_type`: cross_above, cross_below, first_touch_zone, recapture_centroid, none
- `trigger_levels`: one or more numeric levels
- `target_type`: fixed_level, centroid, 1R, close, multi_day_window
- `target_levels`: one or more numeric levels
- `stop_type`: fixed_level, opposite_zone_edge, 0.5R_buffer, time_stop
- `tradeable_confidence`: high, medium, low

## What To Build Next

- Use an LLM classification pass to map each March row into that schema, but only after a deterministic pre-filter keeps the obviously tradeable rows.
- Start the real backtest with only `breakout_long`, `breakout_short`, `support_bounce_long`, and `rejection_short`.
- Keep centroid/heatmap strategies as a second module once we decide a fair entry rule.
- Swap Yahoo bars for ClickHouse SPX minute data later without changing the schema or trade simulator.

## Sample Trades

| Date | Family | Entry (ET) | Entry | Exit reason | Exit | Pnl pts | Source |
| --- | --- | --- | ---: | --- | ---: | ---: | --- |
| 2026-03-04 | rejection_short | 10:19 | 6856.91 | close | 6869.48 | -12.57 | "Nothing new in the Middle-East" | Intraday post (04/March) |
| 2026-03-09 | breakout_long | 15:21 | 6758.22 | close | 6795.90 | 37.68 | Forecasts are coming true | Weekly post (09-13/March) |
| 2026-03-10 | breakout_long | 09:30 | 6758.22 | close | 6781.52 | 23.30 | Forecasts are coming true | Weekly post (09-13/March) |
| 2026-03-10 | rejection_short | 13:27 | 6842.00 | target | 6830.00 | 12.00 | Intraday post (10/March) |
| 2026-03-11 | breakout_long | 09:30 | 6758.22 | close | 6775.78 | 17.56 | Forecasts are coming true | Weekly post (09-13/March) |
| 2026-03-11 | rejection_short | 10:00 | 6797.22 | close | 6775.78 | 21.44 | Intraday post (11/March) |

