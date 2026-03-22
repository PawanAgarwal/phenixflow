# Candidate OptionDepth Strategy

This note summarizes the current best POC that came out of the Feb-March 2026 OD image backtest.

## Source Translation

- `gamma peaks` are treated as support or resistance walls, so we trade rejection or reclaim around them.
- `gamma troughs` are treated as travel lanes, so we use them to validate continuation after a zero-line break.
- `zero gamma` is treated as a regime boundary, not an automatic trade by itself.
- `late-day convergence` is treated as a pin candidate, but the current sample did not add enough good trades to keep it in the final subset.

## Exploratory Rules

- `peak_rejection`
  - Long when price touches a lower OD peak and reclaims it.
  - Short when price touches an upper OD peak and rejects it.
  - Target is the nearest trough or, if needed, the zero line / next peak in the move direction.

- `zero_to_peak_continuation`
  - Long after two closes above the OD zero line with room to the next OD peak.
  - Short after two closes below the OD zero line with room to the next OD peak.
  - Stop stays on the opposite side of the zero line.

## Candidate Tradable Subset

The broad exploratory set is still noisy. The current candidate subset is:

- calibration score `>= 11`
- entry time `>= 10:00 ET`
- first qualifying trade per day only

Why these filters:

- `calibration score >= 11`
  - The extracted OD map aligns much better with Alma's commentary/script anchors on these days.

- `entry >= 10:00 ET`
  - The opening half hour was the noisiest part of the sample and hurt both rejection and continuation trades.

- `first trade per day only`
  - This prevents overtrading both sides of the same OD map.

## Results On This Sample

- `candidate_v1`
  - `10` trades
  - `+140.97` SPX points
  - `60.0%` win rate
  - `4.61` profit factor
  - `20.04` max drawdown

- `candidate_v1_strict`
  - Same rules, but calibration score `>= 12`
  - `7` trades
  - `+142.48` SPX points
  - `71.4%` win rate
  - `8.76` profit factor
  - `10.85` max drawdown

## Important Caveat

These filters were discovered on the same Feb-March sample they were tested on. Treat them as in-sample POCs, not as production evidence.

## Next Validation Step

- run the same candidate rules out-of-sample on January 2026 and on the next newly archived OD days
- replace Yahoo minute data with ClickHouse / ThetaData when available
- add a regime flag from the heatmap background so we can separate positive-gamma fades from negative-gamma acceleration days
