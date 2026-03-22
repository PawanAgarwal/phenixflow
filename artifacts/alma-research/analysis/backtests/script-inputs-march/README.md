# Script Inputs March POC

This POC tests the raw SPX `SCRIPT INPUTS` ladder only, without Alma commentary.

## Interpretation Used

- The March SPX ladders behave like a symmetric standardized move grid around the prior close.
- Empirically, the rows line up very closely with approximately `1 sigma`, `1.35 sigma` (`risk`), `2 sigma`, `3 sigma`, and `4 sigma` bands.
- That makes the raw block look more like a probability / expected-move ladder than a direct directional signal.
- So the first fair test is not "buy or sell immediately," but whether the ladder predicts containment, exhaustion, or band-to-band continuation.

## March Sample

- Script-input days tested: 9
- Date range: 2026-03-02 to 2026-03-13
- Avg estimated sigma from the 68.2 bands: 37.59 SPX points

## Containment

| Metric | Count | Rate |
| --- | ---: | ---: |
| Close inside +/-1 sigma | 4 | 44.4% |
| Close inside +/-2 sigma | 7 | 77.8% |
| Close inside +/-3 sigma | 9 | 100.0% |
| Full session range inside +/-1 sigma | 1 | 11.1% |
| Full session range inside +/-2 sigma | 4 | 44.4% |
| Full session range inside +/-3 sigma | 8 | 88.9% |
| Full session range inside +/-4 sigma | 9 | 100.0% |

## Touch Outcomes

The table below asks whether touching a band led to reversion toward the inner rung first, or continuation to the next outer rung first.

| Event | Total | Revert first | Continue first | Neither by close | Not touched |
| --- | ---: | ---: | ---: | ---: | ---: |
| Upper risk -> upper1 vs upper2 | 9 | 2 | 0 | 1 | 6 |
| Lower risk -> lower1 vs lower2 | 9 | 1 | 4 | 0 | 4 |
| Upper 2 sigma -> upper1 vs upper3 | 9 | 0 | 0 | 0 | 9 |
| Lower 2 sigma -> lower1 vs lower3 | 9 | 2 | 1 | 2 | 4 |
| Upper 3 sigma -> upper2 vs upper4 | 9 | 0 | 0 | 0 | 9 |
| Lower 3 sigma -> lower2 vs lower4 | 9 | 1 | 0 | 0 | 8 |

## Initial Read

- The raw script ladder looks usable as a regime and level framework, even without commentary.
- Its first likely use is as a band model: expected body, stress zone, and tail zone.
- The most practical trade tests are band-touch reactions and confirmed band-break continuations.
- The ladder alone still does not tell us direction. Direction likely has to come from price path, speed commentary, or another filter.

## Recommended Script-Only Backtest Design

1. Treat the raw ladder as the state space for the day, not the full signal.
2. Test mean reversion at `risk` and `2 sigma` touches.
3. Test continuation only after a confirmed break of `2 sigma`, using the next outer rung as target.
4. Treat `3 sigma` and `4 sigma` as exhaustion / tail zones and test fade setups separately.
5. Record path features too: first band touched, deepest band reached, and final close band.

## Example: 2026-03-13

- Close: 6672.62
- +/-1 sigma: 6631.04 to 6713.95
- Risk levels: 6616.74 to 6728.50
- +/-2 sigma: 6589.71 to 6755.53
- +/-3 sigma: 6548.13 to 6796.86
- +/-4 sigma: 6506.80 to 6838.44

That is the cleanest way to backtest the raw script input itself before mixing in Alma commentary.

## Daily Rows

| Date | Close | Sigma | Session high | Session low | Session close | Deepest upper band touched | Deepest lower band touched |
| --- | ---: | ---: | ---: | ---: | ---: | --- | --- |
| 2026-03-02 | 6878.88 | 31.91 | 6901.01 | 6796.85 | 6881.59 | none | lower2 |
| 2026-03-04 | 6816.63 | 36.80 | 6885.94 | 6811.64 | 6869.48 | upperRisk | none |
| 2026-03-05 | 6869.50 | 33.86 | 6870.43 | 6770.78 | 6830.53 | none | lower2 |
| 2026-03-06 | 6830.71 | 35.46 | 6773.42 | 6711.56 | 6739.96 | none | lower3 |
| 2026-03-09 | 6740.02 | 43.81 | 6810.44 | 6636.04 | 6795.90 | upperRisk | lower2 |
| 2026-03-10 | 6795.99 | 37.78 | 6845.08 | 6759.74 | 6781.52 | upper1 | none |
| 2026-03-11 | 6781.48 | 39.11 | 6811.15 | 6745.59 | 6775.78 | none | none |
| 2026-03-12 | 6775.80 | 38.17 | 6740.88 | 6670.40 | 6672.58 | none | lower2 |
| 2026-03-13 | 6672.62 | 41.45 | 6733.30 | 6623.92 | 6632.25 | upperRisk | lower1 |
