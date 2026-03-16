# TailThatWagsDog Prototype Canary

Trade date: `2026-03-13`
Spot close: `6632.25`

## Benchmark

- Public app spot: `6632.19`
- Public app regime: `AMPLIFYING`
- Public app zero-gamma: `6916.00`
- Public app 1-day `P(above 6600)`: `94.9%`
- Public app 1-day `P(below 6650)`: `74.5%`
- Public app 1-week `P(above 6600)`: `73.2%`
- Public app 1-week `P(below 6650)`: `58.4%`

## Basket Comparison

### `mar_apr_may`

- Expiries: `2026-03-20, 2026-04-17, 2026-05-15`
- Expiries used: `2026-03-20, 2026-04-17, 2026-05-15`
- Contracts used: `1580`
- Public `GEX` per `1%` move: `-38571.66M`
- Public `GEX` per point: `-581.58M`
- Public `VEX` hedge per vol point: `5100.95M`
- `VGR`: `0.13x`
- Regime: `AMPLIFYING`
- Zero-gamma: `6858.46`
- Zero-gamma pct above spot: `3.41%`
- Zero-gamma abs diff vs app: `57.54`
- Regime matches app: `True`
- 1-day `P(above 6600)`: `66.1%`
- 1-day `P(below 6650)`: `68.0%`
- 1-week `P(above 6600)`: `51.2%`
- 1-week `P(below 6650)`: `64.0%`

### `apr_may_jun`

- Expiries: `2026-04-17, 2026-05-15, 2026-06-19`
- Expiries used: `2026-04-17, 2026-05-15`
- Contracts used: `1123`
- Public `GEX` per `1%` move: `-9409.12M`
- Public `GEX` per point: `-141.87M`
- Public `VEX` hedge per vol point: `3773.31M`
- `VGR`: `0.40x`
- Regime: `AMPLIFYING`
- Zero-gamma: `6971.65`
- Zero-gamma pct above spot: `5.12%`
- Zero-gamma abs diff vs app: `55.65`
- Regime matches app: `True`
- 1-day `P(above 6600)`: `66.1%`
- 1-day `P(below 6650)`: `68.0%`
- 1-week `P(above 6600)`: `51.2%`
- 1-week `P(below 6650)`: `64.0%`

## Best Public Match

- Best basket by zero-gamma + regime fit: `apr_may_jun`
- Best basket regime: `AMPLIFYING`
- Best basket zero-gamma: `6971.65`
- Best basket stress HTML: `stress_surface_best.html`

## Intraday If/Then Read

- Opening state: `below_zero_gamma`
- If spot drops and IV rises, next effect: `selloff_with_iv_expansion_should_self_reinforce`
- If spot rallies and IV compresses, next effect: `rally_with_iv_compression_should_be_more_two_sided_than_breakout_like`
- `spot -2% / IV +2` stress: `-25922.98M`
- `spot -5% / IV +5` stress: `-57165.41M`
- `spot +2% / IV -2` stress: `23615.86M`

Interpretation:
- With spot at `6632.25` and zero-gamma at `6971.65`, the public model says the market is `amplifying` and `below zero gamma`.
- If the session shows price weakening together with IV expansion, treat that as the highest-conviction continuation pattern in this framework.
- If price rallies but IV does not compress, the rally is less trustworthy than the raw directional move alone suggests.
- If price reclaims the zero-gamma region and the stress surface turns less negative, the next effect should be volatility compression and more two-sided trade.

## Forecast Baseline

- April BL mean: `6549.05`
- April BL std: `349.71`
- April BL skewness: `-0.7926`
- April BL kurtosis: `3.83`

## Caveats

- This is a public-data prototype, not a reconstruction of the account's proprietary directional-index logic.
- We are using current snapshot OI on `2026-03-15`, which should still reflect the `2026-03-13` close because the market has not reopened yet.
- We are not using limit-order-book or venue-specific participant/open-close data yet.
- The probability layer is a rough BL + Edgeworth/Cornish-Fisher overlay, not a fully calibrated realized-vol model.
