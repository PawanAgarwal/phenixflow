# OptionDepth Gamma Legend Layer Comparison

## Legend Items Present

- `Gamma<br>(∆ / 2.5 pts)`
- `Gamma Peak`
- `Gamma Trough`
- `Gamma Zero`
- `OHLC`

## Region Match

- Sign agreement on the heatmap grid: `0.8863`
- Exact clipped color-band agreement: `0.7497`
- Positive-region IoU: `0.7038`
- Negative-region IoU: `0.8442`
- Near-neutral-region IoU (`|gamma| <= 320`): `0.8630`

## Overlay Layers

### Gamma Peak

- OptionDepth segments: `16`
- ThetaData proxy segments: `2`
- OptionDepth point count: `650`
- ThetaData proxy point count: `182`
- OD -> proxy MAE at shared times: `57.92`
- OD -> proxy p90 distance: `122.82`
- Proxy -> OD MAE at shared times: `124.32`
- Shared 5-minute columns: `91`
- OD-only columns: `0`
- Proxy-only columns: `0`

### Gamma Trough

- OptionDepth segments: `16`
- ThetaData proxy segments: `1`
- OptionDepth point count: `680`
- ThetaData proxy point count: `91`
- OD -> proxy MAE at shared times: `109.80`
- OD -> proxy p90 distance: `193.64`
- Proxy -> OD MAE at shared times: `79.82`
- Shared 5-minute columns: `91`
- OD-only columns: `0`
- Proxy-only columns: `0`

### Gamma Zero

- OptionDepth segments: `5`
- ThetaData proxy segments: `1`
- OptionDepth point count: `392`
- ThetaData proxy point count: `91`
- OD -> proxy MAE at shared times: `116.38`
- OD -> proxy p90 distance: `218.14`
- Proxy -> OD MAE at shared times: `20.48`
- Shared 5-minute columns: `91`
- OD-only columns: `0`
- Proxy-only columns: `0`

## Read

- `Gamma / (∆ / 2.5 pts)` is the full heatmap field; the overlay layers are separate contour-segment collections.
- On this day, OptionDepth plotted many more peak/trough/zero branches than a single-path summary would suggest.
- The region map can be directionally close while the individual legend overlays still differ a lot in topology and branching.
