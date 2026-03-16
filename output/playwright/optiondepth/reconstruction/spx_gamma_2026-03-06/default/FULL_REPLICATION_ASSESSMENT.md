# Full OptionDepth Gamma Chart Replication Assessment

## What OptionDepth Is Actually Plotting

- `Gamma / (Δ / 2.5 pts)` is the heatmap field.
  - The colorbar on the left is the gamma scale.
  - On this captured day the visible scale is clipped to about `[-1600, 1600]`.
- The right-side axis is `SPX price`.
- `OHLC` is overlaid on that same right-side price axis.
- `Gamma Peak`, `Gamma Trough`, and `Gamma Zero` are not single lines.
  - On `2026-03-06`, OptionDepth plotted:
    - `16` peak segments
    - `16` trough segments
    - `5` zero-gamma segments

## What I Reproduced

- A full chart replica with the same structural components:
  - gamma heatmap
  - peak overlays
  - trough overlays
  - zero-gamma overlays
  - OHLC overlay on the SPX price axis
- Final artifact:
  - `gamma_complete_replica.html`
  - `gamma_complete_replica_screenshot.png`

## How Close The Heatmap Regions Are

- Heatmap sign agreement: `0.8863`
- Exact clipped color-band agreement: `0.7497`
- Positive-region IoU: `0.7038`
- Negative-region IoU: `0.8442`
- Near-neutral-region IoU (`|gamma| <= 320`): `0.8630`

Read:
- The broad positive/negative/neutral regions are fairly close.
- The negative and neutral regions are the best match.
- So the ThetaData reconstruction does recover a lot of the same large-scale map.

## How Close The Overlay Layers Are

### Gamma Peak

- OptionDepth: `16` segments
- ThetaData replica: `2` segments
- OD -> replica MAE at shared columns: `57.92` SPX points
- Replica -> OD MAE at shared columns: `124.32` SPX points

### Gamma Trough

- OptionDepth: `16` segments
- ThetaData replica: `1` segment
- OD -> replica MAE at shared columns: `109.80` SPX points
- Replica -> OD MAE at shared columns: `79.82` SPX points

### Gamma Zero

- OptionDepth: `5` segments
- ThetaData replica: `1` segment
- OD -> replica MAE at shared columns: `116.38` SPX points
- Replica -> OD MAE at shared columns: `20.48` SPX points

Read:
- Our replica gets the main zero-gamma branch into roughly the right area.
- It does not reproduce OptionDepth's multiple branching zero-gamma structure.
- The peak/trough overlays are the weakest part of the replication.

## What Cannot Be Reliably Replicated From ThetaData Alone

- The exact multi-branch `Gamma Peak` topology.
- The exact multi-branch `Gamma Trough` topology.
- The exact multi-branch `Gamma Zero` topology.
- The internal logic OptionDepth uses to decide which extrema branches are important enough to draw.
- Any proprietary dealer-inventory netting or flow-adjusted positioning model behind those overlays.

## Why Those Pieces Likely Differ

- ThetaData gives us public greeks and public open interest.
- It does not give us OptionDepth's internal estimate of dealer inventory and sign.
- The heatmap can be approximated from public inputs.
- The overlay branches appear to depend on a richer internal surface than a plain public `OI + greeks` reconstruction.

## Bottom Line

- Yes, we can reproduce the full chart structure.
- Yes, we can get a meaningfully similar heatmap region map.
- No, we cannot completely replicate OptionDepth's overlay lines from ThetaData alone.
- The best statement I’m comfortable making is:
  - `The broad gamma regions are reproducible.`
  - `The exact Peak/Trough/Zero branch geometry is not.`
