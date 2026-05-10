# `pym-v5-ml-option-top8-50-50`

## Purpose

`pym-v5-ml-option-top8-50-50` tracks a static blend between the best current
two-speed ML PYM overlay and the option-flow top-8 PYM overlay.

It was added so the dashboard can monitor the blend going forward as a
first-class strategy.

## Plain-English Rule

At each EOD signal:

```text
ml_target = two_speed_attention_pym_light_governed target
option_target = grid_pym_option_rank_top8_zm0p5 target
target = 50% * ml_target + 50% * option_target
```

Ticker weights that appear in both targets are added together. The result is a
single normalized long-only target portfolio.

## Service Adapter

- Adapter: `apps/strategy-service/src/strategies/pym-v5-ml-artifact.js`
- Registry id: `pym-v5-ml-option-top8-50-50`
- Artifact strategy id: `blend_50_ml_50_option_top8`
- Family: `pym_ml_option_blend`

The service reads the latest local risk-overlay artifact matching:

```text
projects/pym-v5-ml-experiments/artifacts/pym-v5-two-speed-risk-overlays-*.json
```

Environment override:

- `PYM_V5_ML_RISK_OVERLAY_REPORT_PATH`

## Inputs

This strategy is artifact-backed. The expected risk-overlay artifact already
contains the blended daily holdings and realized returns.

The artifact is derived from:

- ML target: `two_speed_attention_pym_light_governed`
- option target: `grid_pym_option_rank_top8_zm0p5`
- Massive-derived next-day returns
- configured cost assumption, currently 2 bps in the research artifact

## Timing

```text
day X close: read ML and option targets available through X
day X close: blend 50% / 50% and expose target
day X+1 close: realize close-to-close return from the artifact
```

## API Surface

- `GET /api/strategies/pym-v5-ml-option-top8-50-50`
- `GET /api/strategies/pym-v5-ml-option-top8-50-50/chart`
- `GET /api/strategies/pym-v5-ml-option-top8-50-50/values`
- `GET /api/strategies/pym-v5-ml-option-top8-50-50/portfolio/latest`
- `GET /api/strategies/pym-v5-ml-option-top8-50-50/portfolio/:date`
- `POST /api/strategies/pym-v5-ml-option-top8-50-50/recompute`

## Caveats

- The service consumes a generated artifact. It does not currently retrain ML or
  rebuild the option overlay inside the Docker service.
- The blend weight, 50% / 50%, is a research choice, not a learned allocation.
- If the risk-overlay artifact is not updated after a new market day, the
  dashboard remains on the latest artifact date.
- The underlying ML and option top-8 strategies both need walk-forward hardening.
