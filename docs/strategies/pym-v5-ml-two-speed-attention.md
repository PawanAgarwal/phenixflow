# `pym-v5-ml-two-speed-attention`

## Purpose

`pym-v5-ml-two-speed-attention` is an artifact-backed daily walk-forward ML
strategy. It uses the PYM V5 portfolio as the universe and teacher, then ranks
the currently active PYM candidates by predicted next-session return.

It is a PYM concentration/filter overlay, not a free-form optimizer.

## Plain-English Rule

At each EOD signal:

```text
candidate_set = current PYM V5 holdings, excluding cash-like holdings
scores = daily walk-forward two-speed model predictions
new_target = top 5 candidates by predicted return
weights = PYM relative weights renormalized across selected candidates

if predicted improvement is not large enough to justify turnover:
    keep prior target
else:
    hold new_target
```

## What "Two-Speed" Means

The research model trains two ridge-style return models each day using only
prior labeled samples:

- long-memory model trained on all prior days
- recent-memory model with exponential decay, using a 63-trading-day half-life

The final score blends those models. A stress score can increase the weight of
the recent-memory model, but this tracked version keeps the recent model's
influence relatively light.

## What "Attention" Means Here

This is not a full Transformer in the service. It is an analog-day feature
encoder over recent cross-asset returns.

For the current day, the model compares recent cross-asset return vectors with
prior days across equity, rates, gold, dollar, oil, volatility, sector, and
levered/inverse ETF proxies. Similar prior days contribute features such as:

- similar-day weighted return
- similar-day weighted absolute return
- analog concentration or match strength

In simple terms, it asks:

```text
When today's cross-asset tape looked like prior days, which active PYM legs
tended to work next?
```

## Service Adapter

- Adapter: `apps/strategy-service/src/strategies/pym-v5-ml-artifact.js`
- Registry id: `pym-v5-ml-two-speed-attention`
- Artifact strategy id: `two_speed_attention_pym_light_governed`
- Family: `pym_ml_research`

The service does not train this model live. It reads a generated ML report
artifact and converts that artifact into the common strategy API shape.

Default artifact contract:

- ML report under `projects/pym-v5-ml-experiments/artifacts/`
- Walk-forward dataset under `projects/pym-v5-ml-experiments/artifacts/`

Environment overrides:

- `PYM_V5_ML_REPORT_PATH`
- `PYM_V5_ML_DATASET_PATH`

## Inputs

The training artifact is built from Massive-backed daily data. Feature families
include:

- PYM current weights and tree outputs
- cross-asset price/return features
- analog-day attention features
- stress features that can include volatility and option-flow pressure

The service API consumes the completed artifact, not the raw training data.

## Timing

```text
day X close: artifact contains ML target built using only prior labeled days
day X close: rebalance target is exposed by the service
day X+1 close: realized close-to-close return is shown in the artifact/API
```

## API Surface

- `GET /api/strategies/pym-v5-ml-two-speed-attention`
- `GET /api/strategies/pym-v5-ml-two-speed-attention/chart`
- `GET /api/strategies/pym-v5-ml-two-speed-attention/values`
- `GET /api/strategies/pym-v5-ml-two-speed-attention/portfolio/latest`
- `GET /api/strategies/pym-v5-ml-two-speed-attention/portfolio/:date`
- `POST /api/strategies/pym-v5-ml-two-speed-attention/recompute`

## Caveats

- The exact coefficients change every day because the model is retrained
  walk-forward in the research pipeline.
- The live service is artifact-backed. If the artifact is stale, the dashboard
  target is stale.
- High turnover and levered/inverse ETF exposure make cost and slippage modeling
  important.
- This strategy should remain labeled research until the model selection process
  is hardened with clean nested out-of-sample validation.
