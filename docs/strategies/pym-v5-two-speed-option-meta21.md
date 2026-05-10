# `pym-v5-two-speed-option-meta21`

## Purpose

`pym-v5-two-speed-option-meta21` is a selector strategy that chooses between
two underlying families:

- `two_speed_attention_pym_light_governed`
- `grid_pym_option_rank_top8_zm0p5`

It does not blend them by fixed weights. Instead, it uses recent realized
strategy performance to decide which candidate selector to trust.

## Plain-English Rule

The service first builds candidate "best of ML or option top-8" selectors using
multiple lookback windows:

```text
lookbacks = [5, 10, 15, 21, 30, 42, 50, 63, 84, 126]
```

For each lookback, the candidate selector asks:

```text
which underlying strategy had the better prior lookback score?
hold that underlying strategy's target today
```

Then the meta selector asks:

```text
over the prior 21 realized trading days, which lookback selector performed best?
hold today's target from that selected lookback selector
```

## Service Adapter

- Adapter: `apps/strategy-service/src/strategies/pym-v5-ml-artifact.js`
- Registry id: `pym-v5-two-speed-option-meta21`
- Artifact strategy id exposed in metadata:
  `walkforward_lookback_best_of_two_speed_or_option_meta21`
- Family: `pym_selector_research`

Unlike `pym-v5-ml-option-top8-50-50`, this strategy is not loaded as one final
overlay series. The service reconstructs the selector from two artifact inputs:

- ML report for `two_speed_attention_pym_light_governed`
- option overlay report for `grid_pym_option_rank_top8_zm0p5`

It intersects their realized dates, rebuilds candidate selector series, and then
builds the 21-day meta selector inside the adapter.

Environment overrides:

- `PYM_V5_ML_REPORT_PATH`
- `PYM_V5_ML_OPTION_REPORT_PATH`
- `PYM_V5_ML_DATASET_PATH`

## Inputs

- ML walk-forward artifact under `projects/pym-v5-ml-experiments/artifacts/`
- option overlay artifact under `projects/pym-v5-replication/artifacts/`
- walk-forward dataset for benchmark SPY/QQQ returns

## Timing

```text
day X close: all selector choices use only prior realized strategy returns
day X close: selected target is exposed
day X+1 close: realized close-to-close return is recorded
```

The meta window is fixed at 21 realized trading days in the service adapter.

## API Surface

- `GET /api/strategies/pym-v5-two-speed-option-meta21`
- `GET /api/strategies/pym-v5-two-speed-option-meta21/chart`
- `GET /api/strategies/pym-v5-two-speed-option-meta21/values`
- `GET /api/strategies/pym-v5-two-speed-option-meta21/portfolio/latest`
- `GET /api/strategies/pym-v5-two-speed-option-meta21/portfolio/:date`
- `POST /api/strategies/pym-v5-two-speed-option-meta21/recompute`

## Caveats

- The selector family was research-discovered and needs clean nested
  out-of-sample validation.
- The 21-day meta window is a design choice and should not be retuned using
  future performance.
- The selector can churn between underlying strategies, so turnover and cost
  modeling matter.
- The service result is only as current as the ML and option artifacts it reads.
