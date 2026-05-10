# `pym-v5-spy-put-pressure-bil`

## Purpose

`pym-v5-spy-put-pressure-bil` is a defensive risk-off overlay on top of the base
PYM V5 target portfolio.

It holds PYM unless SPY option put pressure is unusually high. When the gate
trips, it parks the portfolio in `BIL`.

## Plain-English Rule

At each EOD signal:

```text
if SPY option put-pressure z-score >= 2.5:
    target = 100% BIL
else:
    target = normal PYM V5 portfolio
```

The tracked underlying option overlay id is:

- `grid_pym_spy_put_z2p5_to_bil`

## Service Adapter

- Adapter: `apps/strategy-service/src/strategies/pym-v5-option-rank.js`
- Registry id: `pym-v5-spy-put-pressure-bil`
- Family: `pym_option_risk_overlay`
- Base strategy: `pym-v5`

The adapter uses the same recompute path as the option-rank strategy but locks
the option overlay id to the SPY put-pressure gate.

Core option logic lives in:

- `projects/pym-v5-replication/src/option-overlay-suite.js`

## Inputs

- PYM V5 target weights for the signal date.
- Massive adjusted EOD bars for realized returns.
- Massive option aggregate features for SPY.

The put-pressure score uses rolling option-flow proxies, including:

- SPY put-call premium ratio z-score
- negative SPY premium imbalance z-score

The strategy takes the elevated put-pressure signal as a risk-off condition.

## Timing

```text
day X close: evaluate PYM and SPY option put-pressure using data through X
day X close: hold PYM target or switch to BIL
day X+1 close: realize close-to-close return
```

## API Surface

- `GET /api/strategies/pym-v5-spy-put-pressure-bil`
- `GET /api/strategies/pym-v5-spy-put-pressure-bil/chart`
- `GET /api/strategies/pym-v5-spy-put-pressure-bil/values`
- `GET /api/strategies/pym-v5-spy-put-pressure-bil/portfolio/latest`
- `GET /api/strategies/pym-v5-spy-put-pressure-bil/portfolio/:date`
- `POST /api/strategies/pym-v5-spy-put-pressure-bil/recompute`

## Caveats

- This overlay can miss rebounds because it moves fully to cash-like exposure
  when the pressure threshold is crossed.
- It was chosen from option-flow research and still needs strict walk-forward
  validation before being treated as stable production edge.
- It depends on timely option-flow feature generation.
