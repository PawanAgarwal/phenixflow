# `pym-v5`

## Purpose

`pym-v5` is the local Massive-backed replication of Composer symphony
`XPGix2infTwwWMORgqmV`, "Eagle's Park your Money V5".

It is the base strategy that most other PYM studies build on.

## Plain-English Rule

At each market close:

1. Load the public Composer strategy tree.
2. Evaluate that tree using Massive adjusted daily bars.
3. Produce the ETF target weights for the day.
4. Hold that target through the next close-to-close session.

The local code evaluates the Composer rule tree directly. It does not manually
copy the full multi-thousand-node tree into a simplified hand-written strategy.

## Service Adapter

- Adapter: `apps/strategy-service/src/strategies/pym-v5.js`
- Registry id: `pym-v5`
- Family: `composer`
- Dashboard display name: `Eagle's Park your Money V5`

The adapter loads:

- Composer score/tree snapshot from `projects/pym-v5-replication/runtime/source/`
- Massive adjusted EOD bars from `projects/pym-v5-replication/runtime/`
- Execution settings from `projects/pym-v5-replication/config/`

Main research and backtest code:

- `projects/pym-v5-replication/src/rebalance-report.js`
- `projects/pym-v5-replication/src/symphony.js`
- `projects/pym-v5-replication/src/backtest.js`

## Inputs

- Massive adjusted daily bars, built from Massive REST aggregates with
  `adjusted=true`.
- Public Composer tree JSON from the Composer public symphony API.
- Configured transaction and slippage cost assumptions.

## Timing

```text
day X close: evaluate Composer tree and create target weights
day X close: rebalance target is recorded
day X+1 close: realized close-to-close return is recorded
```

This is the causal timing used by the service. Diagnostic same-close modes may
exist in research scripts, but they are not the live service convention.

## API Surface

- `GET /api/strategies/pym-v5`
- `GET /api/strategies/pym-v5/chart`
- `GET /api/strategies/pym-v5/values`
- `GET /api/strategies/pym-v5/portfolio/latest`
- `GET /api/strategies/pym-v5/portfolio/:date`
- `POST /api/strategies/pym-v5/recompute`
- `POST /api/strategies/pym-v5/refresh-data`

## Operational Notes

The strategy service can refresh Massive adjusted EOD bars by spawning:

- `projects/pym-v5-replication/scripts/build-massive-eod-daily-bars.js`

The refresh endpoint requires Massive credentials in the environment.

## Caveats

- Composer's exact production data vendor and edge-case semantics can differ
  from the local replication.
- Long moving-window indicators need warmup history. Early-window backtests are
  sensitive to how much pre-window data is available.
- This is a replication of a public strategy tree, not the original Composer
  engine.
