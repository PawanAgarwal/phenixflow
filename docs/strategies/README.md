# Strategy Documentation Index

This folder documents the daily strategies exposed by the PhenixFlow strategy
service and studies dashboard.

The goal is to leave enough context for a future human or AI agent to understand
what each strategy does, where the code lives, what data it consumes, how timing
works, and what still needs care before a strategy is treated as production
research.

## Live Strategy Registry

The live strategy list is created in:

- `apps/strategy-service/src/default-registry.js`

Each strategy adapter implements:

- `getMetadata()`
- `getReport()`
- `recompute()`
- optionally `refreshData()`

The Express API is in:

- `apps/strategy-service/src/app.js`

The studies dashboard is a thin UI/proxy over that API:

- `apps/studies-dashboard/src/app.js`
- `apps/studies-dashboard/public/app.js`

## Current Strategies

| Strategy id | Doc | Adapter | Primary source |
| --- | --- | --- | --- |
| `pym-v5` | [pym-v5.md](pym-v5.md) | `apps/strategy-service/src/strategies/pym-v5.js` | Composer public tree plus Massive adjusted EOD bars |
| `pym-v5-option-rank-top8` | [pym-v5-option-rank-top8.md](pym-v5-option-rank-top8.md) | `apps/strategy-service/src/strategies/pym-v5-option-rank.js` | PYM V5 plus Massive option-flow features |
| `pym-v5-spy-put-pressure-bil` | [pym-v5-spy-put-pressure-bil.md](pym-v5-spy-put-pressure-bil.md) | `apps/strategy-service/src/strategies/pym-v5-option-rank.js` | PYM V5 plus SPY option put-pressure gate |
| `pym-v5-ml-two-speed-attention` | [pym-v5-ml-two-speed-attention.md](pym-v5-ml-two-speed-attention.md) | `apps/strategy-service/src/strategies/pym-v5-ml-artifact.js` | ML walk-forward artifact |
| `pym-v5-ml-option-top8-50-50` | [pym-v5-ml-option-top8-50-50.md](pym-v5-ml-option-top8-50-50.md) | `apps/strategy-service/src/strategies/pym-v5-ml-artifact.js` | ML risk-overlay artifact |
| `pym-v5-two-speed-option-meta21` | [pym-v5-two-speed-option-meta21.md](pym-v5-two-speed-option-meta21.md) | `apps/strategy-service/src/strategies/pym-v5-ml-artifact.js` | ML and option artifact selector |

## Service Architecture

```text
Massive runtime data and research artifacts
          |
          v
strategy adapter getReport()/recompute()
          |
          v
StrategyRegistry
          |
          v
apps/strategy-service Express API on port 3120
          |
          v
apps/studies-dashboard proxy/UI on port 3130
```

The Docker setup for the strategy service is:

- `docker-compose.strategy-service.yml`
- `apps/strategy-service/Dockerfile`

The container mounts local runtime and artifact folders rather than committing
large market data or generated reports into git.

## Shared Timing Convention

Every strategy-service `getMetadata()` response includes an `execution` object
that is the source of truth for downstream runtime scheduling.

Daily EOD strategies use:

```json
{
  "timingClass": "EOD",
  "timezone": "America/New_York",
  "session": "REGULAR",
  "activation": {
    "type": "after_market_close",
    "time": "16:05"
  },
  "signalCadence": "daily_eod",
  "idempotencyKeyFields": ["strategyId", "signalDate"]
}
```

Their causal interpretation is:

```text
day X close: calculate signal and target weights using data available through X
day X close: rebalance target is considered an EOD close target
day X+1 close: realize close-to-close return for the held target
```

Intraday and scalp strategies use `timingClass` values `INTRADAY` or `SCALP`
with `activation.type = "regular_session_window"` and explicit ET
`startTime`/`endTime` values. They use
`signalCadence = "continuous_intraday"` and add `signalTimestamp` to
idempotency key fields.

API snapshots use the signal/rebalance date as `date`. The realized return, when
present, points to the next trading day in `realized.date` or `nextDate`.

## Data Policy

New market and strategy analysis in this repo uses Massive data only. The
canonical roots are described in the repo runbook and root README.

Runtime data and research artifacts are usually not committed. The docs describe
the contract expected by the service:

- Massive adjusted EOD bars under `projects/pym-v5-replication/runtime/`
- Massive option-flow features under `projects/pym-v5-replication/runtime/`
- ML and overlay artifacts under `projects/pym-v5-ml-experiments/artifacts/`

## When Adding A Strategy

1. Add or update a strategy adapter under `apps/strategy-service/src/strategies/`.
2. Register it in `apps/strategy-service/src/default-registry.js`.
3. Add a test that the strategy id appears in dashboard discovery.
4. Add a markdown file in this folder.
5. Link the new doc from this index and from `apps/strategy-service/README.md`.
6. Verify API and dashboard discovery before pushing.
