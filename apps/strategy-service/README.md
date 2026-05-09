# PhenixFlow Strategy Service

Generic strategy API service for daily portfolio strategies. It currently serves
the base PYM V5 study and the PYM option-rank top-8 study as separate strategy
APIs under one registry.

## Run Locally

```bash
npm run strategy-service:start
```

Default URL:
`http://localhost:3120`

## Run With Docker

```bash
docker compose -f docker-compose.strategy-service.yml up --build
```

The compose file mounts the existing PYM runtime and artifacts folders into the
container. The ignored `.env.local` file is used for Massive credentials when a
strategy refresh endpoint needs to download updated adjusted EOD bars.

The default local container is capped at `0.50` CPU, `512m` RAM, `768m` swap,
and `128` pids. That is enough for the current in-memory EOD and option-feature
reports while leaving room for lightweight refresh jobs.

## Strategy APIs

List strategies:

```http
GET /api/strategies
```

Strategy metadata and summary:

```http
GET /api/strategies/pym-v5
GET /api/strategies/pym-v5-option-rank-top8
```

Chart data for a range:

```http
GET /api/strategies/pym-v5/chart?start=2026-01-01&end=2026-05-07
GET /api/strategies/pym-v5-option-rank-top8/chart?start=2025-01-01&end=2026-05-07
```

Daily values for a range:

```http
GET /api/strategies/pym-v5/values?start=2025-01-01&end=2026-05-07
```

Latest portfolio and change from previous EOD target:

```http
GET /api/strategies/pym-v5/portfolio/latest
GET /api/strategies/pym-v5-option-rank-top8/portfolio/latest
GET /api/strategies/pym-v5/changes/latest
```

Portfolio for a specific date:

```http
GET /api/strategies/pym-v5/portfolio/2026-05-07
GET /api/strategies/pym-v5-option-rank-top8/portfolio/2026-05-07
```

## Current Studies

- `pym-v5`: replicated Composer PYM V5 tree on Massive adjusted EOD bars.
- `pym-v5-option-rank-top8`: PYM V5 holdings filtered/ranked by Massive option
  aggregate flow using option overlay strategy `grid_pym_option_rank_top8_zm0p5`.
  It uses EOD PYM targets and day-X option-flow features, then realizes
  close-to-close returns into X+1.

Recompute in memory from mounted runtime data:

```http
POST /api/strategies/pym-v5/recompute
```

Refresh Massive adjusted EOD data, then recompute:

```http
POST /api/strategies/pym-v5/refresh-data
```

## Adding Strategies

Implement an adapter with:

- `getMetadata()`
- `getReport()`
- `recompute()`
- optional `refreshData()`

Then register it in `apps/strategy-service/src/default-registry.js`.
