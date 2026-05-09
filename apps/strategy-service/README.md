# PhenixFlow Strategy Service

Generic strategy API service for daily portfolio strategies. PYM V5 is the first
registered strategy, and additional strategies can be added as adapters in
`apps/strategy-service/src/strategies`.

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

The default local container is capped at `0.50` CPU, `384m` RAM, `512m` swap,
and `128` pids. That is enough for the current in-memory daily EOD reports while
leaving room for lightweight refresh jobs.

## Strategy APIs

List strategies:

```http
GET /api/strategies
```

Strategy metadata and summary:

```http
GET /api/strategies/pym-v5
```

Chart data for a range:

```http
GET /api/strategies/pym-v5/chart?start=2026-01-01&end=2026-05-07
```

Daily values for a range:

```http
GET /api/strategies/pym-v5/values?start=2025-01-01&end=2026-05-07
```

Latest portfolio and change from previous EOD target:

```http
GET /api/strategies/pym-v5/portfolio/latest
GET /api/strategies/pym-v5/changes/latest
```

Portfolio for a specific date:

```http
GET /api/strategies/pym-v5/portfolio/2026-05-07
```

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
