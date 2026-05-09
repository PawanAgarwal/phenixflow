# PhenixFlow Studies Dashboard

Small Docker-friendly UI for browsing study results served by the PhenixFlow
strategy service. It discovers studies from `/api/strategies` and renders each
one as a dashboard tab.

## Run Locally

```bash
STRATEGY_API_URL=http://localhost:3120 npm run studies-dashboard:start
```

Default URL:
`http://localhost:3130`

## Run With Docker

```bash
docker compose -f docker-compose.studies-dashboard.yml up --build -d
```

The dashboard container proxies `/api/strategies/*` to `STRATEGY_API_URL`.
For the local Docker setup, the compose file points at the already-running
strategy service through `http://host.docker.internal:3120`.

## Views

- Study performance versus SPY.
- Tabs for every strategy API registered in `phenixflow-strategy-service`.
- Latest target portfolio with weight changes versus the prior target.
- Last week of day-over-day net returns, turnover, and largest target moves.
