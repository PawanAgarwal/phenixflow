# Flow API

Primary options-flow backend app for Project phenixflow.

This app owns the live API, ingestion worker, historical enrichment pipeline, and most flow-specific research/backfill scripts.

## Location

`apps/flow-api`

## Run From Repo Root

```bash
npm start
npm test
npm run ingest:start
```

## App Structure

- `app.js`, `server.js`, `flow.js`, `historical-flow.js`: API/runtime entry points.
- `config/`: flow-app config files and ThetaData templates.
- `scripts/`: app-owned backfill, scoring, and operational scripts.
- `test/`: Vitest coverage for the flow runtime.

## Shared Infrastructure

- Shared ThetaData HTTP helpers live in `packages/theta-client`.
- Shared ClickHouse helpers live in `packages/clickhouse-core`.
- ClickHouse machine/config/bootstrap scripts live in `infra/clickhouse`.

## Getting Started

### Prerequisites
- Node.js 20+

### Install

```bash
npm install
```

### Run

```bash
npm start
```

Default port: `3000` (override with `PORT`).

### Test

```bash
npm test
```

### Lint

```bash
npm run lint
```
