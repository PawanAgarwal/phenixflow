# phenixflow

Primary backend repository for Project phoenixflow.

## Scaffold

This repository currently includes a minimal Express-based service scaffold with:
- `GET /health` endpoint
- `GET /api/flow` endpoint with cursor pagination, sorting, and filters
- unit tests with Vitest + Supertest
- linting with ESLint

## API

### `GET /api/flow`

Fetch flow records with cursor pagination, sorting, and full filter support.

Query parameters:
- Pagination
  - `limit` (default `25`, min `1`, max `100`)
  - `cursor` (opaque next-page token)
- Sorting
  - `sortBy`: `id|symbol|strategy|status|timeframe|pnl|volume|createdAt|updatedAt`
  - `sortOrder`: `asc|desc` (defaults to `desc`)
- Filters
  - Exact match: `id`, `symbol`, `strategy`, `status`, `timeframe`
  - Ranges: `minPnl`, `maxPnl`, `minVolume`, `maxVolume`
  - Time range: `from`, `to` (applies to `createdAt`)
  - Search: `search` (case-insensitive, across core flow fields)

Response shape:
- `data`: array of flow rows
- `page`: pagination envelope
  - `limit`
  - `hasMore`
  - `nextCursor`
  - `sortBy`
  - `sortOrder`
  - `total`

Examples:

```bash
# First page
curl "http://localhost:3000/api/flow?limit=3"

# Next page
curl "http://localhost:3000/api/flow?limit=3&cursor=<opaque_cursor>"

# Filtered and sorted
curl "http://localhost:3000/api/flow?symbol=AAPL&status=open&minPnl=100&sortBy=pnl&sortOrder=desc"
```

## Autonomous Agent Workflow

- Queue of record: `PM_QUEUE.md`
- Coordination contract: `AGENT_PROTOCOL.md`
- OpenClaw role agents claim/execute/handoff tasks using git-backed queue updates.

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

Contract coverage is currently verified in `test/app.test.js` for:
- `GET /health` status/body contract
- `GET /api/flow` query contract, response schema, pagination, and regression behavior

### Lint

```bash
npm run lint
```
