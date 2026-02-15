# phenixflow

Primary backend repository for Project phoenixflow.

## Scaffold

This repository includes:
- `GET /health` endpoint
- `GET /api/flow` endpoint with cursor pagination, sorting, and full filter support
- unit tests with Vitest + Supertest
- linting with ESLint

## API

### `GET /api/flow`

Returns flow records with query-based filtering and cursor pagination.

#### Query params

- Pagination
  - `limit` (integer, default `25`, max `100`)
  - `cursor` (opaque cursor from previous page; tied to sort + filters)
- Sorting
  - `sortBy`: `id|symbol|strategy|status|timeframe|pnl|volume|createdAt|updatedAt` (default `createdAt`)
  - `sortOrder`: `asc|desc` (default `desc`)
- Filters
  - exact: `id`, `symbol`, `strategy`, `status`, `timeframe`
  - range/date: `minPnl`, `maxPnl`, `minVolume`, `maxVolume`, `createdFrom`, `createdTo`, `updatedFrom`, `updatedTo`
  - aliases: `from`→`createdFrom`, `to`→`createdTo`, `pnlMin`→`minPnl`, `pnlMax`→`maxPnl`, `volumeMin`→`minVolume`, `volumeMax`→`maxVolume`
  - text: `search` (case-insensitive contains across id/symbol/strategy/status/timeframe)
  - `quickFilters`: comma-separated list of `openOnly|closedOnly|winners|losers|highVolume|recentlyUpdated`

Response rows are enriched with computed fields (`isProfitable`, `pnlDirection`, epoch timestamps, and `ageHours`) and include a top-level `meta` object describing applied filters.

Validation errors return `400` with `{ "error": "..." }`.

## Getting Started

```bash
npm install
npm start
npm test
npm run lint
```
