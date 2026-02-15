# phenixflow

Primary backend repository for Project phoenixflow.

## Scaffold

This repository currently includes a minimal Express-based service scaffold with:
- `GET /health` endpoint
- unit tests with Vitest + Supertest
- linting with ESLint

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

### Lint

```bash
npm run lint
```

## Query Result Cache (MON-56)

`GET /api/flow` and `GET /api/flow/facets` now share an in-memory cache keyed by a deterministic normalized query hash.

- Normalization is based on effective filter inputs + `filterVersion` only.
- Hashing uses SHA-256 over normalized JSON.
- Cache values are event-id sets (matching `flow.id` values).
- Cache metrics can be emitted via the optional `emitMetric(name, payload)` function in service-layer options:
  - `cache-miss`
  - `cache-hit`

