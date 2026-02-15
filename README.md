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

## MON-79 ThetaData smoke workflow

Run:

```bash
THETADATA_BASE_URL="https://<thetadata-host>" \
THETADATA_DOWNLOAD_PATH="/v2/<real-download-endpoint>?<query>" \
THETADATA_ENTITLEMENT_PATH="/v2/system/entitlements" \
THETADATA_API_KEY="<token>" \
npm run mon79:thetadata:smoke
```

Notes:
- `THETADATA_DOWNLOAD_PATH` is required and must point to a **real** downloadable dataset endpoint.
- Auth options: either `THETADATA_API_KEY` or `THETADATA_USERNAME` + `THETADATA_PASSWORD`.
- Artifacts are written under `artifacts/mon-79/` with a report JSON containing:
  - endpoint statuses/attempt counts
  - retry/backoff policy and timeout taxonomy
  - artifact path, byte count, sha256, and row count (when JSON shape allows)

