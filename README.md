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

Vitest is split into two test packages/configs:
- `api`: endpoint/contract tests (`test/api/**`) via `npm run test:api`
- `core`: domain + harness tests (`test/core/**`, `test/shadow/**`) via `npm run test:core`

### CI / headless test run

```bash
npm run test:ci
```

`test:ci` runs Vitest in non-watch/headless mode with a compact reporter for CI logs.

### Lint

```bash
npm run lint
```
