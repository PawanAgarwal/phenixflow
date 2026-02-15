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
