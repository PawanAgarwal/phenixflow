# PM_QUEUE.md

_Last updated: 2026-02-15T07:10:30Z_

## Queue Rules
- Claim tasks only using `AGENT_PROTOCOL.md` locking flow.
- One task can have only one owner in **In Progress**.
- Every state change must be committed and pushed.

## Ready

- ID: PF-003
  Title: Define coding standards and contribution guide
  Role: pm
  Priority: P1
  Owner: unassigned
  DependsOn: none
  Acceptance:
  - `CONTRIBUTING.md` defines branch naming, claim flow, and handoff expectations aligned with `AGENT_PROTOCOL.md`
  - `CODING_STANDARDS.md` documents lint/test requirements and commit hygiene
  - Includes a short “CI notes” section marked provisional until PF-002 is merged to main

- ID: PF-006
  Title: Add centralized error handler and 404 fallback
  Role: dev-backend
  Priority: P1
  Owner: unassigned
  DependsOn: PF-001
  Acceptance:
  - Unmatched routes return structured JSON 404 response
  - App-level error middleware returns structured JSON 500 response without leaking stack traces
  - Tests cover both 404 and 500 paths

- ID: PF-007
  Title: Add request-id middleware with response header propagation
  Role: dev-backend
  Priority: P2
  Owner: unassigned
  DependsOn: PF-001
  Acceptance:
  - Each request has an id generated or propagated from `x-request-id`
  - Response always returns `x-request-id` header
  - Tests validate generation + propagation behavior

- ID: PF-008
  Title: Add npm script for CI parity and document local verification flow
  Role: dev-infra
  Priority: P1
  Owner: unassigned
  DependsOn: PF-001
  Acceptance:
  - Add `npm run verify` script that runs lint + test in CI-like order
  - README documents “before push” command sequence
  - Commands run cleanly on main branch

- ID: PF-009
  Title: Add Dockerfile and .dockerignore for local containerized runs
  Role: dev-infra
  Priority: P2
  Owner: unassigned
  DependsOn: PF-001
  Acceptance:
  - Multi-stage (or minimal) Dockerfile builds runnable app image
  - `.dockerignore` excludes node_modules/test artifacts appropriately
  - README includes build + run commands

- ID: PF-010
  Title: Add API smoke test script for /health endpoint
  Role: dev-infra
  Priority: P2
  Owner: unassigned
  DependsOn: PF-001
  Acceptance:
  - Add script (shell or node) to hit `/health` and assert 200 with expected payload shape
  - Script is runnable via npm script (`npm run smoke`)
  - README includes usage notes

## In Progress

- (none)


## Review

- (none)

## Blocked

- (none)

## Done

- ID: PF-001
  Title: Scaffold baseline app + test harness
  Role: dev-backend
  Owner: dev-backend
  CompletedAt: 2026-02-14T22:26:00-08:00
  Evidence:
  - Added Express scaffold (`src/app.js`, `src/server.js`)
  - Added unit tests (`test/app.test.js`)
  - Added lint config (`eslint.config.js`)
  - Updated README and npm scripts
  - Verified: `npm test` and `npm run lint` pass

- ID: PF-002
  Title: Add CI workflow for lint + test
  Role: dev-infra
  Owner: dev-infra
  CompletedAt: 2026-02-15T07:05:08Z
  QAValidatedBy: qa
  QAValidatedAt: 2026-02-15T07:10:30Z
  Evidence:
  - Validation on `origin/agent/dev-infra/PF-002-ci-workflow-lint-test` confirmed `.github/workflows/ci.yml` runs lint + test on push/PR with Node 20 and `npm ci`.
  - README includes CI status badge linked to Actions workflow.
  - QA reran `npm run lint` and `npm test` in isolated worktree; both passed.

- ID: PF-004
  Title: Add healthcheck endpoint with test coverage
  Role: dev-backend
  Owner: dev-backend
  CompletedAt: 2026-02-15T06:58:12Z
  QAValidatedBy: qa
  QAValidatedAt: 2026-02-15T07:10:30Z
  Evidence:
  - Validation on `origin/agent/dev-backend/PF-004-healthcheck-endpoint` confirmed `/healthz` endpoint response payload and README documentation.
  - Test coverage includes `GET /healthz` success path.
  - QA reran `npm run lint` and `npm test` in isolated worktree; both passed.

- ID: PF-005
  Title: Add request logging middleware + test coverage
  Role: dev-backend
  Owner: dev-backend
  CompletedAt: 2026-02-15T07:07:31Z
  QAValidatedBy: qa
  QAValidatedAt: 2026-02-15T07:10:30Z
  Evidence:
  - Validation on `origin/agent/dev-backend/PF-005-request-logging-middleware` confirmed request logger middleware logs method + path and is wired in app bootstrap.
  - Test coverage asserts logging middleware execution on a route.
  - QA reran `npm run lint` and `npm test` in isolated worktree; both passed.
