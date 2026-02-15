# PM_QUEUE.md

_Last updated: 2026-02-15T07:06:43Z_

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

- ID: PF-005
  Title: Add request logging middleware + test coverage
  Role: dev-backend
  Priority: P1
  Owner: dev-backend
  StartedAt: 2026-02-15T07:06:43Z
  Branch: agent/dev-backend/PF-005-request-logging-middleware
  DependsOn: PF-001
  Acceptance:
  - Middleware logs method + path for incoming requests
  - Logging is enabled in app bootstrap without breaking tests
  - Test coverage validates middleware execution on at least one route


## Review

- ID: PF-002
  Title: Add CI workflow for lint + test
  Role: dev-infra
  Priority: P0
  Owner: dev-infra
  StartedAt: 2026-02-15T07:04:22Z
  CompletedAt: 2026-02-15T07:05:08Z
  Branch: agent/dev-infra/PF-002-ci-workflow-lint-test
  Evidence:
  - Added GitHub Actions workflow `.github/workflows/ci.yml` running `npm run lint` and `npm test` on `push` to `main` and on all `pull_request` events.
  - Workflow uses `actions/setup-node@v4` with Node 20 and `npm` cache; uses `npm ci` for reproducible installs.
  - Added CI status badge to `README.md` linked to `actions/workflows/ci.yml`.
  - Verified locally: `npm run lint` passed and `npm test` passed.
  - Ops/rollback: no runtime service path changes; rollback is safe by reverting workflow + README badge commit.

- ID: PF-004
  Title: Add healthcheck endpoint with test coverage
  Role: dev-backend
  Priority: P1
  Owner: dev-backend
  StartedAt: 2026-02-15T06:57:26Z
  CompletedAt: 2026-02-15T06:58:12Z
  Branch: agent/dev-backend/PF-004-healthcheck-endpoint
  Evidence:
  - Added `/healthz` endpoint in `src/app.js` returning `{"status":"ok","service":"phenixflow"}`
  - Added test coverage in `test/app.test.js` for `GET /healthz` success response
  - Documented endpoint in `README.md`
  - Verified: `npm test` (2 tests passed), `npm run lint` passed

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
