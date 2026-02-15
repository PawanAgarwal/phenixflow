# PM_QUEUE.md

_Last updated: 2026-02-15T20:00:54Z_

## Queue Rules
- Claim tasks only using `AGENT_PROTOCOL.md` locking flow.
- One task can have only one owner in **In Progress**.
- Every state change must be committed and pushed.

## Ready

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


- ID: PF-016
  Title: Add repository contract docs and example in src/data/README.md
  Role: dev-data
  Priority: P2
  Owner: unassigned
  DependsOn: PF-011
  Acceptance:
  - Document repository interfaces, expected return/error contract, and extension pattern
  - Include one concrete example repository stub aligned with contract
  - Lint/test continue to pass


- ID: PF-017
  Title: Add seed data fixture strategy for future integration tests
  Role: dev-data
  Priority: P2
  Owner: unassigned
  DependsOn: PF-011
  Acceptance:
  - Add fixture directory structure and loader helper
  - Provide sample fixture and usage docs for tests
  - Add at least one unit test for fixture loader


- ID: PF-018
  Title: Add QA checklist template for Review -> Done gate
  Role: qa
  Priority: P1
  Owner: unassigned
  DependsOn: none
  Acceptance:
  - Add reusable QA checklist template covering lint, tests, acceptance criteria, and docs
  - Document defect routing rules (Ready vs Blocked)
  - Link checklist from README or CONTRIBUTING


- ID: PF-019
  Title: Add Makefile shortcuts for common local workflows
  Role: dev-infra
  Priority: P3
  Owner: unassigned
  DependsOn: PF-001
  Acceptance:
  - Add `make lint`, `make test`, `make verify`, and `make run`
  - Targets call existing npm scripts without duplicating logic
  - README includes quickstart with Make targets


- ID: PF-020
  Title: Add CODEOWNERS and branch protection guidance docs
  Role: pm
  Priority: P2
  Owner: unassigned
  DependsOn: PF-003
  Acceptance:
  - Add `.github/CODEOWNERS` draft aligned to current roles
  - Document recommended branch protection settings in docs
  - Include note marking settings as repository-admin applied


- ID: PF-022
  Title: Add npm script for mutation-safe test reruns (`npm run test:watch:ci`)
  Role: dev-infra
  Priority: P3
  Owner: unassigned
  DependsOn: PF-001
  Acceptance:
  - Add a deterministic watch/changed-file CI-safe test rerun script
  - Document when to use `test` vs `test:watch:ci`
  - Lint/test remain green


- ID: PF-023
  Title: Add repository error taxonomy doc and typed error helpers
  Role: dev-data
  Priority: P2
  Owner: unassigned
  DependsOn: PF-001
  Acceptance:
  - Add typed data-layer errors (e.g., NotFound, Conflict, Validation)
  - Document mapping from data-layer errors to app-layer behavior
  - Add unit tests for error helper creation/shape


- ID: PF-024
  Title: Build QA triage playbook for flaky test detection and routing
  Role: qa
  Priority: P2
  Owner: unassigned
  DependsOn: none
  Acceptance:
  - Add a QA playbook section for identifying flaky failures vs product defects
  - Define retry limits and escalation paths
  - Link playbook from CONTRIBUTING or README

## In Progress

- ID: PF-007
  Title: Add request-id middleware with response header propagation
  Role: dev-backend
  Priority: P1
  Owner: dev-backend
  StartedAt: 2026-02-15T12:03:37Z
  Branch: agent/dev-backend/PF-007-request-id-middleware-response-header-propagation
  DependsOn: PF-001
  Acceptance:
  - Each request has an id generated or propagated from `x-request-id`
  - Response always returns `x-request-id` header
  - Tests validate generation + propagation behavior


- ID: PF-003
  Title: Define coding standards and contribution guide
  Role: pm
  Priority: P1
  Owner: pm
  StartedAt: 2026-02-15T07:18:00Z
  Branch: agent/pm/PF-003-coding-standards-and-contribution-guide
  DependsOn: none
  Acceptance:
  - `CONTRIBUTING.md` defines branch naming, claim flow, and handoff expectations aligned with `AGENT_PROTOCOL.md`
  - `CODING_STANDARDS.md` documents lint/test requirements and commit hygiene
  - Includes a short “CI notes” section marked provisional until PF-002 is merged to main


- ID: PF-008
  Title: Add npm script for CI parity and document local verification flow
  Role: dev-infra
  Priority: P1
  Owner: dev-infra
  StartedAt: 2026-02-15T07:20:00Z
  Branch: agent/dev-infra/PF-008-npm-verify-script-docs
  DependsOn: PF-001
  Acceptance:
  - Add `npm run verify` script that runs lint + test in CI-like order
  - README documents “before push” command sequence
  - Commands run cleanly on main branch


- ID: PF-011
  Title: Add data layer bootstrap for app-layer dependencies
  Role: dev-data
  Priority: P2
  Owner: dev-data
  StartedAt: 2026-02-15T07:22:00Z
  Branch: agent/dev-data/PF-011-data-layer-bootstrap
  DependsOn: PF-001
  Acceptance:
  - Add `src/data/` module boundary with typed placeholder repository/contracts for future data entities
  - Export a minimal repository factory and error wrapper used by future handlers
  - Add/extend tests showing the module shape and expected return contract


- ID: PF-012
  Title: Conduct sprint-0 retrospective and define process improvements
  Role: retro
  Priority: P2
  Owner: retro
  StartedAt: 2026-02-15T07:23:00Z
  Branch: agent/retro/PF-012-sprint-retro-process
  DependsOn: none
  Acceptance:
  - Identify at least 3 process blockers from first sprint run and proposed mitigations
  - Produce actionable retro action items with owners and target dates
  - Update `AGENTS.md` communication/runtime notes as needed

## Review

- (none)

## Blocked

- (none)

## Done

- ID: MON-68
  Title: E8-W7 Add API contract tests for all new endpoints
  Role: dev-backend
  Priority: P1
  Owner: dev-backend
  StartedAt: 2026-02-15T19:57:33Z
  CompletedAt: 2026-02-15T20:00:54Z
  QAValidatedBy: qa
  QAValidatedAt: 2026-02-15T20:00:54Z
  Branch: pawanagarwal/mon-68-e8-w7-add-api-contract-tests-for-all-new-endpoints
  DependsOn: none
  Acceptance:
  - Contract tests cover status codes and response schemas.
  - Pagination/facets/stream/detail endpoints included.
  - Backward-compat paths are validated.
  Evidence:
  - Dev session: agent:dev-backend:subagent:16b3357b-f876-4b3d-a3f4-46f2e957855e
  - QA session: agent:qa:subagent:65d8d6c0-77a8-47a7-bd75-9adba243045f (pass, recommendation=merge)
  - PR: https://github.com/PawanAgarwal/phenixflow/pull/4
  - Merge commit on main: 0707782b0b4f3fdb0e14b1ee5971ca5cde725cd6
  - Validation: `npm run lint` pass, `npm test` pass (7/7)
  - Slack message ids: C0AEN968ZM5/1771185472.636189, 1771185564.332419, 1771185570.522119; C0AEN943UVD/1771185472.902889, 1771185564.594049, 1771185570.780039

- ID: PF-009
  Title: Add Dockerfile and .dockerignore for local containerized runs
  Role: dev-infra
  Priority: P2
  Owner: dev-infra
  CompletedAt: 2026-02-15T14:57:00Z
  QAValidatedBy: qa
  QAValidatedAt: 2026-02-15T14:57:00Z
  DependsOn: PF-001
  Acceptance:
  - Multi-stage (or minimal) Dockerfile builds runnable app image
  - `.dockerignore` excludes node_modules/test artifacts appropriately
  - README includes build + run commands
  Evidence:
  - Dockerfile uses multi-stage pattern (`deps` + runtime) on Node 20 Alpine with production dependency install (`npm ci --omit=dev`).
  - `.dockerignore` excludes `node_modules`, git metadata, coverage/test artifacts, and local noise files.
  - README documents container workflow with `docker build -t phenixflow:local .` and `docker run --rm -p 3000:3000 -e PORT=3000 phenixflow:local`.
  - QA validation: acceptance criteria checked and `npm run lint` + `npm test` rerun (pass). Docker CLI unavailable in this environment, so image build/run execution could not be performed here.


- ID: PF-021
  Title: Add /ready endpoint exposing service readiness metadata
  Role: dev-backend
  Priority: P2
  Owner: dev-backend
  CompletedAt: 2026-02-15T14:57:00Z
  QAValidatedBy: qa
  QAValidatedAt: 2026-02-15T14:57:00Z
  StartedAt: 2026-02-15T14:16:41Z
  Branch: agent/dev-backend/PF-021-ready-endpoint-exposing-service-readiness-metadata-20260215c
  DependsOn: PF-001
  Acceptance:
  - Add `GET /ready` endpoint returning readiness status and app version metadata
  - Endpoint returns non-200 only when required startup prerequisites are unavailable
  - Add tests for success and unavailable scenarios





  Evidence:
  - Added `GET /ready` endpoint in `src/app.js` returning readiness status with service/version metadata from `package.json`.
  - Added unavailable readiness path returning `503` with a structured reason when startup prerequisites are unavailable.
  - Extended `test/app.test.js` with `/ready` success and unavailable scenario coverage.
  - Updated `README.md` scaffold section to document `/ready` endpoint behavior.
  - Verified locally: `npm run lint` and `npm test`.
  - QA validation: checked branch acceptance criteria and reran `npm run lint` + `npm test` (pass).


- ID: PF-014
  Title: Add config module with typed env parsing and defaults
  Role: dev-backend
  Priority: P1
  Owner: dev-backend
  CompletedAt: 2026-02-15T14:57:00Z
  QAValidatedBy: qa
  QAValidatedAt: 2026-02-15T14:57:00Z
  StartedAt: 2026-02-15T07:51:16Z
  Branch: agent/dev-backend/PF-014-config-module-typed-env-parsing-defaults
  DependsOn: PF-001
  Acceptance:
  - Create centralized config loader for `PORT`, `NODE_ENV`, and app metadata
  - Invalid env values return clear startup errors
  - Tests validate defaulting and error paths
  Evidence:
  - Added centralized config loader in `src/config.js` parsing `PORT`, `NODE_ENV`, and app metadata with defaults.
  - Updated `src/server.js` to boot from `loadConfig()` and use validated config values for startup logging and port binding.
  - Added `test/config.test.js` coverage for defaulting, env overrides, invalid `PORT`, and invalid `NODE_ENV` error paths.
  - Verified locally: `npm run lint` and `npm test`.
  - QA validation: checked branch acceptance criteria and reran `npm run lint` + `npm test` (pass).


- ID: PF-015
  Title: Add structured logger utility with request context support
  Role: dev-backend
  Priority: P2
  Owner: dev-backend
  CompletedAt: 2026-02-15T14:57:00Z
  QAValidatedBy: qa
  QAValidatedAt: 2026-02-15T14:57:00Z
  StartedAt: 2026-02-15T09:22:11Z
  Branch: agent/dev-backend/PF-015-structured-logger-request-context-support-20260215
  DependsOn: PF-007
  Acceptance:
  - Logger utility supports level + JSON output
  - Request logs include request-id when present
  - Existing middleware updated to use shared logger
  Evidence:
  - Added shared structured logger utility in `src/logger.js` with level filtering, JSON output, and child context support.
  - Added `src/requestLogger.js` middleware using shared logger and including `requestId` when `x-request-id` is present.
  - Updated `src/app.js` to use request logging middleware for all routes.
  - Added tests in `test/logger.test.js` and extended `test/app.test.js` for request-id log context behavior.
  - Verified locally: `npm run lint` and `npm test`.
  - QA validation: checked branch acceptance criteria and reran `npm run lint` + `npm test` (pass).


- ID: PF-013
  Title: Add graceful shutdown handling for SIGINT/SIGTERM
  Role: dev-backend
  Priority: P1
  Owner: dev-backend
  CompletedAt: 2026-02-15T14:57:00Z
  QAValidatedBy: qa
  QAValidatedAt: 2026-02-15T14:57:00Z
  StartedAt: 2026-02-15T07:58:12Z
  Branch: agent/dev-backend/PF-013-graceful-shutdown-handling
  DependsOn: PF-001
  Acceptance:
  - Server handles SIGINT/SIGTERM and closes listener cleanly
  - Shutdown path logs start/finish and returns non-zero on shutdown error
  - Tests cover shutdown helper behavior
  Evidence:
  - Added graceful shutdown helpers in `src/gracefulShutdown.js` with start/finish logging and non-zero exit code on close errors.
  - Updated `src/server.js` to register SIGINT/SIGTERM handlers and close the listener cleanly before process exit.
  - Added `test/gracefulShutdown.test.js` coverage for success/error shutdown paths and signal registration behavior.
  - Verified locally: `npm run lint` and `npm test`.
  - QA validation: checked branch acceptance criteria and reran `npm run lint` + `npm test` (pass).


- ID: PF-006
  Title: Add centralized error handler and 404 fallback
  Role: dev-backend
  Priority: P1
  Owner: dev-backend
  CompletedAt: 2026-02-15T14:57:00Z
  QAValidatedBy: qa
  QAValidatedAt: 2026-02-15T14:57:00Z
  StartedAt: 2026-02-15T07:16:08Z
  Branch: agent/dev-backend/PF-006-centralized-error-handler-404-fallback
  Evidence:
  - Added app-level 404 fallback and 500 error middleware with structured JSON payloads in `src/app.js`.
  - Added tests for unknown-route 404 and thrown-error 500 paths in `test/app.test.js`.
  - Verified locally: `npm run lint` and `npm test`.
  - QA validation: checked branch acceptance criteria and reran `npm run lint` + `npm test` (pass).


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

