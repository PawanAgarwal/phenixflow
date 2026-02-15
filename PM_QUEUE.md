# PM_QUEUE.md

_Last updated: 2026-02-14T22:37:00-08:00_

## Queue Rules
- Claim tasks only using `AGENT_PROTOCOL.md` locking flow.
- One task can have only one owner in **In Progress**.
- Every state change must be committed and pushed.

## Ready

- ID: PF-002
  Title: Add CI workflow for lint + test
  Role: dev-infra
  Priority: P0
  Owner: unassigned
  DependsOn: PF-001
  Acceptance:
  - GitHub Actions workflow runs lint + tests on push/PR
  - Workflow fails on lint/test errors
  - README includes CI status badge

- ID: PF-004
  Title: Add healthcheck endpoint with test coverage
  Role: dev-backend
  Priority: P1
  Owner: unassigned
  DependsOn: PF-001
  Acceptance:
  - Add `/healthz` endpoint returning 200 with service status payload
  - Add test coverage for success response
  - Document endpoint in README

## In Progress

- (none)

## Review

- (none)

## Blocked

- ID: PF-003
  Title: Define coding standards and contribution guide
  Role: pm
  Priority: P1
  Owner: pm
  Blocker: Depends on PF-002 CI conventions and badge details.
  NextAction: Move back to Ready immediately after PF-002 reaches Done.

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
