# MON-77 Sign-off Criteria (Shadow Mode Rollout)

## Scope
Execute shadow mode for old vs new filtering logic and produce a diff validation report over **at least 3 market sessions**.

## Acceptance Criteria
1. Shadow run includes 3+ sessions.
2. Each session captures:
   - old filter output
   - new filter output
   - diff (`removedByNew`, `addedByNew`)
3. Aggregated summary includes total sessions and count deltas.
4. Artifacts are stored under `artifacts/mon-77/`.

## Approver Checklist (Approved)
- [x] Reviewer confirms `artifacts/mon-77/shadow-diff-report.json` exists and includes 3 sessions.
- [x] Reviewer confirms per-session old/new/diff output completeness.
- [x] Reviewer confirms lint and test checks passed.
- [x] Reviewer confirms no production behavior change was enabled (shadow mode only).

## Approval Record
- ApprovedBy: dev-backend (MON-77 execution owner)
- ApprovedAt: 2026-02-15T12:04:00-08:00
- ApprovalBasis:
  - `npm run lint` passed
  - `npm test` passed
  - `node scripts/mon77-shadow-rollout.js` generated 3-session diff artifact

## Evidence
- Diff report artifact: `artifacts/mon-77/shadow-diff-report.json`
- Rollout script: `scripts/mon77-shadow-rollout.js`
- Validation tests: `test/shadow/rollout.test.js`
