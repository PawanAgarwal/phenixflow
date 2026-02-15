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

## Approver Checklist (Ready for approval)
- [ ] Reviewer confirms `artifacts/mon-77/shadow-diff-report.json` exists and includes 3 sessions.
- [ ] Reviewer confirms per-session old/new/diff output completeness.
- [ ] Reviewer confirms lint and test checks passed.
- [ ] Reviewer confirms no production behavior change was enabled (shadow mode only).

## Evidence
- Diff report artifact: `artifacts/mon-77/shadow-diff-report.json`
- Rollout script: `scripts/mon77-shadow-rollout.js`
- Validation tests: `test/shadow/rollout.test.js`
