# AGENT_PROTOCOL.md

## Purpose
Coordinate a PM-led team where PM dispatches all work through subagents from Linear with PM_QUEUE.md as the executable handoff ledger.

## Source of Truth
1. `CURRENT_CONTEXT.md` in each workspace defines active project and execution workspace.
2. Linear is the source and single source of required work (`team MON`, `project phoenixflow`).
3. `PM_QUEUE.md` in the repo is the execution ledger and evidence log.
4. Slack is the operations audit for actions taken by PM and workers.
5. Git history is the immutable audit trail for claims, handoffs, and completion.

## State Model
Use these states consistently:
- `Backlog`/`Todo` = available to start
- `In Progress` = actively owned by a worker
- `In Review` = implemented and waiting for QA
- `Blocked` = waiting on dependency
- `Done` = complete and accepted

When any ambiguity appears, PM owns resolution.

## PM Orchestration Model
1. PM runs on cron and is the only process that picks fresh work.
2. PM queries Linear and PM_QUEUE, then dispatches work only to subagents listed in openclaw subagent allowlist (`pm.subagents.allowAgents`).
3. PM keeps at most `subagents.maxConcurrent` in-flight tasks at any time.
4. PM balances with completion events in the same cycle: when a worker finishes, PM updates state and may immediately dispatch the next task.
5. PM owns all state transitions: `Backlog`/`In Progress`/`In Review`/`Blocked`/`Done`.

## Dispatch and Handoff Rules (PM -> Subagent)
- PM must only dispatch from tasks that exist in Linear and are reflected in PM_QUEUE.
- Every dispatch message must include:
  - Linear ID
  - PM_QUEUE identifier
  - `Role`, priority, and acceptance criteria
  - Expected deliverables + timeout budget
  - Branch naming convention
- On dispatch, PM must post to Slack and set PM_QUEUE owner+state+started timestamp.
- PM should never leave a dispatch unrepresented in PM_QUEUE.

## Subagent Execution Rules
- Subagents execute only tasks sent by PM; they do not scan Linear directly.
- Subagents may process one task at a time.
- Subagents should push a completion report with:
  - status (`done`, `blocked`, `needs-replan`)
  - key outcomes / test results
  - commit hashes and changed files
  - evidence links where applicable
- On finishing, PM updates Linear and PM_QUEUE based on that report.

## Locking Rules
- Only one owner may hold a task in `In Progress`.
- If no owner is assigned or owner is stale, PM can reassign.
- Claiming is valid only after PM_QUEUE and Linear state update are aligned.
- If `git push` fails, rebase and retry.
- If a subagent check-in is missing due contention/lock, PM posts in Slack and retries.

## Timeout and Recovery
- Each dispatched subagent task has a hard max runtime (`SUBAGENT_TASK_TIMEOUT_MINUTES`, default 90).
- On timeout:
  - PM marks task as `Blocked` with `Owner: pm-requeue`.
  - PM terminates the stuck subagent session.
  - PM posts Slack notification with reason + evidence.
  - PM immediately reassigns the task to another available subagent and updates PM_QUEUE + Linear.

## Role Match Rules
- `dev-backend` works only `Role: dev-backend`.
- `dev-data` works only `Role: dev-data`.
- `dev-infra` works only `Role: dev-infra`.
- `qa` validates only `In Review` work from PM_QUEUE/Linear.
- `pm` and `retro` can touch any role only for orchestration/improvements.

## Escalation & Error Handling
- If blocked more than 10 minutes (non-timeout), include `Blocker`, `Owner`, and `NextAction`, move to `Blocked`, and post in blocker channel.
- For timeout recovery, include the exact timeout event, kill action, and reassignment details.
- PM should reassess and resequence backlog immediately on blockers.

## Idle and PM Status Rule
If PM_queue has no runnable work after a full scan:
- post one slack message for that cycle in orchestration channel:
  - `🟡 Idle | <agent-name>`
  - `No matching open work found for role`
  - `Checked Linear + PM_QUEUE in this cycle`
  - `Will retry on next scheduled loop`
- Do not create duplicate idle posts in the same cycle.

## Worker Reporting to PM
- Workers do not update backlog selection; they only report completion/blocked outcomes.
- PM is responsible for creating follow-on tasks in Linear when decomposition is needed.

## Heartbeat / Loop Cadence
- PM watchdog: every 3 minutes. If PM already running, do not create a duplicate PM run.
- PM runs are the sole source of task assignment.
- Retro cadence remains PM/ sprint-window driven.
- Subagents are event-driven under PM dispatch and are not cron-dispatched.
