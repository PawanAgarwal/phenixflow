# AGENT_PROTOCOL.md

## Purpose
Defines how PM, dev, QA, and retro agents coordinate autonomously without stepping on each other.

## Source of Truth
1. `PM_QUEUE.md` in this repository is the execution queue of record.
2. `CURRENT_CONTEXT.md` in each OpenClaw workspace defines active project assignment.
3. Git history is the audit log for claims, handoffs, and completion.

## Roles
- PM: owns queue quality, prioritization, and unblocking.
- Dev agents: claim and execute implementation tasks by area.
- QA: validates review-ready tasks and gates completion.
- Retro: proposes process improvements.

## Locking and Claiming (No Collisions)
Before starting any task:
1. `git pull --rebase origin main`
2. Select the highest-priority matching task from `PM_QUEUE.md` with `Owner: unassigned`.
3. Move task to **In Progress**, set:
- `Owner: <agent-id>`
- `StartedAt: <ISO timestamp>`
- `Branch: agent/<agent-id>/<task-id>-<slug>`
4. Commit queue change and push.
5. If push is rejected, re-pull/rebase and retry claim. Never assume claim until push succeeds.

Only one owner may hold a task in **In Progress**.

## Role-Match Rule
- `dev-backend` may claim only tasks with `Role: dev-backend`.
- `dev-data` may claim only tasks with `Role: dev-data`.
- `dev-infra` may claim only tasks with `Role: dev-infra`.
- `qa` pulls only from **Review**.
- Exceptions must be explicitly assigned by PM by setting `Owner` before claim.

## Execution Flow
- Dev claims task -> implements on task branch -> updates queue status.
- On completion, dev moves task to **Review** with evidence.
- QA validates:
- pass -> move to **Done**
- fail -> move to **Blocked** or back to **Ready** with defect notes

## Blocked Handling
When blocked:
- Move task to **Blocked** with `Blocker`, `Owner`, and `NextAction`.
- PM watchdog must create unblock action and re-sequence work.

## Heartbeat Expectations
- PM watchdog every 3 minutes.
- Dev/QA loops every 7 minutes.
- Queue must include enough **Ready** items to keep agents busy.

## Communication Rule
Primary communication is via updates in `PM_QUEUE.md` and git commits.
Direct inter-session messaging is optional and not required for correctness.
