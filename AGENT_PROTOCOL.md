# AGENT_PROTOCOL.md

## Purpose
Coordinate a persistent PM/engineering/QA/retro team that claims and executes work continuously from Linear with PM_QUEUE.md as the executable handoff ledger.

## Source of Truth
1. `CURRENT_CONTEXT.md` in each workspace defines active project and execution workspace.
2. Linear is the discovery source for available work (`team MON`, `project phoenixflow`).
3. `PM_QUEUE.md` in the repo is the execution ledger and evidence log.
4. Git history is the immutable audit trail for claims, handoffs, and completion.

## State Model
Use these states consistently:
- `Backlog`/`Todo` = available to start
- `In Progress` = actively owned by a worker
- `In Review` = implemented and waiting for QA
- `Blocked` = waiting on dependency
- `Done` = complete and accepted

When any ambiguity appears, PM owns resolution.

## Agent Work Discovery and Claiming
All agents run a cycle in this order:
1. Read `CURRENT_CONTEXT.md`.
2. Query Linear for open tickets in `phoenixflow` and update PM_QUEUE quickly:
   - Use team project `phoenixflow`.
   - Confirm ticket ownership and state before claiming.
3. Claim only from entries in PM_QUEUE where:
   - `Role` matches the agent role,
   - `State` is `Backlog` (or equivalent ready state),
   - `Owner` is unassigned.
4. Before starting implementation:
   - `git pull --rebase origin main`
   - Set lock fields in PM_QUEUE (`Owner`, `StartedAt`, `Branch`).
5. Immediately move linear issue state to `progress`.
6. Implement and test.
7. On passing validation, move to `review` in Linear and PM_QUEUE.
8. QA resolves to `done` or `blocked` after validation.

If PM_QUEUE has no eligible entry for a relevant Linear issue, PM creates/normalizes it in PM_QUEUE with owner `unassigned` and state `Backlog`.

## Locking Rules
- Only one owner may hold a task in `In Progress`.
- Claiming is valid only after PM_QUEUE and Linear state update are committed together.
- If `git push` fails, rebase and retry.
- If an agent cannot claim within 2 minutes due contention or locks, post status in Slack and retry in the next loop.

## Role Match Rules
- `dev-backend` works only `Role: dev-backend`.
- `dev-data` works only `Role: dev-data`.
- `dev-infra` works only `Role: dev-infra`.
- `qa` validates only `In Review` work from PM_QUEUE/Linear.
- `pm` and `retro` can touch any role only for orchestration/improvements.

## Escalation & Error Handling
- If blocked more than 10 minutes, include `Blocker`, `Owner`, and `NextAction`, move to `Blocked`, and post in blocker channel.
- PM should reassess and resequence backlog immediately on blockers.

## Idle Rule
If a role has no eligible Linear/PM_QUEUE work after a full scan:
- post one slack message for that cycle in its channel:
  - `🟡 Idle | <agent-name>`
  - `No matching open work found for role`
  - `Checked Linear + PM_QUEUE in this cycle`
  - `Will retry on next scheduled loop`
- Do not create duplicate idle posts in the same cycle.

## Heartbeat / Loop Cadence
- PM watchdog: every 3 minutes.
- Dev/QA loops: every ~7 minutes.
- Retro/retroactive cadence remains PM driven by sprint windows.
- Agents are expected to continue working autonomously when loops fire.
