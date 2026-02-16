# Sprint Goals (Active Chain)

_Last updated: 2026-02-16_

## Current Sprint Chain
- Sprint: **E8 — Validation and Hardening**
- Objective: complete deterministic validation coverage for the flow engine and API.

## Ordered Work Items
1. MON-62 (E8-W1) — test framework + isolated SQLite harness
2. MON-63 (E8-W2) — deterministic fixture packs for trades/quotes/OI/spot
3. MON-64 (E8-W3) — parser/schema-drift tests
4. MON-65 (E8-W4) — dedup/idempotency replay tests
5. MON-66 (E8-W5) — enrichment formula tests
6. MON-67 (E8-W6) — filter truth-table tests
7. MON-68 (E8-W7) — API contract tests
8. MON-69 (E8-W9) — performance suite
9. MON-70 (E8-W8) — cache coherence + hot/cold parity tests

## Dependency Rule
- Execute strictly in dependency order.
- Do not dispatch downstream items before upstream completion/unblock.

## Evidence Gate (per item)
- Implementation complete with PR.
- `npm run lint` and `npm test` (or item-specific test suite) pass.
- QA validation completed before moving to Done.
