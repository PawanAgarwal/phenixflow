# What Is Remaining To Be Done

## Latest Assessment (2026-03-01)
No major mission-critical backend gaps remain for the current sigScore + cache-reuse mission.

## Evidence From Latest Validation
1. Historical API checks passed for recent cached days across top symbols.
2. Minute-level rollups show full market-session coverage (`390` minute buckets/day for validated symbol-days).
3. sigScore minute aggregates are present and bounded (`[0,1]`) on validated days.
4. Cache reuse is active; historical reads commonly return `day_cache_full` + `metric_cache_full`.

## Mission Focus
Primary mission for this doc:
1. Keep `sigScore` useful for unusual-flow detection.
2. Keep score behavior reproducible and versioned.
3. Reuse downloaded data and caches aggressively.
4. Avoid false confidence from stale or incomplete inputs.

## What Is Already Good Enough For Mission
1. Historical sync/enrichment/cache pipeline is working and persisted in SQLite.
2. Expanded sigScore terms are implemented in runtime formulas.
3. Supplemental cache reuse for spot/OI/greeks is implemented.
4. Ingest reliability controls exist (retry/backoff/jitter, dead-letter, bounded buffering).
5. Rule activation tooling exists (`scripts/rules/activate-rule-version.js`).
6. Calibration script exists (`scripts/sigscore/calibrate-unusual.js`).

## Mission-Critical Items Completed

### R1. sigScore contract drift across seed/spec/runtime
- Status: Implemented
- What changed:
  - seeded explicit model-aware rule versions (`v1_baseline_default`, `v4_expanded_default`) in `scripts/db/sql/003_seed.sql`,
  - documented versioned `sigScore` contract in `docs/PHENIX_API_SPEC.md`.

### R2. Live `/api/flow` rule-version awareness
- Status: Implemented
- What changed:
  - live query/facets/summary/catalog now resolve runtime scoring config with active `ruleVersion`/`scoringModel`,
  - chip thresholds use resolved runtime config in `src/flow.js` instead of only env defaults.

### R3. Strict score-quality gating on live path
- Status: Implemented
- What changed:
  - strict mode now requires `scoreQuality=complete` for score-dependent chips,
  - missing quality is only tolerated in explicit fixture/testing compatibility mode.

### R4. Production fallback-to-fixtures removal
- Status: Implemented
- What changed:
  - `source=real-ingest` no longer silently falls back to fixtures on artifact errors,
  - production path no longer auto-falls back to fixtures when SQLite is empty/unavailable.

### R5. Calibration-to-promotion gate
- Status: Implemented
- What changed:
  - rule activation now enforces a calibration gate by default (report readability, minimum rows, minimum precision proxy),
  - explicit bypass remains available via `--skip-calibration-gate` for controlled exceptions.

## Major Remaining Work
1. None for the current mission scope.

## De-Prioritized (Not Blocking Mission)
1. UI chip/drawer integration and UI consistency tests (frontend repo scope).
2. p95 SLO benchmark dashboards/alert routing.
3. Feature-flag naming parity (`FLOW_FILTERS_V2` vs `FLOW_SHADOW_MODE`).
4. Formal coverage-percentage gate enforcement.
