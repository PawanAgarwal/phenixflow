# What Is Remaining To Be Done

## Mission Focus
Build and operate a reliable **sigScore for unusual options flow** that:
- captures the most important signal dimensions,
- is reproducible and explainable,
- reuses downloaded data aggressively,
- avoids scoring on stale or incomplete inputs.

This document intentionally filters the broader project backlog to only the work that materially impacts sigScore quality and cached-data strategy.

## Current Strengths (Already in Place)
- Historical flow pipeline already supports raw sync, enrichment, chip evaluation, and day/metric cache states (`full`/`partial`).
- sigScore currently includes a richer feature set (time, spread, sweep, multileg, delta, IV skew), not just the original 5-term formula.
- Per-minute derived rollups store sigScore and component aggregates for reuse and analysis.
- Cache-aware backfill scripts already skip work when day + required metric caches are complete.

## Key Discrepancies and Recommended Direction

### 1) sigScore Definition Mismatch (Goals/API spec/seed config vs runtime)
- Current mismatch:
  - Docs/spec/seed config describe the original 5-term score.
  - Runtime computes an expanded v4 score.
- Recommended approach:
  - Keep the **expanded runtime approach** (better for mission: includes more unusual-flow signal context).
  - Formalize versioning: `v1_baseline` and `v4_expanded` as first-class, selectable rule versions.
  - Update spec/docs to match runtime behavior and version boundaries.
- Why this is better:
  - Mission asks for a “decent sigScore encompassing important signals”; expanded features are more aligned than reverting to the simpler baseline.

### 2) Rule Version Table Exists but Is Not Active Control Plane
- Current mismatch:
  - `filter_rule_versions` exists, but scoring logic is still effectively code-weight driven.
- Recommended approach:
  - Make `filter_rule_versions` the active source for score weights/thresholds.
  - Persist `rule_version` on every enriched row and expose active version in API metadata.
- Why this is better:
  - Enables controlled tuning without hidden behavior drift; preserves comparability of historical scores.

### 3) Metric Availability Policy Is Not Yet Unified
- Current mismatch:
  - Some missing metrics trigger `metric_unavailable`; other paths default to zeros/nulls.
- Recommended approach:
  - Adopt a **two-tier policy**:
    - strict mode for ranking/alerting chips (`high-sig`, `unusual`, `urgent`): require complete critical metrics,
    - permissive mode for browse/listing: allow partial score but attach `scoreQuality` metadata.
- Why this is better:
  - Prevents false positives in unusual-flow ranking while preserving UI continuity.

## Implementation Status (Updated 2026-02-28)
All recommendations in this document have now been implemented in the repository. The sections below remain as a traceable checklist, with statuses updated to `Implemented`.

## Remaining Work (Prioritized)

## P0 (Must-Do for Mission)

### R1. Establish a Single sigScore Contract (Versioned and Documented)
- Status: Implemented
- Remaining details:
  - Freeze formulas and input normalization per version.
  - Define required/optional inputs for each version.
  - Publish exact score decomposition schema and API payload fields.
- sigScore impact:
  - Eliminates ambiguity and score drift.
  - Makes “unusual” ranking stable across time.
- Data strategy impact:
  - Supports deterministic recompute and reproducible backtests.

### R2. Make Rule Configuration DB-Driven at Runtime
- Status: Implemented
- Remaining details:
  - Load active weights/thresholds from `filter_rule_versions`.
  - Add safe activation workflow (checksum + activation timestamp + rollback).
  - Return active rule version in all score-bearing responses.
- sigScore impact:
  - Controlled tuning and auditability.
- Data strategy impact:
  - Consistent re-enrichment and historical comparability.

### R3. Enforce Score Quality Gating for Unusual-Flow Decisions
- Status: Implemented
- Remaining details:
  - Define a critical-metric set for score validity (`value`, `executionSide`, `sentiment`, `repeat3m`, `otmPct`, `oi`, `volOiRatio`, etc.).
  - Add `scoreQuality` and `missingMetrics` fields.
  - Ensure `high-sig`/`unusual` chips only use valid-quality scores in strict mode.
- sigScore impact:
  - Reduces false confidence and noisy unusual-flow detection.
- Data strategy impact:
  - Makes cache completeness meaningful for downstream decisions.

### R4. Unify Live and Historical Read Paths Around Enriched Cache
- Status: Implemented
- Remaining details:
  - Ensure `/api/flow` uses the same enriched+cached strategy as historical enrichment outputs.
  - Remove fixture fallback behavior for production runtime and replace with explicit “no live data/degraded” metadata.
  - Keep one canonical field/transform path for score-bearing rows.
- sigScore impact:
  - Prevents divergence between “what is scored” and “what is served live.”
- data strategy impact:
  - Maximizes reuse of precomputed enrichment and avoids duplicate compute.

### R5. Add Explicit Degraded Mode for Stale/Backlogged Enrichment
- Status: Implemented
- Remaining details:
  - Trigger degraded mode when enrichment lag/backlog breaches thresholds.
  - Return explicit metadata (e.g., degraded reason, lag seconds, score-quality scope).
  - Suppress or clearly mark score-dependent chips in degraded mode.
- sigScore impact:
  - Avoids misleading high-confidence score output on stale data.
- Data strategy impact:
  - Makes freshness a first-class part of cache policy.

### R6. Harden Ingestion Reliability to Protect Signal Completeness
- Status: Implemented
- Remaining details:
  - Add retry with exponential backoff + jitter.
  - Add dead-letter capture for malformed rows.
  - Add bounded queue/backpressure handling and drop accounting.
- sigScore impact:
  - Fewer missing events -> more accurate repeat/volume and score inputs.
- Data strategy impact:
  - More trustworthy raw cache, fewer corrupted/partial days.

## P1 (High-Value Next)

### R7. Extend Cache Reuse to Supplemental Feeds (Spot/OI/Greeks)
- Status: Implemented
- Remaining details:
  - Persist endpoint-level snapshots keyed for reuse (symbol/day/minute/contract).
  - Add TTL/validity windows and invalidation rules.
  - Prefer local cached supplemental data before network calls.
- sigScore impact:
  - Stabilizes score components across reruns and retries.
- Data strategy impact:
  - Reduces repeated downloads and external dependency load.

### R8. Add Observability Focused on Score and Cache Quality
- Status: Implemented
- Remaining details:
  - Track null/missing rates per score component.
  - Track cache hit rate by data type (raw, enriched, spot, OI, greeks).
  - Track distribution drift for sigScore and key components.
- sigScore impact:
  - Early detection of score degradation and skew.
- Data strategy impact:
  - Measurable reuse efficiency and data health.

### R9. Build Calibration Loop for “Unusual” Detection Quality
- Status: Implemented
- Remaining details:
  - Define offline evaluation set from cached historical sessions.
  - Measure precision/recall proxies for `high-sig` and `unusual`.
  - Tune weights/thresholds with versioned promotion criteria.
- sigScore impact:
  - Moves score quality from intuition to measured performance.
- Data strategy impact:
  - Leverages stored cache as a repeatable evaluation corpus.

## P2 (Useful but Not Immediate for This Mission)

### R10. Top-200 Universe Management and Operational Scaling
- Status: Implemented
- Remaining details:
  - Add maintainable symbol-universe config and update workflow.
  - Validate ingestion/enrichment behavior at universe scale.
- sigScore impact:
  - Improves comparability and breadth; not required to fix score correctness first.
- Data strategy impact:
  - Larger cache footprint and stronger need for partitioning/retention policy.

## Recommended Execution Order
1. R1 + R2 (lock definition and control plane).
2. R3 + R5 (quality/freshness gating semantics).
3. R4 + R6 (live-path consistency and ingest reliability).
4. R7 + R8 (cache reuse depth and quality observability).
5. R9 (calibration and iterative tuning).
6. R10 (scale-out once score correctness/reuse are stable).

## Out of Scope for This Document
- UI chip/drawer implementation details.
- General frontend behavior not tied to score correctness or cache strategy.
- Non-sigScore product features (market cap/sector/earnings filters).

## Residual Follow-Up (Post-Recommendation)
- Operational SLO dashboards and alert routing (p95 latency, lag, error budgets) should be completed as production hardening follow-up.
- Rule/config governance can be extended with formal promotion workflow docs (staging -> canary -> full activation) for team operations.
