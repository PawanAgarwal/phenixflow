# Phenix Project Goals

## Project Summary
This document captures the Core Quant V1 plan for the Bullflow-style filter engine, aligned to Phenix goals.

Locked decisions:
1. Scope: Core Quant filters first.
2. Delivery: API-first backend scoring and filter contracts (UI integration can follow in a separate repo).
3. Latency target: freshness and cache completeness are prioritized over UI latency SLOs for current mission phase.
4. Coverage: top 200 tickers.
5. Data policy: Theta-only.
6. Rule strategy: configurable heuristics.
7. Feed mode: Theta streaming as primary ingest path.

## Broad Capabilities to Deliver
1. Real-time flow ingestion and enrichment for a top-200 universe.
2. Deterministic, configurable chip/filter engine for Core Quant rules.
3. Versioned sigScore control plane (`filter_rule_versions`) that governs scoring behavior consistently.
4. Backward-compatible saved filters/alerts with expanded V2 filter state.
5. Cache-first historical and live read paths that reuse downloaded data and avoid duplicate recompute.
6. Reliability semantics for degraded/partial data to prevent misleading score-driven decisions.

## Success Criteria
1. `sigScore` and chip outputs are reproducible for a given `rule_version` on replayed cached sessions.
2. Historical enriched minute rollups provide complete intraday coverage (`390` minute buckets on full market days for tracked symbols).
3. Score-bearing outputs are explicitly quality-tagged (`complete` vs `partial`) and strict-mode chip gating is enforced for unusual-flow decisions.
4. Live and historical score/chip logic use the same active rule configuration source.
5. Downloaded supplemental data (spot/OI/greeks) is reused from cache on reruns whenever validity windows allow.

## Implementation Update (2026-02-28)
Implemented in backend:
1. Versioned sigScore contract with two models:
   - `v1_baseline` (original 5-term weighting).
   - `v4_expanded` (default; includes additional market-structure/greeks/time features).
2. Runtime rule control-plane wired to active `filter_rule_versions` record:
   - active version + checksum support,
   - enriched rows persist `rule_version`,
   - rule activation script: `scripts/rules/activate-rule-version.js`.
3. Score-quality policy implemented:
   - enriched rows persist `score_quality` and `missing_metrics_json`,
   - strict gating enforced for score-dependent chips (`high-sig`, `unusual`, `urgent`) in enrichment path.
4. Data freshness/degraded semantics for `/api/flow`:
   - explicit degraded metadata when lag or source fallback conditions are present,
   - score-dependent chip calculations are suppressed in degraded mode.
5. Supplemental data cache reuse implemented:
   - reusable cache for spot/OI/greeks endpoint results with TTL.
6. Ingestion reliability hardening implemented:
   - retry/backoff/jitter fetch strategy,
   - dead-letter capture for unparseable rows,
   - bounded buffering and dropped-row accounting,
   - ingest worker counters.
7. Calibration tooling added:
   - `scripts/sigscore/calibrate-unusual.js` generates offline calibration report artifacts.
8. Universe management baseline added:
   - maintainable symbol universe file: `config/top200-universe.json`,
   - worker supports `INGEST_SYMBOLS` / `INGEST_UNIVERSE_FILE`.

## Mission Re-Baseline (2026-03-01)
After validating recent symbol-day runs and minute-level score coverage, the mission-critical path is narrowed to backend scoring quality + cache reuse consistency.

Mission-critical closure update (2026-03-01):
1. Seed/spec/runtime sigScore contract now uses explicit versioned model definitions.
2. Live `/api/flow` path now resolves runtime rule-version config for score-bearing chip logic.
3. Strict score-quality gating is enforced on live score-dependent chips (with test/fixture compatibility carve-out only).
4. Production score-bearing reads no longer silently fall back to fixtures.
5. Rule activation now supports a calibration gate for promotion control.
6. `v5_swing` scoring model is implemented in runtime formulas and rule-resolution path (shadow/candidate ready).
7. Supplemental metric strategy now includes stock 1m cache reuse and bounded Theta supplemental parallelism (`THETADATA_SUPPLEMENTAL_CONCURRENCY`, default `18`).
8. Enriched rows now persist score explainability payload (`sig_score_components_json`) and v5 component norms.
9. Swing calibration tooling now exists for 1/3/5-day directional+magnitude labels (`scripts/sigscore/calibrate-swing.js`) with candidate rule artifact generation.

De-prioritized (not blocking current mission completion):
1. UI chip/drawer integration and UI-specific consistency tests.
2. p95 latency/dashboard alerting as release gates.
3. Feature-flag naming parity work (`FLOW_FILTERS_V2` aliasing) when behavior is otherwise equivalent.
4. Coverage percentage gate enforcement as a hard release requirement.

## Phase-1 Scope
1. Implement calculable filters/chips and row-level computed metrics.
2. Expose enriched fields and filter params in Flow API.
3. Align live and historical API paths to a single rule-versioned scoring/filter contract.
4. Keep current saved filters/alerts model compatible.

## Deferred to Phase-2+
1. Market cap, sector, and earnings-soon filters.
2. Non-Theta enrichment and broker/account integrations.
3. Broader redesign outside the flow/filter engine.

## V1 Filter and Chip Definitions

| Filter/Chip | Default Rule |
|---|---|
| `Calls` | `right = CALL` |
| `Puts` | `right = PUT` |
| `Bid` | `price <= bid` |
| `Ask` | `price >= ask` and not `AA` |
| `AA` | `price >= ask + max(0.01, 0.10 * (ask - bid))` |
| `100k+` | `value >= 100000` |
| `Whales` | `value >= 500000` |
| `Sizable` | `value >= 250000` |
| `Large Size` | `size >= 1000` |
| `OTM` | `otmPct > 0` |
| `LEAPS` | `dte >= 365` |
| `Weeklies` | expiration is not standard monthly 3rd-Friday contract |
| `Vol>OI` | `volOiRatio > 1.0` |
| `Repeat Flow` | `repeat3m >= 20` |
| `Rising Vol` | `symbolVol1m >= 2.5 * symbolVolBaseline15m` |
| `AM Spike` | `09:30-10:30 ET` and `symbolVol1m >= 3.0 * rollingOpenWindowBaseline` |
| `High Sig` | `sigScore >= 0.90` |
| `Unusual` | `value >= 100000` and `volOiRatio >= 2.0` |
| `Urgent` | `repeat3m >= 20` OR (`value >= 250000` and `dte <= 14` and `volOiRatio >= 2.5`) |
| `Bullflow` | `bullishRatio15m >= 0.65` and row sentiment bullish |
| `Position Builders` | `21 <= dte <= 180` and `abs(otmPct) <= 15` and `size >= 250` and `side in (ASK,AA)` |
| `Grenade` | `dte <= 7` and `otmPct >= 5` and `value >= 100000` |

## Computed Field Formulas (Row-Level)
1. `value = price * size * 100`.
2. `dte = ceil((expirationDateET - tradeTsET)/86400)`.
3. `spot` from nearest underlying quote at or just before trade timestamp.
4. `otmPct` based on `right`, `strike`, `spot`.
5. `dayVolume` from contract/day rolling aggregate.
6. `oi` from latest open-interest snapshot for contract/date.
7. `volOiRatio = dayVolume / max(oi, 1)`.
8. `repeat3m` from same contract+side trade count in trailing 180s.
9. `sentiment` mapping:
   - bullish: `(CALL and ASK/AA)` or `(PUT and BID)`
   - bearish: `(PUT and ASK/AA)` or `(CALL and BID)`
   - neutral: otherwise
10. `sigScore` in `[0..1]`:
   baseline (`v1_baseline`): `0.35*valuePctile + 0.25*volOiNorm + 0.20*repeatNorm + 0.10*otmNorm + 0.10*sideConfidence`.
   expanded (`v4_expanded`): adds weighted contributions for `dteNorm`, `spreadNorm`, `sweepNorm`, `multilegNorm`, `timeNorm`, `deltaNorm`, `ivSkewNorm`.
   swing (`v5_swing`, candidate): `valueShockNorm`, `volOiNorm`, `repeatNorm`, `otmNorm`, `dteSwingNorm`, `flowImbalanceNorm`, `deltaPressureNorm`, `cpOiPressureNorm`, `ivSkewSurfaceNorm`, `ivTermSlopeNorm`, `underlyingTrendConfirmNorm`, `liquidityQualityNorm`, `sweepNorm`, `multilegPenaltyNorm` with availability-aware weight renormalization.
11. `scoreQuality`/`missingMetrics` must account for score-component availability; missing score components are surfaced explicitly.

## Program Setup (PM Tool)
1. Project name: `Bullflow Filters - Core Quant V1`.
2. Milestones:
   - `M0 Spec and Contracts` (2 days)
   - `M1 Ingestion and Storage` (5 days)
   - `M2 Metrics and Rule Engine` (5 days)
   - `M3 API and Query Layer` (4 days)
   - `M4 Mission Consistency Hardening` (4 days)
   - `M5 Operational Hardening` (deferred/non-gating for mission)
3. Labels:
   - `filters`
   - `thetadata`
   - `flow-api`
   - `ui-flow`
   - `alerts`
   - `performance`
   - `blocked-external`
4. Definition of done:
   - code + tests + docs + smoke command output recorded.

## Delivery Plan (Milestone Breakdown)
1. `M0 Spec and Contracts`
   - Freeze rule config schema and chip dictionary.
   - Build endpoint matrix for Theta streaming + quote + OI + underlying quote APIs.
   - Create fixture dataset contract for deterministic tests.
2. `M1 Ingestion and Storage`
   - Add streaming ingestion worker and reconnect logic.
   - Persist raw trade events and quote-at-trade data.
   - Add rolling aggregate tables for contract/day and symbol/window stats.
3. `M2 Metrics and Rule Engine`
   - Implement derived metric calculator (`dte`, `otmPct`, `volOiRatio`, `repeat3m`, `sentiment`, `sigScore`).
   - Implement configurable rule engine with versioned threshold config.
   - Persist chip flags for fast query filtering.
4. `M3 API and Query Layer`
   - Extend `/api/flow` with server-side filter params and chip selectors.
   - Add `/api/flow/summary` for top tiles and ratios.
   - Add `/api/flow/filters/catalog` returning chips, ranges, and active threshold version.
5. `M4 Mission Consistency Hardening`
   - Ensure live `/api/flow` and historical enrichment consume the same active rule-version thresholds and score model.
   - Eliminate production fixture fallback from score-bearing query paths.
   - Tighten strict score-quality gating for unusual/high-sig/urgent chip decisions.
6. `M5 Operational Hardening` (deferred/non-gating)
   - Run replay tests on historical session samples.
   - Expand observability counters for score-quality and cache-hit distributions.
   - Add optional p95 latency/load checks for top-200 symbol scenario.

## Data Model Goals
1. Keep `option_trades` as raw source.
2. Add `option_trade_enriched` keyed by trade identity for computed fields.
3. Add `contract_stats_intraday` keyed by `(symbol, expiration, strike, right, sessionDate)` for `dayVolume`, `oi`, `volOiRatio`.
4. Add `symbol_stats_intraday` keyed by `(symbol, minuteBucket)` for `risingVol` and `amSpike`.
5. Add `filter_rule_versions` table with active config snapshot and checksum.

## Validation and Test Scenarios
1. Unit tests for each chip threshold and computed metric formula.
2. Unit tests for sentiment/side edge cases (`bid/ask missing`, `zero spread`, `AA boundary`).
3. Integration tests: ingest fixture -> enrich -> `/api/flow` filter query expected ids.
4. Integration tests: repeat-flow 20-in-3-min detection.
5. API tests: cursor pagination stability with new sort/filter predicates.
6. Minute-rollup validation tests: complete `390` intraday buckets and bounded score aggregates on full market days.
7. Performance tests: top-200 stream simulation is optional hardening, not mission gate.
8. Regression tests: existing filters/alerts paths still function with expanded payloads.

## Rollout and Risk Controls
1. Keep a runtime kill-switch for experimental scoring/filter behavior.
2. Compare candidate vs active scoring outputs on replayed cached sessions before activation.
3. Roll out rule-version changes gradually and persist `rule_version` on enriched rows.
4. Fail-safe: if enrichment lag > 30s or live data is unavailable, return explicit degraded metadata.

## Assumptions and Defaults
1. Theta streaming entitlement is available and stable.
2. Market/session calculations use `America/New_York`.
3. Top-200 ticker universe is preconfigured and editable weekly.
4. `ETF` vs `Stock` classification uses an internal maintained symbol-class list.
5. `Sweeps` and `Blocks` mapping depends on Theta condition codes and is versioned in config.
6. Non-Theta fields (`market cap`, `sector`, `earnings`) remain disabled in V1 UI.

## API Surface (Target Endpoints)
1. Extend `GET /api/flow` response rows with:
   - `spot`, `dte`, `otmPct`, `dayVolume`, `oi`, `volOiRatio`, `repeat3m`, `sigScore`, `sentiment`, `chips`.
2. Extend `GET /api/flow` query params:
   - `chips`, `side`, `type`, `sentiment`, `minSigScore`, `maxSigScore`, `minDte`, `maxDte`, `minOtmPct`, `maxOtmPct`, `minVolOi`, `minRepeat3m`.
3. Add `GET /api/flow/summary`.
4. Add `GET /api/flow/filters/catalog`.
5. Keep backward compatibility for existing params:
   - `minValue`, `maxValue`, `right`, `expiration`, cursor pagination.
6. Keep saved filters/alerts compatibility while introducing expanded payload/state.

## Shared Type Contracts (Target)
1. `FlowRowV2`
2. `FlowFilterQueryV2`
3. `ChipRuleConfig`
4. `ChipId`
5. `Sentiment`

## Repository Touchpoints (Reference Execution Paths)
Primary reference implementation path:
- `/Users/pawanagarwal/.openclaw/workspace/projects/bullflow-clone`

Planned file touchpoints:
1. `/Users/pawanagarwal/.openclaw/workspace/projects/bullflow-clone/apps/api/src/index.ts`
2. `/Users/pawanagarwal/.openclaw/workspace/projects/bullflow-clone/apps/api/src/db.ts`
3. `/Users/pawanagarwal/.openclaw/workspace/projects/bullflow-clone/apps/api/src/thetadata/tradeQuote.ts`
4. `/Users/pawanagarwal/.openclaw/workspace/projects/bullflow-clone/packages/core/src/thetadata/client.ts`
5. `/Users/pawanagarwal/.openclaw/workspace/projects/bullflow-clone/apps/web/src/App.tsx`

Planned new files:
1. `/Users/pawanagarwal/.openclaw/workspace/projects/bullflow-clone/apps/api/src/filters/rules.ts`
2. `/Users/pawanagarwal/.openclaw/workspace/projects/bullflow-clone/apps/api/src/filters/engine.ts`
3. `/Users/pawanagarwal/.openclaw/workspace/projects/bullflow-clone/apps/api/src/flow/enrichment.ts`
4. `/Users/pawanagarwal/.openclaw/workspace/projects/bullflow-clone/apps/api/src/flow/types.ts`

## Infrastructure Requirements
1. Theta streaming ingestion worker with reconnect and replay-safe resume.
2. Storage for raw trades, enriched rows, and rolling aggregate stats.
3. Config-driven rule engine with versioned thresholds and checksums.
4. API query layer that is deterministic and cache-first for score-bearing reads.
5. Observability:
   - score-quality mix (`complete` vs `partial`)
   - cache hit/miss metrics for supplemental data
   - ingest parse failure metrics and dropped-row accounting
6. CI gates for lint, unit, integration, and replay/performance checks.
7. Optional feature flag/shadow reporting pipeline for controlled experiments.
8. Reliability controls:
   - health/readiness probes
   - fallback to raw row mode when enrichment lag breaches threshold
   - deterministic fixtures for regression testing
