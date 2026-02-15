# Phenix Project Goals

## Project Summary
This document captures the Core Quant V1 plan for the Bullflow-style filter engine, aligned to Phenix goals.

Locked decisions:
1. Scope: Core Quant filters first.
2. Delivery: API + UI filters.
3. Latency target: 1-5s updates.
4. Coverage: top 200 tickers.
5. Data policy: Theta-only.
6. Rule strategy: configurable heuristics.
7. Feed mode: Theta streaming as primary ingest path.

## Broad Capabilities to Deliver
1. Real-time flow ingestion and enrichment for a top-200 universe.
2. Deterministic, configurable chip/filter engine for Core Quant rules.
3. Server-side query/filter API that fully drives UI chip and drawer behavior.
4. Backward-compatible saved filters/alerts with expanded V2 filter state.
5. Rollout-safe filter evolution using feature flags and shadow comparison.
6. Operational reliability with p95 latency targets, observability, and graceful degradation.

## Success Criteria
1. Ingest-to-UI lag <= 5s p95 during regular market hours.
2. `GET /api/flow` p95 <= 350ms for `limit=50` with 3 active filters.
3. Deterministic unit coverage for chip formulas and range logic >= 95%.
4. Repeat-flow scenario (`20 repeats in 3 mins`) is detected in integration tests.
5. UI chip toggles and drawer filters produce server-side consistent results.

## Phase-1 Scope
1. Implement calculable filters/chips and row-level computed metrics.
2. Expose enriched fields and filter params in Flow API.
3. Add chip bar and drawer controls in Flow UI.
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
   `0.35*valuePctile + 0.25*volOiNorm + 0.20*repeatNorm + 0.10*otmNorm + 0.10*sideConfidence`.

## Program Setup (PM Tool)
1. Project name: `Bullflow Filters - Core Quant V1`.
2. Milestones:
   - `M0 Spec and Contracts` (2 days)
   - `M1 Ingestion and Storage` (5 days)
   - `M2 Metrics and Rule Engine` (5 days)
   - `M3 API and Query Layer` (4 days)
   - `M4 UI Integration` (4 days)
   - `M5 Validation and Hardening` (4 days)
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
5. `M4 UI Integration`
   - Replace placeholder chip behavior with API-backed chips.
   - Expand right-panel filter controls to include key range filters.
   - Wire save/load preset payloads to full filter state.
6. `M5 Validation and Hardening`
   - Run replay tests on historical session samples.
   - Add p95 latency/load checks for top-200 symbol scenario.
   - Add observability counters for ingest lag, parse failures, and filter hit distributions.

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
6. UI tests: chip toggle + range inputs -> URL/query -> expected row subset.
7. Performance tests: top-200 stream simulation with p95 query latency and ingest lag checks.
8. Regression tests: existing filters/alerts paths still function with expanded payloads.

## Rollout and Risk Controls
1. Feature-flag new chips behind `FLOW_FILTERS_V2`.
2. Start in shadow mode: compute chips without enabling default UI filtering.
3. Compare shadow metrics versus visible rows for 3 market sessions.
4. Roll out gradually: core chips first (`Calls`, `Puts`, `Bid`, `Ask`, `100k+`, `Whales`, `OTM`), then advanced chips.
5. Fail-safe: if enrichment lag > 30s, API falls back to raw row view with explicit status.

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
4. API query layer optimized for p95 target (`<= 350ms` for `limit=50` with 3 active filters).
5. Observability:
   - ingest lag metrics
   - parse failure metrics
   - filter-hit distribution metrics
   - request latency and error-rate dashboards
6. CI gates for lint, unit, integration, and replay/performance checks.
7. Feature flag infrastructure and shadow-mode reporting pipeline.
8. Reliability controls:
   - health/readiness probes
   - fallback to raw row mode when enrichment lag breaches threshold
   - deterministic fixtures for regression testing
