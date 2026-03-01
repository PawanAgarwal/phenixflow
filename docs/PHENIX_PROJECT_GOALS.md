# Phenix Project Goals (Current Mission)

Last updated: 2026-03-01

## Mission
Build a deterministic, explainable `sigScore` that captures unusual options flow with the best available directional signal for **1-5 day swing outcomes**, while maximizing cache reuse and minimizing redundant data downloads.

## Locked Product Decisions
1. Horizon: `swing_1_5d`.
2. Model style: heuristic scoring with calibrated/overrideable weights (`v5_swing`).
3. Data scope: Theta-derived options + stock minute data only.
4. Quality policy: score-bearing decisions must honor `scoreQuality` (`complete` vs `partial`) and `missingMetrics`.
5. Cache policy: reuse-first for historical dates; bounded parallel supplemental fetches (`THETADATA_SUPPLEMENTAL_CONCURRENCY`, default `18`).

## What The Backend Must Deliver
1. Deterministic historical enrichment and replay for a symbol-day.
2. Versioned scoring/rules via `filter_rule_versions` with active pointer support.
3. `sigScore` in `[0,1]` with explainability (`sigScoreComponents`) and explicit quality/missing metadata.
4. Cache-first data strategy across raw trades, enrichment metrics, and supplemental Theta pulls.
5. Backward-compatible flow APIs and saved filters/alerts.

## Implemented Capabilities (Current State)
1. `v5_swing` scoring implemented end-to-end with availability-aware renormalization.
2. Directional swing components implemented and persisted:
   - `flowImbalanceNorm`, `deltaPressureNorm`, `underlyingTrendConfirmNorm`,
   - `cpOiPressureNorm`, `ivSkewSurfaceNorm`, `ivTermSlopeNorm`,
   - `valueShockNorm`, `dteSwingNorm`, `liquidityQualityNorm`, `multilegPenaltyNorm`.
3. Execution-side inference improved for inside-spread prints (midpoint/tick/last-side fallback), materially reducing direction loss.
4. Delta fallback proxy added when greeks delta is unavailable.
5. Score quality + explainability persisted on enriched rows:
   - `score_quality`, `missing_metrics_json`, `sig_score_components_json`.
6. Strict gating in chip logic for score-dependent chips (`high-sig`, `unusual`, `urgent`) with directional eligibility requirements.
7. Rule resolution hardened so forced model overrides do not mix mismatched active-rule weights.
8. Swing calibration tooling added (`scripts/sigscore/calibrate-swing.js`) for candidate rule generation.

## Current Mission Metrics (Latest Validation Window)
Validated window: 2026-02-19 to 2026-02-27 (`AAPL`, `MSFT`, `AMZN`).

1. Total scored rows: `2,797,200`.
2. Complete quality rows: `2,783,340`.
3. Partial quality rows: `13,860`.
4. Directional component missing rates:
   - `scoreComponent:flowImbalanceNorm`: `0`
   - `scoreComponent:deltaPressureNorm`: `1`
   - `scoreComponent:underlyingTrendConfirmNorm`: `0`
5. Max observed `sigScore`: `0.741810` (no `>=0.85` rows in this window).

## Mission-Critical Work Remaining
1. Complete walk-forward swing calibration on broader symbol/time coverage and promote a calibrated `v5_swing` rule version when gates pass.
2. Keep periodic replay validation to guard determinism, quality, and cache-hit behavior as new data accumulates.

## Explicit Non-Goals (Current Phase)
1. Frontend UI polish/integration details.
2. Dashboard/alerting UX hardening as release gate.
3. Non-Theta alternative data feeds.
