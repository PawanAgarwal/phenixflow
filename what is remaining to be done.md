# What Is Remaining To Be Done

## Latest Assessment (2026-03-01)
Core implementation for the `v5_swing` mission is now in place:
1. `v5_swing` scoring model added with availability-aware renormalization.
2. Swing-focused component norms are computed and persisted on enriched rows.
3. Stock 1m supplemental cache reuse is integrated into enrichment.
4. Theta supplemental fetches now run with bounded parallelism (default `18`).
5. Swing calibration tooling exists with candidate rule artifact generation.

## Mission-Critical Work Still Remaining

### R1. Build enough stock-cache coverage for valid swing calibration
- Status: Remaining
- Why it matters:
  - Swing calibration needs underlying price paths for `1/3/5` trading-day labels.
  - Without enough stock 1m cache rows, `precisionProxy` is not meaningful.
- Required action:
  - Backfill stock 1m supplemental cache for target symbols/date windows used in calibration.
  - Re-run `node scripts/sigscore/calibrate-swing.js --max-rows=<n>` and verify non-trivial `evaluatedRows`.
- Impact:
  - Enables data-backed weight tuning for future-move signal quality.

### R2. Promote a calibrated `v5_swing` candidate rule version
- Status: Remaining
- Why it matters:
  - `v5_swing` runtime path exists, but active production rule can still be `v4_expanded`.
- Required action:
  - Generate candidate config/checksum from swing calibration output.
  - Insert candidate into `filter_rule_versions`.
  - Activate with `scripts/rules/activate-rule-version.js` once gate metrics pass.
- Impact:
  - Moves future-movement-aware scoring from implemented-capability to active behavior.

### R3. Run end-to-end validation on real cached symbol-days
- Status: Remaining
- Why it matters:
  - Confirms minute-by-minute `sigScore`, `scoreQuality`, and `sigScoreComponents` are coherent on production-like data.
- Required action:
  - Run historical sync/enrichment for top symbols and last 7 days.
  - Verify:
    - `sig_score` remains in `[0,1]`,
    - `score_quality` and `missing_metrics_json` match component availability,
    - warm-cache reruns reduce supplemental misses.
- Impact:
  - Prevents silent scoring regressions and validates cache strategy.

## Completed In This Round
1. Added `v5_swing` support in:
   - `/Users/pawanagarwal/github/phenixflow/src/historical-formulas.js`
   - `/Users/pawanagarwal/github/phenixflow/src/scoring/rule-config.js`
   - `/Users/pawanagarwal/github/phenixflow/scripts/db/sql/003_seed.sql`
2. Added swing component persistence + explainability payload:
   - `/Users/pawanagarwal/github/phenixflow/src/historical-flow.js`
3. Added supplemental stock cache integration and parallel fetch queue (`18` default):
   - `/Users/pawanagarwal/github/phenixflow/src/historical-flow.js`
4. Added swing calibration/candidate tooling:
   - `/Users/pawanagarwal/github/phenixflow/scripts/sigscore/calibrate-swing.js`
   - `/Users/pawanagarwal/github/phenixflow/package.json` (`sigscore:calibrate:swing`)
5. Updated runtime/API metadata surfacing:
   - `/Users/pawanagarwal/github/phenixflow/src/flow.js`

## De-Prioritized (Non-Blocking)
1. Frontend chip/drawer polish in external UI repo.
2. p95 dashboards/alert routing hard gates.
3. Feature-flag naming parity cleanup.
