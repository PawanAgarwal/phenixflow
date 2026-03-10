# Backfill Operational Learnings

Last updated: 2026-03-10

This document captures concrete lessons from multi-day historical backfill and remediation runs so future agents do not repeat avoidable failures.

## 1. Non-Negotiable Execution Rules
1. Do not move to next day until current day passes:
   - download verification,
   - enrichment verification,
   - failure classification/requeue complete.
2. Do not count unattempted days as missing.
3. Use targeted reruns, not full-range reruns, for remediation.
4. Keep Theta concurrency at provider cap (`<= 4`) unless provider limits change.
5. Keep run artifacts and produce an end-of-wave verification summary.

## 2. Failure Signatures and Correct Actions
| Symptom | Root Cause | Correct Action | Preventive Guardrail |
|---|---|---|---|
| Day marked missing before it was even run | Missing logic used expected grid only | Classify with attempted-vs-unattempted using chunk-status tables | Coverage analyzer defaults to attempted-only missing |
| `BACKFILL_FORCE=1` run still skipped expected refresh behavior | Force did not propagate through full materialization path | Pass `forceRecompute` into `materializeHistoricalDayInClickHouse` and apply in sync decision | Treat force propagation as part of smoke tests |
| Quote minute coverage equals trade minute coverage on many days | Quote plane was not fully refreshed for those symbol-days | Run quote-only with `BACKFILL_RAW_COMPONENTS=quote BACKFILL_FORCE=1 BACKFILL_FORCE_QUOTE_FULL=1` | Add quote-gap pattern detector to remediation triage |
| Early-close days appeared missing | Early-close treated as non-open in some generation/expectation logic | Include `early_close` as tradable session with correct shortened window | Calendar logic explicitly handles `open` + `early_close` |
| Repeatedly rerunning same day after each fix | No canary progression discipline | Validate on one canary day, then proceed to next day | Add day progression gate in runbook |
| Long run with poor visibility | Insufficient telemetry and per-job coverage detail | Enable gap telemetry and stream heartbeats while debugging | Keep telemetry toggle documented and standardized |
| Worker OOM or unstable throughput | Over-aggressive worker/memory settings | Reduce workers and/or heap, split heavy-symbol windows | Keep memory budget caps and large-symbol windows in canonical params |

## 3. Required Verification Dimensions
For each run wave:
1. Slot closure:
   - stock vs padded expectation,
   - quote vs core expectation,
   - enrich vs trade parity.
2. Attempt-state accounting:
   - expected symbol-days,
   - attempted symbol-days per stream,
   - attempted missing symbol-days.
3. Throughput:
   - wall clock time,
   - raw quote rows ingested,
   - enriched rows,
   - normalized speed (`seconds per 1m slot`, `rows/sec`).
4. Retry health:
   - transient retries,
   - failed jobs,
   - requeue completion status.

## 4. Start-From-Scratch Playbook
1. Preflight:
   - load env (`.env.mon79.local`),
   - verify ThetaTerminal and ClickHouse availability.
2. Generate open/early-close symbol-day list.
3. Run download wave with stable profile (`workers=4`, `ndjson`, bounded memory).
4. Run enrichment wave.
5. Run coverage analysis in attempted-only mode.
6. Build targeted missing lists by component.
7. Apply component-specific remediation:
   - quote-only: include `BACKFILL_FORCE_QUOTE_FULL=1` when quote-gap pattern is detected.
8. Re-run enrichment for affected days.
9. Re-verify and repeat only for unresolved attempted gaps.

## 5. Commands to Keep Handy
Coverage analysis (attempted-only default):

```bash
node scripts/backfill/analyze-1m-coverage.js \
  --symbol-days <symbol_days.tsv> \
  --calendar <calendar_detailed.tsv> \
  --from YYYY-MM-DD \
  --to YYYY-MM-DD \
  --source raw \
  --out-dir artifacts/reports \
  --tag <timestamp>
```

Quote-hole targeted remediation:

```bash
BACKFILL_MODE=download \
BACKFILL_FORCE=1 \
BACKFILL_RAW_COMPONENTS=quote \
BACKFILL_FORCE_QUOTE_FULL=1 \
BACKFILL_SYMBOL_DAY_LIST_PATH=<missing_quote_list.tsv> \
THETADATA_MAX_CONCURRENT_CONNECTIONS=4 \
BACKFILL_WORKERS=4 \
bash scripts/backfill/backfill-clickhouse-historical-days-parallel.sh
```

Enrich parity remediation:

```bash
BACKFILL_MODE=enrich \
BACKFILL_FORCE=1 \
BACKFILL_SYMBOL_DAY_LIST_PATH=<missing_enrich_vs_trade.tsv> \
BACKFILL_WORKERS=2 \
bash scripts/backfill/backfill-clickhouse-historical-days-parallel.sh
```

## 6. Documentation Linkage
1. Runtime knobs and profiles: `docs/BACKFILL_RUNTIME_PARAMETERS.md`
2. Agent operating contract: `AGENTS.md`
3. System architecture: `docs/PHENIX_ARCHITECTURE.md`
