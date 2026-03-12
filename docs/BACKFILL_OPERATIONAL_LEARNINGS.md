# Backfill Operational Learnings

Last updated: 2026-03-11

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
| Day-level rewrites reported success but rows later missing | Async ClickHouse delete mutation (`mutations_sync=0`) raced with insert | Use `CLICKHOUSE_DELETE_MUTATION_SYNC=1` for backfill delete+rewrite paths | Keep mutation sync deterministic by default; only relax in controlled benchmarks |
| Quote/stock symbol-day took minutes while Theta download took seconds | Synchronous day-scope delete mutation dominated wall time (`ALTER ... DELETE ... mutations_sync=1`) | Use insert-only stock/quote upserts (`CLICKHOUSE_INSERT_ONLY_STOCK_QUOTE=1`) and rely on ReplacingMergeTree latest-row semantics | Keep stock/quote delete+rewrite disabled by default; only enable for controlled corrective runs |
| Delete-heavy remediation slowed unpredictably mid-run | Mutation latency spikes on local ClickHouse due merge pressure / queued mutations | Enable mutation budget protection (`CLICKHOUSE_DELETE_BUDGET_PROTECTION=1`) so repeated spikes auto-downgrade allowlisted safe tables to insert-only | Keep strict-delete fact tables out of the auto-downgrade allowlist unless explicitly testing non-deterministic paths |
| Enrich support-table rewrites dominated per-symbol wall time | Multiple ReplacingMergeTree support tables were still doing synchronous delete-before-insert | Optionally enable `CLICKHOUSE_INSERT_ONLY_ENRICH_SUPPORT=1` for backfill waves to skip delete mutations on derived/support tables | Only enable when downstream reads are dedupe-safe (`argMax`/`FINAL`) or follow with cleanup rewrite pass |
| Download/enrich hot loop still spent time in ClickHouse counts | Plain day-scoped `count()`/existence checks hit large raw tables repeatedly | Use projection-backed day row counts (`p_cov_day_symbol_minute`) and `LIMIT 1` existence checks for branch decisions | Keep hot-path queries projection-first; avoid raw `count()` in loop control paths |
| Full-range 1m coverage (`--source raw`) was very slow on quote table | `uniqExact(minute_bucket_utc)` over billions of raw quote rows forced near-full scans | Use projection-backed grouped-minute query path and materialize projection partitions for active backfill horizon | Keep `p_cov_day_symbol_minute` projection present/materialized on quote+stock+trade+enriched tables |
| Explicit raw-component remediation (without tradequote) still triggered trade sync and became mutation-bound | Trade stream was designed as base day-cache sync for full runs, and legacy `BACKFILL_FORCE_TRADE_SYNC` could force-apply during selective component waves | Make `tradequote` an explicit raw-component token; default to trade-sync `skip` whenever explicit component selection excludes `tradequote`; keep override via `BACKFILL_TRADE_SYNC_MODE=force` only for trade-stream repair | Monitor `[BACKFILL_RAW_COMPONENTS_TRADE_SYNC_POLICY]`; if non-skip override usage stays zero across waves, remove that override path |
| Quote remediation in force mode transferred full-day payloads even for small holes | Force quote mode bypassed gap-window planning and always requested broad windows | In `BACKFILL_FORCE_QUOTE_FULL=1`, prefer contiguous missing-minute windows and only fall back to full-day when gap planning is not efficient | Monitor `[QUOTE_GAP_FILL]` strategy mix and `[QUOTE_WINDOW_PLAN]` window counts; keep force-full strict mode as opt-out only |
| Download workers hit OOM on heavy symbols (AAPL/MSFT/NVDA) even before many jobs completed | Greeks hydration accumulated all expirations for a symbol-day in memory before write | Stream greeks by expiration and write incrementally; perform day-scope delete once on first batch and append remaining batches | Keep per-worker heap in the `1024-1536` MB range and validate heavy-symbol canary emits `[GREEKS_SYNC_STATS]` without heap growth before full-range relaunch |
| Pipeline workers intermittently OOM/restarted mid-wave under overlap even after stream fixes | 1 GB default V8 heap in pipeline launcher was too tight once multiple components overlapped | Raise pipeline launcher default heap to `1536` and keep worker-count caps from RAM budget logic | For overlap waves, prefer `DOWNLOAD_WORKERS=4 ENRICH_WORKERS=2` and only increase after canary stability |
| Enrich-only tail run hit OOM even after pipeline hardening | Parallel runner still defaulted worker heap to 1 GB (`BACKFILL_NODE_MAX_OLD_SPACE_MB=1024`) | Raise parallel runner default heap to `1536` so manual enrich/download reruns use the same safe baseline | Keep per-worker heap aligned across pipeline + parallel launchers to avoid mode-specific instability |
| Requested `4/4` overlap started as `3/3`, reducing Theta throughput | 10GB RAM guardrail + overlap worker cap auto-downgraded both stages | Use asymmetric overlap: `DOWNLOAD_WORKERS=4 ENRICH_WORKERS=2` to saturate Theta cap while staying within budget | If launcher prints worker-cap downgrade, relaunch with `4/2` for download-heavy waves rather than accepting `3/3` |
| Enrichment appeared "finished with gaps" right after pass 1 | `WAIT raw_not_ready` is expected during overlap; pass 1 only seeds pending list | Treat pass-1 waits as transient, continue loops until pending set drains after raw completion | Never classify enrich failure until `enrich.done` exists and pass-complete pending count reaches zero |
| Occasional `/v3/calendar/on_date` timeout seen during load | Control-plane timeout under concurrent load; not necessarily data failure | Allow transient timeout and continue when job returns `OK`; only act on job-level `FAIL` | Separate telemetry for `THETA_DOWNLOAD ok:false` vs job failure and avoid requeueing successful jobs |
| Early-close days appeared missing | Early-close treated as non-open in some generation/expectation logic | Include `early_close` as tradable session with correct shortened window | Calendar logic explicitly handles `open` + `early_close` |
| Repeatedly rerunning same day after each fix | No canary progression discipline | Validate on one canary day, then proceed to next day | Add day progression gate in runbook |
| Long run with poor visibility | Insufficient telemetry and per-job coverage detail | Enable gap telemetry and stream heartbeats while debugging | Keep telemetry toggle documented and standardized |
| Could not explain “slow run” vs expected network throughput | Request telemetry had rows/duration but no byte counters, so actual bandwidth and payload size were unknown | Add `bytesDownloaded` to all `[THETA_DOWNLOAD]` log paths and aggregate bytes/sec in `scripts/backfill/aggregate-run-telemetry.js` | Use bytes/sec + rows/sec together; if bytes/sec is high but wall time is slow, optimize DB path before changing Theta concurrency |
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
