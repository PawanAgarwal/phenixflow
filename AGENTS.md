# PhenixFlow Agents Runbook

## Goal
- Maintain complete last-30-day coverage for target symbols in ClickHouse for:
  - option trade+quote stream (`option_trades`)
  - option quote 1m raw (`option_quote_minute_raw`)
  - stock price 1m raw (`stock_ohlc_minute_raw`)
  - enriched option rows (`option_trade_enriched`)
- Maximize throughput without OOM, while keeping retries/resume safe and idempotent.

## Hard Resource Guardrails
- Agent memory budget: target <= 10 GB RSS.
- Never run unbounded in-memory day arrays when streaming alternatives exist.
- Prefer chunked read/write (2k-5k rows) with bounded queues and explicit backpressure.
- Keep ClickHouse read windows bounded; split windows on oversized-string/heap signals.

## ThetaData Connection/Concurrency Rules
- Respect provider hard cap: max 4 concurrent connections.
- Use centralized concurrency control; never exceed configured cap.
- Preferred operating mode:
  - stream historical endpoints (ndjson)
  - process symbol-day in parallel up to safe cap
  - reduce parallelism immediately if repeated 5xx/timeout bursts occur
- Retries:
  - retry only transient failures (timeouts, connection resets, 429/5xx)
  - use exponential backoff with cap
  - avoid aggressive short timeouts that trigger retry storms

## Download Reliability Rules
- Resume from last completed minute for retryable stream failures.
- On resume, delete/rewrite only the resumed minute scope to avoid duplicate rows.
- Never redownload full day if minute-level resume point is known.
- For large symbols (e.g. SPY/QQQ), request smaller windows (e.g. 60m) to reduce server/client stress.
- Emit heartbeat logs while streaming (every N rows) so stalled streams are detectable.

## Enrichment Performance Rules
- Enrichment must run streaming read -> compute -> streaming write.
- Do not block on full 30-day downloads before enriching; pipeline download/enrich continuously.
- Run enrichment workers at roughly 60% of physical cores by default; raise/lower based on:
  - CPU underutilization
  - memory headroom
  - ClickHouse insert/query pressure
- If CPU is low and memory headroom exists, scale workers up incrementally.

## ClickHouse Behavior Expectations
- ClickHouse supports parallel reads/writes; use this with bounded batches.
- Keep inserts idempotent with deterministic keys and scoped deletes only where necessary.
- Use replacing tables for status/cache tables to avoid heavy delete-before-insert when possible.

## Chunk Status Policy
- Maintain:
  - `options.option_download_chunk_status`
  - `options.option_enrich_chunk_status`
- Canonical chunk baseline for stream comparability is trade chunk grid (`option_trade_quote_1m`).
- For each chunk record status as one of:
  - `complete`, `partial`, `missing`, `extra`
- Rebuild chunk-status from raw/enriched tables after major backfill waves.

## Calendar/Holiday Policy
- Use Theta calendar API for market-open checks.
- Skip closed days explicitly; do not treat holidays/weekends as failures.

## Monitoring and Auto-Healing
- Continuously monitor:
  - worker liveness
  - rows/sec and chunk progress
  - retry/error rates
  - network throughput
- If download bandwidth remains <1 MB/s for sustained window, treat as degraded:
  - inspect stream heartbeats and failure logs
  - rebalance concurrency/window sizes
  - restart only failed units, not full run

## Backfill Execution Practices
- Prefer targeted missing symbol-day lists over full-range reruns.
- For raw-component-only remediation, ensure force mode is used when day cache would otherwise skip execution.
- Keep reports/artifacts for each run and summarize:
  - total/completed/failed jobs
  - rows hydrated per component
  - remaining missing symbol-days/chunks

## Run Completion Gates (Required)
- Do not advance from day `D` to day `D+1` until `D` passes:
  - download verification (stock/quote/trade expectations),
  - enrichment verification (`enrich == trade` minute coverage),
  - worker failures resolved or explicitly classified as provider no-data.
- If a worker fails, restart only failed units and requeue only failed symbol-days.
- Avoid broad reruns after every code change. Use one canary day to validate fix, then continue with next day.

## Gap Classification Policy
- Missing counts must distinguish:
  - `unattempted` symbol-days (not run yet),
  - `attempted_missing` symbol-days (run but still short),
  - `provider_sparse` symbol-days (attempted, but provider appears to have sparse minutes).
- Default reporting should only treat `attempted_missing` as failure.
- For quote gaps where `quoteSlots == tradeSlots` and both are far below quote expectation:
  - run quote-only remediation with `BACKFILL_RAW_COMPONENTS=quote`,
  - set `BACKFILL_FORCE=1` and `BACKFILL_FORCE_QUOTE_FULL=1`.

## Throughput Benchmark Policy
- Record baseline per run:
  - wall clock duration,
  - total raw quote rows ingested,
  - total enriched rows,
  - expected vs actual minute slots,
  - normalized speed (`seconds per 1m slot` and `rows/sec`).
- Compare each rerun against the benchmark before claiming optimization.

## Git/Change Management
- Commit in understandable chunks:
  - streaming/perf changes
  - reliability/retry changes
  - observability/chunk-status changes
- Exclude transient artifacts/reports from commits unless explicitly requested.

## Canonical Runtime Parameters
- Use `docs/BACKFILL_RUNTIME_PARAMETERS.md` as the single source of truth for:
  - worker count and memory budget selection
  - Theta stream timeout/heartbeat settings
  - ClickHouse mutation/timeout settings
  - targeted quote-only/stock-only/enrich-only runs
- Use `docs/BACKFILL_OPERATIONAL_LEARNINGS.md` for failure signatures, remediation loops, and anti-rerun guardrails learned from prior long runs.
