# PhenixFlow Agents Runbook

## Current Source Of Truth
- All new market and strategy analysis in this repo uses Massive data only.
- The project-level master plan is `projects/spy-intraday-prediction/PLAN.md`.
- Do not route new analysis through legacy database backfills, `options.*` table names, or
  Theta-derived historical tables.

## Goal
- Keep SPY intraday prediction research reproducible on the Massive flat-file/parquet dataset.
- Preserve sealed historical results separately from intraday/provisional runs.
- Maximize throughput without OOM by streaming large files and writing bounded artifacts.

## Data Roots
Use these canonical roots unless the user explicitly provides a different local path:

- Historical Massive cache: `/Volumes/SEC4TB/massive-data/massive/`
- Intraday/live Massive parquet: `/Volumes/SEC4TB/massive-data/massive-live/parquet/massive/`
- Calendar cache: `/Volumes/SEC4TB/massive-data/calendar/us-equities-options-calendar.json`

Expected Massive datasets:

- `stock_quotes_1m`: SPY, market ETFs, sector ETFs, risk/factor ETFs, and Mag7 1m bars.
- `indices_1m`: SPX/VIX-family 1m bars.
- `option_quotes_1m`: OPRA option 1m aggregates.
- `option_trades_all`: OPRA option trades.

## Hard Resource Guardrails
- Agent memory budget: target <= 10 GB RSS.
- Never load full multi-month option datasets into one unbounded in-memory array.
- Prefer streaming reads, bounded iterators, chunked outputs, and explicit progress logging.
- Keep runtime artifacts under the project `runtime/` and `artifacts/` directories unless the user
  asks for a different destination.

## Coverage Rules
- Coverage checks must inspect Massive files/manifests only.
- For EOD tasks, resolve `auto` end dates against both historical Massive cache and live Massive
  parquet. If historical/S3 files are not present for the latest open day but live parquet exists,
  use the live parquet day instead of stopping or launching a broad historical backfill.
- Skip weekends and market holidays explicitly using the cached exchange calendar when available.
- Distinguish:
  - `unattempted` dates/files,
  - `attempted_missing` dates/files,
  - `provider_sparse` dates/files where the provider data exists but is thin.
- Default reporting should treat only `attempted_missing` as failure.

## Research Protocol
- Official sealed protocol:
  - Train: January 2026.
  - Test: February 2026, March 2026, and April 2026 through `2026-04-27`.
  - Treat `2026-04-28` and later intraday files as provisional unless a newer plan says otherwise.
- Sensitivity tracks may use more history, but report them separately from official January-only
  results.
- All feature rows must be causal: use only data at or before the prediction minute.
- Rank by prediction quality first, then SPY long/cash/short policy performance with costs,
  slippage, drawdown, turnover, exposure share, and buy-and-hold comparison.

## Massive Download Rules
- Prefer existing local Massive caches over network downloads.
- For daily strategy-service refreshes after EOD, use
  `npm run strategy-service:refresh-daily-fast` as the default path. It reuses
  prior same-code artifacts, appends missing ML/TSLL days where possible, writes
  the normalized strategy result contract to SQLite, and should avoid a full
  historical replay unless strategy logic, costs/slippage, or historical inputs
  changed.
- Before refreshing strategy-service EOD inputs, run/trigger the coverage-aware
  `projects/pym-v5-replication/scripts/refresh-eod-inputs.js` path. It skips already-current EOD
  bars, appends only missing option-feature days, and rebuilds stress only when its artifact is
  behind the requested end date.
- If refreshing flat files, use the Massive flat-file downloader and S3 credentials from the
  environment.
- Retry only transient failures such as timeouts, connection resets, 429, and 5xx responses.
- Use exponential backoff with a cap and emit heartbeat/progress logs so stalled downloads are
  detectable.

## Run Completion Gates
- Do not claim a run is complete until coverage, dataset build, and experiment artifacts are present
  for the requested date span.
- For changed model logic, use one small canary/smoke run first, then run the full requested window.
- Compare reruns against the prior benchmark before calling an optimization successful:
  - wall clock duration,
  - rows/minutes scanned,
  - prediction count,
  - monthly returns and drawdowns,
  - normalized throughput.

## Verification
- Run the Massive-only guardrail tests after changing project data access code.
- Use targeted tests for the touched modeling module before running the whole suite.
- If Python dependencies are missing, report that clearly and keep the Node research path usable.

## Git/Change Management
- Commit in understandable chunks:
  - data-access or coverage changes,
  - modeling/research changes,
  - reporting/observability changes,
  - documentation/runbook changes.
- Exclude transient runtime artifacts/reports from commits unless explicitly requested.
