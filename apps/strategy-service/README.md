# PhenixFlow Strategy Service

Generic strategy API service for daily portfolio strategies. It currently serves
the base PYM V5 study, option-flow overlays, and PYM ML research studies as
separate strategy APIs under one registry.

## Run Locally

```bash
npm run strategy-service:start
```

Default URL:
`http://localhost:3120`

## Run With Docker

```bash
docker compose -f docker-compose.strategy-service.yml up --build
```

The compose file mounts the existing PYM runtime and artifacts folders into the
container. The ignored `.env.local` file is used for Massive credentials when a
strategy refresh endpoint needs to download updated adjusted EOD bars.

The default local container is capped at `0.50` CPU, `512m` RAM, `768m` swap,
and `128` pids. That is enough for the current in-memory EOD and option-feature
reports while leaving room for lightweight refresh jobs.

## Strategy APIs

List strategies:

```http
GET /api/strategies
```

Every strategy metadata payload includes a compact `execution` summary so
runtime schedulers can discover timing without hardcoding strategy-specific
times:

```json
{
  "execution": {
    "manifestVersion": "execution-manifest.v1",
    "strategyVersion": "pym-v5.execution.v1",
    "status": "paper_enabled",
    "promotion": {
      "authorized": true,
      "domain": "production_candidate",
      "authorizedStatuses": ["paper_enabled", "live_enabled"]
    },
    "timingClass": "EOD",
    "timezone": "America/New_York",
    "session": "REGULAR",
    "activation": {
      "type": "after_market_close",
      "time": "16:05"
    },
    "signalCadence": "daily_eod",
    "idempotencyKeyFields": ["strategyId", "strategyVersion", "signalDate"]
  }
}
```

Promoted strategies also expose an authoritative, validated execution manifest:

```http
GET /api/execution-manifests
GET /api/execution-manifests/pym-v5
GET /api/execution-manifests/tsll-seconds-passive-scalper
```

The metadata summary and manifest API are generated from the same definitions
in `apps/strategy-service/src/strategies/execution.js`, so they cannot drift.
PhenixFlow does not execute broker logic; the manifest is a stable handoff
contract for external paper/live trading runtimes.

Manifest fields include `strategyId`, `strategyVersion`, `status`, timing,
`promotion`, `symbols`, `signalEndpoint`, `idempotencyKeyFields`,
`executionDefaults`, `riskDefaults`, `theoreticalPerformance`, and
`provenance`. Only `pym-v5` and `tsll-seconds-passive-scalper` are currently
authorized for production promotion and may use `paper_enabled` or
`live_enabled`; all other strategy metadata remains explicitly
`research_only` with `promotion.authorized = false`.

The TSLL seconds scalper also exposes a downloadable executable kernel artifact:

```http
GET /api/kernels/tsll-seconds-passive-scalper.execution.v1/manifest
GET /api/kernels/tsll-seconds-passive-scalper.execution.v1/download
```

The execution manifest `kernel` object keeps `artifactUri` pointed at the
manifest/checksum endpoint and adds `downloadUri`, `artifactSha256`,
`downloadSha256`, `checksumsSha256`, `settingsSha256`, `fixtureSuiteSha256`,
`runtime`, and `sidecarApi`. External automation should download the ZIP,
verify the package SHA against `artifactSha256`, unpack it in a clean directory,
verify `checksums.sha256.json`, run `npm run replay`, and then import
`dist/kernel.mjs`.

Strategy metadata and summary:

```http
GET /api/strategies/pym-v5
GET /api/strategies/pym-v5-option-rank-top8
GET /api/strategies/pym-v5-ml-two-speed-attention
GET /api/strategies/pym-v5-ml-calm-trend-router
GET /api/strategies/pym-v5-ml-option-top8-50-50
GET /api/strategies/pym-v5-two-speed-option-meta21
GET /api/strategies/pym-v5-spy-put-pressure-bil
GET /api/strategies/pym-v5-sleeve-meta-21d-cap25
GET /api/strategies/pym-v5-cap25-lgbm-blend40
GET /api/strategies/pym-v5-cap25-lgbm-blend40-stress
GET /api/strategies/tsll-seconds-passive-scalper
```

Chart data for a range:

```http
GET /api/strategies/pym-v5/chart?start=2026-01-01&end=2026-05-07
GET /api/strategies/pym-v5-option-rank-top8/chart?start=2025-01-01&end=2026-05-07
GET /api/strategies/pym-v5-ml-two-speed-attention/chart?start=2026-01-01&end=2026-05-07
GET /api/strategies/pym-v5-ml-calm-trend-router/chart?start=2026-01-01&end=2026-05-07
GET /api/strategies/pym-v5-ml-option-top8-50-50/chart?start=2026-01-01&end=2026-05-07
GET /api/strategies/pym-v5-two-speed-option-meta21/chart?start=2026-01-01&end=2026-05-07
GET /api/strategies/pym-v5-spy-put-pressure-bil/chart?start=2025-01-01&end=2026-05-07
GET /api/strategies/tsll-seconds-passive-scalper/chart?start=2026-02-02&end=2026-02-27
```

Daily values for a range:

```http
GET /api/strategies/pym-v5/values?start=2025-01-01&end=2026-05-07
```

Latest portfolio and change from previous EOD target:

```http
GET /api/strategies/pym-v5/portfolio/latest
GET /api/strategies/pym-v5-option-rank-top8/portfolio/latest
GET /api/strategies/pym-v5-ml-two-speed-attention/portfolio/latest
GET /api/strategies/pym-v5-ml-calm-trend-router/portfolio/latest
GET /api/strategies/pym-v5-ml-option-top8-50-50/portfolio/latest
GET /api/strategies/pym-v5-two-speed-option-meta21/portfolio/latest
GET /api/strategies/pym-v5-spy-put-pressure-bil/portfolio/latest
GET /api/strategies/tsll-seconds-passive-scalper/portfolio/latest
GET /api/strategies/pym-v5/changes/latest
```

Portfolio for a specific date:

```http
GET /api/strategies/pym-v5/portfolio/2026-05-07
GET /api/strategies/pym-v5-option-rank-top8/portfolio/2026-05-07
GET /api/strategies/pym-v5-ml-two-speed-attention/portfolio/2026-05-07
GET /api/strategies/pym-v5-ml-calm-trend-router/portfolio/2026-05-07
GET /api/strategies/pym-v5-ml-option-top8-50-50/portfolio/2026-05-07
GET /api/strategies/pym-v5-two-speed-option-meta21/portfolio/2026-05-07
GET /api/strategies/pym-v5-spy-put-pressure-bil/portfolio/2026-05-07
GET /api/strategies/tsll-seconds-passive-scalper/portfolio/2026-02-27
```

## Current Studies

- `pym-v5`: replicated Composer PYM V5 tree on Massive adjusted EOD bars.
- `pym-v5-option-rank-top8`: PYM V5 holdings filtered/ranked by Massive option
  aggregate flow using option overlay strategy `grid_pym_option_rank_top8_zm0p5`.
  It uses EOD PYM targets and day-X option-flow features, then realizes
  close-to-close returns into X+1.
- `pym-v5-ml-two-speed-attention`: artifact-backed daily walk-forward ML study
  for `two_speed_attention_pym_light_governed`.
- `pym-v5-ml-calm-trend-router`: artifact-backed router that holds raw
  two-speed ML except when prior labeled samples are at least `40`, stress is
  below `0.25`, SPY/QQQ 21-day returns are positive, and SPY 21-day volatility
  is below `22%`; in that calm-trend regime it holds `35%` raw ML and `65%`
  option top-8.
- `pym-v5-ml-option-top8-50-50`: artifact-backed 50/50 blend of
  `two_speed_attention_pym_light_governed` and option overlay
  `grid_pym_option_rank_top8_zm0p5`. It reads the latest local
  `pym-v5-two-speed-risk-overlays-*.json` artifact unless
  `PYM_V5_ML_RISK_OVERLAY_REPORT_PATH` is set.
- `pym-v5-two-speed-option-meta21`: artifact-backed selector study for
  `walkforward_lookback_best_of_two_speed_or_option_meta21`. It chooses among
  two-speed-vs-option lookback selectors using only prior 21 realized trading
  days.
- `pym-v5-spy-put-pressure-bil`: PYM V5 with SPY option put-pressure overlay
  `grid_pym_spy_put_z2p5_to_bil`. It holds the replicated PYM ETF portfolio
  unless SPY option put-pressure z-score is at least `2.5`, then rotates to
  `BIL` at EOD and realizes close-to-close returns into X+1.
- `pym-v5-sleeve-meta-21d-cap25`: reweights the eight base PYM V5
  sub-strategies daily by their trailing 21-day annualized Sharpe, with no
  floor and a `25%` per-sleeve cap so the portfolio always spreads across the
  strongest 4+ sleeves. Uses the same Composer tree and bars file as `pym-v5`.
  In-sample 2025-01-02 to 2026-05-08: roughly `+118%` vs base PYM `+81%` at
  `-11.20%` vs `-12.87%` max drawdown; OOS 2026-only delivers `+44%` vs base
  `+20%` at essentially equal drawdown (`-5.92%` vs `-5.99%`). A floor-based
  variant was tested first but the cap variant beat it on every dimension in
  both windows, so only the cap version is registered. See
  `projects/pym-v5-replication/docs/extension-strategies-research-notes.md`
  for the full research log.
- `pym-v5-cap25-lgbm-blend40`: blends the cap25 sleeve-meta target at 60%
  with a tightly-regularized daily walk-forward LightGBM model at 40%. The
  LGBM picks the top-5 PYM teacher candidates each day by predicted next-
  session return and equal-weights them; the model uses 20 trees, 3 leaves,
  `regLambda=5` so it can train on tiny daily samples without overfitting.
  Full window 2025-02-03 to 2026-05-08: `+154%` ret, `-9.37%` DD, Sharpe
  `3.35` vs cap25 alone `+107%`, `-11.20%`, Sharpe `2.86`. OOS 2026-only:
  `+61%`, `-3.27%`, Sharpe `5.66` vs cap25 alone `+44%`, `-5.92%`, Sharpe
  `4.21`. The LGBM artifact lives at
  `projects/pym-v5-ml-experiments/artifacts/pym-v5-daily-walkforward-lgbm-tiny-grid-*.json`
  and is regenerated via the `--lgbm-only` flag of
  `projects/pym-v5-ml-experiments/python/run_daily_walkforward.py`.
- `pym-v5-cap25-lgbm-blend40-stress`: same blend as
  `pym-v5-cap25-lgbm-blend40`, with an additional options-derived stress
  overlay that scales gross exposure down on high-stress days. Stress is a
  composite of VIXY 5d log-return z-score (always available),
  ^VIX z-score (when local Massive REST data is present, 2023-02+),
  and OCC equity put/call ratio z-score (when OCC files are present,
  2021-01+). When stress > 0 the gross is scaled smoothly; > 2σ caps at 20%
  gross with the slack in BIL. Over a 10-year backtest 2017-04 → 2026-05
  (cost 2 bps): cuts max drawdown from -21.2% to -12.2%, cuts vol from 26.0%
  to 18.8%, lifts Sharpe from 2.45 to 3.08, and modestly trades CAGR from
  82.7% to 75.3%. See
  `projects/pym-v5-ml-experiments/docs/options-stress-overlay-research-notes.md`.
- `tsll-seconds-passive-scalper`: TSLL intraday seconds-bar scalping tracker.
  It buys 3 cents below the prior completed 1-second close, targets +3 cents,
  stops at 5 cents, exits after 10 seconds, and reports daily P/L from the
  committed February 2026 Massive tick-trade artifact.

Detailed per-strategy architecture docs live under `docs/strategies/`.

Recompute in memory from mounted runtime data:

```http
POST /api/strategies/pym-v5/recompute
```

Refresh data and recompute (every registered strategy supports this; the
specific build steps depend on the strategy's data dependencies — see
`docs/per-strategy-data-needs.md`):

```http
POST /api/strategies/pym-v5/refresh-data
POST /api/strategies/pym-v5-cap25-lgbm-blend40-stress/refresh-data
... (any registered strategyId)
```

Refresh ALL strategies in the background (returns 202 immediately;
non-blocking; each strategy's refresh runs in parallel as a background
sequence of steps):

```http
POST /api/refresh-all
```

Poll progress of all in-flight or last-completed refreshes:

```http
GET /api/refresh-status
```

## Daily Fast Refresh

Use the fast refresh command for the normal after-EOD daily strategy run:

```bash
npm run strategy-service:refresh-daily-fast
```

This is the default operational path for daily updates. It is not a full
historical backtest; it refreshes only missing Massive-derived inputs, appends
ML, TSLL, and wheel days from prior same-code artifacts when checkpoints are
available, rebuilds the registered strategy snapshot, and persists normalized
daily P/L, holdings, and intraday trades to SQLite for later analysis.

Main local outputs:

- `artifacts/strategy-service/strategy-service-refresh-YYYY-MM-DD.json`
- `artifacts/strategy-service/strategy-service-refresh-YYYY-MM-DD.md`
- `apps/strategy-service/runtime/strategy-results.sqlite`
- Updated project artifacts under the relevant `projects/**/artifacts` or
  `projects/**/reports` folder.

Run a full backtest or force the underlying refresh script instead when
strategy code, costs/slippage, model settings, or historical Massive inputs
changed. The fast path is intended to make the next EOD run quick while
preserving the same result as a full replay for unchanged prior artifacts. The
wheel artifact stores `wheel-backtest-checkpoint.v1`; the fast path passes
`--no-full-rebuild` so a missing checkpoint is reported instead of accidentally
launching the slow replay.

## Adding Strategies

Implement an adapter with:

- `getMetadata()`
- `getReport()`
- `recompute()`
- `refreshData()` — every registered strategy now provides this, even if
  it's a no-op for static-artifact strategies. Use the helpers in
  `src/strategies/refresh-helpers.js` (`runRefreshSequence` for live-data
  strategies, `noopRefresh` for artifact-only).

`getMetadata()` must include an `execution` object. Use
`dailyEodExecution()` for daily EOD rebalances and `regularSessionExecution()`
for intraday/scalping strategies so the field stays consistent across the
registry.

For promoted paper/live strategies, add or update the manifest definition in
`src/strategies/execution.js` and call `executionSummaryForStrategy(strategyId)`
from `getMetadata()`. Do not duplicate timing/status/idempotency literals in the
adapter.

Then register it in `apps/strategy-service/src/default-registry.js`.

See `docs/per-strategy-data-needs.md` for a complete map of which
strategies need which data sources and what each refresh sequence does.
