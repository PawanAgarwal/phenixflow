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

Strategy metadata and summary:

```http
GET /api/strategies/pym-v5
GET /api/strategies/pym-v5-option-rank-top8
GET /api/strategies/pym-v5-ml-two-speed-attention
GET /api/strategies/pym-v5-ml-calm-trend-router
GET /api/strategies/pym-v5-ml-option-top8-50-50
GET /api/strategies/pym-v5-two-speed-option-meta21
GET /api/strategies/pym-v5-spy-put-pressure-bil
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
- `tsll-seconds-passive-scalper`: TSLL intraday seconds-bar scalping tracker.
  It buys 3 cents below the prior completed 1-second close, targets +3 cents,
  stops at 5 cents, exits after 10 seconds, and reports daily P/L from the
  committed February 2026 Massive tick-trade artifact.

Detailed per-strategy architecture docs live under `docs/strategies/`.

Recompute in memory from mounted runtime data:

```http
POST /api/strategies/pym-v5/recompute
```

Refresh Massive adjusted EOD data, then recompute:

```http
POST /api/strategies/pym-v5/refresh-data
```

## Adding Strategies

Implement an adapter with:

- `getMetadata()`
- `getReport()`
- `recompute()`
- optional `refreshData()`

Then register it in `apps/strategy-service/src/default-registry.js`.
