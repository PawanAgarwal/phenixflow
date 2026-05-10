# Per-Strategy Data Needs and Refresh Flows

This document lists, for each registered strategy in the strategy service,
exactly which data sources it depends on, how those data sources are
refreshed, and what `POST /api/strategies/{id}/refresh-data` does for that
strategy.

The `refreshData()` endpoint is a non-blocking trigger — it spawns the
appropriate build scripts as a background sequence and returns 202 with a
status object. Poll `GET /api/refresh-status` to track progress (current
step, completed steps, error). Refresh state survives until the next
refresh is triggered.

## Quick reference: refresh categories

| Category | Strategies | What refresh does | Wall time |
|---|---|---|---|
| **EOD-bars only** | `pym-v5`, `pym-v5-sleeve-meta-21d-cap25`, `pym-v5-cap25-lgbm-blend40` | Fetch latest Massive EOD bars, then recompute | ~30-60s |
| **EOD-bars + stress** | `pym-v5-cap25-lgbm-blend40-stress` | Above + rebuild VIX/OCC stress signal | ~60-90s |
| **EOD-bars + option features** | `pym-v5-option-rank-top8`, `pym-v5-spy-put-pressure-bil` | Fetch EOD bars + rebuild Massive option features for all PYM roots | ~5-10 min |
| **Artifact-only (noop)** | All `pym-v5-ml-*`, `option-income-wheel-trend-ivrv`, `tsll-seconds-passive-scalper` | Re-read pre-computed artifact + recompute (no external fetch) | <1s |

## Per-strategy detail

### `pym-v5` — Composer PYM V5

**Data dependencies:**
- **Score JSON** — Composer public symphony tree at
  `runtime/source/composer-XPGix2infTwwWMORgqmV-score.json`. Refreshed
  manually via `npm run pym-v5:fetch-sources` (not currently part of
  refreshData).
- **Massive EOD adjusted daily bars** for the 60 PYM tickers
  (SPY/QQQ/TQQQ/SOXL/etc., see [README.md](../../../projects/pym-v5-replication/README.md#massive-only-backtest-path)).
  File: `runtime/pym-v5-massive-eod-adjusted-daily-bars-{start}-{end}.jsonl`.

**Refresh sequence:**
1. `node projects/pym-v5-replication/scripts/build-massive-eod-daily-bars.js --fetch-start 2024-01-01`
2. Recompute: re-evaluate Composer tree on new bars, rebuild snapshots
   and equity series.

### `pym-v5-sleeve-meta-21d-cap25` — Sleeve-Meta Cap-25%

**Data dependencies:** Same as `pym-v5` (Composer score + EOD bars).

**Refresh sequence:** Same as `pym-v5` — build bars + recompute. The
sleeve-meta math (per-sleeve trailing Sharpe + 25% per-sleeve cap) runs
on the same bars file.

### `pym-v5-cap25-lgbm-blend40` — Cap25 + 40% LightGBM Blend

**Data dependencies:**
- Composer score + Massive EOD bars (same as cap25).
- **LightGBM walk-forward artifact** at
  `projects/pym-v5-ml-experiments/artifacts/pym-v5-daily-walkforward-lgbm-tiny-grid-*.json`.
  This is generated out-of-band by the Python walk-forward engine
  (`npm run pym-v5:ml-walkforward-lgbm`, ~10 min for one spec). Each
  artifact has fixed start/end dates (no incremental extension yet).

**Refresh sequence:**
1. Build EOD bars (as for `pym-v5`).
2. Recompute: re-blend cap25 weights with the existing LGBM holdings
   from the artifact.

**Important caveat:** in-API refresh updates today's realized
close-to-close return on signal dates the LGBM artifact already covers,
but it does NOT produce new ML predictions for new signal dates. To
extend the LGBM coverage to a fresh date, run
`npm run pym-v5:ml-walkforward-lgbm` manually (slow). The strategy
gracefully falls back to cap25-only weights on dates without LGBM
predictions.

### `pym-v5-cap25-lgbm-blend40-stress` — Above + Options Stress Overlay

**Data dependencies:**
- Composer score + EOD bars (same as cap25).
- LightGBM walk-forward artifact (same as blend40).
- **Options-stress signal** at
  `projects/pym-v5-ml-experiments/artifacts/options-stress-signal-*.jsonl`.
  Composite of:
  - VIXY 5-day log-return z-score (from EOD bars, always available 2017+)
  - ^VIX z-score (Massive REST `I:VIX` endpoint, available 2023-02-14+)
  - OCC equity put/call ratio z-score (local OCC EOD aggregate OI files
    at `/Volumes/SEC4TB/massive-data/occ/option_open_interest_eod/`,
    refreshed daily by the `openinterest-occ-eod-oi` Docker container,
    available 2021-01-04+)

**Refresh sequence:**
1. Build EOD bars.
2. Build stress signal (`npm run pym-v5:build-stress-signal` —
   re-fetches latest ^VIX, re-parses local OCC files, ~30s).
3. Recompute: re-blend cap25 + LGBM with the latest stress overlay.

**Same caveat as blend40** for LGBM artifact extension.

### `pym-v5-option-rank-top8` — PYM + Option-Flow Top-8 Overlay

**Data dependencies:**
- Composer score + EOD bars.
- **Massive option features** at
  `runtime/pym-v5-option-bar-features-{start}-{end}.jsonl`. Built from
  local `option_quotes_1m` aggregate option bars by
  `projects/pym-v5-replication/src/option-features.js`. Per-day, per-option-root
  features (premium, IV proxy, put/call ratio, etc.) for the PYM tradable
  universe.

**Refresh sequence:**
1. Build EOD bars.
2. Build option features (`build-option-features.js --start 2025-01-02`).
   This processes all OPRA option_quotes_1m files in the window — slower
   than EOD bars (~5-10 min for the full window).
3. Recompute: re-rank PYM holdings by option-flow momentum, keep top 8.

### `pym-v5-spy-put-pressure-bil` — PYM + SPY Put-Pressure Risk-Off

**Data dependencies:** Same as `pym-v5-option-rank-top8` (EOD bars +
option features).

**Refresh sequence:** Same as `pym-v5-option-rank-top8`. The SPY
put-pressure z-score is one of the option features, so the same
build-option-features step covers it.

### `pym-v5-ml-two-speed-attention` — Two-Speed Attention ML

**Data dependencies:**
- **ML walk-forward artifact** at
  `projects/pym-v5-ml-experiments/artifacts/pym-v5-daily-walkforward-micro-features-*.json`.
  Generated out-of-band by the Python walk-forward engine
  (`projects/pym-v5-ml-experiments/python/run_daily_walkforward.py`).
  Contains per-day predicted holdings for 16+ ML strategy variants.

**Refresh sequence:**
1. Re-read the artifact and recompute (noop refresh — no external fetch).

**To extend the artifact to today** (out-of-band):
1. Re-export feature dataset:
   `npm run pym-v5:ml-export-dataset -- --start 2025-01-02`
2. Run walk-forward Python: `node projects/pym-v5-ml-experiments/python/run_daily_walkforward.py ...`

### `pym-v5-ml-calm-trend-router` — Calm-Trend Router

**Data dependencies:**
- ML walk-forward artifact (same as two-speed-attention).
- **Risk overlay artifact** at
  `projects/pym-v5-ml-experiments/artifacts/pym-v5-two-speed-risk-overlays-*.json`.
  Pre-computed combinations of ML + option-overlay strategies.

**Refresh sequence:** Noop — re-read artifacts and recompute.

### `pym-v5-ml-option-top8-50-50` — 50/50 ML + Option Top-8 Blend

**Data dependencies:**
- Risk overlay artifact (same as calm-trend-router).

**Refresh sequence:** Noop — re-read artifact and recompute.

### `pym-v5-two-speed-option-meta21` — Two-Speed/Option Meta21 Selector

**Data dependencies:**
- ML walk-forward artifact + option overlay artifact at
  `projects/pym-v5-replication/artifacts/pym-v5-option-overlay-suite-grid-top8-zm0p5-*.json`.

**Refresh sequence:** Noop — re-read artifacts and recompute.

### `option-income-wheel-trend-ivrv` — Option Income Wheel

**Data dependencies:**
- Pre-computed wheel backtest artifact at
  `projects/spy-intraday-prediction/artifacts/wheel-expanded-backtest-*.json`.
  This is a frozen February 2026 backtest report.

**Refresh sequence:** Noop — re-read artifact and recompute.

### `tsll-seconds-passive-scalper` — TSLL Seconds Scalper

**Data dependencies:**
- Pre-computed seconds-bar scalping artifact at
  `projects/tsll-scalping/reports/tsll-seconds-passive-fixed-feb2026.json`.
  Frozen February 2026 report.

**Refresh sequence:** Noop — re-read artifact and recompute.

## API endpoints

```http
# Trigger refresh on a single strategy (returns 202 with status)
POST /api/strategies/{strategyId}/refresh-data

# Trigger refresh on every registered strategy in parallel (returns 202)
POST /api/refresh-all

# Poll progress of all in-flight or last-completed refreshes
GET /api/refresh-status
```

The `GET /api/refresh-status` response shape:

```json
{
  "data": [
    {
      "id": "pym-v5-cap25-lgbm-blend40-stress",
      "loadedAt": "2026-05-10T20:30:00.000Z",
      "refresh": {
        "running": false,
        "startedAt": "2026-05-10T20:28:34.000Z",
        "finishedAt": "2026-05-10T20:30:00.000Z",
        "exitCode": 0,
        "currentStep": null,
        "completedSteps": ["build-massive-eod-bars", "build-stress-signal"],
        "plannedSteps": ["build-massive-eod-bars", "build-stress-signal"],
        "error": null
      }
    }
  ]
}
```

## Operating notes

- `POST /api/refresh-all` is **non-blocking** — it triggers all refreshes
  and returns immediately. Each refresh runs in a background process. The
  ordering is "parallel" in the sense that they're all started together,
  but several share work (multiple strategies need EOD bars, etc.).
  Currently each strategy spawns its own bars-build subprocess, so
  there's redundant work — this can be deduplicated in a Phase 2 pass.
- The OCC OI Docker container (`openinterest-occ-eod-oi`) keeps OCC EOD
  files fresh on its own schedule. The stress-signal refresh just reads
  what's there.
- The Composer score JSON is currently NOT auto-refreshed. Run
  `npm run pym-v5:fetch-sources` if upstream changes the symphony tree.
- LGBM walk-forward artifact extension (~10 min per spec) is also
  manual. Future work: incremental walk-forward that only computes new
  signal dates would let `cap25-lgbm-blend40` and
  `cap25-lgbm-blend40-stress` truly pick up new ML predictions on each
  refresh.

## Daily auto-refresh (Phase 2 — not yet built)

To make the dashboard automatically reflect new EOD data each day:

1. Add a `launchd` plist (or systemd timer) that runs M-F at ~5:30 PM
   ET, after market close.
2. The script does
   `curl -X POST http://localhost:3120/api/refresh-all`.
3. Optional: wait + check `GET /api/refresh-status` until all strategies
   show `running: false`, then alert if any have `error != null`.

A reference launchd plist already exists for the separate PYM rebalance
server at `projects/pym-v5-replication/launchd/com.phenixflow.pym-v5-rebalance.plist`;
copy that pattern.
