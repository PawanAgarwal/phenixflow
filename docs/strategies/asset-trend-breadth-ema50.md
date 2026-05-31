# `asset-trend-breadth-ema50`

## Purpose

`asset-trend-breadth-ema50` is a cross-asset trend-following portfolio that holds the
strongest-trending asset classes and scales total exposure down — toward cash — when the
*breadth* of the whole asset universe deteriorates. It is built to beat SPY's Sharpe ratio
with materially smaller drawdowns.

## Plain-English Rule

Selection (monthly, last trading day):

```text
universe = ~245 cross-asset ETFs/proxies (research/asset-universe)
eligible = assets with positive 12-month (252d) total return AND price > 200d SMA
book     = top 20 eligible by 12-month momentum, inverse-volatility weighted (30% cap)
           (if fewer than 20 qualify, the shortfall stays in cash)
```

Exposure gate (daily, EOD):

```text
breadth  = % of the universe trading above its own EMA-50
exposure = clip((breadth - 0.18) / (0.50 - 0.18), 0, 1)
           -> 0% (full cash) when breadth <= 18%, 100% when breadth >= 50%, linear between
target   = book * exposure   (remainder held in cash / BIL)
```

The book changes monthly; the exposure gate updates every day, so the strategy de-risks
between rebalances when broad market breadth weakens.

## Results (net of 5 bps, daily basis, risk-free = cash)

| Window | Sharpe | CAGR | MaxDD | SPY Sharpe |
|--------|--------|------|-------|------------|
| OOS 2019-07 → 2026-05 | **1.10** | ~20% | -17% | 0.72 |
| Full 2017-07 → 2026-05 | 0.94 | ~16% | -17% | 0.72 |
| 2022-2023 (bear) | ~0.8 | — | -12% | 0.02 |

## Verification

- **Two independent engines reconcile** to ~3e-4 on Sharpe/CAGR/MaxDD
  (`research/asset-trend-strategy/verify_breadth_ema50.py`): the fast analytic engine and a
  fully explicit from-scratch daily loop.
- **No lookahead:** the honest 1-day-lagged result (decide at EOD, trade next close) is
  Sharpe 1.10; the illegal same-day version is 2.11 — confirming the lag is respected.
- **Walk-forward** (pick the breadth ramp + EMA length from prior years only): OOS Sharpe
  0.99 vs no-gate base 0.97, SPY 0.68, with a tight parameter-stability grid (1.05–1.11).

## Service Adapter

- Adapter: `apps/strategy-service/src/strategies/breadth-ema50.js`
- Registry id: `asset-trend-breadth-ema50`
- Family: `cross-asset-momentum`
- Cadence: `daily_eod` (after market close, 16:10 ET) — `research_only`, not paper/live promoted.
- Report artifact: `projects/asset-trend-breadth/artifacts/breadth-ema50-report.json`
- Artifact generator: `research/asset-trend-strategy/export_artifact.py`

## Inputs

- Yahoo Finance daily adjusted close for the `research/asset-universe` tickers (~245 names)
  plus SPY (benchmark) and BIL (cash/risk-free).

## Timing

```text
day X close: rank 12m momentum (monthly) + compute breadth EMA-50 exposure using data through X
day X close (EOD+10m): set book * exposure target
day X+1 close: realize close-to-close return
```

## API Surface

- `GET /api/strategies/asset-trend-breadth-ema50`
- `GET /api/strategies/asset-trend-breadth-ema50/chart`
- `GET /api/strategies/asset-trend-breadth-ema50/values`
- `GET /api/strategies/asset-trend-breadth-ema50/open-positions`
- `GET /api/strategies/asset-trend-breadth-ema50/portfolio/latest`
- `GET /api/strategies/asset-trend-breadth-ema50/portfolio/:date`
- `POST /api/strategies/asset-trend-breadth-ema50/recompute`

## Caveats

- Single market regime (2017–2026, US-equity-heavy); many overlay/mechanism variants were
  tested, so the headline OOS 1.10 carries some selection optimism — the realistic
  walk-forward edge over the ungated book is modest on Sharpe (its main benefit is drawdown
  reduction and a large edge over SPY).
- The book is a concentrated commodity / metals / energy-transition / semis tilt and can hold
  leveraged or thin thematic sleeves (inverse-vol weighting keeps each small); modeled cost is
  a flat 5 bps one-way and does not include wider after-hours spreads on thin names.
- Artifact is generated from Yahoo data and is point-in-time research output, not trade advice.
