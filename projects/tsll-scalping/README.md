# TSLL Scalping Research

Massive-only TSLL tick-scalping research. This project streams local Massive flat files, extracts
TSLL stock trades into compact 5-second bars, joins completed 1-minute SPY/QQQ/TSLA/TSLL market
bars, and adds causal prior-day daily-chart context. Option features are disabled by default.

## Data

Canonical local roots:

- Historical Massive cache: `/Volumes/SEC4TB/massive-data/massive/`
- Calendar cache: `/Volumes/SEC4TB/massive-data/calendar/us-equities-options-calendar.json`

Datasets used:

- `stock_trades_all`: TSLL tick trades.
- `stock_quotes_1m`: SPY, QQQ, TSLA, TSLL 1-minute market context.
- Upstream tick stock quotes, if needed later: Massive flat-file `us_stocks_sip/quotes_v1`.

No legacy database tables are used.

## Canary

Run a short canary:

```bash
npm run tsll-scalping:canary -- --start-date 2026-01-02 --end-date 2026-01-09 --min-trades 25
```

Run a stricter cost check:

```bash
npm run tsll-scalping:canary -- --start-date 2026-01-02 --end-date 2026-01-09 --min-trades 25 --cost-cents-per-side 1
```

Run the same TSLL tests over the configured extended-hours window, 04:00-20:00 ET:

```bash
npm run tsll-scalping:seconds-mm -- --start-date 2026-05-27 --end-date 2026-05-29 --fixed-candidate --rest-seconds --no-daily-context --session extended
```

Run the seconds-scalp candidate-improvement screen over that same extended-hours window:

```bash
node projects/tsll-scalping/scripts/analyze-scalp-improvements.js --start 2025-01-02 --end 2026-05-29 --session extended
```

Compare against older no-option baseline entries:

```bash
npm run tsll-scalping:canary -- --start-date 2026-01-02 --end-date 2026-01-09 --min-trades 25 --include-baseline-strategies
```

The first pass writes compact day caches under `runtime/`, so rerunning the strategy grid does not
rescan multi-GB raw trade files.

## Tick Quote Passive Market Making

Filter Massive stock SIP quote flat files to TSLL only without changing the shared Massive cache:

```bash
npm run tsll-scalping:download-quotes -- --date 2026-01-09
```

Probe access without downloading the multi-GB object:

```bash
npm run tsll-scalping:download-quotes -- --date 2026-01-09 --probe-only --dataset us_stocks_sip/quotes_v1
```

Run the passive bid-to-ask simulator on the filtered quote file joined to local TSLL trades:

```bash
npm run tsll-scalping:passive-mm -- --date 2026-01-09 --min-trades 10
```

The simulator tests touch-fill, latency, and trade-through variants. Touch fills are optimistic
because SIP top-of-book data cannot prove queue priority; latency and trade-through variants are
stricter approximations.

If tick quote access is not available, run the seconds proxy from local TSLL trades:

```bash
npm run tsll-scalping:seconds-mm -- --start-date 2026-01-08 --end-date 2026-01-09 --min-trades 20
```

This derives 1-second OHLCV bars from tick trades, places buy limits below the prior completed
second close, and tries to sell a fixed number of cents higher. It is useful for screening the
volatility-capture idea, but it cannot validate actual bid/ask queue fills.

## Strategies Tested

- `daily_trend_pullback`: buy VWAP pullbacks only when daily TSLL volatility and TSLA/QQQ/SPY
  regime filters are supportive.
- `daily_orb_breakout`: buy 5-minute or 15-minute opening-range breakouts with daily context,
  VWAP confirmation, and optional prior-day NR7 filtering.
- `daily_atr_reversal`: buy downside intraday ATR exhaustion only after a short-term bounce and
  non-crashing SPY/QQQ context.
- `daily_intraday_momentum_scalp`: adapt the first-half-hour intraday momentum idea to TSLL
  scalps after a strong opening move.
- Optional baseline comparison: `tsla_lead_lag`, `dip_reversal_macro`, `vwap_snapback`,
  `micro_breakout`, and `range_fade`.

Execution is long-only. Signals are evaluated on a completed 5-second bar and enter at the next
bar open. Stops and targets use 5-second high/low, and if both touch in the same bar the stop is
assumed first.

## Research Leads

The daily-context variants are based on:

- Gao, Han, Li, and Zhou, "Market Intraday Momentum", Journal of Financial Economics, 2018:
  first-half-hour market return predicts the last-half-hour return in liquid ETFs.
- Opening Range Breakout research/practice: use 5/15/30 minute opening ranges, volume confirmation,
  and strict transaction-cost checks.
- VWAP research/practice: use VWAP as a fair-value anchor, but separate trend and mean-reversion
  regimes rather than fading every deviation.

## Current Canary Read

The 2026-01-02 through 2026-01-09 no-option daily-grid canary was not profitable. The best daily
variant was `daily_intraday_momentum_scalp`, with 108 trades and -25.17 cents/share at 0.5 cents
per side. Adding the older no-option baseline strategies reproduced a possible `tsla_lead_lag`
edge at 0.5 cents per side, but it disappeared at 1.0 cent per side. Treat this as hypothesis
generation, not a tradable strategy yet.

Scale January 2025 onward only after a candidate survives:

- a realistic spread/slippage assumption,
- positive day and month breadth,
- out-of-sample windows,
- enough trades to avoid one-news-day luck,
- drawdown and turnover checks.
