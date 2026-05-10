# TSLL Scalping Research

Massive-only TSLL tick-scalping research. This project streams local Massive flat files, extracts
TSLL stock trades into compact 5-second bars, joins completed 1-minute SPY/QQQ/TSLA/TSLL market
bars, and adds completed-minute TSLL/TSLA option-flow proxies from Massive option bars/trades.

## Data

Canonical local roots:

- Historical Massive cache: `/Volumes/SEC4TB/massive-data/massive/`
- Calendar cache: `/Volumes/SEC4TB/massive-data/calendar/us-equities-options-calendar.json`

Datasets used:

- `stock_trades_all`: TSLL tick trades.
- `stock_quotes_1m`: SPY, QQQ, TSLA, TSLL 1-minute market context.
- `option_trades_all`: TSLL/TSLA option trade flow.
- `option_quotes_1m`: TSLL/TSLA option aggregate flow.

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

The first pass writes compact day caches under `runtime/`, so rerunning the strategy grid does not
rescan multi-GB raw trade files.

## Strategies Tested

- `tsla_lead_lag`: buy TSLL when TSLA/QQQ are pushing up and TSLL is lagging but turning.
- `dip_reversal_macro`: buy a TSLL dip only after short-term bounce plus non-negative market context.
- `vwap_snapback`: buy below intraday VWAP after a bounce with TSLA/QQQ confirmation.
- `micro_breakout`: buy a 60-second high breakout with TSLA/QQQ and volume confirmation.
- `range_fade`: buy near a 3-minute low after a bounce and non-crashing macro context.
- `option_flow_dip`: buy dips only when TSLL or TSLA option call/put imbalance is supportive.

Execution is long-only. Signals are evaluated on a completed 5-second bar and enter at the next
bar open. Stops and targets use 5-second high/low, and if both touch in the same bar the stop is
assumed first.

## Current Canary Read

The 2026-01-02 through 2026-01-09 canary found a possible `tsla_lead_lag` edge at 0.5 cents per
side, but it disappeared at 1.0 cent per side. Treat this as hypothesis generation, not a tradable
strategy yet.

Scale January 2025 onward only after a candidate survives:

- a realistic spread/slippage assumption,
- positive day and month breadth,
- out-of-sample windows,
- enough trades to avoid one-news-day luck,
- drawdown and turnover checks.
