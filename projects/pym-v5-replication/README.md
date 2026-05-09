# PYM V5 Replication

Massive-only replication workspace for Composer symphony `XPGix2infTwwWMORgqmV`,
`Eagle's Park your Money V5`, from the PYM V5 report.

## Source Notes

- Notion report: `PYM V5 report`
- Composer factsheet: `https://app.composer.trade/symphony/XPGix2infTwwWMORgqmV/factsheet`
- Composer public API source used for the rule tree:
  `https://backtest-api.composer.trade/api/v1/public/symphonies/XPGix2infTwwWMORgqmV/score`

The report describes a daily, equal-weighted fund-of-strategies using leveraged Nasdaq,
gold, emerging markets, defensive sectors, Treasury, and volatility ETF sleeves. The local
implementation evaluates the public Composer rule tree directly instead of hand-copying
the 2,000-node strategy.

## Massive-Only Backtest Path

1. Fetch source snapshots into runtime:
   `npm run pym-v5:fetch-sources`
2. Check local Massive stock flat-file coverage:
   `npm run pym-v5:coverage`
3. Build daily ETF bars from `stock_quotes_1m`:
   `npm run pym-v5:build-daily`
4. Run the causal close-to-close replication:
   `npm run pym-v5:backtest`

## Massive Adjusted EOD Path

Massive exposes downloadable stock day aggregates as `us_stocks_sip/day_aggs_v1`.
Those flat files are raw split-unadjusted aggregates, while Composer-like replication
needs adjusted daily bars. For that, use Massive REST daily aggregate bars with
`adjusted=true`:

1. Build adjusted Massive EOD bars with warmup:
   `MASSIVE_API_KEY=... npm run pym-v5:massive-eod-build -- --fetch-start 2024-01-01`
2. Run the same Composer tree against that Massive EOD file:
   `npm run pym-v5:backtest -- --label massive-eod-rsi-wilder --provider "Massive adjusted daily" --build-start 2024-01-01 --daily-bars projects/pym-v5-replication/runtime/pym-v5-massive-eod-adjusted-daily-bars-2024-01-01-2026-05-06.jsonl --rsi-mode wilder`
3. Compare against Composer:
   `npm run pym-v5:compare-composer -- --label massive-eod-rsi-wilder`

## Rebalance Service

Start the local dashboard and API:
`npm run pym-v5:service`

Default URL:
`http://localhost:3117`

The machine-local LaunchAgent lives at:
`~/Library/LaunchAgents/com.phenixflow.pym-v5-rebalance.plist`

A committed reference copy with debugging metadata is kept at:
`projects/pym-v5-replication/launchd/com.phenixflow.pym-v5-rebalance.plist`

Useful endpoints:

- `GET /api/rebalance/summary`
- `GET /api/rebalance/latest`
- `GET /api/rebalance/days`
- `GET /api/rebalance/days/:date`
- `POST /api/rebalance/recompute`
- `POST /api/rebalance/refresh-eod`

The service loads the latest `pym-v5-massive-eod-adjusted-daily-bars-*.jsonl`
runtime file, evaluates the Composer tree at each EOD close from January 2025
onward, and exposes the current post-close target composition.

## Intraday Research

Run the Massive-only intraday strategy suite:
`npm run pym-v5:intraday-suite -- --start 2025-01-02`

Run mark-to-mark decomposition for open/hourly windows:
`npm run pym-v5:intraday-mark-study -- --start 2025-01-02 --cost-bps 4`

The suite uses the prior EOD PYM target as the daily regime, streams Massive
`stock_quotes_1m` files one day at a time, and tests flat-overnight intraday
execution variants after transaction/slippage costs.

Corrected flat-overnight findings through `2026-05-07`:

- Full previous-EOD PYM basket intraday, 9:35-15:55, is positive at the `2 bps`
  turnover-cost model but much weaker than the EOD strategy: `+17.14%`.
- Top-weight variants are cleaner because they avoid dozens of tiny line-item
  trades:
  - top 3 weights, 9:35-15:55: `+22.11%`, about 6 trades/day.
  - top 3 weights, 9:35-14:30: `+27.80%`, about 6 trades/day.
  - top 5 weights, 9:35-14:30: `+20.22%`, about 10 trades/day.
- Mark decomposition shows the EOD signal is strongest from prior close to the
  next open/early session; independent hourly round trips are mostly too small
  after costs, and 14:30-15:55 is the weakest bucket.
- VWAP/momentum variants with frequent rebalance churn were not profitable in
  the first full-window tests.

These are research outputs, not live-trading recommendations. The next gate is
spread-aware cost modeling by ticker and out-of-sample parameter discipline.

## Option-Flow Research

Build daily Massive option-flow features:
`npm run pym-v5:build-option-features -- --start 2025-01-02 --end 2026-05-06`

Run the PYM/option overlay sweep:
`npm run pym-v5:option-overlay-suite -- --start 2025-01-02 --end 2026-05-06 --label grid-full`

The local Massive cache has OPRA option aggregate bars and raw trades, but no
historical Greeks or open-interest files. This suite therefore uses option-flow
and short-dated ATM-flow proxies rather than true gamma exposure or dealer GEX.

Full-window grid findings through `2026-05-06`, using the same `2 bps` turnover
cost model as the EOD replication:

- Base replicated PYM: `+81.88%`, max drawdown `-12.87%`, Sharpe `2.412`.
- SPY buy/hold: `+25.11%`; QQQ buy/hold: `+36.17%`.
- PYM holdings ranked by option momentum, top 8 with z >= `-0.5`:
  `+125.97%`, max drawdown `-14.61%`, Sharpe `2.139`.
- PYM holdings ranked by option momentum, top 10 with z >= `-0.5`:
  `+119.12%`, max drawdown `-16.71%`, Sharpe `2.107`.
- PYM with SPY put-pressure z >= `2.5` moved to BIL:
  `+91.02%`, max drawdown `-6.72%`, Sharpe `2.766`.

The option-rank winners are in-sample discoveries with higher turnover, so they
need walk-forward validation before being treated as real edge. The SPY put-
pressure overlay is more conservative: it improved total return, drawdown, and
Sharpe versus base PYM in the full-window sweep.

## Yahoo Adjusted EOD Diagnostic

This project also has an external-data diagnostic path for isolating adjusted EOD data
effects from Composer rule semantics:

1. Build adjusted Yahoo daily bars with warmup:
   `npm run pym-v5:yahoo-build-daily`
2. Run the same tree evaluator against that file:
   `npm run pym-v5:backtest -- --label yahoo-adjusted --provider "Yahoo Finance adjusted daily" --build-start 2024-01-01 --daily-bars projects/pym-v5-replication/runtime/pym-v5-yahoo-adjusted-daily-bars-2024-01-01-2026-05-06.jsonl`
3. Compare against Composer:
   `npm run pym-v5:compare-composer -- --label yahoo-adjusted`

Yahoo output is diagnostic only. The official repo protocol remains Massive-only unless
the project runbook is changed.

Current diagnostic finding:

- Yahoo adjusted EOD + simple rolling RSI: `+36.97%`.
- Yahoo adjusted EOD + Wilder RSI: `+89.36%`.
- Composer reference for the same `2025-01-02` to `2026-05-06` window: `+85.91%`.

This strongly indicates Composer's `relative-strength-index` behaves like Wilder RSI.
The remaining difference is small enough to be plausibly explained by Composer's exact
data vendor, fee/slippage implementation, and branch/tie edge cases.

Runtime outputs stay under `runtime/`; final reports stay under `artifacts/`.

## Timing Convention

The default local backtest is causal: rules are evaluated on the prior completed close,
then the resulting holdings earn the next close-to-close return. A `same_close` timing mode
exists only as a diagnostic approximation for platforms that report same-day close-based
backtests; it is not causal and should not be treated as a tradable result.

## Current Data Limitation

The local Massive `stock_quotes_1m` cache currently begins in January 2025, so long-window
indicators such as the 200-day SMA do not have pre-2025 warmup history. Early-2025 results
therefore use only the local history available at that point and should be read separately
from Composer's full-history 2008-2026 backtest.
