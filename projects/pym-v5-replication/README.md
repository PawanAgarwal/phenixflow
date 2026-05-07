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
