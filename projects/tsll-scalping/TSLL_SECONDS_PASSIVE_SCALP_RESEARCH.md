# TSLL Seconds Passive Scalp Research

Generated: 2026-05-10

## Strategy Now Tracked

`tsll-seconds-passive-scalper`

- Symbol: `TSLL`
- Data: Massive stock trades converted into 1-second OHLCV bars.
- Entry: place a buy limit 3 cents below the prior completed 1-second close.
- Exit: sell at +3 cents, stop at -5 cents, or exit after 10 seconds.
- Filters: regular session only, skip first 5 minutes and last 10 minutes, require at least 3 cents of prior 60-second range, require SPY/QQQ/TSLA 1-minute context not materially weak.
- Cost setting in the dashboard seed report: 0 explicit cents per side.

The dashboard strategy is an artifact-backed tracker registered in the strategy service as:

```http
GET /api/strategies/tsll-seconds-passive-scalper
```

It appears automatically in the studies dashboard strategy tabs.

## What Did Not Work

### Tick Quote Download

Goal: download/filter Massive `us_stocks_sip/quotes_v1` for TSLL only, then join TSLL quotes with TSLL trades.

Result:

- The Massive S3 signer and local key are valid.
- The same key returned `200` for `us_stocks_sip/trades_v1`.
- The quote dataset returned `403 NOT_AUTHORIZED` for `us_stocks_sip/quotes_v1` on both 2025 and 2026 dates.
- No alternate S3 access/secret pair was found in the sibling Massive project.

Conclusion: this is an entitlement issue for the stock SIP quote flat-file dataset, not missing credentials.

### Daily-Context 5-Second Long Scalps

Goal: use daily chart context, SPY/QQQ/TSLA moves, and 5-second TSLL bars to find higher-probability long scalps.

Result:

- The 2026-01-02 through 2026-01-09 no-option daily-grid canary was not profitable.
- Best daily-grid variant was `daily_intraday_momentum_scalp`: 108 trades, -25.17 cents/share at 0.5 cents per side.
- The older `tsla_lead_lag` baseline showed a possible edge at 0.5 cents per side but disappeared at 1.0 cent per side.

Conclusion: daily-context momentum/reversal filters were useful research scaffolding, but not enough to justify scale-up.

## What Worked

### Seconds Passive Limit Proxy

Because tick quotes were not available, the useful proxy was:

1. Convert TSLL tick trades into 1-second bars.
2. Place a passive-style buy limit below the prior completed second close.
3. Count the buy as filled only if the next bar trades down to that limit.
4. Exit at a fixed cent target, fixed stop, or short timeout.

Initial two-day screen on 2026-01-08 to 2026-01-09:

- 46,800 1-second bars.
- About 399k TSLL trades.
- Best fixed candidate: buy 3c below prior close, target +3c, stop 5c, max hold 10s.
- Zero explicit cost: +66.09 cents/share, 29 trades, +2.279 cents/trade.
- 0.5c/side sensitivity: +37.09 cents/share.
- 1.0c/side sensitivity: +8.09 cents/share.

February 2026 holdout using the fixed candidate, not re-optimized:

- Trading days: 19.
- Trades: 248.
- Positive days: 19/19 at zero explicit cost.
- Net P/L: +449.37 cents/share.
- Per 1,000 shares/trade: +$4,493.70.
- Avg P/L: +1.812 cents/trade.
- Win rate: 81.85%.
- Return on total buy turnover: 0.1155%.
- Return on same TSLL capital bucket recycled intraday: 28.64%.

0.5c/side hidden-cost sensitivity:

- Net P/L: +201.37 cents/share.
- Positive days: 16/19.
- Avg P/L: +0.812 cents/trade.
- Break-even hidden cost is about 0.906 cents per side.

## Daily February 2026 Zero-Cost P/L

| Date | Trades | P/L c/share | P/L per 1k shares | Recycled capital return |
| --- | ---: | ---: | ---: | ---: |
| 2026-02-02 | 13 | 22.74 | $227.40 | 1.41% |
| 2026-02-03 | 11 | 5.15 | $51.50 | 0.31% |
| 2026-02-04 | 17 | 24.02 | $240.20 | 1.50% |
| 2026-02-05 | 26 | 38.20 | $382.00 | 2.64% |
| 2026-02-06 | 22 | 30.14 | $301.40 | 1.99% |
| 2026-02-09 | 14 | 34.65 | $346.50 | 2.23% |
| 2026-02-10 | 7 | 10.28 | $102.80 | 0.63% |
| 2026-02-11 | 22 | 55.39 | $553.90 | 3.28% |
| 2026-02-12 | 19 | 46.24 | $462.40 | 2.76% |
| 2026-02-13 | 34 | 61.59 | $615.90 | 3.91% |
| 2026-02-17 | 17 | 39.61 | $396.10 | 2.62% |
| 2026-02-18 | 6 | 12.01 | $120.10 | 0.78% |
| 2026-02-19 | 5 | 1.76 | $17.60 | 0.12% |
| 2026-02-20 | 20 | 48.03 | $480.30 | 3.13% |
| 2026-02-23 | 1 | 3.00 | $30.00 | 0.20% |
| 2026-02-24 | 1 | 3.00 | $30.00 | 0.21% |
| 2026-02-25 | 6 | 0.71 | $7.10 | 0.04% |
| 2026-02-26 | 5 | 6.85 | $68.50 | 0.45% |
| 2026-02-27 | 2 | 6.00 | $60.00 | 0.41% |

## Caveats

- Seconds bars can prove that a price traded, but not that our passive order had queue priority.
- The backtest cannot yet model real bid/ask spread, order book depth, partial fills, maker rebates, cancel latency, or adverse selection.
- Same-second target and stop are resolved conservatively as stop first.
- The next scale-up should run March/April and then January 2025 onward before treating this as a live candidate.

## Repro Commands

```bash
npm run tsll-scalping:seconds-mm -- --start-date 2026-02-01 --end-date 2026-02-28 --fixed-candidate --no-daily-context --cost-cents-per-side 0 --min-trades 0 --output projects/tsll-scalping/artifacts/tsll-seconds-passive-mm-fixed-feb2026-cost0
npm run tsll-scalping:export-seconds-report -- --source projects/tsll-scalping/artifacts/tsll-seconds-passive-mm-fixed-feb2026-cost0.json --output projects/tsll-scalping/reports/tsll-seconds-passive-fixed-feb2026.json
```
