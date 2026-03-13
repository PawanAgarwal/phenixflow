# Theta Greeks Input Sources (IV/Delta and Local Reconstruction)

## Purpose
Define which inputs are required to compute IV/Greeks locally, where those inputs come from today, and which inputs still need explicit sources.

## Required Inputs

For Black-Scholes-style first-order Greeks we need:
- Option price (or IV directly)
- Underlying spot `S`
- Strike `K`
- Expiration/time-to-expiry `T`
- Option side (call/put)
- Risk-free rate `r`
- Dividend input `q` (or annual dividend converted to yield convention)

## Source Coverage Matrix

| Input | Current source in PhenixFlow | Coverage status | Theta docs notes |
|---|---|---|---|
| Option price (mid) | `options.option_quote_minute_raw` (`bid`,`ask`,`last`) | Available | Greeks history endpoints are midpoint-based by default for option/underlying context. |
| Trade price | `options.option_trades` / trade_quote stream | Available | Trade Greeks endpoints are trade-context (trade + underlying quote). |
| Underlying spot `S` | `options.stock_ohlc_minute_raw` and `underlying_price` in greek rows | Available | Greeks endpoints also support `stock_price`/`underlying_timestamp` overrides when needed. |
| Strike `K`, right | Option contract fields in raw quote/trade/greek rows | Available | Native in all option endpoints. |
| Expiration and `T` | `expiration` + `minute_bucket_utc` timestamps | Available | `T` derived locally from timestamp/expiry convention. |
| Implied vol `sigma` | `options.option_greeks_minute_raw.implied_vol` or Theta IV endpoint | Available | IV endpoint returns implied vol only. |
| Risk-free rate `r` | **No persisted table yet**; now configurable via env (`THETADATA_GREEKS_RATE_TYPE`, `THETADATA_GREEKS_RATE_VALUE`) | Partially available (config-driven) | Theta supports `rate_type`/`rate_value` inputs on Greeks history endpoints. |
| Dividend input | **No persisted table yet**; now configurable via env (`THETADATA_GREEKS_ANNUAL_DIVIDEND`, `THETADATA_GREEKS_DIVIDEND_OVERRIDES`) | Partially available (config-driven) | Theta defaults to no dividends unless `annual_dividend` is supplied. |
| Trade IV feed | `trade_greeks/implied_volatility` endpoint | Blocked by entitlement on current account | Trade Greeks endpoints require Professional subscription. |

## What Was Missing (Before This Update)

1. Explicit configurable source for `r` in historical Greeks requests.
2. Explicit configurable source for dividend assumptions.
3. A single place documenting which inputs are hard data vs modeling assumptions.

## Prepared Data Sources (Implemented)

`src/historical-flow.js` now passes optional Greek model inputs to Theta Greeks history endpoints:
- `THETADATA_GREEKS_RATE_TYPE` (default `sofr`)
- `THETADATA_GREEKS_RATE_VALUE`
- `THETADATA_GREEKS_ANNUAL_DIVIDEND`
- `THETADATA_GREEKS_DIVIDEND_OVERRIDES` (symbol-level precedence)
- `THETADATA_GREEKS_VERSION`

This makes `r` and dividend assumptions explicit and reproducible in runtime config, even without separate local factor tables.

## Recommended Next Step (Optional)

If you want deterministic backtests independent of Theta defaults:
1. Persist a daily `risk_free_curve` table (keyed by date + tenor or flat daily rate).
2. Persist a `symbol_dividend_assumptions` table (effective-date based).
3. Stamp selected `r/q/version` assumptions into run metadata per backfill wave.

## Implemented SOFR Source (Fed / NY Fed)

Daily SOFR ingestion is now available via:

```bash
npm run clickhouse:sofr:sync
```

This command:
1. Creates/uses `options.reference_sofr_daily`.
2. Pulls data from New York Fed public API (`markets.newyorkfed.org`).
3. Loads the last 3 years on first run.
4. Runs in refresh mode by default (incremental with overlap for revision safety) on subsequent runs.

Manual controls:

```bash
# Explicit range
node scripts/clickhouse/sync-sofr-daily.js --from 2023-01-01 --to 2026-03-12

# Dry run
node scripts/clickhouse/sync-sofr-daily.js --dry-run 1
```

Daily refresh (example `cron`):

```bash
15 18 * * 1-5 cd /Users/pawanagarwal/github/phenixflow && npm run -s clickhouse:sofr:sync >> artifacts/reports/sofr-sync.log 2>&1
```
