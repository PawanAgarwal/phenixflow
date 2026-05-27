# Sources

This strategy review used local Massive data plus downloaded public/vendor reference files.

## Local Massive Datasets

Canonical roots from the repo runbook:

- Historical Massive cache: `/Volumes/SEC4TB/massive-data/massive/`
- Intraday/live Massive parquet: `/Volumes/SEC4TB/massive-data/massive-live/parquet/massive/`
- Calendar cache: `/Volumes/SEC4TB/massive-data/calendar/us-equities-options-calendar.json`

Datasets used by the proxy run:

- `stock_quotes_1m`
- `option_quotes_1m`
- `indices_1m`

Important pricing caveat: `option_quotes_1m` is an aggregate bar dataset with `open/high/low/close/volume/transactions`. It is not executable bid/ask quote data.

## Downloaded Runtime Inputs

These are ignored under `runtime/` but are documented here for reproducibility:

- `projects/spy-intraday-prediction/runtime/cross-sectional-wheel-sources/sp500-historical-components-fja05680-2026-01-17.csv`
- `projects/spy-intraday-prediction/runtime/cross-sectional-wheel-sources/sp500-pit-union-2025-01-02-2026-05-26.json`
- `projects/spy-intraday-prediction/runtime/cross-sectional-wheel-sources/risk-free-dgs3mo-fred.csv`
- `projects/spy-intraday-prediction/runtime/cross-sectional-wheel-sources/massive-dividends-sp500pit-2025-01-02-2026-05-26.csv`
- `projects/spy-intraday-prediction/runtime/cross-sectional-wheel-sources/massive-dividends-sp500pit-2025-01-02-2026-05-26-summary.json`

Source URLs:

- S&P 500 historical membership proxy: https://github.com/fja05680/sp500
- FRED DGS3MO CSV: https://fred.stlouisfed.org/graph/fredgraph.csv?id=DGS3MO
- FRED DGS3MO series page: https://fred.stlouisfed.org/series/DGS3MO
- Massive dividends endpoint docs: https://massive.com/docs/rest/stocks/corporate-actions/dividends
- Massive option quote endpoint docs: https://massive.com/docs/rest/options/trades-quotes/quotes
- Massive options flat-file docs: https://massive.com/docs/flat-files/options/overview

## Entitlement Checks

Local Massive credentials were present, but option quote access was not entitled:

- REST `/v3/quotes/{optionsTicker}` returned `NOT_AUTHORIZED`.
- S3 quote-prefix probes returned `403`.
- S3 `us_options_opra/trades_v1`, `minute_aggs_v1`, and `day_aggs_v1` were accessible.

Because of this, the strict preflight still fails on `missing_executable_option_pricing_source`, while the proxy run uses aggregate option price as a mid-price approximation with a 5% haircut.
