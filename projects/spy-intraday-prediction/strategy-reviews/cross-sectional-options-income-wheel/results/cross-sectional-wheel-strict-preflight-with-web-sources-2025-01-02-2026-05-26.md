# Cross-Sectional Options-Income Wheel Strict Preflight

Generated: 2026-05-27T06:35:14.581Z
Status: FAIL
Window: 2025-01-02 through 2026-05-26 (349 open days)
Initial capital: $100,000

## Base Strategy

- Universe: full point-in-time S&P 500 membership only.
- Entry: daily close scan; sell 21-trading-day puts when Wilder RSI(14) < 30.
- Put strike: 95.00% of spot; call strike: 95.00% of spot.
- Risk: at most 5 committed names and 30.00% of equity per position.
- First variant: when prior-day VIX > 22, sell ITM puts at 105.00% of spot.

## Gate Results

### Blocking Errors

- missing_executable_option_pricing_source: No executable option pricing source was supplied. Massive option_quotes_1m aggregate bars are not bid/ask quotes and are not accepted for the strict baseline.

### Warnings

- history_short_of_5y_target: Available/requested history is shorter than the target 5 years; report it as limited history, not a durable edge.

## Data Sources

- Stock bars: stock_quotes_1m
- Local option aggregate bars: option_quotes_1m (not accepted as strict bid/ask fills)
- Point-in-time membership: /Users/pawanagarwal/github/phenixflow/projects/spy-intraday-prediction/runtime/cross-sectional-wheel-sources/sp500-historical-components-fja05680-2026-01-17.csv
- Option pricing mode: missing
- Dividends: /Users/pawanagarwal/github/phenixflow/projects/spy-intraday-prediction/runtime/cross-sectional-wheel-sources/massive-dividends-sp500pit-2025-01-02-2026-05-26.csv
- Risk-free: /Users/pawanagarwal/github/phenixflow/projects/spy-intraday-prediction/runtime/cross-sectional-wheel-sources/risk-free-dgs3mo-fred.csv

## Experiment Knobs

- putMoneyness: base=0.95 values=[0.9,0.95,1,1.05]
- callMoneyness: base=0.95 values=[0.9,0.95,1,1.05]
- putRsiMax: base=30 values=[20,25,30,35,40]
- callRsiMin: base=70 values=[60,65,70,75,80]
- expiryTradingDays: base=21 values=[10,15,21,30,45]
- maxCommittedPositions: base=5 values=[3,5,8,10]
- maxPositionPct: base=0.3 values=[0.1,0.2,0.3]
- universe: base="point_in_time_sp500" values=["point_in_time_sp500"]
- vixOverlay: base=null values=[null,{"threshold":22,"calmPutMoneyness":0.95,"highVixPutMoneyness":1.05}]

This preflight intentionally fails before any backtest result can be headlined if survivorship-free membership, executable option pricing, dividends, or risk-free inputs are missing.

