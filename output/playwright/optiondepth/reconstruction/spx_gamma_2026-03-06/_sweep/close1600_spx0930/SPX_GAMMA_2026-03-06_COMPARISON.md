# SPX Gamma Reconstruction vs OptionDepth (2026-03-06)

- Best model: `dealer_short_all_options|strike=le_300|dte=gt_365`
- Surface correlation: `0.5596`
- Surface NRMSE after affine fit: `0.8288`
- Peak line MAE: `64.40` points
- Trough line MAE: `130.85` points
- Zero line MAE: `44.45` points
- ThetaData OHLC matched bars: `391`
- ThetaData OHLC close MAE vs OptionDepth: `0.0049`

## Build Inputs

- OptionDepth target chart: `raw/optiondepth/od_chart_props_2026-03-06.json`
- ThetaData SPXW greeks: `raw/thetadata/spxw_greeks_eod_20260305.json`
- ThetaData SPX greeks: `raw/thetadata/spx_greeks_eod_20260305.json`
- ThetaData SPXW OI: `raw/thetadata/spxw_oi_20260306.json`
- ThetaData SPX OI: `raw/thetadata/spx_oi_20260306.json`
- ThetaData SPX index OHLC: `raw/thetadata/spx_index_ohlc_1m_20260306.json`

## Proxy Method

- Merge ThetaData `EOD` greeks from `2026-03-05` with open interest reported on `2026-03-06`.
- Reprice each contract on the OptionDepth price grid with a Black-Scholes-style gamma formula using the contract's implied volatility and a drift term inferred from ThetaData `d1` / `d2`.
- Aggregate to market-level gamma with two sign conventions: public `call + / put -` and dealer-short `all negative`.
- Fit a simple affine transform to align unit scale before comparing to OptionDepth.

## Read

- This is a close public proxy, not an exact clone of OptionDepth's proprietary inventory model.
- The best match here should be interpreted as 'how close we can get with public OI + ThetaData greeks + a transparent sign convention', not as proof of OptionDepth's internal method.
