# SPX Gamma Reconstruction vs OptionDepth (2026-03-06)

- Best model: `dealer_short_all_options|strike=101_150|dte=gt_180`
- Surface correlation: `0.7325`
- Surface NRMSE after affine fit: `0.6808`
- Peak line MAE: `64.51` points
- Trough line MAE: `108.90` points
- Zero line MAE: `41.92` points
- ThetaData OHLC matched bars: `391`
- ThetaData OHLC close MAE vs OptionDepth: `0.0049`

## Build Inputs

- OptionDepth target chart: `raw/optiondepth/od_chart_props_2026-03-06.json`
- ThetaData SPXW greeks: `raw/thetadata/spxw_greeks_eod_20260305.json`
- ThetaData SPX greeks: `raw/thetadata/spx_greeks_eod_20260305.json`
- ThetaData SPXW OI: `raw/thetadata/spxw_oi_20260306.json`
- ThetaData SPX OI: `raw/thetadata/spx_oi_20260306.json`
- ThetaData SPX index OHLC: `raw/thetadata/spx_index_ohlc_1m_20260306.json`
- Rendered side-by-side HTML: `reconstruction/spx_gamma_2026-03-06/default/gamma_compare.html`
- Rendered screenshot: `reconstruction/spx_gamma_2026-03-06/default/gamma_compare_screenshot.png`

## Proxy Method

- Merge ThetaData `EOD` greeks from `2026-03-05` with open interest reported on `2026-03-06`.
- Reprice each contract on the OptionDepth price grid with a Black-Scholes-style gamma formula using the contract's implied volatility and a drift term inferred from ThetaData `d1` / `d2`.
- Aggregate to market-level gamma with two sign conventions: public `call + / put -` and dealer-short `all negative`.
- Fit a simple affine transform to align unit scale before comparing to OptionDepth.

## What Matched Best

- The best surface came from the `dealer-short-all-options` sign convention, which is effectively "market makers are short the customer book".
- The best strike scope was only the contracts within roughly `150` points of spot. Wider strike buckets reduced the match.
- Including longer-dated expirations still helped slightly, but the fit was dominated by the near-spot contracts rather than the full far-wing chain.

## What This Suggests

- A plain ThetaData + OI reconstruction can recover a meaningful amount of the same structure OptionDepth shows, especially the broad location of the gamma ridge and the sign/regime changes around spot.
- The remaining gap is still large enough that OptionDepth is probably not just plotting raw public OI * Black-Scholes gamma. Their internal inventory/sign model is likely adding non-public assumptions or flow-based netting.
- The strong improvement from narrowing the strike scope suggests OptionDepth's displayed gamma map is driven heavily by the contracts closest to spot, even if their proprietary model uses the wider chain in the background.

## Where The Proxy Still Misses

- The peak and trough paths are still off by a lot in absolute price terms, especially the trough line.
- The zero-gamma line is directionally usable but still misses by about `42` SPX points on average.
- ThetaData minute `close` values are almost identical to OptionDepth's OHLC overlay, but the opening bar differs materially. That points to a source or bar-construction mismatch at the open rather than a broad data-feed problem.

## Read

- This is a close public proxy, not an exact clone of OptionDepth's proprietary inventory model.
- The best match here should be interpreted as 'how close we can get with public OI + ThetaData greeks + a transparent sign convention', not as proof of OptionDepth's internal method.
