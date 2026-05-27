# Cross-Sectional Options-Income Wheel Review Bundle

This folder packages the strategy spec, proxy backtest results, data-source notes, and reproduction commands for another agent/reviewer.

## Strategy

Cross-sectional RSI-gated options-income wheel on a point-in-time S&P 500 proxy universe:

- Start with `$100,000`.
- At most 5 committed names.
- Per-position cap: 30% of current equity.
- Sell roughly 21-trading-day puts when Wilder RSI(14) < 30.
- Base put strike: `0.95 * spot`.
- Covered-call exit when assigned shares have no open option and Wilder RSI(14) > 70.
- Call strike: `0.95 * spot`.
- Proxy pricing requested on 2026-05-27: use local option aggregate price as mid and apply 5% slippage/haircut to short-option proceeds.

Two variants are included:

- `wheel_cs_static_otm_itm_rsi21`: base `0.95` put / `0.95` call.
- `wheel_cs_vix_overlay_rsi21`: use prior-day VIX; if VIX > 22, sell `1.05 * spot` ITM puts, otherwise sell `0.95 * spot` puts.

## Results

Primary result:

- [results/proxy-wheel-mid5-full-2025-01-02-2026-05-26.md](results/proxy-wheel-mid5-full-2025-01-02-2026-05-26.md)
- [results/proxy-wheel-mid5-full-2025-01-02-2026-05-26.json](results/proxy-wheel-mid5-full-2025-01-02-2026-05-26.json)

Canary:

- [results/proxy-wheel-mid5-canary-2025-01-21-2025-02-28.md](results/proxy-wheel-mid5-canary-2025-01-21-2025-02-28.md)
- [results/proxy-wheel-mid5-canary-2025-01-21-2025-02-28.json](results/proxy-wheel-mid5-canary-2025-01-21-2025-02-28.json)

Strict preflight:

- [results/cross-sectional-wheel-strict-preflight-with-web-sources-2025-01-02-2026-05-26.md](results/cross-sectional-wheel-strict-preflight-with-web-sources-2025-01-02-2026-05-26.md)
- [results/cross-sectional-wheel-strict-preflight-with-web-sources-2025-01-02-2026-05-26.json](results/cross-sectional-wheel-strict-preflight-with-web-sources-2025-01-02-2026-05-26.json)

## Headline Findings

Full proxy window, `2025-01-02` through `2026-05-26`:

| Strategy | Return | Max DD | Trades | Assignments | Gross premium/yr |
| --- | ---: | ---: | ---: | ---: | ---: |
| VIX overlay | +3.33% | -17.67% | 14 | 7 | 6.04% |
| Base static | -11.16% | -20.04% | 28 | 10 | 6.75% |
| SPY benchmark | +28.38% | -18.95% | n/a | n/a | n/a |
| QQQ benchmark | +43.13% | -22.86% | n/a | n/a | n/a |

Last-six-month proxy slice, `2025-11-26` through `2026-05-26`:

| Strategy | Return | Max DD | Trades | Assignments |
| --- | ---: | ---: | ---: | ---: |
| VIX overlay | +0.49% | -15.17% | 4 | 2 |
| Base static | -10.44% | -16.75% | 6 | 4 |
| SPY benchmark | +10.42% | -9.13% | n/a | n/a |
| QQQ benchmark | +18.87% | -11.80% | n/a | n/a |

Post-hoc dividend and idle-cash interest estimates, not compounded into the engine sizing:

| Strategy | Full extra return estimate | Last-six-month extra return estimate |
| --- | ---: | ---: |
| VIX overlay | +4.13% | +1.43% |
| Base static | +4.69% | +1.39% |

Interpretation: the VIX overlay is the only proxy variant with positive raw return, but it materially underperforms SPY and QQQ. The base static wheel is not profitable under the requested mid-price/5% haircut assumption.

## Reproduction

Build the PIT-union universe from the downloaded S&P 500 snapshot file:

```bash
python3 - <<'PY'
import csv, json
from pathlib import Path
start='2025-01-02'; end='2026-05-26'
root=Path('projects/spy-intraday-prediction/runtime/cross-sectional-wheel-sources')
source=root/'sp500-historical-components-fja05680-2026-01-17.csv'
out=root/f'sp500-pit-union-{start}-{end}.json'
symbols=set()
with source.open(newline='') as f:
    for row in csv.DictReader(f):
        if start <= row['date'] <= end:
            symbols.update(s.strip().upper() for s in row['tickers'].split(',') if s.strip())
out.write_text(json.dumps({'source': str(source), 'startDate': start, 'endDate': end, 'symbols': sorted(symbols)}, indent=2)+'\n')
PY
```

Run the full proxy backtest:

```bash
node --max-old-space-size=8192 projects/spy-intraday-prediction/scripts/backtest-wheel-strategy.js \
  --suite cs-premium \
  --strategies wheel_cs_static_otm_itm_rsi21,wheel_cs_vix_overlay_rsi21 \
  --universe projects/spy-intraday-prediction/runtime/cross-sectional-wheel-sources/sp500-pit-union-2025-01-02-2026-05-26.json \
  --limit 600 \
  --capital 100000 \
  --start-date 2025-01-02 \
  --end-date 2026-05-26 \
  --entry-minute-et 959 \
  --entry-window-minutes 1 \
  --premium-haircut-pct 0.05 \
  --commission 0.65 \
  --max-position-pct 0.30 \
  --max-utilization 1.0 \
  --max-open-options 5 \
  --output projects/spy-intraday-prediction/artifacts/proxy-wheel-mid5-full-2025-01-02-2026-05-26.json
```

Run the strict gate:

```bash
node projects/spy-intraday-prediction/scripts/backtest-cross-sectional-wheel-strict.js \
  --membership projects/spy-intraday-prediction/runtime/cross-sectional-wheel-sources/sp500-historical-components-fja05680-2026-01-17.csv \
  --dividends projects/spy-intraday-prediction/runtime/cross-sectional-wheel-sources/massive-dividends-sp500pit-2025-01-02-2026-05-26.csv \
  --risk-free projects/spy-intraday-prediction/runtime/cross-sectional-wheel-sources/risk-free-dgs3mo-fred.csv \
  --commission 0.65 \
  --slippage-bps 1
```

## Caveats

- The proxy run is not the strict executable bid/ask baseline. Local Massive option quote access returned `NOT_AUTHORIZED`/`403`, so aggregate option prices are treated as mid-price proxies.
- The local full window is only 349 open days, short of the requested 5-year target.
- `2026-04-28` onward is provisional/live-cache data under the project runbook.
- Dividends and idle-cash interest were estimated post-hoc and are not compounded into position sizing in the engine output.
