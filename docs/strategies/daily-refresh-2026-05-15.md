# Strategy Service Daily Refresh - 2026-05-15

Generated locally: `2026-05-18T04:42:42.076Z`

Full generated report:
[strategy-service-refresh-2026-05-15.md](/Users/pawanagarwal/github/phenixflow/artifacts/strategy-service/strategy-service-refresh-2026-05-15.md)

JSON payload:
`artifacts/strategy-service/strategy-service-refresh-2026-05-15.json`

## Format

The daily report separates the timing pieces that were previously collapsed
into one ambiguous `Holdings` column:

- **P/L date D**: the date whose P/L is being reported.
- **P/L source holdings/trades**: what actually produced P/L on D.
- **D target for next P/L**: holdings set after D for the next realized P/L day.
- **Holding changes into D target**: changes from the P/L source holdings to the
  D target, when both are available.
- **Trailing performance**: return and Sharpe since start, 1Y, 3M, 1M, and 1W.

For EOD strategies, P/L on D comes from the target holdings set after the close
on D-1 and marked to the close on D. For intraday strategies, P/L comes from
same-day emitted trades or explicit flat-day records.

## Refresh Status

- Registered strategies refreshed: `27/27`
- Stale strategy count: `0`
- SQLite latest daily result date: `2026-05-15`
- SQLite rows: `9,100` daily results, `51,001` holdings, `12,059` trades
- P/L statuses: `13` EOD mark, `9` traded, `5` flat/cash
- D target coverage for next P/L: `27/27` strategies available
- Wheel checkpoint: `wheel-backtest-checkpoint.v1` stored in
  `projects/spy-intraday-prediction/artifacts/wheel-expanded-backtest-2025-01-02-2026-05-15.json`

## Examples

| Strategy | P/L date D | P/L source holdings/trades | Latest P/L | D target for next P/L | Holding changes into D target |
|---|---:|---|---:|---|---|
| `pym-v5` | 2026-05-15 | 2026-05-14 target: VIXY 27.46%, BIL 14.29%, EDZ 12.50%, UGL 12.50%, TLT 6.25%, TMF 6.25% | 0.76% | 2026-05-15 target: QQQ 16.07%, EDZ 12.50%, TMF 12.50%, UGL 12.50%, BIL 8.87%, TLT 6.70% | VIXY -27.46%, QQQ +14.29%, TMF +6.25%, UVXY -6.25%, VTI +6.25%, TQQQ +6.03% |
| `pym-v5-ml-two-speed-attention` | 2026-05-15 | 2026-05-14 target: VIXY 65.78%, UVXY 14.97%, SHV 7.49%, TMV 7.49%, QQQ 4.28% | 1.03% | 2026-05-15 target: VIXY 65.78%, UVXY 14.97%, SHV 7.49%, TMV 7.49%, QQQ 4.28% | no rebalance |
| `pym-v5-ml-calm-trend-router` | 2026-05-15 | 2026-05-14 target: VIXY 53.31%, UGL 13.79%, TLT 6.89%, TMF 6.89%, TMV 6.07%, UVXY 5.24% | -0.35% | 2026-05-15 target: UGL 31.08%, VIXY 23.02%, VTI 15.54%, VT 8.88%, UVXY 5.24%, TMV 4.10% | VIXY -30.28%, UGL +17.29%, VTI +15.54%, VT +8.88%, TLT -6.89%, TMF -6.89% |
| `pym-v5-ml-option-top8-50-50` | 2026-05-15 | 2026-05-14 target: VIXY 56.18%, UGL 10.61%, UVXY 7.49%, TMV 6.39%, TLT 5.30%, TMF 5.30% | -0.03% | 2026-05-15 target: VIXY 32.89%, UGL 23.91%, VTI 11.95%, UVXY 7.49%, VT 6.83%, TMV 4.88% | VIXY -23.30%, UGL +13.30%, VTI +11.95%, VT +6.83%, TLT -5.30%, TMF -5.30% |
| `pym-v5-two-speed-option-meta21` | 2026-05-15 | 2026-05-14 target: VIXY 46.59%, UGL 21.21%, TLT 10.61%, TMF 10.61%, TMV 5.30%, QQQ 3.03% | -1.09% | 2026-05-15 target: VIXY 65.78%, UVXY 14.97%, SHV 7.49%, TMV 7.49%, QQQ 4.28% | UGL -21.21%, VIXY +19.18%, UVXY +14.97%, TLT -10.61%, TMF -10.61%, SHV +7.49% |
| `option-income-wheel-trend-ivrv` | 2026-05-15 | O:AAPL260522P00260000 SHORT; O:UNH260522P00350000 SHORT; O:GS260515P00420000 SHORT; O:HAL260515P00033500 SHORT | -0.03% | 2026-05-15 target: CASH 88.66%, ASSIGNED_STOCK 11.36%, SHORT_OPTION_MARK 0.02% | ASSIGNED_STOCK -0.04%, CASH +0.02%, SHORT_OPTION_MARK -0.02% |
| `tsll-seconds-passive-scalper` | 2026-05-15 | TSLL trades: 5 same-day scalps | 0.13% | 2026-05-15 target: TSLL 100.00% |  |
| `pym-gated-intraday-best-combo` | 2026-05-15 | SQQQ SHORT 3.64% | 3.64% | 2026-05-15 target: SQQQ -300.00% |  |

## Trailing Performance Examples

Each cell is `return / Sharpe`.

| Strategy | Since start | 1Y | 3M | 1M | 1W |
|---|---:|---:|---:|---:|---:|
| `pym-v5` | 82.36% / 2.39 | 61.15% / 3.71 | 6.51% / 1.49 | 0.35% / 0.44 | 0.88% / 3.23 |
| `pym-v5-option-rank-top8` | 113.29% / 1.98 | 60.75% / 2.20 | 10.32% / 1.43 | 4.38% / 2.11 | -0.97% / -2.87 |
| `pym-v5-ml-two-speed-attention` | 187.14% / 1.97 | 33.36% / 0.96 | 0.47% / 0.28 | 2.03% / 0.87 | -3.05% / -6.09 |
| `pym-v5-ml-calm-trend-router` | 264.10% / 2.48 | 71.76% / 1.80 | 2.69% / 0.46 | 3.68% / 1.86 | -1.69% / -5.35 |
| `option-income-wheel-trend-ivrv` | 8.39% / 2.54 | 6.65% / 2.95 | 3.15% / 4.70 | 2.47% / 8.73 | -0.09% / -4.05 |
| `tsll-seconds-passive-scalper` | 441.32% / 6.84 | 83.47% / 11.13 | 3.89% / 9.76 | 2.26% / 12.21 | 1.25% / 23.77 |
| `pym-gated-intraday-best-combo` | 80.36% / 1.28 | 45.87% / 1.93 | 15.69% / 1.72 | -4.62% / -1.88 | -0.96% / -1.17 |

See the full generated report for all `27` strategies.

## Daily Fast Run Rule

Use `npm run strategy-service:refresh-daily-fast` for normal after-EOD refreshes.
The fast path appends same-code ML, wheel, and TSLL artifacts where possible,
then persists the normalized strategy result contract to SQLite. Force a full
rebuild only when strategy logic, execution assumptions, costs/slippage, or
historical Massive inputs changed.
