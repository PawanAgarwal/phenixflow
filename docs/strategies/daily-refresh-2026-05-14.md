# Strategy Service Daily Refresh - 2026-05-14

Generated locally: `2026-05-16T00:15:01.175Z`

Full generated report:
[strategy-service-refresh-2026-05-14.md](/Users/pawanagarwal/github/phenixflow/artifacts/strategy-service/strategy-service-refresh-2026-05-14.md)

JSON payload:
`artifacts/strategy-service/strategy-service-refresh-2026-05-14.json`

## Format

The daily report now separates the timing pieces that were previously collapsed
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
- SQLite latest daily result date: `2026-05-14`
- SQLite rows: `9,073` daily results, `50,839` holdings, `12,034` trades
- P/L basis on `2026-05-14`: `14` EOD mark-to-market rows, `13` intraday rows
- P/L statuses: `14` EOD mark, `7` traded, `6` flat/cash
- D target coverage for next P/L: `27/27` strategies available
- Same missing-target issue fixed for derived ML adapters:
  `pym-v5-ml-calm-trend-router`, `pym-v5-ml-option-top8-50-50`, and
  `pym-v5-two-speed-option-meta21`
- Wheel checkpoint: `wheel-backtest-checkpoint.v1` stored in
  `projects/spy-intraday-prediction/artifacts/wheel-expanded-backtest-2025-01-02-2026-05-14.json`

## Examples

| Strategy | P/L date D | P/L source holdings/trades | Latest P/L | D target for next P/L | Holding changes into D target |
|---|---:|---|---:|---|---|
| `pym-v5` | 2026-05-14 | 2026-05-13 target: VIXY 27.46%, BIL 14.29%, EDZ 12.50%, UGL 12.50%, TLT 6.25%, UTSL 6.25% | -1.08% | 2026-05-14 target: VIXY 27.46%, BIL 14.29%, EDZ 12.50%, UGL 12.50%, TLT 6.25%, TMF 6.25% | TMF +6.25%, UTSL -6.25% |
| `pym-v5-ml-two-speed-attention` | 2026-05-14 | 2026-05-13 target: VIXY 65.78%, UVXY 14.97%, SHV 7.49%, TMV 7.49%, QQQ 4.28% | -2.13% | 2026-05-14 target: VIXY 65.78%, UVXY 14.97%, SHV 7.49%, TMV 7.49%, QQQ 4.28% | no rebalance |
| `pym-v5-ml-calm-trend-router` | 2026-05-14 | 2026-05-13 target: UGL 28.66%, VIXY 23.02%, UVXY 19.57%, TLT 14.33%, QQQ 5.59%, IAU 3.58% | -1.67% | 2026-05-14 target: VIXY 53.31%, UGL 13.79%, TLT 6.89%, TMF 6.89%, TMV 6.07%, UVXY 5.24% | VIXY +30.28%, UGL -14.87%, UVXY -14.33%, TLT -7.44%, TMF +6.89%, TMV +3.45% |
| `pym-v5-ml-option-top8-50-50` | 2026-05-14 | 2026-05-13 target: VIXY 32.89%, UGL 22.05%, UVXY 18.51%, TLT 11.02%, QQQ 5.29%, SHV 3.74% | -1.78% | 2026-05-14 target: VIXY 56.18%, UGL 10.61%, UVXY 7.49%, TMV 6.39%, TLT 5.30%, TMF 5.30% | VIXY +23.30%, UGL -11.44%, UVXY -11.02%, TLT -5.72%, TMF +5.30%, TMV +2.65% |
| `pym-v5-two-speed-option-meta21` | 2026-05-14 | 2026-05-13 target: UGL 44.09%, TLT 22.05%, UVXY 22.05%, QQQ 6.30%, IAU 5.51% | -1.45% | 2026-05-14 target: VIXY 46.59%, UGL 21.21%, TLT 10.61%, TMF 10.61%, TMV 5.30%, QQQ 3.03% | VIXY +46.59%, UGL -22.88%, UVXY -22.05%, TLT -11.44%, TMF +10.61%, TMV +5.30% |
| `option-income-wheel-trend-ivrv` | 2026-05-14 | O:AVGO260522P00300000 SHORT; O:AFRM260522P00053000 SHORT; O:INTC260522P00072000 SHORT; O:CAT260522P00390000 SHORT | 0.06% | 2026-05-14 target: CASH 88.64%, ASSIGNED_STOCK 11.40%, SHORT_OPTION_MARK 0.04% | CASH -0.05%, ASSIGNED_STOCK +0.04%, SHORT_OPTION_MARK -0.01% |
| `tsll-seconds-passive-scalper` | 2026-05-14 | TSLL trades: 7 same-day scalps | 0.11% | 2026-05-14 target: TSLL 100.00% |  |
| `pym-gated-intraday-best-combo` | 2026-05-14 | SQQQ SHORT -2.29% | -2.29% | 2026-05-14 target: SQQQ -300.00% |  |

## Trailing Performance Examples

Each cell is `return / Sharpe`.

| Strategy | Since start | 1Y | 3M | 1M | 1W |
|---|---:|---:|---:|---:|---:|
| `pym-v5` | 80.99% / 2.36 | 61.38% / 3.72 | 5.71% / 1.34 | -0.12% / -0.08 | -0.49% / -1.76 |
| `pym-v5-option-rank-top8` | 115.63% / 2.01 | 62.59% / 2.25 | 11.54% / 1.59 | 6.44% / 3.05 | 0.02% / 0.14 |
| `pym-v5-ml-two-speed-attention` | 184.22% / 1.96 | 32.30% / 0.94 | -0.55% / 0.20 | 3.24% / 1.27 | -0.91% / -1.11 |
| `option-income-wheel-trend-ivrv` | 8.42% / 2.56 | 6.73% / 2.98 | 3.18% / 4.79 | 2.72% / 9.66 | 0.32% / 5.31 |
| `tsll-seconds-passive-scalper` | 440.53% / 6.85 | 83.46% / 11.13 | 3.75% / 9.53 | 2.15% / 11.55 | 1.34% / 27.36 |

See the full generated report for all `27` strategies.

## Daily Fast Run Rule

Use `npm run strategy-service:refresh-daily-fast` for normal after-EOD refreshes.
The wheel strategy now persists a checkpoint in its JSON artifact, so the next
same-code daily run can append missing days instead of replaying the full
history. Force a full rebuild only when strategy logic, execution assumptions,
costs/slippage, or historical Massive inputs changed.
