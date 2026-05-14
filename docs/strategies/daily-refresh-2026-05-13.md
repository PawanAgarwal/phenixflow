# Strategy Service Daily Refresh - 2026-05-13

Generated locally: `2026-05-14T14:19:48.073Z`

Rows after `2026-04-27` are provisional per the project runbook. This refresh
uses the normalized strategy result contract: EOD strategies report P/L from
yesterday's target holdings marked to the next EOD close; intraday strategies
report P/L from emitted trades or explicit flat-day records.

## Refresh Status

- Registered strategies refreshed: `27`
- Snapshot JSON: `artifacts/strategy-service/strategy-service-refresh-2026-05-13.json`
- Snapshot Markdown: `artifacts/strategy-service/strategy-service-refresh-2026-05-13.md`
- SQLite store: `apps/strategy-service/runtime/strategy-results.sqlite`
- SQLite latest daily result date: `2026-05-13`
- SQLite rows: `9,046` daily results, `50,694` holdings, `11,973` trades
- This refresh import wrote: `9,046` daily results, `50,694` holdings, `11,973` trades
- P/L basis on `2026-05-13`: `14` EOD mark-to-market rows, `13` intraday rows
- P/L statuses: `14` EOD mark, `1` strategy-reported, `3` traded, `9` flat/cash

## Best And Worst Latest P/L

| Rank | Strategy | Latest P/L | P/L $ | Total return | Equity | Holdings |
|---:|---|---:|---:|---:|---:|---|
| 1 | `pym-v5-sleeve-meta-21d-cap25` | 0.83% | $181 | 119.20% | $21,920 | VIXY 36.99%, BIL 26.28%, UVXY 12.50%, TLT 5.44%, UTSL 5.44%, IAU 3.05% |
| 2 | `pym-v5-option-rank-top8` | 0.64% | $139 | 118.77% | $21,877 | UGL 44.09%, TLT 22.05%, UVXY 22.05%, QQQ 6.30%, IAU 5.51% |
| 3 | `pym-v5-ml-calm-trend-router` | 0.56% | $207 | 271.59% | $37,159 | TMF 20.90%, TLT 19.12%, UVXY 19.12%, IAU 17.21%, TMV 9.56%, PSQ 5.57% |
| 4 | `pym-v5-ml-option-top8-50-50` | 0.53% | $136 | 158.33% | $25,833 | TMF 29.86%, TLT 14.71%, UVXY 14.71%, IAU 13.24%, PSQ 7.96%, TMV 7.35% |
| 5 | `pym-v5-ml-two-speed-attention` | 0.42% | $121 | 190.39% | $29,039 | TMF 59.71%, PSQ 15.92%, TECL 11.14%, SOXL 10.38%, IEF 2.84% |
| 6 | `pym-v5-two-speed-option-meta21` | 0.42% | $128 | 206.42% | $30,642 | TMF 59.71%, PSQ 15.92%, TECL 11.14%, SOXL 10.38%, IEF 2.84% |
| 7 | `pym-v5-cap25-lgbm-blend40` | 0.36% | $93 | 155.54% | $25,554 | VIXY 36.99%, BIL 26.28%, UVXY 12.50%, TLT 5.44%, UTSL 5.44%, IAU 3.05% |
| 8 | `pym-v5-cap25-lgbm-blend40-stress` | 0.36% | $75 | 107.55% | $20,755 | VIXY 36.99%, BIL 26.28%, UVXY 12.50%, TLT 5.44%, UTSL 5.44%, IAU 3.05% |
| 25 | `pym-v5` | -0.42% | -$76 | 82.96% | $18,296 | VIXY 27.46%, BIL 14.29%, EDZ 12.50%, UGL 12.50%, TLT 6.25%, UTSL 6.25% |
| 26 | `pym-gated-intraday-overnight-1x` | -0.57% | -$66 | 14.74% | $11,474 | SH -100.00% |
| 27 | `pym-gated-intraday-best-combo` | -1.68% | -$305 | 78.11% | $17,811 | SQQQ -300.00% |

## All Registered Strategies

| Strategy | Realized date | Status | Latest P/L | P/L $ | Total return | Equity | Latest trades | Holdings |
|---|---:|---|---:|---:|---:|---:|---:|---|
| `pym-v5` | 2026-05-13 | EOD mark | -0.42% | -$76 | 82.96% | $18,296 | 0 | VIXY 27.46%, BIL 14.29%, EDZ 12.50%, UGL 12.50%, TLT 6.25%, UTSL 6.25% |
| `pym-v5-option-rank-top8` | 2026-05-13 | EOD mark | 0.64% | $139 | 118.77% | $21,877 | 0 | UGL 44.09%, TLT 22.05%, UVXY 22.05%, QQQ 6.30%, IAU 5.51% |
| `pym-v5-ml-two-speed-attention` | 2026-05-13 | EOD mark | 0.42% | $121 | 190.39% | $29,039 | 0 | TMF 59.71%, PSQ 15.92%, TECL 11.14%, SOXL 10.38%, IEF 2.84% |
| `pym-v5-ml-calm-trend-router` | 2026-05-13 | EOD mark | 0.56% | $207 | 271.59% | $37,159 | 0 | TMF 20.90%, TLT 19.12%, UVXY 19.12%, IAU 17.21%, TMV 9.56%, PSQ 5.57% |
| `pym-v5-ml-option-top8-50-50` | 2026-05-13 | EOD mark | 0.53% | $136 | 158.33% | $25,833 | 0 | TMF 29.86%, TLT 14.71%, UVXY 14.71%, IAU 13.24%, PSQ 7.96%, TMV 7.35% |
| `pym-v5-two-speed-option-meta21` | 2026-05-13 | EOD mark | 0.42% | $128 | 206.42% | $30,642 | 0 | TMF 59.71%, PSQ 15.92%, TECL 11.14%, SOXL 10.38%, IEF 2.84% |
| `pym-v5-spy-put-pressure-bil` | 2026-05-13 | EOD mark | -0.42% | -$80 | 92.15% | $19,215 | 0 | VIXY 27.46%, BIL 14.29%, EDZ 12.50%, UGL 12.50%, TLT 6.25%, UTSL 6.25% |
| `pym-v5-sleeve-meta-21d-cap25` | 2026-05-13 | EOD mark | 0.83% | $181 | 119.20% | $21,920 | 0 | VIXY 36.99%, BIL 26.28%, UVXY 12.50%, TLT 5.44%, UTSL 5.44%, IAU 3.05% |
| `pym-v5-cap25-lgbm-blend40` | 2026-05-13 | EOD mark | 0.36% | $93 | 155.54% | $25,554 | 0 | VIXY 36.99%, BIL 26.28%, UVXY 12.50%, TLT 5.44%, UTSL 5.44%, IAU 3.05% |
| `pym-v5-cap25-lgbm-blend40-stress` | 2026-05-13 | EOD mark | 0.36% | $75 | 107.55% | $20,755 | 0 | VIXY 36.99%, BIL 26.28%, UVXY 12.50%, TLT 5.44%, UTSL 5.44%, IAU 3.05% |
| `option-income-wheel-trend-ivrv` | 2026-05-13 | reported | -0.10% | -$1,102 | 8.27% | $1,082,714 | 0 | CASH 88.63%, ASSIGNED_STOCK 11.37% |
| `tsll-seconds-passive-scalper` | 2026-05-13 | traded | 0.18% | $201 | 439.88% | $111,664 | 14 | TSLL 100.00% |
| `pym-gated-intraday-baseline` | 2026-05-13 | flat/cash | 0.00% | $0 | 12.03% | $11,203 | 0 | CASH 100.00% |
| `pym-gated-intraday-lev3x` | 2026-05-13 | flat/cash | 0.00% | $0 | 41.00% | $14,100 | 0 | CASH 100.00% |
| `pym-gated-intraday-overnight-1x` | 2026-05-13 | traded | -0.57% | -$66 | 14.74% | $11,474 | 1 | SH -100.00% |
| `pym-gated-intraday-best-combo` | 2026-05-13 | traded | -1.68% | -$305 | 78.11% | $17,811 | 1 | SQQQ -300.00% |
| `pym-gated-intraday-deadzone-biasprop-1500exit-3x` | 2026-05-13 | flat/cash | 0.00% | $0 | 49.99% | $14,999 | 0 | CASH 100.00% |
| `occ-pc-contrarian-intraday-1x-long-only` | 2026-05-13 | flat/cash | 0.00% | $0 | 2.74% | $10,274 | 0 | CASH 100.00% |
| `occ-pc-contrarian-intraday-3x` | 2026-05-13 | flat/cash | 0.00% | $0 | 11.71% | $11,171 | 0 | CASH 100.00% |
| `vix-term-contrarian-intraday-vix3m-1x` | 2026-05-13 | flat/cash | 0.00% | $0 | 8.36% | $10,836 | 0 | CASH 100.00% |
| `vix-term-contrarian-intraday-inv-long-3x-overnight` | 2026-05-13 | flat/cash | 0.00% | $0 | 35.64% | $13,564 | 0 | CASH 100.00% |
| `vvix-spike-contrarian-overnight-3x` | 2026-05-13 | flat/cash | 0.00% | $0 | 60.15% | $16,015 | 0 | CASH 100.00% |
| `gap-down-fade-intraday-3x` | 2026-05-13 | flat/cash | 0.00% | $0 | 43.03% | $14,303 | 0 | CASH 100.00% |
| `fear-extreme-portfolio-equalweight-4x` | 2026-05-13 | EOD mark | 0.00% | $0 | 14.59% | $11,459 | 0 | CASH 100.00% |
| `fear-basket-vvix-occ3x-vix3xon-3x` | 2026-05-13 | EOD mark | 0.00% | $0 | 36.21% | $13,621 | 0 | CASH 100.00% |
| `fear-basket-vvix-vix3xon-3x` | 2026-05-13 | EOD mark | 0.00% | $0 | 49.05% | $14,905 | 0 | CASH 100.00% |
| `gap-fade-vix3xon-hedge-3x` | 2026-05-13 | EOD mark | 0.00% | $0 | 41.98% | $14,198 | 0 | CASH 100.00% |

## Notes For The Next Fast Run

- The 2026-05-13 snapshot and SQLite store are ready as the prior state for the
  next EOD fast refresh.
- Use `npm run strategy-service:refresh-daily-fast` for the next daily refresh.
- Run a full replay or force rebuild only if strategy logic, costs/slippage,
  model settings, or historical Massive inputs changed.
