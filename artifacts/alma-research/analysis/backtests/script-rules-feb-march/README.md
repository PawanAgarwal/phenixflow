# Script Rules Feb-March Backtest

This backtest uses only the raw SPX `SCRIPT INPUTS` ladder and the three deterministic rule families derived from it.

Rules used:
- `risk_fade`: touch the risk band, reclaim inside `1 sigma`, target prior close, stop at `2 sigma`.
- `two_sigma_breakout`: two consecutive closes beyond `2 sigma`, target `3 sigma`, stop at risk band.
- `three_sigma_exhaustion`: touch `3 sigma`, close back inside the tail zone, target `2 sigma`, stop at `4 sigma`.
- Intrabar ambiguity is handled conservatively: if stop and target could both hit in the same 1-minute bar, the trade is counted as stopped.

## Coverage

- Script-input days tested: 28
- Date range: 2026-02-02 to 2026-03-13
- Gross trades triggered: 36
- Yahoo interval usage: 1m=18, 2m=10

## Overall

| Bucket | Trades | Wins | Losses | Win rate | Gross profit | Gross loss | Net profit | Max drawdown | Stops | Profit factor |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| All trades | 36 | 17 | 19 | 47.2% | 406.67 | -444.05 | -37.38 | 155.15 | 12 | 0.92 |

## By Strategy

| Bucket | Trades | Wins | Losses | Win rate | Gross profit | Gross loss | Net profit | Max drawdown | Stops | Profit factor |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| risk_fade | 18 | 9 | 9 | 50.0% | 240.96 | -167.90 | 73.06 | 72.42 | 3 | 1.44 |
| three_sigma_exhaustion | 5 | 2 | 3 | 40.0% | 59.48 | -81.93 | -22.45 | 54.01 | 2 | 0.73 |
| two_sigma_breakout | 13 | 6 | 7 | 46.2% | 106.23 | -194.22 | -87.99 | 128.85 | 7 | 0.55 |

## By Month

| Bucket | Trades | Wins | Losses | Win rate | Gross profit | Gross loss | Net profit | Max drawdown | Stops | Profit factor |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 2026-02 | 25 | 11 | 14 | 44.0% | 228.10 | -308.01 | -79.91 | 130.78 | 8 | 0.74 |
| 2026-03 | 11 | 6 | 5 | 54.5% | 178.57 | -136.05 | 42.53 | 67.70 | 4 | 1.31 |

## Stops Triggered

| Date | Data | Strategy | Side | Entry ET | Entry | Stop | Exit ET | Exit | PnL | Source |
| --- | --- | --- | --- | --- | ---: | ---: | --- | ---: | ---: | --- |
| 2026-02-03 | 2m | risk_fade | long | 10:50 | 6952.63 | 6922.71 | 12:04 | 6922.71 | -29.92 | [2026-02-03_warsh-is-not-a-hawk-intraday-post](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-03_warsh-is-not-a-hawk-intraday-post/content.txt) |
| 2026-02-03 | 2m | three_sigma_exhaustion | long | 12:42 | 6896.90 | 6868.97 | 14:00 | 6868.97 | -27.93 | [2026-02-03_warsh-is-not-a-hawk-intraday-post](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-03_warsh-is-not-a-hawk-intraday-post/content.txt) |
| 2026-02-04 | 2m | risk_fade | long | 12:02 | 6888.85 | 6859.56 | 12:26 | 6859.56 | -29.29 | [2026-02-04_intraday-post-04feb-038](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-04_intraday-post-04feb-038/content.txt) |
| 2026-02-04 | 2m | two_sigma_breakout | short | 12:34 | 6853.09 | 6878.55 | 14:30 | 6878.55 | -25.46 | [2026-02-04_intraday-post-04feb-038](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-04_intraday-post-04feb-038/content.txt) |
| 2026-02-05 | 2m | two_sigma_breakout | short | 09:32 | 6814.79 | 6840.56 | 09:38 | 6840.56 | -25.77 | [2026-02-05_volatility-is-back-intraday-post](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-05_volatility-is-back-intraday-post/content.txt) |
| 2026-02-06 | 2m | three_sigma_exhaustion | short | 11:36 | 6898.58 | 6934.35 | 15:50 | 6934.35 | -35.77 | [2026-02-06_high-volume-intraday-post-06feb](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-06_high-volume-intraday-post-06feb/content.txt) |
| 2026-02-26 | 1m | two_sigma_breakout | short | 10:19 | 6880.15 | 6905.23 | 11:16 | 6905.23 | -25.08 | [2026-02-26_nvda-er-intraday-post-26feb](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-26_nvda-er-intraday-post-26feb/content.txt) |
| 2026-02-27 | 1m | two_sigma_breakout | short | 09:42 | 6838.59 | 6866.90 | 10:08 | 6866.90 | -28.31 | [2026-02-27_ppi-intraday-post-27feb](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-27_ppi-intraday-post-27feb/content.txt) |
| 2026-03-02 | 1m | two_sigma_breakout | short | 09:31 | 6814.91 | 6835.87 | 09:37 | 6835.87 | -20.96 | [2026-03-02_intraday-post-02march](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-03-02_intraday-post-02march/content.txt) |
| 2026-03-05 | 1m | risk_fade | long | 10:50 | 6835.72 | 6801.79 | 12:30 | 6801.79 | -33.93 | [2026-03-05_intraday-post-05march](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-03-05_intraday-post-05march/content.txt) |
| 2026-03-05 | 1m | two_sigma_breakout | short | 12:31 | 6790.10 | 6823.86 | 15:50 | 6823.86 | -33.76 | [2026-03-05_intraday-post-05march](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-03-05_intraday-post-05march/content.txt) |
| 2026-03-09 | 1m | two_sigma_breakout | short | 09:50 | 6646.09 | 6680.97 | 10:45 | 6680.97 | -34.88 | [2026-03-09_intraday-post-09march](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-03-09_intraday-post-09march/content.txt) |

## All Trades

| Date | Data | Strategy | Side | Entry ET | Entry | Stop | Target | Exit ET | Exit | Exit reason | PnL | MAE | MFE | Source |
| --- | --- | --- | --- | --- | ---: | ---: | ---: | --- | ---: | --- | ---: | ---: | ---: | --- |
| 2026-02-02 | 2m | risk_fade | short | 11:00 | 6966.93 | 6995.66 | 6939.03 | 16:00 | 6976.50 | close | -9.57 | 24.99 | 4.82 | [2026-02-02_intraday-post-02feb](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-02_intraday-post-02feb/content.txt) |
| 2026-02-03 | 2m | risk_fade | long | 10:50 | 6952.63 | 6922.71 | 6976.44 | 12:04 | 6922.71 | stop | -29.92 | 30.03 | 4.24 | [2026-02-03_warsh-is-not-a-hawk-intraday-post](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-03_warsh-is-not-a-hawk-intraday-post/content.txt) |
| 2026-02-03 | 2m | two_sigma_breakout | short | 12:14 | 6912.95 | 6940.22 | 6895.79 | 12:42 | 6895.79 | target | 17.16 | 9.79 | 17.22 | [2026-02-03_warsh-is-not-a-hawk-intraday-post](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-03_warsh-is-not-a-hawk-intraday-post/content.txt) |
| 2026-02-03 | 2m | three_sigma_exhaustion | long | 12:42 | 6896.90 | 6868.97 | 6922.71 | 14:00 | 6868.97 | stop | -27.93 | 29.23 | 12.05 | [2026-02-03_warsh-is-not-a-hawk-intraday-post](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-03_warsh-is-not-a-hawk-intraday-post/content.txt) |
| 2026-02-04 | 2m | risk_fade | long | 12:02 | 6888.85 | 6859.56 | 6917.81 | 12:26 | 6859.56 | stop | -29.29 | 31.31 | 2.63 | [2026-02-04_intraday-post-04feb-038](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-04_intraday-post-04feb-038/content.txt) |
| 2026-02-04 | 2m | two_sigma_breakout | short | 12:34 | 6853.09 | 6878.55 | 6830.37 | 14:30 | 6878.55 | stop | -25.46 | 29.90 | 14.29 | [2026-02-04_intraday-post-04feb-038](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-04_intraday-post-04feb-038/content.txt) |
| 2026-02-05 | 2m | two_sigma_breakout | short | 09:32 | 6814.79 | 6840.56 | 6788.82 | 09:38 | 6840.56 | stop | -25.77 | 32.98 | 0.45 | [2026-02-05_volatility-is-back-intraday-post](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-05_volatility-is-back-intraday-post/content.txt) |
| 2026-02-05 | 2m | three_sigma_exhaustion | long | 10:14 | 6791.58 | 6757.62 | 6820.17 | 11:30 | 6820.17 | target | 28.59 | 11.45 | 30.95 | [2026-02-05_volatility-is-back-intraday-post](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-05_volatility-is-back-intraday-post/content.txt) |
| 2026-02-06 | 2m | two_sigma_breakout | long | 09:44 | 6879.04 | 6844.21 | 6900.28 | 11:34 | 6900.28 | target | 21.24 | 25.44 | 22.31 | [2026-02-06_high-volume-intraday-post-06feb](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-06_high-volume-intraday-post-06feb/content.txt) |
| 2026-02-06 | 2m | three_sigma_exhaustion | short | 11:36 | 6898.58 | 6934.35 | 6866.37 | 15:50 | 6934.35 | stop | -35.77 | 46.31 | 6.58 | [2026-02-06_high-volume-intraday-post-06feb](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-06_high-volume-intraday-post-06feb/content.txt) |
| 2026-02-09 | 2m | risk_fade | short | 13:20 | 6961.17 | 6992.17 | 6932.30 | 16:00 | 6964.80 | close | -3.63 | 18.93 | 0.70 | [2026-02-09_intraday-post-09feb](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-09_intraday-post-09feb/content.txt) |
| 2026-02-11 | 2m | risk_fade | short | 09:54 | 6970.27 | 7001.89 | 6941.81 | 10:10 | 6941.81 | target | 28.46 | 0.00 | 29.51 | [2026-02-11_nfp-intraday-post-11feb](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-11_nfp-intraday-post-11feb/content.txt) |
| 2026-02-12 | 2m | two_sigma_breakout | short | 11:14 | 6876.32 | 6900.84 | 6850.97 | 12:56 | 6850.97 | target | 25.35 | 3.54 | 25.86 | [2026-02-12_intraday-post-12feb](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-12_intraday-post-12feb/content.txt) |
| 2026-02-12 | 2m | three_sigma_exhaustion | long | 12:56 | 6851.00 | 6820.89 | 6881.18 | 16:00 | 6832.76 | close | -18.24 | 26.96 | 28.13 | [2026-02-12_intraday-post-12feb](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-12_intraday-post-12feb/content.txt) |
| 2026-02-13 | 2m | risk_fade | short | 13:00 | 6865.10 | 6899.56 | 6832.76 | 15:24 | 6832.76 | target | 32.34 | 15.84 | 33.92 | [2026-02-13_cpi-day-intraday-post-13feb](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-13_cpi-day-intraday-post-13feb/content.txt) |
| 2026-02-17 | 1m | risk_fade | long | 10:18 | 6810.13 | 6769.97 | 6836.17 | 11:12 | 6836.17 | target | 26.04 | 34.63 | 27.82 | [2026-02-17_intraday-post-17feb](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-17_intraday-post-17feb/content.txt) |
| 2026-02-18 | 1m | risk_fade | short | 14:31 | 6876.98 | 6910.93 | 6843.22 | 16:00 | 6881.32 | close | -4.34 | 12.77 | 19.53 | [2026-02-18_intraday-post-18feb-f49](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-18_intraday-post-18feb-f49/content.txt) |
| 2026-02-19 | 1m | risk_fade | long | 13:46 | 6849.21 | 6817.05 | 6881.31 | 16:00 | 6861.90 | close | 12.69 | 12.34 | 12.73 | [2026-02-19_intraday-post-19feb-a5e](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-19_intraday-post-19feb-a5e/content.txt) |
| 2026-02-20 | 1m | risk_fade | short | 10:02 | 6893.54 | 6927.43 | 6861.89 | 10:16 | 6861.89 | target | 31.65 | 2.25 | 34.03 | [2026-02-20_intraday-post-20feb-589](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-20_intraday-post-20feb-589/content.txt) |
| 2026-02-23 | 1m | two_sigma_breakout | short | 10:55 | 6841.53 | 6867.50 | 6815.94 | 16:00 | 6837.79 | close | 3.74 | 14.02 | 21.71 | [2026-02-23_intraday-post-23feb](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-23_intraday-post-23feb/content.txt) |
| 2026-02-24 | 1m | risk_fade | short | 11:31 | 6870.46 | 6905.86 | 6837.75 | 16:00 | 6890.11 | close | -19.65 | 28.71 | 2.74 | [2026-02-24_intraday-post-24feb-2cc](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-24_intraday-post-24feb-2cc/content.txt) |
| 2026-02-25 | 1m | risk_fade | short | 10:07 | 6921.15 | 6954.20 | 6890.07 | 16:00 | 6946.20 | close | -25.05 | 31.36 | 2.79 | [2026-02-25_intraday-post-25feb-8ed](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-25_intraday-post-25feb-8ed/content.txt) |
| 2026-02-26 | 1m | two_sigma_breakout | short | 10:19 | 6880.15 | 6905.23 | 6855.05 | 11:16 | 6905.23 | stop | -25.08 | 26.31 | 20.42 | [2026-02-26_nvda-er-intraday-post-26feb](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-26_nvda-er-intraday-post-26feb/content.txt) |
| 2026-02-27 | 1m | two_sigma_breakout | short | 09:42 | 6838.59 | 6866.90 | 6815.40 | 10:08 | 6866.90 | stop | -28.31 | 29.99 | 6.85 | [2026-02-27_ppi-intraday-post-27feb](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-27_ppi-intraday-post-27feb/content.txt) |
| 2026-02-27 | 1m | risk_fade | long | 15:59 | 6878.04 | 6846.60 | 6908.86 | 16:00 | 6878.88 | close | 0.84 | 0.00 | 0.87 | [2026-02-27_ppi-intraday-post-27feb](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-27_ppi-intraday-post-27feb/content.txt) |
| 2026-03-02 | 1m | two_sigma_breakout | short | 09:31 | 6814.91 | 6835.87 | 6783.08 | 09:37 | 6835.87 | stop | -20.96 | 25.72 | 3.86 | [2026-03-02_intraday-post-02march](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-03-02_intraday-post-02march/content.txt) |
| 2026-03-02 | 1m | risk_fade | long | 09:49 | 6848.12 | 6815.06 | 6878.88 | 11:44 | 6878.88 | target | 30.76 | 13.21 | 32.13 | [2026-03-02_intraday-post-02march](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-03-02_intraday-post-02march/content.txt) |
| 2026-03-05 | 1m | risk_fade | long | 10:50 | 6835.72 | 6801.79 | 6869.50 | 12:30 | 6801.79 | stop | -33.93 | 37.17 | 4.53 | [2026-03-05_intraday-post-05march](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-03-05_intraday-post-05march/content.txt) |
| 2026-03-05 | 1m | two_sigma_breakout | short | 12:31 | 6790.10 | 6823.86 | 6767.84 | 15:50 | 6823.86 | stop | -33.76 | 35.10 | 19.32 | [2026-03-05_intraday-post-05march](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-03-05_intraday-post-05march/content.txt) |
| 2026-03-06 | 1m | two_sigma_breakout | short | 09:31 | 6739.29 | 6782.91 | 6724.24 | 09:35 | 6724.24 | target | 15.05 | 4.20 | 17.83 | [2026-03-06_intraday-post-06march](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-03-06_intraday-post-06march/content.txt) |
| 2026-03-06 | 1m | three_sigma_exhaustion | long | 09:38 | 6728.90 | 6688.87 | 6759.79 | 10:31 | 6759.79 | target | 30.89 | 17.34 | 33.78 | [2026-03-06_intraday-post-06march](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-03-06_intraday-post-06march/content.txt) |
| 2026-03-09 | 1m | two_sigma_breakout | short | 09:50 | 6646.09 | 6680.97 | 6608.46 | 10:45 | 6680.97 | stop | -34.88 | 37.22 | 10.05 | [2026-03-09_intraday-post-09march](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-03-09_intraday-post-09march/content.txt) |
| 2026-03-09 | 1m | risk_fade | long | 11:03 | 6697.71 | 6652.41 | 6740.02 | 15:19 | 6740.02 | target | 42.31 | 6.30 | 48.47 | [2026-03-09_intraday-post-09march](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-03-09_intraday-post-09march/content.txt) |
| 2026-03-09 | 1m | risk_fade | short | 15:50 | 6783.39 | 6827.63 | 6740.02 | 16:00 | 6795.90 | close | -12.51 | 16.14 | 2.21 | [2026-03-09_intraday-post-09march](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-03-09_intraday-post-09march/content.txt) |
| 2026-03-12 | 1m | two_sigma_breakout | short | 09:56 | 6696.27 | 6724.35 | 6661.19 | 16:00 | 6672.58 | close | 23.69 | 26.61 | 25.87 | [2026-03-12_intraday-post-12march-fa0](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-03-12_intraday-post-12march-fa0/content.txt) |
| 2026-03-13 | 1m | risk_fade | short | 10:02 | 6708.49 | 6755.53 | 6672.62 | 10:55 | 6672.62 | target | 35.87 | 6.57 | 37.73 | [2026-03-13_pce-breakdown-intraday-post-13march](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-03-13_pce-breakdown-intraday-post-13march/content.txt) |
