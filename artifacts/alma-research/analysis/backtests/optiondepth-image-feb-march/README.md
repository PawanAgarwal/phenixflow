# OptionDepth Image Feb-March Backtest

This backtest is a first `image-native` POC for Alma's archived `OptionsDepth Heatmap` posts.

Method summary:
- OD curves are segmented locally from the archived image.
- The chart is price-calibrated using the same post's OD commentary plus nearby script range candidates.
- Trade rules are deterministic after extraction.
- Signals tested: `zero_line_breakout`, `corridor_reversion`, `late_day_pin`.

## Coverage

- Daily OD posts considered: 29
- Days with usable script-backed calibration: 28
- Date range: 2026-02-02 to 2026-03-13
- Yahoo interval usage: 1m=18, 2m=10

## Overall

| Bucket | Trades | Wins | Losses | Win rate | Gross profit | Gross loss | Net profit | Max drawdown | Stops | Profit factor |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| All trades | 47 | 20 | 27 | 42.5% | 274.21 | -389.86 | -115.65 | 195.81 | 27 | 0.70 |

## By Strategy

| Bucket | Trades | Wins | Losses | Win rate | Gross profit | Gross loss | Net profit | Max drawdown | Stops | Profit factor |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| corridor_reversion | 33 | 14 | 19 | 42.4% | 177.55 | -268.96 | -91.41 | 140.94 | 19 | 0.66 |
| late_day_pin | 1 | 1 | 0 | 100.0% | 10.10 | 0.00 | 10.10 | 0.00 | 0 |  |
| zero_line_breakout | 13 | 5 | 8 | 38.5% | 86.56 | -120.90 | -34.34 | 86.64 | 8 | 0.72 |

## By Month

| Bucket | Trades | Wins | Losses | Win rate | Gross profit | Gross loss | Net profit | Max drawdown | Stops | Profit factor |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 2026-02 | 29 | 13 | 16 | 44.8% | 214.46 | -231.92 | -17.46 | 87.62 | 16 | 0.92 |
| 2026-03 | 18 | 7 | 11 | 38.9% | 59.75 | -157.94 | -98.19 | 118.19 | 11 | 0.38 |

## Stops Triggered

| Date | Data | Strategy | Side | Entry ET | Entry | Stop | Exit ET | Exit | PnL | Source |
| --- | --- | --- | --- | --- | ---: | ---: | --- | ---: | ---: | --- |
| 2026-02-03 | 2m | corridor_reversion | long | 10:04 | 6952.26 | 6940.72 | 10:12 | 6940.72 | -11.54 | [2026-02-03_warsh-is-not-a-hawk-intraday-post](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-03_warsh-is-not-a-hawk-intraday-post/content.txt) |
| 2026-02-04 | 2m | corridor_reversion | long | 11:56 | 6885.34 | 6871.99 | 12:26 | 6871.99 | -13.35 | [2026-02-04_intraday-post-04feb-038](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-04_intraday-post-04feb-038/content.txt) |
| 2026-02-09 | 2m | zero_line_breakout | short | 09:52 | 6923.78 | 6944.74 | 10:16 | 6944.74 | -20.96 | [2026-02-09_intraday-post-09feb](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-09_intraday-post-09feb/content.txt) |
| 2026-02-10 | 2m | zero_line_breakout | long | 09:52 | 6971.71 | 6957.93 | 13:26 | 6957.93 | -13.78 | [2026-02-10_retail-sales-are-stagnant-intraday](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-10_retail-sales-are-stagnant-intraday/content.txt) |
| 2026-02-13 | 2m | zero_line_breakout | short | 09:50 | 6826.81 | 6844.06 | 10:10 | 6844.06 | -17.25 | [2026-02-13_cpi-day-intraday-post-13feb](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-13_cpi-day-intraday-post-13feb/content.txt) |
| 2026-02-17 | 1m | corridor_reversion | long | 09:31 | 6832.23 | 6806.19 | 09:50 | 6806.19 | -26.04 | [2026-02-17_intraday-post-17feb](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-17_intraday-post-17feb/content.txt) |
| 2026-02-17 | 1m | zero_line_breakout | long | 09:49 | 6819.76 | 6806.24 | 09:50 | 6806.24 | -13.52 | [2026-02-17_intraday-post-17feb](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-17_intraday-post-17feb/content.txt) |
| 2026-02-18 | 1m | zero_line_breakout | short | 09:50 | 6855.79 | 6868.44 | 09:54 | 6868.44 | -12.65 | [2026-02-18_intraday-post-18feb-f49](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-18_intraday-post-18feb-f49/content.txt) |
| 2026-02-19 | 1m | corridor_reversion | long | 09:31 | 6860.93 | 6850.07 | 09:35 | 6850.07 | -10.86 | [2026-02-19_intraday-post-19feb-a5e](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-19_intraday-post-19feb-a5e/content.txt) |
| 2026-02-19 | 1m | corridor_reversion | short | 09:59 | 6867.38 | 6879.11 | 10:03 | 6879.11 | -11.73 | [2026-02-19_intraday-post-19feb-a5e](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-19_intraday-post-19feb-a5e/content.txt) |
| 2026-02-20 | 1m | corridor_reversion | short | 09:48 | 6859.80 | 6868.50 | 10:00 | 6868.50 | -8.70 | [2026-02-20_intraday-post-20feb-589](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-20_intraday-post-20feb-589/content.txt) |
| 2026-02-20 | 1m | zero_line_breakout | short | 09:50 | 6856.43 | 6869.02 | 10:00 | 6869.02 | -12.59 | [2026-02-20_intraday-post-20feb-589](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-20_intraday-post-20feb-589/content.txt) |
| 2026-02-23 | 1m | corridor_reversion | long | 09:47 | 6893.18 | 6871.06 | 10:04 | 6871.06 | -22.12 | [2026-02-23_intraday-post-23feb](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-23_intraday-post-23feb/content.txt) |
| 2026-02-24 | 1m | zero_line_breakout | long | 09:47 | 6864.88 | 6851.27 | 09:56 | 6851.27 | -13.61 | [2026-02-24_intraday-post-24feb-2cc](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-24_intraday-post-24feb-2cc/content.txt) |
| 2026-02-24 | 1m | corridor_reversion | short | 11:15 | 6881.69 | 6890.53 | 12:11 | 6890.53 | -8.84 | [2026-02-24_intraday-post-24feb-2cc](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-24_intraday-post-24feb-2cc/content.txt) |
| 2026-02-27 | 1m | corridor_reversion | long | 09:32 | 6852.89 | 6838.51 | 09:43 | 6838.51 | -14.38 | [2026-02-27_ppi-intraday-post-27feb](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-27_ppi-intraday-post-27feb/content.txt) |
| 2026-03-02 | 1m | corridor_reversion | short | 10:21 | 6855.27 | 6866.12 | 11:07 | 6866.12 | -10.85 | [2026-03-02_intraday-post-02march](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-03-02_intraday-post-02march/content.txt) |
| 2026-03-04 | 1m | corridor_reversion | long | 09:32 | 6841.49 | 6821.47 | 09:40 | 6821.47 | -20.02 | [2026-03-04_nothing-new-in-the-middle-east-intraday](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-03-04_nothing-new-in-the-middle-east-intraday/content.txt) |
| 2026-03-04 | 1m | corridor_reversion | short | 09:36 | 6824.31 | 6846.69 | 10:10 | 6846.69 | -22.38 | [2026-03-04_nothing-new-in-the-middle-east-intraday](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-03-04_nothing-new-in-the-middle-east-intraday/content.txt) |
| 2026-03-05 | 1m | corridor_reversion | long | 10:37 | 6819.74 | 6805.24 | 12:21 | 6805.24 | -14.50 | [2026-03-05_intraday-post-05march](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-03-05_intraday-post-05march/content.txt) |
| 2026-03-06 | 1m | corridor_reversion | long | 10:37 | 6770.91 | 6761.99 | 10:41 | 6761.99 | -8.92 | [2026-03-06_intraday-post-06march](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-03-06_intraday-post-06march/content.txt) |
| 2026-03-09 | 1m | corridor_reversion | long | 09:34 | 6675.17 | 6662.99 | 09:38 | 6662.99 | -12.18 | [2026-03-09_intraday-post-09march](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-03-09_intraday-post-09march/content.txt) |
| 2026-03-09 | 1m | corridor_reversion | short | 11:36 | 6698.98 | 6707.12 | 11:53 | 6707.12 | -8.14 | [2026-03-09_intraday-post-09march](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-03-09_intraday-post-09march/content.txt) |
| 2026-03-10 | 1m | corridor_reversion | long | 10:35 | 6802.01 | 6786.29 | 15:29 | 6786.29 | -15.72 | [2026-03-10_intraday-post-10march-a1a](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-03-10_intraday-post-10march-a1a/content.txt) |
| 2026-03-11 | 1m | zero_line_breakout | short | 09:47 | 6781.20 | 6797.74 | 10:00 | 6797.74 | -16.54 | [2026-03-11_intraday-post-11march](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-03-11_intraday-post-11march/content.txt) |
| 2026-03-11 | 1m | corridor_reversion | short | 09:48 | 6777.89 | 6798.36 | 10:00 | 6798.36 | -20.47 | [2026-03-11_intraday-post-11march](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-03-11_intraday-post-11march/content.txt) |
| 2026-03-13 | 1m | corridor_reversion | short | 10:47 | 6673.94 | 6682.16 | 10:50 | 6682.16 | -8.22 | [2026-03-13_pce-breakdown-intraday-post-13march](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-03-13_pce-breakdown-intraday-post-13march/content.txt) |

## All Trades

| Date | Data | Strategy | Side | Entry ET | Entry | Stop | Target | Exit ET | Exit | Exit reason | PnL | MAE | MFE | Source |
| --- | --- | --- | --- | --- | ---: | ---: | ---: | --- | ---: | --- | ---: | ---: | ---: | --- |
| 2026-02-03 | 2m | corridor_reversion | short | 09:32 | 6982.93 | 6992.33 | 6972.93 | 09:42 | 6972.93 | target | 10.00 | 4.06 | 12.20 | [2026-02-03_warsh-is-not-a-hawk-intraday-post](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-03_warsh-is-not-a-hawk-intraday-post/content.txt) |
| 2026-02-03 | 2m | corridor_reversion | long | 10:04 | 6952.26 | 6940.72 | 6984.86 | 10:12 | 6940.72 | stop | -11.54 | 13.95 | 0.00 | [2026-02-03_warsh-is-not-a-hawk-intraday-post](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-03_warsh-is-not-a-hawk-intraday-post/content.txt) |
| 2026-02-04 | 2m | zero_line_breakout | short | 09:52 | 6912.29 | 6935.10 | 6877.99 | 11:42 | 6877.99 | target | 34.30 | 17.71 | 34.53 | [2026-02-04_intraday-post-04feb-038](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-04_intraday-post-04feb-038/content.txt) |
| 2026-02-04 | 2m | corridor_reversion | long | 11:56 | 6885.34 | 6871.99 | 6927.10 | 12:26 | 6871.99 | stop | -13.35 | 27.80 | 6.64 | [2026-02-04_intraday-post-04feb-038](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-04_intraday-post-04feb-038/content.txt) |
| 2026-02-05 | 2m | corridor_reversion | long | 09:36 | 6839.12 | 6828.48 | 6849.12 | 09:42 | 6849.12 | target | 10.00 | 3.18 | 13.26 | [2026-02-05_volatility-is-back-intraday-post](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-05_volatility-is-back-intraday-post/content.txt) |
| 2026-02-05 | 2m | zero_line_breakout | short | 09:58 | 6827.33 | 6842.56 | 6809.33 | 10:04 | 6809.33 | target | 18.00 | 5.38 | 19.50 | [2026-02-05_volatility-is-back-intraday-post](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-05_volatility-is-back-intraday-post/content.txt) |
| 2026-02-09 | 2m | zero_line_breakout | short | 09:52 | 6923.78 | 6944.74 | 6893.23 | 10:16 | 6944.74 | stop | -20.96 | 21.94 | 3.41 | [2026-02-09_intraday-post-09feb](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-09_intraday-post-09feb/content.txt) |
| 2026-02-10 | 2m | corridor_reversion | long | 09:52 | 6971.71 | 6960.10 | 6981.71 | 10:06 | 6981.71 | target | 10.00 | 1.42 | 13.50 | [2026-02-10_retail-sales-are-stagnant-intraday](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-10_retail-sales-are-stagnant-intraday/content.txt) |
| 2026-02-10 | 2m | zero_line_breakout | long | 09:52 | 6971.71 | 6957.93 | 6992.02 | 13:26 | 6957.93 | stop | -13.78 | 15.31 | 15.12 | [2026-02-10_retail-sales-are-stagnant-intraday](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-10_retail-sales-are-stagnant-intraday/content.txt) |
| 2026-02-12 | 2m | zero_line_breakout | short | 09:58 | 6945.93 | 6961.29 | 6927.93 | 10:40 | 6927.93 | target | 18.00 | 10.96 | 21.32 | [2026-02-12_intraday-post-12feb](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-12_intraday-post-12feb/content.txt) |
| 2026-02-13 | 2m | corridor_reversion | short | 09:32 | 6833.14 | 6847.60 | 6823.14 | 09:36 | 6823.14 | target | 10.00 | 5.39 | 12.94 | [2026-02-13_cpi-day-intraday-post-13feb](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-13_cpi-day-intraday-post-13feb/content.txt) |
| 2026-02-13 | 2m | corridor_reversion | long | 09:42 | 6816.57 | 6805.02 | 6836.06 | 09:44 | 6836.06 | target | 19.49 | 4.21 | 23.59 | [2026-02-13_cpi-day-intraday-post-13feb](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-13_cpi-day-intraday-post-13feb/content.txt) |
| 2026-02-13 | 2m | zero_line_breakout | short | 09:50 | 6826.81 | 6844.06 | 6811.02 | 10:10 | 6844.06 | stop | -17.25 | 20.36 | 11.77 | [2026-02-13_cpi-day-intraday-post-13feb](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-13_cpi-day-intraday-post-13feb/content.txt) |
| 2026-02-17 | 1m | corridor_reversion | long | 09:31 | 6832.23 | 6806.19 | 6846.66 | 09:50 | 6806.19 | stop | -26.04 | 30.89 | 13.01 | [2026-02-17_intraday-post-17feb](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-17_intraday-post-17feb/content.txt) |
| 2026-02-17 | 1m | zero_line_breakout | long | 09:49 | 6819.76 | 6806.24 | 6837.76 | 09:50 | 6806.24 | stop | -13.52 | 18.42 | 2.58 | [2026-02-17_intraday-post-17feb](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-17_intraday-post-17feb/content.txt) |
| 2026-02-17 | 1m | corridor_reversion | short | 09:50 | 6802.30 | 6833.00 | 6787.87 | 09:58 | 6787.87 | target | 14.43 | 3.06 | 15.28 | [2026-02-17_intraday-post-17feb](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-17_intraday-post-17feb/content.txt) |
| 2026-02-18 | 1m | zero_line_breakout | short | 09:50 | 6855.79 | 6868.44 | 6823.72 | 09:54 | 6868.44 | stop | -12.65 | 16.08 | 0.75 | [2026-02-18_intraday-post-18feb-f49](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-18_intraday-post-18feb-f49/content.txt) |
| 2026-02-18 | 1m | corridor_reversion | short | 14:31 | 6876.98 | 6886.26 | 6860.44 | 14:57 | 6860.44 | target | 16.54 | 2.17 | 18.30 | [2026-02-18_intraday-post-18feb-f49](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-18_intraday-post-18feb-f49/content.txt) |
| 2026-02-19 | 1m | corridor_reversion | long | 09:31 | 6860.93 | 6850.07 | 6870.93 | 09:35 | 6850.07 | stop | -10.86 | 12.01 | 0.80 | [2026-02-19_intraday-post-19feb-a5e](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-19_intraday-post-19feb-a5e/content.txt) |
| 2026-02-19 | 1m | zero_line_breakout | long | 09:51 | 6860.61 | 6848.95 | 6873.11 | 09:56 | 6873.11 | target | 12.50 | 0.33 | 14.36 | [2026-02-19_intraday-post-19feb-a5e](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-19_intraday-post-19feb-a5e/content.txt) |
| 2026-02-19 | 1m | corridor_reversion | short | 09:59 | 6867.38 | 6879.11 | 6856.95 | 10:03 | 6879.11 | stop | -11.73 | 11.74 | 6.15 | [2026-02-19_intraday-post-19feb-a5e](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-19_intraday-post-19feb-a5e/content.txt) |
| 2026-02-20 | 1m | corridor_reversion | short | 09:48 | 6859.80 | 6868.50 | 6849.80 | 10:00 | 6868.50 | stop | -8.70 | 12.14 | 8.04 | [2026-02-20_intraday-post-20feb-589](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-20_intraday-post-20feb-589/content.txt) |
| 2026-02-20 | 1m | zero_line_breakout | short | 09:50 | 6856.43 | 6869.02 | 6822.07 | 10:00 | 6869.02 | stop | -12.59 | 15.51 | 4.67 | [2026-02-20_intraday-post-20feb-589](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-20_intraday-post-20feb-589/content.txt) |
| 2026-02-23 | 1m | corridor_reversion | short | 09:40 | 6900.87 | 6914.72 | 6890.87 | 09:45 | 6890.87 | target | 10.00 | 1.64 | 11.24 | [2026-02-23_intraday-post-23feb](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-23_intraday-post-23feb/content.txt) |
| 2026-02-23 | 1m | corridor_reversion | long | 09:47 | 6893.18 | 6871.06 | 6908.47 | 10:04 | 6871.06 | stop | -22.12 | 24.37 | 4.91 | [2026-02-23_intraday-post-23feb](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-23_intraday-post-23feb/content.txt) |
| 2026-02-24 | 1m | corridor_reversion | long | 09:32 | 6828.07 | 6804.88 | 6859.27 | 09:45 | 6859.27 | target | 31.20 | 12.64 | 33.39 | [2026-02-24_intraday-post-24feb-2cc](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-24_intraday-post-24feb-2cc/content.txt) |
| 2026-02-24 | 1m | zero_line_breakout | long | 09:47 | 6864.88 | 6851.27 | 6884.53 | 09:56 | 6851.27 | stop | -13.61 | 18.08 | 5.41 | [2026-02-24_intraday-post-24feb-2cc](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-24_intraday-post-24feb-2cc/content.txt) |
| 2026-02-24 | 1m | corridor_reversion | short | 11:15 | 6881.69 | 6890.53 | 6859.27 | 12:11 | 6890.53 | stop | -8.84 | 9.67 | 13.97 | [2026-02-24_intraday-post-24feb-2cc](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-24_intraday-post-24feb-2cc/content.txt) |
| 2026-02-27 | 1m | corridor_reversion | long | 09:32 | 6852.89 | 6838.51 | 6862.89 | 09:43 | 6838.51 | stop | -14.38 | 21.15 | 4.15 | [2026-02-27_ppi-intraday-post-27feb](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-27_ppi-intraday-post-27feb/content.txt) |
| 2026-03-02 | 1m | corridor_reversion | long | 09:31 | 6814.91 | 6792.34 | 6824.91 | 09:34 | 6824.91 | target | 10.00 | 3.86 | 14.36 | [2026-03-02_intraday-post-02march](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-03-02_intraday-post-02march/content.txt) |
| 2026-03-02 | 1m | corridor_reversion | short | 10:21 | 6855.27 | 6866.12 | 6797.95 | 11:07 | 6866.12 | stop | -10.85 | 11.02 | 15.12 | [2026-03-02_intraday-post-02march](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-03-02_intraday-post-02march/content.txt) |
| 2026-03-04 | 1m | corridor_reversion | long | 09:32 | 6841.49 | 6821.47 | 6855.07 | 09:40 | 6821.47 | stop | -20.02 | 22.28 | 3.04 | [2026-03-04_nothing-new-in-the-middle-east-intraday](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-03-04_nothing-new-in-the-middle-east-intraday/content.txt) |
| 2026-03-04 | 1m | corridor_reversion | short | 09:36 | 6824.31 | 6846.69 | 6779.80 | 10:10 | 6846.69 | stop | -22.38 | 23.09 | 12.67 | [2026-03-04_nothing-new-in-the-middle-east-intraday](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-03-04_nothing-new-in-the-middle-east-intraday/content.txt) |
| 2026-03-04 | 1m | late_day_pin | short | 14:00 | 6879.58 | 6895.54 | 6863.62 | 16:00 | 6869.48 | close | 10.10 | 6.36 | 12.03 | [2026-03-04_nothing-new-in-the-middle-east-intraday](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-03-04_nothing-new-in-the-middle-east-intraday/content.txt) |
| 2026-03-05 | 1m | corridor_reversion | short | 09:39 | 6851.79 | 6865.65 | 6850.57 | 09:40 | 6850.57 | target | 1.22 | 0.42 | 7.44 | [2026-03-05_intraday-post-05march](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-03-05_intraday-post-05march/content.txt) |
| 2026-03-05 | 1m | zero_line_breakout | long | 09:55 | 6853.69 | 6842.57 | 6857.45 | 09:57 | 6857.45 | target | 3.76 | 1.96 | 6.94 | [2026-03-05_intraday-post-05march](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-03-05_intraday-post-05march/content.txt) |
| 2026-03-05 | 1m | corridor_reversion | long | 10:37 | 6819.74 | 6805.24 | 6850.57 | 12:21 | 6805.24 | stop | -14.50 | 15.64 | 20.51 | [2026-03-05_intraday-post-05march](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-03-05_intraday-post-05march/content.txt) |
| 2026-03-06 | 1m | corridor_reversion | long | 10:37 | 6770.91 | 6761.99 | 6780.91 | 10:41 | 6761.99 | stop | -8.92 | 10.28 | 0.82 | [2026-03-06_intraday-post-06march](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-03-06_intraday-post-06march/content.txt) |
| 2026-03-09 | 1m | corridor_reversion | long | 09:34 | 6675.17 | 6662.99 | 6685.17 | 09:38 | 6662.99 | stop | -12.18 | 13.75 | 1.33 | [2026-03-09_intraday-post-09march](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-03-09_intraday-post-09march/content.txt) |
| 2026-03-09 | 1m | corridor_reversion | short | 11:36 | 6698.98 | 6707.12 | 6624.13 | 11:53 | 6707.12 | stop | -8.14 | 9.55 | 2.16 | [2026-03-09_intraday-post-09march](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-03-09_intraday-post-09march/content.txt) |
| 2026-03-10 | 1m | corridor_reversion | short | 09:33 | 6786.09 | 6805.71 | 6775.70 | 09:42 | 6775.70 | target | 10.39 | 5.25 | 12.05 | [2026-03-10_intraday-post-10march-a1a](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-03-10_intraday-post-10march-a1a/content.txt) |
| 2026-03-10 | 1m | corridor_reversion | long | 10:35 | 6802.01 | 6786.29 | 6868.29 | 15:29 | 6786.29 | stop | -15.72 | 16.02 | 43.07 | [2026-03-10_intraday-post-10march-a1a](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-03-10_intraday-post-10march-a1a/content.txt) |
| 2026-03-11 | 1m | corridor_reversion | long | 09:32 | 6794.87 | 6771.84 | 6809.15 | 10:01 | 6809.15 | target | 14.28 | 21.23 | 16.28 | [2026-03-11_intraday-post-11march](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-03-11_intraday-post-11march/content.txt) |
| 2026-03-11 | 1m | zero_line_breakout | short | 09:47 | 6781.20 | 6797.74 | 6763.20 | 10:00 | 6797.74 | stop | -16.54 | 27.00 | 7.56 | [2026-03-11_intraday-post-11march](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-03-11_intraday-post-11march/content.txt) |
| 2026-03-11 | 1m | corridor_reversion | short | 09:48 | 6777.89 | 6798.36 | 6763.61 | 10:00 | 6798.36 | stop | -20.47 | 30.31 | 4.25 | [2026-03-11_intraday-post-11march](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-03-11_intraday-post-11march/content.txt) |
| 2026-03-13 | 1m | corridor_reversion | short | 10:47 | 6673.94 | 6682.16 | 6637.91 | 10:50 | 6682.16 | stop | -8.22 | 10.62 | 0.00 | [2026-03-13_pce-breakdown-intraday-post-13march](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-03-13_pce-breakdown-intraday-post-13march/content.txt) |
| 2026-03-13 | 1m | corridor_reversion | long | 13:55 | 6638.32 | 6630.06 | 6648.32 | 14:32 | 6648.32 | target | 10.00 | 4.32 | 11.74 | [2026-03-13_pce-breakdown-intraday-post-13march](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-03-13_pce-breakdown-intraday-post-13march/content.txt) |

## Calibration Diagnostics

| Date | Data | Usable | Score | Coverage | Avg err | Breakout | Support zone | Resistance zone | Pin zone | Source |
| --- | --- | --- | ---: | ---: | ---: | ---: | --- | --- | --- | --- |
| 2026-02-02 | 2m | yes | 8 | 88.9% | 2.2 | 6923.41 | 6889.77-6889.77 | 6925.33-6935.99 |  | [2026-02-02_intraday-post-02feb](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-02_intraday-post-02feb/content.txt) |
| 2026-02-03 | 2m | yes | 11 | 100.0% | 1.9 | 6984.86 | 6946.72-6946.72 | 6986.33-6986.33 |  | [2026-02-03_warsh-is-not-a-hawk-intraday-post](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-03_warsh-is-not-a-hawk-intraday-post/content.txt) |
| 2026-02-04 | 2m | yes | 10 | 100.0% | 1.3 | 6927.10 | 6877.99-6877.99 | 6942.12-6942.12 | 6957.75-6970.54 | [2026-02-04_intraday-post-04feb-038](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-04_intraday-post-04feb-038/content.txt) |
| 2026-02-05 | 2m | yes | 10 | 76.9% | 4.0 | 6834.56 | 6834.48-6834.48 | 6904.46-6904.46 | 6922.56-6931.65 | [2026-02-05_volatility-is-back-intraday-post](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-05_volatility-is-back-intraday-post/content.txt) |
| 2026-02-06 | 2m | yes | 12 | 85.7% | 3.2 | 6844.91 | 6806.16-6806.16 | 6843.91-6843.91 | 6767.64-6773.06 | [2026-02-06_high-volume-intraday-post-06feb](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-06_high-volume-intraday-post-06feb/content.txt) |
| 2026-02-09 | 2m | yes | 10 | 100.0% | 1.0 | 6936.74 | 6893.23-6893.23 | 6919.30-6919.30 |  | [2026-02-09_intraday-post-09feb](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-09_intraday-post-09feb/content.txt) |
| 2026-02-10 | 2m | yes | 8 | 100.0% | 4.1 | 6965.93 | 6966.10-6966.10 | 6992.02-6992.02 | 6876.60-6887.97 | [2026-02-10_retail-sales-are-stagnant-intraday](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-10_retail-sales-are-stagnant-intraday/content.txt) |
| 2026-02-11 | 2m | yes | 11 | 100.0% | 1.1 | 6948.29 | 6958.06-6958.06 | 7006.41-7014.77 | 6876.84-6887.86 | [2026-02-11_nfp-intraday-post-11feb](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-11_nfp-intraday-post-11feb/content.txt) |
| 2026-02-12 | 2m | yes | 17 | 100.0% | 0.4 | 6953.29 | 6954.34-6954.34 | 6987.44-6987.44 | 7045.89-7054.34 | [2026-02-12_intraday-post-12feb](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-12_intraday-post-12feb/content.txt) |
| 2026-02-13 | 2m | yes | 10 | 100.0% | 1.7 | 6836.06 | 6811.02-6811.02 | 6841.60-6841.60 |  | [2026-02-13_cpi-day-intraday-post-13feb](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-13_cpi-day-intraday-post-13feb/content.txt) |
| 2026-02-17 | 1m | yes | 11 | 100.0% | 1.5 | 6814.24 | 6814.44-6824.75 | 6814.44-6824.75 | 6783.13-6790.00 | [2026-02-17_intraday-post-17feb](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-17_intraday-post-17feb/content.txt) |
| 2026-02-18 | 1m | yes | 7 | 100.0% | 4.0 | 6860.44 | 6823.72-6823.72 | 6880.26-6880.26 | 6770.18-6786.16 | [2026-02-18_intraday-post-18feb-f49](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-18_intraday-post-18feb-f49/content.txt) |
| 2026-02-19 | 1m | yes | 15 | 88.2% | 1.2 | 6856.95 | 6856.07-6856.07 | 6873.11-6873.11 |  | [2026-02-19_intraday-post-19feb-a5e](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-19_intraday-post-19feb-a5e/content.txt) |
| 2026-02-20 | 1m | yes | 15 | 100.0% | 0.7 | 6861.02 | 6817.84-6826.29 | 6862.50-6862.50 | 6986.88-6999.90 | [2026-02-20_intraday-post-20feb-589](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-20_intraday-post-20feb-589/content.txt) |
| 2026-02-23 | 1m | yes | 11 | 100.0% | 2.0 | 6908.47 | 6879.09-6889.13 | 6904.80-6908.72 | 7031.15-7035.31 | [2026-02-23_intraday-post-23feb](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-23_intraday-post-23feb/content.txt) |
| 2026-02-24 | 1m | yes | 10 | 100.0% | 1.6 | 6859.27 | 6813.46-6824.19 | 6884.53-6884.53 | 6808.99-6813.57 | [2026-02-24_intraday-post-24feb-2cc](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-24_intraday-post-24feb-2cc/content.txt) |
| 2026-02-25 | 1m | yes | 15 | 100.0% | 0.7 | 6900.15 | 6883.96-6883.96 | 6917.37-6917.37 | 6880.47-6889.08 | [2026-02-25_intraday-post-25feb-8ed](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-25_intraday-post-25feb-8ed/content.txt) |
| 2026-02-26 | 1m | yes | 10 | 100.0% | 1.7 | 6982.55 | 6921.93-6921.93 | 6958.57-6966.20 |  | [2026-02-26_nvda-er-intraday-post-26feb](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-26_nvda-er-intraday-post-26feb/content.txt) |
| 2026-02-27 | 1m | yes | 9 | 90.0% | 3.0 | 6818.01 | 6844.51-6844.51 | 6904.47-6904.47 |  | [2026-02-27_ppi-intraday-post-27feb](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-27_ppi-intraday-post-27feb/content.txt) |
| 2026-03-02 | 1m | yes | 15 | 100.0% | 1.6 | 6797.95 | 6798.34-6798.34 | 6860.12-6860.12 | 7004.35-7010.00 | [2026-03-02_intraday-post-02march](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-03-02_intraday-post-02march/content.txt) |
| 2026-03-03 |  | no | 0 | 0.0% | 0.0 |  |  |  |  | [2026-03-03_brent-above-80-eu-gas-spikes-intraday](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-03-03_brent-above-80-eu-gas-spikes-intraday/content.txt) |
| 2026-03-04 | 1m | yes | 19 | 90.5% | 0.4 | 6779.80 | 6829.23-6838.93 | 6829.23-6838.93 | 6855.99-6871.31 | [2026-03-04_nothing-new-in-the-middle-east-intraday](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-03-04_nothing-new-in-the-middle-east-intraday/content.txt) |
| 2026-03-05 | 1m | yes | 15 | 93.8% | 0.6 | 6850.57 | 6811.24-6816.74 | 6855.25-6859.65 | 6912.18-6925.38 | [2026-03-05_intraday-post-05march](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-03-05_intraday-post-05march/content.txt) |
| 2026-03-06 | 1m | yes | 13 | 86.7% | 2.1 | 6716.91 | 6767.99-6767.99 | 6788.70-6788.70 | 6830.56-6836.52 | [2026-03-06_intraday-post-06march](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-03-06_intraday-post-06march/content.txt) |
| 2026-03-09 | 1m | yes | 10 | 76.9% | 1.8 | 6624.13 | 6668.99-6672.87 | 6701.12-6701.12 |  | [2026-03-09_intraday-post-09march](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-03-09_intraday-post-09march/content.txt) |
| 2026-03-10 | 1m | yes | 8 | 100.0% | 3.7 | 6868.29 | 6792.29-6799.71 | 6792.29-6799.71 | 6868.57-6872.00 | [2026-03-10_intraday-post-10march-a1a](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-03-10_intraday-post-10march-a1a/content.txt) |
| 2026-03-11 | 1m | yes | 10 | 90.9% | 0.5 | 6789.74 | 6780.00-6790.20 | 6780.00-6790.20 | 6845.62-6860.00 | [2026-03-11_intraday-post-11march](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-03-11_intraday-post-11march/content.txt) |
| 2026-03-12 | 1m | yes | 14 | 100.0% | 0.7 | 6735.33 | 6735.57-6738.77 | 6754.75-6761.15 | 6645.82-6663.03 | [2026-03-12_intraday-post-12march-fa0](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-03-12_intraday-post-12march-fa0/content.txt) |
| 2026-03-13 | 1m | yes | 13 | 100.0% | 0.5 | 6637.91 | 6636.06-6636.06 | 6676.16-6676.16 | 6590.25-6593.39 | [2026-03-13_pce-breakdown-intraday-post-13march](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-03-13_pce-breakdown-intraday-post-13march/content.txt) |
