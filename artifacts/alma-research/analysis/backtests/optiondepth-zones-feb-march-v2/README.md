# OptionDepth Zones Feb-March Backtest

This backtest is a second-pass `image-native` POC for Alma's archived `OptionsDepth Heatmap` posts.

Method summary:
- OD curves are segmented locally from the archived image.
- The chart is price-calibrated using the same post's OD commentary plus nearby script range candidates.
- Trade rules are deterministic after extraction and separate gamma peaks from troughs.
- Signals tested: `peak_rejection`, `zero_to_peak_continuation`, `late_day_pin`.

Candidate tradable subset:
- `candidate_v1`: calibration score `>= 11`, entry time `>= 10:00 ET`, first qualifying trade per day only.
- `candidate_v1_strict`: calibration score `>= 12`, entry time `>= 10:00 ET`, first qualifying trade per day only.

## Coverage

- Daily OD posts considered: 29
- Days with usable script-backed calibration: 28
- Date range: 2026-02-02 to 2026-03-13
- Yahoo interval usage: 1m=18, 2m=10

## Candidate Subset

| Bucket | Trades | Wins | Losses | Win rate | Gross profit | Gross loss | Net profit | Max drawdown | Stops | Profit factor |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| candidate_v1 | 10 | 6 | 4 | 60.0% | 179.99 | -39.02 | 140.97 | 20.04 | 3 | 4.61 |
| candidate_v1_strict | 7 | 5 | 2 | 71.4% | 160.84 | -18.36 | 142.48 | 10.85 | 1 | 8.76 |

## Candidate Trades

| Bucket | Date | Strategy | Side | Entry ET | Entry | Stop | Target | Exit ET | Exit | Exit reason | PnL | Source |
| --- | --- | --- | --- | --- | ---: | ---: | ---: | --- | ---: | --- | ---: | --- |
| candidate_v1 | 2026-02-03 | peak_rejection | long | 10:02 | 6952.19 | 6940.72 | 6984.86 | 10:12 | 6940.72 | stop | -11.47 | [2026-02-03_warsh-is-not-a-hawk-intraday-post](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-03_warsh-is-not-a-hawk-intraday-post/content.txt) |
| candidate_v1 | 2026-02-17 | peak_rejection | long | 11:05 | 6821.36 | 6808.44 | 6840.51 | 11:13 | 6840.51 | target | 19.15 | [2026-02-17_intraday-post-17feb](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-17_intraday-post-17feb/content.txt) |
| candidate_v1 | 2026-02-19 | zero_to_peak_continuation | short | 11:45 | 6854.39 | 6870.95 | 6809.90 | 16:00 | 6861.90 | close | -7.51 | [2026-02-19_intraday-post-19feb-a5e](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-19_intraday-post-19feb-a5e/content.txt) |
| candidate_v1 | 2026-02-20 | zero_to_peak_continuation | long | 10:00 | 6871.94 | 6847.02 | 6969.52 | 16:00 | 6909.50 | close | 37.56 | [2026-02-20_intraday-post-20feb-589](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-20_intraday-post-20feb-589/content.txt) |
| candidate_v1 | 2026-02-23 | peak_rejection | long | 10:02 | 6882.28 | 6873.09 | 6889.13 | 10:04 | 6873.09 | stop | -9.19 | [2026-02-23_intraday-post-23feb](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-23_intraday-post-23feb/content.txt) |
| candidate_v1 | 2026-03-02 | peak_rejection | short | 10:21 | 6855.27 | 6866.12 | 6797.95 | 11:07 | 6866.12 | stop | -10.85 | [2026-03-02_intraday-post-02march](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-03-02_intraday-post-02march/content.txt) |
| candidate_v1 | 2026-03-04 | peak_rejection | long | 10:00 | 6837.98 | 6823.23 | 6863.55 | 10:21 | 6863.55 | target | 25.57 | [2026-03-04_nothing-new-in-the-middle-east-intraday](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-03-04_nothing-new-in-the-middle-east-intraday/content.txt) |
| candidate_v1 | 2026-03-05 | zero_to_peak_continuation | short | 10:19 | 6844.34 | 6864.57 | 6770.81 | 14:32 | 6770.81 | target | 73.53 | [2026-03-05_intraday-post-05march](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-03-05_intraday-post-05march/content.txt) |
| candidate_v1 | 2026-03-06 | peak_rejection | long | 10:23 | 6752.57 | 6744.22 | 6767.99 | 10:36 | 6767.99 | target | 15.42 | [2026-03-06_intraday-post-06march](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-03-06_intraday-post-06march/content.txt) |
| candidate_v1 | 2026-03-13 | peak_rejection | short | 10:10 | 6700.33 | 6707.84 | 6691.57 | 10:17 | 6691.57 | target | 8.76 | [2026-03-13_pce-breakdown-intraday-post-13march](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-03-13_pce-breakdown-intraday-post-13march/content.txt) |
| candidate_v1_strict | 2026-02-19 | zero_to_peak_continuation | short | 11:45 | 6854.39 | 6870.95 | 6809.90 | 16:00 | 6861.90 | close | -7.51 | [2026-02-19_intraday-post-19feb-a5e](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-19_intraday-post-19feb-a5e/content.txt) |
| candidate_v1_strict | 2026-02-20 | zero_to_peak_continuation | long | 10:00 | 6871.94 | 6847.02 | 6969.52 | 16:00 | 6909.50 | close | 37.56 | [2026-02-20_intraday-post-20feb-589](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-20_intraday-post-20feb-589/content.txt) |
| candidate_v1_strict | 2026-03-02 | peak_rejection | short | 10:21 | 6855.27 | 6866.12 | 6797.95 | 11:07 | 6866.12 | stop | -10.85 | [2026-03-02_intraday-post-02march](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-03-02_intraday-post-02march/content.txt) |
| candidate_v1_strict | 2026-03-04 | peak_rejection | long | 10:00 | 6837.98 | 6823.23 | 6863.55 | 10:21 | 6863.55 | target | 25.57 | [2026-03-04_nothing-new-in-the-middle-east-intraday](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-03-04_nothing-new-in-the-middle-east-intraday/content.txt) |
| candidate_v1_strict | 2026-03-05 | zero_to_peak_continuation | short | 10:19 | 6844.34 | 6864.57 | 6770.81 | 14:32 | 6770.81 | target | 73.53 | [2026-03-05_intraday-post-05march](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-03-05_intraday-post-05march/content.txt) |
| candidate_v1_strict | 2026-03-06 | peak_rejection | long | 10:23 | 6752.57 | 6744.22 | 6767.99 | 10:36 | 6767.99 | target | 15.42 | [2026-03-06_intraday-post-06march](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-03-06_intraday-post-06march/content.txt) |
| candidate_v1_strict | 2026-03-13 | peak_rejection | short | 10:10 | 6700.33 | 6707.84 | 6691.57 | 10:17 | 6691.57 | target | 8.76 | [2026-03-13_pce-breakdown-intraday-post-13march](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-03-13_pce-breakdown-intraday-post-13march/content.txt) |

## Overall

| Bucket | Trades | Wins | Losses | Win rate | Gross profit | Gross loss | Net profit | Max drawdown | Stops | Profit factor |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| All trades | 44 | 14 | 30 | 31.8% | 320.24 | -424.59 | -104.35 | 183.14 | 29 | 0.75 |

## By Strategy

| Bucket | Trades | Wins | Losses | Win rate | Gross profit | Gross loss | Net profit | Max drawdown | Stops | Profit factor |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| peak_rejection | 27 | 9 | 18 | 33.3% | 148.98 | -208.37 | -59.39 | 92.52 | 18 | 0.71 |
| zero_to_peak_continuation | 17 | 5 | 12 | 29.4% | 171.26 | -216.22 | -44.96 | 123.04 | 11 | 0.79 |

## By Month

| Bucket | Trades | Wins | Losses | Win rate | Gross profit | Gross loss | Net profit | Max drawdown | Stops | Profit factor |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 2026-02 | 30 | 8 | 22 | 26.7% | 173.59 | -313.25 | -139.66 | 172.29 | 21 | 0.55 |
| 2026-03 | 14 | 6 | 8 | 42.9% | 146.65 | -111.34 | 35.31 | 68.94 | 8 | 1.32 |

## Stops Triggered

| Date | Data | Strategy | Side | Entry ET | Entry | Stop | Exit ET | Exit | PnL | Source |
| --- | --- | --- | --- | --- | ---: | ---: | --- | ---: | ---: | --- |
| 2026-02-03 | 2m | peak_rejection | long | 10:02 | 6952.19 | 6940.72 | 10:12 | 6940.72 | -11.47 | [2026-02-03_warsh-is-not-a-hawk-intraday-post](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-03_warsh-is-not-a-hawk-intraday-post/content.txt) |
| 2026-02-04 | 2m | peak_rejection | long | 11:56 | 6885.34 | 6871.99 | 12:26 | 6871.99 | -13.35 | [2026-02-04_intraday-post-04feb-038](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-04_intraday-post-04feb-038/content.txt) |
| 2026-02-05 | 2m | peak_rejection | long | 09:38 | 6845.68 | 6828.48 | 09:56 | 6828.48 | -17.20 | [2026-02-05_volatility-is-back-intraday-post](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-05_volatility-is-back-intraday-post/content.txt) |
| 2026-02-05 | 2m | zero_to_peak_continuation | long | 12:26 | 6838.38 | 6820.56 | 13:10 | 6820.56 | -17.82 | [2026-02-05_volatility-is-back-intraday-post](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-05_volatility-is-back-intraday-post/content.txt) |
| 2026-02-09 | 2m | zero_to_peak_continuation | short | 09:52 | 6923.78 | 6944.74 | 10:16 | 6944.74 | -20.96 | [2026-02-09_intraday-post-09feb](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-09_intraday-post-09feb/content.txt) |
| 2026-02-10 | 2m | peak_rejection | long | 09:50 | 6968.03 | 6960.10 | 13:20 | 6960.10 | -7.93 | [2026-02-10_retail-sales-are-stagnant-intraday](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-10_retail-sales-are-stagnant-intraday/content.txt) |
| 2026-02-10 | 2m | zero_to_peak_continuation | long | 09:52 | 6971.71 | 6956.80 | 13:26 | 6956.80 | -14.91 | [2026-02-10_retail-sales-are-stagnant-intraday](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-10_retail-sales-are-stagnant-intraday/content.txt) |
| 2026-02-13 | 2m | peak_rejection | short | 09:48 | 6833.25 | 6847.60 | 10:28 | 6847.60 | -14.35 | [2026-02-13_cpi-day-intraday-post-13feb](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-13_cpi-day-intraday-post-13feb/content.txt) |
| 2026-02-13 | 2m | zero_to_peak_continuation | short | 09:50 | 6826.81 | 6850.06 | 10:28 | 6850.06 | -23.25 | [2026-02-13_cpi-day-intraday-post-13feb](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-13_cpi-day-intraday-post-13feb/content.txt) |
| 2026-02-17 | 1m | zero_to_peak_continuation | long | 09:49 | 6819.76 | 6805.05 | 09:50 | 6805.05 | -14.71 | [2026-02-17_intraday-post-17feb](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-17_intraday-post-17feb/content.txt) |
| 2026-02-18 | 1m | zero_to_peak_continuation | short | 09:50 | 6855.79 | 6874.44 | 09:55 | 6874.44 | -18.65 | [2026-02-18_intraday-post-18feb-f49](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-18_intraday-post-18feb-f49/content.txt) |
| 2026-02-18 | 1m | peak_rejection | short | 14:31 | 6876.98 | 6886.26 | 15:53 | 6886.26 | -9.28 | [2026-02-18_intraday-post-18feb-f49](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-18_intraday-post-18feb-f49/content.txt) |
| 2026-02-19 | 1m | peak_rejection | long | 09:44 | 6864.33 | 6850.07 | 09:45 | 6850.07 | -14.26 | [2026-02-19_intraday-post-19feb-a5e](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-19_intraday-post-19feb-a5e/content.txt) |
| 2026-02-19 | 1m | zero_to_peak_continuation | long | 09:51 | 6860.61 | 6842.95 | 12:48 | 6842.95 | -17.66 | [2026-02-19_intraday-post-19feb-a5e](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-19_intraday-post-19feb-a5e/content.txt) |
| 2026-02-20 | 1m | peak_rejection | short | 09:48 | 6859.80 | 6868.50 | 10:00 | 6868.50 | -8.70 | [2026-02-20_intraday-post-20feb-589](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-20_intraday-post-20feb-589/content.txt) |
| 2026-02-20 | 1m | zero_to_peak_continuation | short | 09:50 | 6856.43 | 6875.02 | 10:01 | 6875.02 | -18.59 | [2026-02-20_intraday-post-20feb-589](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-20_intraday-post-20feb-589/content.txt) |
| 2026-02-23 | 1m | peak_rejection | long | 10:02 | 6882.28 | 6873.09 | 10:04 | 6873.09 | -9.19 | [2026-02-23_intraday-post-23feb](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-23_intraday-post-23feb/content.txt) |
| 2026-02-24 | 1m | zero_to_peak_continuation | long | 09:47 | 6864.88 | 6850.43 | 09:56 | 6850.43 | -14.45 | [2026-02-24_intraday-post-24feb-2cc](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-24_intraday-post-24feb-2cc/content.txt) |
| 2026-02-24 | 1m | zero_to_peak_continuation | short | 09:55 | 6852.36 | 6873.27 | 10:26 | 6873.27 | -20.91 | [2026-02-24_intraday-post-24feb-2cc](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-24_intraday-post-24feb-2cc/content.txt) |
| 2026-02-24 | 1m | peak_rejection | short | 11:14 | 6882.55 | 6890.53 | 12:11 | 6890.53 | -7.98 | [2026-02-24_intraday-post-24feb-2cc](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-24_intraday-post-24feb-2cc/content.txt) |
| 2026-02-26 | 1m | peak_rejection | long | 09:49 | 6926.05 | 6915.93 | 10:01 | 6915.93 | -10.12 | [2026-02-26_nvda-er-intraday-post-26feb](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-26_nvda-er-intraday-post-26feb/content.txt) |
| 2026-03-02 | 1m | peak_rejection | short | 10:21 | 6855.27 | 6866.12 | 11:07 | 6866.12 | -10.85 | [2026-03-02_intraday-post-02march](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-03-02_intraday-post-02march/content.txt) |
| 2026-03-05 | 1m | peak_rejection | short | 09:39 | 6851.79 | 6865.65 | 09:59 | 6865.65 | -13.86 | [2026-03-05_intraday-post-05march](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-03-05_intraday-post-05march/content.txt) |
| 2026-03-09 | 1m | peak_rejection | short | 11:35 | 6699.50 | 6707.12 | 11:53 | 6707.12 | -7.62 | [2026-03-09_intraday-post-09march](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-03-09_intraday-post-09march/content.txt) |
| 2026-03-10 | 1m | peak_rejection | long | 10:32 | 6796.70 | 6786.29 | 15:29 | 6786.29 | -10.41 | [2026-03-10_intraday-post-10march-a1a](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-03-10_intraday-post-10march-a1a/content.txt) |
| 2026-03-10 | 1m | peak_rejection | short | 11:44 | 6826.81 | 6838.86 | 13:24 | 6838.86 | -12.05 | [2026-03-10_intraday-post-10march-a1a](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-03-10_intraday-post-10march-a1a/content.txt) |
| 2026-03-11 | 1m | peak_rejection | short | 09:48 | 6777.89 | 6799.38 | 10:00 | 6799.38 | -21.49 | [2026-03-11_intraday-post-11march](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-03-11_intraday-post-11march/content.txt) |
| 2026-03-11 | 1m | zero_to_peak_continuation | long | 10:01 | 6804.93 | 6778.13 | 10:23 | 6778.13 | -26.80 | [2026-03-11_intraday-post-11march](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-03-11_intraday-post-11march/content.txt) |
| 2026-03-13 | 1m | peak_rejection | long | 13:55 | 6638.32 | 6630.06 | 15:38 | 6630.06 | -8.26 | [2026-03-13_pce-breakdown-intraday-post-13march](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-03-13_pce-breakdown-intraday-post-13march/content.txt) |

## All Trades

| Date | Data | Strategy | Side | Entry ET | Entry | Stop | Target | Exit ET | Exit | Exit reason | PnL | MAE | MFE | Source |
| --- | --- | --- | --- | --- | ---: | ---: | ---: | --- | ---: | --- | ---: | ---: | ---: | --- |
| 2026-02-03 | 2m | peak_rejection | short | 09:38 | 6978.98 | 6992.33 | 6946.35 | 10:02 | 6946.35 | target | 32.63 | 5.49 | 35.95 | [2026-02-03_warsh-is-not-a-hawk-intraday-post](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-03_warsh-is-not-a-hawk-intraday-post/content.txt) |
| 2026-02-03 | 2m | peak_rejection | long | 10:02 | 6952.19 | 6940.72 | 6984.86 | 10:12 | 6940.72 | stop | -11.47 | 13.88 | 3.22 | [2026-02-03_warsh-is-not-a-hawk-intraday-post](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-03_warsh-is-not-a-hawk-intraday-post/content.txt) |
| 2026-02-04 | 2m | peak_rejection | long | 11:56 | 6885.34 | 6871.99 | 6927.10 | 12:26 | 6871.99 | stop | -13.35 | 27.80 | 6.64 | [2026-02-04_intraday-post-04feb-038](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-04_intraday-post-04feb-038/content.txt) |
| 2026-02-05 | 2m | peak_rejection | long | 09:38 | 6845.68 | 6828.48 | 6904.46 | 09:56 | 6828.48 | stop | -17.20 | 18.32 | 12.17 | [2026-02-05_volatility-is-back-intraday-post](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-05_volatility-is-back-intraday-post/content.txt) |
| 2026-02-05 | 2m | zero_to_peak_continuation | short | 09:58 | 6827.33 | 6848.56 | 6771.67 | 16:00 | 6798.18 | close | 29.15 | 20.24 | 47.20 | [2026-02-05_volatility-is-back-intraday-post](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-05_volatility-is-back-intraday-post/content.txt) |
| 2026-02-05 | 2m | zero_to_peak_continuation | long | 12:26 | 6838.38 | 6820.56 | 6904.46 | 13:10 | 6820.56 | stop | -17.82 | 18.44 | 9.19 | [2026-02-05_volatility-is-back-intraday-post](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-05_volatility-is-back-intraday-post/content.txt) |
| 2026-02-09 | 2m | zero_to_peak_continuation | short | 09:52 | 6923.78 | 6944.74 | 6919.30 | 10:16 | 6944.74 | stop | -20.96 | 21.94 | 3.41 | [2026-02-09_intraday-post-09feb](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-09_intraday-post-09feb/content.txt) |
| 2026-02-10 | 2m | peak_rejection | long | 09:50 | 6968.03 | 6960.10 | 6992.02 | 13:20 | 6960.10 | stop | -7.93 | 10.10 | 18.80 | [2026-02-10_retail-sales-are-stagnant-intraday](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-10_retail-sales-are-stagnant-intraday/content.txt) |
| 2026-02-10 | 2m | zero_to_peak_continuation | long | 09:52 | 6971.71 | 6956.80 | 6992.02 | 13:26 | 6956.80 | stop | -14.91 | 15.31 | 15.12 | [2026-02-10_retail-sales-are-stagnant-intraday](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-10_retail-sales-are-stagnant-intraday/content.txt) |
| 2026-02-13 | 2m | peak_rejection | short | 09:48 | 6833.25 | 6847.60 | 6811.02 | 10:28 | 6847.60 | stop | -14.35 | 18.40 | 18.21 | [2026-02-13_cpi-day-intraday-post-13feb](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-13_cpi-day-intraday-post-13feb/content.txt) |
| 2026-02-13 | 2m | zero_to_peak_continuation | short | 09:50 | 6826.81 | 6850.06 | 6786.76 | 10:28 | 6850.06 | stop | -23.25 | 24.84 | 11.77 | [2026-02-13_cpi-day-intraday-post-13feb](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-13_cpi-day-intraday-post-13feb/content.txt) |
| 2026-02-17 | 1m | peak_rejection | short | 09:38 | 6837.09 | 6846.51 | 6824.75 | 09:44 | 6824.75 | target | 12.34 | 8.15 | 15.27 | [2026-02-17_intraday-post-17feb](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-17_intraday-post-17feb/content.txt) |
| 2026-02-17 | 1m | zero_to_peak_continuation | long | 09:49 | 6819.76 | 6805.05 | 6840.51 | 09:50 | 6805.05 | stop | -14.71 | 18.42 | 2.58 | [2026-02-17_intraday-post-17feb](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-17_intraday-post-17feb/content.txt) |
| 2026-02-17 | 1m | zero_to_peak_continuation | short | 09:51 | 6803.08 | 6823.47 | 6787.88 | 09:58 | 6787.88 | target | 15.20 | 2.28 | 16.06 | [2026-02-17_intraday-post-17feb](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-17_intraday-post-17feb/content.txt) |
| 2026-02-17 | 1m | peak_rejection | long | 11:05 | 6821.36 | 6808.44 | 6840.51 | 11:13 | 6840.51 | target | 19.15 | 2.44 | 21.96 | [2026-02-17_intraday-post-17feb](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-17_intraday-post-17feb/content.txt) |
| 2026-02-18 | 1m | zero_to_peak_continuation | short | 09:50 | 6855.79 | 6874.44 | 6786.16 | 09:55 | 6874.44 | stop | -18.65 | 18.95 | 0.75 | [2026-02-18_intraday-post-18feb-f49](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-18_intraday-post-18feb-f49/content.txt) |
| 2026-02-18 | 1m | zero_to_peak_continuation | long | 09:53 | 6864.44 | 6852.44 | 6880.26 | 09:58 | 6880.26 | target | 15.82 | 0.61 | 17.76 | [2026-02-18_intraday-post-18feb-f49](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-18_intraday-post-18feb-f49/content.txt) |
| 2026-02-18 | 1m | peak_rejection | short | 14:31 | 6876.98 | 6886.26 | 6823.72 | 15:53 | 6886.26 | stop | -9.28 | 10.72 | 19.53 | [2026-02-18_intraday-post-18feb-f49](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-18_intraday-post-18feb-f49/content.txt) |
| 2026-02-19 | 1m | peak_rejection | long | 09:44 | 6864.33 | 6850.07 | 6873.11 | 09:45 | 6850.07 | stop | -14.26 | 14.35 | 0.73 | [2026-02-19_intraday-post-19feb-a5e](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-19_intraday-post-19feb-a5e/content.txt) |
| 2026-02-19 | 1m | zero_to_peak_continuation | long | 09:51 | 6860.61 | 6842.95 | 6908.01 | 12:48 | 6842.95 | stop | -17.66 | 19.24 | 18.51 | [2026-02-19_intraday-post-19feb-a5e](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-19_intraday-post-19feb-a5e/content.txt) |
| 2026-02-19 | 1m | zero_to_peak_continuation | short | 11:45 | 6854.39 | 6870.95 | 6809.90 | 16:00 | 6861.90 | close | -7.51 | 9.66 | 21.33 | [2026-02-19_intraday-post-19feb-a5e](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-19_intraday-post-19feb-a5e/content.txt) |
| 2026-02-20 | 1m | peak_rejection | short | 09:48 | 6859.80 | 6868.50 | 6842.06 | 10:00 | 6868.50 | stop | -8.70 | 12.14 | 8.04 | [2026-02-20_intraday-post-20feb-589](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-20_intraday-post-20feb-589/content.txt) |
| 2026-02-20 | 1m | zero_to_peak_continuation | short | 09:50 | 6856.43 | 6875.02 | 6779.70 | 10:01 | 6875.02 | stop | -18.59 | 51.84 | 4.67 | [2026-02-20_intraday-post-20feb-589](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-20_intraday-post-20feb-589/content.txt) |
| 2026-02-20 | 1m | zero_to_peak_continuation | long | 10:00 | 6871.94 | 6847.02 | 6969.52 | 16:00 | 6909.50 | close | 37.56 | 13.43 | 43.92 | [2026-02-20_intraday-post-20feb-589](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-20_intraday-post-20feb-589/content.txt) |
| 2026-02-23 | 1m | peak_rejection | short | 09:40 | 6900.87 | 6914.72 | 6889.13 | 09:46 | 6889.13 | target | 11.74 | 1.64 | 13.15 | [2026-02-23_intraday-post-23feb](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-23_intraday-post-23feb/content.txt) |
| 2026-02-23 | 1m | peak_rejection | long | 10:02 | 6882.28 | 6873.09 | 6889.13 | 10:04 | 6873.09 | stop | -9.19 | 13.47 | 2.27 | [2026-02-23_intraday-post-23feb](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-23_intraday-post-23feb/content.txt) |
| 2026-02-24 | 1m | zero_to_peak_continuation | long | 09:47 | 6864.88 | 6850.43 | 6884.53 | 09:56 | 6850.43 | stop | -14.45 | 18.08 | 5.41 | [2026-02-24_intraday-post-24feb-2cc](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-24_intraday-post-24feb-2cc/content.txt) |
| 2026-02-24 | 1m | zero_to_peak_continuation | short | 09:55 | 6852.36 | 6873.27 | 6813.46 | 10:26 | 6873.27 | stop | -20.91 | 21.69 | 14.87 | [2026-02-24_intraday-post-24feb-2cc](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-24_intraday-post-24feb-2cc/content.txt) |
| 2026-02-24 | 1m | peak_rejection | short | 11:14 | 6882.55 | 6890.53 | 6824.19 | 12:11 | 6890.53 | stop | -7.98 | 8.81 | 14.83 | [2026-02-24_intraday-post-24feb-2cc](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-24_intraday-post-24feb-2cc/content.txt) |
| 2026-02-26 | 1m | peak_rejection | long | 09:49 | 6926.05 | 6915.93 | 7010.48 | 10:01 | 6915.93 | stop | -10.12 | 13.80 | 4.38 | [2026-02-26_nvda-er-intraday-post-26feb](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-26_nvda-er-intraday-post-26feb/content.txt) |
| 2026-03-02 | 1m | peak_rejection | short | 10:21 | 6855.27 | 6866.12 | 6797.95 | 11:07 | 6866.12 | stop | -10.85 | 11.02 | 15.12 | [2026-03-02_intraday-post-02march](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-03-02_intraday-post-02march/content.txt) |
| 2026-03-04 | 1m | peak_rejection | long | 10:00 | 6837.98 | 6823.23 | 6863.55 | 10:21 | 6863.55 | target | 25.57 | 13.29 | 28.00 | [2026-03-04_nothing-new-in-the-middle-east-intraday](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-03-04_nothing-new-in-the-middle-east-intraday/content.txt) |
| 2026-03-05 | 1m | peak_rejection | short | 09:39 | 6851.79 | 6865.65 | 6831.04 | 09:59 | 6865.65 | stop | -13.86 | 14.14 | 14.22 | [2026-03-05_intraday-post-05march](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-03-05_intraday-post-05march/content.txt) |
| 2026-03-05 | 1m | zero_to_peak_continuation | short | 10:19 | 6844.34 | 6864.57 | 6770.81 | 14:32 | 6770.81 | target | 73.53 | 0.00 | 73.56 | [2026-03-05_intraday-post-05march](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-03-05_intraday-post-05march/content.txt) |
| 2026-03-06 | 1m | peak_rejection | long | 10:23 | 6752.57 | 6744.22 | 6767.99 | 10:36 | 6767.99 | target | 15.42 | 3.12 | 16.62 | [2026-03-06_intraday-post-06march](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-03-06_intraday-post-06march/content.txt) |
| 2026-03-09 | 1m | peak_rejection | long | 09:43 | 6655.05 | 6633.12 | 6668.99 | 10:04 | 6668.99 | target | 13.94 | 16.46 | 14.85 | [2026-03-09_intraday-post-09march](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-03-09_intraday-post-09march/content.txt) |
| 2026-03-09 | 1m | peak_rejection | short | 11:35 | 6699.50 | 6707.12 | 6672.87 | 11:53 | 6707.12 | stop | -7.62 | 9.03 | 2.68 | [2026-03-09_intraday-post-09march](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-03-09_intraday-post-09march/content.txt) |
| 2026-03-10 | 1m | peak_rejection | long | 10:32 | 6796.70 | 6786.29 | 6854.57 | 15:29 | 6786.29 | stop | -10.41 | 10.71 | 48.38 | [2026-03-10_intraday-post-10march-a1a](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-03-10_intraday-post-10march-a1a/content.txt) |
| 2026-03-10 | 1m | peak_rejection | short | 11:44 | 6826.81 | 6838.86 | 6799.71 | 13:24 | 6838.86 | stop | -12.05 | 12.53 | 17.59 | [2026-03-10_intraday-post-10march-a1a](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-03-10_intraday-post-10march-a1a/content.txt) |
| 2026-03-11 | 1m | peak_rejection | long | 09:37 | 6793.76 | 6770.82 | 6803.19 | 10:00 | 6803.19 | target | 9.43 | 20.12 | 14.44 | [2026-03-11_intraday-post-11march](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-03-11_intraday-post-11march/content.txt) |
| 2026-03-11 | 1m | peak_rejection | short | 09:48 | 6777.89 | 6799.38 | 6757.97 | 10:00 | 6799.38 | stop | -21.49 | 30.31 | 4.25 | [2026-03-11_intraday-post-11march](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-03-11_intraday-post-11march/content.txt) |
| 2026-03-11 | 1m | zero_to_peak_continuation | long | 10:01 | 6804.93 | 6778.13 | 6822.90 | 10:23 | 6778.13 | stop | -26.80 | 29.69 | 0.56 | [2026-03-11_intraday-post-11march](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-03-11_intraday-post-11march/content.txt) |
| 2026-03-13 | 1m | peak_rejection | short | 10:10 | 6700.33 | 6707.84 | 6691.57 | 10:17 | 6691.57 | target | 8.76 | 2.72 | 11.02 | [2026-03-13_pce-breakdown-intraday-post-13march](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-03-13_pce-breakdown-intraday-post-13march/content.txt) |
| 2026-03-13 | 1m | peak_rejection | long | 13:55 | 6638.32 | 6630.06 | 6676.16 | 15:38 | 6630.06 | stop | -8.26 | 8.57 | 19.04 | [2026-03-13_pce-breakdown-intraday-post-13march](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-03-13_pce-breakdown-intraday-post-13march/content.txt) |

## Calibration Diagnostics

| Date | Data | Usable | Score | Coverage | Avg err | Zero line | Lower peak | Upper peak | Lower trough | Upper trough | Pin zone | Source |
| --- | --- | --- | ---: | ---: | ---: | ---: | --- | --- | --- | --- | --- | --- |
| 2026-02-02 | 2m | yes | 8 | 88.9% | 2.2 | 6923.41 | 6889.77-6889.77 | 6925.33-6925.33 | 6840.00-6840.00 | 6935.99-6935.99 |  | [2026-02-02_intraday-post-02feb](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-02_intraday-post-02feb/content.txt) |
| 2026-02-03 | 2m | yes | 11 | 100.0% | 1.9 | 6984.86 | 6946.72-6946.72 | 6986.33-6986.33 |  |  |  | [2026-02-03_warsh-is-not-a-hawk-intraday-post](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-03_warsh-is-not-a-hawk-intraday-post/content.txt) |
| 2026-02-04 | 2m | yes | 10 | 100.0% | 1.3 | 6927.10 | 6877.99-6877.99 | 6942.12-6942.12 | 6861.35-6861.35 |  | 6957.75-6970.54 | [2026-02-04_intraday-post-04feb-038](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-04_intraday-post-04feb-038/content.txt) |
| 2026-02-05 | 2m | yes | 10 | 76.9% | 4.0 | 6834.56 | 6834.48-6834.48 | 6904.46-6904.46 |  |  | 6922.56-6931.65 | [2026-02-05_volatility-is-back-intraday-post](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-05_volatility-is-back-intraday-post/content.txt) |
| 2026-02-06 | 2m | yes | 12 | 85.7% | 3.2 | 6844.91 | 6806.16-6806.16 | 6843.91-6843.91 |  |  | 6767.64-6773.06 | [2026-02-06_high-volume-intraday-post-06feb](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-06_high-volume-intraday-post-06feb/content.txt) |
| 2026-02-09 | 2m | yes | 10 | 100.0% | 1.0 | 6936.74 | 6893.23-6893.23 | 6919.30-6919.30 |  | 6983.51-6990.03 |  | [2026-02-09_intraday-post-09feb](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-09_intraday-post-09feb/content.txt) |
| 2026-02-10 | 2m | yes | 8 | 100.0% | 4.1 | 6965.93 | 6966.10-6966.10 | 6992.02-6992.02 | 6876.60-6876.60 |  | 6876.60-6887.97 | [2026-02-10_retail-sales-are-stagnant-intraday](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-10_retail-sales-are-stagnant-intraday/content.txt) |
| 2026-02-11 | 2m | yes | 11 | 100.0% | 1.1 | 6948.29 | 6958.06-6958.06 | 7006.41-7014.77 | 6881.46-6881.46 |  | 6876.84-6887.86 | [2026-02-11_nfp-intraday-post-11feb](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-11_nfp-intraday-post-11feb/content.txt) |
| 2026-02-12 | 2m | yes | 17 | 100.0% | 0.4 | 6953.29 | 6954.34-6954.34 | 6987.44-6987.44 | 6890.14-6890.14 |  | 7045.89-7054.34 | [2026-02-12_intraday-post-12feb](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-12_intraday-post-12feb/content.txt) |
| 2026-02-13 | 2m | yes | 10 | 100.0% | 1.7 | 6836.06 | 6786.76-6786.76 | 6841.60-6841.60 | 6811.02-6811.02 |  |  | [2026-02-13_cpi-day-intraday-post-13feb](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-13_cpi-day-intraday-post-13feb/content.txt) |
| 2026-02-17 | 1m | yes | 11 | 100.0% | 1.5 | 6814.24 | 6814.44-6814.44 | 6840.51-6840.51 |  | 6824.75-6824.75 | 6783.13-6790.00 | [2026-02-17_intraday-post-17feb](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-17_intraday-post-17feb/content.txt) |
| 2026-02-18 | 1m | yes | 7 | 100.0% | 4.0 | 6860.44 | 6778.89-6786.16 | 6880.26-6880.26 | 6823.72-6823.72 |  | 6770.18-6786.16 | [2026-02-18_intraday-post-18feb-f49](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-18_intraday-post-18feb-f49/content.txt) |
| 2026-02-19 | 1m | yes | 15 | 88.2% | 1.2 | 6856.95 | 6856.07-6856.07 | 6908.01-6908.01 |  | 6873.11-6873.11 |  | [2026-02-19_intraday-post-19feb-a5e](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-19_intraday-post-19feb-a5e/content.txt) |
| 2026-02-20 | 1m | yes | 15 | 100.0% | 0.7 | 6861.02 | 6771.70-6779.70 | 6862.50-6862.50 | 6817.84-6826.29 | 6876.78-6884.31 | 6986.88-6999.90 | [2026-02-20_intraday-post-20feb-589](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-20_intraday-post-20feb-589/content.txt) |
| 2026-02-23 | 1m | yes | 11 | 100.0% | 2.0 | 6908.47 | 6879.09-6879.09 | 6904.80-6908.72 | 6889.13-6889.13 |  | 7031.15-7035.31 | [2026-02-23_intraday-post-23feb](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-23_intraday-post-23feb/content.txt) |
| 2026-02-24 | 1m | yes | 10 | 100.0% | 1.6 | 6859.27 | 6813.46-6813.46 | 6884.53-6884.53 | 6824.19-6824.19 |  | 6808.99-6813.57 | [2026-02-24_intraday-post-24feb-2cc](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-24_intraday-post-24feb-2cc/content.txt) |
| 2026-02-25 | 1m | yes | 15 | 100.0% | 0.7 | 6900.15 | 6883.96-6883.96 | 6917.37-6917.37 | 6818.68-6818.68 |  | 6880.47-6889.08 | [2026-02-25_intraday-post-25feb-8ed](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-25_intraday-post-25feb-8ed/content.txt) |
| 2026-02-26 | 1m | yes | 10 | 100.0% | 1.7 | 6982.55 | 6921.93-6921.93 | 6958.57-6966.20 |  | 7010.48-7010.48 |  | [2026-02-26_nvda-er-intraday-post-26feb](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-26_nvda-er-intraday-post-26feb/content.txt) |
| 2026-02-27 | 1m | yes | 9 | 90.0% | 3.0 | 6818.01 | 6783.48-6783.48 | 6904.47-6904.47 |  | 6844.51-6844.51 |  | [2026-02-27_ppi-intraday-post-27feb](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-02-27_ppi-intraday-post-27feb/content.txt) |
| 2026-03-02 | 1m | yes | 15 | 100.0% | 1.6 | 6797.95 | 6798.34-6798.34 | 6860.12-6860.12 |  |  | 7004.35-7010.00 | [2026-03-02_intraday-post-02march](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-03-02_intraday-post-02march/content.txt) |
| 2026-03-03 |  | no | 0 | 0.0% | 0.0 |  |  |  |  |  |  | [2026-03-03_brent-above-80-eu-gas-spikes-intraday](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-03-03_brent-above-80-eu-gas-spikes-intraday/content.txt) |
| 2026-03-04 | 1m | yes | 19 | 90.5% | 0.4 | 6779.80 | 6829.23-6829.23 | 6855.99-6855.99 | 6749.94-6749.94 | 6807.33-6807.33 | 6855.99-6871.31 | [2026-03-04_nothing-new-in-the-middle-east-intraday](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-03-04_nothing-new-in-the-middle-east-intraday/content.txt) |
| 2026-03-05 | 1m | yes | 15 | 93.8% | 0.6 | 6850.57 | 6756.50-6756.50 | 6855.25-6859.65 | 6811.24-6816.74 | 6902.00-6902.00 | 6912.18-6925.38 | [2026-03-05_intraday-post-05march](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-03-05_intraday-post-05march/content.txt) |
| 2026-03-06 | 1m | yes | 13 | 86.7% | 2.1 | 6716.91 | 6750.22-6750.22 | 6788.70-6788.70 |  | 6738.74-6738.74 | 6830.56-6836.52 | [2026-03-06_intraday-post-06march](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-03-06_intraday-post-06march/content.txt) |
| 2026-03-09 | 1m | yes | 10 | 76.9% | 1.8 | 6624.13 | 6639.89-6647.41 | 6701.12-6701.12 | 6622.68-6622.68 | 6668.99-6672.87 |  | [2026-03-09_intraday-post-09march](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-03-09_intraday-post-09march/content.txt) |
| 2026-03-10 | 1m | yes | 8 | 100.0% | 3.7 | 6868.29 | 6792.29-6792.29 | 6829.28-6832.86 | 6854.57-6858.29 |  | 6868.57-6872.00 | [2026-03-10_intraday-post-10march-a1a](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-03-10_intraday-post-10march-a1a/content.txt) |
| 2026-03-11 | 1m | yes | 10 | 90.9% | 0.5 | 6789.74 | 6780.00-6790.20 | 6780.00-6790.20 | 6754.72-6757.97 | 6807.60-6807.60 | 6845.62-6860.00 | [2026-03-11_intraday-post-11march](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-03-11_intraday-post-11march/content.txt) |
| 2026-03-12 | 1m | yes | 14 | 100.0% | 0.7 | 6735.33 | 6735.57-6738.77 | 6785.74-6785.74 | 6721.31-6721.31 | 6754.75-6761.15 | 6645.82-6663.03 | [2026-03-12_intraday-post-12march-fa0](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-03-12_intraday-post-12march-fa0/content.txt) |
| 2026-03-13 | 1m | yes | 13 | 100.0% | 0.5 | 6637.91 | 6636.06-6636.06 | 6701.84-6701.84 | 6590.25-6593.39 | 6676.16-6676.16 | 6590.25-6593.39 | [2026-03-13_pce-breakdown-intraday-post-13march](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/posts/2026-03-13_pce-breakdown-intraday-post-13march/content.txt) |
