# Multi-Year Calmar Screen (Partial vs Full) up to 5Y (as of 2026-03-14)

Universe: broad-discovery non-single option/derivative-income ETFs.
For each ETF and timeframe, SPY/QQQ Calmar is computed on the exact same ETF window.
Bucket definitions:
- NY full: full N calendar years history (window = last N years).
- NY partial: live history between (N-1) and <N years (window = ETF inception to end).

## Summary
| Horizon | Mode    | ETF Count | Avg ETF Calmar | Avg SPY Calmar (matched) | Avg QQQ Calmar (matched) | Beat SPY | Beat QQQ | Beat Either | Beat Both |
| ------- | ------- | --------- | -------------- | ------------------------ | ------------------------ | -------- | -------- | ----------- | --------- |
| 1Y      | full    | 84        | 1.310          | 1.575                    | 1.756                    | 29       | 14       | 29          | 14        |
| 1Y      | partial | 25        | 0.152          | 1.283                    | 1.034                    | 7        | 9        | 9           | 7         |
| 2Y      | full    | 59        | 0.930          | 0.788                    | 0.741                    | 26       | 33       | 33          | 26        |
| 2Y      | partial | 25        | 0.622          | 0.699                    | 0.691                    | 10       | 9        | 10          | 9         |
| 3Y      | full    | 29        | 1.041          | 1.144                    | 1.219                    | 8        | 4        | 8           | 4         |
| 3Y      | partial | 30        | 1.112          | 1.005                    | 0.927                    | 8        | 16       | 16          | 8         |
| 4Y      | full    | 17        | 0.634          | 0.629                    | 0.599                    | 6        | 7        | 7           | 6         |
| 4Y      | partial | 12        | 0.715          | 0.902                    | 0.984                    | 4        | 3        | 4           | 3         |
| 5Y      | full    | 14        | 0.527          | 0.506                    | 0.398                    | 6        | 10       | 10          | 6         |
| 5Y      | partial | 3         | 0.236          | 0.515                    | 0.423                    | 0        | 1        | 1           | 0         |

## 1Y Partial - ETFs Beating SPY or QQQ Calmar
Count winners: **9** / 25 | Avg ETF Calmar: **0.152** | Avg SPY matched: **1.283** | Avg QQQ matched: **1.034**

| Rank | Ticker | Provider                   | Start      | End        | TradingDays | ETF Calmar | SPY Calmar | QQQ Calmar | Beat SPY | Beat QQQ | Name                                               |
| ---- | ------ | -------------------------- | ---------- | ---------- | ----------- | ---------- | ---------- | ---------- | -------- | -------- | -------------------------------------------------- |
| 1    | CHPY   | YieldMax ETFs              | 2025-04-08 | 2026-03-13 | 233         | 9.838      | 5.978      | 6.024      | Y        | Y        | YieldMax Semiconductor Portfolio Option Income ETF |
| 2    | SEPI   | Shelton Capital Management | 2025-09-08 | 2026-03-13 | 129         | 2.101      | 1.038      | 0.708      | Y        | Y        | Shelton Equity Premium Income ETF                  |
| 3    | HEMI   | Hartford Mutual Funds      | 2025-12-17 | 2026-03-13 | 58          | -0.316     | -0.953     | -0.676     | Y        | Y        | Hartford Equity Premium Income ETF                 |
| 4    | HAKY   | Amplify ETFs               | 2026-01-21 | 2026-03-13 | 36          | -0.793     | -4.471     | -3.683     | Y        | Y        | Amplify HACK Cybersecurity Covered Call ETF        |
| 5    | YBMN   | Defiance ETFs LLC          | 2025-11-25 | 2026-03-13 | 73          | -1.076     | -1.132     | -1.272     | Y        | Y        | Defiance BMNR Option Income ETF                    |
| 6    | SOLM   | Amplify ETFs               | 2025-11-04 | 2026-03-13 | 88          | -1.513     | -0.962     | -1.768     | N        | Y        | Amplify Solana 3% Monthly Option Income ETF        |
| 7    | BALQ   | iShares                    | 2025-12-03 | 2026-03-13 | 68          | -1.631     | -2.145     | -2.597     | Y        | Y        | iShares Nasdaq Premium Income Active ETF           |
| 8    | ULTI   | Rex                        | 2025-10-31 | 2026-03-13 | 90          | -1.806     | -1.496     | -1.994     | N        | Y        | REX IncomeMax Option Strategy ETF                  |
| 9    | SCEP   | Sterling Capital Funds     | 2025-12-11 | 2026-03-13 | 62          | -2.762     | -2.913     | -3.000     | Y        | Y        | Sterling Capital Hedged Equity Premium Income ETF  |

## 1Y Full - ETFs Beating SPY or QQQ Calmar
Count winners: **29** / 84 | Avg ETF Calmar: **1.310** | Avg SPY matched: **1.575** | Avg QQQ matched: **1.756**

| Rank | Ticker | Provider                        | Start      | End        | TradingDays | ETF Calmar | SPY Calmar | QQQ Calmar | Beat SPY | Beat QQQ | Name                                                          |
| ---- | ------ | ------------------------------- | ---------- | ---------- | ----------- | ---------- | ---------- | ---------- | -------- | -------- | ------------------------------------------------------------- |
| 1    | GDXY   | YieldMax ETFs                   | 2025-03-13 | 2026-03-13 | 251         | 3.588      | 1.575      | 1.756      | Y        | Y        | YieldMax Gold Miners Option Income Strategy ETF               |
| 2    | SOXY   | YieldMax ETFs                   | 2025-03-13 | 2026-03-13 | 251         | 2.958      | 1.575      | 1.756      | Y        | Y        | YieldMax Target 12 Semiconductor Option Income ETF            |
| 3    | DIVO   | Amplify ETFs                    | 2025-03-13 | 2026-03-13 | 251         | 2.282      | 1.575      | 1.756      | Y        | Y        | Amplify CWP Enhanced Dividend Income ETF                      |
| 4    | USOY   | Defiance ETFs LLC               | 2025-03-13 | 2026-03-13 | 251         | 2.211      | 1.575      | 1.756      | Y        | Y        | Defiance Oil Enhanced Options Income ETF                      |
| 5    | IDVO   | Amplify ETFs                    | 2025-03-13 | 2026-03-13 | 251         | 2.204      | 1.575      | 1.756      | Y        | Y        | Amplify CWP International Enhanced Dividend Income ETF        |
| 6    | QCLR   | Global X Funds                  | 2025-03-13 | 2026-03-13 | 251         | 1.971      | 1.575      | 1.756      | Y        | Y        | Global X NASDAQ 100 Collar 95-110 ETF                         |
| 7    | XCLR   | Global X Funds                  | 2025-03-13 | 2026-03-13 | 251         | 1.925      | 1.575      | 1.756      | Y        | Y        | Global X S&P 500 Collar 95-110 ETF                            |
| 8    | YMAG   | YieldMax ETFs                   | 2025-03-13 | 2026-03-13 | 251         | 1.856      | 1.575      | 1.756      | Y        | Y        | YieldMax Magnificent 7 Fund of Option Income ETFs             |
| 9    | KHPI   | Kensington Asset Management LLC | 2025-03-13 | 2026-03-13 | 251         | 1.855      | 1.575      | 1.756      | Y        | Y        | Kensington Hedged Premium Income ETF                          |
| 10   | GPTY   | YieldMax ETFs                   | 2025-03-13 | 2026-03-13 | 251         | 1.850      | 1.575      | 1.756      | Y        | Y        | YieldMax AI & Tech Portfolio Option Income ETF                |
| 11   | GPIQ   | Goldman Sachs                   | 2025-03-13 | 2026-03-13 | 251         | 1.818      | 1.575      | 1.756      | Y        | Y        | Goldman Sachs Nasdaq-100 Premium Income ETF                   |
| 12   | WTPI   | WisdomTree                      | 2025-03-13 | 2026-03-13 | 251         | 1.783      | 1.575      | 1.756      | Y        | Y        | WisdomTree Equity Premium Income Fund                         |
| 13   | LQDW   | iShares                         | 2025-03-13 | 2026-03-13 | 251         | 1.783      | 1.575      | 1.756      | Y        | Y        | iShares Investment Grade Corporate Bond Buywrite Strategy ETF |
| 14   | LJUL   | Innovator ETFs                  | 2025-03-13 | 2026-03-13 | 251         | 1.780      | 1.575      | 1.756      | Y        | Y        | Innovator Premium Income 15 Buffer ETF - July                 |
| 15   | QYLG   | Global X Funds                  | 2025-03-13 | 2026-03-13 | 251         | 1.751      | 1.575      | 1.756      | Y        | N        | Global X Nasdaq 100 Covered Call & Growth ETF                 |
| 16   | VEGA   | AdvisorShares                   | 2025-03-13 | 2026-03-13 | 251         | 1.740      | 1.575      | 1.756      | Y        | N        | AdvisorShares STAR Global Buy-Write ETF                       |
| 17   | BALI   | BlackRock                       | 2025-03-13 | 2026-03-13 | 251         | 1.695      | 1.575      | 1.756      | Y        | N        | iShares U.S. Large Cap Premium Income Active ETF              |
| 18   | HYGW   | iShares                         | 2025-03-13 | 2026-03-13 | 251         | 1.689      | 1.575      | 1.756      | Y        | N        | iShares High Yield Corporate Bond Buywrite Strategy ETF       |
| 19   | FEPI   | REX Advisers, LLC               | 2025-03-13 | 2026-03-13 | 251         | 1.683      | 1.575      | 1.756      | Y        | N        | REX FANG & Innovation Equity Premium Income ETF               |
| 20   | NDIV   | Amplify ETFs                    | 2025-03-13 | 2026-03-13 | 251         | 1.663      | 1.575      | 1.756      | Y        | N        | Amplify Energy & Natural Resources Covered Call ETF           |
| 21   | IWMI   | Neos Funds                      | 2025-03-13 | 2026-03-13 | 251         | 1.657      | 1.575      | 1.756      | Y        | N        | NEOS Russell 2000 High Income ETF                             |
| 22   | QQQI   | Neos Funds                      | 2025-03-13 | 2026-03-13 | 251         | 1.651      | 1.575      | 1.756      | Y        | N        | NEOS NASDAQ-100(R) High Income ETF                            |
| 23   | FTQI   | First Trust                     | 2025-03-13 | 2026-03-13 | 251         | 1.625      | 1.575      | 1.756      | Y        | N        | First Trust Nasdaq BuyWrite Income ETF                        |
| 24   | BIGY   | YieldMax ETFs                   | 2025-03-13 | 2026-03-13 | 251         | 1.624      | 1.575      | 1.756      | Y        | N        | YieldMax Target 12 Big 50 Option Income ETF                   |
| 25   | JEPQ   | JPMorgan                        | 2025-03-13 | 2026-03-13 | 251         | 1.621      | 1.575      | 1.756      | Y        | N        | JPMorgan Nasdaq Equity Premium Income ETF                     |
| 26   | XIJN   | First Trust                     | 2025-03-13 | 2026-03-13 | 251         | 1.619      | 1.575      | 1.756      | Y        | N        | FT Vest U.S. Equity Buffer & Premium Income ETF - June        |
| 27   | QYLD   | Global X Funds                  | 2025-03-13 | 2026-03-13 | 251         | 1.601      | 1.575      | 1.756      | Y        | N        | Global X NASDAQ 100 Covered Call ETF                          |
| 28   | NUSI   | Neos Funds                      | 2025-03-13 | 2026-03-13 | 250         | 1.595      | 1.575      | 1.756      | Y        | N        | NEOS Nasdaq-100 Hedged Equity Income ETF                      |
| 29   | NBOS   | Neuberger Berman                | 2025-03-13 | 2026-03-13 | 251         | 1.579      | 1.575      | 1.756      | Y        | N        | Neuberger Option Strategy ETF                                 |

## 2Y Partial - ETFs Beating SPY or QQQ Calmar
Count winners: **10** / 25 | Avg ETF Calmar: **0.622** | Avg SPY matched: **0.699** | Avg QQQ matched: **0.691**

| Rank | Ticker | Provider                        | Start      | End        | TradingDays | ETF Calmar | SPY Calmar | QQQ Calmar | Beat SPY | Beat QQQ | Name                                                    |
| ---- | ------ | ------------------------------- | ---------- | ---------- | ----------- | ---------- | ---------- | ---------- | -------- | -------- | ------------------------------------------------------- |
| 1    | GDXY   | YieldMax ETFs                   | 2024-05-21 | 2026-03-13 | 453         | 1.962      | 0.767      | 0.723      | Y        | Y        | YieldMax Gold Miners Option Income Strategy ETF         |
| 2    | LJUL   | Innovator ETFs                  | 2024-07-01 | 2026-03-13 | 426         | 1.777      | 0.714      | 0.601      | Y        | Y        | Innovator Premium Income 15 Buffer ETF - July           |
| 3    | LAPR   | Innovator ETFs                  | 2024-04-01 | 2026-03-13 | 489         | 1.540      | 0.761      | 0.729      | Y        | Y        | Innovator Premium Income 15 Buffer ETF - April          |
| 4    | XIJN   | First Trust                     | 2024-06-24 | 2026-03-13 | 431         | 1.486      | 0.722      | 0.642      | Y        | Y        | FT Vest U.S. Equity Buffer & Premium Income ETF - June  |
| 5    | XIMR   | First Trust                     | 2024-03-19 | 2026-03-13 | 497         | 1.285      | 0.787      | 0.754      | Y        | Y        | FT Vest U.S. Equity Buffer & Premium Income ETF - March |
| 6    | USOY   | Defiance ETFs LLC               | 2024-05-10 | 2026-03-13 | 460         | 1.157      | 0.822      | 0.798      | Y        | Y        | Defiance Oil Enhanced Options Income ETF                |
| 7    | SOXY   | YieldMax ETFs                   | 2024-12-03 | 2026-03-13 | 318         | 1.156      | 0.473      | 0.536      | Y        | Y        | YieldMax Target 12 Semiconductor Option Income ETF      |
| 8    | MLPD   | Global X Funds                  | 2024-05-08 | 2026-03-13 | 462         | 1.032      | 0.841      | 0.807      | Y        | Y        | Global X MLP & Energy Infrastructure Covered Call ETF   |
| 9    | KHPI   | Kensington Asset Management LLC | 2024-09-05 | 2026-03-13 | 380         | 0.909      | 0.775      | 0.830      | Y        | Y        | Kensington Hedged Premium Income ETF                    |
| 10   | BIGY   | YieldMax ETFs                   | 2024-11-21 | 2026-03-13 | 325         | 0.560      | 0.540      | 0.613      | Y        | N        | YieldMax Target 12 Big 50 Option Income ETF             |

## 2Y Full - ETFs Beating SPY or QQQ Calmar
Count winners: **33** / 59 | Avg ETF Calmar: **0.930** | Avg SPY matched: **0.788** | Avg QQQ matched: **0.741**

| Rank | Ticker | Provider                  | Start      | End        | TradingDays | ETF Calmar | SPY Calmar | QQQ Calmar | Beat SPY | Beat QQQ | Name                                                          |
| ---- | ------ | ------------------------- | ---------- | ---------- | ----------- | ---------- | ---------- | ---------- | -------- | -------- | ------------------------------------------------------------- |
| 1    | JEPY   | Defiance ETFs             | 2024-03-13 | 2026-03-13 | 500         | 7.591      | 0.788      | 0.741      | Y        | Y        | Defiance S&P 500 Enhanced Options 0DTE Income ETF             |
| 2    | NUSI   | Neos Funds                | 2024-03-13 | 2026-03-13 | 500         | 4.654      | 0.788      | 0.741      | Y        | Y        | NEOS Nasdaq-100 Hedged Equity Income ETF                      |
| 3    | JULJ   | Innovator ETFs            | 2024-03-13 | 2026-03-13 | 501         | 1.541      | 0.788      | 0.741      | Y        | Y        | Innovator Premium Income 30 Barrier ETF - July                |
| 4    | HYGW   | iShares                   | 2024-03-13 | 2026-03-13 | 501         | 1.463      | 0.788      | 0.741      | Y        | Y        | iShares High Yield Corporate Bond Buywrite Strategy ETF       |
| 5    | IDVO   | Amplify ETFs              | 2024-03-13 | 2026-03-13 | 501         | 1.430      | 0.788      | 0.741      | Y        | Y        | Amplify CWP International Enhanced Dividend Income ETF        |
| 6    | LQDW   | iShares                   | 2024-03-13 | 2026-03-13 | 501         | 1.316      | 0.788      | 0.741      | Y        | Y        | iShares Investment Grade Corporate Bond Buywrite Strategy ETF |
| 7    | DIVO   | Amplify ETFs              | 2024-03-13 | 2026-03-13 | 501         | 1.244      | 0.788      | 0.741      | Y        | Y        | Amplify CWP Enhanced Dividend Income ETF                      |
| 8    | APRJ   | Innovator ETFs            | 2024-03-13 | 2026-03-13 | 501         | 1.211      | 0.788      | 0.741      | Y        | Y        | Innovator Premium Income 30 Barrier ETF - April               |
| 9    | JULH   | Innovator ETFs            | 2024-03-13 | 2026-03-13 | 501         | 1.172      | 0.788      | 0.741      | Y        | Y        | Innovator Premium Income 20 Barrier ETF - July                |
| 10   | APRH   | Innovator ETFs            | 2024-03-13 | 2026-03-13 | 501         | 1.165      | 0.788      | 0.741      | Y        | Y        | Innovator Premium Income 20 Barrier ETF - April               |
| 11   | LOCT   | Innovator ETFs            | 2024-03-13 | 2026-03-13 | 501         | 1.048      | 0.788      | 0.741      | Y        | Y        | Innovator Premium Income 15 Buffer ETF - October              |
| 12   | LJAN   | Innovator ETFs            | 2024-03-13 | 2026-03-13 | 501         | 0.977      | 0.788      | 0.741      | Y        | Y        | Innovator Premium Income 15 Buffer ETF - January              |
| 13   | OCTJ   | Innovator ETFs            | 2024-03-13 | 2026-03-13 | 501         | 0.930      | 0.788      | 0.741      | Y        | Y        | Innovator Premium Income 30 Barrier ETF - October             |
| 14   | VEGA   | AdvisorShares             | 2024-03-13 | 2026-03-13 | 501         | 0.909      | 0.788      | 0.741      | Y        | Y        | AdvisorShares STAR Global Buy-Write ETF                       |
| 15   | NDIV   | Amplify ETFs              | 2024-03-13 | 2026-03-13 | 501         | 0.888      | 0.788      | 0.741      | Y        | Y        | Amplify Energy & Natural Resources Covered Call ETF           |
| 16   | BUCK   | Simplify Asset Management | 2024-03-13 | 2026-03-13 | 501         | 0.882      | 0.788      | 0.741      | Y        | Y        | Simplify Treasury Option Income ETF                           |
| 17   | BUYW   | Main Management ETFs      | 2024-03-13 | 2026-03-13 | 501         | 0.874      | 0.788      | 0.741      | Y        | Y        | Main Buywrite ETF                                             |
| 18   | XISE   | First Trust               | 2024-03-13 | 2026-03-13 | 501         | 0.868      | 0.788      | 0.741      | Y        | Y        | FT Vest U.S. Equity Buffer & Premium Income ETF – September   |
| 19   | XIDE   | First Trust               | 2024-03-13 | 2026-03-13 | 501         | 0.854      | 0.788      | 0.741      | Y        | Y        | FT Vest U.S. Equity Buffer & Premium Income ETF - December    |
| 20   | NBOS   | Neuberger Berman          | 2024-03-13 | 2026-03-13 | 501         | 0.853      | 0.788      | 0.741      | Y        | Y        | Neuberger Option Strategy ETF                                 |
| 21   | BALI   | BlackRock                 | 2024-03-13 | 2026-03-13 | 501         | 0.841      | 0.788      | 0.741      | Y        | Y        | iShares U.S. Large Cap Premium Income Active ETF              |
| 22   | ACIO   | APTUS ETFs                | 2024-03-13 | 2026-03-13 | 501         | 0.830      | 0.788      | 0.741      | Y        | Y        | Aptus Collared Investment Opportunity ETF                     |
| 23   | SPYI   | Neos Funds                | 2024-03-13 | 2026-03-13 | 501         | 0.810      | 0.788      | 0.741      | Y        | Y        | Neos S&P 500(R) High Income ETF                               |
| 24   | QQQI   | Neos Funds                | 2024-03-13 | 2026-03-13 | 501         | 0.802      | 0.788      | 0.741      | Y        | Y        | NEOS NASDAQ-100(R) High Income ETF                            |
| 25   | GPIX   | Goldman Sachs             | 2024-03-13 | 2026-03-13 | 501         | 0.796      | 0.788      | 0.741      | Y        | Y        | Goldman Sachs S&P 500 Premium Income ETF                      |
| 26   | GPIQ   | Goldman Sachs             | 2024-03-13 | 2026-03-13 | 501         | 0.791      | 0.788      | 0.741      | Y        | Y        | Goldman Sachs Nasdaq-100 Premium Income ETF                   |
| 27   | JANJ   | Innovator ETFs            | 2024-03-13 | 2026-03-13 | 501         | 0.787      | 0.788      | 0.741      | N        | Y        | Innovator Premium Income 30 Barrier ETF - January             |
| 28   | PBP    | Invesco                   | 2024-03-13 | 2026-03-13 | 501         | 0.783      | 0.788      | 0.741      | N        | Y        | Invesco S&P 500 BuyWrite ETF                                  |
| 29   | WTPI   | WisdomTree                | 2024-03-13 | 2026-03-13 | 501         | 0.771      | 0.788      | 0.741      | N        | Y        | WisdomTree Equity Premium Income Fund                         |
| 30   | QDTE   | Roundhill Investments     | 2024-03-13 | 2026-03-13 | 501         | 0.759      | 0.788      | 0.741      | N        | Y        | Roundhill Innovation-100 0DTE Covered Call Strategy ETF       |
| 31   | QCLR   | Global X Funds            | 2024-03-13 | 2026-03-13 | 501         | 0.743      | 0.788      | 0.741      | N        | Y        | Global X NASDAQ 100 Collar 95-110 ETF                         |
| 32   | XCLR   | Global X Funds            | 2024-03-13 | 2026-03-13 | 501         | 0.743      | 0.788      | 0.741      | N        | Y        | Global X S&P 500 Collar 95-110 ETF                            |
| 33   | YMAG   | YieldMax ETFs             | 2024-03-13 | 2026-03-13 | 501         | 0.742      | 0.788      | 0.741      | N        | Y        | YieldMax Magnificent 7 Fund of Option Income ETFs             |

## 3Y Partial - ETFs Beating SPY or QQQ Calmar
Count winners: **16** / 30 | Avg ETF Calmar: **1.112** | Avg SPY matched: **1.005** | Avg QQQ matched: **0.927**

| Rank | Ticker | Provider              | Start      | End        | TradingDays | ETF Calmar | SPY Calmar | QQQ Calmar | Beat SPY | Beat QQQ | Name                                                        |
| ---- | ------ | --------------------- | ---------- | ---------- | ----------- | ---------- | ---------- | ---------- | -------- | -------- | ----------------------------------------------------------- |
| 1    | JEPY   | Defiance ETFs         | 2023-09-18 | 2026-03-13 | 622         | 8.694      | 1.011      | 0.951      | Y        | Y        | Defiance S&P 500 Enhanced Options 0DTE Income ETF           |
| 2    | JULJ   | Innovator ETFs        | 2023-07-03 | 2026-03-13 | 676         | 1.653      | 0.932      | 0.874      | Y        | Y        | Innovator Premium Income 30 Barrier ETF - July              |
| 3    | APRJ   | Innovator ETFs        | 2023-04-03 | 2026-03-13 | 738         | 1.310      | 1.021      | 1.061      | Y        | Y        | Innovator Premium Income 30 Barrier ETF - April             |
| 4    | JULH   | Innovator ETFs        | 2023-07-03 | 2026-03-13 | 676         | 1.273      | 0.932      | 0.874      | Y        | Y        | Innovator Premium Income 20 Barrier ETF - July              |
| 5    | APRH   | Innovator ETFs        | 2023-04-03 | 2026-03-13 | 738         | 1.259      | 1.021      | 1.061      | Y        | Y        | Innovator Premium Income 20 Barrier ETF - April             |
| 6    | GPIX   | Goldman Sachs         | 2023-10-27 | 2026-03-13 | 594         | 1.232      | 1.278      | 1.167      | N        | Y        | Goldman Sachs S&P 500 Premium Income ETF                    |
| 7    | LOCT   | Innovator ETFs        | 2023-10-02 | 2026-03-13 | 613         | 1.208      | 1.128      | 1.025      | Y        | Y        | Innovator Premium Income 15 Buffer ETF - October            |
| 8    | BALI   | BlackRock             | 2023-09-28 | 2026-03-13 | 615         | 1.148      | 1.116      | 1.042      | Y        | Y        | iShares U.S. Large Cap Premium Income Active ETF            |
| 9    | OCTJ   | Innovator ETFs        | 2023-10-02 | 2026-03-13 | 613         | 1.079      | 1.128      | 1.025      | N        | Y        | Innovator Premium Income 30 Barrier ETF - October           |
| 10   | LJAN   | Innovator ETFs        | 2024-01-02 | 2026-03-13 | 550         | 1.030      | 0.961      | 0.882      | Y        | Y        | Innovator Premium Income 15 Buffer ETF - January            |
| 11   | XISE   | First Trust           | 2023-09-18 | 2026-03-13 | 623         | 0.993      | 1.011      | 0.951      | N        | Y        | FT Vest U.S. Equity Buffer & Premium Income ETF – September |
| 12   | XIDE   | First Trust           | 2023-12-18 | 2026-03-13 | 559         | 0.906      | 0.949      | 0.842      | N        | Y        | FT Vest U.S. Equity Buffer & Premium Income ETF - December  |
| 13   | NBOS   | Neuberger Berman      | 2024-01-29 | 2026-03-13 | 532         | 0.880      | 0.882      | 0.762      | N        | Y        | Neuberger Option Strategy ETF                               |
| 14   | QQQI   | Neos Funds            | 2024-01-30 | 2026-03-13 | 531         | 0.848      | 0.886      | 0.780      | N        | Y        | NEOS NASDAQ-100(R) High Income ETF                          |
| 15   | YMAG   | YieldMax ETFs         | 2024-01-30 | 2026-03-13 | 531         | 0.826      | 0.886      | 0.780      | N        | Y        | YieldMax Magnificent 7 Fund of Option Income ETFs           |
| 16   | QDTE   | Roundhill Investments | 2024-03-07 | 2026-03-13 | 505         | 0.709      | 0.788      | 0.705      | N        | Y        | Roundhill Innovation-100 0DTE Covered Call Strategy ETF     |

## 3Y Full - ETFs Beating SPY or QQQ Calmar
Count winners: **8** / 29 | Avg ETF Calmar: **1.041** | Avg SPY matched: **1.144** | Avg QQQ matched: **1.219**

| Rank | Ticker | Provider       | Start      | End        | TradingDays | ETF Calmar | SPY Calmar | QQQ Calmar | Beat SPY | Beat QQQ | Name                                                    |
| ---- | ------ | -------------- | ---------- | ---------- | ----------- | ---------- | ---------- | ---------- | -------- | -------- | ------------------------------------------------------- |
| 1    | NUSI   | Neos Funds     | 2023-03-13 | 2026-03-13 | 752         | 4.097      | 1.144      | 1.219      | Y        | Y        | NEOS Nasdaq-100 Hedged Equity Income ETF                |
| 2    | HYGW   | iShares        | 2023-03-13 | 2026-03-13 | 753         | 1.650      | 1.144      | 1.219      | Y        | Y        | iShares High Yield Corporate Bond Buywrite Strategy ETF |
| 3    | IDVO   | Amplify ETFs   | 2023-03-13 | 2026-03-13 | 753         | 1.445      | 1.144      | 1.219      | Y        | Y        | Amplify CWP International Enhanced Dividend Income ETF  |
| 4    | DIVO   | Amplify ETFs   | 2023-03-13 | 2026-03-13 | 753         | 1.309      | 1.144      | 1.219      | Y        | Y        | Amplify CWP Enhanced Dividend Income ETF                |
| 5    | QCLR   | Global X Funds | 2023-03-13 | 2026-03-13 | 753         | 1.188      | 1.144      | 1.219      | Y        | N        | Global X NASDAQ 100 Collar 95-110 ETF                   |
| 6    | XCLR   | Global X Funds | 2023-03-13 | 2026-03-13 | 753         | 1.161      | 1.144      | 1.219      | Y        | N        | Global X S&P 500 Collar 95-110 ETF                      |
| 7    | ACIO   | APTUS ETFs     | 2023-03-13 | 2026-03-13 | 753         | 1.159      | 1.144      | 1.219      | Y        | N        | Aptus Collared Investment Opportunity ETF               |
| 8    | VEGA   | AdvisorShares  | 2023-03-13 | 2026-03-13 | 753         | 1.155      | 1.144      | 1.219      | Y        | N        | AdvisorShares STAR Global Buy-Write ETF                 |

## 4Y Partial - ETFs Beating SPY or QQQ Calmar
Count winners: **4** / 12 | Avg ETF Calmar: **0.715** | Avg SPY matched: **0.902** | Avg QQQ matched: **0.984**

| Rank | Ticker | Provider             | Start      | End        | TradingDays | ETF Calmar | SPY Calmar | QQQ Calmar | Beat SPY | Beat QQQ | Name                                                    |
| ---- | ------ | -------------------- | ---------- | ---------- | ----------- | ---------- | ---------- | ---------- | -------- | -------- | ------------------------------------------------------- |
| 1    | IDVO   | Amplify ETFs         | 2022-09-08 | 2026-03-13 | 880         | 1.364      | 0.912      | 0.979      | Y        | Y        | Amplify CWP International Enhanced Dividend Income ETF  |
| 2    | HYGW   | iShares              | 2022-08-22 | 2026-03-13 | 892         | 1.007      | 0.843      | 0.897      | Y        | Y        | iShares High Yield Corporate Bond Buywrite Strategy ETF |
| 3    | BUYW   | Main Management ETFs | 2022-09-12 | 2026-03-13 | 878         | 1.000      | 0.868      | 0.931      | Y        | Y        | Main Buywrite ETF                                       |
| 4    | JEPQ   | JPMorgan             | 2022-05-04 | 2026-03-13 | 967         | 0.729      | 0.721      | 0.760      | Y        | N        | JPMorgan Nasdaq Equity Premium Income ETF               |

## 4Y Full - ETFs Beating SPY or QQQ Calmar
Count winners: **7** / 17 | Avg ETF Calmar: **0.634** | Avg SPY matched: **0.629** | Avg QQQ matched: **0.599**

| Rank | Ticker | Provider       | Start      | End        | TradingDays | ETF Calmar | SPY Calmar | QQQ Calmar | Beat SPY | Beat QQQ | Name                                      |
| ---- | ------ | -------------- | ---------- | ---------- | ----------- | ---------- | ---------- | ---------- | -------- | -------- | ----------------------------------------- |
| 1    | NUSI   | Neos Funds     | 2022-03-14 | 2026-03-13 | 1002        | 2.021      | 0.629      | 0.599      | Y        | Y        | NEOS Nasdaq-100 Hedged Equity Income ETF  |
| 2    | DIVO   | Amplify ETFs   | 2022-03-14 | 2026-03-13 | 1003        | 0.860      | 0.629      | 0.599      | Y        | Y        | Amplify CWP Enhanced Dividend Income ETF  |
| 3    | ACIO   | APTUS ETFs     | 2022-03-14 | 2026-03-13 | 1003        | 0.842      | 0.629      | 0.599      | Y        | Y        | Aptus Collared Investment Opportunity ETF |
| 4    | XCLR   | Global X Funds | 2022-03-14 | 2026-03-13 | 1003        | 0.720      | 0.629      | 0.599      | Y        | Y        | Global X S&P 500 Collar 95-110 ETF        |
| 5    | QCLR   | Global X Funds | 2022-03-14 | 2026-03-13 | 1003        | 0.660      | 0.629      | 0.599      | Y        | Y        | Global X NASDAQ 100 Collar 95-110 ETF     |
| 6    | FTHI   | First Trust    | 2022-03-14 | 2026-03-13 | 1003        | 0.658      | 0.629      | 0.599      | Y        | Y        | First Trust BuyWrite Income ETF           |
| 7    | JEPI   | JPMorgan       | 2022-03-14 | 2026-03-13 | 1003        | 0.617      | 0.629      | 0.599      | N        | Y        | JPMorgan Equity Premium Income ETF        |

## 5Y Partial - ETFs Beating SPY or QQQ Calmar
Count winners: **1** / 3 | Avg ETF Calmar: **0.236** | Avg SPY matched: **0.515** | Avg QQQ matched: **0.423**

| Rank | Ticker | Provider       | Start      | End        | TradingDays | ETF Calmar | SPY Calmar | QQQ Calmar | Beat SPY | Beat QQQ | Name                                  |
| ---- | ------ | -------------- | ---------- | ---------- | ----------- | ---------- | ---------- | ---------- | -------- | -------- | ------------------------------------- |
| 1    | QCLR   | Global X Funds | 2021-08-26 | 2026-03-13 | 1140        | 0.346      | 0.434      | 0.329      | N        | Y        | Global X NASDAQ 100 Collar 95-110 ETF |

## 5Y Full - ETFs Beating SPY or QQQ Calmar
Count winners: **10** / 14 | Avg ETF Calmar: **0.527** | Avg SPY matched: **0.506** | Avg QQQ matched: **0.398**

| Rank | Ticker | Provider       | Start      | End        | TradingDays | ETF Calmar | SPY Calmar | QQQ Calmar | Beat SPY | Beat QQQ | Name                                       |
| ---- | ------ | -------------- | ---------- | ---------- | ----------- | ---------- | ---------- | ---------- | -------- | -------- | ------------------------------------------ |
| 1    | NUSI   | Neos Funds     | 2021-03-15 | 2026-03-13 | 1254        | 1.195      | 0.506      | 0.398      | Y        | Y        | NEOS Nasdaq-100 Hedged Equity Income ETF   |
| 2    | DIVO   | Amplify ETFs   | 2021-03-15 | 2026-03-13 | 1255        | 0.817      | 0.506      | 0.398      | Y        | Y        | Amplify CWP Enhanced Dividend Income ETF   |
| 3    | ACIO   | APTUS ETFs     | 2021-03-15 | 2026-03-13 | 1255        | 0.693      | 0.506      | 0.398      | Y        | Y        | Aptus Collared Investment Opportunity ETF  |
| 4    | JEPI   | JPMorgan       | 2021-03-15 | 2026-03-13 | 1255        | 0.649      | 0.506      | 0.398      | Y        | Y        | JPMorgan Equity Premium Income ETF         |
| 5    | WTPI   | WisdomTree     | 2021-03-15 | 2026-03-13 | 1255        | 0.599      | 0.506      | 0.398      | Y        | Y        | WisdomTree Equity Premium Income Fund      |
| 6    | FTHI   | First Trust    | 2021-03-15 | 2026-03-13 | 1255        | 0.594      | 0.506      | 0.398      | Y        | Y        | First Trust BuyWrite Income ETF            |
| 7    | FTQI   | First Trust    | 2021-03-15 | 2026-03-13 | 1255        | 0.467      | 0.506      | 0.398      | N        | Y        | First Trust Nasdaq BuyWrite Income ETF     |
| 8    | XYLG   | Global X Funds | 2021-03-15 | 2026-03-13 | 1255        | 0.462      | 0.506      | 0.398      | N        | Y        | Global X S&P 500 Covered Call & Growth ETF |
| 9    | PBP    | Invesco        | 2021-03-15 | 2026-03-13 | 1255        | 0.436      | 0.506      | 0.398      | N        | Y        | Invesco S&P 500 BuyWrite ETF               |
| 10   | XYLD   | Global X Funds | 2021-03-15 | 2026-03-13 | 1255        | 0.402      | 0.506      | 0.398      | N        | Y        | Global X S&P 500 Covered Call ETF          |
