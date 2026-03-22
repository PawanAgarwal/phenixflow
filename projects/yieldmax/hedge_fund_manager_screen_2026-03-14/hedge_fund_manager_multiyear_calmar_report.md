# Hedge Fund / Manager Expanded Calmar Screen (as of 2026-03-14)

This expands the prior option-income screen to include:
- Broad hedge-fund-style and alternatives ETFs (internet discovery + seeds).
- Publicly traded hedge fund managers / alternative-asset manager equities.
- Synthetic manager composites where at least two ETFs were available for a manager.

Return series uses Yahoo `Adj Close` (distributions/dividends reinvested where provided).
Calmar uses the same definition as before: annualized return / abs(max drawdown).

Partial/full bucket definitions:
- NY full: full N calendar years available.
- NY partial: at least (N-1) years and <N years available.

## Research Sources Used
- https://stockanalysis.com/etf/screener/
- https://etfdb.com/etfdb-category/long-short/
- https://etfdb.com/etfdb-category/managed-futures/
- https://etfdb.com/etfdb-category/market-neutral/
- https://etfdb.com/etfdb-category/multi-alternative/
- https://etfdb.com/etfdb-category/global-macro/
- https://etfdb.com/etfdb-category/merger-arbitrage/
- https://en.wikipedia.org/wiki/Pershing_Square_Holdings
- https://en.wikipedia.org/wiki/Man_Group

## Discovery Stats
| Metric                           | Value      |
| -------------------------------- | ---------- |
| StockAnalysis ETFs parsed        | 5018       |
| StockAnalysis keyword candidates | 337        |
| ETFDB category candidates        | 29         |
| Yahoo ETF search candidates      | 44         |
| ETF seed list count              | 41         |
| ETF combined candidates          | 376        |
| ETF accepted final               | 202        |
| ETF rejected final               | 174        |
| Manager seed list count          | 20         |
| Manager Yahoo search adds        | 0          |
| Manager combined candidates      | 20         |
| Manager accepted final           | 18         |
| Manager rejected final           | 2          |
| Manager composites created       | 22         |
| Benchmark end date               | 2026-03-13 |

## HedgeStyleETF Summary
| Horizon | Mode    | Count | Avg Calmar | Avg SPY (matched) | Avg QQQ (matched) | Beat SPY | Beat QQQ | Beat Either | Beat Both |
| ------- | ------- | ----- | ---------- | ----------------- | ----------------- | -------- | -------- | ----------- | --------- |
| 1Y      | full    | 157   | 1.755      | 1.582             | 1.770             | 76       | 55       | 76          | 55        |
| 1Y      | partial | 43    | 1.586      | 0.752             | 0.448             | 23       | 25       | 26          | 22        |
| 2Y      | full    | 123   | 0.868      | 0.803             | 0.747             | 60       | 70       | 70          | 60        |
| 2Y      | partial | 35    | 1.085      | 0.739             | 0.730             | 17       | 17       | 18          | 16        |
| 3Y      | full    | 84    | 1.041      | 1.168             | 1.240             | 31       | 25       | 31          | 25        |
| 3Y      | partial | 39    | 0.808      | 1.010             | 0.936             | 9        | 19       | 19          | 9         |
| 4Y      | full    | 66    | 0.574      | 0.620             | 0.587             | 28       | 31       | 31          | 28        |
| 4Y      | partial | 19    | 0.797      | 0.929             | 1.018             | 7        | 6        | 7           | 6         |
| 5Y      | full    | 53    | 0.426      | 0.499             | 0.388             | 19       | 31       | 31          | 19        |
| 5Y      | partial | 13    | 0.409      | 0.483             | 0.387             | 3        | 6        | 6           | 3         |

## ManagerEquity Summary
| Horizon | Mode    | Count | Avg Calmar | Avg SPY (matched) | Avg QQQ (matched) | Beat SPY | Beat QQQ | Beat Either | Beat Both |
| ------- | ------- | ----- | ---------- | ----------------- | ----------------- | -------- | -------- | ----------- | --------- |
| 1Y      | full    | 18    | 0.451      | 1.575             | 1.756             | 3        | 3        | 3           | 3         |
| 2Y      | full    | 18    | 0.321      | 0.788             | 0.741             | 4        | 4        | 4           | 4         |
| 3Y      | full    | 18    | 0.442      | 1.144             | 1.219             | 0        | 0        | 0           | 0         |
| 4Y      | full    | 17    | 0.283      | 0.629             | 0.599             | 2        | 2        | 2           | 2         |
| 4Y      | partial | 1     | 0.442      | 0.942             | 1.093             | 0        | 0        | 0           | 0         |
| 5Y      | full    | 16    | 0.247      | 0.506             | 0.398             | 0        | 3        | 3           | 0         |
| 5Y      | partial | 1     | 0.194      | 0.466             | 0.393             | 0        | 0        | 0           | 0         |

## ManagerComposite Summary
| Horizon | Mode    | Count | Avg Calmar | Avg SPY (matched) | Avg QQQ (matched) | Beat SPY | Beat QQQ | Beat Either | Beat Both |
| ------- | ------- | ----- | ---------- | ----------------- | ----------------- | -------- | -------- | ----------- | --------- |
| 1Y      | full    | 18    | 1.248      | 1.575             | 1.756             | 8        | 6        | 8           | 6         |
| 1Y      | partial | 4     | -0.474     | 1.050             | 1.188             | 1        | 1        | 1           | 1         |
| 2Y      | full    | 16    | 0.717      | 0.788             | 0.741             | 8        | 9        | 9           | 8         |
| 2Y      | partial | 2     | 0.139      | 0.799             | 0.769             | 0        | 0        | 0           | 0         |
| 3Y      | full    | 11    | 0.959      | 1.144             | 1.219             | 4        | 3        | 4           | 3         |
| 3Y      | partial | 5     | 0.884      | 1.054             | 0.966             | 2        | 3        | 3           | 2         |
| 4Y      | full    | 9     | 0.699      | 0.629             | 0.599             | 6        | 6        | 6           | 6         |
| 4Y      | partial | 2     | 0.364      | 0.922             | 1.007             | 0        | 0        | 0           | 0         |
| 5Y      | full    | 8     | 0.558      | 0.506             | 0.398             | 5        | 5        | 5           | 5         |
| 5Y      | partial | 1     | 0.309      | 0.409             | 0.303             | 0        | 1        | 1           | 0         |

## HedgeStyleETF Winners By Timeframe (Beat SPY or QQQ)

### 1Y Partial
| Rank | Ticker | Manager               | Start      | End        | Calmar | SPY    | QQQ    | Beat SPY | Beat QQQ | Name                                                      |
| ---- | ------ | --------------------- | ---------- | ---------- | ------ | ------ | ------ | -------- | -------- | --------------------------------------------------------- |
| 1    | IALT   | BlackRock/iShares     | 2025-12-11 | 2026-03-13 | 20.437 | -2.913 | -3.000 | Y        | Y        | iShares Systematic Alternatives Active ETF                |
| 2    | CHPY   | YieldMax              | 2025-04-08 | 2026-03-13 | 9.838  | 5.978  | 6.024  | Y        | Y        | YieldMax Semiconductor Portfolio Option Income ETF        |
| 3    | SCLS   | Stoneport Advisors    | 2025-11-04 | 2026-03-13 | 8.547  | -0.962 | -1.768 | Y        | Y        | Stoneport Advisors Commodity Long Short ETF               |
| 4    | FFUT   | Fidelity Managed      | 2025-06-05 | 2026-03-13 | 7.101  | 3.318  | 2.288  | Y        | Y        | Fidelity Managed Futures ETF                              |
| 5    | ASGM   | Virtus AlphaSimplex   | 2025-08-05 | 2026-03-13 | 5.658  | 2.024  | 1.337  | Y        | Y        | Virtus AlphaSimplex Global Macro ETF                      |
| 6    | QQHG   | Invesco               | 2025-05-07 | 2026-03-13 | 5.577  | 4.518  | 3.566  | Y        | Y        | Invesco QQQ Hedged Advantage ETF                          |
| 7    | ATTR   | Arin Tactical         | 2025-10-28 | 2026-03-13 | 5.214  | -1.724 | -1.981 | Y        | Y        | Arin Tactical Tail Risk ETF                               |
| 8    | HFGM   | Unlimited HFGM        | 2025-04-15 | 2026-03-13 | 5.122  | 5.351  | 4.283  | N        | Y        | Unlimited HFGM Global Macro ETF                           |
| 9    | SPYH   | NEOS                  | 2025-04-03 | 2026-03-13 | 5.016  | 4.159  | 4.409  | Y        | Y        | NEOS S&P 500 Hedged Equity Income ETF                     |
| 10   | HEDG   | Equable Shares        | 2025-10-13 | 2026-03-13 | 4.477  | 0.087  | -0.381 | Y        | Y        | Equable Shares Hedged Equity ETF                          |
| 11   | SFTY   | Horizon Managed       | 2025-06-26 | 2026-03-13 | 2.935  | 2.504  | 1.630  | Y        | Y        | Horizon Managed Risk ETF                                  |
| 12   | SFTX   | Horizon International | 2025-12-04 | 2026-03-13 | 2.562  | -2.226 | -2.585 | Y        | Y        | Horizon International Managed Risk ETF                    |
| 13   | QALT   | SEI DBi               | 2025-08-25 | 2026-03-13 | 2.532  | 1.344  | 1.028  | Y        | Y        | SEI DBi Multi-Strategy Alternative ETF                    |
| 14   | HEQQ   | JPMorgan              | 2025-03-27 | 2026-03-13 | 2.434  | 1.551  | 1.975  | Y        | Y        | JPMorgan Nasdaq Hedged Equity Laddered Overlay ETF        |
| 15   | HOLA   | JPMorgan              | 2025-07-14 | 2026-03-13 | 2.348  | 1.986  | 1.357  | Y        | Y        | JPMorgan International Hedged Equity Laddered Overlay ETF |
| 16   | SEPI   | Shelton Equity        | 2025-09-08 | 2026-03-13 | 2.101  | 1.038  | 0.708  | Y        | Y        | Shelton Equity Premium Income ETF                         |
| 17   | SCMC   | Sterling              | 2025-12-11 | 2026-03-13 | 0.503  | -2.913 | -3.000 | Y        | Y        | Sterling Capital Multi-Strategy Income ETF                |
| 18   | HEMI   | Hartford              | 2025-12-17 | 2026-03-13 | -0.316 | -0.953 | -0.676 | Y        | Y        | Hartford Equity Premium Income ETF                        |
| 19   | HAKY   | Amplify               | 2026-01-21 | 2026-03-13 | -0.793 | -4.471 | -3.683 | Y        | Y        | Amplify HACK Cybersecurity Covered Call ETF               |
| 20   | YBMN   | Defiance              | 2025-11-25 | 2026-03-13 | -1.076 | -1.132 | -1.272 | Y        | Y        | Defiance BMNR Option Income ETF                           |
| 21   | SOLM   | Amplify               | 2025-11-04 | 2026-03-13 | -1.513 | -0.962 | -1.768 | N        | Y        | Amplify Solana 3% Monthly Option Income ETF               |
| 22   | BALQ   | BlackRock/iShares     | 2025-12-03 | 2026-03-13 | -1.631 | -2.145 | -2.597 | Y        | Y        | iShares Nasdaq Premium Income Active ETF                  |
| 23   | ULTI   | REX                   | 2025-10-31 | 2026-03-13 | -1.806 | -1.496 | -1.994 | N        | Y        | REX IncomeMax Option Strategy ETF                         |
| 24   | NLSI   | NEOS                  | 2025-12-10 | 2026-03-13 | -1.870 | -2.701 | -3.126 | Y        | Y        | Neos Long/Short Equity Income ETF                         |
| 25   | SCEP   | Sterling              | 2025-12-11 | 2026-03-13 | -2.762 | -2.913 | -3.000 | Y        | Y        | Sterling Capital Hedged Equity Premium Income ETF         |
| 26   | WTLS   | WisdomTree            | 2026-01-23 | 2026-03-13 | -5.075 | -5.360 | -4.773 | Y        | N        | Wisdomtree Efficient Long/Short US Equity Fund            |

### 1Y Full
| Rank | Ticker | Manager                  | Start      | End        | Calmar | SPY   | QQQ   | Beat SPY | Beat QQQ | Name                                                          |
| ---- | ------ | ------------------------ | ---------- | ---------- | ------ | ----- | ----- | -------- | -------- | ------------------------------------------------------------- |
| 1    | EQLS   | Simplify                 | 2025-03-13 | 2025-05-30 | 9.564  | 2.796 | 4.003 | Y        | Y        | Simplify Market Neutral Equity Long/Short ETF                 |
| 2    | DBMF   | iMGP DBi                 | 2025-03-13 | 2026-03-13 | 6.327  | 1.575 | 1.756 | Y        | Y        | iMGP DBi Managed Futures Strategy ETF                         |
| 3    | TOAK   | Twin Oak                 | 2025-03-13 | 2026-03-13 | 5.690  | 1.575 | 1.756 | Y        | Y        | Twin Oak Short Horizon Absolute Return ETF                    |
| 4    | FMF    | First Trust              | 2025-03-13 | 2026-03-13 | 4.947  | 1.575 | 1.756 | Y        | Y        | First Trust Managed Futures Strategy Fund                     |
| 5    | WTMF   | WisdomTree               | 2025-03-13 | 2026-03-13 | 4.557  | 1.575 | 1.756 | Y        | Y        | WisdomTree Managed Futures Strategy Fund                      |
| 6    | MSMR   | McElhenny Sheffield      | 2025-03-13 | 2026-03-13 | 4.497  | 1.575 | 1.756 | Y        | Y        | McElhenny Sheffield Managed Risk ETF                          |
| 7    | ARP    | Pmv Adaptive             | 2025-03-13 | 2026-03-13 | 4.227  | 1.575 | 1.756 | Y        | Y        | Pmv Adaptive Risk Parity ETF                                  |
| 8    | LALT   | First Trust              | 2025-03-13 | 2026-03-13 | 3.841  | 1.575 | 1.756 | Y        | Y        | First Trust Multi-Strategy Alternative ETF                    |
| 9    | FARX   | Frontier Asset           | 2025-03-13 | 2026-03-13 | 3.822  | 1.575 | 1.756 | Y        | Y        | Frontier Asset Absolute Return ETF                            |
| 10   | CLSE   | Convergence Long/Short   | 2025-03-13 | 2026-03-13 | 3.636  | 1.575 | 1.756 | Y        | Y        | Convergence Long/Short Equity ETF                             |
| 11   | ORR    | Militia Long/Short       | 2025-03-13 | 2026-03-13 | 3.591  | 1.575 | 1.756 | Y        | Y        | Militia Long/Short Equity ETF                                 |
| 12   | GDXY   | YieldMax                 | 2025-03-13 | 2026-03-13 | 3.588  | 1.575 | 1.756 | Y        | Y        | YieldMax Gold Miners Option Income Strategy ETF               |
| 13   | TFPN   | Blueprint Chesapeake     | 2025-03-13 | 2026-03-13 | 3.518  | 1.575 | 1.756 | Y        | Y        | Blueprint Chesapeake Multi-Asset Trend ETF                    |
| 14   | ISMF   | BlackRock/iShares        | 2025-03-13 | 2026-03-13 | 3.444  | 1.575 | 1.756 | Y        | Y        | iShares Managed Futures Active ETF                            |
| 15   | OVLH   | Overlay Shares           | 2025-03-13 | 2026-03-13 | 3.394  | 1.575 | 1.756 | Y        | Y        | Overlay Shares Hedged Large Cap Equity ETF                    |
| 16   | HEGD   | Swan Hedged              | 2025-03-13 | 2026-03-13 | 3.344  | 1.575 | 1.756 | Y        | Y        | Swan Hedged Equity US Large Cap ETF                           |
| 17   | MARB   | First Trust              | 2025-03-13 | 2026-03-13 | 3.123  | 1.575 | 1.756 | Y        | Y        | First Trust Merger Arbitrage ETF                              |
| 18   | ARB    | AltShares Merger         | 2025-03-13 | 2026-03-13 | 3.083  | 1.575 | 1.756 | Y        | Y        | AltShares Merger Arbitrage ETF                                |
| 19   | SOXY   | YieldMax                 | 2025-03-13 | 2026-03-13 | 2.958  | 1.575 | 1.756 | Y        | Y        | YieldMax Target 12 Semiconductor Option Income ETF            |
| 20   | EHLS   | Even Herd                | 2025-03-13 | 2026-03-13 | 2.930  | 1.575 | 1.756 | Y        | Y        | Even Herd Long Short ETF                                      |
| 21   | FAAR   | First Trust              | 2025-03-13 | 2026-03-13 | 2.896  | 1.575 | 1.756 | Y        | Y        | First Trust Alternative Absolute Return Strategy ETF          |
| 22   | GMOM   | Cambria Global           | 2025-03-13 | 2026-03-13 | 2.861  | 1.575 | 1.756 | Y        | Y        | Cambria Global Momentum ETF                                   |
| 23   | DXJ    | WisdomTree               | 2025-03-13 | 2026-03-13 | 2.748  | 1.575 | 1.756 | Y        | Y        | WisdomTree Japan Hedged Equity Fund                           |
| 24   | MNA    | NYLI Merger              | 2025-03-13 | 2026-03-13 | 2.715  | 1.575 | 1.756 | Y        | Y        | NYLI Merger Arbitrage ETF                                     |
| 25   | ZHDG   | ZEGA Buy                 | 2025-03-13 | 2026-03-13 | 2.640  | 1.575 | 1.756 | Y        | Y        | ZEGA Buy and Hedge ETF                                        |
| 26   | MRSK   | Toews Agility            | 2025-03-13 | 2026-03-13 | 2.590  | 1.575 | 1.756 | Y        | Y        | Toews Agility Shares Managed Risk ETF                         |
| 27   | FLSP   | Franklin Systematic      | 2025-03-13 | 2026-03-13 | 2.589  | 1.575 | 1.756 | Y        | Y        | Franklin Systematic Style Premia ETF                          |
| 28   | HF     | DGA Core                 | 2025-03-13 | 2026-03-13 | 2.577  | 1.575 | 1.756 | Y        | Y        | DGA Core Plus Absolute Return ETF                             |
| 29   | DBEM   | Xtrackers MSCI           | 2025-03-13 | 2026-03-13 | 2.551  | 1.575 | 1.756 | Y        | Y        | Xtrackers MSCI Emerging Markets Hedged Equity ETF             |
| 30   | DBJP   | Xtrackers MSCI           | 2025-03-13 | 2026-03-13 | 2.526  | 1.575 | 1.756 | Y        | Y        | Xtrackers MSCI Japan Hedged Equity ETF                        |
| 31   | EVNT   | AltShares Event-Driven   | 2025-03-13 | 2026-03-13 | 2.517  | 1.575 | 1.756 | Y        | Y        | AltShares Event-Driven ETF                                    |
| 32   | RPAR   | RPAR Risk                | 2025-03-13 | 2026-03-13 | 2.388  | 1.575 | 1.756 | Y        | Y        | RPAR Risk Parity ETF                                          |
| 33   | AHLT   | American Beacon          | 2025-03-13 | 2026-03-13 | 2.304  | 1.575 | 1.756 | Y        | Y        | American Beacon AHL Trend ETF                                 |
| 34   | KSPY   | Kraneshares Hedgeye      | 2025-03-13 | 2026-03-13 | 2.300  | 1.575 | 1.756 | Y        | Y        | Kraneshares Hedgeye Hedged Equity Index ETF                   |
| 35   | DIVO   | Amplify                  | 2025-03-13 | 2026-03-13 | 2.282  | 1.575 | 1.756 | Y        | Y        | Amplify CWP Enhanced Dividend Income ETF                      |
| 36   | FHEQ   | Fidelity Hedged          | 2025-03-13 | 2026-03-13 | 2.244  | 1.575 | 1.756 | Y        | Y        | Fidelity Hedged Equity ETF                                    |
| 37   | USOY   | Defiance                 | 2025-03-13 | 2026-03-13 | 2.211  | 1.575 | 1.756 | Y        | Y        | Defiance Oil Enhanced Options Income ETF                      |
| 38   | IDVO   | Amplify                  | 2025-03-13 | 2026-03-13 | 2.204  | 1.575 | 1.756 | Y        | Y        | Amplify CWP International Enhanced Dividend Income ETF        |
| 39   | DBND   | DoubleLine Opportunistic | 2025-03-13 | 2026-03-13 | 2.011  | 1.575 | 1.756 | Y        | Y        | DoubleLine Opportunistic Core Bond ETF                        |
| 40   | HEQT   | Simplify                 | 2025-03-13 | 2026-03-13 | 1.996  | 1.575 | 1.756 | Y        | Y        | Simplify Hedged Equity ETF                                    |
| 41   | XTR    | Global X                 | 2025-03-13 | 2026-03-13 | 1.994  | 1.575 | 1.756 | Y        | Y        | Global X S&P 500 Tail Risk ETF                                |
| 42   | QCLR   | Global X                 | 2025-03-13 | 2026-03-13 | 1.971  | 1.575 | 1.756 | Y        | Y        | Global X NASDAQ 100 Collar 95-110 ETF                         |
| 43   | XCLR   | Global X                 | 2025-03-13 | 2026-03-13 | 1.925  | 1.575 | 1.756 | Y        | Y        | Global X S&P 500 Collar 95-110 ETF                            |
| 44   | QTR    | Global X                 | 2025-03-13 | 2026-03-13 | 1.866  | 1.575 | 1.756 | Y        | Y        | Global X NASDAQ 100 Tail Risk ETF                             |
| 45   | YMAG   | YieldMax                 | 2025-03-13 | 2026-03-13 | 1.856  | 1.575 | 1.756 | Y        | Y        | YieldMax Magnificent 7 Fund of Option Income ETFs             |
| 46   | KHPI   | Kensington               | 2025-03-13 | 2026-03-13 | 1.855  | 1.575 | 1.756 | Y        | Y        | Kensington Hedged Premium Income ETF                          |
| 47   | GPTY   | YieldMax                 | 2025-03-13 | 2026-03-13 | 1.850  | 1.575 | 1.756 | Y        | Y        | YieldMax AI & Tech Portfolio Option Income ETF                |
| 48   | ASMF   | Virtus AlphaSimplex      | 2025-03-13 | 2026-03-13 | 1.835  | 1.575 | 1.756 | Y        | Y        | Virtus AlphaSimplex Managed Fut                               |
| 49   | RSBT   | Return Stacked           | 2025-03-13 | 2026-03-13 | 1.829  | 1.575 | 1.756 | Y        | Y        | Return Stacked Bonds & Managed Futures ETF                    |
| 50   | DBAW   | Xtrackers MSCI           | 2025-03-13 | 2026-03-13 | 1.821  | 1.575 | 1.756 | Y        | Y        | Xtrackers MSCI All World ex US Hedged Equity ETF              |
| 51   | GPIQ   | Goldman Sachs            | 2025-03-13 | 2026-03-13 | 1.818  | 1.575 | 1.756 | Y        | Y        | Goldman Sachs Nasdaq-100 Premium Income ETF                   |
| 52   | WTPI   | WisdomTree               | 2025-03-13 | 2026-03-13 | 1.783  | 1.575 | 1.756 | Y        | Y        | WisdomTree Equity Premium Income Fund                         |
| 53   | LQDW   | BlackRock/iShares        | 2025-03-13 | 2026-03-13 | 1.783  | 1.575 | 1.756 | Y        | Y        | iShares Investment Grade Corporate Bond Buywrite Strategy ETF |
| 54   | LJUL   | Innovator                | 2025-03-13 | 2026-03-13 | 1.780  | 1.575 | 1.756 | Y        | Y        | Innovator Premium Income 15 Buffer ETF - July                 |
| 55   | GTR    | WisdomTree               | 2025-03-13 | 2026-03-13 | 1.765  | 1.575 | 1.756 | Y        | Y        | WisdomTree Target Range Fund                                  |
| 56   | QAI    | NYLI Hedge               | 2025-03-13 | 2026-03-13 | 1.754  | 1.575 | 1.756 | Y        | N        | NYLI Hedge Multi-Strategy Tracker ETF                         |
| 57   | QYLG   | Global X                 | 2025-03-13 | 2026-03-13 | 1.751  | 1.575 | 1.756 | Y        | N        | Global X Nasdaq 100 Covered Call & Growth ETF                 |
| 58   | VEGA   | AdvisorShares            | 2025-03-13 | 2026-03-13 | 1.740  | 1.575 | 1.756 | Y        | N        | AdvisorShares STAR Global Buy-Write ETF                       |
| 59   | BALI   | BlackRock/iShares        | 2025-03-13 | 2026-03-13 | 1.695  | 1.575 | 1.756 | Y        | N        | iShares U.S. Large Cap Premium Income Active ETF              |
| 60   | HYGW   | BlackRock/iShares        | 2025-03-13 | 2026-03-13 | 1.689  | 1.575 | 1.756 | Y        | N        | iShares High Yield Corporate Bond Buywrite Strategy ETF       |
| 61   | QHDG   | Innovator                | 2025-03-13 | 2026-03-13 | 1.688  | 1.575 | 1.756 | Y        | N        | Innovator Hedged Nasdaq-100 ETF                               |
| 62   | FEPI   | REX                      | 2025-03-13 | 2026-03-13 | 1.683  | 1.575 | 1.756 | Y        | N        | REX FANG & Innovation Equity Premium Income ETF               |
| 63   | NDIV   | Amplify                  | 2025-03-13 | 2026-03-13 | 1.663  | 1.575 | 1.756 | Y        | N        | Amplify Energy & Natural Resources Covered Call ETF           |
| 64   | IWMI   | NEOS                     | 2025-03-13 | 2026-03-13 | 1.657  | 1.575 | 1.756 | Y        | N        | NEOS Russell 2000 High Income ETF                             |
| 65   | QQQI   | NEOS                     | 2025-03-13 | 2026-03-13 | 1.651  | 1.575 | 1.756 | Y        | N        | NEOS NASDAQ-100(R) High Income ETF                            |
| 66   | RSST   | Return Stacked           | 2025-03-13 | 2026-03-13 | 1.637  | 1.575 | 1.756 | Y        | N        | Return Stacked U.S. Stocks & Managed Futures ETF              |
| 67   | SHUS   | Stratified LargeCap      | 2025-03-13 | 2026-03-13 | 1.633  | 1.575 | 1.756 | Y        | N        | Stratified LargeCap Hedged ETF                                |
| 68   | FTQI   | First Trust              | 2025-03-13 | 2026-03-13 | 1.625  | 1.575 | 1.756 | Y        | N        | First Trust Nasdaq BuyWrite Income ETF                        |
| 69   | BIGY   | YieldMax                 | 2025-03-13 | 2026-03-13 | 1.624  | 1.575 | 1.756 | Y        | N        | YieldMax Target 12 Big 50 Option Income ETF                   |
| 70   | JEPQ   | JPMorgan                 | 2025-03-13 | 2026-03-13 | 1.621  | 1.575 | 1.756 | Y        | N        | JPMorgan Nasdaq Equity Premium Income ETF                     |
| 71   | XIJN   | First Trust              | 2025-03-13 | 2026-03-13 | 1.619  | 1.575 | 1.756 | Y        | N        | FT Vest U.S. Equity Buffer & Premium Income ETF - June        |
| 72   | QYLD   | Global X                 | 2025-03-13 | 2026-03-13 | 1.601  | 1.575 | 1.756 | Y        | N        | Global X NASDAQ 100 Covered Call ETF                          |
| 73   | NUSI   | NEOS                     | 2025-03-13 | 2026-03-13 | 1.595  | 1.575 | 1.756 | Y        | N        | NEOS Nasdaq-100 Hedged Equity Income ETF                      |
| 74   | FTLS   | First Trust              | 2025-03-13 | 2026-03-13 | 1.588  | 1.575 | 1.756 | Y        | N        | First Trust Long/Short Equity ETF                             |
| 75   | QQQH   | NEOS                     | 2025-03-13 | 2026-03-13 | 1.588  | 1.575 | 1.756 | Y        | N        | NEOS Nasdaq-100 Hedged Equity Income ETF                      |
| 76   | NBOS   | Neuberger Option         | 2025-03-13 | 2026-03-13 | 1.579  | 1.575 | 1.756 | Y        | N        | Neuberger Option Strategy ETF                                 |

### 2Y Partial
| Rank | Ticker | Manager             | Start      | End        | Calmar | SPY   | QQQ   | Beat SPY | Beat QQQ | Name                                                    |
| ---- | ------ | ------------------- | ---------- | ---------- | ------ | ----- | ----- | -------- | -------- | ------------------------------------------------------- |
| 1    | TOAK   | Twin Oak            | 2024-08-20 | 2026-03-13 | 6.031  | 0.688 | 0.670 | Y        | Y        | Twin Oak Short Horizon Absolute Return ETF              |
| 2    | ORR    | Militia Long/Short  | 2025-01-15 | 2026-03-13 | 3.921  | 0.599 | 0.585 | Y        | Y        | Militia Long/Short Equity ETF                           |
| 3    | ISMF   | BlackRock/iShares   | 2025-03-13 | 2026-03-13 | 3.444  | 1.575 | 1.756 | Y        | Y        | iShares Managed Futures Active ETF                      |
| 4    | FARX   | Frontier Asset      | 2024-12-20 | 2026-03-13 | 2.420  | 0.582 | 0.546 | Y        | Y        | Frontier Asset Absolute Return ETF                      |
| 5    | GDXY   | YieldMax            | 2024-05-21 | 2026-03-13 | 1.962  | 0.767 | 0.723 | Y        | Y        | YieldMax Gold Miners Option Income Strategy ETF         |
| 6    | RSBA   | Return Stacked      | 2024-12-18 | 2026-03-13 | 1.926  | 0.634 | 0.559 | Y        | Y        | Return Stacked Bonds & Merger Arbitrage ETF             |
| 7    | LJUL   | Innovator           | 2024-07-01 | 2026-03-13 | 1.777  | 0.714 | 0.601 | Y        | Y        | Innovator Premium Income 15 Buffer ETF - July           |
| 8    | LAPR   | Innovator           | 2024-04-01 | 2026-03-13 | 1.540  | 0.761 | 0.729 | Y        | Y        | Innovator Premium Income 15 Buffer ETF - April          |
| 9    | XIJN   | First Trust         | 2024-06-24 | 2026-03-13 | 1.486  | 0.722 | 0.642 | Y        | Y        | FT Vest U.S. Equity Buffer & Premium Income ETF - June  |
| 10   | XIMR   | First Trust         | 2024-03-19 | 2026-03-13 | 1.285  | 0.787 | 0.754 | Y        | Y        | FT Vest U.S. Equity Buffer & Premium Income ETF - March |
| 11   | USOY   | Defiance            | 2024-05-10 | 2026-03-13 | 1.157  | 0.822 | 0.798 | Y        | Y        | Defiance Oil Enhanced Options Income ETF                |
| 12   | SOXY   | YieldMax            | 2024-12-03 | 2026-03-13 | 1.156  | 0.473 | 0.536 | Y        | Y        | YieldMax Target 12 Semiconductor Option Income ETF      |
| 13   | MLPD   | Global X            | 2024-05-08 | 2026-03-13 | 1.032  | 0.841 | 0.807 | Y        | Y        | Global X MLP & Energy Infrastructure Covered Call ETF   |
| 14   | FHEQ   | Fidelity Hedged     | 2024-04-11 | 2026-03-13 | 0.968  | 0.801 | 0.740 | Y        | Y        | Fidelity Hedged Equity ETF                              |
| 15   | KSPY   | Kraneshares Hedgeye | 2024-07-16 | 2026-03-13 | 0.912  | 0.604 | 0.527 | Y        | Y        | Kraneshares Hedgeye Hedged Equity Index ETF             |
| 16   | KHPI   | Kensington          | 2024-09-05 | 2026-03-13 | 0.909  | 0.775 | 0.830 | Y        | Y        | Kensington Hedged Premium Income ETF                    |
| 17   | QHDG   | Innovator           | 2024-08-20 | 2026-03-13 | 0.686  | 0.688 | 0.670 | N        | Y        | Innovator Hedged Nasdaq-100 ETF                         |
| 18   | BIGY   | YieldMax            | 2024-11-21 | 2026-03-13 | 0.560  | 0.540 | 0.613 | Y        | N        | YieldMax Target 12 Big 50 Option Income ETF             |

### 2Y Full
| Rank | Ticker | Manager                  | Start      | End        | Calmar | SPY   | QQQ   | Beat SPY | Beat QQQ | Name                                                          |
| ---- | ------ | ------------------------ | ---------- | ---------- | ------ | ----- | ----- | -------- | -------- | ------------------------------------------------------------- |
| 1    | NUSI   | NEOS                     | 2024-03-13 | 2026-03-13 | 4.653  | 0.788 | 0.741 | Y        | Y        | NEOS Nasdaq-100 Hedged Equity Income ETF                      |
| 2    | MNA    | NYLI Merger              | 2024-03-13 | 2026-03-13 | 2.970  | 0.788 | 0.741 | Y        | Y        | NYLI Merger Arbitrage ETF                                     |
| 3    | ARP    | Pmv Adaptive             | 2024-03-13 | 2026-03-13 | 2.378  | 0.788 | 0.741 | Y        | Y        | Pmv Adaptive Risk Parity ETF                                  |
| 4    | EVNT   | AltShares Event-Driven   | 2024-03-13 | 2026-03-13 | 2.202  | 0.788 | 0.741 | Y        | Y        | AltShares Event-Driven ETF                                    |
| 5    | ARB    | AltShares Merger         | 2024-03-13 | 2026-03-13 | 2.192  | 0.788 | 0.741 | Y        | Y        | AltShares Merger Arbitrage ETF                                |
| 6    | MARB   | First Trust              | 2024-03-13 | 2026-03-13 | 2.085  | 0.788 | 0.741 | Y        | Y        | First Trust Merger Arbitrage ETF                              |
| 7    | MSMR   | McElhenny Sheffield      | 2024-03-13 | 2026-03-13 | 1.782  | 0.788 | 0.741 | Y        | Y        | McElhenny Sheffield Managed Risk ETF                          |
| 8    | LALT   | First Trust              | 2024-03-13 | 2026-03-13 | 1.774  | 0.788 | 0.741 | Y        | Y        | First Trust Multi-Strategy Alternative ETF                    |
| 9    | JULJ   | Innovator                | 2024-03-13 | 2026-03-13 | 1.541  | 0.788 | 0.741 | Y        | Y        | Innovator Premium Income 30 Barrier ETF - July                |
| 10   | DBEM   | Xtrackers MSCI           | 2024-03-13 | 2026-03-13 | 1.497  | 0.788 | 0.741 | Y        | Y        | Xtrackers MSCI Emerging Markets Hedged Equity ETF             |
| 11   | HYGW   | BlackRock/iShares        | 2024-03-13 | 2026-03-13 | 1.463  | 0.788 | 0.741 | Y        | Y        | iShares High Yield Corporate Bond Buywrite Strategy ETF       |
| 12   | FLSP   | Franklin Systematic      | 2024-03-13 | 2026-03-13 | 1.436  | 0.788 | 0.741 | Y        | Y        | Franklin Systematic Style Premia ETF                          |
| 13   | FAAR   | First Trust              | 2024-03-13 | 2026-03-13 | 1.431  | 0.788 | 0.741 | Y        | Y        | First Trust Alternative Absolute Return Strategy ETF          |
| 14   | IDVO   | Amplify                  | 2024-03-13 | 2026-03-13 | 1.430  | 0.788 | 0.741 | Y        | Y        | Amplify CWP International Enhanced Dividend Income ETF        |
| 15   | OVLH   | Overlay Shares           | 2024-03-13 | 2026-03-13 | 1.379  | 0.788 | 0.741 | Y        | Y        | Overlay Shares Hedged Large Cap Equity ETF                    |
| 16   | HEGD   | Swan Hedged              | 2024-03-13 | 2026-03-13 | 1.346  | 0.788 | 0.741 | Y        | Y        | Swan Hedged Equity US Large Cap ETF                           |
| 17   | CTA    | Simplify                 | 2024-03-13 | 2026-03-13 | 1.334  | 0.788 | 0.741 | Y        | Y        | Simplify Managed Futures Strategy ETF                         |
| 18   | LQDW   | BlackRock/iShares        | 2024-03-13 | 2026-03-13 | 1.316  | 0.788 | 0.741 | Y        | Y        | iShares Investment Grade Corporate Bond Buywrite Strategy ETF |
| 19   | DBAW   | Xtrackers MSCI           | 2024-03-13 | 2026-03-13 | 1.309  | 0.788 | 0.741 | Y        | Y        | Xtrackers MSCI All World ex US Hedged Equity ETF              |
| 20   | SIXH   | ETC 6                    | 2024-03-13 | 2026-03-13 | 1.258  | 0.788 | 0.741 | Y        | Y        | ETC 6 Meridian Hedged Equity-Index Option Strategy ETF        |
| 21   | DBND   | DoubleLine Opportunistic | 2024-03-13 | 2026-03-13 | 1.254  | 0.788 | 0.741 | Y        | Y        | DoubleLine Opportunistic Core Bond ETF                        |
| 22   | DIVO   | Amplify                  | 2024-03-13 | 2026-03-13 | 1.244  | 0.788 | 0.741 | Y        | Y        | Amplify CWP Enhanced Dividend Income ETF                      |
| 23   | APRJ   | Innovator                | 2024-03-13 | 2026-03-13 | 1.211  | 0.788 | 0.741 | Y        | Y        | Innovator Premium Income 30 Barrier ETF - April               |
| 24   | DXJ    | WisdomTree               | 2024-03-13 | 2026-03-13 | 1.197  | 0.788 | 0.741 | Y        | Y        | WisdomTree Japan Hedged Equity Fund                           |
| 25   | JULH   | Innovator                | 2024-03-13 | 2026-03-13 | 1.172  | 0.788 | 0.741 | Y        | Y        | Innovator Premium Income 20 Barrier ETF - July                |
| 26   | APRH   | Innovator                | 2024-03-13 | 2026-03-13 | 1.165  | 0.788 | 0.741 | Y        | Y        | Innovator Premium Income 20 Barrier ETF - April               |
| 27   | ALTY   | Global X                 | 2024-03-13 | 2026-03-13 | 1.141  | 0.788 | 0.741 | Y        | Y        | Global X Alternative Income ETF                               |
| 28   | CLSE   | Convergence Long/Short   | 2024-03-13 | 2026-03-13 | 1.115  | 0.788 | 0.741 | Y        | Y        | Convergence Long/Short Equity ETF                             |
| 29   | CAOS   | Alpha Architect          | 2024-03-13 | 2026-03-13 | 1.095  | 0.788 | 0.741 | Y        | Y        | Alpha Architect Tail Risk ETF                                 |
| 30   | HEQT   | Simplify                 | 2024-03-13 | 2026-03-13 | 1.064  | 0.788 | 0.741 | Y        | Y        | Simplify Hedged Equity ETF                                    |
| 31   | DBJP   | Xtrackers MSCI           | 2024-03-13 | 2026-03-13 | 1.061  | 0.788 | 0.741 | Y        | Y        | Xtrackers MSCI Japan Hedged Equity ETF                        |
| 32   | LOCT   | Innovator                | 2024-03-13 | 2026-03-13 | 1.048  | 0.788 | 0.741 | Y        | Y        | Innovator Premium Income 15 Buffer ETF - October              |
| 33   | GMOM   | Cambria Global           | 2024-03-13 | 2026-03-13 | 1.034  | 0.788 | 0.741 | Y        | Y        | Cambria Global Momentum ETF                                   |
| 34   | DBEF   | Xtrackers MSCI           | 2024-03-13 | 2026-03-13 | 1.028  | 0.788 | 0.741 | Y        | Y        | Xtrackers MSCI EAFE Hedged Equity ETF                         |
| 35   | RPAR   | RPAR Risk                | 2024-03-13 | 2026-03-13 | 0.992  | 0.788 | 0.741 | Y        | Y        | RPAR Risk Parity ETF                                          |
| 36   | CCEF   | Calamos CEF              | 2024-03-13 | 2026-03-13 | 0.985  | 0.788 | 0.741 | Y        | Y        | Calamos CEF Income & Arbitrage ETF                            |
| 37   | LJAN   | Innovator                | 2024-03-13 | 2026-03-13 | 0.977  | 0.788 | 0.741 | Y        | Y        | Innovator Premium Income 15 Buffer ETF - January              |
| 38   | ZHDG   | ZEGA Buy                 | 2024-03-13 | 2026-03-13 | 0.973  | 0.788 | 0.741 | Y        | Y        | ZEGA Buy and Hedge ETF                                        |
| 39   | QAI    | NYLI Hedge               | 2024-03-13 | 2026-03-13 | 0.964  | 0.788 | 0.741 | Y        | Y        | NYLI Hedge Multi-Strategy Tracker ETF                         |
| 40   | OCTJ   | Innovator                | 2024-03-13 | 2026-03-13 | 0.930  | 0.788 | 0.741 | Y        | Y        | Innovator Premium Income 30 Barrier ETF - October             |
| 41   | CBLS   | Clough Hedged            | 2024-03-13 | 2026-03-13 | 0.920  | 0.788 | 0.741 | Y        | Y        | Clough Hedged Equity ETF                                      |
| 42   | VEGA   | AdvisorShares            | 2024-03-13 | 2026-03-13 | 0.909  | 0.788 | 0.741 | Y        | Y        | AdvisorShares STAR Global Buy-Write ETF                       |
| 43   | NDIV   | Amplify                  | 2024-03-13 | 2026-03-13 | 0.888  | 0.788 | 0.741 | Y        | Y        | Amplify Energy & Natural Resources Covered Call ETF           |
| 44   | BUCK   | Simplify                 | 2024-03-13 | 2026-03-13 | 0.882  | 0.788 | 0.741 | Y        | Y        | Simplify Treasury Option Income ETF                           |
| 45   | BUYW   | Main Management          | 2024-03-13 | 2026-03-13 | 0.874  | 0.788 | 0.741 | Y        | Y        | Main Buywrite ETF                                             |
| 46   | XISE   | First Trust              | 2024-03-13 | 2026-03-13 | 0.868  | 0.788 | 0.741 | Y        | Y        | FT Vest U.S. Equity Buffer & Premium Income ETF – September   |
| 47   | DBEZ   | Xtrackers MSCI           | 2024-03-13 | 2026-03-13 | 0.862  | 0.788 | 0.741 | Y        | Y        | Xtrackers MSCI Eurozone Hedged Equity ETF                     |
| 48   | FMF    | First Trust              | 2024-03-13 | 2026-03-13 | 0.858  | 0.788 | 0.741 | Y        | Y        | First Trust Managed Futures Strategy Fund                     |
| 49   | XIDE   | First Trust              | 2024-03-13 | 2026-03-13 | 0.854  | 0.788 | 0.741 | Y        | Y        | FT Vest U.S. Equity Buffer & Premium Income ETF - December    |
| 50   | NBOS   | Neuberger Option         | 2024-03-13 | 2026-03-13 | 0.853  | 0.788 | 0.741 | Y        | Y        | Neuberger Option Strategy ETF                                 |
| 51   | BALI   | BlackRock/iShares        | 2024-03-13 | 2026-03-13 | 0.841  | 0.788 | 0.741 | Y        | Y        | iShares U.S. Large Cap Premium Income Active ETF              |
| 52   | HELO   | JPMorgan                 | 2024-03-13 | 2026-03-13 | 0.832  | 0.788 | 0.741 | Y        | Y        | Jpmorgan Hedged Equity Laddered Overlay ETF                   |
| 53   | DBEU   | Xtrackers MSCI           | 2024-03-13 | 2026-03-13 | 0.830  | 0.788 | 0.741 | Y        | Y        | Xtrackers MSCI Europe Hedged Equity ETF                       |
| 54   | ACIO   | Aptus                    | 2024-03-13 | 2026-03-13 | 0.830  | 0.788 | 0.741 | Y        | Y        | Aptus Collared Investment Opportunity ETF                     |
| 55   | PHEQ   | Parametric Hedged        | 2024-03-13 | 2026-03-13 | 0.825  | 0.788 | 0.741 | Y        | Y        | Parametric Hedged Equity ETF                                  |
| 56   | SPYI   | NEOS                     | 2024-03-13 | 2026-03-13 | 0.810  | 0.788 | 0.741 | Y        | Y        | Neos S&P 500(R) High Income ETF                               |
| 57   | QQQI   | NEOS                     | 2024-03-13 | 2026-03-13 | 0.802  | 0.788 | 0.741 | Y        | Y        | NEOS NASDAQ-100(R) High Income ETF                            |
| 58   | GPIX   | Goldman Sachs            | 2024-03-13 | 2026-03-13 | 0.796  | 0.788 | 0.741 | Y        | Y        | Goldman Sachs S&P 500 Premium Income ETF                      |
| 59   | MRSK   | Toews Agility            | 2024-03-13 | 2026-03-13 | 0.792  | 0.788 | 0.741 | Y        | Y        | Toews Agility Shares Managed Risk ETF                         |
| 60   | GPIQ   | Goldman Sachs            | 2024-03-13 | 2026-03-13 | 0.791  | 0.788 | 0.741 | Y        | Y        | Goldman Sachs Nasdaq-100 Premium Income ETF                   |
| 61   | JANJ   | Innovator                | 2024-03-13 | 2026-03-13 | 0.787  | 0.788 | 0.741 | N        | Y        | Innovator Premium Income 30 Barrier ETF - January             |
| 62   | XTR    | Global X                 | 2024-03-13 | 2026-03-13 | 0.785  | 0.788 | 0.741 | N        | Y        | Global X S&P 500 Tail Risk ETF                                |
| 63   | HDG    | ProShares Hedge          | 2024-03-13 | 2026-03-13 | 0.785  | 0.788 | 0.741 | N        | Y        | ProShares Hedge Replication ETF                               |
| 64   | PBP    | Invesco                  | 2024-03-13 | 2026-03-13 | 0.783  | 0.788 | 0.741 | N        | Y        | Invesco S&P 500 BuyWrite ETF                                  |
| 65   | WTPI   | WisdomTree               | 2024-03-13 | 2026-03-13 | 0.771  | 0.788 | 0.741 | N        | Y        | WisdomTree Equity Premium Income Fund                         |
| 66   | QDTE   | Roundhill                | 2024-03-13 | 2026-03-13 | 0.759  | 0.788 | 0.741 | N        | Y        | Roundhill Innovation-100 0DTE Covered Call Strategy ETF       |
| 67   | DBMF   | iMGP DBi                 | 2024-03-13 | 2026-03-13 | 0.754  | 0.788 | 0.741 | N        | Y        | iMGP DBi Managed Futures Strategy ETF                         |
| 68   | QCLR   | Global X                 | 2024-03-13 | 2026-03-13 | 0.743  | 0.788 | 0.741 | N        | Y        | Global X NASDAQ 100 Collar 95-110 ETF                         |
| 69   | XCLR   | Global X                 | 2024-03-13 | 2026-03-13 | 0.743  | 0.788 | 0.741 | N        | Y        | Global X S&P 500 Collar 95-110 ETF                            |
| 70   | YMAG   | YieldMax                 | 2024-03-13 | 2026-03-13 | 0.742  | 0.788 | 0.741 | N        | Y        | YieldMax Magnificent 7 Fund of Option Income ETFs             |

### 3Y Partial
| Rank | Ticker | Manager           | Start      | End        | Calmar | SPY   | QQQ   | Beat SPY | Beat QQQ | Name                                                        |
| ---- | ------ | ----------------- | ---------- | ---------- | ------ | ----- | ----- | -------- | -------- | ----------------------------------------------------------- |
| 1    | JULJ   | Innovator         | 2023-07-03 | 2026-03-13 | 1.652  | 0.932 | 0.874 | Y        | Y        | Innovator Premium Income 30 Barrier ETF - July              |
| 2    | HF     | DGA Core          | 2023-08-03 | 2026-03-13 | 1.349  | 0.939 | 0.888 | Y        | Y        | DGA Core Plus Absolute Return ETF                           |
| 3    | APRJ   | Innovator         | 2023-04-03 | 2026-03-13 | 1.310  | 1.021 | 1.061 | Y        | Y        | Innovator Premium Income 30 Barrier ETF - April             |
| 4    | JULH   | Innovator         | 2023-07-03 | 2026-03-13 | 1.273  | 0.932 | 0.874 | Y        | Y        | Innovator Premium Income 20 Barrier ETF - July              |
| 5    | APRH   | Innovator         | 2023-04-03 | 2026-03-13 | 1.259  | 1.021 | 1.061 | Y        | Y        | Innovator Premium Income 20 Barrier ETF - April             |
| 6    | GPIX   | Goldman Sachs     | 2023-10-27 | 2026-03-13 | 1.232  | 1.278 | 1.167 | N        | Y        | Goldman Sachs S&P 500 Premium Income ETF                    |
| 7    | LOCT   | Innovator         | 2023-10-02 | 2026-03-13 | 1.208  | 1.128 | 1.025 | Y        | Y        | Innovator Premium Income 15 Buffer ETF - October            |
| 8    | BALI   | BlackRock/iShares | 2023-09-28 | 2026-03-13 | 1.148  | 1.116 | 1.042 | Y        | Y        | iShares U.S. Large Cap Premium Income Active ETF            |
| 9    | CCEF   | Calamos CEF       | 2024-01-16 | 2026-03-13 | 1.122  | 0.964 | 0.856 | Y        | Y        | Calamos CEF Income & Arbitrage ETF                          |
| 10   | HELO   | JPMorgan          | 2023-09-29 | 2026-03-13 | 1.121  | 1.124 | 1.042 | N        | Y        | Jpmorgan Hedged Equity Laddered Overlay ETF                 |
| 11   | OCTJ   | Innovator         | 2023-10-02 | 2026-03-13 | 1.079  | 1.128 | 1.025 | N        | Y        | Innovator Premium Income 30 Barrier ETF - October           |
| 12   | PHEQ   | Parametric Hedged | 2023-10-19 | 2026-03-13 | 1.076  | 1.160 | 1.058 | N        | Y        | Parametric Hedged Equity ETF                                |
| 13   | LJAN   | Innovator         | 2024-01-02 | 2026-03-13 | 1.030  | 0.961 | 0.882 | Y        | Y        | Innovator Premium Income 15 Buffer ETF - January            |
| 14   | XISE   | First Trust       | 2023-09-18 | 2026-03-13 | 0.993  | 1.011 | 0.951 | N        | Y        | FT Vest U.S. Equity Buffer & Premium Income ETF – September |
| 15   | XIDE   | First Trust       | 2023-12-18 | 2026-03-13 | 0.906  | 0.949 | 0.842 | N        | Y        | FT Vest U.S. Equity Buffer & Premium Income ETF - December  |
| 16   | NBOS   | Neuberger Option  | 2024-01-29 | 2026-03-13 | 0.880  | 0.882 | 0.762 | N        | Y        | Neuberger Option Strategy ETF                               |
| 17   | QQQI   | NEOS              | 2024-01-30 | 2026-03-13 | 0.848  | 0.886 | 0.780 | N        | Y        | NEOS NASDAQ-100(R) High Income ETF                          |
| 18   | YMAG   | YieldMax          | 2024-01-30 | 2026-03-13 | 0.826  | 0.886 | 0.780 | N        | Y        | YieldMax Magnificent 7 Fund of Option Income ETFs           |
| 19   | QDTE   | Roundhill         | 2024-03-07 | 2026-03-13 | 0.709  | 0.788 | 0.705 | N        | Y        | Roundhill Innovation-100 0DTE Covered Call Strategy ETF     |

### 3Y Full
| Rank | Ticker | Manager                | Start      | End        | Calmar | SPY   | QQQ   | Beat SPY | Beat QQQ | Name                                                    |
| ---- | ------ | ---------------------- | ---------- | ---------- | ------ | ----- | ----- | -------- | -------- | ------------------------------------------------------- |
| 1    | NUSI   | NEOS                   | 2023-03-13 | 2026-03-13 | 4.097  | 1.144 | 1.219 | Y        | Y        | NEOS Nasdaq-100 Hedged Equity Income ETF                |
| 2    | MSMR   | McElhenny Sheffield    | 2023-03-13 | 2026-03-13 | 2.146  | 1.144 | 1.219 | Y        | Y        | McElhenny Sheffield Managed Risk ETF                    |
| 3    | EVNT   | AltShares Event-Driven | 2023-03-13 | 2026-03-13 | 2.099  | 1.144 | 1.219 | Y        | Y        | AltShares Event-Driven ETF                              |
| 4    | ARP    | Pmv Adaptive           | 2023-03-13 | 2026-03-13 | 2.087  | 1.144 | 1.219 | Y        | Y        | Pmv Adaptive Risk Parity ETF                            |
| 5    | CAOS   | Alpha Architect        | 2023-03-13 | 2026-03-13 | 1.824  | 1.144 | 1.219 | Y        | Y        | Alpha Architect Tail Risk ETF                           |
| 6    | HEGD   | Swan Hedged            | 2023-03-13 | 2026-03-13 | 1.774  | 1.144 | 1.219 | Y        | Y        | Swan Hedged Equity US Large Cap ETF                     |
| 7    | MNA    | NYLI Merger            | 2023-03-13 | 2026-03-13 | 1.767  | 1.144 | 1.219 | Y        | Y        | NYLI Merger Arbitrage ETF                               |
| 8    | OVLH   | Overlay Shares         | 2023-03-13 | 2026-03-13 | 1.737  | 1.144 | 1.219 | Y        | Y        | Overlay Shares Hedged Large Cap Equity ETF              |
| 9    | FLSP   | Franklin Systematic    | 2023-03-13 | 2026-03-13 | 1.681  | 1.144 | 1.219 | Y        | Y        | Franklin Systematic Style Premia ETF                    |
| 10   | HYGW   | BlackRock/iShares      | 2023-03-13 | 2026-03-13 | 1.650  | 1.144 | 1.219 | Y        | Y        | iShares High Yield Corporate Bond Buywrite Strategy ETF |
| 11   | DXJ    | WisdomTree             | 2023-03-13 | 2026-03-13 | 1.587  | 1.144 | 1.219 | Y        | Y        | WisdomTree Japan Hedged Equity Fund                     |
| 12   | CLSE   | Convergence Long/Short | 2023-03-13 | 2026-03-13 | 1.534  | 1.144 | 1.219 | Y        | Y        | Convergence Long/Short Equity ETF                       |
| 13   | LALT   | First Trust            | 2023-03-13 | 2026-03-13 | 1.466  | 1.144 | 1.219 | Y        | Y        | First Trust Multi-Strategy Alternative ETF              |
| 14   | IDVO   | Amplify                | 2023-03-13 | 2026-03-13 | 1.445  | 1.144 | 1.219 | Y        | Y        | Amplify CWP International Enhanced Dividend Income ETF  |
| 15   | ARB    | AltShares Merger       | 2023-03-13 | 2026-03-13 | 1.435  | 1.144 | 1.219 | Y        | Y        | AltShares Merger Arbitrage ETF                          |
| 16   | SIXH   | ETC 6                  | 2023-03-13 | 2026-03-13 | 1.423  | 1.144 | 1.219 | Y        | Y        | ETC 6 Meridian Hedged Equity-Index Option Strategy ETF  |
| 17   | DBJP   | Xtrackers MSCI         | 2023-03-13 | 2026-03-13 | 1.408  | 1.144 | 1.219 | Y        | Y        | Xtrackers MSCI Japan Hedged Equity ETF                  |
| 18   | CTA    | Simplify               | 2023-03-13 | 2026-03-13 | 1.363  | 1.144 | 1.219 | Y        | Y        | Simplify Managed Futures Strategy ETF                   |
| 19   | HEQT   | Simplify               | 2023-03-13 | 2026-03-13 | 1.332  | 1.144 | 1.219 | Y        | Y        | Simplify Hedged Equity ETF                              |
| 20   | DBAW   | Xtrackers MSCI         | 2023-03-13 | 2026-03-13 | 1.332  | 1.144 | 1.219 | Y        | Y        | Xtrackers MSCI All World ex US Hedged Equity ETF        |
| 21   | DIVO   | Amplify                | 2023-03-13 | 2026-03-13 | 1.309  | 1.144 | 1.219 | Y        | Y        | Amplify CWP Enhanced Dividend Income ETF                |
| 22   | DBEM   | Xtrackers MSCI         | 2023-03-13 | 2026-03-13 | 1.287  | 1.144 | 1.219 | Y        | Y        | Xtrackers MSCI Emerging Markets Hedged Equity ETF       |
| 23   | XTR    | Global X               | 2023-03-13 | 2026-03-13 | 1.233  | 1.144 | 1.219 | Y        | Y        | Global X S&P 500 Tail Risk ETF                          |
| 24   | DBEF   | Xtrackers MSCI         | 2023-03-13 | 2026-03-13 | 1.227  | 1.144 | 1.219 | Y        | Y        | Xtrackers MSCI EAFE Hedged Equity ETF                   |
| 25   | FTLS   | First Trust            | 2023-03-13 | 2026-03-13 | 1.226  | 1.144 | 1.219 | Y        | Y        | First Trust Long/Short Equity ETF                       |
| 26   | QCLR   | Global X               | 2023-03-13 | 2026-03-13 | 1.188  | 1.144 | 1.219 | Y        | N        | Global X NASDAQ 100 Collar 95-110 ETF                   |
| 27   | ZHDG   | ZEGA Buy               | 2023-03-13 | 2026-03-13 | 1.182  | 1.144 | 1.219 | Y        | N        | ZEGA Buy and Hedge ETF                                  |
| 28   | QTR    | Global X               | 2023-03-13 | 2026-03-13 | 1.163  | 1.144 | 1.219 | Y        | N        | Global X NASDAQ 100 Tail Risk ETF                       |
| 29   | XCLR   | Global X               | 2023-03-13 | 2026-03-13 | 1.161  | 1.144 | 1.219 | Y        | N        | Global X S&P 500 Collar 95-110 ETF                      |
| 30   | ACIO   | Aptus                  | 2023-03-13 | 2026-03-13 | 1.159  | 1.144 | 1.219 | Y        | N        | Aptus Collared Investment Opportunity ETF               |
| 31   | VEGA   | AdvisorShares          | 2023-03-13 | 2026-03-13 | 1.155  | 1.144 | 1.219 | Y        | N        | AdvisorShares STAR Global Buy-Write ETF                 |

### 4Y Partial
| Rank | Ticker | Manager           | Start      | End        | Calmar | SPY   | QQQ   | Beat SPY | Beat QQQ | Name                                                    |
| ---- | ------ | ----------------- | ---------- | ---------- | ------ | ----- | ----- | -------- | -------- | ------------------------------------------------------- |
| 1    | ARP    | Pmv Adaptive      | 2022-12-22 | 2026-03-13 | 1.875  | 1.086 | 1.280 | Y        | Y        | Pmv Adaptive Risk Parity ETF                            |
| 2    | CAOS   | Alpha Architect   | 2023-03-06 | 2026-03-13 | 1.548  | 1.033 | 1.152 | Y        | Y        | Alpha Architect Tail Risk ETF                           |
| 3    | LALT   | First Trust       | 2023-02-02 | 2026-03-13 | 1.382  | 0.941 | 1.049 | Y        | Y        | First Trust Multi-Strategy Alternative ETF              |
| 4    | IDVO   | Amplify           | 2022-09-08 | 2026-03-13 | 1.364  | 0.912 | 0.979 | Y        | Y        | Amplify CWP International Enhanced Dividend Income ETF  |
| 5    | HYGW   | BlackRock/iShares | 2022-08-22 | 2026-03-13 | 1.007  | 0.843 | 0.897 | Y        | Y        | iShares High Yield Corporate Bond Buywrite Strategy ETF |
| 6    | BUYW   | Main Management   | 2022-09-12 | 2026-03-13 | 1.000  | 0.868 | 0.931 | Y        | Y        | Main Buywrite ETF                                       |
| 7    | JEPQ   | JPMorgan          | 2022-05-04 | 2026-03-13 | 0.729  | 0.721 | 0.760 | Y        | N        | JPMorgan Nasdaq Equity Premium Income ETF               |

### 4Y Full
| Rank | Ticker | Manager                | Start      | End        | Calmar | SPY    | QQQ    | Beat SPY | Beat QQQ | Name                                                   |
| ---- | ------ | ---------------------- | ---------- | ---------- | ------ | ------ | ------ | -------- | -------- | ------------------------------------------------------ |
| 1    | NUSI   | NEOS                   | 2022-03-14 | 2026-03-13 | 2.021  | 0.629  | 0.599  | Y        | Y        | NEOS Nasdaq-100 Hedged Equity Income ETF               |
| 2    | MSMR   | McElhenny Sheffield    | 2022-03-14 | 2026-03-13 | 1.484  | 0.629  | 0.599  | Y        | Y        | McElhenny Sheffield Managed Risk ETF                   |
| 3    | DXJ    | WisdomTree             | 2022-03-14 | 2026-03-13 | 1.393  | 0.629  | 0.599  | Y        | Y        | WisdomTree Japan Hedged Equity Fund                    |
| 4    | DBJP   | Xtrackers MSCI         | 2022-03-14 | 2026-03-13 | 1.199  | 0.629  | 0.599  | Y        | Y        | Xtrackers MSCI Japan Hedged Equity ETF                 |
| 5    | DBEF   | Xtrackers MSCI         | 2022-03-14 | 2026-03-13 | 1.077  | 0.629  | 0.599  | Y        | Y        | Xtrackers MSCI EAFE Hedged Equity ETF                  |
| 6    | DBAW   | Xtrackers MSCI         | 2022-03-14 | 2026-03-13 | 1.052  | 0.629  | 0.599  | Y        | Y        | Xtrackers MSCI All World ex US Hedged Equity ETF       |
| 7    | CLSE   | Convergence Long/Short | 2022-03-14 | 2026-03-13 | 1.029  | 0.629  | 0.599  | Y        | Y        | Convergence Long/Short Equity ETF                      |
| 8    | HEQT   | Simplify               | 2022-03-14 | 2026-03-13 | 0.985  | 0.629  | 0.599  | Y        | Y        | Simplify Hedged Equity ETF                             |
| 9    | SIXH   | ETC 6                  | 2022-03-14 | 2026-03-13 | 0.980  | 0.629  | 0.599  | Y        | Y        | ETC 6 Meridian Hedged Equity-Index Option Strategy ETF |
| 10   | FLSP   | Franklin Systematic    | 2022-03-14 | 2026-03-13 | 0.892  | 0.629  | 0.599  | Y        | Y        | Franklin Systematic Style Premia ETF                   |
| 11   | DBEU   | Xtrackers MSCI         | 2022-03-14 | 2026-03-13 | 0.872  | 0.629  | 0.599  | Y        | Y        | Xtrackers MSCI Europe Hedged Equity ETF                |
| 12   | DBEZ   | Xtrackers MSCI         | 2022-03-14 | 2026-03-13 | 0.872  | 0.629  | 0.599  | Y        | Y        | Xtrackers MSCI Eurozone Hedged Equity ETF              |
| 13   | MARB   | First Trust            | 2022-03-14 | 2026-03-13 | 0.869  | 0.629  | 0.599  | Y        | Y        | First Trust Merger Arbitrage ETF                       |
| 14   | FTLS   | First Trust            | 2022-03-14 | 2026-03-13 | 0.862  | 0.629  | 0.599  | Y        | Y        | First Trust Long/Short Equity ETF                      |
| 15   | DIVO   | Amplify                | 2022-03-14 | 2026-03-13 | 0.860  | 0.629  | 0.599  | Y        | Y        | Amplify CWP Enhanced Dividend Income ETF               |
| 16   | ACIO   | Aptus                  | 2022-03-14 | 2026-03-13 | 0.842  | 0.629  | 0.599  | Y        | Y        | Aptus Collared Investment Opportunity ETF              |
| 17   | HEGD   | Swan Hedged            | 2022-03-14 | 2026-03-13 | 0.841  | 0.629  | 0.599  | Y        | Y        | Swan Hedged Equity US Large Cap ETF                    |
| 18   | HEDJ   | WisdomTree             | 2022-03-14 | 2026-03-13 | 0.824  | 0.629  | 0.599  | Y        | Y        | WisdomTree Europe Hedged Equity Fund                   |
| 19   | XCLR   | Global X               | 2022-03-14 | 2026-03-13 | 0.720  | 0.629  | 0.599  | Y        | Y        | Global X S&P 500 Collar 95-110 ETF                     |
| 20   | ARB    | AltShares Merger       | 2022-03-14 | 2026-03-13 | 0.705  | 0.629  | 0.599  | Y        | Y        | AltShares Merger Arbitrage ETF                         |
| 21   | HTUS   | Hull Tactical          | 2022-03-14 | 2026-03-13 | 0.705  | 0.629  | 0.599  | Y        | Y        | Hull Tactical US ETF                                   |
| 22   | WTMF   | WisdomTree             | 2022-03-14 | 2026-03-13 | 0.689  | 0.629  | 0.599  | Y        | Y        | WisdomTree Managed Futures Strategy Fund               |
| 23   | EVNT   | AltShares Event-Driven | 2022-03-14 | 2026-03-13 | 0.681  | 0.629  | 0.599  | Y        | Y        | AltShares Event-Driven ETF                             |
| 24   | DBEM   | Xtrackers MSCI         | 2022-03-14 | 2026-03-13 | 0.670  | 0.629  | 0.599  | Y        | Y        | Xtrackers MSCI Emerging Markets Hedged Equity ETF      |
| 25   | CTA    | Simplify               | 2022-03-14 | 2026-03-13 | 0.666  | 0.629  | 0.599  | Y        | Y        | Simplify Managed Futures Strategy ETF                  |
| 26   | QCLR   | Global X               | 2022-03-14 | 2026-03-13 | 0.660  | 0.629  | 0.599  | Y        | Y        | Global X NASDAQ 100 Collar 95-110 ETF                  |
| 27   | FTHI   | First Trust            | 2022-03-14 | 2026-03-13 | 0.658  | 0.629  | 0.599  | Y        | Y        | First Trust BuyWrite Income ETF                        |
| 28   | XTR    | Global X               | 2022-03-14 | 2026-03-13 | 0.653  | 0.629  | 0.599  | Y        | Y        | Global X S&P 500 Tail Risk ETF                         |
| 29   | MRSK   | Toews Agility          | 2022-03-14 | 2026-03-13 | 0.623  | 0.629  | 0.599  | N        | Y        | Toews Agility Shares Managed Risk ETF                  |
| 30   | JEPI   | JPMorgan               | 2022-03-14 | 2026-03-13 | 0.617  | 0.629  | 0.599  | N        | Y        | JPMorgan Equity Premium Income ETF                     |
| 31   | QLS    | IQ Hedge               | 2022-03-14 | 2023-01-31 | -0.148 | -0.048 | -0.257 | N        | Y        | IQ Hedge Long/Short Tracker ETF                        |

### 5Y Partial
| Rank | Ticker | Manager                | Start      | End        | Calmar | SPY   | QQQ   | Beat SPY | Beat QQQ | Name                                  |
| ---- | ------ | ---------------------- | ---------- | ---------- | ------ | ----- | ----- | -------- | -------- | ------------------------------------- |
| 1    | CLSE   | Convergence Long/Short | 2022-02-22 | 2026-03-13 | 1.038  | 0.582 | 0.531 | Y        | Y        | Convergence Long/Short Equity ETF     |
| 2    | HEQT   | Simplify               | 2021-11-02 | 2026-03-13 | 0.709  | 0.415 | 0.311 | Y        | Y        | Simplify Hedged Equity ETF            |
| 3    | MSMR   | McElhenny Sheffield    | 2021-11-17 | 2026-03-13 | 0.647  | 0.405 | 0.298 | Y        | Y        | McElhenny Sheffield Managed Risk ETF  |
| 4    | CTA    | Simplify               | 2022-03-08 | 2026-03-13 | 0.602  | 0.629 | 0.581 | N        | Y        | Simplify Managed Futures Strategy ETF |
| 5    | XTR    | Global X               | 2021-09-07 | 2026-03-13 | 0.356  | 0.425 | 0.313 | N        | Y        | Global X S&P 500 Tail Risk ETF        |
| 6    | QCLR   | Global X               | 2021-08-26 | 2026-03-13 | 0.346  | 0.434 | 0.329 | N        | Y        | Global X NASDAQ 100 Collar 95-110 ETF |

### 5Y Full
| Rank | Ticker | Manager                | Start      | End        | Calmar | SPY   | QQQ    | Beat SPY | Beat QQQ | Name                                                   |
| ---- | ------ | ---------------------- | ---------- | ---------- | ------ | ----- | ------ | -------- | -------- | ------------------------------------------------------ |
| 1    | NUSI   | NEOS                   | 2021-03-15 | 2026-03-13 | 1.195  | 0.506 | 0.398  | Y        | Y        | NEOS Nasdaq-100 Hedged Equity Income ETF               |
| 2    | DXJ    | WisdomTree             | 2021-03-15 | 2026-03-13 | 1.093  | 0.506 | 0.398  | Y        | Y        | WisdomTree Japan Hedged Equity Fund                    |
| 3    | SIXH   | ETC 6                  | 2021-03-15 | 2026-03-13 | 0.942  | 0.506 | 0.398  | Y        | Y        | ETC 6 Meridian Hedged Equity-Index Option Strategy ETF |
| 4    | FLSP   | Franklin Systematic    | 2021-03-15 | 2026-03-13 | 0.937  | 0.506 | 0.398  | Y        | Y        | Franklin Systematic Style Premia ETF                   |
| 5    | DBJP   | Xtrackers MSCI         | 2021-03-15 | 2026-03-13 | 0.867  | 0.506 | 0.398  | Y        | Y        | Xtrackers MSCI Japan Hedged Equity ETF                 |
| 6    | DBEF   | Xtrackers MSCI         | 2021-03-15 | 2026-03-13 | 0.851  | 0.506 | 0.398  | Y        | Y        | Xtrackers MSCI EAFE Hedged Equity ETF                  |
| 7    | FTLS   | First Trust            | 2021-03-15 | 2026-03-13 | 0.825  | 0.506 | 0.398  | Y        | Y        | First Trust Long/Short Equity ETF                      |
| 8    | DIVO   | Amplify                | 2021-03-15 | 2026-03-13 | 0.817  | 0.506 | 0.398  | Y        | Y        | Amplify CWP Enhanced Dividend Income ETF               |
| 9    | MARB   | First Trust            | 2021-03-15 | 2026-03-13 | 0.789  | 0.506 | 0.398  | Y        | Y        | First Trust Merger Arbitrage ETF                       |
| 10   | ARB    | AltShares Merger       | 2021-03-15 | 2026-03-13 | 0.736  | 0.506 | 0.398  | Y        | Y        | AltShares Merger Arbitrage ETF                         |
| 11   | ACIO   | Aptus                  | 2021-03-15 | 2026-03-13 | 0.693  | 0.506 | 0.398  | Y        | Y        | Aptus Collared Investment Opportunity ETF              |
| 12   | DBEU   | Xtrackers MSCI         | 2021-03-15 | 2026-03-13 | 0.657  | 0.506 | 0.398  | Y        | Y        | Xtrackers MSCI Europe Hedged Equity ETF                |
| 13   | JEPI   | JPMorgan               | 2021-03-15 | 2026-03-13 | 0.649  | 0.506 | 0.398  | Y        | Y        | JPMorgan Equity Premium Income ETF                     |
| 14   | WTPI   | WisdomTree             | 2021-03-15 | 2026-03-13 | 0.599  | 0.506 | 0.398  | Y        | Y        | WisdomTree Equity Premium Income Fund                  |
| 15   | DBAW   | Xtrackers MSCI         | 2021-03-15 | 2026-03-13 | 0.597  | 0.506 | 0.398  | Y        | Y        | Xtrackers MSCI All World ex US Hedged Equity ETF       |
| 16   | FTHI   | First Trust            | 2021-03-15 | 2026-03-13 | 0.594  | 0.506 | 0.398  | Y        | Y        | First Trust BuyWrite Income ETF                        |
| 17   | HTUS   | Hull Tactical          | 2021-03-15 | 2026-03-13 | 0.590  | 0.506 | 0.398  | Y        | Y        | Hull Tactical US ETF                                   |
| 18   | HEGD   | Swan Hedged            | 2021-03-15 | 2026-03-13 | 0.561  | 0.506 | 0.398  | Y        | Y        | Swan Hedged Equity US Large Cap ETF                    |
| 19   | MRSK   | Toews Agility          | 2021-03-15 | 2026-03-13 | 0.526  | 0.506 | 0.398  | Y        | Y        | Toews Agility Shares Managed Risk ETF                  |
| 20   | HEDJ   | WisdomTree             | 2021-03-15 | 2026-03-13 | 0.506  | 0.506 | 0.398  | N        | Y        | WisdomTree Europe Hedged Equity Fund                   |
| 21   | FAAR   | First Trust            | 2021-03-15 | 2026-03-13 | 0.497  | 0.506 | 0.398  | N        | Y        | First Trust Alternative Absolute Return Strategy ETF   |
| 22   | DBEZ   | Xtrackers MSCI         | 2021-03-15 | 2026-03-13 | 0.494  | 0.506 | 0.398  | N        | Y        | Xtrackers MSCI Eurozone Hedged Equity ETF              |
| 23   | FTQI   | First Trust            | 2021-03-15 | 2026-03-13 | 0.467  | 0.506 | 0.398  | N        | Y        | First Trust Nasdaq BuyWrite Income ETF                 |
| 24   | WTMF   | WisdomTree             | 2021-03-15 | 2026-03-13 | 0.463  | 0.506 | 0.398  | N        | Y        | WisdomTree Managed Futures Strategy Fund               |
| 25   | XYLG   | Global X               | 2021-03-15 | 2026-03-13 | 0.462  | 0.506 | 0.398  | N        | Y        | Global X S&P 500 Covered Call & Growth ETF             |
| 26   | PBP    | Invesco                | 2021-03-15 | 2026-03-13 | 0.436  | 0.506 | 0.398  | N        | Y        | Invesco S&P 500 BuyWrite ETF                           |
| 27   | OVLH   | Overlay Shares         | 2021-03-15 | 2026-03-13 | 0.425  | 0.506 | 0.398  | N        | Y        | Overlay Shares Hedged Large Cap Equity ETF             |
| 28   | DBMF   | iMGP DBi               | 2021-03-15 | 2026-03-13 | 0.425  | 0.506 | 0.398  | N        | Y        | iMGP DBi Managed Futures Strategy ETF                  |
| 29   | LBAY   | Leatherback Long/Short | 2021-03-15 | 2026-03-13 | 0.420  | 0.506 | 0.398  | N        | Y        | Leatherback Long/Short Alternative Yield ETF           |
| 30   | XYLD   | Global X               | 2021-03-15 | 2026-03-13 | 0.402  | 0.506 | 0.398  | N        | Y        | Global X S&P 500 Covered Call ETF                      |
| 31   | QLS    | IQ Hedge               | 2021-03-15 | 2023-01-31 | -0.066 | 0.120 | -0.099 | N        | Y        | IQ Hedge Long/Short Tracker ETF                        |

## ManagerEquity Winners By Timeframe (Beat SPY or QQQ)

### 1Y Partial
None

### 1Y Full
| Rank | Ticker | Manager                   | Start      | End        | Calmar | SPY   | QQQ   | Beat SPY | Beat QQQ | Name                            |
| ---- | ------ | ------------------------- | ---------- | ---------- | ------ | ----- | ----- | -------- | -------- | ------------------------------- |
| 1    | AMG    | Affiliated Managers Group | 2025-03-13 | 2026-03-13 | 3.928  | 1.575 | 1.756 | Y        | Y        | Affiliated Managers Group, Inc. |
| 2    | BHMG.L | Brevan Howard             | 2025-03-13 | 2026-03-13 | 2.666  | 1.575 | 1.756 | Y        | Y        | BH Macro Limited                |
| 3    | MNGPF  | Man Group                 | 2025-03-13 | 2026-03-13 | 1.781  | 1.575 | 1.756 | Y        | Y        | Man Group Plc                   |

### 2Y Partial
None

### 2Y Full
| Rank | Ticker | Manager                   | Start      | End        | Calmar | SPY   | QQQ   | Beat SPY | Beat QQQ | Name                             |
| ---- | ------ | ------------------------- | ---------- | ---------- | ------ | ----- | ----- | -------- | -------- | -------------------------------- |
| 1    | BHMG.L | Brevan Howard             | 2024-03-13 | 2026-03-13 | 1.285  | 0.788 | 0.741 | Y        | Y        | BH Macro Limited                 |
| 2    | AMG    | Affiliated Managers Group | 2024-03-13 | 2026-03-13 | 1.119  | 0.788 | 0.741 | Y        | Y        | Affiliated Managers Group, Inc.  |
| 3    | TFG.L  | Tetragon                  | 2024-03-13 | 2026-03-13 | 0.955  | 0.788 | 0.741 | Y        | Y        | Tetragon Financial Group Limited |
| 4    | AB     | AllianceBernstein         | 2024-03-13 | 2026-03-13 | 0.934  | 0.788 | 0.741 | Y        | Y        | AllianceBernstein Holding L.P.   |

### 3Y Partial
None

### 3Y Full
None

### 4Y Partial
None

### 4Y Full
| Rank | Ticker | Manager                   | Start      | End        | Calmar | SPY   | QQQ   | Beat SPY | Beat QQQ | Name                             |
| ---- | ------ | ------------------------- | ---------- | ---------- | ------ | ----- | ----- | -------- | -------- | -------------------------------- |
| 1    | AMG    | Affiliated Managers Group | 2022-03-14 | 2026-03-13 | 0.666  | 0.629 | 0.599 | Y        | Y        | Affiliated Managers Group, Inc.  |
| 2    | TFG.L  | Tetragon                  | 2022-03-14 | 2026-03-13 | 0.658  | 0.629 | 0.599 | Y        | Y        | Tetragon Financial Group Limited |

### 5Y Partial
None

### 5Y Full
| Rank | Ticker | Manager                  | Start      | End        | Calmar | SPY   | QQQ   | Beat SPY | Beat QQQ | Name                             |
| ---- | ------ | ------------------------ | ---------- | ---------- | ------ | ----- | ----- | -------- | -------- | -------------------------------- |
| 1    | MNGPF  | Man Group                | 2021-03-15 | 2026-03-13 | 0.487  | 0.506 | 0.398 | N        | Y        | Man Group Plc                    |
| 2    | APO    | Apollo Global Management | 2021-03-15 | 2026-03-13 | 0.444  | 0.506 | 0.398 | N        | Y        | Apollo Global Management, Inc.   |
| 3    | TFG.L  | Tetragon                 | 2021-03-15 | 2026-03-13 | 0.438  | 0.506 | 0.398 | N        | Y        | Tetragon Financial Group Limited |

## ManagerComposite Winners By Timeframe (Beat SPY or QQQ)

### 1Y Partial
| Rank | Ticker        | Manager  | Start      | End        | Calmar | SPY    | QQQ    | Beat SPY | Beat QQQ | Name     |
| ---- | ------------- | -------- | ---------- | ---------- | ------ | ------ | ------ | -------- | -------- | -------- |
| 1    | MGR::Sterling | Sterling | 2025-12-12 | 2026-03-13 | -1.168 | -2.135 | -1.967 | Y        | Y        | Sterling |

### 1Y Full
| Rank | Ticker                   | Manager             | Start      | End        | Calmar | SPY   | QQQ   | Beat SPY | Beat QQQ | Name                |
| ---- | ------------------------ | ------------------- | ---------- | ---------- | ------ | ----- | ----- | -------- | -------- | ------------------- |
| 1    | MGR::Virtus AlphaSimplex | Virtus AlphaSimplex | 2025-03-13 | 2026-03-13 | 2.200  | 1.575 | 1.756 | Y        | Y        | Virtus AlphaSimplex |
| 2    | MGR::YieldMax            | YieldMax            | 2025-03-13 | 2026-03-13 | 2.149  | 1.575 | 1.756 | Y        | Y        | YieldMax            |
| 3    | MGR::First Trust         | First Trust         | 2025-03-13 | 2026-03-13 | 1.960  | 1.575 | 1.756 | Y        | Y        | First Trust         |
| 4    | MGR::Return Stacked      | Return Stacked      | 2025-03-13 | 2026-03-13 | 1.891  | 1.575 | 1.756 | Y        | Y        | Return Stacked      |
| 5    | MGR::WisdomTree          | WisdomTree          | 2025-03-13 | 2026-03-13 | 1.876  | 1.575 | 1.756 | Y        | Y        | WisdomTree          |
| 6    | MGR::BlackRock/iShares   | BlackRock/iShares   | 2025-03-13 | 2026-03-13 | 1.783  | 1.575 | 1.756 | Y        | Y        | BlackRock/iShares   |
| 7    | MGR::Xtrackers MSCI      | Xtrackers MSCI      | 2025-03-13 | 2026-03-13 | 1.720  | 1.575 | 1.756 | Y        | N        | Xtrackers MSCI      |
| 8    | MGR::Goldman Sachs       | Goldman Sachs       | 2025-03-13 | 2026-03-13 | 1.707  | 1.575 | 1.756 | Y        | N        | Goldman Sachs       |

### 2Y Partial
None

### 2Y Full
| Rank | Ticker                 | Manager           | Start      | End        | Calmar | SPY   | QQQ   | Beat SPY | Beat QQQ | Name              |
| ---- | ---------------------- | ----------------- | ---------- | ---------- | ------ | ----- | ----- | -------- | -------- | ----------------- |
| 1    | MGR::NEOS              | NEOS              | 2024-03-13 | 2026-03-13 | 1.918  | 0.788 | 0.741 | Y        | Y        | NEOS              |
| 2    | MGR::Xtrackers MSCI    | Xtrackers MSCI    | 2024-03-13 | 2026-03-13 | 1.203  | 0.788 | 0.741 | Y        | Y        | Xtrackers MSCI    |
| 3    | MGR::WisdomTree        | WisdomTree        | 2024-03-13 | 2026-03-13 | 1.024  | 0.788 | 0.741 | Y        | Y        | WisdomTree        |
| 4    | MGR::Innovator         | Innovator         | 2024-03-13 | 2026-03-13 | 1.006  | 0.788 | 0.741 | Y        | Y        | Innovator         |
| 5    | MGR::First Trust       | First Trust       | 2024-03-13 | 2026-03-13 | 0.961  | 0.788 | 0.741 | Y        | Y        | First Trust       |
| 6    | MGR::BlackRock/iShares | BlackRock/iShares | 2024-03-13 | 2026-03-13 | 0.864  | 0.788 | 0.741 | Y        | Y        | BlackRock/iShares |
| 7    | MGR::Goldman Sachs     | Goldman Sachs     | 2024-03-13 | 2026-03-13 | 0.795  | 0.788 | 0.741 | Y        | Y        | Goldman Sachs     |
| 8    | MGR::YieldMax          | YieldMax          | 2024-03-13 | 2026-03-13 | 0.788  | 0.788 | 0.741 | Y        | Y        | YieldMax          |
| 9    | MGR::JPMorgan          | JPMorgan          | 2024-03-13 | 2026-03-13 | 0.787  | 0.788 | 0.741 | N        | Y        | JPMorgan          |

### 3Y Partial
| Rank | Ticker             | Manager       | Start      | End        | Calmar | SPY   | QQQ   | Beat SPY | Beat QQQ | Name          |
| ---- | ------------------ | ------------- | ---------- | ---------- | ------ | ----- | ----- | -------- | -------- | ------------- |
| 1    | MGR::Innovator     | Innovator     | 2023-04-04 | 2026-03-13 | 1.180  | 1.034 | 1.069 | Y        | Y        | Innovator     |
| 2    | MGR::Goldman Sachs | Goldman Sachs | 2023-10-30 | 2026-03-13 | 1.154  | 1.247 | 1.143 | N        | Y        | Goldman Sachs |
| 3    | MGR::YieldMax      | YieldMax      | 2024-01-18 | 2026-03-13 | 0.988  | 0.959 | 0.839 | Y        | Y        | YieldMax      |

### 3Y Full
| Rank | Ticker              | Manager        | Start      | End        | Calmar | SPY   | QQQ   | Beat SPY | Beat QQQ | Name           |
| ---- | ------------------- | -------------- | ---------- | ---------- | ------ | ----- | ----- | -------- | -------- | -------------- |
| 1    | MGR::NEOS           | NEOS           | 2023-03-13 | 2026-03-13 | 1.957  | 1.144 | 1.219 | Y        | Y        | NEOS           |
| 2    | MGR::WisdomTree     | WisdomTree     | 2023-03-13 | 2026-03-13 | 1.523  | 1.144 | 1.219 | Y        | Y        | WisdomTree     |
| 3    | MGR::Xtrackers MSCI | Xtrackers MSCI | 2023-03-13 | 2026-03-13 | 1.336  | 1.144 | 1.219 | Y        | Y        | Xtrackers MSCI |
| 4    | MGR::JPMorgan       | JPMorgan       | 2023-03-13 | 2026-03-13 | 1.172  | 1.144 | 1.219 | Y        | N        | JPMorgan       |

### 4Y Partial
None

### 4Y Full
| Rank | Ticker              | Manager        | Start      | End        | Calmar | SPY   | QQQ   | Beat SPY | Beat QQQ | Name           |
| ---- | ------------------- | -------------- | ---------- | ---------- | ------ | ----- | ----- | -------- | -------- | -------------- |
| 1    | MGR::WisdomTree     | WisdomTree     | 2022-03-14 | 2026-03-13 | 1.129  | 0.629 | 0.599 | Y        | Y        | WisdomTree     |
| 2    | MGR::Xtrackers MSCI | Xtrackers MSCI | 2022-03-14 | 2026-03-13 | 1.117  | 0.629 | 0.599 | Y        | Y        | Xtrackers MSCI |
| 3    | MGR::NEOS           | NEOS           | 2022-03-14 | 2026-03-13 | 0.995  | 0.629 | 0.599 | Y        | Y        | NEOS           |
| 4    | MGR::First Trust    | First Trust    | 2022-03-14 | 2026-03-13 | 0.710  | 0.629 | 0.599 | Y        | Y        | First Trust    |
| 5    | MGR::JPMorgan       | JPMorgan       | 2022-03-14 | 2026-03-13 | 0.681  | 0.629 | 0.599 | Y        | Y        | JPMorgan       |
| 6    | MGR::Simplify       | Simplify       | 2022-03-14 | 2026-03-13 | 0.645  | 0.629 | 0.599 | Y        | Y        | Simplify       |

### 5Y Partial
| Rank | Ticker        | Manager  | Start      | End        | Calmar | SPY   | QQQ   | Beat SPY | Beat QQQ | Name     |
| ---- | ------------- | -------- | ---------- | ---------- | ------ | ----- | ----- | -------- | -------- | -------- |
| 1    | MGR::Simplify | Simplify | 2021-11-03 | 2026-03-13 | 0.309  | 0.409 | 0.303 | N        | Y        | Simplify |

### 5Y Full
| Rank | Ticker              | Manager        | Start      | End        | Calmar | SPY   | QQQ   | Beat SPY | Beat QQQ | Name           |
| ---- | ------------------- | -------------- | ---------- | ---------- | ------ | ----- | ----- | -------- | -------- | -------------- |
| 1    | MGR::WisdomTree     | WisdomTree     | 2021-03-15 | 2026-03-13 | 0.869  | 0.506 | 0.398 | Y        | Y        | WisdomTree     |
| 2    | MGR::First Trust    | First Trust    | 2021-03-15 | 2026-03-13 | 0.788  | 0.506 | 0.398 | Y        | Y        | First Trust    |
| 3    | MGR::Xtrackers MSCI | Xtrackers MSCI | 2021-03-15 | 2026-03-13 | 0.685  | 0.506 | 0.398 | Y        | Y        | Xtrackers MSCI |
| 4    | MGR::JPMorgan       | JPMorgan       | 2021-03-15 | 2026-03-13 | 0.673  | 0.506 | 0.398 | Y        | Y        | JPMorgan       |
| 5    | MGR::NEOS           | NEOS           | 2021-03-15 | 2026-03-13 | 0.555  | 0.506 | 0.398 | Y        | Y        | NEOS           |
