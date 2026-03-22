# Person-Centric Fund Manager Track Record Screen (as of 2026-03-14)

This report tracks named portfolio managers (people), not fund families.
Manager names + tenure were extracted from Morningstar ETF `People` pages.
Person performance is stitched across managed ETFs using tenure windows and equal-weight daily blending across concurrent assignments.
Then Calmar is compared to matched-window SPY/QQQ for 1Y–5Y full/partial sections.

## Coverage
| Metric                                                | Count |
| ----------------------------------------------------- | ----- |
| Funds selected (1Y full eligible from prior universe) | 105   |
| Funds with parsed manager team                        | 105   |
| Unique people identified                              | 136   |
| People with usable stitched history                   | 136   |

## Person Screen Summary (vs SPY/QQQ)
| Horizon | Mode    | People | Avg Calmar | Avg SPY (matched) | Avg QQQ (matched) | Beat SPY | Beat QQQ | Beat Either | Beat Both |
| ------- | ------- | ------ | ---------- | ----------------- | ----------------- | -------- | -------- | ----------- | --------- |
| 1Y      | full    | 124    | 1.784      | 1.575             | 1.756             | 69       | 48       | 69          | 48        |
| 1Y      | partial | 12     | 1.804      | -3.566            | -3.470            | 11       | 11       | 11          | 11        |
| 2Y      | full    | 102    | 0.862      | 0.788             | 0.741             | 57       | 59       | 59          | 57        |
| 2Y      | partial | 22     | 1.456      | 0.749             | 0.762             | 10       | 9        | 11          | 8         |
| 3Y      | full    | 71     | 0.993      | 1.144             | 1.219             | 24       | 22       | 24          | 22        |
| 3Y      | partial | 31     | 0.774      | 1.029             | 0.954             | 9        | 14       | 14          | 9         |
| 4Y      | full    | 58     | 0.568      | 0.629             | 0.599             | 25       | 30       | 30          | 25        |
| 4Y      | partial | 13     | 0.958      | 0.943             | 1.044             | 5        | 5        | 5           | 5         |
| 5Y      | full    | 51     | 0.468      | 0.506             | 0.398             | 25       | 29       | 29          | 25        |
| 5Y      | partial | 7      | 0.535      | 0.459             | 0.361             | 4        | 4        | 4           | 4         |

## Top People (Good Bucket)
| Person               | Funds | Sections | BeatEitherRate | Avg Calmar | 1Y Full Calmar |
| -------------------- | ----- | -------- | -------------- | ---------- | -------------- |
| Daniel Snover        | 1     | 4        | 100.0%         | 2.643      | 4.227          |
| Jeffrey W. Kernagis  | 1     | 4        | 100.0%         | 2.643      | 4.227          |
| Daniel J. Lindquist  | 1     | 4        | 100.0%         | 2.122      | 3.841          |
| Francis J. Ok        | 2     | 3        | 100.0%         | 2.109      | 2.295          |
| Christopher W. Floyd | 1     | 3        | 100.0%         | 1.903      | 2.589          |
| Eric Becker          | 2     | 5        | 100.0%         | 1.752      | 2.857          |
| John S. Orrico       | 2     | 5        | 100.0%         | 1.752      | 2.857          |
| Christopher Plunkett | 1     | 5        | 100.0%         | 1.630      | 3.083          |
| Ryland Matthews      | 1     | 4        | 100.0%         | 1.598      | 2.204          |
| Sundaram Chettiappan | 1     | 5        | 100.0%         | 1.507      | 2.589          |
| Vaneet Chadha        | 1     | 5        | 100.0%         | 1.507      | 2.589          |
| Kevin G. Simpson     | 2     | 5        | 100.0%         | 1.454      | 2.266          |
| Ashif Shaikh         | 6     | 4        | 100.0%         | 1.403      | 1.720          |
| Marlene Walker-Smith | 2     | 5        | 100.0%         | 1.354      | 1.694          |
| Josh L. Smith        | 1     | 5        | 100.0%         | 1.303      | 2.282          |
| Raj Garigipati       | 2     | 3        | 100.0%         | 1.219      | 1.707          |
| Patrick Dwyer        | 6     | 5        | 100.0%         | 1.212      | 1.720          |
| Shlomo Bassous       | 6     | 5        | 100.0%         | 1.212      | 1.720          |
| Matthew A. Heimann   | 1     | 3        | 100.0%         | 1.203      | 1.740          |
| Andrew Serowik       | 2     | 5        | 100.0%         | 1.121      | 1.662          |
| Derek Devens         | 1     | 3        | 100.0%         | 1.107      | 1.579          |
| Eric Zhou            | 1     | 3        | 100.0%         | 1.107      | 1.579          |
| Rory Ewing           | 1     | 3        | 100.0%         | 1.107      | 1.579          |
| Brian Murphy         | 1     | 5        | 80.0%          | 1.594      | 3.123          |
| Michael Grayson      | 1     | 5        | 80.0%          | 1.594      | 3.123          |
| Michael Peck         | 1     | 5        | 80.0%          | 1.594      | 3.123          |
| Greg Barrato         | 2     | 5        | 80.0%          | 1.429      | 2.295          |
| David France         | 3     | 5        | 80.0%          | 1.403      | 1.558          |
| Todd Frysinger       | 3     | 5        | 80.0%          | 1.403      | 1.558          |
| Ammie Weidner        | 1     | 5        | 80.0%          | 1.232      | 1.556          |

## Bottom People (Bad Bucket)
| Person              | Funds | Sections | BeatEitherRate | Avg Calmar | 1Y Full Calmar |
| ------------------- | ----- | -------- | -------------- | ---------- | -------------- |
| David Jackson       | 1     | 3        | 0.0%           | -0.618     | -0.909         |
| Philip Lee          | 1     | 5        | 0.0%           | -0.276     | -0.808         |
| William H. DeRoche  | 1     | 5        | 0.0%           | -0.276     | -0.808         |
| Josh Belko          | 1     | 5        | 0.0%           | -0.276     | -0.808         |
| Peter simasek       | 1     | 5        | 0.0%           | -0.031     | 0.120          |
| David C. Pursell    | 1     | 5        | 0.0%           | -0.021     | 0.120          |
| David Berns         | 4     | 5        | 0.0%           | 0.125      | -0.698         |
| Nancy Davis         | 1     | 5        | 0.0%           | 0.145      | 0.930          |
| David Aspell        | 1     | 5        | 0.0%           | 0.221      | 0.934          |
| Gerald L. Prior     | 1     | 5        | 0.0%           | 0.221      | 0.934          |
| Timothy J. Rudderow | 1     | 5        | 0.0%           | 0.221      | 0.934          |
| Jacob Hemmer        | 1     | 3        | 0.0%           | 0.330      | 0.406          |
| Gerry O’Donnell     | 1     | 3        | 0.0%           | 0.382      | 0.697          |
| Kevin Kelly         | 1     | 3        | 0.0%           | 0.382      | 0.697          |
| David P. Kalis      | 1     | 3        | 0.0%           | 0.401      | -0.064         |
| Gary D. Black       | 1     | 3        | 0.0%           | 0.401      | -0.064         |
| David Hemming       | 1     | 5        | 0.0%           | 0.424      | 0.794          |
| Peter Hubbard       | 1     | 5        | 0.0%           | 0.424      | 0.794          |
| Theodore Samulowitz | 1     | 5        | 0.0%           | 0.424      | 0.794          |
| Drew Justman        | 1     | 3        | 0.0%           | 0.442      | 0.811          |
| Ray DiBernardo      | 1     | 3        | 0.0%           | 0.442      | 0.811          |
| Michael J. Winter   | 1     | 5        | 20.0%          | 0.500      | 1.280          |
| Andrew F. Ngim      | 1     | 3        | 0.0%           | 0.520      | 0.633          |
| Darius Coby         | 1     | 3        | 0.0%           | 0.520      | 0.633          |
| Seth Lancaster      | 1     | 3        | 0.0%           | 0.520      | 0.633          |
| Michael Neches      | 2     | 5        | 0.0%           | 0.601      | 1.147          |
| Tarak Davé          | 2     | 5        | 0.0%           | 0.601      | 1.147          |
| Vincent M. Lorusso  | 1     | 5        | 20.0%          | 0.699      | 1.369          |
| Austin Wen          | 4     | 3        | 0.0%           | 0.762      | 1.259          |
| Vanessa Yang        | 15    | 5        | 0.0%           | 0.805      | 1.507          |

## Winners By Timeframe (People Beating SPY or QQQ)

### 1Y Partial
People in section: **12** | Winners vs SPY or QQQ: **11**

| Rank | Person               | Funds | Assignments | Start      | End        | Calmar  | SPY     | QQQ     | Beat SPY | Beat QQQ | Funds List                         |
| ---- | -------------------- | ----- | ----------- | ---------- | ---------- | ------- | ------- | ------- | -------- | -------- | ---------------------------------- |
| 1    | Matt Brandt          | 2     | 2           | 2025-12-24 | 2026-03-13 | 31.826  | -3.754  | -3.370  | Y        | Y        | GDXY, USOY                         |
| 2    | Giuliana Bordigoni   | 1     | 1           | 2025-08-14 | 2026-03-13 | 9.009   | 1.137   | 0.587   | Y        | Y        | AHLT                               |
| 3    | Tansu Demirbilek     | 1     | 1           | 2025-09-17 | 2026-03-13 | 6.032   | 0.429   | 0.231   | Y        | Y        | ASMF                               |
| 4    | Matthew Osowiecki    | 1     | 1           | 2025-09-29 | 2026-03-13 | 2.163   | 0.037   | -0.198  | Y        | Y        | EVNT                               |
| 5    | Jeff Greco           | 1     | 1           | 2025-07-21 | 2026-03-13 | 2.058   | 1.838   | 1.094   | Y        | Y        | QHDG                               |
| 6    | Rebekah Lipp         | 1     | 1           | 2025-07-21 | 2026-03-13 | 2.058   | 1.838   | 1.094   | Y        | Y        | QHDG                               |
| 7    | Scott Snyder         | 6     | 6           | 2025-11-26 | 2026-03-13 | 0.793   | -1.618  | -1.736  | Y        | Y        | BIGY, GDXY, GPTY, LFGY, SOXY, USOY |
| 8    | Nicholas Quinn       | 4     | 4           | 2025-11-26 | 2026-03-13 | -0.721  | -1.618  | -1.736  | Y        | Y        | BIGY, GPTY, LFGY, SOXY             |
| 9    | John S. McNamara III | 1     | 1           | 2026-01-21 | 2026-03-13 | -2.331  | -4.471  | -3.683  | Y        | Y        | KSPY                               |
| 10   | Andrew Hicks         | 2     | 2           | 2026-03-02 | 2026-03-13 | -14.618 | -18.966 | -17.505 | Y        | Y        | YMAG, YMAX                         |
| 11   | Kimberly Chan        | 2     | 2           | 2026-03-02 | 2026-03-13 | -14.618 | -18.966 | -17.505 | Y        | Y        | YMAG, YMAX                         |

### 1Y Full
People in section: **124** | Winners vs SPY or QQQ: **69**

| Rank | Person                | Funds | Assignments | Start      | End        | Calmar | SPY   | QQQ   | Beat SPY | Beat QQQ | Funds List                               |
| ---- | --------------------- | ----- | ----------- | ---------- | ---------- | ------ | ----- | ----- | -------- | -------- | ---------------------------------------- |
| 1    | Andrew Beer           | 1     | 1           | 2025-03-13 | 2026-03-13 | 6.327  | 1.575 | 1.756 | Y        | Y        | DBMF                                     |
| 2    | Mathias Mamou-Mani    | 1     | 1           | 2025-03-13 | 2026-03-13 | 6.327  | 1.575 | 1.756 | Y        | Y        | DBMF                                     |
| 3    | Greg Stoner           | 1     | 1           | 2025-03-13 | 2026-03-13 | 5.690  | 1.575 | 1.756 | Y        | Y        | TOAK                                     |
| 4    | Zachary Wainwright    | 1     | 1           | 2025-03-13 | 2026-03-13 | 5.690  | 1.575 | 1.756 | Y        | Y        | TOAK                                     |
| 5    | Daniel Snover         | 1     | 1           | 2025-03-13 | 2026-03-13 | 4.227  | 1.575 | 1.756 | Y        | Y        | ARP                                      |
| 6    | Jeffrey W. Kernagis   | 1     | 1           | 2025-03-13 | 2026-03-13 | 4.227  | 1.575 | 1.756 | Y        | Y        | ARP                                      |
| 7    | Daniel J. Lindquist   | 1     | 1           | 2025-03-13 | 2026-03-13 | 3.841  | 1.575 | 1.756 | Y        | Y        | LALT                                     |
| 8    | Paul Wright           | 1     | 1           | 2025-03-13 | 2026-03-13 | 3.822  | 1.575 | 1.756 | Y        | Y        | FARX                                     |
| 9    | Clifford Stanton      | 1     | 1           | 2025-03-13 | 2026-03-13 | 3.822  | 1.575 | 1.756 | Y        | Y        | FARX                                     |
| 10   | Ali Toyran            | 1     | 1           | 2025-03-13 | 2026-03-13 | 3.822  | 1.575 | 1.756 | Y        | Y        | FARX                                     |
| 11   | David Orr             | 1     | 1           | 2025-03-13 | 2026-03-13 | 3.591  | 1.575 | 1.756 | Y        | Y        | ORR                                      |
| 12   | Jerry Ralph Parker    | 1     | 1           | 2025-03-13 | 2026-03-13 | 3.518  | 1.575 | 1.756 | Y        | Y        | TFPN                                     |
| 13   | Jon Robinson          | 1     | 1           | 2025-03-13 | 2026-03-13 | 3.518  | 1.575 | 1.756 | Y        | Y        | TFPN                                     |
| 14   | Brian Murphy          | 1     | 1           | 2025-03-13 | 2026-03-13 | 3.123  | 1.575 | 1.756 | Y        | Y        | MARB                                     |
| 15   | Michael Grayson       | 1     | 1           | 2025-03-13 | 2026-03-13 | 3.123  | 1.575 | 1.756 | Y        | Y        | MARB                                     |
| 16   | Michael Peck          | 1     | 1           | 2025-03-13 | 2026-03-13 | 3.123  | 1.575 | 1.756 | Y        | Y        | MARB                                     |
| 17   | Christopher Plunkett  | 1     | 1           | 2025-03-13 | 2026-03-13 | 3.083  | 1.575 | 1.756 | Y        | Y        | ARB                                      |
| 18   | Sam Jurrens           | 1     | 1           | 2025-03-13 | 2026-03-13 | 2.930  | 1.575 | 1.756 | Y        | Y        | EHLS                                     |
| 19   | Eric Becker           | 2     | 2           | 2025-03-13 | 2026-03-13 | 2.857  | 1.575 | 1.756 | Y        | Y        | ARB, EVNT                                |
| 20   | John S. Orrico        | 2     | 2           | 2025-03-13 | 2026-03-13 | 2.857  | 1.575 | 1.756 | Y        | Y        | ARB, EVNT                                |
| 21   | Charles Collins       | 1     | 1           | 2025-03-13 | 2026-03-13 | 2.590  | 1.575 | 1.756 | Y        | Y        | MRSK                                     |
| 22   | Jason A. Graffius     | 1     | 1           | 2025-03-13 | 2026-03-13 | 2.590  | 1.575 | 1.756 | Y        | Y        | MRSK                                     |
| 23   | Randall D. Schroeder  | 1     | 1           | 2025-03-13 | 2026-03-13 | 2.590  | 1.575 | 1.756 | Y        | Y        | MRSK                                     |
| 24   | Christopher W. Floyd  | 1     | 1           | 2025-03-13 | 2026-03-13 | 2.589  | 1.575 | 1.756 | Y        | Y        | FLSP                                     |
| 25   | Sundaram Chettiappan  | 1     | 1           | 2025-03-13 | 2026-03-13 | 2.589  | 1.575 | 1.756 | Y        | Y        | FLSP                                     |
| 26   | Vaneet Chadha         | 1     | 1           | 2025-03-13 | 2026-03-13 | 2.589  | 1.575 | 1.756 | Y        | Y        | FLSP                                     |
| 27   | Christopher Day       | 1     | 1           | 2025-03-13 | 2026-03-13 | 2.577  | 1.575 | 1.756 | Y        | Y        | HF                                       |
| 28   | Jay Pestrichelli      | 7     | 7           | 2025-03-13 | 2026-03-13 | 2.538  | 1.575 | 1.756 | Y        | Y        | BIGY, GDXY, GPTY, LFGY, SOXY, USOY, ZHDG |
| 29   | Torrey Zaches         | 3     | 3           | 2025-03-13 | 2026-03-13 | 2.422  | 1.575 | 1.756 | Y        | Y        | GTR, WTMF, WTPI                          |
| 30   | James H Stavena       | 3     | 3           | 2025-03-13 | 2026-03-13 | 2.422  | 1.575 | 1.756 | Y        | Y        | GTR, WTMF, WTPI                          |
| 31   | Charles A. Ragauss    | 6     | 6           | 2025-03-13 | 2026-03-13 | 2.322  | 1.575 | 1.756 | Y        | Y        | EHLS, HF, HFND, LBAY, NDIV, RPAR         |
| 32   | Russell Korgaonkar    | 1     | 1           | 2025-03-13 | 2026-03-13 | 2.304  | 1.575 | 1.756 | Y        | Y        | AHLT                                     |
| 33   | Greg Barrato          | 2     | 2           | 2025-03-13 | 2026-03-13 | 2.295  | 1.575 | 1.756 | Y        | Y        | MNA, QAI                                 |
| 34   | Francis J. Ok         | 2     | 2           | 2025-03-13 | 2026-03-13 | 2.295  | 1.575 | 1.756 | Y        | Y        | MNA, QAI                                 |
| 35   | Josh L. Smith         | 1     | 1           | 2025-03-13 | 2026-03-13 | 2.282  | 1.575 | 1.756 | Y        | Y        | DIVO                                     |
| 36   | Kevin G. Simpson      | 2     | 2           | 2025-03-13 | 2026-03-13 | 2.266  | 1.575 | 1.756 | Y        | Y        | DIVO, IDVO                               |
| 37   | Ryland Matthews       | 1     | 1           | 2025-03-13 | 2026-03-13 | 2.204  | 1.575 | 1.756 | Y        | Y        | IDVO                                     |
| 38   | John W. Gambla        | 7     | 7           | 2025-03-13 | 2026-03-13 | 2.155  | 1.575 | 1.756 | Y        | Y        | FAAR, FMF, FTHI, FTKI, FTLS, FTQI, LALT  |
| 39   | Rob A. Guttschow      | 7     | 7           | 2025-03-13 | 2026-03-13 | 2.155  | 1.575 | 1.756 | Y        | Y        | FAAR, FMF, FTHI, FTKI, FTLS, FTQI, LALT  |
| 40   | Jeffrey E. Gundlach   | 1     | 1           | 2025-03-13 | 2026-03-13 | 2.011  | 1.575 | 1.756 | Y        | Y        | DBND                                     |
| 41   | Jeffrey J. Sherman    | 1     | 1           | 2025-03-13 | 2026-03-13 | 2.011  | 1.575 | 1.756 | Y        | Y        | DBND                                     |
| 42   | Jeffrey A. Schwarte   | 1     | 1           | 2025-03-13 | 2026-03-13 | 1.996  | 1.575 | 1.756 | Y        | Y        | HEQT                                     |
| 43   | Qiao Duan             | 6     | 6           | 2025-03-13 | 2026-03-13 | 1.966  | 1.575 | 1.756 | Y        | Y        | EHLS, HF, TFPN, WEEL, YMAG, YMAX         |
| 44   | Christopher P. Mullen | 2     | 2           | 2025-03-13 | 2026-03-13 | 1.843  | 1.575 | 1.756 | Y        | Y        | WEEL, ZHDG                               |
| 45   | Alexander D. Healy    | 1     | 1           | 2025-03-13 | 2026-03-13 | 1.835  | 1.575 | 1.756 | Y        | Y        | ASMF                                     |
| 46   | Kathryn M. Kaminski   | 1     | 1           | 2025-03-13 | 2026-03-13 | 1.835  | 1.575 | 1.756 | Y        | Y        | ASMF                                     |
| 47   | Ken Miller            | 2     | 2           | 2025-03-13 | 2026-03-13 | 1.794  | 1.575 | 1.756 | Y        | Y        | CTA, HEQT                                |
| 48   | Michael J. Venuto     | 3     | 3           | 2025-03-13 | 2026-03-13 | 1.784  | 1.575 | 1.756 | Y        | Y        | LBAY, NDIV, RPAR                         |
| 49   | Matthew A. Heimann    | 1     | 1           | 2025-03-13 | 2026-03-13 | 1.740  | 1.575 | 1.756 | Y        | N        | VEGA                                     |
| 50   | Kenneth R. Hyman      | 1     | 1           | 2025-03-13 | 2026-03-13 | 1.740  | 1.575 | 1.756 | Y        | N        | VEGA                                     |
| 51   | Patrick Dwyer         | 6     | 6           | 2025-03-13 | 2026-03-13 | 1.720  | 1.575 | 1.756 | Y        | N        | DBAW, DBEF, DBEM, DBEU, DBEZ, DBJP       |
| 52   | Shlomo Bassous        | 6     | 6           | 2025-03-13 | 2026-03-13 | 1.720  | 1.575 | 1.756 | Y        | N        | DBAW, DBEF, DBEM, DBEU, DBEZ, DBJP       |
| 53   | Ashif Shaikh          | 6     | 6           | 2025-03-13 | 2026-03-13 | 1.720  | 1.575 | 1.756 | Y        | N        | DBAW, DBEF, DBEM, DBEU, DBEZ, DBJP       |
| 54   | Raj Garigipati        | 2     | 2           | 2025-03-13 | 2026-03-13 | 1.707  | 1.575 | 1.756 | Y        | N        | GPIQ, GPIX                               |
| 55   | Aron Kershner         | 2     | 2           | 2025-03-13 | 2026-03-13 | 1.707  | 1.575 | 1.756 | Y        | N        | GPIQ, GPIX                               |
| 56   | John Sienkiewicz      | 2     | 2           | 2025-03-13 | 2026-03-13 | 1.707  | 1.575 | 1.756 | Y        | N        | GPIQ, GPIX                               |
| 57   | Marlene Walker-Smith  | 2     | 2           | 2025-03-13 | 2026-03-13 | 1.694  | 1.575 | 1.756 | Y        | N        | DXJ, HEDJ                                |
| 58   | Robert T. Cummings    | 1     | 1           | 2025-03-13 | 2026-03-13 | 1.688  | 1.575 | 1.756 | Y        | N        | QHDG                                     |
| 59   | Andrew Serowik        | 2     | 2           | 2025-03-13 | 2026-03-13 | 1.662  | 1.575 | 1.756 | Y        | N        | SHUS, SIXH                               |
| 60   | Rafael Zayas          | 4     | 4           | 2025-03-13 | 2026-03-13 | 1.657  | 1.575 | 1.756 | Y        | N        | AIPI, ARP, FEPI, SRHR                    |
| 61   | Dustin Lewellyn       | 3     | 3           | 2025-03-13 | 2026-03-13 | 1.650  | 1.575 | 1.756 | Y        | N        | DIVO, HCOW, IDVO                         |
| 62   | Brian Cooper          | 1     | 1           | 2025-03-13 | 2026-03-13 | 1.633  | 1.575 | 1.756 | Y        | N        | SHUS                                     |
| 63   | Todd Alberico         | 1     | 1           | 2025-03-13 | 2026-03-13 | 1.633  | 1.575 | 1.756 | Y        | N        | SHUS                                     |
| 64   | Garrett Paolella      | 2     | 2           | 2025-03-13 | 2026-03-13 | 1.623  | 1.575 | 1.756 | Y        | N        | QQQH, QQQI                               |
| 65   | Troy Cates            | 2     | 2           | 2025-03-13 | 2026-03-13 | 1.623  | 1.575 | 1.756 | Y        | N        | QQQH, QQQI                               |
| 66   | Eric Moreau           | 1     | 1           | 2025-03-13 | 2026-03-13 | 1.621  | 1.575 | 1.756 | Y        | N        | JEPQ                                     |
| 67   | Derek Devens          | 1     | 1           | 2025-03-13 | 2026-03-13 | 1.579  | 1.575 | 1.756 | Y        | N        | NBOS                                     |
| 68   | Eric Zhou             | 1     | 1           | 2025-03-13 | 2026-03-13 | 1.579  | 1.575 | 1.756 | Y        | N        | NBOS                                     |
| 69   | Rory Ewing            | 1     | 1           | 2025-03-13 | 2026-03-13 | 1.579  | 1.575 | 1.756 | Y        | N        | NBOS                                     |

### 2Y Partial
People in section: **22** | Winners vs SPY or QQQ: **11**

| Rank | Person              | Funds | Assignments | Start      | End        | Calmar | SPY   | QQQ   | Beat SPY | Beat QQQ | Funds List |
| ---- | ------------------- | ----- | ----------- | ---------- | ---------- | ------ | ----- | ----- | -------- | -------- | ---------- |
| 1    | Greg Stoner         | 1     | 1           | 2024-08-21 | 2026-03-13 | 6.047  | 0.676 | 0.656 | Y        | Y        | TOAK       |
| 2    | Zachary Wainwright  | 1     | 1           | 2024-08-21 | 2026-03-13 | 6.047  | 0.676 | 0.656 | Y        | Y        | TOAK       |
| 3    | David Orr           | 1     | 1           | 2025-01-16 | 2026-03-13 | 3.944  | 0.611 | 0.618 | Y        | Y        | ORR        |
| 4    | Ali Toyran          | 1     | 1           | 2024-12-23 | 2026-03-13 | 2.391  | 0.554 | 0.508 | Y        | Y        | FARX       |
| 5    | Clifford Stanton    | 1     | 1           | 2024-12-23 | 2026-03-13 | 2.391  | 0.554 | 0.508 | Y        | Y        | FARX       |
| 6    | Paul Wright         | 1     | 1           | 2024-12-23 | 2026-03-13 | 2.391  | 0.554 | 0.508 | Y        | Y        | FARX       |
| 7    | Aron Kershner       | 2     | 2           | 2025-03-11 | 2026-03-13 | 1.629  | 1.490 | 1.684 | Y        | N        | GPIQ, GPIX |
| 8    | John Sienkiewicz    | 2     | 2           | 2025-03-11 | 2026-03-13 | 1.629  | 1.490 | 1.684 | Y        | N        | GPIQ, GPIX |
| 9    | Jason England       | 1     | 1           | 2024-11-04 | 2026-03-13 | 0.868  | 0.700 | 0.732 | Y        | Y        | BUCK       |
| 10   | Jeffrey A. Schwarte | 1     | 1           | 2024-11-04 | 2026-03-13 | 0.784  | 0.700 | 0.732 | Y        | Y        | HEQT       |
| 11   | Robert T. Cummings  | 1     | 1           | 2024-08-21 | 2026-03-13 | 0.675  | 0.676 | 0.656 | N        | Y        | QHDG       |

### 2Y Full
People in section: **102** | Winners vs SPY or QQQ: **59**

| Rank | Person               | Funds | Assignments | Start      | End        | Calmar | SPY   | QQQ   | Beat SPY | Beat QQQ | Funds List                               |
| ---- | -------------------- | ----- | ----------- | ---------- | ---------- | ------ | ----- | ----- | -------- | -------- | ---------------------------------------- |
| 1    | Eric Becker          | 2     | 2           | 2024-03-13 | 2026-03-13 | 2.579  | 0.788 | 0.741 | Y        | Y        | ARB, EVNT                                |
| 2    | John S. Orrico       | 2     | 2           | 2024-03-13 | 2026-03-13 | 2.579  | 0.788 | 0.741 | Y        | Y        | ARB, EVNT                                |
| 3    | Daniel Snover        | 1     | 1           | 2024-03-13 | 2026-03-13 | 2.378  | 0.788 | 0.741 | Y        | Y        | ARP                                      |
| 4    | Jeffrey W. Kernagis  | 1     | 1           | 2024-03-13 | 2026-03-13 | 2.378  | 0.788 | 0.741 | Y        | Y        | ARP                                      |
| 5    | Christopher Plunkett | 1     | 1           | 2024-03-13 | 2026-03-13 | 2.192  | 0.788 | 0.741 | Y        | Y        | ARB                                      |
| 6    | Brian Murphy         | 1     | 1           | 2024-03-13 | 2026-03-13 | 2.085  | 0.788 | 0.741 | Y        | Y        | MARB                                     |
| 7    | Michael Grayson      | 1     | 1           | 2024-03-13 | 2026-03-13 | 2.085  | 0.788 | 0.741 | Y        | Y        | MARB                                     |
| 8    | Michael Peck         | 1     | 1           | 2024-03-13 | 2026-03-13 | 2.085  | 0.788 | 0.741 | Y        | Y        | MARB                                     |
| 9    | Greg Barrato         | 2     | 2           | 2024-03-13 | 2026-03-13 | 1.980  | 0.788 | 0.741 | Y        | Y        | MNA, QAI                                 |
| 10   | Francis J. Ok        | 2     | 2           | 2024-03-13 | 2026-03-13 | 1.980  | 0.788 | 0.741 | Y        | Y        | MNA, QAI                                 |
| 11   | Daniel J. Lindquist  | 1     | 1           | 2024-03-13 | 2026-03-13 | 1.774  | 0.788 | 0.741 | Y        | Y        | LALT                                     |
| 12   | Ken Miller           | 2     | 2           | 2024-03-13 | 2026-03-13 | 1.568  | 0.788 | 0.741 | Y        | Y        | CTA, HEQT                                |
| 13   | Christopher W. Floyd | 1     | 1           | 2024-03-13 | 2026-03-13 | 1.436  | 0.788 | 0.741 | Y        | Y        | FLSP                                     |
| 14   | Vaneet Chadha        | 1     | 1           | 2024-03-13 | 2026-03-13 | 1.436  | 0.788 | 0.741 | Y        | Y        | FLSP                                     |
| 15   | Sundaram Chettiappan | 1     | 1           | 2024-03-13 | 2026-03-13 | 1.436  | 0.788 | 0.741 | Y        | Y        | FLSP                                     |
| 16   | Ryland Matthews      | 1     | 1           | 2024-03-13 | 2026-03-13 | 1.430  | 0.788 | 0.741 | Y        | Y        | IDVO                                     |
| 17   | Kevin G. Simpson     | 2     | 2           | 2024-03-13 | 2026-03-13 | 1.415  | 0.788 | 0.741 | Y        | Y        | DIVO, IDVO                               |
| 18   | Michael Green        | 1     | 1           | 2024-03-13 | 2026-03-13 | 1.334  | 0.788 | 0.741 | Y        | Y        | CTA                                      |
| 19   | Ammie Weidner        | 1     | 1           | 2024-03-13 | 2026-03-13 | 1.258  | 0.788 | 0.741 | Y        | Y        | SIXH                                     |
| 20   | Andrew Mies          | 1     | 1           | 2024-03-13 | 2026-03-13 | 1.258  | 0.788 | 0.741 | Y        | Y        | SIXH                                     |
| 21   | Jeffrey E. Gundlach  | 1     | 1           | 2024-03-13 | 2026-03-13 | 1.254  | 0.788 | 0.741 | Y        | Y        | DBND                                     |
| 22   | Jeffrey J. Sherman   | 1     | 1           | 2024-03-13 | 2026-03-13 | 1.254  | 0.788 | 0.741 | Y        | Y        | DBND                                     |
| 23   | Josh L. Smith        | 1     | 1           | 2024-03-13 | 2026-03-13 | 1.244  | 0.788 | 0.741 | Y        | Y        | DIVO                                     |
| 24   | Patrick Dwyer        | 6     | 6           | 2024-03-13 | 2026-03-13 | 1.203  | 0.788 | 0.741 | Y        | Y        | DBAW, DBEF, DBEM, DBEU, DBEZ, DBJP       |
| 25   | Shlomo Bassous       | 6     | 6           | 2024-03-13 | 2026-03-13 | 1.203  | 0.788 | 0.741 | Y        | Y        | DBAW, DBEF, DBEM, DBEU, DBEZ, DBJP       |
| 26   | Ashif Shaikh         | 6     | 6           | 2024-03-13 | 2026-03-13 | 1.203  | 0.788 | 0.741 | Y        | Y        | DBAW, DBEF, DBEM, DBEU, DBEZ, DBJP       |
| 27   | Nam To               | 1     | 1           | 2024-03-13 | 2026-03-13 | 1.141  | 0.788 | 0.741 | Y        | Y        | ALTY                                     |
| 28   | Marlene Walker-Smith | 2     | 2           | 2024-03-13 | 2026-03-13 | 1.068  | 0.788 | 0.741 | Y        | Y        | DXJ, HEDJ                                |
| 29   | Todd Frysinger       | 3     | 3           | 2024-03-13 | 2026-03-13 | 1.029  | 0.788 | 0.741 | Y        | Y        | DXJ, HEDJ, INDH                          |
| 30   | David France         | 3     | 3           | 2024-03-13 | 2026-03-13 | 1.029  | 0.788 | 0.741 | Y        | Y        | DXJ, HEDJ, INDH                          |
| 31   | Shaheen Iqubal       | 1     | 1           | 2024-03-13 | 2026-03-13 | 0.985  | 0.788 | 0.741 | Y        | Y        | CCEF                                     |
| 32   | R. Matthew Freund    | 1     | 1           | 2024-03-13 | 2026-03-13 | 0.985  | 0.788 | 0.741 | Y        | Y        | CCEF                                     |
| 33   | Charles A. Ragauss   | 6     | 6           | 2024-03-13 | 2026-03-13 | 0.979  | 0.788 | 0.741 | Y        | Y        | EHLS, HF, HFND, LBAY, NDIV, RPAR         |
| 34   | Andrew Serowik       | 2     | 2           | 2024-03-13 | 2026-03-13 | 0.972  | 0.788 | 0.741 | Y        | Y        | SHUS, SIXH                               |
| 35   | Vincent M. Lorusso   | 1     | 1           | 2024-03-13 | 2026-03-13 | 0.920  | 0.788 | 0.741 | Y        | Y        | CBLS                                     |
| 36   | Matthew A. Heimann   | 1     | 1           | 2024-03-13 | 2026-03-13 | 0.909  | 0.788 | 0.741 | Y        | Y        | VEGA                                     |
| 37   | Kenneth R. Hyman     | 1     | 1           | 2024-03-13 | 2026-03-13 | 0.909  | 0.788 | 0.741 | Y        | Y        | VEGA                                     |
| 38   | Garrett Paolella     | 2     | 2           | 2024-03-13 | 2026-03-13 | 0.896  | 0.788 | 0.741 | Y        | Y        | QQQH, QQQI                               |
| 39   | Troy Cates           | 2     | 2           | 2024-03-13 | 2026-03-13 | 0.896  | 0.788 | 0.741 | Y        | Y        | QQQH, QQQI                               |
| 40   | Michael J. Venuto    | 3     | 3           | 2024-03-13 | 2026-03-13 | 0.889  | 0.788 | 0.741 | Y        | Y        | LBAY, NDIV, RPAR                         |
| 41   | Shailesh Gupta       | 1     | 1           | 2024-03-13 | 2026-03-13 | 0.882  | 0.788 | 0.741 | Y        | Y        | BUCK                                     |
| 42   | Dustin Lewellyn      | 3     | 3           | 2024-03-13 | 2026-03-13 | 0.881  | 0.788 | 0.741 | Y        | Y        | DIVO, HCOW, IDVO                         |
| 43   | John W. Gambla       | 7     | 7           | 2024-03-13 | 2026-03-13 | 0.871  | 0.788 | 0.741 | Y        | Y        | FAAR, FMF, FTHI, FTKI, FTLS, FTQI, LALT  |
| 44   | Rob A. Guttschow     | 7     | 7           | 2024-03-13 | 2026-03-13 | 0.871  | 0.788 | 0.741 | Y        | Y        | FAAR, FMF, FTHI, FTKI, FTLS, FTQI, LALT  |
| 45   | Rory Ewing           | 1     | 1           | 2024-03-13 | 2026-03-13 | 0.853  | 0.788 | 0.741 | Y        | Y        | NBOS                                     |
| 46   | Derek Devens         | 1     | 1           | 2024-03-13 | 2026-03-13 | 0.853  | 0.788 | 0.741 | Y        | Y        | NBOS                                     |
| 47   | Eric Zhou            | 1     | 1           | 2024-03-13 | 2026-03-13 | 0.853  | 0.788 | 0.741 | Y        | Y        | NBOS                                     |
| 48   | Alex Zweber          | 2     | 2           | 2024-03-13 | 2026-03-13 | 0.830  | 0.788 | 0.741 | Y        | Y        | PAPI, PHEQ                               |
| 49   | Larry Berman         | 2     | 2           | 2024-03-13 | 2026-03-13 | 0.830  | 0.788 | 0.741 | Y        | Y        | PAPI, PHEQ                               |
| 50   | Michael Zaslavsky    | 2     | 2           | 2024-03-13 | 2026-03-13 | 0.830  | 0.788 | 0.741 | Y        | Y        | PAPI, PHEQ                               |
| 51   | James H Stavena      | 3     | 3           | 2024-03-13 | 2026-03-13 | 0.827  | 0.788 | 0.741 | Y        | Y        | GTR, WTMF, WTPI                          |
| 52   | Torrey Zaches        | 3     | 3           | 2024-03-13 | 2026-03-13 | 0.827  | 0.788 | 0.741 | Y        | Y        | GTR, WTMF, WTPI                          |
| 53   | Jay Pestrichelli     | 7     | 7           | 2024-03-13 | 2026-03-13 | 0.812  | 0.788 | 0.741 | Y        | Y        | BIGY, GDXY, GPTY, LFGY, SOXY, USOY, ZHDG |
| 54   | Raj Garigipati       | 2     | 2           | 2024-03-13 | 2026-03-13 | 0.795  | 0.788 | 0.741 | Y        | Y        | GPIQ, GPIX                               |
| 55   | Charles Collins      | 1     | 1           | 2024-03-13 | 2026-03-13 | 0.792  | 0.788 | 0.741 | Y        | Y        | MRSK                                     |
| 56   | Jason A. Graffius    | 1     | 1           | 2024-03-13 | 2026-03-13 | 0.792  | 0.788 | 0.741 | Y        | Y        | MRSK                                     |
| 57   | Randall D. Schroeder | 1     | 1           | 2024-03-13 | 2026-03-13 | 0.792  | 0.788 | 0.741 | Y        | Y        | MRSK                                     |
| 58   | Andrew Beer          | 1     | 1           | 2024-03-13 | 2026-03-13 | 0.754  | 0.788 | 0.741 | N        | Y        | DBMF                                     |
| 59   | Mathias Mamou-Mani   | 1     | 1           | 2024-03-13 | 2026-03-13 | 0.754  | 0.788 | 0.741 | N        | Y        | DBMF                                     |

### 3Y Partial
People in section: **31** | Winners vs SPY or QQQ: **14**

| Rank | Person               | Funds | Assignments | Start      | End        | Calmar | SPY   | QQQ   | Beat SPY | Beat QQQ | Funds List |
| ---- | -------------------- | ----- | ----------- | ---------- | ---------- | ------ | ----- | ----- | -------- | -------- | ---------- |
| 1    | Francis J. Ok        | 2     | 2           | 2023-10-03 | 2026-03-13 | 2.053  | 1.166 | 1.067 | Y        | Y        | MNA, QAI   |
| 2    | Christopher W. Floyd | 1     | 1           | 2023-10-03 | 2026-03-13 | 1.685  | 1.166 | 1.067 | Y        | Y        | FLSP       |
| 3    | Christopher Day      | 1     | 1           | 2023-08-04 | 2026-03-13 | 1.372  | 0.952 | 0.899 | Y        | Y        | HF         |
| 4    | Raj Garigipati       | 2     | 2           | 2023-10-30 | 2026-03-13 | 1.154  | 1.247 | 1.143 | N        | Y        | GPIQ, GPIX |
| 5    | R. Matthew Freund    | 1     | 1           | 2024-01-17 | 2026-03-13 | 1.149  | 0.983 | 0.872 | Y        | Y        | CCEF       |
| 6    | Shaheen Iqubal       | 1     | 1           | 2024-01-17 | 2026-03-13 | 1.149  | 0.983 | 0.872 | Y        | Y        | CCEF       |
| 7    | Alex Zweber          | 2     | 2           | 2023-10-20 | 2026-03-13 | 1.118  | 1.196 | 1.095 | N        | Y        | PAPI, PHEQ |
| 8    | Larry Berman         | 2     | 2           | 2023-10-20 | 2026-03-13 | 1.118  | 1.196 | 1.095 | N        | Y        | PAPI, PHEQ |
| 9    | Michael Zaslavsky    | 2     | 2           | 2023-10-20 | 2026-03-13 | 1.118  | 1.196 | 1.095 | N        | Y        | PAPI, PHEQ |
| 10   | Shailesh Gupta       | 1     | 1           | 2023-11-20 | 2026-03-13 | 1.016  | 1.031 | 0.910 | N        | Y        | BUCK       |
| 11   | Matthew A. Heimann   | 1     | 1           | 2023-08-01 | 2026-03-13 | 0.959  | 0.895 | 0.837 | Y        | Y        | VEGA       |
| 12   | Derek Devens         | 1     | 1           | 2024-01-30 | 2026-03-13 | 0.889  | 0.886 | 0.780 | Y        | Y        | NBOS       |
| 13   | Eric Zhou            | 1     | 1           | 2024-01-30 | 2026-03-13 | 0.889  | 0.886 | 0.780 | Y        | Y        | NBOS       |
| 14   | Rory Ewing           | 1     | 1           | 2024-01-30 | 2026-03-13 | 0.889  | 0.886 | 0.780 | Y        | Y        | NBOS       |

### 3Y Full
People in section: **71** | Winners vs SPY or QQQ: **24**

| Rank | Person               | Funds | Assignments | Start      | End        | Calmar | SPY   | QQQ   | Beat SPY | Beat QQQ | Funds List                         |
| ---- | -------------------- | ----- | ----------- | ---------- | ---------- | ------ | ----- | ----- | -------- | -------- | ---------------------------------- |
| 1    | Daniel Snover        | 1     | 1           | 2023-03-13 | 2026-03-13 | 2.087  | 1.144 | 1.219 | Y        | Y        | ARP                                |
| 2    | Jeffrey W. Kernagis  | 1     | 1           | 2023-03-13 | 2026-03-13 | 2.087  | 1.144 | 1.219 | Y        | Y        | ARP                                |
| 3    | Eric Becker          | 2     | 2           | 2023-03-13 | 2026-03-13 | 1.996  | 1.144 | 1.219 | Y        | Y        | ARB, EVNT                          |
| 4    | John S. Orrico       | 2     | 2           | 2023-03-13 | 2026-03-13 | 1.996  | 1.144 | 1.219 | Y        | Y        | ARB, EVNT                          |
| 5    | Greg Barrato         | 2     | 2           | 2023-03-13 | 2026-03-13 | 1.948  | 1.144 | 1.219 | Y        | Y        | MNA, QAI                           |
| 6    | Ken Miller           | 2     | 2           | 2023-03-13 | 2026-03-13 | 1.748  | 1.144 | 1.219 | Y        | Y        | CTA, HEQT                          |
| 7    | David France         | 3     | 3           | 2023-03-13 | 2026-03-13 | 1.721  | 1.144 | 1.219 | Y        | Y        | DXJ, HEDJ, INDH                    |
| 8    | Todd Frysinger       | 3     | 3           | 2023-03-13 | 2026-03-13 | 1.721  | 1.144 | 1.219 | Y        | Y        | DXJ, HEDJ, INDH                    |
| 9    | Sundaram Chettiappan | 1     | 1           | 2023-03-13 | 2026-03-13 | 1.681  | 1.144 | 1.219 | Y        | Y        | FLSP                               |
| 10   | Vaneet Chadha        | 1     | 1           | 2023-03-13 | 2026-03-13 | 1.681  | 1.144 | 1.219 | Y        | Y        | FLSP                               |
| 11   | Marlene Walker-Smith | 2     | 2           | 2023-03-13 | 2026-03-13 | 1.512  | 1.144 | 1.219 | Y        | Y        | DXJ, HEDJ                          |
| 12   | Daniel J. Lindquist  | 1     | 1           | 2023-03-13 | 2026-03-13 | 1.466  | 1.144 | 1.219 | Y        | Y        | LALT                               |
| 13   | Kevin G. Simpson     | 2     | 2           | 2023-03-13 | 2026-03-13 | 1.453  | 1.144 | 1.219 | Y        | Y        | DIVO, IDVO                         |
| 14   | Ryland Matthews      | 1     | 1           | 2023-03-13 | 2026-03-13 | 1.445  | 1.144 | 1.219 | Y        | Y        | IDVO                               |
| 15   | Christopher Plunkett | 1     | 1           | 2023-03-13 | 2026-03-13 | 1.435  | 1.144 | 1.219 | Y        | Y        | ARB                                |
| 16   | Ammie Weidner        | 1     | 1           | 2023-03-13 | 2026-03-13 | 1.423  | 1.144 | 1.219 | Y        | Y        | SIXH                               |
| 17   | Andrew Mies          | 1     | 1           | 2023-03-13 | 2026-03-13 | 1.423  | 1.144 | 1.219 | Y        | Y        | SIXH                               |
| 18   | Michael Green        | 1     | 1           | 2023-03-13 | 2026-03-13 | 1.363  | 1.144 | 1.219 | Y        | Y        | CTA                                |
| 19   | Patrick Dwyer        | 6     | 6           | 2023-03-13 | 2026-03-13 | 1.336  | 1.144 | 1.219 | Y        | Y        | DBAW, DBEF, DBEM, DBEU, DBEZ, DBJP |
| 20   | Ashif Shaikh         | 6     | 6           | 2023-03-13 | 2026-03-13 | 1.336  | 1.144 | 1.219 | Y        | Y        | DBAW, DBEF, DBEM, DBEU, DBEZ, DBJP |
| 21   | Shlomo Bassous       | 6     | 6           | 2023-03-13 | 2026-03-13 | 1.336  | 1.144 | 1.219 | Y        | Y        | DBAW, DBEF, DBEM, DBEU, DBEZ, DBJP |
| 22   | Josh L. Smith        | 1     | 1           | 2023-03-13 | 2026-03-13 | 1.309  | 1.144 | 1.219 | Y        | Y        | DIVO                               |
| 23   | Andrew Serowik       | 2     | 2           | 2023-03-13 | 2026-03-13 | 1.173  | 1.144 | 1.219 | Y        | N        | SHUS, SIXH                         |
| 24   | Kenneth R. Hyman     | 1     | 1           | 2023-03-13 | 2026-03-13 | 1.155  | 1.144 | 1.219 | Y        | N        | VEGA                               |

### 4Y Partial
People in section: **13** | Winners vs SPY or QQQ: **5**

| Rank | Person              | Funds | Assignments | Start      | End        | Calmar | SPY   | QQQ   | Beat SPY | Beat QQQ | Funds List                         |
| ---- | ------------------- | ----- | ----------- | ---------- | ---------- | ------ | ----- | ----- | -------- | -------- | ---------------------------------- |
| 1    | Daniel Snover       | 1     | 1           | 2022-12-23 | 2026-03-13 | 1.881  | 1.076 | 1.278 | Y        | Y        | ARP                                |
| 2    | Jeffrey W. Kernagis | 1     | 1           | 2022-12-23 | 2026-03-13 | 1.881  | 1.076 | 1.278 | Y        | Y        | ARP                                |
| 3    | Daniel J. Lindquist | 1     | 1           | 2023-02-03 | 2026-03-13 | 1.405  | 0.965 | 1.082 | Y        | Y        | LALT                               |
| 4    | Ashif Shaikh        | 6     | 6           | 2022-10-04 | 2026-03-13 | 1.354  | 1.032 | 1.100 | Y        | Y        | DBAW, DBEF, DBEM, DBEU, DBEZ, DBJP |
| 5    | Ryland Matthews     | 1     | 1           | 2022-09-09 | 2026-03-13 | 1.312  | 0.886 | 0.947 | Y        | Y        | IDVO                               |

### 4Y Full
People in section: **58** | Winners vs SPY or QQQ: **30**

| Rank | Person               | Funds | Assignments | Start      | End        | Calmar | SPY   | QQQ   | Beat SPY | Beat QQQ | Funds List                              |
| ---- | -------------------- | ----- | ----------- | ---------- | ---------- | ------ | ----- | ----- | -------- | -------- | --------------------------------------- |
| 1    | David France         | 3     | 3           | 2022-03-14 | 2026-03-13 | 1.618  | 0.629 | 0.599 | Y        | Y        | DXJ, HEDJ, INDH                         |
| 2    | Todd Frysinger       | 3     | 3           | 2022-03-14 | 2026-03-13 | 1.618  | 0.629 | 0.599 | Y        | Y        | DXJ, HEDJ, INDH                         |
| 3    | Marlene Walker-Smith | 2     | 2           | 2022-03-14 | 2026-03-13 | 1.381  | 0.629 | 0.599 | Y        | Y        | DXJ, HEDJ                               |
| 4    | Patrick Dwyer        | 6     | 6           | 2022-03-14 | 2026-03-13 | 1.117  | 0.629 | 0.599 | Y        | Y        | DBAW, DBEF, DBEM, DBEU, DBEZ, DBJP      |
| 5    | Shlomo Bassous       | 6     | 6           | 2022-03-14 | 2026-03-13 | 1.117  | 0.629 | 0.599 | Y        | Y        | DBAW, DBEF, DBEM, DBEU, DBEZ, DBJP      |
| 6    | Kevin G. Simpson     | 2     | 2           | 2022-03-14 | 2026-03-13 | 1.116  | 0.629 | 0.599 | Y        | Y        | DIVO, IDVO                              |
| 7    | Ammie Weidner        | 1     | 1           | 2022-03-14 | 2026-03-13 | 0.980  | 0.629 | 0.599 | Y        | Y        | SIXH                                    |
| 8    | Andrew Mies          | 1     | 1           | 2022-03-14 | 2026-03-13 | 0.980  | 0.629 | 0.599 | Y        | Y        | SIXH                                    |
| 9    | Andrew Serowik       | 2     | 2           | 2022-03-14 | 2026-03-13 | 0.911  | 0.629 | 0.599 | Y        | Y        | SHUS, SIXH                              |
| 10   | Sundaram Chettiappan | 1     | 1           | 2022-03-14 | 2026-03-13 | 0.892  | 0.629 | 0.599 | Y        | Y        | FLSP                                    |
| 11   | Vaneet Chadha        | 1     | 1           | 2022-03-14 | 2026-03-13 | 0.892  | 0.629 | 0.599 | Y        | Y        | FLSP                                    |
| 12   | Brian Murphy         | 1     | 1           | 2022-03-14 | 2026-03-13 | 0.869  | 0.629 | 0.599 | Y        | Y        | MARB                                    |
| 13   | Michael Grayson      | 1     | 1           | 2022-03-14 | 2026-03-13 | 0.869  | 0.629 | 0.599 | Y        | Y        | MARB                                    |
| 14   | Michael Peck         | 1     | 1           | 2022-03-14 | 2026-03-13 | 0.869  | 0.629 | 0.599 | Y        | Y        | MARB                                    |
| 15   | Josh L. Smith        | 1     | 1           | 2022-03-14 | 2026-03-13 | 0.860  | 0.629 | 0.599 | Y        | Y        | DIVO                                    |
| 16   | Dustin Lewellyn      | 3     | 3           | 2022-03-14 | 2026-03-13 | 0.816  | 0.629 | 0.599 | Y        | Y        | DIVO, HCOW, IDVO                        |
| 17   | Eric Becker          | 2     | 2           | 2022-03-14 | 2026-03-13 | 0.797  | 0.629 | 0.599 | Y        | Y        | ARB, EVNT                               |
| 18   | John S. Orrico       | 2     | 2           | 2022-03-14 | 2026-03-13 | 0.797  | 0.629 | 0.599 | Y        | Y        | ARB, EVNT                               |
| 19   | Christopher Plunkett | 1     | 1           | 2022-03-14 | 2026-03-13 | 0.705  | 0.629 | 0.599 | Y        | Y        | ARB                                     |
| 20   | Greg Barrato         | 2     | 2           | 2022-03-14 | 2026-03-13 | 0.671  | 0.629 | 0.599 | Y        | Y        | MNA, QAI                                |
| 21   | Hamilton Reiner      | 3     | 3           | 2022-03-14 | 2026-03-13 | 0.666  | 0.629 | 0.599 | Y        | Y        | HELO, JEPI, JEPQ                        |
| 22   | Michael Green        | 1     | 1           | 2022-03-14 | 2026-03-13 | 0.666  | 0.629 | 0.599 | Y        | Y        | CTA                                     |
| 23   | Raffaele Zingone     | 2     | 2           | 2022-03-14 | 2026-03-13 | 0.639  | 0.629 | 0.599 | Y        | Y        | HELO, JEPI                              |
| 24   | Torrey Zaches        | 3     | 3           | 2022-03-14 | 2026-03-13 | 0.633  | 0.629 | 0.599 | Y        | Y        | GTR, WTMF, WTPI                         |
| 25   | James H Stavena      | 3     | 3           | 2022-03-14 | 2026-03-13 | 0.633  | 0.629 | 0.599 | Y        | Y        | GTR, WTMF, WTPI                         |
| 26   | John W. Gambla       | 7     | 7           | 2022-03-14 | 2026-03-13 | 0.628  | 0.629 | 0.599 | N        | Y        | FAAR, FMF, FTHI, FTKI, FTLS, FTQI, LALT |
| 27   | Rob A. Guttschow     | 7     | 7           | 2022-03-14 | 2026-03-13 | 0.628  | 0.629 | 0.599 | N        | Y        | FAAR, FMF, FTHI, FTKI, FTLS, FTQI, LALT |
| 28   | Charles Collins      | 1     | 1           | 2022-03-14 | 2026-03-13 | 0.623  | 0.629 | 0.599 | N        | Y        | MRSK                                    |
| 29   | Jason A. Graffius    | 1     | 1           | 2022-03-14 | 2026-03-13 | 0.623  | 0.629 | 0.599 | N        | Y        | MRSK                                    |
| 30   | Randall D. Schroeder | 1     | 1           | 2022-03-14 | 2026-03-13 | 0.623  | 0.629 | 0.599 | N        | Y        | MRSK                                    |

### 5Y Partial
People in section: **7** | Winners vs SPY or QQQ: **4**

| Rank | Person         | Funds | Assignments | Start      | End        | Calmar | SPY   | QQQ   | Beat SPY | Beat QQQ | Funds List      |
| ---- | -------------- | ----- | ----------- | ---------- | ---------- | ------ | ----- | ----- | -------- | -------- | --------------- |
| 1    | David France   | 3     | 3           | 2021-07-01 | 2026-03-13 | 1.091  | 0.454 | 0.350 | Y        | Y        | DXJ, HEDJ, INDH |
| 2    | Todd Frysinger | 3     | 3           | 2021-07-01 | 2026-03-13 | 1.091  | 0.454 | 0.350 | Y        | Y        | DXJ, HEDJ, INDH |
| 3    | Michael Green  | 1     | 1           | 2022-03-09 | 2026-03-13 | 0.638  | 0.596 | 0.546 | Y        | Y        | CTA             |
| 4    | Torrey Zaches  | 3     | 3           | 2021-09-02 | 2026-03-13 | 0.465  | 0.421 | 0.315 | Y        | Y        | GTR, WTMF, WTPI |

### 5Y Full
People in section: **51** | Winners vs SPY or QQQ: **29**

| Rank | Person               | Funds | Assignments | Start      | End        | Calmar | SPY   | QQQ   | Beat SPY | Beat QQQ | Funds List                              |
| ---- | -------------------- | ----- | ----------- | ---------- | ---------- | ------ | ----- | ----- | -------- | -------- | --------------------------------------- |
| 1    | Marlene Walker-Smith | 2     | 2           | 2021-03-15 | 2026-03-13 | 1.113  | 0.506 | 0.398 | Y        | Y        | DXJ, HEDJ                               |
| 2    | Kevin G. Simpson     | 2     | 2           | 2021-03-15 | 2026-03-13 | 1.022  | 0.506 | 0.398 | Y        | Y        | DIVO, IDVO                              |
| 3    | Ammie Weidner        | 1     | 1           | 2021-03-15 | 2026-03-13 | 0.942  | 0.506 | 0.398 | Y        | Y        | SIXH                                    |
| 4    | Andrew Mies          | 1     | 1           | 2021-03-15 | 2026-03-13 | 0.942  | 0.506 | 0.398 | Y        | Y        | SIXH                                    |
| 5    | Sundaram Chettiappan | 1     | 1           | 2021-03-15 | 2026-03-13 | 0.937  | 0.506 | 0.398 | Y        | Y        | FLSP                                    |
| 6    | Vaneet Chadha        | 1     | 1           | 2021-03-15 | 2026-03-13 | 0.937  | 0.506 | 0.398 | Y        | Y        | FLSP                                    |
| 7    | Andrew Serowik       | 2     | 2           | 2021-03-15 | 2026-03-13 | 0.887  | 0.506 | 0.398 | Y        | Y        | SHUS, SIXH                              |
| 8    | Josh L. Smith        | 1     | 1           | 2021-03-15 | 2026-03-13 | 0.817  | 0.506 | 0.398 | Y        | Y        | DIVO                                    |
| 9    | Brian Murphy         | 1     | 1           | 2021-03-15 | 2026-03-13 | 0.789  | 0.506 | 0.398 | Y        | Y        | MARB                                    |
| 10   | Michael Grayson      | 1     | 1           | 2021-03-15 | 2026-03-13 | 0.789  | 0.506 | 0.398 | Y        | Y        | MARB                                    |
| 11   | Michael Peck         | 1     | 1           | 2021-03-15 | 2026-03-13 | 0.789  | 0.506 | 0.398 | Y        | Y        | MARB                                    |
| 12   | Dustin Lewellyn      | 3     | 3           | 2021-03-15 | 2026-03-13 | 0.768  | 0.506 | 0.398 | Y        | Y        | DIVO, HCOW, IDVO                        |
| 13   | Christopher Plunkett | 1     | 1           | 2021-03-15 | 2026-03-13 | 0.736  | 0.506 | 0.398 | Y        | Y        | ARB                                     |
| 14   | Rob A. Guttschow     | 7     | 7           | 2021-03-15 | 2026-03-13 | 0.690  | 0.506 | 0.398 | Y        | Y        | FAAR, FMF, FTHI, FTKI, FTLS, FTQI, LALT |
| 15   | John W. Gambla       | 7     | 7           | 2021-03-15 | 2026-03-13 | 0.690  | 0.506 | 0.398 | Y        | Y        | FAAR, FMF, FTHI, FTKI, FTLS, FTQI, LALT |
| 16   | Patrick Dwyer        | 6     | 6           | 2021-03-15 | 2026-03-13 | 0.685  | 0.506 | 0.398 | Y        | Y        | DBAW, DBEF, DBEM, DBEU, DBEZ, DBJP      |
| 17   | Shlomo Bassous       | 6     | 6           | 2021-03-15 | 2026-03-13 | 0.685  | 0.506 | 0.398 | Y        | Y        | DBAW, DBEF, DBEM, DBEU, DBEZ, DBJP      |
| 18   | Raffaele Zingone     | 2     | 2           | 2021-03-15 | 2026-03-13 | 0.667  | 0.506 | 0.398 | Y        | Y        | HELO, JEPI                              |
| 19   | Hamilton Reiner      | 3     | 3           | 2021-03-15 | 2026-03-13 | 0.662  | 0.506 | 0.398 | Y        | Y        | HELO, JEPI, JEPQ                        |
| 20   | Eric Becker          | 2     | 2           | 2021-03-15 | 2026-03-13 | 0.529  | 0.506 | 0.398 | Y        | Y        | ARB, EVNT                               |
| 21   | John S. Orrico       | 2     | 2           | 2021-03-15 | 2026-03-13 | 0.529  | 0.506 | 0.398 | Y        | Y        | ARB, EVNT                               |
| 22   | Charles Collins      | 1     | 1           | 2021-03-15 | 2026-03-13 | 0.526  | 0.506 | 0.398 | Y        | Y        | MRSK                                    |
| 23   | Jason A. Graffius    | 1     | 1           | 2021-03-15 | 2026-03-13 | 0.526  | 0.506 | 0.398 | Y        | Y        | MRSK                                    |
| 24   | Randall D. Schroeder | 1     | 1           | 2021-03-15 | 2026-03-13 | 0.526  | 0.506 | 0.398 | Y        | Y        | MRSK                                    |
| 25   | James H Stavena      | 3     | 3           | 2021-03-15 | 2026-03-13 | 0.525  | 0.506 | 0.398 | Y        | Y        | GTR, WTMF, WTPI                         |
| 26   | Andrew Beer          | 1     | 1           | 2021-03-15 | 2026-03-13 | 0.425  | 0.506 | 0.398 | N        | Y        | DBMF                                    |
| 27   | Mathias Mamou-Mani   | 1     | 1           | 2021-03-15 | 2026-03-13 | 0.425  | 0.506 | 0.398 | N        | Y        | DBMF                                    |
| 28   | Michael J. Venuto    | 3     | 3           | 2021-03-15 | 2026-03-13 | 0.420  | 0.506 | 0.398 | N        | Y        | LBAY, NDIV, RPAR                        |
| 29   | Michael J. Winter    | 1     | 1           | 2021-03-15 | 2026-03-13 | 0.420  | 0.506 | 0.398 | N        | Y        | LBAY                                    |

## Notes
- This is a best-effort public-data build; not every ETF had parsable people/tenure metadata.
- Manager timeline bars are used as tenure windows where available; missing end dates are treated as Present.
- Stitched person return uses equal-weight blend across concurrent assignments; it is a proxy, not AUM-weighted.