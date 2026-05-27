# Put Selling and Wheel Backtest

Generated: 2026-05-27T14:47:35.705Z
Window: 2025-01-21 through 2025-02-28
Indicator warmup: 2025-01-02 through 2025-01-21
Provider: Massive local data
Universe size: 521
Initial capital: $100,000

## Strategy Framing

- Cash-secured put variants sell OTM puts and reserve full assignment notional.
- Wheel variants sell cash-secured puts, hold assigned shares, then sell covered calls against those shares until called away.
- This implementation uses expiration assignment only and daily mark-to-market equity from Massive 1-minute option marks.
- The option minute aggregate close is not a bid/ask quote, so entry proceeds are haircut before commissions.

## Execution Assumptions

- Entry window: 15:59 ET for 1 minutes.
- Minimum option premium: $0.1.
- Premium haircut: 5.0%.
- Commission: $0.65 per contract.
- Max position: 30.0% of equity per symbol.
- Max put collateral utilization: 100.0% of equity.

## Coverage

- Attempted open days: 28
- Processed days: 28
- Attempted missing files: 0
- Provider-sparse days: 0

## Benchmarks

| Symbol | Return | Max DD | Observations |
| --- | ---: | ---: | ---: |
| SPY | -1.47% | -4.55% | 28 |
| QQQ | -3.13% | -7.27% | 28 |

## Results

| Strategy | Return | CAGR | Vol | Sharpe | Calmar | Max DD | STO | Assignments | Premium/yr | Ending open |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| wheel_cs_static_otm_itm_rsi21 | -0.19% | -1.68% | 0.95% | -1.77 | -6.95 | -0.24% | 5 | 0 | 5.14% | 5 |
| wheel_cs_vix_overlay_rsi21 | -0.35% | -3.13% | 1.29% | -2.43 | -7.57 | -0.41% | 5 | 0 | 6.25% | 5 |

## Tail Diagnostics

### wheel_cs_static_otm_itm_rsi21

- Skew: -1.236
- Excess kurtosis: 4.426
- CVaR(5% daily): -0.18%
- Worst month: 2025-02 -0.19%

### wheel_cs_vix_overlay_rsi21

- Skew: -1.064
- Excess kurtosis: 3.659
- CVaR(5% daily): -0.25%
- Worst month: 2025-02 -0.35%

## Monthly Returns

### wheel_cs_static_otm_itm_rsi21

| Month | Return | Max DD | Days |
| --- | ---: | ---: | ---: |
| 2025-01 | 0.00% | 0.00% | 9 |
| 2025-02 | -0.19% | -0.24% | 19 |

### wheel_cs_vix_overlay_rsi21

| Month | Return | Max DD | Days |
| --- | ---: | ---: | ---: |
| 2025-01 | 0.00% | 0.00% | 9 |
| 2025-02 | -0.35% | -0.41% | 19 |

## Research Sources

- Options Industry Council: cash-secured put strategy description and collateral framing: https://www.optionseducation.org/strategies/all-strategies/cash-secured-put
- Cboe Options Institute via Fidelity: cash-secured puts are generally for investors willing to buy the underlying at the strike: https://www.fidelity.com/learning-center/investment-products/options/options-strategy-guide/shortput-cashsecured
- FINRA assignment overview: short option sellers can be assigned and must fulfill the contract: https://www.finra.org/investors/insights/trading-options-understanding-assignment
- Schwab wheel overview: wheel cycles through cash-secured puts, assigned stock, and covered calls: https://www.schwab.com/learn/story/three-things-to-know-about-wheel-strategy

## Limitations

- Option entries use Massive OPRA 1-minute aggregate close inside the entry window, with a premium haircut applied to short-option proceeds.
- Open short options are marked daily from the last available 1-minute option mark; missing marks fall back to max(intrinsic value, prior mark).
- Implied volatility and delta filters are Black-Scholes estimates from Massive minute aggregate option prices, not provider Greeks.
- Trend and realized-volatility filters use prior daily closes only.
- Entries may expire after the report end date; those positions remain open and are marked through the final processed day.
- Historical Massive CSV flat files are preferred; live Massive parquet is used only when the historical file is not available for a requested day.
- VIX-overlay variants use the prior trading day VIX close to keep intraday option entries causal.
- Assignment is modeled at expiration only; early assignment, dividends, borrow constraints, taxes, and margin interest are not modeled.
- Idle cash interest is not accrued in this backtester; positive T-bill interest would improve cash-heavy variants modestly but was not needed to reject this run.
- Cash-put variants liquidate assigned shares at expiration close; wheel variants keep assigned shares and sell covered calls when eligible.
- The universe is a liquid local proxy unless a complete holdings file is explicitly supplied.
- This is research infrastructure and historical simulation, not investment advice or production approval.

