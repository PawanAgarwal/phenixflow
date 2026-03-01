# v5 Swing Post-Fix Validation (Last 7 Trading Sessions)

- Generated: 2026-03-01T20:02:18.420Z
- Symbols: AAPL, MSFT, AMZN
- Total rows: 2,797,200
- Complete rows: 2,783,340
- Partial rows: 13,860
- sigScore >= 0.90: 0
- sigScore >= 0.85: 0
- Max sigScore: 0.741810

## Missing Components (all rows)
- scoreComponent:flowImbalanceNorm: 0
- scoreComponent:deltaPressureNorm: 1
- scoreComponent:underlyingTrendConfirmNorm: 0
- deltaNorm: 1

## Directional Missing (bullish/bearish only)
- Directional rows: 2,797,200
- flowImbalance missing: 0 (0.00%)
- deltaPressure missing: 1 (0.00%)
- underlyingTrend missing: 0 (0.00%)
- directional OTHER side rows: 0
- directional NULL delta rows: 1

## Top SigScore Rows
- MSFT 2026-02-26T14:23:08.057Z sig=0.741810 quality=complete sentiment=bullish side=ASK; drivers: deltaPressureNorm, flowImbalanceNorm, underlyingTrendConfirmNorm, volOiNorm, liquidityQualityNorm
- MSFT 2026-02-26T14:21:41.214Z sig=0.734944 quality=complete sentiment=bullish side=ASK; drivers: deltaPressureNorm, flowImbalanceNorm, underlyingTrendConfirmNorm, valueShockNorm, liquidityQualityNorm
- MSFT 2026-02-26T14:18:02.142Z sig=0.724815 quality=complete sentiment=bullish side=ASK; drivers: deltaPressureNorm, flowImbalanceNorm, volOiNorm, underlyingTrendConfirmNorm, valueShockNorm

## Delta vs Baseline
- completeRowsDelta: 1,959,709
- partialRowsDelta: -1,959,709
- missingFlowImbalanceDelta: -1,074,174
- missingDeltaPressureDelta: -1,922,577
- missingUnderlyingTrendDelta: -1,161,621
- missingDeltaNormDelta: -1,495,755
