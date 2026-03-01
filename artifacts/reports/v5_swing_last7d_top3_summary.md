# v5 Swing SigScore Validation (Last 7 Trading Sessions)

## Scope
- Window: 2026-02-19, 2026-02-20, 2026-02-23, 2026-02-24, 2026-02-25, 2026-02-26, 2026-02-27
- Symbols tested end-to-end: AAPL, MSFT, AMZN
- Scoring model forced for enrichment: `v5_swing`
- Theta supplemental concurrency: `18`

## Data/coverage check
- Minute-derived coverage per symbol/day was stable:
  - AAPL avg minute rows/day: 390.14 (min 390, max 391)
  - MSFT avg minute rows/day: 390.14 (min 390, max 391)
  - AMZN avg minute rows/day: 390.00 (min 390, max 390)

## SigScore outcomes
- Total v5-scored rows in window (AAPL+MSFT+AMZN): 2,797,200
- `sigScore >= 0.90`: 0 rows
- `sigScore >= 0.85`: 0 rows
- Overall max `sigScore`: 0.790903

Per symbol:
- AAPL: 927,175 rows, max 0.768749, high>=0.9: 0, >=0.85: 0
- MSFT: 970,728 rows, max 0.779168, high>=0.9: 0, >=0.85: 0
- AMZN: 899,297 rows, max 0.790903, high>=0.9: 0, >=0.85: 0

## Top near-high events (not significant threshold)
1. AMZN 2026-02-27T15:57:06.930Z, sigScore 0.790903 (partial)
   - Top drivers: `volOiNorm`, `repeatNorm`, `otmNorm`, `valueShockNorm`, `ivSkewSurfaceNorm`
   - Sentiment/execution: neutral / OTHER
2. MSFT 2026-02-23T12:51:24.061Z, sigScore 0.779168 (partial)
   - Top drivers: `volOiNorm`, `repeatNorm`, `valueShockNorm`, `cpOiPressureNorm`, `ivSkewSurfaceNorm`
   - Sentiment/execution: neutral / OTHER
3. AAPL 2026-02-26T14:30:49.056Z, sigScore 0.768749 (partial)
   - Top drivers: `volOiNorm`, `repeatNorm`, `underlyingTrendConfirmNorm`, `valueShockNorm`, `liquidityQualityNorm`
   - Sentiment/execution: bullish / ASK

## Validity assessment
- Quality mix (all rows):
  - `complete`: 823,631
  - `partial`: 1,973,569
- Dominant missing metrics:
  - `scoreComponent:deltaPressureNorm`: 1,922,578 rows
  - `deltaNorm`: 1,495,756 rows
  - `scoreComponent:underlyingTrendConfirmNorm`: 1,161,621 rows
  - `scoreComponent:flowImbalanceNorm`: 1,074,174 rows
- Important interpretation:
  - Highest scores were mostly partial and often neutral sentiment.
  - Many top rows were driven by size/ratio-style components (`volOiNorm`, `repeatNorm`) while directional components were unavailable.
  - This reduces confidence for directional action despite elevated local scores.

## Conclusion
- No new `sigScore` values reached a significant/action threshold (>=0.90) for the tested 7-session window.
- Based on current quality profile and missing directional components, no score in this run is strong enough to recommend action.
