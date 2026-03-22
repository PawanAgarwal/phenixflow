# VixRegime Version 3

Version 3 is the fixed-window binary directional model.

Definition:
- `Bullish`: `SPY close(X+1) / close(X) - 1 >= 0`
- `Bearish`: `SPY close(X+1) / close(X) - 1 < 0`

Training approach:
- Train once on the same fixed window as Version 1:
  - `2025-03-19` to `2025-06-17`
- Freeze the model after training
- Apply the same model month by month after the train window

Model family:
- weighted binary logistic regression

Feature set:
- `volCore`
- `priceTrend`

Default parameters:
- `halfLifeRows = 126`
- `Bullish weight = 1.0`
- `Bearish weight = 1.5`
- `bullishThreshold = 0.5`
- `learningRate = 0.015`
- `reg = 0.0005`
- `epochs = 350`
- `bullishPosMultiplier = 1.0`

Policies evaluated:
- `spyCash`: `Bullish -> SPY`, `Bearish -> CASH`
- `spxlCash`: `Bullish -> SPXL`, `Bearish -> CASH`
- `spySpxs`: `Bullish -> SPY`, `Bearish -> SPXS`
- `spxlSpxs`: `Bullish -> SPXL`, `Bearish -> SPXS`

Primary script:
- [evaluate-monthly-fixed-v3-binary-directional.js](/Users/pawanagarwal/github/phenixflow/vixregime/scripts/evaluate-monthly-fixed-v3-binary-directional.js)

Primary report:
- [vixregime-monthly-fixed-v3-binary-directional.json](/Users/pawanagarwal/github/phenixflow/vixregime/artifacts/reports/vixregime-monthly-fixed-v3-binary-directional.json)

Current first-pass results:
- Aggregate classification accuracy: `51.85%`
- Aggregate `spyCash`: `+9.96%` vs `SPY +8.70%`
- Aggregate `spxlCash`: `+32.06%` vs `SPY +8.70%`

2026 month-by-month highlights:
- `2026-01`
  - accuracy: `50.0%`
  - `spyCash`: `-0.15%`
  - `spxlCash`: `-0.79%`
  - `SPY`: `+1.78%`
- `2026-02`
  - accuracy: `63.16%`
  - `spyCash`: `+1.05%`
  - `spxlCash`: `+2.85%`
  - `SPY`: `-1.39%`
- `2026-03`
  - accuracy: `71.43%`
  - `spyCash`: `+1.90%`
  - `spxlCash`: `+5.70%`
  - `SPY`: `-5.25%`

Operational note:
- The `SPXS` mappings can show very large returns in some months.
- Treat `spyCash` and `spxlCash` as the more credible first-pass policies.
