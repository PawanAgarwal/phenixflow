# two_speed_attention_pym_light_governed

Plain-English interpretation of the daily walk-forward strategy.

## One-Line Summary

Start with the normal PYM V5 portfolio, use a learned "similar recent market days" model to rank the current PYM holdings, keep the best five non-cash candidates, and avoid switching unless the predicted improvement is large enough to justify turnover.

## What It Is

This is a causal daily retrained model. On each signal date:

1. Train only on prior labeled days.
2. Predict next-day returns for the PYM tradable universe.
3. Rank only the tickers currently selected by the PYM V5 tree.
4. Keep the top five non-cash candidates.
5. Re-normalize using PYM's original relative weights.
6. Keep yesterday's portfolio if the predicted edge is too small.

The strategy is not a free-form optimizer. It is a PYM concentration/filter overlay.

## Simple Rule Form

At each EOD signal:

```text
candidate_set = current PYM V5 holdings excluding BIL/cash
scores = two_speed_prediction(candidate_set)
new_portfolio = top 5 candidates by score, using PYM weights normalized to 100%

if expected improvement over yesterday < 3 bps + 0.75 * estimated switching cost:
    keep yesterday's portfolio
else:
    hold new_portfolio
```

## What "Two Speed" Means

The model trains two ridge regressions every day:

- Long-memory model: trained on all prior days equally.
- Recent-memory model: trained on all prior days, but with a 63-trading-day half-life.

The final prediction blends them:

```text
prediction = (1 - recent_weight) * long_memory_prediction
             + recent_weight * recent_memory_prediction
```

The recent weight is intentionally light:

```text
recent_weight = clamp(0.08 + 0.22 * stress_score, min 0.10, max 0.90)
```

In practice, because the stress score is bounded 0 to 1, this means roughly:

- Calm tape: about 10% recent model, 90% long-memory model.
- Stressed tape: up to about 30% recent model, 70% long-memory model.

That is why this version is called `light`.

## What "Attention" Means Here

This is not a Transformer. It is a dependency-light analog-day encoder.

For each day, it looks at the latest cross-asset return vector across core tickers such as:

- SPY, QQQ, IWM
- TLT, IEF, TMF, TMV
- GLD, UGL, GLL
- UUP, UDN, USO
- VIXY, UVXY, SVIX, SVXY
- sector/risk ETFs and levered/inverse ETFs

Then it searches recent history for days whose cross-asset return pattern looked similar.

It does this at three temperatures:

- `0.35`: sharp nearest-neighbor style analogs.
- `0.75`: medium analog blend.
- `1.5`: broader, smoother analog blend.

For each temperature and ticker, the feature set includes:

- Similar-day weighted return.
- Similar-day weighted absolute return.
- A max-weight/concentration feature showing whether the analog match is strong or diffuse.

In simple terms:

> "When today looks like prior days X, Y, and Z across equities, rates, dollar, oil, gold, and volatility, which PYM holdings tended to work next?"

## What Data It Uses

Direct prediction features:

- `attention`: 232 analog-day features.
- `pym`: 57 current PYM weight features.

The return model does not directly use the broad option feature block, liquidity block, or microstructure block.

However, the two-speed blend uses a separate stress score. That stress score can use:

- SPY 21-day volatility.
- SPY/QQQ drawdown.
- VIXY and UVXY 5-day returns.
- SPY/SPX option put-call pressure.
- SPY/QQQ range/liquidity stress.

So options affect how much the model trusts recent history, not which ticker receives a direct option-flow score.

## What "Governed" Means

The strategy has a turnover governor:

```text
required_edge = 3 bps + 0.75 * estimated switching cost
```

If the new top-five portfolio does not beat the previous portfolio by that predicted edge, the strategy holds the previous portfolio.

This is why the strategy can sometimes keep holdings that are not exactly today's fresh top-five list.

## Why It Can Beat PYM

PYM V5 already chooses a broad, rule-based portfolio. This model keeps PYM's directional structure but concentrates it:

- PYM says which area of the tree is active.
- The ML overlay asks which of those active holdings has the best analog-day next-return profile.
- The top-five concentration raises upside when PYM has many small holdings.
- The turnover governor avoids some noisy day-to-day reshuffling.

In Composer-like language:

> Use PYM as the universe and regime filter, then apply an analog-day momentum/risk model to keep only the strongest five active PYM legs.

## Empirical Behavior In This Run

Aligned test window: `2025-02-04` through `2026-05-08`, net of 2 bps cost.

- Total return: `196.17%`
- Sharpe: `2.05`
- Max drawdown: `-18.65%`
- Average daily turnover: `122.56%`

Average weights over the run were concentrated in:

- TQQQ: `16.45%`
- UGL: `16.37%`
- EDZ: `12.15%`
- EDC: `6.28%`
- SVXY: `3.93%`
- QQQ: `3.81%`
- SOXL: `2.93%`
- CURE: `2.89%`
- TECL: `2.75%`
- SQQQ: `2.62%`

Final 2026-05-08 held-through-close portfolio:

- TMF: `59.71%`
- PSQ: `15.92%`
- TECL: `11.14%`
- SOXL: `10.38%`
- IEF: `2.84%`

## Feature Importance Snapshot

Using the final available training window, standardized coefficient magnitude suggests the model is mostly driven by analog-day features, with PYM weights as a strong anchor.

Long-memory model approximate importance:

- Analog absolute-return context: `39.63%`
- Analog signed-return context: `39.09%`
- PYM current weights: `17.30%`
- Analog match concentration: `3.98%`

Recent 63-day half-life model approximate importance:

- Analog signed-return context: `45.46%`
- Analog absolute-return context: `34.48%`
- PYM current weights: `18.27%`
- Analog match concentration: `1.80%`

Readable dominant inputs include financials, defensive sectors, small-caps, oil/gold/rates/volatility proxies, and whether PYM itself is pointing at levered/inverse tech, gold, or emerging-market risk.

## Current Caveats

- It is still ML, not a fixed Composer tree.
- Coefficients are retrained daily, so the exact rule surface changes through time.
- It has high turnover and uses levered/inverse ETFs.
- It depends on PYM as a teacher/universe, so any PYM replication mismatch propagates into it.
- It should be distilled into a shallow rule list or decision tree before treating it as a human-readable production strategy.

## Distillation Target

A simple approximation to test next:

1. Start with current PYM V5 holdings.
2. Score candidates by:
   - PYM current weight,
   - similar-day momentum of the candidate's asset family,
   - rate/gold/volatility analog signals,
   - stress-adjusted recent momentum.
3. Keep top five.
4. Preserve PYM relative weights.
5. Add a minimum edge/turnover threshold before switching.
