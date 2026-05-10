# Gap-Feature LGBM Experiment — Negative Result

## TL;DR

**Adding overnight-gap features to the LGBM model HURT performance on every
spec variant and every test split. Do not ship a gap-augmented LGBM
strategy.** The original SPY gap-down overlay's apparent edge was
sample-specific noise; it did not survive a proper walk-forward feature
test. This document exists so future researchers don't re-test the same
hypothesis.

The gap-feature builder code is preserved as reusable infrastructure for
testing other feature groups, not because gap features are useful.

## Context

A separate study (April 2026) of SPY opening-gap fading suggested a small
conditional sleeve might help on gap-down days:
- 2026-only: gap-down 30% sleeve, ≥100bps, full-fill → +0.96pp 2026 YTD,
  +0.13 Sharpe vs base PYM.

The conservative interpretation in that study was already that this looked
mostly regime-fit. The proposed action was: don't ship as a standalone
overlay, but test if the gap signal carries information for an ML model.

## The clean test

Hypothesis: do overnight-gap features add information the LGBM model
wouldn't already get from the existing `attention` (close-to-close return
sequences) and `pym` (teacher weights) feature groups?

Method:
1. Add a new `gap` feature group containing 30 features over five gap-prone
   tickers (SPY/QQQ/IWM/TQQQ/SOXL):
   - `<ticker>_gap_bps` — yesterday-close → today-open return
   - `<ticker>_gap_z21` — 21-day z-score of the gap
   - `<ticker>_intraday_pct` — today-open → today-close return (gap-fill)
   - `<ticker>_close_loc` — where today closed in today's range
   - `<ticker>_gap_abs_avg5` — 5-day average |gap|, vol regime indicator
   - `<ticker>_gap_cont5` — 5-day continuation count (sign(gap)==sign(intra))

2. Re-export the walk-forward dataset with the new feature group.

3. Train four LGBM specs with `tinyB`-style hyperparameters
   (20 trees, 3 leaves, regLambda=5):
   - **baseline**: `[attention, pym]` — the production tinyB
   - `[attention, pym, gap]` tinyB — same hyperparams, gap added
   - `[attention, pym, gap]` tinyE — slightly higher learning rate
   - `[pym, gap]` — gap features without attention

4. Compare standalone Sharpe and as the 40% leg in `cap25-blend40` over
   both the full window 2025-02-03 → 2026-05-07 and the OOS 2026 only
   (87 days).

## Result (2 bps cost, full window)

### Standalone

| Variant | Return | Max DD | Sharpe | Δ Sharpe |
|---|---:|---:|---:|---:|
| **baseline tinyB** | **231.5%** | -15.9% | **2.893** | — |
| + gap features (tinyB) | 219.1% | -17.1% | 2.815 | -0.077 |
| + gap features (tinyE) | 162.7% | -19.4% | 2.431 | -0.462 |
| pym + gap only (no attention) | 57.2% | -22.8% | 1.190 | -1.703 |

### As 40% leg in cap25-blend40

| Variant | Return | Max DD | Sharpe | Δ Sharpe |
|---|---:|---:|---:|---:|
| cap25 alone | 106.9% | -11.2% | 2.860 | — |
| cap25 + 40% baseline LGBM | 153.6% | -9.4% | **3.346** | +0.486 |
| cap25 + 40% gap-LGBM (tinyB) | 149.5% | -9.4% | 3.270 | -0.077 vs blend |
| cap25 + 40% gap-LGBM (tinyE) | 130.9% | -9.4% | 3.080 | -0.267 vs blend |

## OOS 2026 only (87 days)

The OOS test is more important — and the picture gets WORSE there:

| Variant | OOS Return | OOS Sharpe | Δ OOS Sharpe |
|---|---:|---:|---:|
| baseline tinyB standalone | 90.5% | **5.298** | — |
| + gap features (tinyB) | 75.7% | 4.350 | **-0.948** |
| + gap features (tinyE) | 80.3% | 4.843 | -0.455 |
| **cap25 + 40% baseline LGBM** | 60.1% | **5.542** | — |
| cap25 + 40% gap-LGBM (tinyB) | 55.0% | 4.886 | **-0.656** |
| cap25 + 40% gap-LGBM (tinyE) | 56.6% | 5.265 | -0.277 |

## Why this is a clean negative result

1. **The OOS Sharpe drop is LARGER than the in-sample drop.** That's not
   overfit; it's the gap features actively introducing harmful noise. A
   well-regularized LGBM with truly uninformative features would be
   roughly agnostic at worst (the model just doesn't split on them).
   Here the gap features compete with useful signal during training.
2. **Every variant lost.** Not 1 of 4 — all 3 gap variants worse than
   baseline, on standalone AND in-blend, on full window AND OOS.
3. **The pym-only-with-gap collapsed** to Sharpe 1.19, confirming gap
   data alone has no edge once the model has to infer everything from it.
4. **The original gap-overlay study's 2026 +0.96pp was variance**, not
   signal. With ~5-10 large gap-down days in 87 trading days, that
   improvement is well within sampling noise of any defensive overlay.

## What was kept (reusable infrastructure)

The negative result doesn't invalidate the testing framework. The
gap-feature builder is generic infrastructure that can be reused for any
"test a new feature group" workflow:

- `appendGapFeatures` and the `gap` group in
  [projects/pym-v5-ml-experiments/src/experiment.js](../src/experiment.js).
- `gap` group exposed by
  [projects/pym-v5-ml-experiments/scripts/export-walkforward-dataset.js](../scripts/export-walkforward-dataset.js).
- Three LGBM specs in
  [projects/pym-v5-ml-experiments/python/run_daily_walkforward.py](../python/run_daily_walkforward.py)
  (`lgbm_topk_attention_pym_gap_eq_tinyB`, `tinyE`,
  `lgbm_topk_pym_gap_only_eq_tiny`). These are NOT in the production
  registry; they are kept so a future researcher can verify the result
  or test variants.

To replicate the test:

```bash
# Re-export dataset with gap features
node projects/pym-v5-ml-experiments/scripts/export-walkforward-dataset.js \
  --start 2025-01-02 --no-options \
  --out projects/pym-v5-ml-experiments/artifacts/pym-v5-walkforward-dataset-with-gap-2025-01-02-2026-05-08.jsonl

# Train the A/B (~25 min for 4 specs)
projects/pym-v5-ml-experiments/.venv/bin/python \
  projects/pym-v5-ml-experiments/python/run_daily_walkforward.py \
  --dataset projects/pym-v5-ml-experiments/artifacts/pym-v5-walkforward-dataset-with-gap-2025-01-02-2026-05-08.jsonl \
  --out projects/pym-v5-ml-experiments/artifacts/pym-v5-daily-walkforward-lgbm-gap-ab-2025-02-01-2026-05-08.json \
  --train-start 2025-01-02 --predict-start 2025-02-01 \
  --lgbm-only \
  --strategies lgbm_topk_attention_pym_eq_tinyB,lgbm_topk_attention_pym_gap_eq_tinyB,lgbm_topk_attention_pym_gap_eq_tinyE,lgbm_topk_pym_gap_only_eq_tiny
```

## What was NOT shipped

- No `pym-v5-cap25-lgbm-gap-blend40` strategy
- No `pym-v5-ml-gap-attention` strategy
- No SPY gap-fade overlay strategy
- The gap-augmented LGBM artifact lives under `artifacts/` (gitignored)

## Implications for future feature work

The pattern in this experiment generalizes. Before adding any new feature
group to the production model:

1. **Add the feature builder under a new group name** (don't pollute
   existing groups — keeps A/B comparison clean).
2. **Re-export the dataset** with both old and new groups available.
3. **Run 3-4 LGBM specs**: baseline, +new-group, +new-group with one
   hyperparameter perturbation, new-group-only-no-baseline.
4. **Compare on full window AND OOS**.
5. **Compare standalone AND as the LGBM leg in the production blend**
   (in this case, cap25 + 40% LGBM).
6. **If new group hurts on either test split, kill it.** Don't try
   exotic interactions or feature engineering rescue moves — the model
   already has 232 attention + 57 pym features and another 30 noisy ones
   are not what it needs.

This 30-minute compute experiment would have saved a lot of time on the
original gap overlay had it been the first thing tested.

## Related work

- The full gap-overlay study evaluation lives in earlier session notes
  (skeptical evaluation produced before this A/B test).
- The production LGBM upgrade research is at
  [lightgbm-upgrade-research-notes.md](./lightgbm-upgrade-research-notes.md).
- The options-stress overlay (which actually does provide diversification)
  is at
  [options-stress-overlay-research-notes.md](./options-stress-overlay-research-notes.md).
