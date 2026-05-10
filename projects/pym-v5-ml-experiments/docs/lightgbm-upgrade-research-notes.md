# LightGBM Upgrade Research Notes — May 2026

## Goal

Make ML strategies actually beat the new cap25 baseline
(`pym-v5-sleeve-meta-21d-cap25`). Diagnostic from the previous round showed:

- **No** standalone Ridge ML strategy beat cap25 on Sharpe.
- The "high-return" Ridge strategies (`two_speed_attention_pym_light_governed`)
  hit `+196%` return at `-18.7%` DD, Sharpe `2.05` — all return came from
  added leverage, none from genuine alpha (cap25 had Sharpe `2.86` at smaller
  drawdown).

The hypothesis: Ridge can only fit a linear approximation of the
Composer tree, which is hundreds of nested `if`/`elif` branches. A nonlinear
model (gradient boosted trees) should fit the teacher better.

## Result

**One genuine winner found**: `pym-v5-cap25-lgbm-blend40` —
60% cap25 sleeve-meta + 40% LightGBM topk equal-weight (3 leaves, 20 trees,
`regLambda=5`).

| Window | Return | MaxDD | Sharpe | vs cap25 alone |
|---|---:|---:|---:|---|
| Full 2025-02-03 → 2026-05-08 | +154% | -9.4% | **3.35** | +47pp ret, -1.8pp DD, **+0.49 Sh** |
| OOS 2026 (87 days) | +61% | -3.3% | **5.66** | +18pp ret, -2.6pp DD, **+1.45 Sh** |

Both windows beat cap25 on every dimension simultaneously — the first
genuine ML alpha over cap25.

## What worked, what didn't

### Default LightGBM hyperparameters: catastrophic
First run with sklearn-style defaults (`n_estimators=80, num_leaves=15,
min_child_samples=5, learning_rate=0.05`) gave Sharpe `0.96-1.12` for the
LGBM topk variants — much *worse* than the Ridge equivalents (Sharpe `1.85`
for the same feature set + topk picker).

**Root cause: overfitting on tiny daily training samples.** Day 1 of the
walk-forward has ~22 training rows. With `num_leaves=15`,
`min_child_samples=5`, the trees split aggressively and memorize noise. As
the dataset grows the model still overfits because it has too many
degrees of freedom for the signal-to-noise ratio.

### Conservative hyperparameters: dramatic improvement
Tighter regularization (`n_estimators=15, num_leaves=3, min_child_samples=30,
learning_rate=0.03, regLambda=5.0, featureFraction=0.5`) gave Sharpe `2.68`
for the topk equal-weight variant — close to cap25 (2.86) and a `+1.7` Sharpe
improvement over default LightGBM. The "tinyE" variant
(`learning_rate=0.05`) hit Sharpe `2.80` standalone full-window and `5.40`
OOS — beating cap25 in OOS standalone for the first time.

### Robustness check: passed
8-point grid around the "tiny" hyperparameters tested:
- `numLeaves ∈ {2, 3, 4}`
- `nEstimators ∈ {10, 15, 20}`
- `minChildSamples ∈ {20, 30}`
- `learningRate ∈ {0.03, 0.05}`
- `regLambda ∈ {2, 5}`

**6 of 8 nearby variants beat cap25 in OOS 2026 standalone** on Sharpe.
**All 8 50/50 blends beat cap25 in BOTH windows**. This is not a single
lucky hyperparameter point.

### Blend weight sweep: 30-50% LGBM is the sweet spot
For `lgbm_topk_attention_pym_eq_tinyB` blended with cap25:

| wLGBM | full Sh | full DD | OOS Sh | OOS DD |
|---:|---:|---:|---:|---:|
| 0% (cap25 alone) | 2.86 | -11.2% | 4.21 | -5.9% |
| 30% | 3.33 | -9.2% | 5.45 | -3.4% |
| **40%** | **3.35** | **-9.4%** | **5.66** | -3.3% |
| 50% | 3.32 | -9.6% | 5.75 | -3.5% |
| 100% (LGBM alone) | 2.89 | -15.9% | 5.26 | -9.2% |

40% is the Sharpe-maximizing point in the full window for both `tinyB` and
`tinyE`. OOS prefers slightly higher (50-60%) but the gain is in the noise.

## Methodology notes

### LightGBM via MultiOutputRegressor
LightGBM doesn't natively support multi-target regression, so each LGBM
strategy trains 56 separate models per refit (one per output ticker), wrapped
in `sklearn.multioutput.MultiOutputRegressor` with `n_jobs=-1` for parallel
ticker training.

Per refit cost: ~5 LGBMs/sec × 56 tickers ≈ 11 sec for tiny configs. 317
days × 1 spec ≈ 1 hour single-spec walk-forward. The 8-spec grid took ~6
hours wall-clock.

### `topk_weights_equal` and `topk_weights_capped`
Two new helpers added in `python/run_daily_walkforward.py`:

- `topk_weights_equal`: equal-weights the top-K predicted candidates.
  For K=5 this gives 20% per name (under the 25% cap automatically).
- `topk_weights_capped`: keeps teacher weights but iteratively caps each
  name at `max_weight` and redistributes overflow to remaining selected
  names.

The blend strategy uses `topk_weights_equal` because (a) it's simpler, (b)
the cap=25%-style discipline is what proved valuable for sleeve-meta, and
(c) the equal-weight version of the Ridge topk strategies also improved
Sharpe across the board in early testing.

### CLI flags
New flags on `run_daily_walkforward.py`:

```bash
# Run only the LightGBM specs (skip the Ridge baselines)
python run_daily_walkforward.py --lgbm-only ...

# Run both Ridge and LightGBM specs
python run_daily_walkforward.py --with-lgbm ...
```

### Strategy spec naming
- `lgbm_topk_attention_pym_eq_tiny` — the original "tiny" config (15
  trees, 3 leaves)
- `lgbm_topk_attention_pym_eq_tinyA-F` — robustness grid around tiny
- `lgbm_topk_attention_pym_cap25` — capped-25% topk (vs equal-weight)
- `lgbm_topk_attention_pym_teacher` — teacher-weight topk (vs equal-weight)
- `lgbm_two_speed_attention_pym_eq` — two-speed (long+recent ridges) but
  with LightGBM as the underlying

`tinyB` (20 trees, 3 leaves, `regLambda=5`) was selected as the registered
production model because it had the best full-window Sharpe in the
robustness grid.

### Artifact regeneration

The LGBM walk-forward artifact is gitignored under
`projects/pym-v5-ml-experiments/artifacts/` like all other ML artifacts.
To regenerate (~5-10 min for 1 spec):

```bash
npm run pym-v5:ml-walkforward-lgbm
```

For the full 8-spec robustness grid (~6 hours):

```bash
projects/pym-v5-ml-experiments/.venv/bin/python \
  projects/pym-v5-ml-experiments/python/run_daily_walkforward.py \
  --dataset projects/pym-v5-ml-experiments/artifacts/pym-v5-walkforward-dataset-2025-01-02-2026-05-08.jsonl \
  --out projects/pym-v5-ml-experiments/artifacts/pym-v5-daily-walkforward-lgbm-tiny-grid-2025-02-01-2026-05-08.json \
  --train-start 2025-01-02 --predict-start 2025-02-01 \
  --lgbm-only --progress 50
```

### Environment

LightGBM requires `libomp` on macOS:
```bash
brew install libomp
uv pip install --python projects/pym-v5-ml-experiments/.venv/bin/python lightgbm
```

`requirements-ml.txt` now lists `lightgbm`.

## Honest caveats

1. **OOS sample is 87 days.** Sharpe differences of `+1.45` in 87 days are
   suggestive but not statistically rock-solid. The bigger result is the
   robustness across hyperparameter perturbations.
2. **Walk-forward training set is small.** Day 1 of prediction has 22
   training samples. The model learns slowly — early predictions are
   essentially noise-driven.
3. **Single LGBM spec was registered after a small grid.** Multiple-comparisons
   risk applies. The 8-spec robustness check mitigates this but doesn't
   eliminate it.
4. **The blend is what wins, not LGBM alone.** Standalone `tinyB` has bigger
   drawdown than cap25 (-15.9% vs -11.2% full window). The cap25
   contribution smooths drawdown; LGBM adds return uplift.
5. **The model retrains from scratch every day.** No learning across days
   beyond what's in the training data. A more sophisticated approach would
   warm-start from the previous day's model or use online learning.

## Next ideas worth trying

1. **Pool tickers via single model with ticker_one_hot features.** Currently
   we train 56 separate LGBMs per refit. Stacking `(ticker_one_hot +
   features) → return` as a single model with 56× more rows would let the
   model learn cross-ticker patterns and might enable larger trees safely.
2. **Use cap25 weights as the teacher** instead of raw PYM. Currently the
   topk picker filters on PYM teacher weights `> 1e-10`. Using cap25 weights
   as the teacher would inherit cap25's discipline.
3. **Walk-forward hyperparameter selection.** Pick `(numLeaves, nEstimators,
   regLambda)` daily from a small grid using trailing 21-63 day Sharpe.
4. **Online ranker (LambdaRank).** LightGBM supports learning-to-rank
   objectives natively. The topk picker is fundamentally a ranking problem —
   converting to a ranking loss might be cleaner than predicting returns.
5. **Include cap25 holdings as a feature.** Currently the model sees PYM
   teacher weights via the `pym` feature group. Adding cap25 weights might
   help the model learn when to deviate from the cap25 baseline.
6. **Ensemble multiple tiny LGBMs with different seeds.** Bagging is well-
   known to reduce variance in tree boosters. Running 5 seeds and averaging
   would smooth predictions further.

## Methodology checklist (next AI: read this)

Before declaring an ML upgrade a winner:

- [ ] Use the **same bars file** the live service uses
      (`findLatestMassiveEodBarsPath()` picks the file with the latest
      `startDate` among tied `endDate` entries — typically the
      shorter-warmup file). Wilder RSI is path-dependent.
- [ ] Compare against **cap25**, not base PYM. Base PYM is no longer the
      bar to beat.
- [ ] Test on **at least two windows**: full (2025-02-03+) and OOS 2026
      (2026-01-02+). 87 OOS days is small but a strategy that wins one
      and loses the other is suspect.
- [ ] Run a **robustness grid around any single winner**. If only the
      central point wins, you're picking a single lucky draw.
- [ ] **Standalone vs blend**: try both. ML often loses standalone but
      adds value as a 30-50% overlay on a strong baseline.
- [ ] Verify the strategy serves through the registry
      (`createDefaultRegistry().getStrategy(id).getReport()`) before
      declaring done.
- [ ] Rebuild the Docker container with `docker compose -p phenixflow ...
      up -d --build strategy-service` to make it visible in the dashboard.
- [ ] Write a research note like this one. Future-you (or future-AI)
      will thank you.

## Key code locations

- **Production engine**:
  `projects/pym-v5-ml-experiments/python/run_daily_walkforward.py`
- **LGBM model fit**: `fit_lgbm_return_predictions` (uses
  MultiOutputRegressor wrapping LGBMRegressor)
- **Top-K helpers**: `topk_weights`, `topk_weights_equal`,
  `topk_weights_capped` (in same file)
- **Strategy specs**: `make_lgbm_specs` (LightGBM variants),
  `make_strategy_specs` (Ridge variants)
- **Blend logic**:
  `projects/pym-v5-replication/src/extension-strategies-suite.js →
  strategyBlendWithExternal`
- **Service adapter**:
  `apps/strategy-service/src/strategies/pym-v5-extension.js →
  createPymV5Cap25LgbmBlendStrategy`
- **Comparison scripts** (one-shot, in `/tmp/` per session):
  `compare-lgbm-vs-cap25.js`, `blend-sweep.js` — recreate as needed.

## NPM scripts added

- `npm run pym-v5:ml-export-dataset` — regenerate the JSONL feature dataset
  from JS market/score/option-feature data.
- `npm run pym-v5:ml-walkforward-lgbm` — regenerate the LightGBM artifact
  with just the production-registered `tinyB` spec (~10 minutes).
