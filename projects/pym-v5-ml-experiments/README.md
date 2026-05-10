# PYM V5 ML Experiments

Offline Massive-only ML experiments for the PYM V5 replication. This project is intentionally separate from the live strategy service.

The first harness tests causal next-session policies using the existing Composer/PYM tree as the teacher:

- `pym_v5_base`: deterministic PYM V5 baseline.
- `imitate_*`: ridge models that learn teacher weights.
- `gate_*`: logistic risk gates that hold PYM or move to a cash-like ETF.
- `topk_*`: ridge next-return rankers that hold the top current PYM candidates.
- `*_attention_*`: a dependency-light, causal self-attention-style sequence encoder over recent returns.

The default split is:

- Training/tuning history starts at `2025-01-02`.
- Hyperparameter training ends at `2025-10-31`.
- Validation runs `2025-11-01` through `2025-12-31`.
- Out-of-sample test starts `2026-01-01`.

Run:

```bash
node projects/pym-v5-ml-experiments/scripts/run-experiments.js
```

Useful flags:

```bash
node projects/pym-v5-ml-experiments/scripts/run-experiments.js --canary
node projects/pym-v5-ml-experiments/scripts/run-experiments.js --no-options
node projects/pym-v5-ml-experiments/scripts/run-experiments.js --lookback 63 --test-start 2026-01-01
```

Generated reports are written under `projects/pym-v5-ml-experiments/artifacts/`.

## Weekly Friday Close / Weekly Return Model

This harness predicts the next weekly SPY return from features known at the current week close. It uses the last trading day of each week as the signal anchor, refits causal ridge models walk-forward, and compares them with simple trend rules plus SPY/BIL benchmarks.

For a serious run, build enough Massive adjusted EOD history first:

```bash
node projects/pym-v5-replication/scripts/build-massive-eod-daily-bars.js \
  --fetch-start 2015-01-01 \
  --end 2026-05-08
```

When multiple Massive EOD files end on the same date, the weekly harness chooses the widest history.

Research close-to-close target:

```bash
node projects/pym-v5-ml-experiments/scripts/run-weekly-return-model.js \
  --target SPY \
  --target-mode close_to_close \
  --predict-start 2025-01-01
```

Tradable next-open target:

```bash
node projects/pym-v5-ml-experiments/scripts/run-weekly-return-model.js \
  --target SPY \
  --target-mode next_open_to_week_close \
  --predict-start 2025-01-01
```

Both reports rank strategies by validation-period score first, keep 2026 test rows separate, and include the latest Friday-close signal.

## Daily Walk-Forward Retraining

Export the causal feature dataset, then run the daily retrain experiment with the project Python env:

```bash
node projects/pym-v5-ml-experiments/scripts/export-walkforward-dataset.js
projects/pym-v5-ml-experiments/.venv/bin/python \
  projects/pym-v5-ml-experiments/python/run_daily_walkforward.py \
  --dataset projects/pym-v5-ml-experiments/artifacts/pym-v5-walkforward-dataset-2025-01-02-2026-05-08.jsonl \
  --out projects/pym-v5-ml-experiments/artifacts/pym-v5-daily-walkforward-2025-02-01-2026-05-08.json
```

This starts by training only on January 2025 samples, predicts from the first trading day after February 1, 2025, and refits each ML strategy every day using only prior labeled samples.

## Python ML Environment

The project-local Python environment is intentionally ignored by git:

```bash
uv venv projects/pym-v5-ml-experiments/.venv --python 3.12
uv pip install --python projects/pym-v5-ml-experiments/.venv/bin/python -r projects/pym-v5-ml-experiments/requirements-ml.txt
```

Use it directly when adding neural/Transformer experiments:

```bash
projects/pym-v5-ml-experiments/.venv/bin/python your_script.py
```
