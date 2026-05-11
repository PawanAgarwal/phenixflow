#!/usr/bin/env python3
"""
LightGBM meta-classifier on the per-minute features-1m dataset.

Goal: train a model to predict next-5m and next-15m SPY returns from the full
flow + greeks + cumulative dealer-flow + OCC overlay feature set, and evaluate
out-of-sample whether it produces a tradeable signal at realistic cost.

Run:
  /Users/pawanagarwal/github/phenixflow/projects/pym-v5-ml-experiments/.venv/bin/python \
    projects/spy-intraday-prediction/python/run_option_flow_lgbm.py \
    --root SPY --horizon 5

Inputs read from projects/spy-intraday-prediction/runtime/features-1m/{ROOT}/date=*/
"""

import argparse
import gzip
import json
import math
import os
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
from lightgbm import LGBMRegressor, LGBMClassifier


PROJECT_ROOT = Path(__file__).resolve().parents[1]
FEATURES_ROOT = PROJECT_ROOT / 'runtime' / 'features-1m'
ARTIFACTS_DIR = PROJECT_ROOT / 'artifacts'


# Drop columns that are pure metadata or constants
META_COLS = {
    'minute_ms', 'date_et', 'minute_of_day_et',
    'spy_open', 'spy_close', 'spy_high', 'spy_low',
    'session_open', 'first_close',
}
# Keep these as features:
LABEL_COLS_TO_DROP = set()  # We compute labels below


def load_jsonl_gz(path: Path):
    with gzip.open(path, 'rt') as f:
        for line in f:
            if not line:
                continue
            yield json.loads(line)


def find_feature_files(root: str, start: str, end: str):
    base = FEATURES_ROOT / root
    out = []
    if not base.exists():
        return out
    for d in sorted(base.iterdir()):
        if not d.name.startswith('date='):
            continue
        day = d.name[5:]
        if day < start or day > end:
            continue
        f = d / f'{day}.jsonl.gz'
        if f.exists():
            out.append((day, f))
    return out


def build_dataset(root: str, start: str, end: str, horizon_minutes: int, drop_cumulative: bool = True):
    """Build (X, y, dates, minute_of_day) arrays. y = forward log return over horizon minutes.

    drop_cumulative: when True (default), drop cum_* features. They monotonically grow within a day
    and let LGBM overfit to time-of-day patterns; for a real signal we want per-minute flow rates.
    """
    files = find_feature_files(root, start, end)
    rows = []
    for day, fp in files:
        day_rows = list(load_jsonl_gz(fp))
        # Compute forward return for each row using spy_close
        closes = [r['spy_close'] for r in day_rows]
        for i, r in enumerate(day_rows):
            target_idx = i + horizon_minutes
            if target_idx >= len(day_rows):
                continue
            fwd = closes[target_idx] / closes[i] - 1
            if not math.isfinite(fwd):
                continue
            r['__fwd_return'] = fwd
            rows.append(r)
    if not rows:
        return None, None, None, None, []
    # Determine feature columns (all numeric, non-meta)
    sample = rows[0]
    feature_cols = []
    for k, v in sample.items():
        if k in META_COLS or k == '__fwd_return':
            continue
        if drop_cumulative and k.startswith('cum_'):
            continue
        if isinstance(v, (int, float)) and v is not None:
            feature_cols.append(k)
        elif v is None:
            feature_cols.append(k)
    feature_cols = sorted(set(feature_cols))
    X = np.zeros((len(rows), len(feature_cols)), dtype=np.float64)
    y = np.zeros(len(rows), dtype=np.float64)
    dates = []
    minute_of_day = np.zeros(len(rows), dtype=np.int32)
    for i, r in enumerate(rows):
        for j, c in enumerate(feature_cols):
            v = r.get(c)
            X[i, j] = float(v) if isinstance(v, (int, float)) and v is not None and math.isfinite(v) else 0.0
        y[i] = r['__fwd_return']
        dates.append(r['date_et'])
        minute_of_day[i] = r['minute_of_day_et']
    return X, y, np.array(dates), minute_of_day, feature_cols


def evaluate_predictions(y_true, y_pred, dates, minute_of_day, horizon, cost_bps=2.0, threshold_bps=2.0):
    """Convert predicted returns into trading signals; compute hit rate + net P&L.

    Rule:
      pred >  +threshold_bps/1e4  → LONG  (entry t+1, exit t+horizon)
      pred <  -threshold_bps/1e4  → SHORT
      else FLAT
    P&L uses the realized fwd return as proxy for trade return; cost subtracted.
    To avoid trade overlap, enforce a cooldown of `horizon` minutes between trades.
    """
    threshold = threshold_bps / 1e4
    cost = cost_bps / 1e4
    n = len(y_pred)
    last_trade_idx_by_day = defaultdict(lambda: -1)
    trades = []
    for i in range(n):
        day = dates[i]
        if i - last_trade_idx_by_day[day] <= horizon:
            continue
        if y_pred[i] > threshold:
            side = 1
        elif y_pred[i] < -threshold:
            side = -1
        else:
            continue
        gross = side * y_true[i]
        net = gross - cost
        trades.append({'date': day, 'minute_of_day_et': int(minute_of_day[i]), 'pred': float(y_pred[i]), 'realized': float(y_true[i]), 'side': side, 'gross': gross, 'net': net})
        last_trade_idx_by_day[day] = i
    if not trades:
        return {'trade_count': 0, 'hit_rate': 0.0, 'total_net_pct': 0.0, 'total_gross_pct': 0.0, 'avg_net_bps': 0.0, 'pearson_r': float(np.corrcoef(y_true, y_pred)[0, 1]) if len(y_true) > 1 else 0.0}
    total_gross = sum(t['gross'] for t in trades)
    total_net = sum(t['net'] for t in trades)
    hits = sum(1 for t in trades if t['net'] > 0)
    pearson = float(np.corrcoef(y_true, y_pred)[0, 1]) if len(y_true) > 1 else 0.0
    return {
        'trade_count': len(trades),
        'hit_rate': hits / len(trades),
        'total_gross_pct': total_gross * 100,
        'total_net_pct': total_net * 100,
        'avg_net_bps': (total_net / len(trades)) * 10_000,
        'pearson_r': pearson,
        'sign_accuracy': float(np.mean(np.sign(y_pred) == np.sign(y_true))),
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--root', default='SPY')
    parser.add_argument('--train-start', default='2026-01-02')
    parser.add_argument('--train-end', default='2026-01-30')
    parser.add_argument('--test-windows', default='2026-02-02:2026-02-27,2026-03-02:2026-03-31,2026-04-01:2026-04-27')
    parser.add_argument('--horizon', type=int, default=5)
    parser.add_argument('--threshold-bps', type=float, default=3.0)
    parser.add_argument('--cost-bps', type=float, default=2.0)
    parser.add_argument('--n-estimators', type=int, default=200)
    parser.add_argument('--learning-rate', type=float, default=0.05)
    parser.add_argument('--num-leaves', type=int, default=15)
    parser.add_argument('--min-data-in-leaf', type=int, default=200)
    parser.add_argument('--reg-lambda', type=float, default=1.0)
    parser.add_argument('--mode', choices=['regressor', 'classifier'], default='regressor')
    parser.add_argument('--keep-cumulative', action='store_true', help='Keep cum_* features (default: drop to reduce overfit)')
    parser.add_argument('--out', default=None, help='output JSON path')
    args = parser.parse_args()

    print(f'\n=== LightGBM meta-classifier on {args.root} flow features ===')
    print(f'horizon={args.horizon}m  threshold={args.threshold_bps}bps  cost={args.cost_bps}bps')

    drop_cum = not args.keep_cumulative
    print(f'\nBuilding TRAIN dataset {args.train_start} → {args.train_end}... drop_cumulative={drop_cum}')
    X_train, y_train, dates_train, mod_train, feat_cols = build_dataset(args.root, args.train_start, args.train_end, args.horizon, drop_cumulative=drop_cum)
    if X_train is None:
        print('No train data found.', file=sys.stderr); sys.exit(1)
    print(f'  shape: {X_train.shape}  features: {len(feat_cols)}')
    print(f'  label stats: mean={y_train.mean()*1e4:.2f}bps std={y_train.std()*1e4:.2f}bps')

    print(f'\nTraining LGBM ({args.mode}, n_estimators={args.n_estimators}, num_leaves={args.num_leaves}, min_leaf={args.min_data_in_leaf}, reg_lambda={args.reg_lambda})...')
    common_args = dict(
        n_estimators=args.n_estimators,
        learning_rate=args.learning_rate,
        num_leaves=args.num_leaves,
        min_data_in_leaf=args.min_data_in_leaf,
        feature_fraction=0.7,
        bagging_fraction=0.7,
        bagging_freq=5,
        reg_lambda=args.reg_lambda,
        verbose=-1,
    )
    if args.mode == 'classifier':
        # Binary: 1 if forward return positive, 0 otherwise. Threshold the predicted prob.
        y_train_bin = (y_train > 0).astype(int)
        model = LGBMClassifier(**common_args)
        model.fit(X_train, y_train_bin)
        train_pred_prob = model.predict_proba(X_train)[:, 1]
        # Convert prob to a directional score in [-1, 1]
        train_pred = (train_pred_prob - 0.5) * 2 * y_train.std()  # rescale to match return magnitude
    else:
        model = LGBMRegressor(**common_args)
        model.fit(X_train, y_train)
        train_pred = model.predict(X_train)
    train_eval = evaluate_predictions(y_train, train_pred, dates_train, mod_train, args.horizon, args.cost_bps, args.threshold_bps)
    print(f'  TRAIN  N={train_eval["trade_count"]:>5d} net={train_eval["total_net_pct"]:>6.2f}%  hit={train_eval["hit_rate"]*100:>5.1f}%  pearson={train_eval["pearson_r"]:>5.3f}  avg={train_eval["avg_net_bps"]:>+.2f}bps')

    results = {
        'config': vars(args),
        'feature_cols_count': len(feat_cols),
        'train_stats': train_eval,
        'test_windows': [],
    }

    for w in args.test_windows.split(','):
        s, e = w.split(':')
        X_t, y_t, d_t, m_t, _ = build_dataset(args.root, s, e, args.horizon, drop_cumulative=drop_cum)
        if X_t is None:
            print(f'No test data for {w}'); continue
        if args.mode == 'classifier':
            pred_prob = model.predict_proba(X_t)[:, 1]
            pred = (pred_prob - 0.5) * 2 * y_train.std()
        else:
            pred = model.predict(X_t)
        ev = evaluate_predictions(y_t, pred, d_t, m_t, args.horizon, args.cost_bps, args.threshold_bps)
        print(f'  {w}  N={ev["trade_count"]:>5d} net={ev["total_net_pct"]:>6.2f}%  hit={ev["hit_rate"]*100:>5.1f}%  pearson={ev["pearson_r"]:>5.3f}  avg={ev["avg_net_bps"]:>+.2f}bps')
        results['test_windows'].append({'window': w, 'stats': ev})

    # Feature importance — top 20
    imp = sorted(zip(feat_cols, model.feature_importances_), key=lambda x: -x[1])[:20]
    print('\nTop-20 feature importances:')
    for name, val in imp:
        print(f'  {name:<40s} {val}')

    results['top_features'] = [(n, int(v)) for n, v in imp]

    out_path = args.out or (ARTIFACTS_DIR / f'lgbm-{args.root}-h{args.horizon}.json')
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, 'w') as f:
        json.dump(results, f, indent=2)
    print(f'\nWritten {out_path}')


if __name__ == '__main__':
    main()
