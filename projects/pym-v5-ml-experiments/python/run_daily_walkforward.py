#!/usr/bin/env python3

import argparse
import datetime as dt
import json
import math
from collections import defaultdict
from pathlib import Path

import numpy as np
from sklearn.linear_model import LogisticRegression, Ridge
from sklearn.multioutput import MultiOutputRegressor

try:
    from lightgbm import LGBMRegressor
    LGBM_AVAILABLE = True
except ImportError:
    LGBMRegressor = None
    LGBM_AVAILABLE = False


DEFAULT_INITIAL_CAPITAL = 10000.0


def finite(value, fallback=0.0):
    try:
        out = float(value)
    except (TypeError, ValueError):
        return fallback
    return out if math.isfinite(out) else fallback


def pct(value):
    return value * 100.0 if math.isfinite(value) else None


def load_dataset(path):
    metadata = None
    samples = []
    with open(path, "r", encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            row = json.loads(line)
            if row.get("type") == "metadata":
                metadata = row
            elif row.get("type") == "sample":
                samples.append(row)
    if metadata is None:
        raise ValueError("dataset metadata missing")
    if not samples:
        raise ValueError("dataset has no samples")
    return metadata, samples


def feature_matrix(samples, feature_groups):
    rows = []
    for sample in samples:
        values = []
        groups = sample["featureGroups"]
        for group in feature_groups:
            values.extend(groups.get(group, []))
        rows.append(values)
    return np.asarray(rows, dtype=np.float64)


def target_matrix(samples, output_tickers, field):
    rows = []
    for sample in samples:
        source = sample[field]
        rows.append([finite(source.get(ticker, 0.0)) for ticker in output_tickers])
    return np.asarray(rows, dtype=np.float64)


def binary_teacher_labels(samples):
    return np.asarray([1 if finite(sample.get("teacherReturn")) > 0 else 0 for sample in samples], dtype=np.int64)


def sample_weights(length, half_life):
    if not half_life or half_life <= 0:
        return None
    ages = np.arange(length - 1, -1, -1, dtype=np.float64)
    weights = np.power(0.5, ages / float(half_life))
    return weights / np.mean(weights)


def weighted_standardize(train_x, current_x, weights=None):
    if weights is None:
        mean = train_x.mean(axis=0)
        std = train_x.std(axis=0)
    else:
        weights = weights / weights.sum()
        mean = (train_x * weights[:, None]).sum(axis=0)
        var = (((train_x - mean) ** 2) * weights[:, None]).sum(axis=0)
        std = np.sqrt(var)
    std[std < 1e-8] = 1.0
    return (train_x - mean) / std, (current_x - mean) / std


def normalize_long_only(raw, output_tickers, safe_ticker, max_weight=1.0):
    weights = {}
    total = 0.0
    for ticker, value in zip(output_tickers, raw):
        clipped = min(max_weight, max(0.0, finite(value)))
        if clipped > 1e-10:
            weights[ticker] = clipped
            total += clipped
    if total <= 1e-10:
        return {safe_ticker: 1.0}
    return {ticker: value / total for ticker, value in weights.items()}


def topk_weights(predictions, output_tickers, teacher_weights, top_k, safe_ticker):
    candidates = []
    for ticker, score in zip(output_tickers, predictions):
        teacher_weight = finite(teacher_weights.get(ticker, 0.0))
        if ticker == safe_ticker or teacher_weight <= 1e-10:
            continue
        candidates.append((ticker, finite(score), teacher_weight))
    candidates.sort(key=lambda row: row[1], reverse=True)
    selected = candidates[:top_k]
    if not selected:
        return {safe_ticker: 1.0}
    total = sum(row[2] for row in selected)
    if total <= 1e-10:
        return {safe_ticker: 1.0}
    return {ticker: teacher_weight / total for ticker, _score, teacher_weight in selected}


def topk_weights_equal(predictions, output_tickers, teacher_weights, top_k, safe_ticker):
    """Equal-weight the top-K candidates from the PYM teacher universe.

    For top_k >= 4 this naturally enforces a 25%-or-tighter per-name cap,
    which is the same diversification discipline that wins for the sleeve-
    meta strategy. For top_k < 4 the per-name weight exceeds 25%.
    """
    candidates = []
    for ticker, score in zip(output_tickers, predictions):
        teacher_weight = finite(teacher_weights.get(ticker, 0.0))
        if ticker == safe_ticker or teacher_weight <= 1e-10:
            continue
        candidates.append((ticker, finite(score)))
    candidates.sort(key=lambda row: row[1], reverse=True)
    selected = candidates[:top_k]
    if not selected:
        return {safe_ticker: 1.0}
    weight = 1.0 / len(selected)
    return {ticker: weight for ticker, _score in selected}


def topk_weights_capped(predictions, output_tickers, teacher_weights, top_k, safe_ticker, max_weight=0.25):
    """Top-K with teacher weights, capped per name with iterative redistribution.

    Selects K names by predicted score from the teacher universe, normalizes
    by teacher weight, then iteratively caps any name above max_weight and
    redistributes the overflow proportionally to the remaining selected names.
    """
    candidates = []
    for ticker, score in zip(output_tickers, predictions):
        teacher_weight = finite(teacher_weights.get(ticker, 0.0))
        if ticker == safe_ticker or teacher_weight <= 1e-10:
            continue
        candidates.append((ticker, finite(score), teacher_weight))
    candidates.sort(key=lambda row: row[1], reverse=True)
    selected = candidates[:top_k]
    if not selected:
        return {safe_ticker: 1.0}
    tickers = [row[0] for row in selected]
    weights = np.array([row[2] for row in selected], dtype=np.float64)
    total = weights.sum()
    if total <= 1e-10:
        return {safe_ticker: 1.0}
    weights = weights / total
    for _ in range(10):
        overflow_mask = weights > max_weight + 1e-12
        if not overflow_mask.any():
            break
        overflow = (weights - max_weight)[overflow_mask].sum()
        weights = np.minimum(weights, max_weight)
        room_mask = weights < max_weight - 1e-12
        if not room_mask.any():
            break
        room_total = (max_weight - weights[room_mask]).sum()
        if room_total <= 1e-12:
            break
        addition = (max_weight - weights[room_mask]) / room_total * overflow
        weights[room_mask] = weights[room_mask] + addition
    return {ticker: float(weights[i]) for i, ticker in enumerate(tickers)}


def predicted_portfolio_return(weights, predictions, output_tickers):
    by_ticker = {ticker: finite(value) for ticker, value in zip(output_tickers, predictions)}
    return sum(finite(weight) * by_ticker.get(ticker, 0.0) for ticker, weight in weights.items())


def portfolio_return(weights, next_returns):
    return sum(finite(weight) * finite(next_returns.get(ticker, 0.0)) for ticker, weight in weights.items())


def turnover(previous, current):
    keys = set(previous) | set(current)
    return sum(abs(finite(current.get(ticker, 0.0)) - finite(previous.get(ticker, 0.0))) for ticker in keys)


def max_drawdown(equity_curve):
    peak = equity_curve[0]["equity"] if equity_curve else DEFAULT_INITIAL_CAPITAL
    drawdown = 0.0
    for point in equity_curve:
        peak = max(peak, point["equity"])
        if peak > 0:
            drawdown = min(drawdown, point["equity"] / peak - 1.0)
    return drawdown


def monthly_returns(equity_curve):
    months = {}
    for point in equity_curve:
        month = point["date"][:7]
        if month not in months:
            months[month] = {"startEquity": point["startEquity"], "endEquity": point["equity"]}
        months[month]["endEquity"] = point["equity"]
    return [
        {
            "month": month,
            "return": values["endEquity"] / values["startEquity"] - 1.0 if values["startEquity"] > 0 else 0.0,
            "returnPct": pct(values["endEquity"] / values["startEquity"] - 1.0 if values["startEquity"] > 0 else 0.0),
        }
        for month, values in sorted(months.items())
    ]


def top_average_weights(equity_curve, limit=12):
    totals = defaultdict(float)
    for point in equity_curve:
        for ticker, weight in point.get("holdings", {}).items():
            totals[ticker] += finite(weight)
    count = max(1, len(equity_curve))
    rows = [
        {"ticker": ticker, "averageWeight": value / count, "averageWeightPct": pct(value / count)}
        for ticker, value in totals.items()
    ]
    rows.sort(key=lambda row: (-row["averageWeight"], row["ticker"]))
    return rows[:limit]


def clamp(value, low=0.0, high=1.0):
    return max(low, min(high, finite(value)))


def feature_value(metadata, sample, group, name):
    names = metadata.get("featureNames", {}).get(group, [])
    try:
        index = names.index(name)
    except ValueError:
        return 0.0
    values = sample.get("featureGroups", {}).get(group, [])
    if index >= len(values):
        return 0.0
    return finite(values[index])


def regime_stress(metadata, sample):
    spy_vol_21 = feature_value(metadata, sample, "price", "SPY_vol_21")
    spy_drawdown_21 = feature_value(metadata, sample, "price", "SPY_drawdown_21")
    qqq_drawdown_21 = feature_value(metadata, sample, "price", "QQQ_drawdown_21")
    vixy_ret_5 = feature_value(metadata, sample, "price", "VIXY_ret_5")
    uvxy_ret_5 = feature_value(metadata, sample, "price", "UVXY_ret_5")
    spy_put_call = feature_value(metadata, sample, "options", "option_SPY_putCallPremiumRatio")
    spx_put_call = max(
        feature_value(metadata, sample, "options", "option_SPX_putCallPremiumRatio"),
        feature_value(metadata, sample, "options", "option_SPXW_putCallPremiumRatio"),
    )
    spy_put_pressure = 1.0 - feature_value(metadata, sample, "options", "option_SPY_callPremiumShare")
    spx_put_pressure = max(
        1.0 - feature_value(metadata, sample, "options", "option_SPX_callPremiumShare"),
        1.0 - feature_value(metadata, sample, "options", "option_SPXW_callPremiumShare"),
    )
    spy_liquidity_stress = max(
        feature_value(metadata, sample, "liquidity", "SPY_rangePct_z_21"),
        feature_value(metadata, sample, "liquidity", "QQQ_rangePct_z_21"),
    )
    pieces = [
        0.18 * clamp((spy_vol_21 - 0.16) / 0.20),
        0.25 * clamp(max(-spy_drawdown_21, -qqq_drawdown_21) / 0.10),
        0.17 * clamp(vixy_ret_5 / 0.18),
        0.13 * clamp(uvxy_ret_5 / 0.30),
        0.08 * clamp((max(spy_put_call, spx_put_call) - 1.1) / 2.5),
        0.08 * clamp((max(spy_put_pressure, spx_put_pressure) - 0.50) / 0.35),
        0.11 * clamp(spy_liquidity_stress / 3.0),
    ]
    return clamp(sum(pieces))


def fit_ridge_return_predictions(spec, train_samples, current_sample, output_tickers, half_life=None, alpha=None):
    train_x = feature_matrix(train_samples, spec["featureGroups"])
    current_x = feature_matrix([current_sample], spec["featureGroups"])
    weights = sample_weights(len(train_samples), half_life)
    train_x, current_x = weighted_standardize(train_x, current_x, weights)
    train_y = target_matrix(train_samples, output_tickers, "nextReturns")
    model = Ridge(alpha=float(alpha if alpha is not None else spec.get("alpha", 1.0)), fit_intercept=True)
    model.fit(train_x, train_y, sample_weight=weights)
    return np.ravel(model.predict(current_x))


def fit_lgbm_return_predictions(spec, train_samples, current_sample, output_tickers, half_life=None):
    """Fit one LightGBM regressor per output ticker via MultiOutputRegressor.

    LightGBM doesn't natively support multi-target regression, so we wrap
    LGBMRegressor in MultiOutputRegressor (one model per ticker, fit in
    parallel via n_jobs). Tree boosting handles the nonlinear interactions
    that the Composer tree encodes natively, where Ridge can only fit a
    linear approximation.
    """
    if not LGBM_AVAILABLE:
        raise RuntimeError("LightGBM not installed. pip install lightgbm")
    train_x = feature_matrix(train_samples, spec["featureGroups"])
    current_x = feature_matrix([current_sample], spec["featureGroups"])
    weights = sample_weights(len(train_samples), half_life)
    train_y = target_matrix(train_samples, output_tickers, "nextReturns")
    base = LGBMRegressor(
        n_estimators=int(spec.get("nEstimators", 80)),
        learning_rate=float(spec.get("learningRate", 0.05)),
        num_leaves=int(spec.get("numLeaves", 15)),
        min_child_samples=int(spec.get("minChildSamples", 5)),
        feature_fraction=float(spec.get("featureFraction", 0.8)),
        bagging_fraction=float(spec.get("baggingFraction", 0.8)),
        bagging_freq=int(spec.get("baggingFreq", 5)),
        reg_alpha=float(spec.get("regAlpha", 0.0)),
        reg_lambda=float(spec.get("regLambda", 0.1)),
        random_state=42,
        verbosity=-1,
        n_jobs=1,
    )
    n_jobs = int(spec.get("nJobs", -1))
    model = MultiOutputRegressor(base, n_jobs=n_jobs)
    if weights is not None:
        model.fit(train_x, train_y, sample_weight=weights)
    else:
        model.fit(train_x, train_y)
    return np.ravel(model.predict(current_x))


def apply_risk_budget(weights, safe_ticker, risk_budget):
    risk_budget = clamp(risk_budget)
    if risk_budget >= 0.999999:
        return weights
    scaled = {ticker: finite(weight) * risk_budget for ticker, weight in weights.items()}
    scaled[safe_ticker] = scaled.get(safe_ticker, 0.0) + (1.0 - risk_budget)
    return {ticker: weight for ticker, weight in scaled.items() if weight > 1e-10}


def summarize_curve(equity_curve, initial_capital):
    returns = [point["netReturn"] for point in equity_curve]
    final_equity = equity_curve[-1]["equity"] if equity_curve else initial_capital
    total_return = final_equity / initial_capital - 1.0 if initial_capital > 0 else 0.0
    volatility = float(np.std(returns, ddof=0) * math.sqrt(252.0)) if returns else 0.0
    avg_daily = float(np.mean(returns)) if returns else 0.0
    cagr = (1.0 + total_return) ** (252.0 / len(equity_curve)) - 1.0 if equity_curve and total_return > -1 else 0.0
    avg_turnover = float(np.mean([point["turnover"] for point in equity_curve])) if equity_curve else 0.0
    return {
        "startDate": equity_curve[0]["signalDate"] if equity_curve else None,
        "endDate": equity_curve[-1]["date"] if equity_curve else None,
        "tradingDays": len(equity_curve),
        "finalEquity": final_equity,
        "totalReturn": total_return,
        "totalReturnPct": pct(total_return),
        "cagr": cagr,
        "cagrPct": pct(cagr),
        "maxDrawdown": max_drawdown(equity_curve),
        "maxDrawdownPct": pct(max_drawdown(equity_curve)),
        "annualizedVolatility": volatility,
        "annualizedVolatilityPct": pct(volatility),
        "sharpe": (avg_daily * 252.0 / volatility) if volatility > 0 else 0.0,
        "averageDailyTurnover": avg_turnover,
        "averageDailyTurnoverPct": pct(avg_turnover),
        "monthlyReturns": monthly_returns(equity_curve),
        "topAverageWeights": top_average_weights(equity_curve),
        "equityCurve": equity_curve,
    }


def make_lgbm_specs(include_options):
    """LightGBM-based variants. Naming: lgbm_<kind>_<features>[_options][_<topkmode>].

    Conservative hyperparameters by default — small num_leaves, high
    min_child_samples, tight regularization — because the walk-forward
    training set is tiny (~22 samples on day 1, growing to ~440). LightGBM
    with sklearn-defaults overfits hard on this signal-to-noise ratio.
    """
    base_groups = ["attention", "pym"]
    conservative = {
        "nEstimators": 30,
        "learningRate": 0.04,
        "numLeaves": 7,
        "minChildSamples": 20,
        "featureFraction": 0.6,
        "baggingFraction": 0.7,
        "regLambda": 1.0,
    }
    specs = [
        # Equal-weight (== natural cap-25 for top_k>=4) is the default.
        {"id": "lgbm_topk_attention_pym_eq", "kind": "lgbm_topk", "featureGroups": base_groups, "topK": 5, "topKMode": "equal", **conservative},
        {"id": "lgbm_topk_attention_pym_eq_top4", "kind": "lgbm_topk", "featureGroups": base_groups, "topK": 4, "topKMode": "equal", **conservative},
        {"id": "lgbm_topk_attention_pym_eq_top8", "kind": "lgbm_topk", "featureGroups": base_groups, "topK": 8, "topKMode": "equal", **conservative},
        # Capped 25% with teacher weight redistribution.
        {"id": "lgbm_topk_attention_pym_cap25", "kind": "lgbm_topk", "featureGroups": base_groups, "topK": 5, "topKMode": "capped", "maxWeight": 0.25, **conservative},
        # Teacher-weighted (matches original Ridge contract for direct comparison).
        {"id": "lgbm_topk_attention_pym_teacher", "kind": "lgbm_topk", "featureGroups": base_groups, "topK": 5, "topKMode": "teacher", **conservative},
        # Price features only — minimum-feature baseline.
        {"id": "lgbm_topk_price_pym_eq", "kind": "lgbm_topk", "featureGroups": ["price", "pym"], "topK": 5, "topKMode": "equal", **conservative},
        # Two-speed LightGBM (long-history + recent-history blend).
        {"id": "lgbm_two_speed_attention_pym_eq", "kind": "two_speed_lgbm_topk", "featureGroups": base_groups, "topK": 5, "topKMode": "equal", "halfLife": 63, "baseRecentWeight": 0.20, "stressRecentWeight": 0.30, "minSwitchEdgeBps": 3, "costMultiplier": 0.75, **conservative},
        {"id": "lgbm_two_speed_attention_pym_cap25", "kind": "two_speed_lgbm_topk", "featureGroups": base_groups, "topK": 5, "topKMode": "capped", "maxWeight": 0.25, "halfLife": 63, "baseRecentWeight": 0.20, "stressRecentWeight": 0.30, "minSwitchEdgeBps": 3, "costMultiplier": 0.75, **conservative},
        # Even more conservative variants, in case the above still overfits.
        {"id": "lgbm_topk_attention_pym_eq_tiny", "kind": "lgbm_topk", "featureGroups": base_groups, "topK": 5, "topKMode": "equal",
         "nEstimators": 15, "learningRate": 0.03, "numLeaves": 3, "minChildSamples": 30, "featureFraction": 0.5, "baggingFraction": 0.6, "regLambda": 5.0},
        # Robustness grid around the "tiny" hyperparameters that beat cap25.
        {"id": "lgbm_topk_attention_pym_eq_tinyA", "kind": "lgbm_topk", "featureGroups": base_groups, "topK": 5, "topKMode": "equal",
         "nEstimators": 10, "learningRate": 0.03, "numLeaves": 3, "minChildSamples": 30, "featureFraction": 0.5, "baggingFraction": 0.6, "regLambda": 5.0},
        {"id": "lgbm_topk_attention_pym_eq_tinyB", "kind": "lgbm_topk", "featureGroups": base_groups, "topK": 5, "topKMode": "equal",
         "nEstimators": 20, "learningRate": 0.03, "numLeaves": 3, "minChildSamples": 30, "featureFraction": 0.5, "baggingFraction": 0.6, "regLambda": 5.0},
        {"id": "lgbm_topk_attention_pym_eq_tinyC", "kind": "lgbm_topk", "featureGroups": base_groups, "topK": 5, "topKMode": "equal",
         "nEstimators": 15, "learningRate": 0.03, "numLeaves": 4, "minChildSamples": 30, "featureFraction": 0.5, "baggingFraction": 0.6, "regLambda": 5.0},
        {"id": "lgbm_topk_attention_pym_eq_tinyD", "kind": "lgbm_topk", "featureGroups": base_groups, "topK": 5, "topKMode": "equal",
         "nEstimators": 15, "learningRate": 0.03, "numLeaves": 2, "minChildSamples": 30, "featureFraction": 0.5, "baggingFraction": 0.6, "regLambda": 5.0},
        {"id": "lgbm_topk_attention_pym_eq_tinyE", "kind": "lgbm_topk", "featureGroups": base_groups, "topK": 5, "topKMode": "equal",
         "nEstimators": 15, "learningRate": 0.05, "numLeaves": 3, "minChildSamples": 30, "featureFraction": 0.5, "baggingFraction": 0.6, "regLambda": 5.0},
        {"id": "lgbm_topk_attention_pym_eq_tinyF", "kind": "lgbm_topk", "featureGroups": base_groups, "topK": 5, "topKMode": "equal",
         "nEstimators": 15, "learningRate": 0.03, "numLeaves": 3, "minChildSamples": 20, "featureFraction": 0.5, "baggingFraction": 0.6, "regLambda": 2.0},
        # Tiny + capped 25% for a more conservative version.
        {"id": "lgbm_topk_attention_pym_cap25_tiny", "kind": "lgbm_topk", "featureGroups": base_groups, "topK": 5, "topKMode": "capped", "maxWeight": 0.25,
         "nEstimators": 15, "learningRate": 0.03, "numLeaves": 3, "minChildSamples": 30, "featureFraction": 0.5, "baggingFraction": 0.6, "regLambda": 5.0},
        # Gap-feature variants (test: do overnight-gap features help the LGBM
        # model? attention/pym only use close-to-close so gap signal is
        # genuinely new info if present in the dataset).
        {"id": "lgbm_topk_attention_pym_gap_eq_tinyB", "kind": "lgbm_topk", "featureGroups": ["attention", "pym", "gap"], "topK": 5, "topKMode": "equal",
         "nEstimators": 20, "learningRate": 0.03, "numLeaves": 3, "minChildSamples": 30, "featureFraction": 0.5, "baggingFraction": 0.6, "regLambda": 5.0},
        {"id": "lgbm_topk_attention_pym_gap_eq_tinyE", "kind": "lgbm_topk", "featureGroups": ["attention", "pym", "gap"], "topK": 5, "topKMode": "equal",
         "nEstimators": 15, "learningRate": 0.05, "numLeaves": 3, "minChildSamples": 30, "featureFraction": 0.5, "baggingFraction": 0.6, "regLambda": 5.0},
        {"id": "lgbm_topk_pym_gap_only_eq_tiny", "kind": "lgbm_topk", "featureGroups": ["pym", "gap"], "topK": 5, "topKMode": "equal",
         "nEstimators": 15, "learningRate": 0.03, "numLeaves": 3, "minChildSamples": 30, "featureFraction": 0.5, "baggingFraction": 0.6, "regLambda": 5.0},
    ]
    if include_options:
        specs.extend([
            {"id": "lgbm_topk_attention_pym_options_eq", "kind": "lgbm_topk", "featureGroups": ["attention", "pym", "options"], "topK": 5, "topKMode": "equal", **conservative},
            {"id": "lgbm_two_speed_attention_pym_options_eq", "kind": "two_speed_lgbm_topk", "featureGroups": ["attention", "pym", "options"], "topK": 5, "topKMode": "equal", "halfLife": 63, "baseRecentWeight": 0.20, "stressRecentWeight": 0.30, "minSwitchEdgeBps": 3, "costMultiplier": 0.75, **conservative},
        ])
    return specs


def make_strategy_specs(include_options):
    specs = [
        {"id": "topk_price_pym", "kind": "topk", "featureGroups": ["price", "pym"], "alpha": 0.1, "topK": 10},
        {"id": "topk_attention_pym", "kind": "topk", "featureGroups": ["attention", "pym"], "alpha": 1.0, "topK": 5},
        {"id": "gate_price_pym", "kind": "gate", "featureGroups": ["price", "pym"], "alpha": 0.01, "threshold": 0.45},
        {"id": "imitate_price", "kind": "imitate", "featureGroups": ["price"], "alpha": 1.0, "maxWeight": 0.5},
        {"id": "imitate_attention", "kind": "imitate", "featureGroups": ["attention"], "alpha": 100.0, "maxWeight": 0.5},
        {"id": "two_speed_attention_pym", "kind": "two_speed_topk", "featureGroups": ["attention", "pym"], "alpha": 1.0, "recentAlpha": 1.0, "topK": 5, "halfLife": 63, "baseRecentWeight": 0.35, "stressRecentWeight": 0.45},
        {"id": "two_speed_attention_pym_governed", "kind": "two_speed_topk", "featureGroups": ["attention", "pym"], "alpha": 1.0, "recentAlpha": 1.0, "topK": 5, "halfLife": 63, "baseRecentWeight": 0.35, "stressRecentWeight": 0.45, "minSwitchEdgeBps": 8, "costMultiplier": 1.25},
        {"id": "two_speed_attention_pym_light", "kind": "two_speed_topk", "featureGroups": ["attention", "pym"], "alpha": 1.0, "recentAlpha": 1.0, "topK": 5, "halfLife": 63, "baseRecentWeight": 0.08, "stressRecentWeight": 0.22},
        {"id": "two_speed_attention_pym_light_governed", "kind": "two_speed_topk", "featureGroups": ["attention", "pym"], "alpha": 1.0, "recentAlpha": 1.0, "topK": 5, "halfLife": 63, "baseRecentWeight": 0.08, "stressRecentWeight": 0.22, "minSwitchEdgeBps": 3, "costMultiplier": 0.75},
        {"id": "topk_attention_liquidity_pym", "kind": "topk", "featureGroups": ["attention", "liquidity", "pym"], "alpha": 1.0, "topK": 5},
        {"id": "two_speed_attention_liquidity_pym_light_governed", "kind": "two_speed_topk", "featureGroups": ["attention", "liquidity", "pym"], "alpha": 1.0, "recentAlpha": 1.0, "topK": 5, "halfLife": 63, "baseRecentWeight": 0.08, "stressRecentWeight": 0.22, "minSwitchEdgeBps": 3, "costMultiplier": 0.75},
        {"id": "topk_attention_micro_pym_a10", "kind": "topk", "featureGroups": ["attention", "micro", "pym"], "alpha": 10.0, "topK": 5},
        {"id": "topk_attention_micro_pym_a100", "kind": "topk", "featureGroups": ["attention", "micro", "pym"], "alpha": 100.0, "topK": 5},
        {"id": "two_speed_attention_micro_pym_light_governed_a10", "kind": "two_speed_topk", "featureGroups": ["attention", "micro", "pym"], "alpha": 10.0, "recentAlpha": 10.0, "topK": 5, "halfLife": 63, "baseRecentWeight": 0.08, "stressRecentWeight": 0.22, "minSwitchEdgeBps": 3, "costMultiplier": 0.75},
        {"id": "two_speed_attention_micro_pym_light_governed_a100", "kind": "two_speed_topk", "featureGroups": ["attention", "micro", "pym"], "alpha": 100.0, "recentAlpha": 100.0, "topK": 5, "halfLife": 63, "baseRecentWeight": 0.08, "stressRecentWeight": 0.22, "minSwitchEdgeBps": 3, "costMultiplier": 0.75},
    ]
    if include_options:
        specs.extend([
            {"id": "topk_price_pym_options", "kind": "topk", "featureGroups": ["price", "pym", "options"], "alpha": 0.1, "topK": 10},
            {"id": "topk_attention_pym_options", "kind": "topk", "featureGroups": ["attention", "pym", "options"], "alpha": 1.0, "topK": 5},
            {"id": "gate_price_pym_options", "kind": "gate", "featureGroups": ["price", "pym", "options"], "alpha": 0.01, "threshold": 0.55},
            {"id": "gate_attention_pym_options", "kind": "gate", "featureGroups": ["attention", "pym", "options"], "alpha": 0.01, "threshold": 0.45},
            {"id": "imitate_price_options", "kind": "imitate", "featureGroups": ["price", "options"], "alpha": 100.0, "maxWeight": 0.5},
            {"id": "topk_price_pym_options_hl63", "kind": "topk", "featureGroups": ["price", "pym", "options"], "alpha": 1.0, "topK": 10, "halfLife": 63},
            {"id": "topk_attention_pym_options_hl63", "kind": "topk", "featureGroups": ["attention", "pym", "options"], "alpha": 1.0, "topK": 5, "halfLife": 63},
            {"id": "gate_attention_pym_options_hl63", "kind": "gate", "featureGroups": ["attention", "pym", "options"], "alpha": 0.1, "threshold": 0.45, "halfLife": 63},
            {"id": "two_speed_attention_pym_options", "kind": "two_speed_topk", "featureGroups": ["attention", "pym", "options"], "alpha": 1.0, "recentAlpha": 1.0, "topK": 5, "halfLife": 63, "baseRecentWeight": 0.30, "stressRecentWeight": 0.50},
            {"id": "two_speed_attention_pym_options_governed", "kind": "two_speed_topk", "featureGroups": ["attention", "pym", "options"], "alpha": 1.0, "recentAlpha": 1.0, "topK": 5, "halfLife": 63, "baseRecentWeight": 0.30, "stressRecentWeight": 0.50, "minSwitchEdgeBps": 8, "costMultiplier": 1.25},
            {"id": "two_speed_attention_pym_options_defensive", "kind": "two_speed_topk", "featureGroups": ["attention", "pym", "options"], "alpha": 1.0, "recentAlpha": 1.0, "topK": 5, "halfLife": 63, "baseRecentWeight": 0.30, "stressRecentWeight": 0.55, "minSwitchEdgeBps": 6, "costMultiplier": 1.0, "riskGate": True, "maxRiskOff": 0.45, "riskReturnThresholdBps": 3},
            {"id": "two_speed_price_pym_options_governed", "kind": "two_speed_topk", "featureGroups": ["price", "pym", "options"], "alpha": 0.1, "recentAlpha": 1.0, "topK": 10, "halfLife": 63, "baseRecentWeight": 0.30, "stressRecentWeight": 0.50, "minSwitchEdgeBps": 8, "costMultiplier": 1.25},
            {"id": "two_speed_attention_pym_options_hl126_governed", "kind": "two_speed_topk", "featureGroups": ["attention", "pym", "options"], "alpha": 1.0, "recentAlpha": 1.0, "topK": 5, "halfLife": 126, "baseRecentWeight": 0.25, "stressRecentWeight": 0.45, "minSwitchEdgeBps": 8, "costMultiplier": 1.25},
            {"id": "topk_attention_liquidity_pym_options", "kind": "topk", "featureGroups": ["attention", "liquidity", "pym", "options"], "alpha": 1.0, "topK": 5},
            {"id": "two_speed_attention_liquidity_pym_options_light_governed", "kind": "two_speed_topk", "featureGroups": ["attention", "liquidity", "pym", "options"], "alpha": 1.0, "recentAlpha": 1.0, "topK": 5, "halfLife": 63, "baseRecentWeight": 0.08, "stressRecentWeight": 0.22, "minSwitchEdgeBps": 3, "costMultiplier": 0.75},
            {"id": "topk_price_liquidity_pym_options", "kind": "topk", "featureGroups": ["price", "liquidity", "pym", "options"], "alpha": 1.0, "topK": 10},
            {"id": "two_speed_price_liquidity_pym_options_light_governed", "kind": "two_speed_topk", "featureGroups": ["price", "liquidity", "pym", "options"], "alpha": 1.0, "recentAlpha": 1.0, "topK": 10, "halfLife": 63, "baseRecentWeight": 0.08, "stressRecentWeight": 0.22, "minSwitchEdgeBps": 3, "costMultiplier": 0.75},
        ])
    return specs


def select_topk_function(spec):
    mode = spec.get("topKMode", "teacher")
    if mode == "equal":
        return topk_weights_equal
    if mode == "capped":
        max_weight = float(spec.get("maxWeight", 0.25))
        return lambda preds, tickers, teacher, k, safe: topk_weights_capped(preds, tickers, teacher, k, safe, max_weight=max_weight)
    return topk_weights


def fit_predict_spec(spec, train_samples, current_sample, output_tickers, safe_ticker, metadata=None, previous_weights=None, cost_bps=2.0):
    if spec["kind"] == "lgbm_topk":
        predictions = fit_lgbm_return_predictions(
            spec, train_samples, current_sample, output_tickers,
            half_life=spec.get("halfLife"),
        )
        topk_fn = select_topk_function(spec)
        return topk_fn(predictions, output_tickers, current_sample["teacherWeights"], spec["topK"], safe_ticker)

    if spec["kind"] == "two_speed_lgbm_topk":
        long_predictions = fit_lgbm_return_predictions(
            spec, train_samples, current_sample, output_tickers, half_life=None,
        )
        recent_predictions = fit_lgbm_return_predictions(
            spec, train_samples, current_sample, output_tickers,
            half_life=spec.get("halfLife", 63),
        )
        stress = regime_stress(metadata or {}, current_sample)
        recent_weight = clamp(
            spec.get("baseRecentWeight", 0.20) + spec.get("stressRecentWeight", 0.30) * stress,
            spec.get("minRecentWeight", 0.05),
            spec.get("maxRecentWeight", 0.85),
        )
        predictions = (1.0 - recent_weight) * long_predictions + recent_weight * recent_predictions
        topk_fn = select_topk_function(spec)
        weights = topk_fn(predictions, output_tickers, current_sample["teacherWeights"], spec["topK"], safe_ticker)
        if previous_weights and spec.get("minSwitchEdgeBps") is not None:
            predicted_edge = predicted_portfolio_return(weights, predictions, output_tickers)
            previous_edge = predicted_portfolio_return(previous_weights, predictions, output_tickers)
            switch_cost = turnover(previous_weights, weights) * cost_bps / 10000.0
            required_edge = spec.get("minSwitchEdgeBps", 0.0) / 10000.0
            required_edge += switch_cost * spec.get("costMultiplier", 1.0)
            if predicted_edge - previous_edge < required_edge:
                return previous_weights
        return weights

    if spec["kind"] == "two_speed_topk":
        long_predictions = fit_ridge_return_predictions(
            spec,
            train_samples,
            current_sample,
            output_tickers,
            half_life=None,
            alpha=spec.get("alpha", 1.0),
        )
        recent_predictions = fit_ridge_return_predictions(
            spec,
            train_samples,
            current_sample,
            output_tickers,
            half_life=spec.get("halfLife", 63),
            alpha=spec.get("recentAlpha", spec.get("alpha", 1.0)),
        )
        stress = regime_stress(metadata or {}, current_sample)
        recent_weight = clamp(
            spec.get("baseRecentWeight", 0.35) + spec.get("stressRecentWeight", 0.45) * stress,
            spec.get("minRecentWeight", 0.10),
            spec.get("maxRecentWeight", 0.90),
        )
        predictions = (1.0 - recent_weight) * long_predictions + recent_weight * recent_predictions
        weights = topk_weights(predictions, output_tickers, current_sample["teacherWeights"], spec["topK"], safe_ticker)
        predicted_edge = predicted_portfolio_return(weights, predictions, output_tickers)
        if spec.get("riskGate"):
            threshold = spec.get("riskReturnThresholdBps", 0.0) / 10000.0
            if predicted_edge < threshold:
                stress_risk_off = spec.get("maxRiskOff", 0.35) * stress
                weights = apply_risk_budget(weights, safe_ticker, 1.0 - stress_risk_off)
        if previous_weights and spec.get("minSwitchEdgeBps") is not None:
            previous_edge = predicted_portfolio_return(previous_weights, predictions, output_tickers)
            switch_cost = turnover(previous_weights, weights) * cost_bps / 10000.0
            required_edge = spec.get("minSwitchEdgeBps", 0.0) / 10000.0
            required_edge += switch_cost * spec.get("costMultiplier", 1.0)
            if predicted_edge - previous_edge < required_edge:
                return previous_weights
        return weights

    train_x = feature_matrix(train_samples, spec["featureGroups"])
    current_x = feature_matrix([current_sample], spec["featureGroups"])
    weights = sample_weights(len(train_samples), spec.get("halfLife"))
    train_x, current_x = weighted_standardize(train_x, current_x, weights)

    if spec["kind"] == "gate":
        labels = binary_teacher_labels(train_samples)
        if labels.min() == labels.max():
            probability = float(labels[0])
        else:
            c_value = 1.0 / max(float(spec.get("alpha", 0.01)), 1e-8)
            model = LogisticRegression(C=c_value, max_iter=800, solver="lbfgs")
            model.fit(train_x, labels, sample_weight=weights)
            probability = float(model.predict_proba(current_x)[0, 1])
        return current_sample["teacherWeights"] if probability >= spec["threshold"] else {safe_ticker: 1.0}

    target_field = "nextReturns" if spec["kind"] == "topk" else "teacherWeights"
    train_y = target_matrix(train_samples, output_tickers, target_field)
    model = Ridge(alpha=float(spec.get("alpha", 1.0)), fit_intercept=True)
    model.fit(train_x, train_y, sample_weight=weights)
    predictions = np.ravel(model.predict(current_x))

    if spec["kind"] == "topk":
        topk_fn = select_topk_function(spec)
        return topk_fn(predictions, output_tickers, current_sample["teacherWeights"], spec["topK"], safe_ticker)
    return normalize_long_only(predictions, output_tickers, safe_ticker, spec.get("maxWeight", 0.5))


def apply_one_day(state, sample, weights, cost_bps, initial_capital):
    previous = state.setdefault("previousWeights", {})
    equity = state.setdefault("equity", initial_capital)
    day_turnover = turnover(previous, weights)
    gross_return = portfolio_return(weights, sample["nextReturns"])
    cost_return = day_turnover * cost_bps / 10000.0
    net_return = gross_return - cost_return
    start_equity = equity
    end_equity = equity * (1.0 + net_return)
    point = {
        "date": sample["nextDate"],
        "signalDate": sample["date"],
        "startEquity": start_equity,
        "equity": end_equity,
        "grossReturn": gross_return,
        "costReturn": cost_return,
        "netReturn": net_return,
        "turnover": day_turnover,
        "holdings": weights,
    }
    state["equity"] = end_equity
    state["previousWeights"] = weights
    state.setdefault("equityCurve", []).append(point)
    return point


def recent_strategy_score(points, lookback):
    recent = points[-lookback:]
    if not recent:
        return 0.0
    total = sum(math.log(max(1e-9, 1.0 + point["netReturn"])) for point in recent)
    peak = 1.0
    equity = 1.0
    drawdown = 0.0
    for point in recent:
        equity *= 1.0 + point["netReturn"]
        peak = max(peak, equity)
        drawdown = min(drawdown, equity / peak - 1.0)
    return total + 0.5 * drawdown


def run_walkforward(metadata, samples, args):
    output_tickers = metadata["outputTickers"]
    safe_ticker = metadata["safeTicker"]
    include_options = "options" in metadata.get("featureNames", {})
    if args.lgbm_only:
        all_specs = make_lgbm_specs(include_options)
    elif args.with_lgbm:
        all_specs = make_strategy_specs(include_options) + make_lgbm_specs(include_options)
    else:
        all_specs = make_strategy_specs(include_options)
    selected_ids = set(args.strategies.split(",")) if args.strategies else None
    specs = [spec for spec in all_specs if selected_ids is None or spec["id"] in selected_ids]
    prediction_samples = [sample for sample in samples if sample["date"] >= args.predict_start and (not args.predict_end or sample["date"] <= args.predict_end)]
    if not prediction_samples:
        raise ValueError("no prediction samples in requested window")

    states = {spec["id"]: {"equity": args.initial_capital, "previousWeights": {}, "equityCurve": []} for spec in specs}
    states["pym_v5_base"] = {"equity": args.initial_capital, "previousWeights": {}, "equityCurve": []}
    states["daily_best_recent_21"] = {"equity": args.initial_capital, "previousWeights": {}, "equityCurve": []}
    states["daily_best_recent_63"] = {"equity": args.initial_capital, "previousWeights": {}, "equityCurve": []}
    chooser_history = {"daily_best_recent_21": [], "daily_best_recent_63": []}

    by_date = {sample["date"]: sample for sample in samples}
    training_start = args.train_start
    first_signal_date = prediction_samples[0]["date"]
    last_signal_date = prediction_samples[-1]["date"]
    skipped = []

    for current_index, sample in enumerate(prediction_samples, start=1):
        train_samples = [row for row in samples if training_start <= row["date"] < sample["date"]]
        if len(train_samples) < args.min_train_samples:
            skipped.append(sample["date"])
            continue

        apply_one_day(states["pym_v5_base"], sample, sample["teacherWeights"], args.cost_bps, args.initial_capital)
        day_predictions = {}
        for spec in specs:
            weights = fit_predict_spec(
                spec,
                train_samples,
                sample,
                output_tickers,
                safe_ticker,
                metadata=metadata,
                previous_weights=states[spec["id"]].get("previousWeights", {}),
                cost_bps=args.cost_bps,
            )
            day_predictions[spec["id"]] = weights
            apply_one_day(states[spec["id"]], sample, weights, args.cost_bps, args.initial_capital)

        for chooser_id, lookback in [("daily_best_recent_21", 21), ("daily_best_recent_63", 63)]:
            candidates = []
            for spec in specs:
                prior_points = states[spec["id"]]["equityCurve"][:-1]
                candidates.append((recent_strategy_score(prior_points, lookback), spec["id"]))
            candidates.sort(reverse=True)
            chosen_id = candidates[0][1] if candidates else specs[0]["id"]
            chooser_history[chooser_id].append({"signalDate": sample["date"], "chosenStrategy": chosen_id})
            apply_one_day(states[chooser_id], sample, day_predictions[chosen_id], args.cost_bps, args.initial_capital)

        if args.progress and current_index % args.progress == 0:
            print(json.dumps({"processed": current_index, "signalDate": sample["date"]}), flush=True)

    strategy_reports = {}
    for strategy_id, state in states.items():
        strategy_reports[strategy_id] = summarize_curve(state["equityCurve"], args.initial_capital)
        if strategy_id in chooser_history:
            counts = defaultdict(int)
            for row in chooser_history[strategy_id]:
                counts[row["chosenStrategy"]] += 1
            strategy_reports[strategy_id]["selectionCounts"] = dict(sorted(counts.items(), key=lambda item: (-item[1], item[0])))
            strategy_reports[strategy_id]["selections"] = chooser_history[strategy_id]

    baseline_return = strategy_reports["pym_v5_base"]["totalReturn"]
    rankings = []
    for strategy_id, report in strategy_reports.items():
        rankings.append({
            "id": strategy_id,
            "totalReturnPct": report["totalReturnPct"],
            "excessReturnPct": pct(report["totalReturn"] - baseline_return),
            "sharpe": report["sharpe"],
            "maxDrawdownPct": report["maxDrawdownPct"],
            "averageDailyTurnoverPct": report["averageDailyTurnoverPct"],
            "tradingDays": report["tradingDays"],
        })
    rankings.sort(key=lambda row: row["totalReturnPct"], reverse=True)

    return {
        "generatedAt": args.generated_at,
        "source": metadata.get("source", {}),
        "settings": {
            "trainStart": args.train_start,
            "predictStartRequested": args.predict_start,
            "predictEndRequested": args.predict_end,
            "firstSignalDate": first_signal_date,
            "lastSignalDate": last_signal_date,
            "minTrainSamples": args.min_train_samples,
            "initialCapital": args.initial_capital,
            "costBps": args.cost_bps,
            "timing": "train_on_prior_labeled_days_signal_eod_close_then_next_close",
            "note": "Each ML strategy is refit daily using only samples with signal dates before the current signal date.",
        },
        "data": {
            "samples": len(samples),
            "predictionSamples": len(prediction_samples),
            "skippedSignals": skipped,
            "outputTickers": output_tickers,
            "safeTicker": safe_ticker,
            "featureGroupSizes": {name: len(values) for name, values in metadata.get("featureNames", {}).items()},
        },
        "strategySpecs": specs,
        "strategies": strategy_reports,
        "rankings": rankings,
    }


def parse_args():
    parser = argparse.ArgumentParser(description="Daily walk-forward PYM V5 ML experiment")
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--train-start", default="2025-01-02")
    parser.add_argument("--predict-start", default="2025-02-01")
    parser.add_argument("--predict-end", default=None)
    parser.add_argument("--min-train-samples", type=int, default=15)
    parser.add_argument("--initial-capital", type=float, default=DEFAULT_INITIAL_CAPITAL)
    parser.add_argument("--cost-bps", type=float, default=2.0)
    parser.add_argument("--strategies", default=None)
    parser.add_argument("--progress", type=int, default=25)
    parser.add_argument("--generated-at", default=None)
    parser.add_argument("--with-lgbm", action="store_true", help="Append LightGBM strategy variants to the default Ridge specs.")
    parser.add_argument("--lgbm-only", action="store_true", help="Run only LightGBM strategy variants.")
    return parser.parse_args()


def main():
    args = parse_args()
    args.generated_at = args.generated_at or dt.datetime.now(dt.UTC).isoformat()
    metadata, samples = load_dataset(args.dataset)
    report = run_walkforward(metadata, samples, args)
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps({
        "outputPath": str(out_path),
        "firstSignalDate": report["settings"]["firstSignalDate"],
        "lastSignalDate": report["settings"]["lastSignalDate"],
        "rankings": [
            {
                "id": row["id"],
                "totalReturnPct": round(row["totalReturnPct"], 2),
                "excessReturnPct": round(row["excessReturnPct"], 2),
                "sharpe": round(row["sharpe"], 3),
                "maxDrawdownPct": round(row["maxDrawdownPct"], 2),
                "averageDailyTurnoverPct": round(row["averageDailyTurnoverPct"], 2),
            }
            for row in report["rankings"]
        ],
    }, indent=2))


if __name__ == "__main__":
    main()
