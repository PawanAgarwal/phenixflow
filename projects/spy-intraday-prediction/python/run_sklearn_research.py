#!/usr/bin/env python3
import argparse
import json
import math
from datetime import UTC, datetime
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = PROJECT_ROOT / "config" / "universe.json"


def require_python_ml():
    try:
        import numpy as np
        import pandas as pd
        from sklearn.ensemble import HistGradientBoostingClassifier, RandomForestClassifier
        from sklearn.linear_model import ElasticNet, LogisticRegression
        from sklearn.metrics import balanced_accuracy_score, confusion_matrix, mean_absolute_error, mean_squared_error
        from sklearn.pipeline import make_pipeline
        from sklearn.preprocessing import StandardScaler
    except Exception as exc:
        return {
            "missing": True,
            "error": str(exc),
            "message": "python_ml_dependencies_missing: install numpy, pandas, and scikit-learn for this optional research track",
        }
    return {
        "np": np,
        "pd": pd,
        "HistGradientBoostingClassifier": HistGradientBoostingClassifier,
        "RandomForestClassifier": RandomForestClassifier,
        "ElasticNet": ElasticNet,
        "LogisticRegression": LogisticRegression,
        "balanced_accuracy_score": balanced_accuracy_score,
        "confusion_matrix": confusion_matrix,
        "mean_absolute_error": mean_absolute_error,
        "mean_squared_error": mean_squared_error,
        "make_pipeline": make_pipeline,
        "StandardScaler": StandardScaler,
    }


def read_jsonl(path):
    with open(path, "r", encoding="utf-8") as handle:
        return [json.loads(line) for line in handle if line.strip()]


def load_config(path):
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def numeric_feature_columns(df):
    banned = {"rowId", "tradeDate", "minuteUtc", "minuteMs"}
    cols = []
    for col in df.columns:
        if col in banned or col.startswith("label_"):
            continue
        if str(df[col].dtype).startswith(("float", "int")):
            cols.append(col)
    return sorted(cols)


def filter_features(cols, feature_set):
    if feature_set == "cross_sectional":
        return [
            col
            for col in cols
            if (
                "_ret_" in col
                or "_breadth_" in col
                or "_rel_spy_" in col
                or "_volume_log" in col
                or col in {"minuteOfDayEt", "minutes_from_open", "minutes_to_close"}
            )
            and not col.startswith("opt_")
            and not col.startswith("opening_opt_")
            and not col.startswith("gamma_")
        ]
    if feature_set == "gamma_regime":
        return [
            col
            for col in cols
            if col.startswith("gamma_proxy_")
            or col.startswith("opt_spx_")
            or col.startswith("opt_spy_")
            or col.startswith("vix")
            or col.startswith("spy_rv_")
            or col in {"minuteOfDayEt", "minutes_from_open", "minutes_to_close", "opening_30m_return"}
        ]
    return cols


def rows_in_window(df, window):
    return df[(df["tradeDate"] >= window["startDate"]) & (df["tradeDate"] <= window["endDate"])].copy()


def score_predictions(actual_returns, probabilities, predicted_returns, metrics):
    y_true = (actual_returns > 0).astype(int)
    y_pred = (probabilities >= 0.5).astype(int)
    cm = metrics["confusion_matrix"](y_true, y_pred, labels=[1, 0])
    rmse = math.sqrt(metrics["mean_squared_error"](actual_returns, predicted_returns))
    return {
        "count": int(len(y_true)),
        "directionalAccuracy": float((y_true == y_pred).mean()) if len(y_true) else None,
        "balancedAccuracy": float(metrics["balanced_accuracy_score"](y_true, y_pred)) if len(y_true) else None,
        "confusion": {
            "tp": int(cm[0][0]),
            "fn": int(cm[0][1]),
            "fp": int(cm[1][0]),
            "tn": int(cm[1][1]),
        },
        "returnMae": float(metrics["mean_absolute_error"](actual_returns, predicted_returns)),
        "returnRmse": float(rmse),
    }


def fit_and_score(df, config, metrics, train_mode_name, train_window, feature_set, horizon):
    label = f"label_{horizon}_return"
    if label not in df.columns:
        return []
    feature_cols = filter_features(numeric_feature_columns(df), feature_set)
    if not feature_cols:
        return []
    train = rows_in_window(df, train_window).dropna(subset=[label])
    train = train[train[label].apply(lambda value: isinstance(value, (int, float)) and math.isfinite(value))]
    if len(train) < 30:
        return []
    x_train = train[feature_cols].fillna(0)
    y_train_direction = (train[label] > 0).astype(int)
    y_train_return = train[label]
    models = {
        "logistic": metrics["make_pipeline"](
            metrics["StandardScaler"](),
            metrics["LogisticRegression"](max_iter=500, C=0.5, class_weight="balanced"),
        ),
        "random_forest": metrics["RandomForestClassifier"](
            n_estimators=160,
            max_depth=7,
            min_samples_leaf=20,
            random_state=17,
            class_weight="balanced_subsample",
            n_jobs=-1,
        ),
        "hist_gradient_boosting": metrics["HistGradientBoostingClassifier"](
            max_iter=160,
            max_leaf_nodes=15,
            learning_rate=0.05,
            random_state=17,
        ),
    }
    return_model = metrics["make_pipeline"](
        metrics["StandardScaler"](),
        metrics["ElasticNet"](alpha=0.0005, l1_ratio=0.4, max_iter=5000, random_state=17),
    )
    return_model.fit(x_train, y_train_return)
    results = []
    for model_name, model in models.items():
        model.fit(x_train, y_train_direction)
        for window in config["windows"]["tests"]:
            test = rows_in_window(df, window).dropna(subset=[label])
            test = test[test[label].apply(lambda value: isinstance(value, (int, float)) and math.isfinite(value))]
            if test.empty:
                continue
            x_test = test[feature_cols].fillna(0)
            if hasattr(model, "predict_proba"):
                probabilities = model.predict_proba(x_test)[:, 1]
            else:
                probabilities = model.predict(x_test)
            predicted_returns = return_model.predict(x_test)
            results.append({
                "trainMode": train_mode_name,
                "featureSet": feature_set,
                "model": model_name,
                "horizon": horizon,
                "split": window["name"],
                "trainRows": int(len(train)),
                "testRows": int(len(test)),
                "featureCount": int(len(feature_cols)),
                "metrics": score_predictions(test[label], probabilities, predicted_returns, metrics),
            })
    return results


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--config", default=str(CONFIG_PATH))
    parser.add_argument("--output", default=None)
    args = parser.parse_args()
    metrics = require_python_ml()
    config = load_config(Path(args.config))
    output = Path(args.output or PROJECT_ROOT / "artifacts" / "python-sklearn-research.json")
    output.parent.mkdir(parents=True, exist_ok=True)
    if metrics.get("missing"):
        report = {
            "generatedAt": datetime.now(UTC).isoformat(),
            "datasetPath": str(Path(args.dataset).resolve()),
            "status": "skipped_missing_dependencies",
            "message": metrics["message"],
            "error": metrics["error"],
            "results": [],
        }
        output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
        print(json.dumps({"outputPath": str(output), "resultCount": 0, "status": report["status"]}, indent=2))
        return
    df = metrics["pd"].DataFrame(read_jsonl(Path(args.dataset)))
    train_modes = [
        (config["research"]["officialTrainName"], config["windows"]["train"]),
        (config["research"]["sensitivityTrainName"], config["windows"]["sensitivityTrain"]),
    ]
    results = []
    for train_name, train_window in train_modes:
        for feature_set in ["cross_sectional", "gamma_regime"]:
            for horizon in ["next_5m", "next_60m"]:
                results.extend(fit_and_score(df, config, metrics, train_name, train_window, feature_set, horizon))
    ranked = sorted(
        results,
        key=lambda item: (
            item["metrics"]["balancedAccuracy"] if item["metrics"]["balancedAccuracy"] is not None else -1,
            item["metrics"]["directionalAccuracy"] if item["metrics"]["directionalAccuracy"] is not None else -1,
        ),
        reverse=True,
    )
    report = {
        "generatedAt": datetime.now(UTC).isoformat(),
        "datasetPath": str(Path(args.dataset).resolve()),
        "rankingBasis": "balanced accuracy first",
        "results": ranked,
    }
    output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"outputPath": str(output), "resultCount": len(results), "topResults": ranked[:10]}, indent=2))


if __name__ == "__main__":
    main()
