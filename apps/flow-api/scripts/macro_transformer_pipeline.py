#!/usr/bin/env python3
"""Macro + market transformer pipeline for multi-horizon direction/magnitude forecasting.

What this script does:
1. Collects last 5 years of market and macro data from multiple public sources.
2. Builds a daily feature store and forward-return targets (1w/1m/3m).
3. Trains a multi-task transformer model (magnitude + direction heads).
4. Produces latest predictions for S&P, Nasdaq, and sector proxies.
5. Tracks forecast accuracy over time as outcomes realize.
6. Builds an upcoming macro release calendar from official sources.

Sources used:
- Stooq: market and sector price history
- FRED: macroeconomic time series
- BLS: CPI and Employment Situation release calendars
- BEA: release schedule
- Federal Reserve: FOMC meeting calendar
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
import random
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd
import requests
import torch
import torch.nn as nn
from bs4 import BeautifulSoup
from pandas.tseries.offsets import BDay
from torch.utils.data import DataLoader, Dataset


USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
)

HORIZONS = {
    "1w": 5,
    "1m": 21,
    "3m": 63,
}

# S&P, Nasdaq, and 11 sector proxies.
ASSET_TO_STOOQ = {
    "SP500": "^spx",
    "NASDAQ": "^ndq",
    "CommunicationServices": "xlc.us",
    "ConsumerDiscretionary": "xly.us",
    "ConsumerStaples": "xlp.us",
    "Energy": "xle.us",
    "Financials": "xlf.us",
    "HealthCare": "xlv.us",
    "Industrials": "xli.us",
    "InformationTechnology": "xlk.us",
    "Materials": "xlb.us",
    "RealEstate": "xlre.us",
    "Utilities": "xlu.us",
}

# Broad macro panel from FRED.
FRED_SERIES = {
    "cpi_headline": "CPIAUCSL",
    "cpi_core": "CPILFESL",
    "pce_headline": "PCEPI",
    "pce_core": "PCEPILFE",
    "ppi_final_demand": "PPIFID",
    "inflation_expect_5y": "T5YIE",
    "inflation_expect_10y": "T10YIE",
    "unemployment_rate": "UNRATE",
    "payrolls_total_nonfarm": "PAYEMS",
    "avg_hourly_earnings": "CES0500000003",
    "labor_force_participation": "CIVPART",
    "jobless_claims_initial": "ICSA",
    "jobless_claims_continued": "CCSA",
    "job_openings": "JTSJOL",
    "quits_rate": "JTSQUR",
    "real_gdp": "GDPC1",
    "industrial_production": "INDPRO",
    "capacity_utilization": "TCU",
    "retail_sales_nominal": "RSAFS",
    "retail_sales_real": "RRSFS",
    "durable_goods_orders": "DGORDER",
    "ism_mfg_pmi": "NAPM",
    "housing_starts": "HOUST",
    "building_permits": "PERMIT",
    "new_home_sales": "HSN1F",
    "consumer_sentiment": "UMCSENT",
    "fed_funds": "FEDFUNDS",
    "treasury_10y": "DGS10",
    "treasury_2y": "DGS2",
    "tbill_3m": "TB3MS",
    "term_spread_10y_2y": "T10Y2Y",
    "baa_10y_spread": "BAA10Y",
    "high_yield_oas": "BAMLH0A0HYM2",
    "investment_grade_oas": "BAMLC0A0CM",
    "dollar_index": "DTWEXBGS",
    "wti_crude": "DCOILWTICO",
    "nfc_index": "NFCI",
    "vix": "VIXCLS",
    "money_supply_m2": "M2SL",
    "mortgage_rate_30y": "MORTGAGE30US",
}


@dataclass
class PipelineConfig:
    base_dir: Path
    start_date: date
    end_date: date
    seq_len: int = 60
    batch_size: int = 64
    epochs: int = 80
    learning_rate: float = 1e-3
    weight_decay: float = 1e-5
    d_model: int = 96
    n_heads: int = 4
    n_layers: int = 2
    ff_dim: int = 192
    dropout: float = 0.15
    early_stopping_patience: int = 10
    random_seed: int = 42

    @property
    def raw_dir(self) -> Path:
        return self.base_dir / "raw"

    @property
    def processed_dir(self) -> Path:
        return self.base_dir / "processed"

    @property
    def model_dir(self) -> Path:
        return self.base_dir / "models"

    @property
    def reports_dir(self) -> Path:
        return self.base_dir / "reports"


def set_global_seed(seed: int) -> None:
    random.seed(seed)
    np.random.seed(seed)
    torch.manual_seed(seed)


def ensure_dirs(cfg: PipelineConfig) -> None:
    for p in [cfg.base_dir, cfg.raw_dir, cfg.processed_dir, cfg.model_dir, cfg.reports_dir]:
        p.mkdir(parents=True, exist_ok=True)


def get_text_with_retries(url: str, timeout: int = 40, retries: int = 5, headers: Optional[Dict[str, str]] = None) -> str:
    h = {"User-Agent": USER_AGENT}
    if headers:
        h.update(headers)

    delay = 1.0
    last_err: Optional[Exception] = None
    for _ in range(retries):
        try:
            resp = requests.get(url, headers=h, timeout=timeout)
            if resp.status_code in {429, 500, 502, 503, 504}:
                raise requests.HTTPError(f"HTTP {resp.status_code}")
            resp.raise_for_status()
            return resp.text
        except Exception as exc:  # pylint: disable=broad-exception-caught
            last_err = exc
            time_sleep(delay)
            delay = min(delay * 1.8, 12.0)

    raise RuntimeError(f"Failed to fetch {url}: {last_err}")


def time_sleep(seconds: float) -> None:
    # Isolated wrapper to keep retry logic easy to test/patch.
    import time

    time.sleep(seconds)


def fetch_stooq_series(symbol: str) -> pd.DataFrame:
    url = f"https://stooq.com/q/d/l/?s={symbol}&i=d"
    text = get_text_with_retries(url, timeout=40, retries=5)
    if text.strip().lower().startswith("no data"):
        raise RuntimeError(f"No Stooq data for symbol: {symbol}")

    df = pd.read_csv(pd.compat.StringIO(text)) if hasattr(pd.compat, "StringIO") else pd.read_csv(__import__("io").StringIO(text))
    if "Date" not in df.columns or "Close" not in df.columns:
        raise RuntimeError(f"Unexpected Stooq schema for {symbol}")

    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df = df.dropna(subset=["Date"]).sort_values("Date")
    df = df[["Date", "Close"]].rename(columns={"Close": symbol})
    df[symbol] = pd.to_numeric(df[symbol], errors="coerce")
    df = df.dropna(subset=[symbol]).reset_index(drop=True)
    return df


def collect_market_data(cfg: PipelineConfig) -> pd.DataFrame:
    frames: List[pd.DataFrame] = []
    for asset, symbol in ASSET_TO_STOOQ.items():
        df = fetch_stooq_series(symbol)
        raw_path = cfg.raw_dir / f"stooq_{symbol.replace('^', 'idx_').replace('.', '_')}.csv"
        df.to_csv(raw_path, index=False)
        df = df.rename(columns={symbol: asset})
        frames.append(df)

    merged = frames[0]
    for df in frames[1:]:
        merged = merged.merge(df, on="Date", how="inner")

    merged = merged.sort_values("Date").reset_index(drop=True)
    merged = merged[(merged["Date"].dt.date >= cfg.start_date) & (merged["Date"].dt.date <= cfg.end_date)]
    merged.to_csv(cfg.processed_dir / "asset_prices_daily.csv", index=False)
    return merged


def fetch_fred_series(series_id: str) -> pd.DataFrame:
    url = f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={series_id}"
    text = get_text_with_retries(url, timeout=40, retries=6)
    df = pd.read_csv(pd.compat.StringIO(text)) if hasattr(pd.compat, "StringIO") else pd.read_csv(__import__("io").StringIO(text))
    if df.shape[1] != 2:
        raise RuntimeError(f"Unexpected FRED schema for {series_id}")
    df.columns = ["Date", series_id]
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df[series_id] = pd.to_numeric(df[series_id], errors="coerce")
    df = df.dropna(subset=["Date"]).sort_values("Date").reset_index(drop=True)
    return df


def collect_macro_data(cfg: PipelineConfig, trading_dates: pd.DatetimeIndex) -> pd.DataFrame:
    series_frames: List[pd.DataFrame] = []

    for feature_name, series_id in FRED_SERIES.items():
        try:
            df = fetch_fred_series(series_id)
        except Exception as exc:  # pylint: disable=broad-exception-caught
            print(f"[WARN] Skipping FRED series {series_id}: {exc}")
            continue

        raw_path = cfg.raw_dir / f"fred_{series_id}.csv"
        df.to_csv(raw_path, index=False)

        df = df.rename(columns={series_id: feature_name})
        series_frames.append(df)

    if not series_frames:
        raise RuntimeError("No macro series were collected from FRED")

    macro = series_frames[0]
    for df in series_frames[1:]:
        macro = macro.merge(df, on="Date", how="outer")

    macro = macro.sort_values("Date").set_index("Date")
    # Some series (e.g., weekly claims) are timestamped on non-trading days.
    # Expand the index first so forward-fill has anchor points, then project to trading dates.
    expanded_idx = macro.index.union(trading_dates)
    macro = macro.reindex(expanded_idx).sort_index().ffill().reindex(trading_dates)
    macro = macro.reset_index().rename(columns={"index": "Date"})

    macro = macro[(macro["Date"].dt.date >= cfg.start_date) & (macro["Date"].dt.date <= cfg.end_date)]
    macro.to_csv(cfg.processed_dir / "macro_series_daily.csv", index=False)
    return macro


def build_feature_frame(market_df: pd.DataFrame, macro_df: pd.DataFrame) -> pd.DataFrame:
    df = market_df.merge(macro_df, on="Date", how="inner").sort_values("Date").reset_index(drop=True)

    feature_cols: Dict[str, pd.Series] = {}

    # Market-based features.
    for asset in ASSET_TO_STOOQ:
        px = pd.to_numeric(df[asset], errors="coerce")
        ret_1 = px.pct_change(1)
        ret_5 = px.pct_change(5)
        ret_21 = px.pct_change(21)
        vol_21 = ret_1.rolling(21).std()

        feature_cols[f"feat_{asset}_ret1"] = ret_1
        feature_cols[f"feat_{asset}_ret5"] = ret_5
        feature_cols[f"feat_{asset}_ret21"] = ret_21
        feature_cols[f"feat_{asset}_vol21"] = vol_21

    # Macro level + short/medium changes + rolling z-score.
    macro_feature_names = [c for c in macro_df.columns if c != "Date"]
    for c in macro_feature_names:
        s = pd.to_numeric(df[c], errors="coerce")
        feature_cols[f"feat_{c}_lvl"] = s
        feature_cols[f"feat_{c}_chg1"] = s.diff(1)
        feature_cols[f"feat_{c}_chg21"] = s.diff(21)
        rolling_std = s.rolling(252).std().replace(0.0, np.nan)
        feature_cols[f"feat_{c}_z252"] = (s - s.rolling(252).mean()) / rolling_std

    feats = pd.DataFrame(feature_cols)
    out = pd.concat([df[["Date"]], feats], axis=1)
    out = out.replace([np.inf, -np.inf], np.nan).ffill().bfill()
    out.to_csv("data/macro_transformer/processed/features_daily.csv", index=False)
    return out


def build_target_frame(market_df: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame({"Date": market_df["Date"]})

    for asset in ASSET_TO_STOOQ:
        px = pd.to_numeric(market_df[asset], errors="coerce")
        for h_name, h_days in HORIZONS.items():
            fwd = px.shift(-h_days) / px - 1.0
            out[f"target_ret_{asset}_{h_name}"] = fwd
            out[f"target_dir_{asset}_{h_name}"] = (fwd > 0).astype(float)

    out.to_csv("data/macro_transformer/processed/targets_daily.csv", index=False)
    return out


def make_sequences(
    features: pd.DataFrame,
    targets: pd.DataFrame,
    seq_len: int,
    ret_cols: Sequence[str],
    dir_cols: Sequence[str],
) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    feat_cols = [c for c in features.columns if c != "Date"]

    merged = features.merge(targets, on="Date", how="inner").sort_values("Date").reset_index(drop=True)

    X_list: List[np.ndarray] = []
    y_ret_list: List[np.ndarray] = []
    y_dir_list: List[np.ndarray] = []
    date_list: List[np.datetime64] = []

    X_raw = merged[feat_cols].values.astype(np.float32)
    y_ret_raw = merged[list(ret_cols)].values.astype(np.float32)
    y_dir_raw = merged[list(dir_cols)].values.astype(np.float32)
    dates_raw = merged["Date"].values

    for i in range(seq_len - 1, len(merged)):
        if np.isnan(y_ret_raw[i]).any() or np.isnan(y_dir_raw[i]).any():
            continue
        window = X_raw[i - seq_len + 1 : i + 1]
        if np.isnan(window).any():
            continue

        X_list.append(window)
        y_ret_list.append(y_ret_raw[i])
        y_dir_list.append(y_dir_raw[i])
        date_list.append(dates_raw[i])

    X = np.stack(X_list).astype(np.float32)
    y_ret = np.stack(y_ret_list).astype(np.float32)
    y_dir = np.stack(y_dir_list).astype(np.float32)
    d = np.array(date_list)
    return X, y_ret, y_dir, d


class SequenceDataset(Dataset):
    def __init__(
        self,
        X: np.ndarray,
        y_ret: np.ndarray,
        y_dir: np.ndarray,
        sample_weights: Optional[np.ndarray] = None,
    ) -> None:
        self.X = torch.tensor(X, dtype=torch.float32)
        self.y_ret = torch.tensor(y_ret, dtype=torch.float32)
        self.y_dir = torch.tensor(y_dir, dtype=torch.float32)
        if sample_weights is None:
            sample_weights = np.ones(len(X), dtype=np.float32)
        self.w = torch.tensor(sample_weights, dtype=torch.float32)

    def __len__(self) -> int:
        return self.X.shape[0]

    def __getitem__(self, idx: int):
        return self.X[idx], self.y_ret[idx], self.y_dir[idx], self.w[idx]


class MultiTaskTransformer(nn.Module):
    def __init__(
        self,
        input_dim: int,
        seq_len: int,
        d_model: int,
        n_heads: int,
        n_layers: int,
        ff_dim: int,
        dropout: float,
        out_dim: int,
    ) -> None:
        super().__init__()
        self.input_proj = nn.Linear(input_dim, d_model)
        self.pos_emb = nn.Parameter(torch.randn(seq_len, d_model) * 0.01)

        enc_layer = nn.TransformerEncoderLayer(
            d_model=d_model,
            nhead=n_heads,
            dim_feedforward=ff_dim,
            dropout=dropout,
            batch_first=True,
            activation="gelu",
        )
        self.encoder = nn.TransformerEncoder(enc_layer, num_layers=n_layers)
        self.norm = nn.LayerNorm(d_model)

        self.reg_head = nn.Sequential(
            nn.Linear(d_model, d_model),
            nn.GELU(),
            nn.Dropout(dropout),
            nn.Linear(d_model, out_dim),
        )
        self.cls_head = nn.Sequential(
            nn.Linear(d_model, d_model),
            nn.GELU(),
            nn.Dropout(dropout),
            nn.Linear(d_model, out_dim),
        )

    def forward(self, x: torch.Tensor) -> Tuple[torch.Tensor, torch.Tensor]:
        # x: [B, T, F]
        h = self.input_proj(x)
        h = h + self.pos_emb.unsqueeze(0)
        h = self.encoder(h)
        # last token representation
        z = self.norm(h[:, -1, :])
        reg = self.reg_head(z)
        cls = self.cls_head(z)
        return reg, cls


def split_indices(n: int) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    n_train = int(n * 0.70)
    n_val = int(n * 0.15)
    idx = np.arange(n)
    train_idx = idx[:n_train]
    val_idx = idx[n_train : n_train + n_val]
    test_idx = idx[n_train + n_val :]
    return train_idx, val_idx, test_idx


def standardize_sequences(
    X: np.ndarray,
    train_idx: np.ndarray,
) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    # Fit scaler on flattened train windows.
    flat = X[train_idx].reshape(-1, X.shape[-1])
    mean = flat.mean(axis=0)
    std = flat.std(axis=0)
    std = np.where(std < 1e-8, 1.0, std)

    X_std = (X - mean.reshape(1, 1, -1)) / std.reshape(1, 1, -1)
    return X_std.astype(np.float32), mean.astype(np.float32), std.astype(np.float32)


def train_model(
    cfg: PipelineConfig,
    X: np.ndarray,
    y_ret: np.ndarray,
    y_dir: np.ndarray,
) -> Tuple[MultiTaskTransformer, Dict[str, float], Dict[str, np.ndarray]]:
    train_idx, val_idx, test_idx = split_indices(len(X))
    X_std, scaler_mean, scaler_std = standardize_sequences(X, train_idx)

    # Recency weighting in training to adapt faster to newer regimes.
    train_weights = np.linspace(0.6, 1.4, len(train_idx), dtype=np.float32)

    ds_train = SequenceDataset(X_std[train_idx], y_ret[train_idx], y_dir[train_idx], train_weights)
    ds_val = SequenceDataset(X_std[val_idx], y_ret[val_idx], y_dir[val_idx])
    ds_test = SequenceDataset(X_std[test_idx], y_ret[test_idx], y_dir[test_idx])

    dl_train = DataLoader(ds_train, batch_size=cfg.batch_size, shuffle=False)
    dl_val = DataLoader(ds_val, batch_size=cfg.batch_size, shuffle=False)
    dl_test = DataLoader(ds_test, batch_size=cfg.batch_size, shuffle=False)

    model = MultiTaskTransformer(
        input_dim=X.shape[-1],
        seq_len=cfg.seq_len,
        d_model=cfg.d_model,
        n_heads=cfg.n_heads,
        n_layers=cfg.n_layers,
        ff_dim=cfg.ff_dim,
        dropout=cfg.dropout,
        out_dim=y_ret.shape[-1],
    )

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    model = model.to(device)

    optimizer = torch.optim.AdamW(model.parameters(), lr=cfg.learning_rate, weight_decay=cfg.weight_decay)
    mse = nn.MSELoss(reduction="none")
    bce = nn.BCEWithLogitsLoss(reduction="none")

    best_state = None
    best_val = float("inf")
    patience = 0

    for epoch in range(cfg.epochs):
        model.train()
        train_losses = []
        for xb, yb_ret, yb_dir, w in dl_train:
            xb = xb.to(device)
            yb_ret = yb_ret.to(device)
            yb_dir = yb_dir.to(device)
            w = w.to(device)

            optimizer.zero_grad()
            pred_ret, pred_dir_logits = model(xb)

            loss_ret = mse(pred_ret, yb_ret).mean(dim=1)
            loss_dir = bce(pred_dir_logits, yb_dir).mean(dim=1)
            loss = ((loss_ret + loss_dir) * w).mean()
            loss.backward()
            torch.nn.utils.clip_grad_norm_(model.parameters(), max_norm=1.0)
            optimizer.step()
            train_losses.append(loss.item())

        model.eval()
        val_losses = []
        with torch.no_grad():
            for xb, yb_ret, yb_dir, _ in dl_val:
                xb = xb.to(device)
                yb_ret = yb_ret.to(device)
                yb_dir = yb_dir.to(device)
                pred_ret, pred_dir_logits = model(xb)
                loss_ret = mse(pred_ret, yb_ret).mean(dim=1)
                loss_dir = bce(pred_dir_logits, yb_dir).mean(dim=1)
                val_losses.append((loss_ret + loss_dir).mean().item())

        avg_val = float(np.mean(val_losses))
        avg_train = float(np.mean(train_losses))
        if avg_val < best_val:
            best_val = avg_val
            best_state = {k: v.detach().cpu().clone() for k, v in model.state_dict().items()}
            patience = 0
        else:
            patience += 1

        if epoch % 10 == 0 or epoch == cfg.epochs - 1:
            print(f"epoch={epoch:03d} train={avg_train:.5f} val={avg_val:.5f}")

        if patience >= cfg.early_stopping_patience:
            print(f"early stopping at epoch {epoch}")
            break

    if best_state is None:
        raise RuntimeError("Training failed to produce a valid model state")

    model.load_state_dict(best_state)

    # Evaluate test set aggregate losses.
    model.eval()
    test_losses = []
    all_ret_pred = []
    all_ret_true = []
    all_dir_prob = []
    all_dir_true = []

    with torch.no_grad():
        for xb, yb_ret, yb_dir, _ in dl_test:
            xb = xb.to(device)
            yb_ret = yb_ret.to(device)
            yb_dir = yb_dir.to(device)
            pred_ret, pred_dir_logits = model(xb)

            loss_ret = mse(pred_ret, yb_ret).mean(dim=1)
            loss_dir = bce(pred_dir_logits, yb_dir).mean(dim=1)
            test_losses.append((loss_ret + loss_dir).mean().item())

            all_ret_pred.append(pred_ret.cpu().numpy())
            all_ret_true.append(yb_ret.cpu().numpy())
            all_dir_prob.append(torch.sigmoid(pred_dir_logits).cpu().numpy())
            all_dir_true.append(yb_dir.cpu().numpy())

    metrics = {
        "val_loss_best": best_val,
        "test_loss": float(np.mean(test_losses)),
    }

    preds = {
        "ret_pred_test": np.concatenate(all_ret_pred, axis=0),
        "ret_true_test": np.concatenate(all_ret_true, axis=0),
        "dir_prob_test": np.concatenate(all_dir_prob, axis=0),
        "dir_true_test": np.concatenate(all_dir_true, axis=0),
        "train_idx": train_idx,
        "val_idx": val_idx,
        "test_idx": test_idx,
        "scaler_mean": scaler_mean,
        "scaler_std": scaler_std,
        "X_std": X_std,
    }
    return model, metrics, preds


def compute_per_target_metrics(
    ret_true: np.ndarray,
    ret_pred: np.ndarray,
    dir_true: np.ndarray,
    dir_prob: np.ndarray,
    ordered_targets: Sequence[str],
) -> pd.DataFrame:
    rows = []
    for j, tgt in enumerate(ordered_targets):
        parts = tgt.split("_")
        asset = parts[2]
        horizon = parts[3]

        y = ret_true[:, j]
        p = ret_pred[:, j]
        prob = dir_prob[:, j]
        d = dir_true[:, j]
        d_pred = (prob >= 0.5).astype(float)

        mae = float(np.mean(np.abs(p - y)))
        rmse = float(np.sqrt(np.mean((p - y) ** 2)))
        acc = float(np.mean((d_pred == d).astype(float)))
        corr = float(np.corrcoef(y, p)[0, 1]) if np.std(y) > 1e-10 and np.std(p) > 1e-10 else np.nan

        rows.append(
            {
                "asset": asset,
                "horizon": horizon,
                "n_test": len(y),
                "mae": mae,
                "rmse": rmse,
                "direction_accuracy": acc,
                "corr": corr,
            }
        )

    return pd.DataFrame(rows)


def build_test_predictions_detailed(
    seq_dates: pd.DatetimeIndex,
    test_idx: np.ndarray,
    ret_cols: Sequence[str],
    ret_true: np.ndarray,
    ret_pred: np.ndarray,
    dir_true: np.ndarray,
    dir_prob: np.ndarray,
) -> pd.DataFrame:
    rows = []
    for i, seq_i in enumerate(test_idx):
        asof = pd.to_datetime(seq_dates[seq_i]).date().isoformat()
        for j, tgt in enumerate(ret_cols):
            parts = tgt.split("_")
            asset = parts[2]
            horizon = parts[3]
            pred_prob = float(dir_prob[i, j])
            pred_direction = "Up" if pred_prob >= 0.5 else "Down"
            realized_direction = "Up" if float(dir_true[i, j]) >= 0.5 else "Down"
            rows.append(
                {
                    "asof_date": asof,
                    "asset": asset,
                    "horizon": horizon,
                    "pred_prob_up": pred_prob,
                    "pred_direction": pred_direction,
                    "pred_return": float(ret_pred[i, j]),
                    "realized_direction": realized_direction,
                    "realized_return": float(ret_true[i, j]),
                    "direction_correct": int(pred_direction == realized_direction),
                    "abs_error": float(abs(ret_pred[i, j] - ret_true[i, j])),
                }
            )
    return pd.DataFrame(rows)


def save_model_bundle(
    cfg: PipelineConfig,
    model: MultiTaskTransformer,
    feature_cols: Sequence[str],
    ret_cols: Sequence[str],
    dir_cols: Sequence[str],
    scaler_mean: np.ndarray,
    scaler_std: np.ndarray,
) -> Path:
    model_path = cfg.model_dir / "transformer_latest.pt"
    torch.save(
        {
            "model_state_dict": model.state_dict(),
            "feature_cols": list(feature_cols),
            "ret_cols": list(ret_cols),
            "dir_cols": list(dir_cols),
            "scaler_mean": scaler_mean,
            "scaler_std": scaler_std,
            "seq_len": cfg.seq_len,
            "d_model": cfg.d_model,
            "n_heads": cfg.n_heads,
            "n_layers": cfg.n_layers,
            "ff_dim": cfg.ff_dim,
            "dropout": cfg.dropout,
            "saved_at": datetime.now(timezone.utc).isoformat(),
        },
        model_path,
    )
    return model_path


def add_trading_days(trading_dates: pd.DatetimeIndex, start_dt: pd.Timestamp, n_days: int) -> Optional[pd.Timestamp]:
    pos = trading_dates.searchsorted(start_dt)
    if pos >= len(trading_dates):
        return pd.Timestamp(start_dt + BDay(n_days)).normalize()
    target_pos = pos + n_days
    if target_pos >= len(trading_dates):
        return pd.Timestamp(start_dt + BDay(n_days)).normalize()
    return trading_dates[target_pos]


def generate_latest_predictions(
    cfg: PipelineConfig,
    model: MultiTaskTransformer,
    latest_window_std: np.ndarray,
    asof_date: pd.Timestamp,
    ret_cols: Sequence[str],
    trading_dates: pd.DatetimeIndex,
) -> pd.DataFrame:
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    model = model.to(device)
    model.eval()

    latest_x = torch.tensor(latest_window_std[np.newaxis, :, :], dtype=torch.float32, device=device)
    with torch.no_grad():
        pred_ret, pred_dir_logits = model(latest_x)
        pred_ret = pred_ret.cpu().numpy().reshape(-1)
        pred_prob = torch.sigmoid(pred_dir_logits).cpu().numpy().reshape(-1)

    run_ts = pd.Timestamp.now(tz="UTC").floor("s")
    asof_date = asof_date.normalize()

    rows = []
    for j, tgt in enumerate(ret_cols):
        parts = tgt.split("_")
        asset = parts[2]
        horizon = parts[3]
        h_days = HORIZONS[horizon]
        dir_sign = 1.0 if pred_prob[j] >= 0.5 else -1.0
        pred_ret_j = float(pred_ret[j])

        target_dt = add_trading_days(trading_dates, asof_date, h_days)
        rows.append(
            {
                "run_timestamp_utc": run_ts.isoformat(),
                "asof_date": asof_date.date().isoformat(),
                "asset": asset,
                "horizon": horizon,
                "horizon_days": h_days,
                "pred_return": pred_ret_j,
                "pred_return_aligned": dir_sign * abs(pred_ret_j),
                "pred_prob_up": float(pred_prob[j]),
                "direction_confidence": float(abs(pred_prob[j] - 0.5) * 2.0),
                "pred_direction": "Up" if pred_prob[j] >= 0.5 else "Down",
                "target_date": "" if target_dt is None else target_dt.date().isoformat(),
                "prediction_id": f"{run_ts.strftime('%Y%m%dT%H%M%S')}_{asset}_{horizon}",
            }
        )

    latest_df = pd.DataFrame(rows).sort_values(["asset", "horizon"]).reset_index(drop=True)
    latest_df.to_csv(cfg.reports_dir / "latest_predictions.csv", index=False)

    history_path = cfg.reports_dir / "prediction_history.csv"
    if history_path.exists():
        prev = pd.read_csv(history_path)
        hist = pd.concat([prev, latest_df], ignore_index=True)
    else:
        hist = latest_df
    hist.to_csv(history_path, index=False)
    return latest_df


def score_matured_predictions(cfg: PipelineConfig, market_df: pd.DataFrame) -> pd.DataFrame:
    history_path = cfg.reports_dir / "prediction_history.csv"
    if not history_path.exists():
        return pd.DataFrame()

    hist = pd.read_csv(history_path)
    if hist.empty:
        return pd.DataFrame()

    scores_path = cfg.reports_dir / "prediction_scores.csv"
    existing_ids = set()
    if scores_path.exists():
        prev_scores = pd.read_csv(scores_path)
        if not prev_scores.empty:
            existing_ids = set(prev_scores["prediction_id"].astype(str).tolist())
    else:
        prev_scores = pd.DataFrame()

    px = market_df.copy()
    px["Date"] = pd.to_datetime(px["Date"])
    px = px.set_index("Date").sort_index()
    last_date = px.index.max()

    rows = []
    for _, r in hist.iterrows():
        pred_id = str(r["prediction_id"])
        if pred_id in existing_ids:
            continue
        asset = str(r["asset"])
        if asset not in px.columns:
            continue

        asof = pd.to_datetime(r["asof_date"], errors="coerce")
        if pd.isna(asof):
            continue

        if pd.notna(r.get("horizon_days")):
            h_days = int(r["horizon_days"])
        else:
            horizon = str(r.get("horizon", "1m"))
            h_days = int(HORIZONS.get(horizon, 21))

        asof_pos = px.index.searchsorted(asof)
        if asof_pos >= len(px.index):
            continue
        asof_ix = px.index[asof_pos]
        target_pos = asof_pos + h_days
        if target_pos >= len(px.index):
            continue
        target_ix = px.index[target_pos]
        if target_ix > last_date:
            continue

        p0 = float(px.loc[asof_ix, asset])
        p1 = float(px.loc[target_ix, asset])
        realized_ret = p1 / p0 - 1.0
        realized_dir = "Up" if realized_ret > 0 else "Down"
        pred_ret = float(r["pred_return"])
        pred_dir = str(r["pred_direction"])

        rows.append(
            {
                "prediction_id": pred_id,
                "asset": asset,
                "horizon": r["horizon"],
                "asof_date": asof_ix.date().isoformat(),
                "target_date": target_ix.date().isoformat(),
                "pred_return": pred_ret,
                "pred_direction": pred_dir,
                "realized_return": realized_ret,
                "realized_direction": realized_dir,
                "direction_correct": int(pred_dir == realized_dir),
                "abs_error": abs(pred_ret - realized_ret),
            }
        )

    new_scores = pd.DataFrame(rows)
    if new_scores.empty:
        return prev_scores if isinstance(prev_scores, pd.DataFrame) else pd.DataFrame()

    if isinstance(prev_scores, pd.DataFrame) and not prev_scores.empty:
        out = pd.concat([prev_scores, new_scores], ignore_index=True)
    else:
        out = new_scores

    out.to_csv(scores_path, index=False)
    return out


def parse_bls_date(s: str) -> Optional[datetime]:
    s = s.strip().replace(".", "")
    for fmt in ["%b %d, %Y", "%B %d, %Y"]:
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    return None


def fetch_bls_release_calendar(url: str, event_group: str, event_name: str) -> List[Dict[str, str]]:
    html = get_text_with_retries(url, timeout=40, retries=5)
    soup = BeautifulSoup(html, "lxml")
    tables = soup.find_all("table")
    if len(tables) < 2:
        return []

    rows: List[Dict[str, str]] = []
    for tr in tables[1].find_all("tr"):
        cells = [c.get_text(" ", strip=True) for c in tr.find_all(["td", "th"])]
        if len(cells) < 3 or cells[0] == "Reference Month":
            continue

        dt = parse_bls_date(cells[1])
        if dt is None:
            continue
        rows.append(
            {
                "event_group": event_group,
                "event_name": event_name,
                "reference_period": cells[0],
                "release_datetime_et": f"{dt.date().isoformat()} {cells[2]}",
                "release_date": dt.date().isoformat(),
                "source": "BLS",
                "source_url": url,
            }
        )

    return rows


def parse_bea_datetime(text: str, year: int) -> Optional[datetime]:
    s = " ".join(text.split())
    for fmt in ["%B %d %I:%M %p %Y", "%b %d %I:%M %p %Y"]:
        try:
            return datetime.strptime(f"{s} {year}", fmt)
        except ValueError:
            continue
    return None


def fetch_bea_release_calendar(url: str) -> List[Dict[str, str]]:
    html = get_text_with_retries(url, timeout=40, retries=5)
    soup = BeautifulSoup(html, "lxml")
    table = soup.find("table")
    if table is None:
        return []

    rows: List[Dict[str, str]] = []
    current_year = None

    for tr in table.find_all("tr"):
        cells = [c.get_text(" ", strip=True) for c in tr.find_all(["td", "th"])]
        if not cells:
            continue

        if cells[0].startswith("Year "):
            m = re.search(r"Year\s+(\d{4})", cells[0])
            if m:
                current_year = int(m.group(1))
            continue

        if current_year is None or len(cells) < 3:
            continue

        dt = parse_bea_datetime(cells[0], current_year)
        if dt is None:
            continue

        rows.append(
            {
                "event_group": "National Accounts",
                "event_name": cells[2],
                "reference_period": "",
                "release_datetime_et": dt.strftime("%Y-%m-%d %I:%M %p"),
                "release_date": dt.date().isoformat(),
                "source": "BEA",
                "source_url": url,
            }
        )

    return rows


def parse_fomc_month_day(month: str, day: str, year: int) -> Optional[date]:
    month_map = {
        "January": 1,
        "February": 2,
        "March": 3,
        "April": 4,
        "May": 5,
        "June": 6,
        "July": 7,
        "August": 8,
        "September": 9,
        "October": 10,
        "November": 11,
        "December": 12,
        "Jan": 1,
        "Feb": 2,
        "Mar": 3,
        "Apr": 4,
        "May": 5,
        "Jun": 6,
        "Jul": 7,
        "Aug": 8,
        "Sep": 9,
        "Oct": 10,
        "Nov": 11,
        "Dec": 12,
    }

    if "/" in month:
        month = month.split("/")[0]
    m_num = month_map.get(month)
    if m_num is None:
        return None
    try:
        return date(year, m_num, int(day))
    except ValueError:
        return None


def fetch_fomc_calendar(url: str, years: Sequence[int]) -> List[Dict[str, str]]:
    html = get_text_with_retries(url, timeout=40, retries=5)
    soup = BeautifulSoup(html, "lxml")
    text = "\n".join(" ".join(t.split()) for t in soup.get_text("\n").splitlines() if t.strip())

    rows: List[Dict[str, str]] = []

    for year in years:
        section_pattern = re.compile(
            rf"{year}\s+FOMC Meetings(.*?)(?:\n\s*{year - 1}\s+FOMC Meetings|\n\s*{year + 1}\s+FOMC Meetings|\n\s*Note:\s|$)",
            flags=re.DOTALL,
        )
        sec_match = section_pattern.search(text)
        if not sec_match:
            continue
        section = sec_match.group(1)

        # Match entries like "March 17-18*" or "Oct/Nov 31-1".
        for m in re.finditer(
            r"(January|February|March|April|May|June|July|August|September|October|November|December|"
            r"Jan|Feb|Mar|Apr|Jun|Jul|Aug|Sep|Oct|Nov|Dec|Jan/Feb|Apr/May|Oct/Nov)\s+(\d{1,2})-(\d{1,2})\*?",
            section,
        ):
            month_token = m.group(1)
            start_day = m.group(2)
            end_day = m.group(3)
            start_dt = parse_fomc_month_day(month_token, start_day, year)
            if start_dt is None:
                continue
            rows.append(
                {
                    "event_group": "Central Bank",
                    "event_name": f"FOMC Meeting ({month_token} {start_day}-{end_day})",
                    "reference_period": "",
                    "release_datetime_et": f"{start_dt.isoformat()} 02:00 PM",
                    "release_date": start_dt.isoformat(),
                    "source": "Federal Reserve",
                    "source_url": url,
                }
            )

    # Deduplicate (year section text can repeat in page chrome).
    dedup: Dict[Tuple[str, str], Dict[str, str]] = {}
    for r in rows:
        key = (r["event_name"], r["release_date"])
        dedup[key] = r
    return list(dedup.values())


def build_release_calendar(cfg: PipelineConfig) -> pd.DataFrame:
    today = cfg.end_date

    rows: List[Dict[str, str]] = []
    rows.extend(
        fetch_bls_release_calendar(
            "https://www.bls.gov/schedule/news_release/cpi.htm",
            event_group="Inflation",
            event_name="Consumer Price Index (CPI)",
        )
    )
    rows.extend(
        fetch_bls_release_calendar(
            "https://www.bls.gov/schedule/news_release/empsit.htm",
            event_group="Labor",
            event_name="Employment Situation (Payrolls/Unemployment)",
        )
    )
    rows.extend(fetch_bea_release_calendar("https://www.bea.gov/news/schedule"))
    rows.extend(fetch_fomc_calendar("https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm", [today.year, today.year + 1]))

    cal = pd.DataFrame(rows)
    if cal.empty:
        cal.to_csv(cfg.reports_dir / "macro_release_calendar.csv", index=False)
        return cal

    cal["release_date"] = pd.to_datetime(cal["release_date"], errors="coerce")
    cal = cal.dropna(subset=["release_date"])
    cal = cal[cal["release_date"].dt.date >= today].copy()
    cal = cal.sort_values(["release_date", "event_group", "event_name"]).reset_index(drop=True)
    cal["release_date"] = cal["release_date"].dt.date.astype(str)

    cal.to_csv(cfg.reports_dir / "macro_release_calendar.csv", index=False)
    return cal


def run_pipeline(cfg: PipelineConfig) -> None:
    ensure_dirs(cfg)
    set_global_seed(cfg.random_seed)

    print("[1/7] Collecting market data...")
    market_df = collect_market_data(cfg)

    print("[2/7] Collecting macro data...")
    trading_dates = pd.DatetimeIndex(pd.to_datetime(market_df["Date"]))
    macro_df = collect_macro_data(cfg, trading_dates=trading_dates)

    print("[3/7] Building feature/target store...")
    features = build_feature_frame(market_df, macro_df)
    targets = build_target_frame(market_df)

    ret_cols = [f"target_ret_{asset}_{h}" for asset in ASSET_TO_STOOQ for h in HORIZONS]
    dir_cols = [f"target_dir_{asset}_{h}" for asset in ASSET_TO_STOOQ for h in HORIZONS]

    X, y_ret, y_dir, seq_dates = make_sequences(
        features=features,
        targets=targets,
        seq_len=cfg.seq_len,
        ret_cols=ret_cols,
        dir_cols=dir_cols,
    )

    print(f"dataset sequences={len(X)} feature_dim={X.shape[-1]} target_dim={y_ret.shape[-1]}")
    if len(X) < 300:
        raise RuntimeError("Too few sequences to train a stable model")

    print("[4/7] Training transformer model...")
    model, agg_metrics, train_artifacts = train_model(cfg, X, y_ret, y_dir)

    print("[5/7] Evaluating and saving model artifacts...")
    metrics_df = compute_per_target_metrics(
        ret_true=train_artifacts["ret_true_test"],
        ret_pred=train_artifacts["ret_pred_test"],
        dir_true=train_artifacts["dir_true_test"],
        dir_prob=train_artifacts["dir_prob_test"],
        ordered_targets=ret_cols,
    )
    metrics_df.to_csv(cfg.reports_dir / "test_metrics.csv", index=False)
    test_pred_detail_df = build_test_predictions_detailed(
        seq_dates=seq_dates,
        test_idx=train_artifacts["test_idx"],
        ret_cols=ret_cols,
        ret_true=train_artifacts["ret_true_test"],
        ret_pred=train_artifacts["ret_pred_test"],
        dir_true=train_artifacts["dir_true_test"],
        dir_prob=train_artifacts["dir_prob_test"],
    )
    test_pred_detail_df.to_csv(cfg.reports_dir / "test_predictions_detailed.csv", index=False)

    feature_cols = [c for c in features.columns if c != "Date"]
    model_path = save_model_bundle(
        cfg=cfg,
        model=model,
        feature_cols=feature_cols,
        ret_cols=ret_cols,
        dir_cols=dir_cols,
        scaler_mean=train_artifacts["scaler_mean"],
        scaler_std=train_artifacts["scaler_std"],
    )

    print("[6/7] Generating latest predictions and scoring history...")
    latest_window = features[feature_cols].values.astype(np.float32)[-cfg.seq_len :]
    latest_window_std = (latest_window - train_artifacts["scaler_mean"]) / train_artifacts["scaler_std"]
    asof_date = pd.to_datetime(features["Date"].iloc[-1])
    latest_preds = generate_latest_predictions(
        cfg=cfg,
        model=model,
        latest_window_std=latest_window_std,
        asof_date=asof_date,
        ret_cols=ret_cols,
        trading_dates=trading_dates,
    )
    score_df = score_matured_predictions(cfg, market_df)

    print("[7/7] Building macro release calendar...")
    cal_df = build_release_calendar(cfg)

    summary = {
        "run_date": cfg.end_date.isoformat(),
        "start_date": cfg.start_date.isoformat(),
        "num_assets": len(ASSET_TO_STOOQ),
        "num_macro_series_requested": len(FRED_SERIES),
        "num_sequences": int(len(X)),
        "aggregate_metrics": agg_metrics,
        "model_path": str(model_path),
        "latest_prediction_rows": int(len(latest_preds)),
        "scored_predictions_rows": int(len(score_df)),
        "test_prediction_detail_rows": int(len(test_pred_detail_df)),
        "calendar_rows": int(len(cal_df)),
    }

    with open(cfg.reports_dir / "run_summary.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print("Done.")
    print(json.dumps(summary, indent=2))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Macro transformer forecasting pipeline")
    parser.add_argument(
        "--base-dir",
        default="data/macro_transformer",
        help="Output base directory",
    )
    parser.add_argument(
        "--end-date",
        default=date.today().isoformat(),
        help="End date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--years",
        type=int,
        default=5,
        help="Lookback window in years",
    )
    parser.add_argument("--epochs", type=int, default=80)
    parser.add_argument("--seq-len", type=int, default=60)
    parser.add_argument("--batch-size", type=int, default=64)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    end_date = datetime.strptime(args.end_date, "%Y-%m-%d").date()
    start_date = end_date - timedelta(days=int(args.years * 365.25))

    cfg = PipelineConfig(
        base_dir=Path(args.base_dir),
        start_date=start_date,
        end_date=end_date,
        seq_len=args.seq_len,
        epochs=args.epochs,
        batch_size=args.batch_size,
    )

    run_pipeline(cfg)


if __name__ == "__main__":
    main()
