#!/usr/bin/env python3
"""Shared data loading + metrics helpers for the asset-trend walk-forward study."""
import os, glob, json
import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
PRICE_DIR = os.path.join(HERE, "data", "prices")
UNIVERSE_DIR = os.path.join(HERE, "..", "asset-universe")
TRADING_DAYS = 252


def load_prices(min_obs=400):
    """Load all per-ticker CSVs into a wide adjclose matrix on a business-day index."""
    frames = {}
    for path in glob.glob(os.path.join(PRICE_DIR, "*.csv")):
        t = os.path.splitext(os.path.basename(path))[0]
        if t.startswith("_"):
            continue
        df = pd.read_csv(path, parse_dates=["date"]).set_index("date")["adjclose"]
        df = df[~df.index.duplicated(keep="last")]
        if df.dropna().shape[0] >= min_obs:
            frames[t] = df
    wide = pd.DataFrame(frames).sort_index()
    # Align to business days, forward-fill small gaps (holidays handled by Yahoo already).
    wide = wide.asfreq("B").ffill(limit=3)
    return wide


def load_volume(price_index, cols):
    """Load raw close + volume into wide frames aligned to the price index/columns."""
    vol_dir = os.path.join(HERE, "data", "volume")
    closes, vols = {}, {}
    for c in cols:
        path = os.path.join(vol_dir, f"{c}.csv")
        if not os.path.exists(path):
            continue
        df = pd.read_csv(path, parse_dates=["date"]).set_index("date")
        df = df[~df.index.duplicated(keep="last")]
        closes[c] = df["close"]
        vols[c] = df["volume"]
    rawclose = pd.DataFrame(closes).reindex(price_index).ffill(limit=3)
    volume = pd.DataFrame(vols).reindex(price_index).ffill(limit=3)
    return rawclose, volume


def asset_class_map():
    """Map primary ticker -> Asset Class using the universe CSV."""
    import csv
    csv_path = os.path.join(UNIVERSE_DIR, "asset_universe.csv")
    m = {}
    for r in csv.DictReader(open(csv_path)):
        ex = (r.get("Examples") or "").split(",")[0].strip().lstrip("~").rstrip("*").strip()
        ex = ex.split()[0].upper() if ex and ex != "-" else ""
        if ex and ex not in m:
            m[ex] = r["Asset Class"]
    return m


def perf_stats(monthly_ret, rf_monthly=None, periods=12):
    """Annualized performance stats from a monthly return series."""
    r = monthly_ret.dropna()
    if len(r) < 2:
        return {}
    if rf_monthly is None:
        rf_monthly = pd.Series(0.0, index=r.index)
    rf_monthly = rf_monthly.reindex(r.index).fillna(0.0)
    excess = r - rf_monthly
    ann_ret = (1 + r).prod() ** (periods / len(r)) - 1
    ann_vol = r.std(ddof=1) * np.sqrt(periods)
    ann_excess = excess.mean() * periods
    sharpe = ann_excess / ann_vol if ann_vol > 0 else np.nan
    downside = r[r < 0].std(ddof=1) * np.sqrt(periods)
    sortino = ann_excess / downside if downside and downside > 0 else np.nan
    equity = (1 + r).cumprod()
    dd = (equity / equity.cummax() - 1).min()
    return {
        "months": len(r),
        "cagr": ann_ret,
        "vol": ann_vol,
        "sharpe": sharpe,
        "sortino": sortino,
        "max_dd": dd,
        "hit_rate": (r > 0).mean(),
    }


def fmt_stats(name, s):
    return (f"{name:<28} n={s.get('months',0):>3}  CAGR={s.get('cagr',0)*100:6.1f}%  "
            f"Vol={s.get('vol',0)*100:5.1f}%  Sharpe={s.get('sharpe',float('nan')):5.2f}  "
            f"Sortino={s.get('sortino',float('nan')):5.2f}  MaxDD={s.get('max_dd',0)*100:6.1f}%  "
            f"Hit={s.get('hit_rate',0)*100:4.1f}%")
