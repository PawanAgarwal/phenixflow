#!/usr/bin/env python3
"""Blend macro-transformer forecasts with live ETF options + sigScore-style components.

Outputs:
- data/macro_transformer/reports/options_sigscore_components.csv
- data/macro_transformer/reports/refined_predictions.csv
- data/macro_transformer/reports/spy_nasdaq_divergence_backtest.csv
"""

from __future__ import annotations

import argparse
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Sequence

import numpy as np
import pandas as pd

try:
    import yfinance as yf
except ImportError as exc:  # pragma: no cover
    raise SystemExit(
        "Missing dependency: yfinance. Install with `pip install yfinance`."
    ) from exc


HORIZONS = {"1w": 5, "1m": 21, "3m": 63}

ETF_TO_ASSET = {
    "SPY": "SP500",
    "QQQ": "NASDAQ",
    "XLC": "CommunicationServices",
    "XLY": "ConsumerDiscretionary",
    "XLP": "ConsumerStaples",
    "XLE": "Energy",
    "XLF": "Financials",
    "XLV": "HealthCare",
    "XLI": "Industrials",
    "XLK": "InformationTechnology",
    "XLB": "Materials",
    "XLRE": "RealEstate",
    "XLU": "Utilities",
}


@dataclass
class Paths:
    reports_dir: Path
    latest_predictions_csv: Path
    test_predictions_detailed_csv: Path
    options_components_csv: Path
    refined_predictions_csv: Path
    divergence_summary_csv: Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Options + sigScore refiner for macro forecasts")
    parser.add_argument(
        "--reports-dir",
        default="data/macro_transformer/reports",
        help="Directory holding macro pipeline report files",
    )
    parser.add_argument(
        "--max-expiries-per-ticker",
        type=int,
        default=10,
        help="Maximum number of expiries fetched per ETF ticker",
    )
    parser.add_argument(
        "--max-dte",
        type=int,
        default=190,
        help="Maximum days-to-expiry considered",
    )
    return parser.parse_args()


def sigmoid(x: float) -> float:
    if x >= 0:
        z = math.exp(-x)
        return 1.0 / (1.0 + z)
    z = math.exp(x)
    return z / (1.0 + z)


def sanitize_numeric(s: pd.Series, fill: float = 0.0) -> pd.Series:
    out = pd.to_numeric(s, errors="coerce")
    return out.fillna(fill)


def resolve_paths(args: argparse.Namespace) -> Paths:
    reports_dir = Path(args.reports_dir)
    return Paths(
        reports_dir=reports_dir,
        latest_predictions_csv=reports_dir / "latest_predictions.csv",
        test_predictions_detailed_csv=reports_dir / "test_predictions_detailed.csv",
        options_components_csv=reports_dir / "options_sigscore_components.csv",
        refined_predictions_csv=reports_dir / "refined_predictions.csv",
        divergence_summary_csv=reports_dir / "spy_nasdaq_divergence_backtest.csv",
    )


def fetch_ticker_history(ticker: str, period: str = "9mo") -> pd.DataFrame:
    hist = yf.Ticker(ticker).history(period=period, auto_adjust=False)
    if hist is None or hist.empty:
        return pd.DataFrame()
    out = hist.reset_index()
    out["Date"] = pd.to_datetime(out["Date"]).dt.tz_localize(None)
    return out


def fetch_option_chain_snapshot(
    ticker: str,
    asof_date: pd.Timestamp,
    max_expiries: int = 10,
    max_dte: int = 190,
) -> pd.DataFrame:
    t = yf.Ticker(ticker)
    expiries = list(t.options or [])
    if not expiries:
        return pd.DataFrame()

    exp_dates = []
    for e in expiries:
        dt = pd.to_datetime(e, errors="coerce")
        if pd.isna(dt):
            continue
        dte = int((dt.normalize() - asof_date.normalize()).days)
        if dte <= 1 or dte > max_dte:
            continue
        exp_dates.append((e, dt.normalize(), dte))
    exp_dates = sorted(exp_dates, key=lambda x: x[1])[:max_expiries]
    if not exp_dates:
        return pd.DataFrame()

    rows: List[pd.DataFrame] = []
    for exp_str, exp_dt, dte in exp_dates:
        try:
            chain = t.option_chain(exp_str)
        except Exception:
            continue
        for side_name, side_df in [("call", chain.calls), ("put", chain.puts)]:
            if side_df is None or side_df.empty:
                continue
            df = side_df.copy()
            df["ticker"] = ticker
            df["expiration"] = exp_dt
            df["dte"] = dte
            df["option_type"] = side_name
            rows.append(df)

    if not rows:
        return pd.DataFrame()

    out = pd.concat(rows, ignore_index=True)
    out["strike"] = sanitize_numeric(out.get("strike", pd.Series(dtype=float)))
    out["volume"] = sanitize_numeric(out.get("volume", pd.Series(dtype=float)))
    out["openInterest"] = sanitize_numeric(out.get("openInterest", pd.Series(dtype=float)))
    out["impliedVolatility"] = sanitize_numeric(out.get("impliedVolatility", pd.Series(dtype=float)))
    out["bid"] = sanitize_numeric(out.get("bid", pd.Series(dtype=float)))
    out["ask"] = sanitize_numeric(out.get("ask", pd.Series(dtype=float)))
    out["lastPrice"] = sanitize_numeric(out.get("lastPrice", pd.Series(dtype=float)))
    out["mid"] = (out["bid"] + out["ask"]) / 2.0
    out.loc[out["mid"] <= 0, "mid"] = out.loc[out["mid"] <= 0, "lastPrice"]
    out["spread_rel"] = (out["ask"] - out["bid"]).clip(lower=0) / out["mid"].replace(0, np.nan)
    out["spread_rel"] = out["spread_rel"].replace([np.inf, -np.inf], np.nan)
    return out


def select_expiry_for_horizon(options_df: pd.DataFrame, horizon_days: int) -> Optional[pd.Timestamp]:
    if options_df.empty:
        return None
    dte_by_exp = options_df.groupby("expiration")["dte"].first().reset_index()
    min_acceptable = max(3, int(horizon_days * 0.6))
    dte_by_exp["penalty"] = np.abs(dte_by_exp["dte"] - horizon_days)
    dte_by_exp.loc[dte_by_exp["dte"] < min_acceptable, "penalty"] += 1000
    chosen = dte_by_exp.sort_values("penalty").head(1)
    if chosen.empty:
        return None
    return pd.to_datetime(chosen["expiration"].iloc[0])


def atm_row(df: pd.DataFrame, spot: float) -> Optional[pd.Series]:
    if df.empty:
        return None
    ix = (df["strike"] - spot).abs().idxmin()
    if pd.isna(ix):
        return None
    return df.loc[ix]


def compute_term_slope(options_df: pd.DataFrame, spot: float) -> float:
    if options_df.empty:
        return 0.0
    rows = []
    for exp, grp in options_df.groupby("expiration"):
        dte = int(grp["dte"].iloc[0])
        calls = grp[grp["option_type"] == "call"]
        puts = grp[grp["option_type"] == "put"]
        c_atm = atm_row(calls, spot)
        p_atm = atm_row(puts, spot)
        if c_atm is None or p_atm is None:
            continue
        iv = np.nanmean([float(c_atm.get("impliedVolatility", np.nan)), float(p_atm.get("impliedVolatility", np.nan))])
        if not np.isfinite(iv) or iv <= 0:
            continue
        rows.append((dte, iv))
    if len(rows) < 2:
        return 0.0
    curve = pd.DataFrame(rows, columns=["dte", "atm_iv"]).sort_values("dte")
    near = curve[(curve["dte"] >= 5) & (curve["dte"] <= 21)]
    far = curve[(curve["dte"] >= 45) & (curve["dte"] <= 120)]
    if near.empty or far.empty:
        near = curve.head(2)
        far = curve.tail(2)
    return float(near["atm_iv"].mean() - far["atm_iv"].mean())


def compute_trend_signal(hist: pd.DataFrame, horizon_days: int) -> float:
    if hist.empty or len(hist) < 3:
        return 0.0
    closes = pd.to_numeric(hist["Close"], errors="coerce").dropna().values
    if len(closes) < 3:
        return 0.0
    k = min(len(closes) - 1, max(2, horizon_days))
    return float(closes[-1] / closes[-1 - k] - 1.0)


def compute_horizon_option_components(
    ticker: str,
    asset: str,
    asof_date: pd.Timestamp,
    spot: float,
    hist: pd.DataFrame,
    options_df: pd.DataFrame,
    horizon: str,
    horizon_days: int,
    term_slope: float,
) -> Dict[str, object]:
    result = {
        "asof_date": asof_date.date().isoformat(),
        "ticker": ticker,
        "asset": asset,
        "horizon": horizon,
        "horizon_days": horizon_days,
        "spot": spot,
        "expiry_used": None,
        "expiry_dte": np.nan,
        "flow_imbalance": np.nan,
        "oi_imbalance": np.nan,
        "delta_pressure": np.nan,
        "iv_skew_surface": np.nan,
        "risk_reversal": np.nan,
        "term_slope": term_slope,
        "liquidity_quality": np.nan,
        "dte_alignment": np.nan,
        "expected_move_abs": np.nan,
        "options_prob_up": np.nan,
        "options_direction": None,
        "options_confidence": np.nan,
        "options_signed_return": np.nan,
    }
    if options_df.empty or not np.isfinite(spot) or spot <= 0:
        return result

    exp = select_expiry_for_horizon(options_df, horizon_days)
    if exp is None:
        return result
    grp = options_df[options_df["expiration"] == exp].copy()
    if grp.empty:
        return result

    calls = grp[grp["option_type"] == "call"].copy()
    puts = grp[grp["option_type"] == "put"].copy()
    if calls.empty or puts.empty:
        return result

    dte = int(grp["dte"].iloc[0])
    total_call_vol = float(calls["volume"].sum())
    total_put_vol = float(puts["volume"].sum())
    total_call_oi = float(calls["openInterest"].sum())
    total_put_oi = float(puts["openInterest"].sum())
    total_vol = total_call_vol + total_put_vol
    total_oi = total_call_oi + total_put_oi
    flow_imbalance = (total_call_vol - total_put_vol) / (total_vol + 1e-9)
    oi_imbalance = (total_call_oi - total_put_oi) / (total_oi + 1e-9)

    grp["moneyness_abs"] = (grp["strike"] / spot - 1.0).abs()
    grp["w"] = np.exp(-grp["moneyness_abs"] / 0.10) * np.sqrt(grp["openInterest"].clip(lower=0) + 1.0)
    grp["sign"] = np.where(grp["option_type"] == "call", 1.0, -1.0)
    delta_pressure = float((grp["sign"] * grp["w"]).sum() / (grp["w"].sum() + 1e-9))

    puts_otm = puts[(puts["strike"] <= spot * 0.98) & (puts["strike"] >= spot * 0.85)]
    calls_otm = calls[(calls["strike"] >= spot * 1.02) & (calls["strike"] <= spot * 1.15)]
    put_iv = float(puts_otm["impliedVolatility"].replace(0, np.nan).mean())
    call_iv = float(calls_otm["impliedVolatility"].replace(0, np.nan).mean())
    if not np.isfinite(put_iv):
        put_iv = float(puts["impliedVolatility"].replace(0, np.nan).mean())
    if not np.isfinite(call_iv):
        call_iv = float(calls["impliedVolatility"].replace(0, np.nan).mean())
    if not np.isfinite(put_iv):
        put_iv = 0.0
    if not np.isfinite(call_iv):
        call_iv = 0.0
    iv_skew_surface = put_iv - call_iv

    c_atm = atm_row(calls, spot)
    p_atm = atm_row(puts, spot)
    if c_atm is None or p_atm is None:
        return result
    c_mid = float(c_atm.get("mid", np.nan))
    p_mid = float(p_atm.get("mid", np.nan))
    c_iv = float(c_atm.get("impliedVolatility", np.nan))
    p_iv = float(p_atm.get("impliedVolatility", np.nan))
    if not np.isfinite(c_mid):
        c_mid = 0.0
    if not np.isfinite(p_mid):
        p_mid = 0.0
    if not np.isfinite(c_iv):
        c_iv = 0.0
    if not np.isfinite(p_iv):
        p_iv = 0.0
    risk_reversal = c_iv - p_iv

    implied_move_to_exp = max(0.0, (c_mid + p_mid) / max(spot, 1e-9))
    expected_move_abs = float(implied_move_to_exp * math.sqrt(max(horizon_days, 1) / max(dte, 1)))
    expected_move_abs = float(np.clip(expected_move_abs, 0.0, 0.60))

    spread_rel = grp["spread_rel"].replace([np.inf, -np.inf], np.nan).dropna()
    median_spread = float(spread_rel.median()) if not spread_rel.empty else 0.60
    liquidity_quality = float(np.clip(1.0 - median_spread, 0.0, 1.0))
    dte_alignment = float(np.clip(1.0 - abs(dte - horizon_days) / max(horizon_days, 1), 0.0, 1.0))

    trend_signal = compute_trend_signal(hist, horizon_days)
    s_flow = math.tanh((0.6 * flow_imbalance + 0.4 * oi_imbalance) * 3.0)
    s_delta = math.tanh(delta_pressure * 2.8)
    s_skew = math.tanh(-iv_skew_surface * 4.5)
    s_rr = math.tanh(risk_reversal * 4.5)
    s_term = math.tanh(-term_slope * 4.0)
    s_trend = math.tanh(trend_signal * 8.0)
    direction_raw = (
        0.24 * s_flow
        + 0.20 * s_delta
        + 0.18 * s_skew
        + 0.12 * s_rr
        + 0.12 * s_term
        + 0.14 * s_trend
    )
    options_prob_up = float(sigmoid(1.8 * direction_raw))
    options_direction = "Up" if options_prob_up >= 0.5 else "Down"
    options_confidence = float(abs(options_prob_up - 0.5) * 2.0)
    options_confidence = float(
        np.clip(options_confidence * (0.65 + 0.35 * liquidity_quality) * (0.50 + 0.50 * dte_alignment), 0.0, 1.0)
    )
    options_signed_return = expected_move_abs if options_direction == "Up" else -expected_move_abs

    result.update(
        {
            "expiry_used": pd.to_datetime(exp).date().isoformat(),
            "expiry_dte": dte,
            "flow_imbalance": flow_imbalance,
            "oi_imbalance": oi_imbalance,
            "delta_pressure": delta_pressure,
            "iv_skew_surface": iv_skew_surface,
            "risk_reversal": risk_reversal,
            "liquidity_quality": liquidity_quality,
            "dte_alignment": dte_alignment,
            "expected_move_abs": expected_move_abs,
            "options_prob_up": options_prob_up,
            "options_direction": options_direction,
            "options_confidence": options_confidence,
            "options_signed_return": options_signed_return,
        }
    )
    return result


def build_options_component_table(
    asof_date: pd.Timestamp,
    max_expiries: int,
    max_dte: int,
) -> pd.DataFrame:
    rows: List[Dict[str, object]] = []
    for ticker, asset in ETF_TO_ASSET.items():
        hist = fetch_ticker_history(ticker, period="9mo")
        spot = float(hist["Close"].dropna().iloc[-1]) if not hist.empty else np.nan
        options_df = fetch_option_chain_snapshot(
            ticker=ticker,
            asof_date=asof_date,
            max_expiries=max_expiries,
            max_dte=max_dte,
        )
        term_slope = compute_term_slope(options_df, spot) if not options_df.empty and np.isfinite(spot) else 0.0
        for horizon, h_days in HORIZONS.items():
            rows.append(
                compute_horizon_option_components(
                    ticker=ticker,
                    asset=asset,
                    asof_date=asof_date,
                    spot=spot,
                    hist=hist,
                    options_df=options_df,
                    horizon=horizon,
                    horizon_days=h_days,
                    term_slope=term_slope,
                )
            )
    return pd.DataFrame(rows).sort_values(["asset", "horizon"]).reset_index(drop=True)


def blend_macro_and_options(macro_df: pd.DataFrame, opt_df: pd.DataFrame) -> pd.DataFrame:
    base = macro_df.copy()
    if "pred_return_aligned" in base.columns:
        macro_signed = base["pred_return_aligned"].astype(float)
    else:
        macro_signed = np.where(
            base["pred_direction"].astype(str) == "Up",
            np.abs(base["pred_return"].astype(float)),
            -np.abs(base["pred_return"].astype(float)),
        )
    base["macro_signed_return"] = macro_signed
    base["macro_confidence"] = np.where(
        base.get("direction_confidence", pd.Series(np.nan)).notna(),
        pd.to_numeric(base["direction_confidence"], errors="coerce").fillna(0.0),
        np.abs(pd.to_numeric(base["pred_prob_up"], errors="coerce").fillna(0.5) - 0.5) * 2.0,
    )

    merged = base.merge(
        opt_df[
            [
                "asset",
                "horizon",
                "options_prob_up",
                "options_direction",
                "options_confidence",
                "options_signed_return",
                "expected_move_abs",
                "ticker",
                "expiry_used",
                "expiry_dte",
                "flow_imbalance",
                "oi_imbalance",
                "delta_pressure",
                "iv_skew_surface",
                "risk_reversal",
                "term_slope",
                "liquidity_quality",
                "dte_alignment",
            ]
        ],
        on=["asset", "horizon"],
        how="left",
    )

    out_rows = []
    for _, r in merged.iterrows():
        macro_prob = float(r["pred_prob_up"])
        macro_signed_ret = float(r["macro_signed_return"])
        macro_conf = float(r["macro_confidence"])

        options_prob = r.get("options_prob_up")
        options_conf = r.get("options_confidence")
        options_signed_ret = r.get("options_signed_return")

        has_options = np.isfinite(pd.to_numeric(options_prob, errors="coerce"))
        if has_options:
            options_prob_f = float(options_prob)
            options_conf_f = float(pd.to_numeric(options_conf, errors="coerce"))
            options_signed_ret_f = float(pd.to_numeric(options_signed_ret, errors="coerce"))
            options_conf_f = float(np.clip(options_conf_f, 0.0, 1.0))

            # Let options dominate only when confidence is high.
            w_dir = float(np.clip(0.18 + 0.52 * options_conf_f, 0.18, 0.70))
            w_mag = float(np.clip(0.20 + 0.55 * options_conf_f, 0.20, 0.75))
            refined_prob = (1.0 - w_dir) * macro_prob + w_dir * options_prob_f
            refined_signed_ret = (1.0 - w_mag) * macro_signed_ret + w_mag * options_signed_ret_f
        else:
            w_dir = 0.0
            w_mag = 0.0
            refined_prob = macro_prob
            refined_signed_ret = macro_signed_ret

        refined_direction = "Up" if refined_prob >= 0.5 else "Down"
        refined_abs_move = abs(refined_signed_ret)
        refined_return_aligned = refined_abs_move if refined_direction == "Up" else -refined_abs_move

        out = dict(r)
        out.update(
            {
                "blend_w_direction": w_dir,
                "blend_w_magnitude": w_mag,
                "refined_prob_up": float(refined_prob),
                "refined_direction": refined_direction,
                "refined_return": float(refined_signed_ret),
                "refined_return_aligned": float(refined_return_aligned),
                "refined_direction_confidence": float(abs(refined_prob - 0.5) * 2.0),
                "delta_prob_vs_macro": float(refined_prob - macro_prob),
                "delta_return_vs_macro": float(refined_return_aligned - macro_signed_ret),
            }
        )
        out_rows.append(out)
    return pd.DataFrame(out_rows).sort_values(["asset", "horizon"]).reset_index(drop=True)


def summarize_spy_nasdaq_divergence(test_pred_detail: pd.DataFrame) -> pd.DataFrame:
    df = test_pred_detail.copy()
    if df.empty:
        return pd.DataFrame()
    df = df[df["asset"].isin(["SP500", "NASDAQ"])].copy()
    if df.empty:
        return pd.DataFrame()

    rows = []
    for horizon, grp in df.groupby("horizon"):
        piv_pred = grp.pivot_table(index="asof_date", columns="asset", values="pred_direction", aggfunc="first")
        piv_real = grp.pivot_table(index="asof_date", columns="asset", values="realized_direction", aggfunc="first")
        common = piv_pred.index.intersection(piv_real.index)
        if len(common) == 0:
            continue
        pred = piv_pred.loc[common].dropna()
        real = piv_real.loc[pred.index].dropna()
        common2 = pred.index.intersection(real.index)
        if len(common2) == 0:
            continue
        pred = pred.loc[common2]
        real = real.loc[common2]
        if not {"SP500", "NASDAQ"}.issubset(pred.columns) or not {"SP500", "NASDAQ"}.issubset(real.columns):
            continue

        pred_div = pred["SP500"] != pred["NASDAQ"]
        real_div = real["SP500"] != real["NASDAQ"]
        n = int(len(pred_div))
        if n == 0:
            continue
        div_hit = float((pred_div == real_div).mean())
        pred_div_rate = float(pred_div.mean())
        real_div_rate = float(real_div.mean())
        cond_n = int(pred_div.sum())
        cond_hit = float((real_div[pred_div]).mean()) if cond_n > 0 else np.nan
        rows.append(
            {
                "horizon": horizon,
                "n_test": n,
                "actual_divergence_rate": real_div_rate,
                "predicted_divergence_rate": pred_div_rate,
                "divergence_classification_hit_rate": div_hit,
                "n_predicted_divergence": cond_n,
                "hit_rate_when_predicting_divergence": cond_hit,
            }
        )
    return pd.DataFrame(rows).sort_values("horizon").reset_index(drop=True)


def main() -> None:
    args = parse_args()
    paths = resolve_paths(args)
    paths.reports_dir.mkdir(parents=True, exist_ok=True)

    if not paths.latest_predictions_csv.exists():
        raise SystemExit(f"Missing {paths.latest_predictions_csv}; run macro_transformer_pipeline first.")

    macro_df = pd.read_csv(paths.latest_predictions_csv)
    if macro_df.empty:
        raise SystemExit(f"{paths.latest_predictions_csv} is empty.")
    asof_date = pd.to_datetime(macro_df["asof_date"].iloc[0], errors="coerce")
    if pd.isna(asof_date):
        asof_date = pd.Timestamp.utcnow().normalize()

    opt_df = build_options_component_table(
        asof_date=asof_date,
        max_expiries=args.max_expiries_per_ticker,
        max_dte=args.max_dte,
    )
    opt_df.to_csv(paths.options_components_csv, index=False)

    refined_df = blend_macro_and_options(macro_df=macro_df, opt_df=opt_df)
    refined_df.to_csv(paths.refined_predictions_csv, index=False)

    if paths.test_predictions_detailed_csv.exists():
        test_detail_df = pd.read_csv(paths.test_predictions_detailed_csv)
    else:
        test_detail_df = pd.DataFrame()
    div_summary = summarize_spy_nasdaq_divergence(test_detail_df)
    div_summary.to_csv(paths.divergence_summary_csv, index=False)

    print("Wrote:")
    print(f"- {paths.options_components_csv}")
    print(f"- {paths.refined_predictions_csv}")
    print(f"- {paths.divergence_summary_csv}")
    if not div_summary.empty:
        print("Divergence summary:")
        print(div_summary.to_string(index=False))


if __name__ == "__main__":
    main()
