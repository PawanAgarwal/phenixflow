import datetime as dt
import math
import sys
from dataclasses import dataclass

import numpy as np
import pandas as pd
import yfinance as yf


def _to_percent(x: float) -> float:
    return float(x) * 100.0


def _max_drawdown(cum: pd.Series) -> float:
    peak = cum.cummax()
    dd = (cum / peak) - 1.0
    return float(dd.min())


def _metrics_from_returns(daily_ret: pd.Series) -> dict:
    daily_ret = daily_ret.dropna()
    if len(daily_ret) < 30:
        return {
            "sharpe": np.nan,
            "sortino": np.nan,
            "calmar": np.nan,
            "cagr": np.nan,
            "max_drawdown": np.nan,
            "vol_ann": np.nan,
        }

    mu = float(daily_ret.mean())
    sigma = float(daily_ret.std(ddof=1))
    sharpe = (mu / sigma) * math.sqrt(252) if sigma > 0 else np.nan

    downside = daily_ret[daily_ret < 0]
    downside_sigma = float(downside.std(ddof=1))
    sortino = (mu / downside_sigma) * math.sqrt(252) if downside_sigma > 0 else np.nan

    cum = (1.0 + daily_ret).cumprod()
    mdd = _max_drawdown(cum)

    years = (daily_ret.index[-1] - daily_ret.index[0]).days / 365.25
    total_return = float(cum.iloc[-1] - 1.0)
    cagr = (1.0 + total_return) ** (1.0 / years) - 1.0 if years > 0 else np.nan

    calmar = (cagr / abs(mdd)) if mdd < 0 else np.nan

    vol_ann = sigma * math.sqrt(252)

    return {
        "sharpe": sharpe,
        "sortino": sortino,
        "calmar": calmar,
        "cagr": cagr,
        "max_drawdown": mdd,
        "vol_ann": vol_ann,
    }


def _get_history(ticker: str) -> pd.DataFrame:
    t = yf.Ticker(ticker)
    hist = t.history(period="max", auto_adjust=False, actions=True)
    if hist is None or hist.empty:
        raise RuntimeError(f"No history for {ticker}")
    return hist


def _compute_single(ticker: str, benchmarks: list[str]) -> dict:
    hist = _get_history(ticker)
    hist = hist.dropna(subset=["Close"])  # keep rows even if Dividends missing
    start = hist.index.min().normalize()
    end = hist.index.max().normalize()

    first_close = float(hist.loc[hist.index.min(), "Close"])
    last_close = float(hist.loc[hist.index.max(), "Close"])
    price_return = (last_close / first_close) - 1.0

    dividends = hist.get("Dividends")
    if dividends is None:
        dividends = pd.Series(0.0, index=hist.index)
    total_distributions = float(dividends.fillna(0.0).sum())
    dist_return = total_distributions / first_close if first_close != 0 else np.nan

    if "Adj Close" in hist.columns and not hist["Adj Close"].isna().all():
        first_adj = float(hist.loc[hist.index.min(), "Adj Close"])
        last_adj = float(hist.loc[hist.index.max(), "Adj Close"])
        total_return = (last_adj / first_adj) - 1.0
        daily_ret = hist["Adj Close"].pct_change()
    else:
        total_return = np.nan
        daily_ret = hist["Close"].pct_change()

    m = _metrics_from_returns(daily_ret)

    out = {
        "ticker": ticker,
        "start": start.date().isoformat(),
        "end": end.date().isoformat(),
        "days": int((end - start).days),
        "price_return_pct": _to_percent(price_return),
        "total_distributions": total_distributions,
        "distribution_return_pct": _to_percent(dist_return),
        "total_return_reinvested_pct": _to_percent(total_return) if not np.isnan(total_return) else np.nan,
        "sharpe": m["sharpe"],
        "sortino": m["sortino"],
        "calmar": m["calmar"],
        "cagr_pct": _to_percent(m["cagr"]) if not np.isnan(m["cagr"]) else np.nan,
        "max_drawdown_pct": _to_percent(m["max_drawdown"]) if not np.isnan(m["max_drawdown"]) else np.nan,
        "vol_ann_pct": _to_percent(m["vol_ann"]) if not np.isnan(m["vol_ann"]) else np.nan,
    }

    for b in benchmarks:
        bh = _get_history(b)
        bh = bh.loc[(bh.index >= start) & (bh.index <= end)].dropna(subset=["Adj Close"])
        bret = bh["Adj Close"].pct_change()
        bm = _metrics_from_returns(bret)
        out[f"{b}_total_return_reinvested_pct"] = _to_percent(float(bh["Adj Close"].iloc[-1] / bh["Adj Close"].iloc[0] - 1.0)) if len(bh) > 1 else np.nan
        out[f"{b}_sharpe"] = bm["sharpe"]
        out[f"{b}_sortino"] = bm["sortino"]
        out[f"{b}_calmar"] = bm["calmar"]

    return out


def _format_md_table(df: pd.DataFrame) -> str:
    # Keep numeric precision reasonable
    fmt_cols = [c for c in df.columns if c not in ("ticker", "start", "end")]
    df2 = df.copy()
    for c in fmt_cols:
        if pd.api.types.is_numeric_dtype(df2[c]):
            df2[c] = df2[c].map(lambda x: "" if pd.isna(x) else f"{x:.3f}")
    return df2.to_markdown(index=False)


def main() -> int:
    yieldmax_tickers = [
        "ABNY",
        "AIYY",
        "AMDY",
        "AMZY",
        "APLY",
        "BABO",
        "BRKC",
        "CONY",
        "CRCO",
        "CVNY",
        "DISO",
        "DRAY",
        "FBY",
        "GDXY",
        "GMEY",
        "GOOY",
        "HIYY",
        "HOOY",
        "JPO",
        "MARO",
        "MRNY",
        "MSFO",
        "MSTY",
        "NFLY",
        "NVDY",
        "OARK",
        "PLTY",
        "PYPY",
        "RBLY",
        "RDYY",
        "SMCY",
        "SNOY",
        "TSLY",
        "TSMY",
        "XOMO",
        "XYZY",
        "YBIT",
        "CRSH",
        "DIPS",
        "FIAT",
        "WNTR",
        "YQQQ",
        "YMAG",
        "YMAX",
        "SLTY",
        "ULTY",
        "FEAT",
        "FIVY",
        "QDTY",
        "RDTY",
        "SDTY",
        "CHPY",
        "GPTY",
        "LFGY",
        "MINY",
        "BIGY",
        "RNTY",
        "SOXY",
        "MSST",
        "NVIT",
        "TEST",
        "DDDD",
    ]

    benchmarks = ["SPY", "QQQ"]

    rows = []
    failures = []
    for t in yieldmax_tickers:
        try:
            rows.append(_compute_single(t, benchmarks))
        except Exception as e:
            failures.append({"ticker": t, "error": str(e)})

    out_dir = "."
    ts = dt.datetime.now().strftime("%Y-%m-%d_%H%M%S")

    df = pd.DataFrame(rows)
    df = df.sort_values(by="total_return_reinvested_pct", ascending=False, na_position="last")
    df.to_csv(f"{out_dir}/yieldmax_metrics_{ts}.csv", index=False)

    if failures:
        pd.DataFrame(failures).to_csv(f"{out_dir}/failures_{ts}.csv", index=False)

    md = []
    md.append("# YieldMax ETF Returns + Risk Ratios (inception-to-date)\n")
    md.append(f"Generated: {dt.datetime.now().isoformat()}\n")
    md.append("## Notes\n")
    md.append("- Returns use Yahoo Finance daily data via `yfinance`.\n")
    md.append("- `price_return_pct` is based on unadjusted close (does not assume reinvestment).\n")
    md.append("- `distribution_return_pct` is `sum(dividends) / first_close` (cash paid / initial price).\n")
    md.append("- `total_return_reinvested_pct` is based on Adjusted Close (assumes distributions reinvested).\n")
    md.append("- Sharpe/Sortino use daily returns (total-return series) and assume 0% risk-free rate.\n")
    md.append("- Calmar = CAGR / |max drawdown|.\n")

    md.append("## Results\n")
    md.append(_format_md_table(df[[
        "ticker",
        "start",
        "end",
        "price_return_pct",
        "total_distributions",
        "distribution_return_pct",
        "total_return_reinvested_pct",
        "sharpe",
        "sortino",
        "calmar",
        "SPY_total_return_reinvested_pct",
        "SPY_sharpe",
        "SPY_sortino",
        "SPY_calmar",
        "QQQ_total_return_reinvested_pct",
        "QQQ_sharpe",
        "QQQ_sortino",
        "QQQ_calmar",
    ]]))

    report_path = f"{out_dir}/REPORT.md"
    with open(report_path, "w", encoding="utf-8") as f:
        f.write("\n".join(md))

    print(f"Wrote {report_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
