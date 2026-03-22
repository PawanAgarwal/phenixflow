#!/usr/bin/env python3
from __future__ import annotations

from datetime import date
from pathlib import Path
import math
import time

import numpy as np
import pandas as pd
import yfinance as yf

BENCHMARKS = ("SPY", "QQQ")
HORIZONS_YEARS = [1, 2, 3, 4, 5]
MAX_RETRIES = 4


def normalize_adj_close(hist: pd.DataFrame) -> pd.Series:
    if hist is None or hist.empty or "Adj Close" not in hist.columns:
        return pd.Series(dtype=float)
    out = hist.copy().sort_index()
    if getattr(out.index, "tz", None) is not None:
        out.index = out.index.tz_localize(None)
    adj = pd.to_numeric(out["Adj Close"], errors="coerce").dropna()
    return adj


def fetch_adj_close(ticker: str) -> pd.Series:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            hist = yf.Ticker(ticker).history(
                period="max",
                interval="1d",
                auto_adjust=False,
                actions=False,
                timeout=30,
            )
            adj = normalize_adj_close(hist)
            if len(adj) >= 2:
                return adj
            return pd.Series(dtype=float)
        except Exception:
            if attempt < MAX_RETRIES:
                time.sleep(1.2 * attempt)
    return pd.Series(dtype=float)


def calmar_from_adj(adj: pd.Series) -> dict[str, float]:
    if adj is None or len(adj) < 2:
        return {
            "annualized_return": np.nan,
            "max_drawdown": np.nan,
            "calmar": np.nan,
            "trading_days": np.nan,
        }

    daily = adj.pct_change().replace([np.inf, -np.inf], np.nan).dropna()
    if len(daily) < 2:
        return {
            "annualized_return": np.nan,
            "max_drawdown": np.nan,
            "calmar": np.nan,
            "trading_days": float(len(daily)),
        }

    total_return = float(adj.iloc[-1] / adj.iloc[0] - 1.0)
    trading_days = float(len(daily))
    years = trading_days / 252.0

    annualized_return = np.nan
    if years > 0 and (1.0 + total_return) > 0:
        annualized_return = float((1.0 + total_return) ** (1.0 / years) - 1.0)

    wealth = (1.0 + daily).cumprod()
    dd = wealth / wealth.cummax() - 1.0
    max_dd = float(dd.min()) if len(dd) else np.nan

    calmar = np.nan
    if np.isfinite(annualized_return) and np.isfinite(max_dd) and max_dd < 0:
        calmar = annualized_return / abs(max_dd)

    return {
        "annualized_return": annualized_return,
        "max_drawdown": max_dd,
        "calmar": calmar,
        "trading_days": trading_days,
    }


def fmt_pct(x: float) -> str:
    return "n/a" if not np.isfinite(x) else f"{x * 100:.2f}%"


def fmt_num(x: float, d: int = 3) -> str:
    return "n/a" if not np.isfinite(x) else f"{x:.{d}f}"


def markdown_table(headers: list[str], rows: list[list[str]]) -> str:
    widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], len(str(cell)))

    def line(vals: list[str]) -> str:
        return "| " + " | ".join(str(vals[i]).ljust(widths[i]) for i in range(len(vals))) + " |"

    out = [line(headers), "| " + " | ".join("-" * w for w in widths) + " |"]
    out.extend(line(r) for r in rows)
    return "\n".join(out)


def resolve_window(
    series: pd.Series,
    benchmark_end: pd.Timestamp,
    horizon: int,
    mode: str,
) -> tuple[pd.Timestamp, pd.Timestamp] | None:
    """Return analysis window for a horizon/mode, or None if ETF not in that bucket.

    Definitions (as requested):
    - NY full: full N calendar years history to benchmark_end.
    - NY partial: at least (N-1) years history, but less than N years.
      Example: 5Y partial => 4 to <5 years live.
    """
    if series is None or len(series) < 2:
        return None

    inception = series.index.min()
    etf_end = min(benchmark_end, series.index.max())
    full_start = benchmark_end - pd.DateOffset(years=horizon)

    if mode == "full":
        if inception > full_start:
            return None
        start = full_start
    else:
        # Partial bucket is a non-overlapping age band:
        # (benchmark_end - N years, benchmark_end - (N-1) years]
        upper = benchmark_end if horizon == 1 else benchmark_end - pd.DateOffset(years=horizon - 1)
        if not (inception > full_start and inception <= upper):
            return None
        start = inception

    if etf_end <= start:
        return None

    return start, etf_end


def main() -> None:
    base = Path(__file__).resolve().parent
    source_csv = base / "broad_discovery_non_single_1y_calmar_screen.csv"
    if not source_csv.exists():
        raise RuntimeError(f"Missing source universe: {source_csv}")

    universe = pd.read_csv(source_csv)
    universe = universe[["Ticker", "Provider", "Name"]].drop_duplicates().reset_index(drop=True)
    tickers = universe["Ticker"].tolist()

    # Fetch histories for ETF universe + benchmarks.
    histories: dict[str, pd.Series] = {}
    fetch_list = tickers + list(BENCHMARKS)
    for i, t in enumerate(fetch_list, start=1):
        print(f"[{i}/{len(fetch_list)}] Fetching {t}...")
        histories[t] = fetch_adj_close(t)

    for b in BENCHMARKS:
        if b not in histories or len(histories[b]) < 2:
            raise RuntimeError(f"Failed benchmark history for {b}")

    benchmark_end = min(histories["SPY"].index.max(), histories["QQQ"].index.max())

    rows: list[dict[str, object]] = []

    for horizon in HORIZONS_YEARS:
        for mode in ("full", "partial"):
            for _, u in universe.iterrows():
                t = str(u["Ticker"])
                s = histories.get(t, pd.Series(dtype=float))
                if len(s) < 2:
                    continue

                window = resolve_window(
                    series=s,
                    benchmark_end=benchmark_end,
                    horizon=horizon,
                    mode=mode,
                )
                if window is None:
                    continue
                start, etf_end = window

                etf_window = s[(s.index >= start) & (s.index <= etf_end)]
                if len(etf_window) < 2:
                    continue

                spy_window = histories["SPY"][(histories["SPY"].index >= start) & (histories["SPY"].index <= etf_end)]
                qqq_window = histories["QQQ"][(histories["QQQ"].index >= start) & (histories["QQQ"].index <= etf_end)]
                if len(spy_window) < 2 or len(qqq_window) < 2:
                    continue

                m_etf = calmar_from_adj(etf_window)
                m_spy = calmar_from_adj(spy_window)
                m_qqq = calmar_from_adj(qqq_window)

                rows.append(
                    {
                        "HorizonYears": horizon,
                        "Mode": mode,
                        "Ticker": t,
                        "Provider": str(u["Provider"]),
                        "Name": str(u["Name"]),
                        "Start": etf_window.index.min().date().isoformat(),
                        "End": etf_window.index.max().date().isoformat(),
                        "TradingDays": int(m_etf["trading_days"]) if np.isfinite(m_etf["trading_days"]) else np.nan,
                        "ETFAnnualizedReturn": m_etf["annualized_return"],
                        "ETFMaxDrawdown": m_etf["max_drawdown"],
                        "ETFCalmar": m_etf["calmar"],
                        "SPYCalmarSameWindow": m_spy["calmar"],
                        "QQQCalmarSameWindow": m_qqq["calmar"],
                        "BeatsSPY": bool(np.isfinite(m_etf["calmar"]) and np.isfinite(m_spy["calmar"]) and m_etf["calmar"] > m_spy["calmar"]),
                        "BeatsQQQ": bool(np.isfinite(m_etf["calmar"]) and np.isfinite(m_qqq["calmar"]) and m_etf["calmar"] > m_qqq["calmar"]),
                        "BeatsEither": bool(
                            np.isfinite(m_etf["calmar"]) and (
                                (np.isfinite(m_spy["calmar"]) and m_etf["calmar"] > m_spy["calmar"])
                                or (np.isfinite(m_qqq["calmar"]) and m_etf["calmar"] > m_qqq["calmar"])
                            )
                        ),
                    }
                )

    detail = pd.DataFrame(rows)
    if detail.empty:
        raise RuntimeError("No metrics computed.")

    detail = detail.sort_values(["HorizonYears", "Mode", "ETFCalmar"], ascending=[True, True, False], na_position="last").reset_index(drop=True)
    detail.to_csv(base / "multiyear_partial_full_calmar_detail.csv", index=False)

    summary_rows = []
    for horizon in HORIZONS_YEARS:
        for mode in ("partial", "full"):
            part = detail[(detail["HorizonYears"] == horizon) & (detail["Mode"] == mode)].copy()
            if part.empty:
                continue
            summary_rows.append(
                {
                    "HorizonYears": horizon,
                    "Mode": mode,
                    "ETFCount": int(len(part)),
                    "AvgETFCalmar": float(part["ETFCalmar"].replace([np.inf, -np.inf], np.nan).dropna().mean()),
                    "AvgSPYCalmarMatched": float(part["SPYCalmarSameWindow"].replace([np.inf, -np.inf], np.nan).dropna().mean()),
                    "AvgQQQCalmarMatched": float(part["QQQCalmarSameWindow"].replace([np.inf, -np.inf], np.nan).dropna().mean()),
                    "CountBeatSPY": int(part["BeatsSPY"].sum()),
                    "CountBeatQQQ": int(part["BeatsQQQ"].sum()),
                    "CountBeatEither": int(part["BeatsEither"].sum()),
                    "CountBeatBoth": int((part["BeatsSPY"] & part["BeatsQQQ"]).sum()),
                }
            )

    summary = pd.DataFrame(summary_rows).sort_values(["HorizonYears", "Mode"]).reset_index(drop=True)
    summary.to_csv(base / "multiyear_partial_full_calmar_summary.csv", index=False)

    # Markdown report
    report: list[str] = []
    report.append(f"# Multi-Year Calmar Screen (Partial vs Full) up to 5Y (as of {date.today().isoformat()})")
    report.append("")
    report.append("Universe: broad-discovery non-single option/derivative-income ETFs.")
    report.append("For each ETF and timeframe, SPY/QQQ Calmar is computed on the exact same ETF window.")
    report.append("Bucket definitions:")
    report.append("- NY full: full N calendar years history (window = last N years).")
    report.append("- NY partial: live history between (N-1) and <N years (window = ETF inception to end).")
    report.append("")

    # Top summary table.
    summary_headers = [
        "Horizon",
        "Mode",
        "ETF Count",
        "Avg ETF Calmar",
        "Avg SPY Calmar (matched)",
        "Avg QQQ Calmar (matched)",
        "Beat SPY",
        "Beat QQQ",
        "Beat Either",
        "Beat Both",
    ]
    summary_md_rows: list[list[str]] = []
    for _, r in summary.iterrows():
        summary_md_rows.append(
            [
                f"{int(r['HorizonYears'])}Y",
                str(r["Mode"]),
                str(int(r["ETFCount"])),
                fmt_num(float(r["AvgETFCalmar"]), 3),
                fmt_num(float(r["AvgSPYCalmarMatched"]), 3),
                fmt_num(float(r["AvgQQQCalmarMatched"]), 3),
                str(int(r["CountBeatSPY"])),
                str(int(r["CountBeatQQQ"])),
                str(int(r["CountBeatEither"])),
                str(int(r["CountBeatBoth"])),
            ]
        )
    report.append("## Summary")
    report.append(markdown_table(summary_headers, summary_md_rows))
    report.append("")

    # Per section winners (beat SPY or QQQ).
    for horizon in HORIZONS_YEARS:
        for mode in ("partial", "full"):
            part = detail[(detail["HorizonYears"] == horizon) & (detail["Mode"] == mode)].copy()
            if part.empty:
                continue
            winners = part[part["BeatsEither"]].copy()
            winners = winners.sort_values(["ETFCalmar"], ascending=False).reset_index(drop=True)
            winners.insert(0, "SectionRank", range(1, len(winners) + 1))

            report.append(f"## {horizon}Y {mode.capitalize()} - ETFs Beating SPY or QQQ Calmar")
            report.append(
                f"Count winners: **{len(winners)}** / {len(part)} | "
                f"Avg ETF Calmar: **{fmt_num(float(part['ETFCalmar'].replace([np.inf, -np.inf], np.nan).dropna().mean()), 3)}** | "
                f"Avg SPY matched: **{fmt_num(float(part['SPYCalmarSameWindow'].replace([np.inf, -np.inf], np.nan).dropna().mean()), 3)}** | "
                f"Avg QQQ matched: **{fmt_num(float(part['QQQCalmarSameWindow'].replace([np.inf, -np.inf], np.nan).dropna().mean()), 3)}**"
            )
            report.append("")

            if winners.empty:
                report.append("None")
                report.append("")
                continue

            headers = [
                "Rank",
                "Ticker",
                "Provider",
                "Start",
                "End",
                "TradingDays",
                "ETF Calmar",
                "SPY Calmar",
                "QQQ Calmar",
                "Beat SPY",
                "Beat QQQ",
                "Name",
            ]
            rows_md: list[list[str]] = []
            for _, w in winners.iterrows():
                rows_md.append(
                    [
                        str(int(w["SectionRank"])),
                        str(w["Ticker"]),
                        str(w["Provider"]),
                        str(w["Start"]),
                        str(w["End"]),
                        str(int(w["TradingDays"])),
                        fmt_num(float(w["ETFCalmar"]), 3),
                        fmt_num(float(w["SPYCalmarSameWindow"]), 3),
                        fmt_num(float(w["QQQCalmarSameWindow"]), 3),
                        "Y" if bool(w["BeatsSPY"]) else "N",
                        "Y" if bool(w["BeatsQQQ"]) else "N",
                        str(w["Name"]),
                    ]
                )
            report.append(markdown_table(headers, rows_md))
            report.append("")

    (base / "multiyear_partial_full_calmar_report.md").write_text("\n".join(report), encoding="utf-8")

    print(f"Saved: {base / 'multiyear_partial_full_calmar_report.md'}")
    print(f"Saved: {base / 'multiyear_partial_full_calmar_summary.csv'}")
    print(f"Saved: {base / 'multiyear_partial_full_calmar_detail.csv'}")


if __name__ == "__main__":
    main()
