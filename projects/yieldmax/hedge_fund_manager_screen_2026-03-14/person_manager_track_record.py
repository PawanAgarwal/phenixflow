#!/usr/bin/env python3
from __future__ import annotations

from datetime import date
from pathlib import Path
import re
import subprocess
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
    s = pd.to_numeric(out["Adj Close"], errors="coerce").dropna()
    return s


def fetch_adj_close(ticker: str) -> pd.Series:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            hist = yf.Ticker(ticker).history(
                period="max",
                interval="1d",
                auto_adjust=False,
                actions=True,
                timeout=30,
            )
            s = normalize_adj_close(hist)
            if len(s) >= 2:
                return s
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


def resolve_window(
    series: pd.Series,
    benchmark_end: pd.Timestamp,
    horizon: int,
    mode: str,
) -> tuple[pd.Timestamp, pd.Timestamp] | None:
    if series is None or len(series) < 2:
        return None

    inception = series.index.min()
    series_end = min(benchmark_end, series.index.max())
    full_start = benchmark_end - pd.DateOffset(years=horizon)

    if mode == "full":
        if inception > full_start:
            return None
        start = full_start
    else:
        upper = benchmark_end if horizon == 1 else benchmark_end - pd.DateOffset(years=horizon - 1)
        if not (inception > full_start and inception <= upper):
            return None
        start = inception

    if series_end <= start:
        return None
    return start, series_end


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


def fmt_num(x: float, d: int = 3) -> str:
    return "n/a" if not np.isfinite(x) else f"{x:.{d}f}"


def run_pwcli(pwcli: str, args: list[str], timeout: int = 60) -> str:
    res = subprocess.run(
        [pwcli, *args],
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    return (res.stdout or "") + ("\n" + res.stderr if res.stderr else "")


def exchange_to_mstar_venues(exchange: str) -> list[str]:
    ex = (exchange or "").upper()
    if ex in {"PCX", "ARCX", "BTS"}:
        return ["arcx", "xnas", "xnys"]
    if ex in {"NGM", "NMS", "NCM", "NAS"}:
        return ["xnas", "arcx", "xnys"]
    if ex in {"NYQ", "ASE", "XNYS"}:
        return ["xnys", "arcx", "xnas"]
    return ["arcx", "xnas", "xnys"]


def parse_snapshot_path(snapshot_output: str) -> str | None:
    m = re.search(r"\((\.playwright-cli/page-[^)]+\.yml)\)", snapshot_output)
    if not m:
        return None
    return m.group(1)


def parse_management_team(snapshot_text: str) -> list[tuple[str, str]]:
    section_match = re.search(
        r'heading "Management Team".*?(?:link "Full Management Team"|heading "Manager Timeline")',
        snapshot_text,
        flags=re.S,
    )
    if not section_match:
        return []
    section = section_match.group(0)

    names = re.findall(r'link "([^"]+)" \[ref=[^\]]+\] \[cursor=pointer\]:', section)
    tenures = re.findall(
        r"generic \[ref=[^\]]+\]: ([A-Za-z]{3} \d{2}, \d{4}\s*–\s*(?:Present|[A-Za-z]{3} \d{2}, \d{4}))",
        section,
    )
    pairs: list[tuple[str, str]] = []
    for i in range(min(len(names), len(tenures))):
        nm = names[i].strip()
        tn = tenures[i].replace("\u2013", "–").strip()
        if nm and tn:
            pairs.append((nm, tn))

    # Deduplicate while preserving order.
    out: list[tuple[str, str]] = []
    seen = set()
    for p in pairs:
        if p in seen:
            continue
        seen.add(p)
        out.append(p)
    return out


def parse_tenure_range(tenure: str) -> tuple[pd.Timestamp | None, pd.Timestamp | None, bool]:
    m = re.match(r"([A-Za-z]{3} \d{2}, \d{4})\s*–\s*(Present|[A-Za-z]{3} \d{2}, \d{4})", tenure)
    if not m:
        return None, None, False
    start = pd.to_datetime(m.group(1), errors="coerce")
    end_raw = m.group(2)
    if str(end_raw).lower() == "present":
        return start, None, True
    end = pd.to_datetime(end_raw, errors="coerce")
    return start, end, False


def scrape_manager_people(
    out_dir: Path,
    fund_universe: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    code_home = Path.home() / ".codex"
    pwcli = str(code_home / "skills" / "playwright" / "scripts" / "playwright_cli.sh")
    if not Path(pwcli).exists():
        raise RuntimeError(f"Missing playwright wrapper: {pwcli}")

    # Open once, then reuse browser context.
    run_pwcli(pwcli, ["open", "https://www.morningstar.com/etfs/arcx/jepi/people"], timeout=90)

    manager_rows: list[dict[str, object]] = []
    unavailable_rows: list[dict[str, object]] = []

    for i, row in fund_universe.iterrows():
        ticker = str(row["Ticker"])
        exchange = str(row["Exchange"])
        venues = exchange_to_mstar_venues(exchange)
        parsed = False

        print(f"[People {i+1}/{len(fund_universe)}] {ticker} ...")
        for venue in venues:
            url = f"https://www.morningstar.com/etfs/{venue}/{ticker.lower()}/people"
            _ = run_pwcli(pwcli, ["goto", url], timeout=70)
            snap_out = run_pwcli(pwcli, ["snapshot"], timeout=70)
            snap_rel = parse_snapshot_path(snap_out)
            if not snap_rel:
                continue
            snap_file = Path.cwd() / snap_rel
            if not snap_file.exists():
                continue

            text = snap_file.read_text(encoding="utf-8", errors="ignore")
            pairs = parse_management_team(text)
            if not pairs:
                continue

            for name, tenure in pairs:
                start, end, present = parse_tenure_range(tenure)
                manager_rows.append(
                    {
                        "Ticker": ticker,
                        "FundName": str(row["Name"]),
                        "Exchange": exchange,
                        "MorningstarVenue": venue,
                        "PeopleURL": url,
                        "Person": name,
                        "TenureRaw": tenure,
                        "TenureStart": start.date().isoformat() if pd.notna(start) else "",
                        "TenureEnd": end.date().isoformat() if pd.notna(end) else "",
                        "TenureEndIsPresent": bool(present),
                    }
                )
            parsed = True
            break

        if not parsed:
            unavailable_rows.append(
                {
                    "Ticker": ticker,
                    "FundName": str(row["Name"]),
                    "Exchange": exchange,
                    "Reason": "No parsable Morningstar management team section",
                }
            )

        time.sleep(0.15)

    # Close browser session.
    try:
        run_pwcli(pwcli, ["close"], timeout=40)
    except Exception:
        pass

    managers = pd.DataFrame(manager_rows).sort_values(["Person", "Ticker"]).reset_index(drop=True) if manager_rows else pd.DataFrame(
        columns=[
            "Ticker",
            "FundName",
            "Exchange",
            "MorningstarVenue",
            "PeopleURL",
            "Person",
            "TenureRaw",
            "TenureStart",
            "TenureEnd",
            "TenureEndIsPresent",
        ]
    )
    unavailable = pd.DataFrame(unavailable_rows).sort_values(["Ticker"]).reset_index(drop=True) if unavailable_rows else pd.DataFrame(
        columns=["Ticker", "FundName", "Exchange", "Reason"]
    )

    managers.to_csv(out_dir / "person_manager_fund_links.csv", index=False)
    unavailable.to_csv(out_dir / "person_manager_fund_links_unavailable.csv", index=False)
    return managers, unavailable


def build_person_histories(
    manager_links: pd.DataFrame,
    fund_histories: dict[str, pd.Series],
    benchmark_end: pd.Timestamp,
) -> tuple[pd.DataFrame, dict[str, pd.Series]]:
    meta_rows: list[dict[str, object]] = []
    person_histories: dict[str, pd.Series] = {}

    for person, part in manager_links.groupby("Person"):
        returns_cols = {}
        used_funds = set()

        for _, r in part.iterrows():
            ticker = str(r["Ticker"])
            s = fund_histories.get(ticker, pd.Series(dtype=float))
            if len(s) < 2:
                continue

            start = pd.to_datetime(r["TenureStart"], errors="coerce")
            end = pd.to_datetime(r["TenureEnd"], errors="coerce")
            if pd.isna(start):
                start = s.index.min()
            if pd.isna(end):
                end = benchmark_end
            start = max(start, s.index.min())
            end = min(end, benchmark_end, s.index.max())
            if end <= start:
                continue

            sub = s[(s.index >= start) & (s.index <= end)]
            if len(sub) < 2:
                continue

            ret = sub.pct_change().replace([np.inf, -np.inf], np.nan).dropna()
            if len(ret) < 2:
                continue
            col = f"{ticker}|{start.date().isoformat()}"
            returns_cols[col] = ret
            used_funds.add(ticker)

        if not returns_cols:
            continue

        ret_df = pd.DataFrame(returns_cols).sort_index()
        person_ret = ret_df.mean(axis=1, skipna=True).dropna()
        if len(person_ret) < 2:
            continue

        person_adj = (1.0 + person_ret).cumprod() * 100.0
        pid = f"PM::{person}"
        person_histories[pid] = person_adj

        meta_rows.append(
            {
                "PersonId": pid,
                "Person": person,
                "FundsUsed": len(used_funds),
                "AssignmentsUsed": ret_df.shape[1],
                "Inception": person_adj.index.min().date().isoformat(),
                "LastDate": person_adj.index.max().date().isoformat(),
                "FundsList": ", ".join(sorted(used_funds)),
            }
        )

    meta = pd.DataFrame(meta_rows).sort_values(["Person"]).reset_index(drop=True) if meta_rows else pd.DataFrame(
        columns=["PersonId", "Person", "FundsUsed", "AssignmentsUsed", "Inception", "LastDate", "FundsList"]
    )
    return meta, person_histories


def compute_multiyear_person_screen(
    person_universe: pd.DataFrame,
    person_histories: dict[str, pd.Series],
    benchmark_histories: dict[str, pd.Series],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    rows: list[dict[str, object]] = []
    benchmark_end = min(benchmark_histories["SPY"].index.max(), benchmark_histories["QQQ"].index.max())

    for horizon in HORIZONS_YEARS:
        for mode in ("full", "partial"):
            for _, u in person_universe.iterrows():
                pid = str(u["PersonId"])
                s = person_histories.get(pid, pd.Series(dtype=float))
                if len(s) < 2:
                    continue

                win = resolve_window(s, benchmark_end=benchmark_end, horizon=horizon, mode=mode)
                if win is None:
                    continue
                start, end = win

                inst = s[(s.index >= start) & (s.index <= end)]
                if len(inst) < 2:
                    continue

                spy = benchmark_histories["SPY"][(benchmark_histories["SPY"].index >= start) & (benchmark_histories["SPY"].index <= end)]
                qqq = benchmark_histories["QQQ"][(benchmark_histories["QQQ"].index >= start) & (benchmark_histories["QQQ"].index <= end)]
                if len(spy) < 2 or len(qqq) < 2:
                    continue

                m_inst = calmar_from_adj(inst)
                m_spy = calmar_from_adj(spy)
                m_qqq = calmar_from_adj(qqq)
                beats_spy = bool(np.isfinite(m_inst["calmar"]) and np.isfinite(m_spy["calmar"]) and m_inst["calmar"] > m_spy["calmar"])
                beats_qqq = bool(np.isfinite(m_inst["calmar"]) and np.isfinite(m_qqq["calmar"]) and m_inst["calmar"] > m_qqq["calmar"])
                beats_either = bool(beats_spy or beats_qqq)

                rows.append(
                    {
                        "HorizonYears": horizon,
                        "Mode": mode,
                        "PersonId": pid,
                        "Person": str(u["Person"]),
                        "FundsUsed": int(u["FundsUsed"]),
                        "AssignmentsUsed": int(u["AssignmentsUsed"]),
                        "Start": inst.index.min().date().isoformat(),
                        "End": inst.index.max().date().isoformat(),
                        "TradingDays": int(m_inst["trading_days"]) if np.isfinite(m_inst["trading_days"]) else np.nan,
                        "Calmar": m_inst["calmar"],
                        "AnnualizedReturn": m_inst["annualized_return"],
                        "MaxDrawdown": m_inst["max_drawdown"],
                        "SPYCalmarSameWindow": m_spy["calmar"],
                        "QQQCalmarSameWindow": m_qqq["calmar"],
                        "BeatsSPY": beats_spy,
                        "BeatsQQQ": beats_qqq,
                        "BeatsEither": beats_either,
                    }
                )

    detail = pd.DataFrame(rows)
    if detail.empty:
        return detail, pd.DataFrame(
            columns=[
                "HorizonYears",
                "Mode",
                "Count",
                "AvgCalmar",
                "AvgSPYCalmarMatched",
                "AvgQQQCalmarMatched",
                "CountBeatSPY",
                "CountBeatQQQ",
                "CountBeatEither",
                "CountBeatBoth",
            ]
        )

    detail = detail.sort_values(["HorizonYears", "Mode", "Calmar"], ascending=[True, True, False], na_position="last").reset_index(drop=True)

    summary_rows = []
    for horizon in HORIZONS_YEARS:
        for mode in ("full", "partial"):
            part = detail[(detail["HorizonYears"] == horizon) & (detail["Mode"] == mode)]
            if part.empty:
                continue
            summary_rows.append(
                {
                    "HorizonYears": horizon,
                    "Mode": mode,
                    "Count": int(len(part)),
                    "AvgCalmar": float(part["Calmar"].replace([np.inf, -np.inf], np.nan).dropna().mean()),
                    "AvgSPYCalmarMatched": float(part["SPYCalmarSameWindow"].replace([np.inf, -np.inf], np.nan).dropna().mean()),
                    "AvgQQQCalmarMatched": float(part["QQQCalmarSameWindow"].replace([np.inf, -np.inf], np.nan).dropna().mean()),
                    "CountBeatSPY": int(part["BeatsSPY"].sum()),
                    "CountBeatQQQ": int(part["BeatsQQQ"].sum()),
                    "CountBeatEither": int(part["BeatsEither"].sum()),
                    "CountBeatBoth": int((part["BeatsSPY"] & part["BeatsQQQ"]).sum()),
                }
            )

    summary = pd.DataFrame(summary_rows).sort_values(["HorizonYears", "Mode"]).reset_index(drop=True)
    return detail, summary


def main() -> None:
    base = Path(__file__).resolve().parent
    as_of = date.today().isoformat()

    etf_universe = pd.read_csv(base / "hedge_style_etf_universe.csv")
    etf_detail = pd.read_csv(base / "hedge_style_etf_multiyear_detail.csv")

    # Focus on funds with at least a full 1Y section (reduces noise, keeps meaningful track records).
    eligible_tickers = set(
        etf_detail[(etf_detail["HorizonYears"] == 1) & (etf_detail["Mode"] == "full")]["Ticker"].astype(str).unique().tolist()
    )
    fund_universe = etf_universe[etf_universe["Ticker"].astype(str).isin(eligible_tickers)].copy().reset_index(drop=True)

    print(f"Funds selected for person-level scrape: {len(fund_universe)}")

    manager_links, links_unavailable = scrape_manager_people(base, fund_universe)

    # Fund histories for funds with at least one manager link.
    fund_histories: dict[str, pd.Series] = {}
    for i, ticker in enumerate(sorted(manager_links["Ticker"].astype(str).unique()), start=1):
        print(f"[Hist {i}] {ticker}...")
        fund_histories[ticker] = fetch_adj_close(ticker)

    benchmark_histories = {b: fetch_adj_close(b) for b in BENCHMARKS}
    for b in BENCHMARKS:
        if len(benchmark_histories[b]) < 2:
            raise RuntimeError(f"Missing benchmark history for {b}")
    benchmark_end = min(benchmark_histories["SPY"].index.max(), benchmark_histories["QQQ"].index.max())

    person_universe, person_histories = build_person_histories(
        manager_links=manager_links,
        fund_histories=fund_histories,
        benchmark_end=benchmark_end,
    )
    person_universe.to_csv(base / "person_manager_universe.csv", index=False)

    detail, summary = compute_multiyear_person_screen(
        person_universe=person_universe,
        person_histories=person_histories,
        benchmark_histories=benchmark_histories,
    )
    detail.to_csv(base / "person_manager_multiyear_detail.csv", index=False)
    summary.to_csv(base / "person_manager_multiyear_summary.csv", index=False)

    # Aggregate person ranking across available sections.
    rank_rows = []
    if not detail.empty:
        for person, part in detail.groupby("Person"):
            sections = len(part)
            beat_either = int(part["BeatsEither"].sum())
            beat_rate = float(beat_either / sections) if sections else np.nan
            one_y_full = part[(part["HorizonYears"] == 1) & (part["Mode"] == "full")]
            one_y_full_calmar = float(one_y_full["Calmar"].iloc[0]) if len(one_y_full) else np.nan
            rank_rows.append(
                {
                    "Person": person,
                    "FundsUsed": int(part["FundsUsed"].max()),
                    "AssignmentsUsed": int(part["AssignmentsUsed"].max()),
                    "SectionsAvailable": sections,
                    "SectionsBeatEither": beat_either,
                    "BeatEitherRate": beat_rate,
                    "AvgCalmarAcrossSections": float(part["Calmar"].replace([np.inf, -np.inf], np.nan).dropna().mean()),
                    "MedianCalmarAcrossSections": float(part["Calmar"].replace([np.inf, -np.inf], np.nan).dropna().median()),
                    "OneYFullCalmar": one_y_full_calmar,
                }
            )
    ranking = pd.DataFrame(rank_rows).sort_values(
        ["BeatEitherRate", "AvgCalmarAcrossSections", "SectionsAvailable"],
        ascending=[False, False, False],
        na_position="last",
    ).reset_index(drop=True) if rank_rows else pd.DataFrame(
        columns=[
            "Person",
            "FundsUsed",
            "AssignmentsUsed",
            "SectionsAvailable",
            "SectionsBeatEither",
            "BeatEitherRate",
            "AvgCalmarAcrossSections",
            "MedianCalmarAcrossSections",
            "OneYFullCalmar",
        ]
    )
    ranking.to_csv(base / "person_manager_rankings.csv", index=False)

    # Good vs bad buckets.
    good = ranking[(ranking["SectionsAvailable"] >= 3) & (ranking["BeatEitherRate"] >= 0.60)].copy()
    bad = ranking[(ranking["SectionsAvailable"] >= 3) & (ranking["BeatEitherRate"] <= 0.20)].copy()

    report: list[str] = []
    report.append(f"# Person-Centric Fund Manager Track Record Screen (as of {as_of})")
    report.append("")
    report.append("This report tracks named portfolio managers (people), not fund families.")
    report.append("Manager names + tenure were extracted from Morningstar ETF `People` pages.")
    report.append("Person performance is stitched across managed ETFs using tenure windows and equal-weight daily blending across concurrent assignments.")
    report.append("Then Calmar is compared to matched-window SPY/QQQ for 1Y–5Y full/partial sections.")
    report.append("")
    report.append("## Coverage")
    cov_rows = [
        ["Funds selected (1Y full eligible from prior universe)", str(len(fund_universe))],
        ["Funds with parsed manager team", str(manager_links["Ticker"].nunique() if len(manager_links) else 0)],
        ["Funds without parsable manager team", str(len(links_unavailable))],
        ["Unique people identified", str(person_universe["Person"].nunique() if len(person_universe) else 0)],
        ["People with usable stitched history", str(len(person_universe))],
    ]
    report.append(markdown_table(["Metric", "Count"], cov_rows))
    report.append("")

    report.append("## Person Screen Summary (vs SPY/QQQ)")
    if summary.empty:
        report.append("None")
    else:
        rows = []
        for _, r in summary.iterrows():
            rows.append(
                [
                    f"{int(r['HorizonYears'])}Y",
                    str(r["Mode"]),
                    str(int(r["Count"])),
                    fmt_num(float(r["AvgCalmar"]), 3),
                    fmt_num(float(r["AvgSPYCalmarMatched"]), 3),
                    fmt_num(float(r["AvgQQQCalmarMatched"]), 3),
                    str(int(r["CountBeatSPY"])),
                    str(int(r["CountBeatQQQ"])),
                    str(int(r["CountBeatEither"])),
                    str(int(r["CountBeatBoth"])),
                ]
            )
        report.append(
            markdown_table(
                [
                    "Horizon",
                    "Mode",
                    "People",
                    "Avg Calmar",
                    "Avg SPY (matched)",
                    "Avg QQQ (matched)",
                    "Beat SPY",
                    "Beat QQQ",
                    "Beat Either",
                    "Beat Both",
                ],
                rows,
            )
        )
    report.append("")

    report.append("## Top People (Good Bucket)")
    if good.empty:
        report.append("None")
    else:
        top = good.head(30)
        rows = []
        for _, r in top.iterrows():
            rows.append(
                [
                    str(r["Person"]),
                    str(int(r["FundsUsed"])),
                    str(int(r["SectionsAvailable"])),
                    f"{float(r['BeatEitherRate']) * 100:.1f}%",
                    fmt_num(float(r["AvgCalmarAcrossSections"]), 3),
                    fmt_num(float(r["OneYFullCalmar"]), 3),
                ]
            )
        report.append(
            markdown_table(
                ["Person", "Funds", "Sections", "BeatEitherRate", "Avg Calmar", "1Y Full Calmar"],
                rows,
            )
        )
    report.append("")

    report.append("## Bottom People (Bad Bucket)")
    if bad.empty:
        report.append("None")
    else:
        bottom = bad.sort_values(["AvgCalmarAcrossSections", "BeatEitherRate"], ascending=[True, True]).head(30)
        rows = []
        for _, r in bottom.iterrows():
            rows.append(
                [
                    str(r["Person"]),
                    str(int(r["FundsUsed"])),
                    str(int(r["SectionsAvailable"])),
                    f"{float(r['BeatEitherRate']) * 100:.1f}%",
                    fmt_num(float(r["AvgCalmarAcrossSections"]), 3),
                    fmt_num(float(r["OneYFullCalmar"]), 3),
                ]
            )
        report.append(
            markdown_table(
                ["Person", "Funds", "Sections", "BeatEitherRate", "Avg Calmar", "1Y Full Calmar"],
                rows,
            )
        )
    report.append("")

    report.append("## Notes")
    report.append("- This is a best-effort public-data build; not every ETF had parsable people/tenure metadata.")
    report.append("- Manager timeline bars are used as tenure windows where available; missing end dates are treated as Present.")
    report.append("- Stitched person return uses equal-weight blend across concurrent assignments; it is a proxy, not AUM-weighted.")

    (base / "person_manager_track_record_report.md").write_text("\n".join(report), encoding="utf-8")

    print(f"Saved: {base / 'person_manager_track_record_report.md'}")
    print(f"Saved: {base / 'person_manager_fund_links.csv'}")
    print(f"Saved: {base / 'person_manager_universe.csv'}")
    print(f"Saved: {base / 'person_manager_multiyear_summary.csv'}")
    print(f"Saved: {base / 'person_manager_rankings.csv'}")


if __name__ == "__main__":
    main()

