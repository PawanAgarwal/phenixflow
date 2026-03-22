#!/usr/bin/env python3
from __future__ import annotations

from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd

HORIZONS_YEARS = [1, 2, 3, 4, 5]


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


def main() -> None:
    base = Path(__file__).resolve().parent
    as_of = date.today().isoformat()

    person_universe = pd.read_csv(base / "person_manager_universe.csv")
    person_links = pd.read_csv(base / "person_manager_fund_links.csv")
    summary = pd.read_csv(base / "person_manager_multiyear_summary.csv")
    detail = pd.read_csv(base / "person_manager_multiyear_detail.csv")
    ranking = pd.read_csv(base / "person_manager_rankings.csv")

    universe_map = person_universe.set_index("Person")

    # Good / bad buckets.
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
        ["Funds selected (1Y full eligible from prior universe)", str(person_links['Ticker'].nunique())],
        ["Funds with parsed manager team", str(person_links["Ticker"].nunique())],
        ["Unique people identified", str(person_links["Person"].nunique())],
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

    report.append("## Winners By Timeframe (People Beating SPY or QQQ)")
    report.append("")
    for horizon in HORIZONS_YEARS:
        for mode in ("partial", "full"):
            part = detail[(detail["HorizonYears"] == horizon) & (detail["Mode"] == mode)].copy()
            report.append(f"### {horizon}Y {mode.capitalize()}")
            if part.empty:
                report.append("None")
                report.append("")
                continue

            winners = part[part["BeatsEither"] == True].copy().sort_values(["Calmar"], ascending=False).reset_index(drop=True)
            report.append(
                f"People in section: **{len(part)}** | Winners vs SPY or QQQ: **{len(winners)}**"
            )
            report.append("")
            if winners.empty:
                report.append("None")
                report.append("")
                continue

            rows = []
            for i, (_, w) in enumerate(winners.iterrows(), start=1):
                person = str(w["Person"])
                funds_list = ""
                if person in universe_map.index:
                    funds_list = str(universe_map.loc[person, "FundsList"])
                rows.append(
                    [
                        str(i),
                        person,
                        str(int(w["FundsUsed"])),
                        str(int(w["AssignmentsUsed"])),
                        str(w["Start"]),
                        str(w["End"]),
                        fmt_num(float(w["Calmar"]), 3),
                        fmt_num(float(w["SPYCalmarSameWindow"]), 3),
                        fmt_num(float(w["QQQCalmarSameWindow"]), 3),
                        "Y" if bool(w["BeatsSPY"]) else "N",
                        "Y" if bool(w["BeatsQQQ"]) else "N",
                        funds_list,
                    ]
                )

            report.append(
                markdown_table(
                    [
                        "Rank",
                        "Person",
                        "Funds",
                        "Assignments",
                        "Start",
                        "End",
                        "Calmar",
                        "SPY",
                        "QQQ",
                        "Beat SPY",
                        "Beat QQQ",
                        "Funds List",
                    ],
                    rows,
                )
            )
            report.append("")

    report.append("## Notes")
    report.append("- This is a best-effort public-data build; not every ETF had parsable people/tenure metadata.")
    report.append("- Manager timeline bars are used as tenure windows where available; missing end dates are treated as Present.")
    report.append("- Stitched person return uses equal-weight blend across concurrent assignments; it is a proxy, not AUM-weighted.")

    out = base / "person_manager_track_record_report.md"
    out.write_text("\n".join(report), encoding="utf-8")
    print(f"Saved: {out}")


if __name__ == "__main__":
    main()

