#!/usr/bin/env python3
"""Build a FactSet EPS revision dataset and test predictive power for S&P 500 returns.

Outputs (under data/factset_eps_study/):
- factset_index_revisions.csv
- factset_sector_revisions.csv
- factset_spx_forward_returns.csv
- factset_predictive_metrics.csv
- factset_sector_predictive_metrics.csv
"""

from __future__ import annotations

import bisect
import csv
import math
import html
import os
import re
import statistics
import urllib.request
from dataclasses import dataclass
from datetime import date, datetime
from html.parser import HTMLParser
from typing import Dict, Iterable, List, Optional, Tuple

OUTPUT_DIR = "data/factset_eps_study"
SPX_CSV_URL = "https://stooq.com/q/d/l/?s=%5Espx&i=d"

# 14 consecutive quarters (Q4 2022 -> Q1 2026), all "first two months" FactSet snapshots.
SOURCES: List[Dict[str, object]] = [
    {
        "quarter": "Q42022",
        "url": "https://insight.factset.com/larger-cuts-than-average-to-eps-estimates-for-sp-500-companies-for-q4-to-date",
        "sector_values": {
            "Energy": 6.2,
            "Utilities": 2.5,
            "Industrials": -1.5,
            "Financials": -2.4,
            "Real Estate": -3.2,
            "Consumer Staples": -3.9,
            "S&P 500": -5.6,
            "Health Care": -6.9,
            "Information Technology": -8.2,
            "Communication Services": -11.4,
            "Consumer Discretionary": -12.2,
            "Materials": -21.3,
        },
    },
    {
        "quarter": "Q12023",
        "url": "https://insight.factset.com/larger-cuts-than-average-to-eps-estimates-for-sp-500-companies-for-q1-to-date",
        "sector_values": {
            "Utilities": 3.1,
            "Financials": -1.5,
            "Consumer Staples": -3.7,
            "Information Technology": -5.0,
            "Communication Services": -5.4,
            "S&P 500": -5.7,
            "Real Estate": -6.6,
            "Energy": -7.7,
            "Industrials": -8.2,
            "Consumer Discretionary": -8.5,
            "Health Care": -8.6,
            "Materials": -12.6,
        },
    },
    {
        "quarter": "Q22023",
        "url": "https://insight.factset.com/analysts-making-smaller-cuts-than-average-to-eps-estimates-for-sp-500-companies-for-q2-1",
        "sector_values": {
            "Communication Services": 1.7,
            "Information Technology": 1.3,
            "Industrials": -0.3,
            "Financials": -0.4,
            "Consumer Discretionary": -0.6,
            "Real Estate": -1.1,
            "S&P 500": -2.0,
            "Utilities": -2.2,
            "Health Care": -3.9,
            "Consumer Staples": -5.1,
            "Materials": -7.9,
            "Energy": -12.1,
        },
    },
    {
        "quarter": "Q32023",
        "url": "https://insight.factset.com/analysts-are-raising-quarterly-sp-500-eps-estimates-for-the-first-time-since-q3-2021",
        "sector_values": {
            "Consumer Discretionary": 6.4,
            "Communication Services": 5.3,
            "Information Technology": 3.8,
            "Energy": 0.7,
            "S&P 500": 0.4,
            "Financials": -0.2,
            "Real Estate": -0.2,
            "Utilities": -1.3,
            "Industrials": -1.7,
            "Consumer Staples": -2.9,
            "Health Care": -3.6,
            "Materials": -12.4,
        },
    },
    {
        "quarter": "Q42023",
        "url": "https://insight.factset.com/analysts-making-larger-cuts-than-average-to-eps-estimates-for-sp-500-companies-for-q4",
        "sector_values": {
            "Information Technology": 1.5,
            "Energy": -1.1,
            "Utilities": -2.0,
            "Financials": -2.3,
            "Real Estate": -2.8,
            "Consumer Staples": -4.0,
            "S&P 500": -5.0,
            "Industrials": -6.3,
            "Consumer Discretionary": -6.4,
            "Materials": -12.7,
            "Health Care": -19.9,
        },
    },
    {
        "quarter": "Q12024",
        "url": "https://insight.factset.com/analysts-making-smaller-cuts-than-average-to-eps-estimates-for-sp-500-companies-for-q1-1",
        "sector_values": {
            "Consumer Discretionary": 2.3,
            "Communication Services": 2.0,
            "Utilities": 0.4,
            "Information Technology": 0.4,
            "Financials": -0.2,
            "Real Estate": -1.2,
            "S&P 500": -2.2,
            "Consumer Staples": -4.1,
            "Industrials": -4.7,
            "Health Care": -5.4,
            "Materials": -11.8,
            "Energy": -12.0,
        },
    },
    {
        "quarter": "Q22024",
        "url": "https://insight.factset.com/analysts-have-increased-eps-estimates-for-sp-500-companies-for-q2-2024-since-march-31",
        "sector_values": {
            "Energy": 6.2,
            "Communication Services": 1.9,
            "Financials": 1.3,
            "Consumer Discretionary": 1.3,
            "Information Technology": 0.6,
            "S&P 500": 0.3,
            "Materials": -0.3,
            "Real Estate": -0.6,
            "Utilities": -0.9,
            "Health Care": -1.1,
            "Consumer Staples": -2.0,
            "Industrials": -4.3,
        },
    },
    {
        "quarter": "Q32024",
        "url": "https://insight.factset.com/are-analysts-cutting-eps-estimates-more-than-average-for-sp-500-companies-for-q3-1",
        "sector_values": {
            "Information Technology": 0.0,
            "Communication Services": -0.1,
            "Financials": -0.5,
            "Real Estate": -2.1,
            "Utilities": -2.5,
            "Consumer Discretionary": -2.6,
            "Consumer Staples": -2.6,
            "S&P 500": -2.8,
            "Health Care": -5.0,
            "Materials": -6.2,
            "Industrials": -7.0,
            "Energy": -11.8,
        },
    },
    {
        "quarter": "Q42024",
        "url": "https://insight.factset.com/are-analysts-cutting-eps-estimates-more-than-average-for-sp-500-companies-for-q4-1",
        "sector_values": {
            "Communication Services": 4.2,
            "Financials": 0.0,
            "Real Estate": -0.3,
            "Information Technology": -0.7,
            "Utilities": -0.8,
            "Consumer Discretionary": -1.7,
            "S&P 500": -2.5,
            "Consumer Staples": -4.1,
            "Industrials": -6.3,
            "Health Care": -7.4,
            "Materials": -11.0,
            "Energy": -12.0,
        },
    },
    {
        "quarter": "Q12025",
        "url": "https://insight.factset.com/analysts-making-larger-cuts-than-average-to-eps-estimates-for-sp-500-companies-for-q1-1",
        "sector_values": {
            "Utilities": -0.6,
            "Information Technology": -0.8,
            "Real Estate": -1.4,
            "Health Care": -2.5,
            "Communication Services": -3.5,
            "S&P 500": -3.5,
            "Financials": -3.8,
            "Energy": -4.2,
            "Industrials": -5.3,
            "Consumer Staples": -5.6,
            "Consumer Discretionary": -8.8,
            "Materials": -16.2,
        },
    },
    {
        "quarter": "Q22025",
        "url": "https://insight.factset.com/analysts-making-larger-cuts-than-average-to-eps-estimates-for-sp-500-companies-for-q2-1",
        "sector_values": {
            "Communication Services": -0.2,
            "Real Estate": -0.7,
            "Utilities": -1.1,
            "Information Technology": -2.1,
            "Financials": -3.1,
            "Materials": -3.9,
            "Consumer Staples": -4.0,
            "S&P 500": -4.0,
            "Health Care": -4.9,
            "Industrials": -5.7,
            "Consumer Discretionary": -6.6,
            "Energy": -18.9,
        },
    },
    {
        "quarter": "Q32025",
        "url": "https://insight.factset.com/analysts-increasing-eps-estimates-slightly-for-sp-500-companies-for-q3-1",
        "sector_values": {
            "Information Technology": 4.4,
            "Energy": 4.0,
            "Communication Services": 2.6,
            "Financials": 1.6,
            "Consumer Discretionary": 0.5,
            "S&P 500": 0.5,
            "Utilities": 0.0,
            "Real Estate": -1.3,
            "Industrials": -2.8,
            "Materials": -2.9,
            "Consumer Staples": -3.3,
            "Health Care": -7.2,
        },
    },
    {
        "quarter": "Q42025",
        "url": "https://insight.factset.com/analysts-increasing-eps-estimates-slightly-for-sp-500-companies-for-q4",
        "sector_values": {
            "Information Technology": 4.5,
            "Financials": 1.3,
            "Energy": 0.5,
            "S&P 500": 0.3,
            "Real Estate": -0.5,
            "Communication Services": -0.6,
            "Consumer Discretionary": -2.2,
            "Industrials": -2.4,
            "Materials": -3.1,
            "Health Care": -3.9,
            "Utilities": -4.1,
            "Consumer Staples": -4.3,
        },
    },
    {
        "quarter": "Q12026",
        "url": "https://insight.factset.com/analysts-lowering-quarterly-eps-estimates-for-first-time-since-q2-2025",
        "sector_values": {
            "Information Technology": 5.2,
            "Communication Services": 0.1,
            "Utilities": 0.0,
            "Financials": -0.1,
            "Real Estate": -0.5,
            "S&P 500": -1.5,
            "Materials": -2.1,
            "Industrials": -2.9,
            "Consumer Staples": -3.7,
            "Consumer Discretionary": -5.1,
            "Energy": -12.3,
            "Health Care": -13.2,
        },
    },
]


class PostBodyParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.capture = False
        self.depth = 0
        self.chunks: List[str] = []

    def handle_starttag(self, tag: str, attrs: List[Tuple[str, Optional[str]]]) -> None:
        attrs_dict = dict(attrs)
        if attrs_dict.get("id") == "hs_cos_wrapper_post_body":
            self.capture = True
            self.depth = 1
            return
        if self.capture:
            self.depth += 1

    def handle_endtag(self, tag: str) -> None:
        if self.capture:
            self.depth -= 1
            if self.depth <= 0:
                self.capture = False
                self.depth = 0

    def handle_data(self, data: str) -> None:
        if self.capture:
            self.chunks.append(data)


@dataclass
class IndexRevisionRow:
    quarter: str
    article_date: str
    observation_start: str
    observation_end: str
    index_change_pct: float
    source_url: str
    source_title: str
    sector_chart_url: str


@dataclass
class ForwardReturnRow:
    quarter: str
    article_date: str
    trade_date: str
    index_change_pct: float
    return_1w: Optional[float]
    return_1m: Optional[float]
    return_3m: Optional[float]
    return_6m: Optional[float]
    return_1y: Optional[float]


def fetch_text(url: str) -> str:
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 (compatible; CodexBot/1.0)"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return resp.read().decode("utf-8", "ignore")


def extract_post_body_text(html_text: str) -> str:
    parser = PostBodyParser()
    parser.feed(html_text)
    return " ".join(" ".join(parser.chunks).split())


def parse_quarter(text: str) -> Optional[str]:
    m = re.search(r"for\s+(Q[1-4]\s*20\d{2})", text, re.IGNORECASE)
    if m:
        return m.group(1).upper().replace(" ", "")
    m = re.search(r"for\s+(Q[1-4]\d{2})", text, re.IGNORECASE)
    if m:
        q = m.group(1).upper()
        return f"Q{q[1]}20{q[2:]}"
    return None


def parse_index_change_pct(text: str) -> Optional[float]:
    m = re.search(
        r"The\s+Q[1-4]\s+bottom-up EPS estimate.*?(decreased|increased|rose|fell).*?by\s+([+-]?\d+\.?\d*)%",
        text,
        re.IGNORECASE,
    )
    if not m:
        return None
    direction = m.group(1).lower()
    value = float(m.group(2))
    if direction in {"decreased", "fell"} and value > 0:
        value = -value
    return value


def parse_date_range(text: str) -> Tuple[Optional[str], Optional[str]]:
    m = re.search(r"from\s+([A-Za-z]+\s+\d{1,2})\s+to\s+([A-Za-z]+\s+\d{1,2})", text)
    if not m:
        return None, None
    return m.group(1), m.group(2)


def parse_article_date(html_text: str) -> Optional[str]:
    matches = re.findall(r'"datePublished"\s*:\s*"([0-9]{4}-[0-9]{2}-[0-9]{2})(?:[^\"]*)"', html_text)
    return matches[0] if matches else None


def parse_title(html_text: str) -> str:
    m = re.search(r"<title>(.*?)</title>", html_text, re.IGNORECASE | re.DOTALL)
    return html.unescape(m.group(1).strip()) if m else ""


def parse_sector_chart_url(html_text: str) -> str:
    patterns = [
        r'https://insight\.factset\.com/hs-fs/hubfs/[^"\?]*sector-level-change-in-q[^"\?]*\.png',
        r'https://insight\.factset\.com/hs-fs/hubfs/[^"\?]*change-in-sector-level[^"\?]*\.png',
    ]
    for pat in patterns:
        m = re.search(pat, html_text, re.IGNORECASE)
        if m:
            return m.group(0)
    return ""


def build_index_rows() -> List[IndexRevisionRow]:
    rows: List[IndexRevisionRow] = []
    for entry in SOURCES:
        quarter = str(entry["quarter"])
        url = str(entry["url"])
        html_text = fetch_text(url)
        body_text = extract_post_body_text(html_text)

        parsed_quarter = parse_quarter(body_text)
        if parsed_quarter != quarter:
            raise ValueError(f"Quarter mismatch for {url}: expected {quarter}, got {parsed_quarter}")

        index_change = parse_index_change_pct(body_text)
        if index_change is None:
            raise ValueError(f"Could not parse index change for {url}")
        sector_values: Dict[str, float] = entry["sector_values"]  # type: ignore[assignment]
        chart_spx_value = sector_values.get("S&P 500")
        if chart_spx_value is None:
            raise ValueError(f"Missing S&P 500 value in sector_values for {quarter}")
        if abs(chart_spx_value - index_change) > 0.11:
            raise ValueError(
                f"Index mismatch for {quarter}: parsed={index_change:.1f}, chart={chart_spx_value:.1f}"
            )

        start_text, end_text = parse_date_range(body_text)
        if not start_text or not end_text:
            raise ValueError(f"Could not parse date range for {url}")

        article_date = parse_article_date(html_text)
        if article_date is None:
            raise ValueError(f"Could not parse article date for {url}")

        rows.append(
            IndexRevisionRow(
                quarter=quarter,
                article_date=article_date,
                observation_start=start_text,
                observation_end=end_text,
                index_change_pct=index_change,
                source_url=url,
                source_title=parse_title(html_text),
                sector_chart_url=parse_sector_chart_url(html_text),
            )
        )

    return sorted(rows, key=lambda r: (r.article_date, r.quarter))


def write_index_csv(rows: Iterable[IndexRevisionRow], path: str) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "quarter",
                "article_date",
                "observation_start",
                "observation_end",
                "index_change_pct",
                "source_url",
                "source_title",
                "sector_chart_url",
            ]
        )
        for row in rows:
            writer.writerow(
                [
                    row.quarter,
                    row.article_date,
                    row.observation_start,
                    row.observation_end,
                    f"{row.index_change_pct:.1f}",
                    row.source_url,
                    row.source_title,
                    row.sector_chart_url,
                ]
            )


def write_sector_csv(index_rows: List[IndexRevisionRow], path: str) -> None:
    quarter_to_date = {r.quarter: r.article_date for r in index_rows}
    quarter_to_url = {r.quarter: r.source_url for r in index_rows}

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["quarter", "article_date", "sector", "eps_revision_pct", "source_url"])
        for entry in SOURCES:
            quarter = str(entry["quarter"])
            sector_values: Dict[str, float] = entry["sector_values"]  # type: ignore[assignment]
            for sector, value in sorted(sector_values.items(), key=lambda kv: kv[0]):
                writer.writerow([quarter, quarter_to_date[quarter], sector, f"{value:.1f}", quarter_to_url[quarter]])


def parse_stooq_spx() -> Tuple[List[date], List[float]]:
    text = fetch_text(SPX_CSV_URL)
    rows = list(csv.DictReader(text.splitlines()))
    dates: List[date] = []
    closes: List[float] = []
    for row in rows:
        d = datetime.strptime(row["Date"], "%Y-%m-%d").date()
        if d.year < 1980:
            continue
        dates.append(d)
        closes.append(float(row["Close"]))
    if not dates:
        raise ValueError("No SPX rows parsed from Stooq")
    return dates, closes


def first_trading_index_on_or_after(trading_dates: List[date], target: date) -> Optional[int]:
    i = bisect.bisect_left(trading_dates, target)
    return i if i < len(trading_dates) else None


def compute_forward_returns(index_rows: List[IndexRevisionRow]) -> List[ForwardReturnRow]:
    trading_dates, closes = parse_stooq_spx()
    horizons = {
        "return_1w": 5,
        "return_1m": 21,
        "return_3m": 63,
        "return_6m": 126,
        "return_1y": 252,
    }

    out: List[ForwardReturnRow] = []
    for row in index_rows:
        event_date = datetime.strptime(row.article_date, "%Y-%m-%d").date()
        idx = first_trading_index_on_or_after(trading_dates, event_date)
        if idx is None:
            continue
        base_close = closes[idx]

        vals: Dict[str, Optional[float]] = {}
        for label, offset in horizons.items():
            j = idx + offset
            vals[label] = (closes[j] / base_close - 1.0) if j < len(closes) else None

        out.append(
            ForwardReturnRow(
                quarter=row.quarter,
                article_date=row.article_date,
                trade_date=trading_dates[idx].isoformat(),
                index_change_pct=row.index_change_pct,
                return_1w=vals["return_1w"],
                return_1m=vals["return_1m"],
                return_3m=vals["return_3m"],
                return_6m=vals["return_6m"],
                return_1y=vals["return_1y"],
            )
        )
    return out


def write_forward_returns_csv(rows: Iterable[ForwardReturnRow], path: str) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "quarter",
                "article_date",
                "trade_date",
                "index_change_pct",
                "return_1w",
                "return_1m",
                "return_3m",
                "return_6m",
                "return_1y",
            ]
        )
        for r in rows:
            writer.writerow(
                [
                    r.quarter,
                    r.article_date,
                    r.trade_date,
                    f"{r.index_change_pct:.1f}",
                    "" if r.return_1w is None else f"{r.return_1w:.6f}",
                    "" if r.return_1m is None else f"{r.return_1m:.6f}",
                    "" if r.return_3m is None else f"{r.return_3m:.6f}",
                    "" if r.return_6m is None else f"{r.return_6m:.6f}",
                    "" if r.return_1y is None else f"{r.return_1y:.6f}",
                ]
            )


def pearson_corr(xs: List[float], ys: List[float]) -> Optional[float]:
    if len(xs) < 2:
        return None
    mean_x = statistics.fmean(xs)
    mean_y = statistics.fmean(ys)
    num = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys))
    den_x = sum((x - mean_x) ** 2 for x in xs)
    den_y = sum((y - mean_y) ** 2 for y in ys)
    if den_x <= 0 or den_y <= 0:
        return None
    return num / math.sqrt(den_x * den_y)


def linear_beta(xs: List[float], ys: List[float]) -> Optional[float]:
    if len(xs) < 2:
        return None
    mean_x = statistics.fmean(xs)
    den = sum((x - mean_x) ** 2 for x in xs)
    if den <= 0:
        return None
    mean_y = statistics.fmean(ys)
    num = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys))
    return num / den


def directional_hit_rate(xs: List[float], ys: List[float]) -> Optional[float]:
    pairs = [(x, y) for x, y in zip(xs, ys) if x != 0 and y != 0]
    if not pairs:
        return None
    hits = sum(1 for x, y in pairs if (x > 0 and y > 0) or (x < 0 and y < 0))
    return hits / len(pairs)


def mean_or_none(values: List[float]) -> Optional[float]:
    return statistics.fmean(values) if values else None


def format_float(v: Optional[float], digits: int = 6) -> str:
    return "" if v is None else f"{v:.{digits}f}"


def build_index_predictive_metrics(forward_rows: List[ForwardReturnRow]) -> List[Dict[str, object]]:
    horizons = ["return_1w", "return_1m", "return_3m", "return_6m", "return_1y"]
    out: List[Dict[str, object]] = []

    for h in horizons:
        xs: List[float] = []
        ys: List[float] = []
        for r in forward_rows:
            y = getattr(r, h)
            if y is None:
                continue
            xs.append(r.index_change_pct)
            ys.append(y)

        n = len(xs)
        corr = pearson_corr(xs, ys)
        beta = linear_beta(xs, ys)
        hit_rate = directional_hit_rate(xs, ys)
        mean_all = mean_or_none(ys)
        mean_pos = mean_or_none([y for x, y in zip(xs, ys) if x > 0])
        mean_neg = mean_or_none([y for x, y in zip(xs, ys) if x < 0])

        out.append(
            {
                "horizon": h,
                "n": n,
                "corr": corr,
                "r2": None if corr is None else corr * corr,
                "beta_return_per_1pct_revision": beta,
                "hit_rate_directional": hit_rate,
                "mean_return": mean_all,
                "mean_return_if_revision_positive": mean_pos,
                "mean_return_if_revision_negative": mean_neg,
            }
        )

    return out


def write_index_metrics_csv(metrics: List[Dict[str, object]], path: str) -> None:
    fields = [
        "horizon",
        "n",
        "corr",
        "r2",
        "beta_return_per_1pct_revision",
        "hit_rate_directional",
        "mean_return",
        "mean_return_if_revision_positive",
        "mean_return_if_revision_negative",
    ]
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for row in metrics:
            writer.writerow(
                {
                    "horizon": row["horizon"],
                    "n": row["n"],
                    "corr": format_float(row["corr"]),
                    "r2": format_float(row["r2"]),
                    "beta_return_per_1pct_revision": format_float(row["beta_return_per_1pct_revision"]),
                    "hit_rate_directional": format_float(row["hit_rate_directional"]),
                    "mean_return": format_float(row["mean_return"]),
                    "mean_return_if_revision_positive": format_float(row["mean_return_if_revision_positive"]),
                    "mean_return_if_revision_negative": format_float(row["mean_return_if_revision_negative"]),
                }
            )


def build_sector_predictive_metrics(
    sector_csv_path: str, forward_rows: List[ForwardReturnRow]
) -> List[Dict[str, object]]:
    quarter_returns: Dict[str, Dict[str, Optional[float]]] = {
        r.quarter: {
            "return_1w": r.return_1w,
            "return_1m": r.return_1m,
            "return_3m": r.return_3m,
            "return_6m": r.return_6m,
            "return_1y": r.return_1y,
        }
        for r in forward_rows
    }

    sector_rows: List[Dict[str, str]] = []
    with open(sector_csv_path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if row["sector"] == "S&P 500":
                continue
            sector_rows.append(row)

    sectors = sorted({r["sector"] for r in sector_rows})
    horizons = ["return_1w", "return_1m", "return_3m", "return_6m", "return_1y"]
    out: List[Dict[str, object]] = []

    for sector in sectors:
        rows = [r for r in sector_rows if r["sector"] == sector]
        for h in horizons:
            xs: List[float] = []
            ys: List[float] = []
            for r in rows:
                q = r["quarter"]
                y = quarter_returns.get(q, {}).get(h)
                if y is None:
                    continue
                xs.append(float(r["eps_revision_pct"]))
                ys.append(y)

            pairs = [(x, y) for x, y in zip(xs, ys) if x != 0 and y != 0]
            hits = sum(1 for x, y in pairs if (x > 0 and y > 0) or (x < 0 and y < 0))
            misses = len(pairs) - hits
            hit_rate = (hits / len(pairs)) if pairs else None

            corr = pearson_corr(xs, ys)
            beta = linear_beta(xs, ys)
            out.append(
                {
                    "sector": sector,
                    "horizon": h,
                    "n": len(xs),
                    "n_directional": len(pairs),
                    "hits": hits,
                    "misses": misses,
                    "hit_rate_directional": hit_rate,
                    "corr": corr,
                    "r2": None if corr is None else corr * corr,
                    "beta_return_per_1pct_revision": beta,
                }
            )

    return out


def write_sector_metrics_csv(metrics: List[Dict[str, object]], path: str) -> None:
    fields = [
        "sector",
        "horizon",
        "n",
        "n_directional",
        "hits",
        "misses",
        "hit_rate_directional",
        "corr",
        "r2",
        "beta_return_per_1pct_revision",
    ]
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for row in metrics:
            writer.writerow(
                {
                    "sector": row["sector"],
                    "horizon": row["horizon"],
                    "n": row["n"],
                    "n_directional": row["n_directional"],
                    "hits": row["hits"],
                    "misses": row["misses"],
                    "hit_rate_directional": format_float(row["hit_rate_directional"]),
                    "corr": format_float(row["corr"]),
                    "r2": format_float(row["r2"]),
                    "beta_return_per_1pct_revision": format_float(row["beta_return_per_1pct_revision"]),
                }
            )


def main() -> None:
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    index_rows = build_index_rows()
    index_csv_path = os.path.join(OUTPUT_DIR, "factset_index_revisions.csv")
    write_index_csv(index_rows, index_csv_path)

    sector_csv_path = os.path.join(OUTPUT_DIR, "factset_sector_revisions.csv")
    write_sector_csv(index_rows, sector_csv_path)

    forward_rows = compute_forward_returns(index_rows)
    forward_csv_path = os.path.join(OUTPUT_DIR, "factset_spx_forward_returns.csv")
    write_forward_returns_csv(forward_rows, forward_csv_path)

    index_metrics = build_index_predictive_metrics(forward_rows)
    index_metrics_path = os.path.join(OUTPUT_DIR, "factset_predictive_metrics.csv")
    write_index_metrics_csv(index_metrics, index_metrics_path)

    sector_metrics = build_sector_predictive_metrics(sector_csv_path, forward_rows)
    sector_metrics_path = os.path.join(OUTPUT_DIR, "factset_sector_predictive_metrics.csv")
    write_sector_metrics_csv(sector_metrics, sector_metrics_path)

    print("Wrote:")
    print(f"- {index_csv_path}")
    print(f"- {sector_csv_path}")
    print(f"- {forward_csv_path}")
    print(f"- {index_metrics_path}")
    print(f"- {sector_metrics_path}")


if __name__ == "__main__":
    main()
