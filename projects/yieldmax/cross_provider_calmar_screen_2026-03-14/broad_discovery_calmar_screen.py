#!/usr/bin/env python3
from __future__ import annotations

from datetime import date
from pathlib import Path
import html
import re
import time
from typing import Iterable

import numpy as np
import pandas as pd
import requests
import yfinance as yf

STOCKANALYSIS_SCREENER_URL = "https://stockanalysis.com/etf/screener/"
BENCHMARKS = ("SPY", "QQQ")
MAX_RETRIES = 4
REQUEST_TIMEOUT = 60

# Non-single YieldMax ETFs from YieldMax taxonomy (already validated in prior run).
YIELDMAX_NON_SINGLE = {
    "CHPY",
    "GDXY",
    "SOXY",
    "YMAG",
    "BIGY",
    "RNTY",
    "YMAX",
    "GPTY",
    "ULTY",
    "LFGY",
    "FIVY",
    "SLTY",
    "FEAT",
    "MINY",
}

# Strong thematic keywords for option-income / derivative-income ETF discovery.
KEYWORDS = [
    "option income",
    "option strategy",
    "covered call",
    "0dte",
    "equity premium",
    "premium income",
    "enhanced options",
    "buy-write",
    "buy write",
    "buywrite",
    "putwrite",
    "put-write",
    "put write",
    "collar",
    "barrier income",
    "target distribution",
    "income edge",
]

# Seed symbols to force include important funds that sometimes evade keyword matching.
SEED_TICKERS = {
    "SBAR",
    "JEPI",
    "JEPQ",
    "DIVO",
    "IDVO",
    "SPYI",
    "QQQI",
    "IWMI",
    "FTQI",
    "PBP",
    "NUSI",
    "QYLD",
    "XYLD",
    "RYLD",
    "QYLG",
    "XYLG",
    "GPIX",
    "GPIQ",
    "TCAL",
    "PAPI",
    "NBOS",
    "BALI",
    "BUCK",
    "QDTE",
    "XDTE",
    "RDTE",
    "FEPI",
    "AIPI",
    "CEPI",
    "QQQY",
    "IWMY",
    "JEPY",
    "SPYT",
}

# Multiple search phrases to broaden internet discovery via Yahoo search API through yfinance.
SEARCH_PHRASES = [
    "covered call etf",
    "option income etf",
    "equity premium income etf",
    "premium income etf",
    "buy write etf",
    "putwrite etf",
    "0dte covered call etf",
    "barrier income etf",
    "target distribution etf",
    "income edge etf",
    "collar etf",
    "enhanced options income etf",
]

KNOWN_SINGLE_STOCK_OPTION_HINTS = [
    " aapl option income strategy etf",
    " amzn option income strategy etf",
    " tsla option income strategy etf",
    " nvda option income strategy etf",
    " mstr option income strategy etf",
    " coin option income strategy etf",
    " meta option income strategy etf",
    " msft option income strategy etf",
    " googl option income strategy etf",
    " nflx option income strategy etf",
    " pltr option income strategy etf",
    " pypl option income strategy etf",
    " smci option income strategy etf",
]


def fetch_text(url: str) -> str:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(url, timeout=REQUEST_TIMEOUT)
            r.raise_for_status()
            return r.text
        except Exception:
            if attempt < MAX_RETRIES:
                time.sleep(1.5 * attempt)
    return ""


def parse_stockanalysis_etfs(html_text: str) -> list[tuple[str, str]]:
    # Parse entries like: {s:"VOO",n:"Vanguard S&P 500 ETF",assetClass:"Equity"...}
    pattern = re.compile(r'\{s:"([A-Z0-9\.\-]+)",n:"([^"]+)"')
    out: list[tuple[str, str]] = []
    for m in pattern.finditer(html_text):
        ticker = m.group(1).strip().upper()
        name = html.unescape(m.group(2).strip())
        if ticker and name:
            out.append((ticker, name))
    # dedupe keeping first
    seen = set()
    uniq: list[tuple[str, str]] = []
    for t, n in out:
        if t in seen:
            continue
        seen.add(t)
        uniq.append((t, n))
    return uniq


def keyword_match(text: str, keywords: Iterable[str]) -> bool:
    txt = text.lower()
    return any(k in txt for k in keywords)


def discover_candidates_from_stockanalysis() -> tuple[set[str], dict[str, str], int]:
    html_text = fetch_text(STOCKANALYSIS_SCREENER_URL)
    parsed = parse_stockanalysis_etfs(html_text)
    name_map = {t: n for t, n in parsed}

    candidates = set()
    for ticker, name in parsed:
        if keyword_match(name, KEYWORDS):
            candidates.add(ticker)

    return candidates, name_map, len(parsed)


def discover_candidates_from_search() -> tuple[set[str], dict[str, str]]:
    symbols: set[str] = set()
    names: dict[str, str] = {}
    for q in SEARCH_PHRASES:
        try:
            s = yf.Search(
                query=q,
                max_results=75,
                news_count=0,
                lists_count=0,
                recommended=0,
                include_cb=False,
                include_nav_links=False,
                enable_fuzzy_query=False,
            )
            quotes = s.quotes or []
        except Exception:
            quotes = []
        for quote in quotes:
            symbol = str(quote.get("symbol", "")).upper().strip()
            qtype = str(quote.get("quoteType", "")).upper().strip()
            shortname = str(quote.get("shortname", "")).strip()
            if not symbol:
                continue
            if qtype and qtype != "ETF":
                continue
            symbols.add(symbol)
            if shortname:
                names.setdefault(symbol, shortname)
        time.sleep(0.1)
    return symbols, names


def normalize_history(hist: pd.DataFrame) -> pd.DataFrame:
    if hist is None or hist.empty:
        return pd.DataFrame(columns=["Adj Close"])
    out = hist.copy().sort_index()
    if getattr(out.index, "tz", None) is not None:
        out.index = out.index.tz_localize(None)
    if "Adj Close" not in out.columns:
        out["Adj Close"] = np.nan
    out["Adj Close"] = pd.to_numeric(out["Adj Close"], errors="coerce")
    out = out.dropna(subset=["Adj Close"])
    return out[["Adj Close"]]


def fetch_info_and_history(ticker: str) -> tuple[dict, pd.DataFrame | None]:
    last_info: dict = {}
    last_hist: pd.DataFrame | None = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            tk = yf.Ticker(ticker)
            info = tk.info or {}
            hist = tk.history(period="max", interval="1d", auto_adjust=False, actions=True, timeout=30)
            hist = normalize_history(hist)
            last_info = info
            last_hist = hist if len(hist) >= 2 else None
            return last_info, last_hist
        except Exception:
            if attempt < MAX_RETRIES:
                time.sleep(1.2 * attempt)
    return last_info, last_hist


def is_us_like_symbol(ticker: str, exchange: str) -> bool:
    if "." in ticker:
        return False
    ex = (exchange or "").upper()
    # U.S. ETF venues commonly returned by Yahoo.
    us_exchanges = {"NMS", "NYQ", "PCX", "ASE", "BTS", "ARCX", "NGM", "NCM", "BATS"}
    if ex and ex not in us_exchanges:
        return False
    return True


def looks_like_single_stock_option(name: str, provider: str, ticker: str) -> bool:
    ln = f" {name.lower()} "
    lp = (provider or "").lower()

    if "yieldmax" in lp:
        return ticker not in YIELDMAX_NON_SINGLE

    if "single stock" in ln:
        return True

    # Many single-stock option wrappers follow this pattern.
    if " option income strategy etf" in ln:
        # If it's explicitly portfolio/index/universe, do not classify as single-stock.
        non_single_terms = [
            "portfolio",
            "universe",
            "sector",
            "u.s. stocks",
            "target",
            "magnificent",
            "miners",
            "real estate",
            "nasdaq",
            "r2000",
            "s&p",
        ]
        if not any(term in ln for term in non_single_terms):
            return True

    if any(h in ln for h in KNOWN_SINGLE_STOCK_OPTION_HINTS):
        return True

    # Exclude leveraged daily single-name products not in our target area.
    if ("2x long" in ln or "2x short" in ln or "3x long" in ln or "3x short" in ln) and "etf" in ln:
        if "covered call" not in ln and "income" not in ln:
            return True

    return False


def is_in_target_area(name: str, category: str, provider: str, ticker: str) -> bool:
    ln = (name or "").lower()
    cat = (category or "").lower()
    lp = (provider or "").lower()

    # Primary inclusion rule.
    if "derivative income" in cat:
        return True
    if keyword_match(ln, KEYWORDS):
        return True

    # Strong provider-specific fallbacks for known products in this area.
    known = {
        "jepi",
        "jepq",
        "divo",
        "idvo",
        "spyi",
        "qqqi",
        "iwmi",
        "ftqi",
        "pbp",
        "nusi",
        "gpix",
        "gpiq",
        "tcal",
        "papi",
        "bali",
        "nbos",
    }
    if ticker.lower() in known:
        return True

    # Some providers classify option-income funds with slightly different names.
    if any(p in lp for p in ["yieldmax", "neos", "defiance", "roundhill", "global x", "simplify", "rex", "jpmorgan", "amplify"]):
        if "income" in ln and "etf" in ln:
            # avoid generic dividend income names unless they also have option cues
            if any(k in ln for k in ["premium", "option", "covered call", "0dte", "barrier", "enhanced options", "target"]):
                return True

    return False


def calmar_on_window(adj: pd.Series) -> dict[str, float]:
    if adj is None or len(adj) < 2:
        return {
            "total_return": np.nan,
            "annualized_return": np.nan,
            "max_drawdown": np.nan,
            "calmar": np.nan,
            "trading_days": np.nan,
        }

    daily = adj.pct_change().replace([np.inf, -np.inf], np.nan).dropna()
    if len(daily) < 2:
        return {
            "total_return": np.nan,
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
    drawdown = wealth / wealth.cummax() - 1.0
    max_drawdown = float(drawdown.min()) if len(drawdown) else np.nan

    calmar = np.nan
    if np.isfinite(annualized_return) and np.isfinite(max_drawdown) and max_drawdown < 0:
        calmar = annualized_return / abs(max_drawdown)

    return {
        "total_return": total_return,
        "annualized_return": annualized_return,
        "max_drawdown": max_drawdown,
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


def main() -> None:
    out_dir = Path(__file__).resolve().parent
    as_of = date.today().isoformat()

    sa_candidates, sa_name_map, sa_total_count = discover_candidates_from_stockanalysis()
    search_candidates, search_name_map = discover_candidates_from_search()

    discovered = set(sa_candidates) | set(search_candidates) | set(SEED_TICKERS) | set(YIELDMAX_NON_SINGLE)

    # Add source-attributed list for transparency.
    source_rows: list[dict[str, object]] = []
    for t in sorted(discovered):
        source_rows.append(
            {
                "Ticker": t,
                "FromStockAnalysisKeywords": t in sa_candidates,
                "FromYahooSearchQueries": t in search_candidates,
                "FromSeeds": t in SEED_TICKERS,
                "FromYieldMaxNonSingleList": t in YIELDMAX_NON_SINGLE,
                "NameHint": sa_name_map.get(t, search_name_map.get(t, "")),
            }
        )
    source_df = pd.DataFrame(source_rows)
    source_df.to_csv(out_dir / "broad_discovery_source_candidates.csv", index=False)

    # Fetch benchmark histories.
    benchmark_hist: dict[str, pd.DataFrame] = {}
    for b in BENCHMARKS:
        hist = None
        for _ in range(MAX_RETRIES):
            h = fetch_info_and_history(b)[1]
            if h is not None and len(h) >= 2:
                hist = h
                break
        if hist is None:
            raise RuntimeError(f"Failed to fetch benchmark history for {b}")
        benchmark_hist[b] = hist

    common_end = min(benchmark_hist["SPY"].index.max(), benchmark_hist["QQQ"].index.max())
    common_start_target = common_end - pd.Timedelta(days=365)
    bm = {}
    for b in BENCHMARKS:
        w = benchmark_hist[b].loc[(benchmark_hist[b].index >= common_start_target) & (benchmark_hist[b].index <= common_end), "Adj Close"]
        bm[b] = calmar_on_window(w)

    spy_calmar = bm["SPY"]["calmar"]
    qqq_calmar = bm["QQQ"]["calmar"]
    avg_calmar = (spy_calmar + qqq_calmar) / 2.0

    # Validate candidates and compute 1Y metrics.
    accepted_rows: list[dict[str, object]] = []
    rejected_rows: list[dict[str, object]] = []
    unavailable_rows: list[dict[str, object]] = []

    candidate_list = sorted(discovered)
    for i, ticker in enumerate(candidate_list, start=1):
        print(f"[{i}/{len(candidate_list)}] Validate {ticker}...")
        info, hist = fetch_info_and_history(ticker)
        if hist is None:
            unavailable_rows.append({"Ticker": ticker, "Issue": "No/insufficient history"})
            continue

        quote_type = str(info.get("quoteType", "")).upper()
        long_name = str(info.get("longName") or info.get("shortName") or sa_name_map.get(ticker) or "")
        provider = str(info.get("fundFamily") or "")
        category = str(info.get("category") or "")
        exchange = str(info.get("exchange") or "")

        if quote_type and quote_type != "ETF":
            rejected_rows.append({"Ticker": ticker, "Reason": f"quoteType={quote_type}", "Name": long_name})
            continue
        if re.search(r"\bETN\b", long_name, flags=re.IGNORECASE):
            rejected_rows.append({"Ticker": ticker, "Reason": "ETN/Note instrument (not ETF)", "Name": long_name})
            continue
        if not is_us_like_symbol(ticker, exchange):
            rejected_rows.append({"Ticker": ticker, "Reason": f"non-US-like exchange={exchange}", "Name": long_name})
            continue
        if not is_in_target_area(long_name, category, provider, ticker):
            rejected_rows.append({"Ticker": ticker, "Reason": "not in option/derivative-income area", "Name": long_name})
            continue
        if looks_like_single_stock_option(long_name, provider, ticker):
            rejected_rows.append({"Ticker": ticker, "Reason": "likely single-stock option ETF", "Name": long_name})
            continue

        w = hist.loc[(hist.index >= common_start_target) & (hist.index <= common_end), "Adj Close"]
        used_partial = False
        if len(w) < 2:
            w = hist.loc[hist.index <= common_end, "Adj Close"]
            used_partial = True
        elif w.index.min() > common_start_target:
            used_partial = True

        m = calmar_on_window(w)
        accepted_rows.append(
            {
                "Provider": provider if provider else "Unknown",
                "Ticker": ticker,
                "Name": long_name,
                "Category": category,
                "Exchange": exchange,
                "Start": w.index.min().date().isoformat() if len(w) else np.nan,
                "End": w.index.max().date().isoformat() if len(w) else np.nan,
                "TradingDays": int(m["trading_days"]) if np.isfinite(m["trading_days"]) else np.nan,
                "AnnualizedReturn": m["annualized_return"],
                "MaxDrawdown": m["max_drawdown"],
                "Calmar": m["calmar"],
                "BeatsSPY": bool(np.isfinite(m["calmar"]) and m["calmar"] > spy_calmar),
                "BeatsQQQ": bool(np.isfinite(m["calmar"]) and m["calmar"] > qqq_calmar),
                "BeatsAvgSPYQQQ": bool(np.isfinite(m["calmar"]) and m["calmar"] > avg_calmar),
                "WindowNote": "Partial (<1Y history)" if used_partial else "Full 1Y",
            }
        )
        time.sleep(0.05)

    accepted = pd.DataFrame(accepted_rows)
    if accepted.empty:
        raise RuntimeError("No ETFs passed broad discovery filters.")

    accepted = accepted.sort_values(["Calmar", "AnnualizedReturn"], ascending=[False, False], na_position="last").reset_index(drop=True)
    accepted.insert(0, "Rank", range(1, len(accepted) + 1))
    accepted.to_csv(out_dir / "broad_discovery_non_single_1y_calmar_screen.csv", index=False)

    rejected = pd.DataFrame(rejected_rows).sort_values(["Ticker"]).reset_index(drop=True) if rejected_rows else pd.DataFrame(columns=["Ticker", "Reason", "Name"])
    rejected.to_csv(out_dir / "broad_discovery_rejected_candidates.csv", index=False)

    unavailable = pd.DataFrame(unavailable_rows).sort_values(["Ticker"]).reset_index(drop=True) if unavailable_rows else pd.DataFrame(columns=["Ticker", "Issue"])
    unavailable.to_csv(out_dir / "broad_discovery_unavailable_candidates.csv", index=False)

    both = accepted[accepted["BeatsSPY"] & accepted["BeatsQQQ"]].copy()
    beat_spy = accepted[accepted["BeatsSPY"]].copy()
    beat_qqq = accepted[accepted["BeatsQQQ"]].copy()
    beat_avg = accepted[accepted["BeatsAvgSPYQQQ"]].copy()

    headers = ["Rank", "Provider", "Ticker", "Start", "End", "TradingDays", "Annualized Return", "Max Drawdown", "Calmar", "Window", "Name"]

    def to_rows(df: pd.DataFrame) -> list[list[str]]:
        rows: list[list[str]] = []
        for _, r in df.iterrows():
            rows.append(
                [
                    str(int(r["Rank"])),
                    str(r["Provider"]),
                    str(r["Ticker"]),
                    str(r["Start"]),
                    str(r["End"]),
                    str(r["TradingDays"]),
                    fmt_pct(float(r["AnnualizedReturn"])),
                    fmt_pct(float(r["MaxDrawdown"])),
                    fmt_num(float(r["Calmar"]), 3),
                    str(r["WindowNote"]),
                    str(r["Name"]),
                ]
            )
        return rows

    source_stats_headers = ["Metric", "Count"]
    source_stats_rows = [
        ["StockAnalysis ETF total parsed", str(sa_total_count)],
        ["StockAnalysis keyword candidates", str(len(sa_candidates))],
        ["Yahoo search-query candidates", str(len(search_candidates))],
        ["Seed tickers", str(len(SEED_TICKERS))],
        ["Combined unique candidates", str(len(discovered))],
        ["Accepted final universe", str(len(accepted))],
        ["Rejected candidates", str(len(rejected))],
        ["Unavailable candidates", str(len(unavailable))],
    ]

    report_lines = [
        f"# Broad Discovery Non-Single ETF 1Y Calmar Screen (as of {as_of})",
        "",
        "## Discovery Approach",
        "- Source 1 (web API/page data): StockAnalysis ETF screener dataset (broad all-ETF universe).",
        "- Source 2 (search API): Yahoo search via `yfinance.Search` with multiple option-income query phrases.",
        "- Source 3 (issuer-specific): YieldMax non-single ETF taxonomy list.",
        "- Then validated each candidate with Yahoo market data + metadata and filtered to non-single option/derivative-income ETFs.",
        "",
        "## Discovery Stats",
        markdown_table(source_stats_headers, source_stats_rows),
        "",
        "## Benchmark Baseline (fixed same window)",
        f"- Window: **{common_start_target.date().isoformat()}** to **{common_end.date().isoformat()}**.",
        f"- SPY Calmar: **{fmt_num(float(spy_calmar), 3)}** (annualized return {fmt_pct(float(bm['SPY']['annualized_return']))}, max DD {fmt_pct(float(bm['SPY']['max_drawdown']))})",
        f"- QQQ Calmar: **{fmt_num(float(qqq_calmar), 3)}** (annualized return {fmt_pct(float(bm['QQQ']['annualized_return']))}, max DD {fmt_pct(float(bm['QQQ']['max_drawdown']))})",
        f"- Avg(SPY,QQQ) Calmar: **{fmt_num(float(avg_calmar), 3)}**",
        "",
        "## ETFs Beating Both SPY And QQQ Calmar",
        markdown_table(headers, to_rows(both)) if len(both) else "None",
        "",
        "## ETFs Beating SPY Calmar",
        markdown_table(headers, to_rows(beat_spy)) if len(beat_spy) else "None",
        "",
        "## ETFs Beating QQQ Calmar",
        markdown_table(headers, to_rows(beat_qqq)) if len(beat_qqq) else "None",
        "",
        "## ETFs Beating Avg(SPY,QQQ) Calmar",
        markdown_table(headers, to_rows(beat_avg)) if len(beat_avg) else "None",
        "",
        "## Full Ranked Universe",
        markdown_table(headers, to_rows(accepted)),
    ]

    (out_dir / "broad_discovery_non_single_1y_calmar_screen.md").write_text("\n".join(report_lines), encoding="utf-8")

    print(f"Saved: {out_dir / 'broad_discovery_non_single_1y_calmar_screen.md'}")
    print(f"Saved: {out_dir / 'broad_discovery_non_single_1y_calmar_screen.csv'}")
    print(f"Final accepted universe: {len(accepted)}")
    print(f"Beats SPY: {len(beat_spy)} | Beats QQQ: {len(beat_qqq)} | Beats both: {len(both)}")
    print(f"SPY Calmar={spy_calmar:.6f} QQQ Calmar={qqq_calmar:.6f} Avg={avg_calmar:.6f}")


if __name__ == "__main__":
    main()
