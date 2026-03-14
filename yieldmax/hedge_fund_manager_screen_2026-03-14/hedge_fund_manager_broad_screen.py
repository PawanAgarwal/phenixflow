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
ETFDB_SOURCE_URLS = [
    # Alternative and hedge-fund-like ETF category pages.
    "https://etfdb.com/etfdb-category/long-short/",
    "https://etfdb.com/etfdb-category/managed-futures/",
    "https://etfdb.com/etfdb-category/market-neutral/",
    "https://etfdb.com/etfdb-category/multi-alternative/",
    "https://etfdb.com/etfdb-category/global-macro/",
    "https://etfdb.com/etfdb-category/merger-arbitrage/",
]
RESEARCH_REFERENCE_URLS = [
    STOCKANALYSIS_SCREENER_URL,
    *ETFDB_SOURCE_URLS,
    "https://en.wikipedia.org/wiki/Pershing_Square_Holdings",
    "https://en.wikipedia.org/wiki/Man_Group",
]

BENCHMARKS = ("SPY", "QQQ")
HORIZONS_YEARS = [1, 2, 3, 4, 5]
MAX_RETRIES = 4
REQUEST_TIMEOUT = 60

DISCOVERY_TERMS = [
    "hedge fund",
    "hedged",
    "hedge",
    "managed futures",
    "managed risk",
    "market neutral",
    "long/short",
    "long short",
    "merger arbitrage",
    "arbitrage",
    "absolute return",
    "multi-strategy",
    "multi strategy",
    "global macro",
    "risk parity",
    "tail risk",
    "trend",
    "trend following",
    "alternative",
    "option income",
    "option strategy",
    "equity premium",
    "premium income",
    "covered call",
    "buy-write",
    "buywrite",
    "putwrite",
    "collar",
    "0dte",
]

TARGET_STRONG_TERMS = [
    "managed futures",
    "market neutral",
    "long/short",
    "long short",
    "merger arbitrage",
    "arbitrage",
    "absolute return",
    "multi-strategy",
    "multi strategy",
    "global macro",
    "risk parity",
    "tail risk",
    "trend following",
    "event-driven",
    "event driven",
    "option income",
    "option strategy",
    "equity premium",
    "premium income",
    "covered call",
    "buy-write",
    "buywrite",
    "putwrite",
    "collar",
    "0dte",
    "hedged equity",
    "managed risk",
]

TARGET_ALT_CATEGORIES = {
    "Derivative Income",
    "Equity Hedged",
    "Equity Market Neutral",
    "Long-Short Equity",
    "Event Driven",
    "Multistrategy",
    "Systematic Trend",
    "Relative Value Arbitrage",
    "Options Trading",
    "Trading--Inverse Equity",
    "Defined Outcome",
}

ETF_SEARCH_PHRASES = [
    "hedge fund strategy etf",
    "liquid alternative etf",
    "managed futures etf",
    "long short etf",
    "market neutral etf",
    "merger arbitrage etf",
    "global macro etf",
    "absolute return etf",
    "tail risk etf",
    "option income etf",
    "equity premium income etf",
]

MANAGER_SEARCH_PHRASES = [
    "publicly traded hedge fund manager",
    "publicly traded alternative asset manager",
    "listed hedge fund company",
]

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

# Seed ETFs across liquid alternatives + option-income alternatives.
ETF_SEEDS = {
    "DBMF",
    "KMLM",
    "CTA",
    "FMF",
    "WTMF",
    "MNA",
    "BTAL",
    "QAI",
    "TAIL",
    "CAOS",
    "RPAR",
    "RSBT",
    "GMOM",
    "IVOL",
    "FTLS",
    "QLS",
    "EQLS",
    "HDG",
    "HFND",
    "DBEH",
    "DBND",
    "DBAW",
    "NUSI",
    "SPYI",
    "QQQI",
    "IWMI",
    "JEPI",
    "JEPQ",
    "DIVO",
    "IDVO",
    "PBP",
    "QYLD",
    "XYLD",
    "QYLG",
    "XYLG",
    "GPIX",
    "GPIQ",
    "FTQI",
    "TCAL",
    "PAPI",
    "SBAR",
}

MANAGER_EQUITY_SEEDS = [
    {"Ticker": "BX", "Manager": "Blackstone", "Source": "Publicly traded alternative asset manager"},
    {"Ticker": "KKR", "Manager": "KKR", "Source": "Publicly traded alternative asset manager"},
    {"Ticker": "APO", "Manager": "Apollo Global Management", "Source": "Publicly traded alternative asset manager"},
    {"Ticker": "ARES", "Manager": "Ares Management", "Source": "Publicly traded alternative asset manager"},
    {"Ticker": "CG", "Manager": "Carlyle Group", "Source": "Publicly traded alternative asset manager"},
    {"Ticker": "OWL", "Manager": "Blue Owl Capital", "Source": "Publicly traded alternative asset manager"},
    {"Ticker": "TPG", "Manager": "TPG", "Source": "Publicly traded alternative asset manager"},
    {"Ticker": "BAM", "Manager": "Brookfield Asset Management", "Source": "Publicly traded alternative asset manager"},
    {"Ticker": "BN", "Manager": "Brookfield Corporation", "Source": "Publicly traded alternative asset manager"},
    {"Ticker": "GCMG", "Manager": "GCM Grosvenor", "Source": "Publicly traded alternative asset manager"},
    {"Ticker": "AMG", "Manager": "Affiliated Managers Group", "Source": "Publicly traded asset manager"},
    {"Ticker": "AB", "Manager": "AllianceBernstein", "Source": "Publicly traded asset manager"},
    {"Ticker": "STEP", "Manager": "StepStone Group", "Source": "Publicly traded alternative asset manager"},
    {"Ticker": "EMG.L", "Manager": "Man Group", "Source": "Publicly traded hedge fund manager"},
    {"Ticker": "MNGPF", "Manager": "Man Group", "Source": "US OTC listing of Man Group"},
    {"Ticker": "PSH.AS", "Manager": "Pershing Square", "Source": "Listed hedge fund vehicle (Pershing Square Holdings)"},
    {"Ticker": "PSHZF", "Manager": "Pershing Square", "Source": "US OTC listing of Pershing Square Holdings"},
    {"Ticker": "BHMG.L", "Manager": "Brevan Howard", "Source": "Listed hedge fund vehicle (BH Macro)"},
    {"Ticker": "BHGU.L", "Manager": "Brevan Howard", "Source": "Listed hedge fund vehicle (BH Global)"},
    {"Ticker": "TFG.L", "Manager": "Tetragon", "Source": "Listed alternative investments vehicle"},
]

MANAGER_KEYWORDS = {
    "yieldmax": "YieldMax",
    "jpmorgan": "JPMorgan",
    "neos": "NEOS",
    "global x": "Global X",
    "first trust": "First Trust",
    "goldman": "Goldman Sachs",
    "simplify": "Simplify",
    "roundhill": "Roundhill",
    "amplify": "Amplify",
    "advisorshares": "AdvisorShares",
    "invesco": "Invesco",
    "ishares": "BlackRock/iShares",
    "wisdomtree": "WisdomTree",
    "defiance": "Defiance",
    "innovator": "Innovator",
    "aptus": "Aptus",
    "main management": "Main Management",
    "hartford": "Hartford",
    "sterling": "Sterling",
    "kensington": "Kensington",
    "rex": "REX",
    "blackrock": "BlackRock/iShares",
}

ALLOWED_MANAGER_QUOTE_TYPES = {
    "EQUITY",
    "COMMON STOCK",
    "STOCK",
    "CLOSE_END_FUND",
}


def fetch_text(url: str) -> str:
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(url, timeout=REQUEST_TIMEOUT, headers=headers)
            if r.status_code in {403, 404}:
                # Hard-denied or missing page; skip quickly.
                return ""
            r.raise_for_status()
            return r.text
        except Exception:
            if attempt < MAX_RETRIES:
                time.sleep(1.2 * attempt)
    return ""


def parse_stockanalysis_etfs(html_text: str) -> list[tuple[str, str]]:
    # Format example inside script payload: {s:"VOO",n:"Vanguard S&P 500 ETF",...}
    pattern = re.compile(r'\{s:"([A-Z0-9\.\-]+)",n:"([^"]+)"')
    out: list[tuple[str, str]] = []
    for m in pattern.finditer(html_text):
        ticker = m.group(1).strip().upper()
        name = html.unescape(m.group(2).strip())
        if ticker and name:
            out.append((ticker, name))

    seen = set()
    uniq: list[tuple[str, str]] = []
    for t, n in out:
        if t in seen:
            continue
        seen.add(t)
        uniq.append((t, n))
    return uniq


def parse_tickers_from_etfdb(html_text: str) -> set[str]:
    # ETFDB link pattern typically includes /etf/TICKER/
    tickers = set(re.findall(r"/etf/([A-Z0-9\.\-]{1,12})/", html_text))
    return {t.upper().strip() for t in tickers if t}


def text_has_terms(text: str, terms: Iterable[str]) -> bool:
    low = (text or "").lower()
    return any(term in low for term in terms)


def normalize_adj_close(hist: pd.DataFrame) -> pd.Series:
    if hist is None or hist.empty or "Adj Close" not in hist.columns:
        return pd.Series(dtype=float)
    out = hist.copy().sort_index()
    if getattr(out.index, "tz", None) is not None:
        out.index = out.index.tz_localize(None)
    s = pd.to_numeric(out["Adj Close"], errors="coerce").dropna()
    return s


def fetch_info_and_adj_close(ticker: str) -> tuple[dict, pd.Series]:
    info: dict = {}
    series = pd.Series(dtype=float)
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            tk = yf.Ticker(ticker)
            info = tk.info or {}
            hist = tk.history(
                period="max",
                interval="1d",
                auto_adjust=False,
                actions=True,
                timeout=30,
            )
            series = normalize_adj_close(hist)
            return info, series
        except Exception:
            if attempt < MAX_RETRIES:
                time.sleep(1.2 * attempt)
    return info, series


def is_us_like_symbol(ticker: str, exchange: str) -> bool:
    if "." in ticker:
        return False
    ex = (exchange or "").upper()
    us_exchanges = {"NMS", "NYQ", "PCX", "ASE", "BTS", "ARCX", "NGM", "NCM", "BATS"}
    return (not ex) or (ex in us_exchanges)


def looks_like_single_stock_option(name: str, provider: str, ticker: str) -> bool:
    ln = f" {name.lower()} "
    lp = (provider or "").lower()

    if "yieldmax" in lp:
        return ticker not in YIELDMAX_NON_SINGLE

    if "single stock" in ln:
        return True

    if " option income strategy etf" in ln:
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
            "big 50",
        ]
        if not any(term in ln for term in non_single_terms):
            return True

    if any(h in ln for h in KNOWN_SINGLE_STOCK_OPTION_HINTS):
        return True

    return False


def is_probable_levered_inverse_etp(name: str) -> bool:
    ln = (name or "").lower()
    levered_flags = ["2x ", "3x ", "ultra ", "daily ", "leveraged", "inverse", "-1x", "-2x", "-3x"]
    if any(flag in ln for flag in levered_flags):
        if "long/short" in ln or "long short" in ln:
            return False
        return True
    return False


def is_target_etf(name: str, category: str, provider: str, summary: str, ticker: str) -> bool:
    text_blob = " | ".join([name or "", category or "", provider or "", summary or ""]).lower()
    ln = (name or "").lower()
    category_clean = (category or "").strip()

    if ticker.upper() in ETF_SEEDS:
        return True

    # Drop a recurring false-positive class (single-company ADR hedged wrappers).
    if "adrhedged" in ln or "adr hedged" in ln:
        return False

    if category_clean in TARGET_ALT_CATEGORIES:
        return True

    if text_has_terms(text_blob, TARGET_STRONG_TERMS):
        # Try to avoid "currency hedged equity index" false positives.
        if "currency hedged" in text_blob and not text_has_terms(
            text_blob,
            [
                "managed futures",
                "long/short",
                "long short",
                "market neutral",
                "arbitrage",
                "option income",
                "equity premium",
                "tail risk",
                "global macro",
                "absolute return",
            ],
        ):
            return False
        return True

    return False


def canonical_manager(provider: str, name: str) -> str:
    lp = (provider or "").lower()
    for key, value in MANAGER_KEYWORDS.items():
        if key in lp:
            return value
    # Fallback from name prefix.
    words = [w for w in re.split(r"\s+", (name or "").strip()) if w]
    if len(words) >= 2:
        return f"{words[0]} {words[1]}"
    if words:
        return words[0]
    return "Unknown"


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
    # Definitions:
    # NY full: full N calendar years.
    # NY partial: at least (N-1) years and <N years.
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


def discover_etf_candidates() -> tuple[pd.DataFrame, pd.DataFrame, dict[str, pd.Series], dict[str, object]]:
    source_sets: dict[str, set[str]] = {
        "StockAnalysisKeyword": set(),
        "YahooSearch": set(),
        "ETFDBPages": set(),
        "Seeds": set(ETF_SEEDS),
    }
    name_hints: dict[str, str] = {}
    stats: dict[str, object] = {}

    # Source 1: StockAnalysis all ETF dataset.
    sa_html = fetch_text(STOCKANALYSIS_SCREENER_URL)
    sa_parsed = parse_stockanalysis_etfs(sa_html)
    stats["StockAnalysisTotalParsed"] = len(sa_parsed)
    for t, n in sa_parsed:
        name_hints.setdefault(t, n)
        if text_has_terms(n, DISCOVERY_TERMS):
            source_sets["StockAnalysisKeyword"].add(t)

    # Source 2: ETFDB category pages.
    etfdb_pages_ok = 0
    for url in ETFDB_SOURCE_URLS:
        text = fetch_text(url)
        if not text:
            continue
        etfdb_pages_ok += 1
        for t in parse_tickers_from_etfdb(text):
            source_sets["ETFDBPages"].add(t)
    stats["ETFDBPagesFetched"] = etfdb_pages_ok

    # Source 3: Yahoo search.
    for phrase in ETF_SEARCH_PHRASES:
        try:
            search = yf.Search(
                query=phrase,
                max_results=100,
                news_count=0,
                lists_count=0,
                recommended=0,
                include_cb=False,
                include_nav_links=False,
                enable_fuzzy_query=False,
            )
            quotes = search.quotes or []
        except Exception:
            quotes = []
        for q in quotes:
            symbol = str(q.get("symbol", "")).upper().strip()
            qtype = str(q.get("quoteType", "")).upper().strip()
            shortname = str(q.get("shortname", "")).strip()
            if not symbol:
                continue
            if qtype and qtype != "ETF":
                continue
            source_sets["YahooSearch"].add(symbol)
            if shortname:
                name_hints.setdefault(symbol, shortname)
        time.sleep(0.1)

    # Union and validation.
    discovered = set().union(*source_sets.values())
    stats["ETFCombinedUniqueCandidates"] = len(discovered)
    stats["ETFFromStockAnalysisKeywords"] = len(source_sets["StockAnalysisKeyword"])
    stats["ETFFromETFDBPages"] = len(source_sets["ETFDBPages"])
    stats["ETFFromYahooSearch"] = len(source_sets["YahooSearch"])
    stats["ETFFromSeeds"] = len(source_sets["Seeds"])

    source_rows = []
    for t in sorted(discovered):
        source_rows.append(
            {
                "Ticker": t,
                "FromStockAnalysisKeyword": t in source_sets["StockAnalysisKeyword"],
                "FromYahooSearch": t in source_sets["YahooSearch"],
                "FromETFDBPages": t in source_sets["ETFDBPages"],
                "FromSeeds": t in source_sets["Seeds"],
                "NameHint": name_hints.get(t, ""),
            }
        )
    source_df = pd.DataFrame(source_rows)

    accepted_rows: list[dict[str, object]] = []
    rejected_rows: list[dict[str, object]] = []
    histories: dict[str, pd.Series] = {}

    all_candidates = sorted(discovered)
    for i, ticker in enumerate(all_candidates, start=1):
        print(f"[ETF {i}/{len(all_candidates)}] Validate {ticker}...")
        info, adj = fetch_info_and_adj_close(ticker)
        if len(adj) < 2:
            rejected_rows.append({"Ticker": ticker, "Reason": "No/insufficient price history", "Name": name_hints.get(ticker, "")})
            continue

        quote_type = str(info.get("quoteType", "")).upper().strip()
        name = str(info.get("longName") or info.get("shortName") or name_hints.get(ticker, ""))
        provider = str(info.get("fundFamily") or "")
        category = str(info.get("category") or "")
        exchange = str(info.get("exchange") or "")
        summary = str(info.get("longBusinessSummary") or "")

        if quote_type and quote_type != "ETF":
            rejected_rows.append({"Ticker": ticker, "Reason": f"quoteType={quote_type}", "Name": name})
            continue
        if re.search(r"\bETN\b", name, flags=re.IGNORECASE):
            rejected_rows.append({"Ticker": ticker, "Reason": "ETN instrument", "Name": name})
            continue
        if not is_us_like_symbol(ticker, exchange):
            rejected_rows.append({"Ticker": ticker, "Reason": f"non-US-like symbol/exchange={exchange}", "Name": name})
            continue
        if is_probable_levered_inverse_etp(name):
            rejected_rows.append({"Ticker": ticker, "Reason": "leveraged/inverse ETP", "Name": name})
            continue
        if looks_like_single_stock_option(name, provider, ticker):
            rejected_rows.append({"Ticker": ticker, "Reason": "single-stock option wrapper", "Name": name})
            continue
        if not is_target_etf(name, category, provider, summary, ticker):
            rejected_rows.append({"Ticker": ticker, "Reason": "not hedge/alternative target area", "Name": name})
            continue

        manager = canonical_manager(provider, name)
        accepted_rows.append(
            {
                "Ticker": ticker,
                "Name": name,
                "Provider": provider if provider else "Unknown",
                "Category": category,
                "Exchange": exchange,
                "QuoteType": quote_type if quote_type else "ETF",
                "Manager": manager,
                "Inception": adj.index.min().date().isoformat(),
                "LastDate": adj.index.max().date().isoformat(),
            }
        )
        histories[ticker] = adj
        time.sleep(0.05)

    accepted = pd.DataFrame(accepted_rows).sort_values(["Manager", "Ticker"]).reset_index(drop=True)
    rejected = pd.DataFrame(rejected_rows).sort_values(["Ticker"]).reset_index(drop=True)
    stats["ETFAcceptedFinal"] = int(len(accepted))
    stats["ETFRejectedFinal"] = int(len(rejected))

    return accepted, rejected, histories, {"stats": stats, "source_df": source_df}


def discover_manager_equities() -> tuple[pd.DataFrame, pd.DataFrame, dict[str, pd.Series], dict[str, object]]:
    seed_map = {item["Ticker"].upper(): item for item in MANAGER_EQUITY_SEEDS}
    discovered = set(seed_map.keys())
    from_search = set()

    for phrase in MANAGER_SEARCH_PHRASES:
        try:
            search = yf.Search(
                query=phrase,
                max_results=100,
                news_count=0,
                lists_count=0,
                recommended=0,
                include_cb=False,
                include_nav_links=False,
                enable_fuzzy_query=False,
            )
            quotes = search.quotes or []
        except Exception:
            quotes = []
        for q in quotes:
            symbol = str(q.get("symbol", "")).upper().strip()
            qtype = str(q.get("quoteType", "")).upper().strip()
            shortname = str(q.get("shortname", "")).strip().lower()
            if not symbol:
                continue
            if qtype and qtype not in {"EQUITY", "COMMON STOCK", "STOCK"}:
                continue
            if any(term in shortname for term in ["capital", "asset", "partners", "management", "invest", "hedge"]):
                discovered.add(symbol)
                from_search.add(symbol)
        time.sleep(0.1)

    accepted_rows: list[dict[str, object]] = []
    rejected_rows: list[dict[str, object]] = []
    histories: dict[str, pd.Series] = {}

    all_candidates = sorted(discovered)
    for i, ticker in enumerate(all_candidates, start=1):
        print(f"[MGR {i}/{len(all_candidates)}] Validate {ticker}...")
        info, adj = fetch_info_and_adj_close(ticker)
        if len(adj) < 2:
            rejected_rows.append({"Ticker": ticker, "Reason": "No/insufficient price history"})
            continue

        quote_type = str(info.get("quoteType", "")).upper().strip()
        name = str(info.get("longName") or info.get("shortName") or "")
        exchange = str(info.get("exchange") or "")

        if quote_type and quote_type not in ALLOWED_MANAGER_QUOTE_TYPES:
            rejected_rows.append({"Ticker": ticker, "Reason": f"quoteType={quote_type}", "Name": name})
            continue

        seed = seed_map.get(ticker)
        manager = seed["Manager"] if seed else (name if name else ticker)
        source = seed["Source"] if seed else "Yahoo search discovery"

        accepted_rows.append(
            {
                "Ticker": ticker,
                "Manager": manager,
                "Name": name if name else manager,
                "Exchange": exchange,
                "QuoteType": quote_type if quote_type else "EQUITY",
                "SourceTag": source,
                "FromSeedList": ticker in seed_map,
                "FromYahooSearch": ticker in from_search,
                "Inception": adj.index.min().date().isoformat(),
                "LastDate": adj.index.max().date().isoformat(),
            }
        )
        histories[ticker] = adj
        time.sleep(0.05)

    accepted = pd.DataFrame(accepted_rows).sort_values(["Manager", "Ticker"]).reset_index(drop=True)
    rejected = pd.DataFrame(rejected_rows).sort_values(["Ticker"]).reset_index(drop=True)
    stats = {
        "ManagerSeedCount": len(seed_map),
        "ManagerYahooSearchAdds": len(from_search),
        "ManagerCombinedCandidates": len(discovered),
        "ManagerAcceptedFinal": len(accepted),
        "ManagerRejectedFinal": len(rejected),
    }
    return accepted, rejected, histories, stats


def build_manager_composites(
    etf_universe: pd.DataFrame,
    etf_histories: dict[str, pd.Series],
) -> tuple[pd.DataFrame, dict[str, pd.Series]]:
    rows: list[dict[str, object]] = []
    histories: dict[str, pd.Series] = {}

    grouped = etf_universe.groupby("Manager")
    for manager, part in grouped:
        tickers = [t for t in part["Ticker"].tolist() if t in etf_histories]
        if len(tickers) < 2:
            continue

        ret_cols = {}
        for t in tickers:
            s = etf_histories[t]
            ret = s.pct_change().replace([np.inf, -np.inf], np.nan)
            ret_cols[t] = ret
        ret_df = pd.DataFrame(ret_cols).sort_index()
        comp_ret = ret_df.mean(axis=1, skipna=True).dropna()
        if len(comp_ret) < 2:
            continue

        comp_adj = (1.0 + comp_ret).cumprod() * 100.0
        comp_id = f"MGR::{manager}"
        histories[comp_id] = comp_adj

        rows.append(
            {
                "CompositeId": comp_id,
                "Manager": manager,
                "ComponentCount": len(tickers),
                "Components": ", ".join(sorted(tickers)),
                "Inception": comp_adj.index.min().date().isoformat(),
                "LastDate": comp_adj.index.max().date().isoformat(),
            }
        )

    df = pd.DataFrame(rows).sort_values(["Manager"]).reset_index(drop=True) if rows else pd.DataFrame(
        columns=["CompositeId", "Manager", "ComponentCount", "Components", "Inception", "LastDate"]
    )
    return df, histories


def compute_multiyear_screen(
    universe: pd.DataFrame,
    histories: dict[str, pd.Series],
    benchmark_histories: dict[str, pd.Series],
    ticker_col: str,
    name_col: str,
    provider_col: str | None,
    manager_col: str | None,
    universe_label: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    rows: list[dict[str, object]] = []
    benchmark_end = min(benchmark_histories["SPY"].index.max(), benchmark_histories["QQQ"].index.max())

    for horizon in HORIZONS_YEARS:
        for mode in ("full", "partial"):
            for _, u in universe.iterrows():
                ticker = str(u[ticker_col])
                s = histories.get(ticker, pd.Series(dtype=float))
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
                        "Universe": universe_label,
                        "HorizonYears": horizon,
                        "Mode": mode,
                        "Ticker": ticker,
                        "Name": str(u[name_col]),
                        "Provider": str(u[provider_col]) if provider_col else "",
                        "Manager": str(u[manager_col]) if manager_col else "",
                        "Start": inst.index.min().date().isoformat(),
                        "End": inst.index.max().date().isoformat(),
                        "TradingDays": int(m_inst["trading_days"]) if np.isfinite(m_inst["trading_days"]) else np.nan,
                        "AnnualizedReturn": m_inst["annualized_return"],
                        "MaxDrawdown": m_inst["max_drawdown"],
                        "Calmar": m_inst["calmar"],
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
                "Universe",
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
                    "Universe": universe_label,
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


def summary_table_rows(summary: pd.DataFrame) -> list[list[str]]:
    rows: list[list[str]] = []
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
    return rows


def winners_table(detail: pd.DataFrame, horizon: int, mode: str) -> str:
    part = detail[(detail["HorizonYears"] == horizon) & (detail["Mode"] == mode)]
    if part.empty:
        return "None"
    winners = part[part["BeatsEither"]].copy().sort_values(["Calmar"], ascending=False).reset_index(drop=True)
    if winners.empty:
        return "None"

    headers = ["Rank", "Ticker", "Manager", "Start", "End", "Calmar", "SPY", "QQQ", "Beat SPY", "Beat QQQ", "Name"]
    rows: list[list[str]] = []
    for i, (_, w) in enumerate(winners.iterrows(), start=1):
        rows.append(
            [
                str(i),
                str(w["Ticker"]),
                str(w["Manager"]),
                str(w["Start"]),
                str(w["End"]),
                fmt_num(float(w["Calmar"]), 3),
                fmt_num(float(w["SPYCalmarSameWindow"]), 3),
                fmt_num(float(w["QQQCalmarSameWindow"]), 3),
                "Y" if bool(w["BeatsSPY"]) else "N",
                "Y" if bool(w["BeatsQQQ"]) else "N",
                str(w["Name"]),
            ]
        )
    return markdown_table(headers, rows)


def main() -> None:
    out_dir = Path(__file__).resolve().parent
    as_of = date.today().isoformat()

    # Discover hedge/alternative ETFs.
    etf_universe, etf_rejected, etf_histories, etf_meta = discover_etf_candidates()
    source_df = etf_meta["source_df"]
    discovery_stats = etf_meta["stats"]

    # Discover manager equities / listed hedge fund vehicles.
    manager_universe, manager_rejected, manager_histories, manager_stats = discover_manager_equities()

    # Benchmark series.
    benchmark_histories: dict[str, pd.Series] = {}
    for b in BENCHMARKS:
        print(f"[BM] Fetch {b}...")
        _, s = fetch_info_and_adj_close(b)
        if len(s) < 2:
            raise RuntimeError(f"Failed benchmark history for {b}")
        benchmark_histories[b] = s
    benchmark_end = min(benchmark_histories["SPY"].index.max(), benchmark_histories["QQQ"].index.max())

    # Build manager composites from accepted ETF universe.
    composite_universe, composite_histories = build_manager_composites(etf_universe, etf_histories)

    # Multi-year screens.
    etf_detail, etf_summary = compute_multiyear_screen(
        universe=etf_universe,
        histories=etf_histories,
        benchmark_histories=benchmark_histories,
        ticker_col="Ticker",
        name_col="Name",
        provider_col="Provider",
        manager_col="Manager",
        universe_label="HedgeStyleETF",
    )
    manager_detail, manager_summary = compute_multiyear_screen(
        universe=manager_universe,
        histories=manager_histories,
        benchmark_histories=benchmark_histories,
        ticker_col="Ticker",
        name_col="Name",
        provider_col=None,
        manager_col="Manager",
        universe_label="ManagerEquity",
    )
    composite_detail, composite_summary = compute_multiyear_screen(
        universe=composite_universe,
        histories=composite_histories,
        benchmark_histories=benchmark_histories,
        ticker_col="CompositeId",
        name_col="Manager",
        provider_col=None,
        manager_col="Manager",
        universe_label="ManagerComposite",
    )

    # Save datasets.
    source_df.to_csv(out_dir / "hedge_style_etf_source_candidates.csv", index=False)
    etf_universe.to_csv(out_dir / "hedge_style_etf_universe.csv", index=False)
    etf_rejected.to_csv(out_dir / "hedge_style_etf_rejected.csv", index=False)
    manager_universe.to_csv(out_dir / "manager_equity_universe.csv", index=False)
    manager_rejected.to_csv(out_dir / "manager_equity_rejected.csv", index=False)
    composite_universe.to_csv(out_dir / "manager_composite_universe.csv", index=False)

    etf_detail.to_csv(out_dir / "hedge_style_etf_multiyear_detail.csv", index=False)
    etf_summary.to_csv(out_dir / "hedge_style_etf_multiyear_summary.csv", index=False)
    manager_detail.to_csv(out_dir / "manager_equity_multiyear_detail.csv", index=False)
    manager_summary.to_csv(out_dir / "manager_equity_multiyear_summary.csv", index=False)
    composite_detail.to_csv(out_dir / "manager_composite_multiyear_detail.csv", index=False)
    composite_summary.to_csv(out_dir / "manager_composite_multiyear_summary.csv", index=False)

    # Report.
    report: list[str] = []
    report.append(f"# Hedge Fund / Manager Expanded Calmar Screen (as of {as_of})")
    report.append("")
    report.append("This expands the prior option-income screen to include:")
    report.append("- Broad hedge-fund-style and alternatives ETFs (internet discovery + seeds).")
    report.append("- Publicly traded hedge fund managers / alternative-asset manager equities.")
    report.append("- Synthetic manager composites where at least two ETFs were available for a manager.")
    report.append("")
    report.append("Return series uses Yahoo `Adj Close` (distributions/dividends reinvested where provided).")
    report.append("Calmar uses the same definition as before: annualized return / abs(max drawdown).")
    report.append("")
    report.append("Partial/full bucket definitions:")
    report.append("- NY full: full N calendar years available.")
    report.append("- NY partial: at least (N-1) years and <N years available.")
    report.append("")
    report.append("## Research Sources Used")
    for url in RESEARCH_REFERENCE_URLS:
        report.append(f"- {url}")
    report.append("")
    report.append("## Discovery Stats")
    stats_rows = [
        ["StockAnalysis ETFs parsed", str(discovery_stats.get("StockAnalysisTotalParsed", 0))],
        ["StockAnalysis keyword candidates", str(discovery_stats.get("ETFFromStockAnalysisKeywords", 0))],
        ["ETFDB category candidates", str(discovery_stats.get("ETFFromETFDBPages", 0))],
        ["Yahoo ETF search candidates", str(discovery_stats.get("ETFFromYahooSearch", 0))],
        ["ETF seed list count", str(discovery_stats.get("ETFFromSeeds", 0))],
        ["ETF combined candidates", str(discovery_stats.get("ETFCombinedUniqueCandidates", 0))],
        ["ETF accepted final", str(discovery_stats.get("ETFAcceptedFinal", 0))],
        ["ETF rejected final", str(discovery_stats.get("ETFRejectedFinal", 0))],
        ["Manager seed list count", str(manager_stats.get("ManagerSeedCount", 0))],
        ["Manager Yahoo search adds", str(manager_stats.get("ManagerYahooSearchAdds", 0))],
        ["Manager combined candidates", str(manager_stats.get("ManagerCombinedCandidates", 0))],
        ["Manager accepted final", str(manager_stats.get("ManagerAcceptedFinal", 0))],
        ["Manager rejected final", str(manager_stats.get("ManagerRejectedFinal", 0))],
        ["Manager composites created", str(len(composite_universe))],
        ["Benchmark end date", benchmark_end.date().isoformat()],
    ]
    report.append(markdown_table(["Metric", "Value"], stats_rows))
    report.append("")

    summary_headers = [
        "Horizon",
        "Mode",
        "Count",
        "Avg Calmar",
        "Avg SPY (matched)",
        "Avg QQQ (matched)",
        "Beat SPY",
        "Beat QQQ",
        "Beat Either",
        "Beat Both",
    ]

    report.append("## HedgeStyleETF Summary")
    report.append(markdown_table(summary_headers, summary_table_rows(etf_summary)) if not etf_summary.empty else "None")
    report.append("")
    report.append("## ManagerEquity Summary")
    report.append(markdown_table(summary_headers, summary_table_rows(manager_summary)) if not manager_summary.empty else "None")
    report.append("")
    report.append("## ManagerComposite Summary")
    report.append(markdown_table(summary_headers, summary_table_rows(composite_summary)) if not composite_summary.empty else "None")
    report.append("")

    for label, detail in [
        ("HedgeStyleETF", etf_detail),
        ("ManagerEquity", manager_detail),
        ("ManagerComposite", composite_detail),
    ]:
        report.append(f"## {label} Winners By Timeframe (Beat SPY or QQQ)")
        report.append("")
        for horizon in HORIZONS_YEARS:
            for mode in ("partial", "full"):
                report.append(f"### {horizon}Y {mode.capitalize()}")
                report.append(winners_table(detail, horizon=horizon, mode=mode))
                report.append("")

    (out_dir / "hedge_fund_manager_multiyear_calmar_report.md").write_text("\n".join(report), encoding="utf-8")

    print(f"Saved: {out_dir / 'hedge_fund_manager_multiyear_calmar_report.md'}")
    print(f"Saved: {out_dir / 'hedge_style_etf_multiyear_summary.csv'}")
    print(f"Saved: {out_dir / 'manager_equity_multiyear_summary.csv'}")
    print(f"Saved: {out_dir / 'manager_composite_multiyear_summary.csv'}")
    print(f"Accepted ETF universe: {len(etf_universe)}")
    print(f"Accepted manager equity universe: {len(manager_universe)}")
    print(f"Manager composites: {len(composite_universe)}")


if __name__ == "__main__":
    main()
