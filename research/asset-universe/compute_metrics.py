#!/usr/bin/env python3
"""
Compute EXACT trailing-5-year metrics (annualized total return / Sharpe / yield)
for every liquid primary ticker in asset_universe.csv, and write metrics.json
(which build_asset_universe.py then joins onto each sleeve).

Run this wherever market-data hosts are reachable (e.g. your local machine):

    pip install yfinance pandas
    python3 compute_metrics.py            # uses Yahoo via yfinance
    python3 build_asset_universe.py       # rebuilds the spreadsheet with metrics

NOTE: In the locked-down web sandbox, market-data hosts are firewalled, so this
script will fail there. It is the reproducible, correct path to fill metrics for
the FULL universe (including exact 5Y Sharpe, which public pages rarely quote on
a clean 5Y window). Existing hand-verified entries in metrics.json are preserved
for any ticker this run cannot fetch.

Definitions:
  cagr   = (adj_close_end / adj_close_start) ** (252/n_days) - 1   (total return, divs reinvested)
  vol    = annualized stdev of daily returns
  sharpe = (annualized_mean_daily_return - risk_free) / vol
  yield  = trailing-12-month cash distributions / latest price
"""

import csv, json, os, time

OUT_DIR = os.path.dirname(os.path.abspath(__file__))
CSV = os.path.join(OUT_DIR, "asset_universe.csv")
METRICS = os.path.join(OUT_DIR, "metrics.json")
RISK_FREE = 0.042   # ~avg 3M T-bill over window; override as you like
YEARS = 5

def primary(ex):
    if not ex or ex == "-":
        return ""
    f = ex.split(",")[0].strip().lstrip("~").rstrip("*").strip()
    f = f.split()[0] if f else ""
    # skip obvious non-fetchables (indices, platforms, cash)
    if f.upper() in {"SPX", "ES", "ZN", "GC", "HYSA", "USDC", "CMBS", "FUNDRISE",
                     "MASTERWORKS", "VINOVEST", "SONG", "FFRHX"}:
        return ""
    return f.upper()

def load_existing():
    try:
        with open(METRICS) as f:
            return json.load(f)
    except FileNotFoundError:
        return {}

def main():
    import yfinance as yf
    import pandas as pd
    import numpy as np

    tickers = []
    seen = set()
    for r in csv.DictReader(open(CSV)):
        p = primary(r["Examples"])
        if p and p not in seen:
            seen.add(p); tickers.append(p)
    print(f"Unique fetchable tickers: {len(tickers)}")

    out = load_existing()
    src = f"yfinance computed {time.strftime('%Y-%m-%d')}"
    ok = fail = 0
    for i, t in enumerate(tickers, 1):
        try:
            tk = yf.Ticker(t)
            px = tk.history(period=f"{YEARS}y", auto_adjust=True)["Close"].dropna()
            if len(px) < 200:
                raise ValueError("insufficient history")
            rets = px.pct_change().dropna()
            n = len(px)
            cagr = (px.iloc[-1] / px.iloc[0]) ** (252.0 / n) - 1
            vol = rets.std() * (252 ** 0.5)
            sharpe = (rets.mean() * 252 - RISK_FREE) / vol if vol else float("nan")
            # trailing 12m distributions / price
            divs = tk.dividends
            if divs is not None and len(divs):
                cutoff = px.index[-1] - pd.Timedelta(days=365)
                ttm = divs[divs.index >= cutoff].sum()
            else:
                ttm = 0.0
            dy = ttm / px.iloc[-1] if px.iloc[-1] else 0.0
            out[t] = {
                "yield": f"{dy*100:.2f}%",
                "sharpe": f"{sharpe:.2f}",
                "cagr": f"{cagr*100:.1f}%",
                "vol": f"{vol*100:.1f}%",
                "src": src,
            }
            ok += 1
            print(f"[{i}/{len(tickers)}] {t}: cagr={out[t]['cagr']} sharpe={out[t]['sharpe']} yld={out[t]['yield']}")
        except Exception as e:
            fail += 1
            print(f"[{i}/{len(tickers)}] {t}: FAILED ({e}) — keeping any existing value")
        time.sleep(0.3)  # be gentle on the data host

    with open(METRICS, "w") as f:
        json.dump(out, f, indent=2, sort_keys=True)
    print(f"\nDone. ok={ok} fail={fail}. Wrote {METRICS}. Now run build_asset_universe.py.")

if __name__ == "__main__":
    main()
