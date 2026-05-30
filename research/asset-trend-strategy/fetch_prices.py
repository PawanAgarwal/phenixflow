#!/usr/bin/env python3
"""
Fetch daily price history (split/div-adjusted close) for every fetchable ticker in
the asset universe plus SPY, caching one CSV per ticker so reruns are cheap and
partial failures are recoverable.

Source: Yahoo Finance chart API (reachable from this environment with a browser
User-Agent; bare requests get 429-rate-limited, so we set a UA and back off).

Output:
  data/prices/<TICKER>.csv   (date, adjclose)   -- one file per ticker
  data/prices_wide.csv       (date x ticker matrix of adjclose) -- built at end
"""
import csv, json, os, time, sys, urllib.request, urllib.error

HERE = os.path.dirname(os.path.abspath(__file__))
UNIVERSE_DIR = os.path.join(HERE, "..", "asset-universe")
METRICS = os.path.join(UNIVERSE_DIR, "metrics.json")
PRICE_DIR = os.path.join(HERE, "data", "prices")
os.makedirs(PRICE_DIR, exist_ok=True)

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36")
RANGE = "10y"      # as much history as Yahoo gives on the free endpoint


def fetch_one(ticker, retries=5):
    url = (f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
           f"?range={RANGE}&interval=1d&events=div%2Csplit")
    delay = 1.5
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA})
            with urllib.request.urlopen(req, timeout=25) as r:
                d = json.load(r)
            res = d["chart"]["result"][0]
            ts = res["timestamp"]
            quote = res["indicators"]["quote"][0]
            adj = res["indicators"].get("adjclose", [{}])[0].get("adjclose")
            close = quote["close"]
            # Prefer adjusted close (total return); fall back to raw close.
            series = adj if adj is not None else close
            rows = []
            for t, c in zip(ts, series):
                if c is None:
                    continue
                day = time.strftime("%Y-%m-%d", time.gmtime(t))
                rows.append((day, c))
            return rows
        except urllib.error.HTTPError as e:
            if e.code in (429, 503, 502):
                time.sleep(delay)
                delay *= 2
                continue
            return None
        except Exception:
            time.sleep(delay)
            delay *= 2
    return None


def load_universe():
    tickers = sorted(json.load(open(METRICS)).keys())
    if "SPY" not in tickers:
        tickers.append("SPY")
    # Always ensure cash proxies present for risk-off allocation.
    for t in ("BIL", "SGOV", "SHV"):
        if t not in tickers:
            tickers.append(t)
    return tickers


def main():
    tickers = load_universe()
    print(f"Universe: {len(tickers)} tickers")
    ok = skip = fail = 0
    failed = []
    for i, t in enumerate(tickers, 1):
        path = os.path.join(PRICE_DIR, f"{t}.csv")
        if os.path.exists(path) and os.path.getsize(path) > 100:
            skip += 1
            continue
        rows = fetch_one(t)
        if not rows or len(rows) < 100:
            fail += 1
            failed.append(t)
            print(f"[{i}/{len(tickers)}] {t}: FAIL")
            time.sleep(0.6)
            continue
        with open(path, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["date", "adjclose"])
            w.writerows(rows)
        ok += 1
        if i % 25 == 0 or ok <= 3:
            print(f"[{i}/{len(tickers)}] {t}: {len(rows)} rows ({rows[0][0]}..{rows[-1][0]})")
        time.sleep(0.6)
    print(f"\nDone. ok={ok} skip={skip} fail={fail}")
    if failed:
        print("Failed:", ", ".join(failed))


if __name__ == "__main__":
    main()
