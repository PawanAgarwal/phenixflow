#!/usr/bin/env python3
"""
Fetch daily VOLUME + raw close for the universe (the price cache only has adjclose).
Enables dollar-volume liquidity screens, OBV / volume-confirmation, and volume-weighted
signals for the daily-rebalance study. One CSV per ticker: date, close, volume.
"""
import csv, glob, json, os, time, urllib.request, urllib.error

HERE = os.path.dirname(os.path.abspath(__file__))
PRICE_DIR = os.path.join(HERE, "data", "prices")
VOL_DIR = os.path.join(HERE, "data", "volume")
os.makedirs(VOL_DIR, exist_ok=True)
UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120.0 Safari/537.36")


def fetch_one(t, retries=5):
    url = (f"https://query1.finance.yahoo.com/v8/finance/chart/{t}"
           f"?range=10y&interval=1d&events=div%2Csplit")
    delay = 1.5
    for _ in range(retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA})
            with urllib.request.urlopen(req, timeout=25) as r:
                d = json.load(r)
            res = d["chart"]["result"][0]
            ts = res["timestamp"]
            q = res["indicators"]["quote"][0]
            close, vol = q["close"], q["volume"]
            rows = []
            for tt, c, v in zip(ts, close, vol):
                if c is None:
                    continue
                rows.append((time.strftime("%Y-%m-%d", time.gmtime(tt)), c, v or 0))
            return rows
        except urllib.error.HTTPError as e:
            if e.code in (429, 502, 503):
                time.sleep(delay); delay *= 2; continue
            return None
        except Exception:
            time.sleep(delay); delay *= 2
    return None


def main():
    tickers = sorted(os.path.splitext(os.path.basename(p))[0]
                     for p in glob.glob(os.path.join(PRICE_DIR, "*.csv")))
    ok = skip = fail = 0
    for i, t in enumerate(tickers, 1):
        path = os.path.join(VOL_DIR, f"{t}.csv")
        if os.path.exists(path) and os.path.getsize(path) > 100:
            skip += 1; continue
        rows = fetch_one(t)
        if not rows or len(rows) < 100:
            fail += 1; print(f"[{i}/{len(tickers)}] {t}: FAIL"); time.sleep(0.6); continue
        with open(path, "w", newline="") as f:
            w = csv.writer(f); w.writerow(["date", "close", "volume"]); w.writerows(rows)
        ok += 1
        if i % 40 == 0:
            print(f"[{i}/{len(tickers)}] {t}: {len(rows)} rows")
        time.sleep(0.6)
    print(f"Done. ok={ok} skip={skip} fail={fail}")


if __name__ == "__main__":
    main()
