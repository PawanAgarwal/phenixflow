#!/usr/bin/env python3
"""
Compute the FINAL strategy's target holdings for EVERY trading day (EOD+10min decision:
weights from day T's close, effective for T+1). Saves the full daily weight matrix and
prints the most recent days plus the day-over-day turnover that drives trading cost.
"""
import sys
import numpy as np
import pandas as pd
import lib
from backtest import build_signals, candidate_weights, CASH_TICKERS
from strategy import FINAL_CFG

N = int(sys.argv[1]) if len(sys.argv) > 1 else 8


def main():
    prices = lib.load_prices()
    cash = next(t for t in CASH_TICKERS if t in prices.columns)
    sel = [c for c in prices.columns if c not in CASH_TICKERS]
    moms, above_ma, vol, daily = build_signals(prices, FINAL_CFG["lookbacks"],
                                               FINAL_CFG["ma_window"])
    days = prices.index[prices.index >= pd.Timestamp("2017-06-30")]

    rows, prev = {}, {}
    turnover = {}
    for t in days:
        w = candidate_weights(t, sel, moms, above_ma, vol, daily, FINAL_CFG)
        rows[t] = w
        if prev:
            turnover[t] = sum(abs(w.get(k, 0.0) - prev.get(k, 0.0))
                              for k in set(w) | set(prev))
        prev = w

    wide = pd.DataFrame(rows).T.fillna(0.0).sort_index()
    wide.to_csv(lib.HERE + "/daily_weights.csv", index_label="date")
    to = pd.Series(turnover).sort_index()

    print(f"Daily holdings computed for {len(wide)} trading days "
          f"({wide.index[0].date()}..{wide.index[-1].date()})")
    print(f"Saved full daily weight matrix -> daily_weights.csv\n")
    print(f"Average one-way DAILY turnover: {to.mean()*100:.1f}%  "
          f"=> ~{to.mean()*252:.1f}x of the book/year")
    print(f"Days with ANY change: {(to>1e-6).mean()*100:.0f}%   "
          f"median daily turnover: {to.median()*100:.1f}%\n")

    print(f"Last {N} trading days — holdings (weight%) and turnover vs prior day:")
    for t in wide.index[-N:]:
        w = rows[t]
        cw = max(0.0, 1.0 - sum(w.values()))
        top = sorted(w.items(), key=lambda x: -x[1])
        held = " ".join(f"{k}:{v*100:.0f}" for k, v in top)
        print(f"  {t.date()}  turn={turnover.get(t,0)*100:4.1f}%  cash={cw*100:3.0f}%  "
              f"[{len(w)} names] {held}")


if __name__ == "__main__":
    main()
