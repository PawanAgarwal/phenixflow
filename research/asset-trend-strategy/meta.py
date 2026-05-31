#!/usr/bin/env python3
"""
Fast meta-layer experimentation over cached candidate returns (data/cand_rets.csv).
Compares walk-forward selection schemes against SPY on an identical OOS window.
Run backtest.py --recompute first to (re)build the cache.
"""
import os
import numpy as np
import pandas as pd
import lib

HERE = lib.HERE
cand = pd.read_csv(os.path.join(HERE, "data", "cand_rets.csv"),
                   parse_dates=["date"]).set_index("date")

prices = lib.load_prices()
rebals = cand.index
spy = prices["SPY"].reindex(prices.index).pct_change()  # daily not used
# Monthly SPY + cash aligned to candidate months (returns realized over each month).
month_px = prices.reindex(rebals)
# candidate month t return corresponds to growth from rebal[t-1] to rebal[t]; cand index is
# already the realized-month end. Build SPY/cash the same way the engine did:
fwd_spy = prices["SPY"].reindex(
    pd.DatetimeIndex(list(rebals) )).pct_change()
spy_m = fwd_spy.reindex(rebals)
cash = prices["BIL"].reindex(rebals).pct_change().reindex(rebals)
rf = cash.copy()


def stats(s):
    return lib.perf_stats(s.dropna(), rf_monthly=rf)


def wf_select(scheme, train=24, ens=5, subset=None):
    cols = [c for c in cand.columns if (subset(c) if subset else True)]
    mat = cand[cols]
    out = {}
    for i in range(len(mat.index)):
        if i < train:
            continue
        d = mat.index[i]
        win = mat.iloc[i - train:i]
        rf_w = rf.reindex(win.index).fillna(0.0)
        if scheme == "equal":
            out[d] = float(mat.loc[d].mean())
            continue
        sh = {c: lib.perf_stats(win[c], rf_monthly=rf_w).get("sharpe", np.nan) for c in cols}
        ranked = sorted(cols, key=lambda c: (-1e9 if pd.isna(sh[c]) else sh[c]), reverse=True)
        top = ranked[:ens]
        out[d] = float(mat.loc[d, top].mean())
    return pd.Series(out)


def is_12m(c):     return c.startswith("lb252_")            # pure 12-month momentum family
def is_12m_iv(c):  return c.startswith("lb252_") and "_invvol_" in c
def is_12m_tr(c):  return c.startswith("lb252_") and "_ma1_" in c   # trend-filter ON (robust)


schemes = {
    "SPY buy & hold": spy_m,
    "Equal-weight ALL candidates": wf_select("equal"),
    "Equal-weight 12m-momentum family": wf_select("equal", subset=is_12m),
    "Equal-weight 12m + inverse-vol": wf_select("equal", subset=is_12m_iv),
    "Equal-weight 12m + trend-ON": wf_select("equal", subset=is_12m_tr),
    "Top3 trailing-Sharpe (12m fam)": wf_select("topN", ens=3, subset=is_12m),
    "Top5 trailing-Sharpe (12m fam)": wf_select("topN", ens=5, subset=is_12m),
    "Top5 trailing-Sharpe (12m+invvol)": wf_select("topN", ens=5, subset=is_12m_iv),
    "Top5 trailing-Sharpe (12m+trend-ON)": wf_select("topN", ens=5, subset=is_12m_tr),
    "Top8 trailing-Sharpe (12m fam)": wf_select("topN", ens=8, subset=is_12m),
}

# Align everything to the common OOS window (longest train burn-in = 24).
oos = wf_select("topN", ens=5, subset=is_12m).index
print(f"OOS window: {oos[0].date()}..{oos[-1].date()}  ({len(oos)} months)\n")
rows = []
for name, s in schemes.items():
    st = stats(s.reindex(oos))
    rows.append((name, st))
    print(lib.fmt_stats(name, st))

best = max((r for r in rows if r[0] != "SPY buy & hold"),
           key=lambda r: r[1].get("sharpe") or -9)
print(f"\nBest scheme: {best[0]}  (Sharpe {best[1]['sharpe']:.2f})")
