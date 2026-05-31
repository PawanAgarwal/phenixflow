#!/usr/bin/env python3
"""
Honest validation of the daily-overlay improvement over the monthly bogey.

(1) Robustness: the chosen overlays across full sample + sub-periods, net of cost.
(2) Walk-forward: each month pick the overlay variant with the best trailing-252d Sharpe
    using ONLY past daily returns, then apply it next month -- removes config cherry-picking.
All on the identical daily return stream; cost = 5 bps one-way unless noted.
"""
import numpy as np
import pandas as pd
import lib
from backtest import CASH_TICKERS
from daily_opt import Overlays, run, stats, BASE, PERIODS

COST = 5

CANDS = {
    "base (no overlay)":      {**BASE, "band": 0.0},
    "regime EMA100->50%":     {**BASE, "regime": 0.5, "regime_win": 100},
    "regime EMA100->33%":     {**BASE, "regime": 0.33, "regime_win": 100},
    "regime EMA50->50%":      {**BASE, "regime": 0.5, "regime_win": 50},
    "regime EMA200->0":       {**BASE, "regime": 0.0, "regime_win": 200},
    "port VT15":              {**BASE, "port_vt": 0.15, "port_vt_win": 30, "port_vt_smooth": 0.5},
    "VT15+regime100/50%":     {**BASE, "port_vt": 0.15, "port_vt_win": 30, "port_vt_smooth": 0.5,
                               "regime": 0.5, "regime_win": 100},
}
HEADLINE = "regime EMA100->50%"   # round, conservative, a-priori-defensible params


def month_ends(idx):
    s = pd.Series(idx, index=idx)
    return pd.DatetimeIndex(s.groupby([idx.year, idx.month]).last().values)


def sharpe(series, rf):
    return stats(series.dropna(), rf)["sharpe"]


def main():
    prices = lib.load_prices()
    cash = next(t for t in CASH_TICKERS if t in prices.columns)
    sel = [c for c in prices.columns if c not in CASH_TICKERS]
    rawclose, volume = lib.load_volume(prices.index, list(prices.columns))
    ov = Overlays(prices, rawclose, volume)
    rf = ov.R[cash]

    # Precompute each candidate's daily NET return (cost=5bps) over the full sample.
    nets = {}
    for name, cfg in CANDS.items():
        net, _, turn = run(prices, ov, cash, sel, cfg, "2017-06-30", COST)
        nets[name] = net
    spy = ov.R["SPY"]

    def row(name, s, win):
        ss = s[s.index >= pd.Timestamp(win[0])]
        ss = ss[ss.index <= pd.Timestamp(win[1])]
        st = stats(ss, rf)
        return (f"{name:<26}{st['cagr']*100:>7.1f}%{st['vol']*100:>6.1f}%"
                f"{st['sharpe']:>8.2f}{st['sortino']:>9.2f}{st['maxdd']*100:>7.1f}%")

    periods = [("FULL 2017-07..2026-05", ("2017-07-01", "2026-12-31")),
               ("OOS  2019-07..2026-05", ("2019-07-01", "2026-12-31")),
               ("2020-2021 (covid/melt-up)", ("2020-01-01", "2021-12-31")),
               ("2022-2023 (bear+recovery)", ("2022-01-01", "2023-12-31")),
               ("2024-2026", ("2024-01-01", "2026-12-31"))]

    print(f"(1) ROBUSTNESS — net of {COST}bps, daily basis (rf=cash)\n")
    for label, win in periods:
        print(f"--- {label} ---")
        print(f"{'variant':<26}{'CAGR':>8}{'Vol':>7}{'Sharpe':>8}{'Sortino':>9}{'MaxDD':>8}")
        print(row("SPY buy & hold", spy, win))
        print(row("monthly base (bogey)", nets["base (no overlay)"], win))
        print(row(HEADLINE, nets[HEADLINE], win))
        print(row("regime EMA100->33%", nets["regime EMA100->33%"], win))
        print()

    # (2) Walk-forward: pick the overlay each month by trailing-252d Sharpe (past only).
    days = nets[HEADLINE].index
    mat = pd.DataFrame(nets).reindex(days)
    me = month_ends(days)
    chosen, idxd, picks = [], [], []
    for i, m in enumerate(me[:-1]):
        train = mat[mat.index <= m].tail(252)
        if len(train) < 252:
            continue
        rf_w = rf.reindex(train.index).fillna(0.0)
        sh = {c: sharpe(train[c], rf_w) for c in mat.columns}
        best = max(sh, key=lambda c: (-9 if pd.isna(sh[c]) else sh[c]))
        seg = mat[(mat.index > m) & (mat.index <= me[i + 1])][best]
        chosen.append(seg); idxd.extend(seg.index)
        picks.append((m, best))
    wf = pd.concat(chosen)
    wf_st = stats(wf, rf)
    base_oos = nets["base (no overlay)"].reindex(wf.index)
    print("(2) WALK-FORWARD overlay selection (trailing-252d Sharpe, past-only), "
          f"net {COST}bps")
    print(f"    OOS {wf.index[0].date()}..{wf.index[-1].date()}")
    print(f"    monthly base (bogey)   Sharpe={stats(base_oos, rf)['sharpe']:.2f}")
    print(f"    walk-forward overlay   Sharpe={wf_st['sharpe']:.2f}  "
          f"CAGR={wf_st['cagr']*100:.1f}%  Vol={wf_st['vol']*100:.1f}%  "
          f"MaxDD={wf_st['maxdd']*100:.1f}%")
    from collections import Counter
    pc = Counter(b for _, b in picks)
    print("    overlay chosen (months):", dict(pc.most_common()))


if __name__ == "__main__":
    main()
