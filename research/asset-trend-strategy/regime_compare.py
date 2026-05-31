#!/usr/bin/env python3
"""
Cross-asset regime gates vs the SPY-only gate, same discipline (daily, net of cost,
walk-forward). Question: does a regime signal built from the FULL asset universe
(breadth) or from the held book's OWN trend beat using SPY as the risk proxy?
"""
import numpy as np
import pandas as pd
import lib
from backtest import CASH_TICKERS
from daily_opt import Overlays, run, stats, BASE

COST = 5

CANDS = {
    "base (no regime)":        {**BASE, "band": 0.0},
    "SPY EMA100->50%":         {**BASE, "regime": 0.5, "regime_type": "spy", "regime_win": 100},
    "SPY EMA100->33%":         {**BASE, "regime": 0.33, "regime_type": "spy", "regime_win": 100},
    "breadth<50%->50%":        {**BASE, "regime": 0.5, "regime_type": "breadth", "breadth_thresh": 0.5},
    "breadth<40%->33%":        {**BASE, "regime": 0.33, "regime_type": "breadth", "breadth_thresh": 0.4},
    "breadth continuous":      {**BASE, "regime": 0.3, "regime_type": "breadth",
                                "breadth_continuous": True, "breadth_ref": 0.6},
    "ownbook EMA100->50%":     {**BASE, "regime": 0.5, "regime_type": "ownbook", "regime_win": 100},
    "ownbook EMA100->33%":     {**BASE, "regime": 0.33, "regime_type": "ownbook", "regime_win": 100},
    "ownbook EMA50->50%":      {**BASE, "regime": 0.5, "regime_type": "ownbook", "regime_win": 50},
    "combo SPY+breadth ->50%": {**BASE, "regime": 0.5, "regime_type": "combo", "regime_win": 100,
                                "breadth_ref": 0.6},
    "combo SPY+breadth ->33%": {**BASE, "regime": 0.33, "regime_type": "combo", "regime_win": 100,
                                "breadth_ref": 0.6},
}


def month_ends(idx):
    s = pd.Series(idx, index=idx)
    return pd.DatetimeIndex(s.groupby([idx.year, idx.month]).last().values)


def main():
    prices = lib.load_prices()
    cash = next(t for t in CASH_TICKERS if t in prices.columns)
    sel = [c for c in prices.columns if c not in CASH_TICKERS]
    rawclose, volume = lib.load_volume(prices.index, list(prices.columns))
    ov = Overlays(prices, rawclose, volume)
    rf = ov.R[cash]

    nets, turns = {}, {}
    for name, cfg in CANDS.items():
        net, _, turn = run(prices, ov, cash, sel, cfg, "2017-06-30", COST)
        nets[name] = net
        turns[name] = turn
    spy = ov.R["SPY"]

    def line(name, s, win, tser=None):
        ss = s[(s.index >= pd.Timestamp(win[0])) & (s.index <= pd.Timestamp(win[1]))]
        st = stats(ss, rf)
        typ = ""
        if tser is not None:
            tt = tser[(tser.index >= pd.Timestamp(win[0])) & (tser.index <= pd.Timestamp(win[1]))]
            typ = f"{tt.sum()/(len(ss)/252):>7.1f}x"
        return (f"{name:<24}{st['cagr']*100:>7.1f}%{st['vol']*100:>6.1f}%"
                f"{st['sharpe']:>8.2f}{st['sortino']:>9.2f}{st['maxdd']*100:>7.1f}%{typ:>9}")

    periods = [("OOS 2019-07..2026-05", ("2019-07-01", "2026-12-31")),
               ("2020-2021", ("2020-01-01", "2021-12-31")),
               ("2022-2023", ("2022-01-01", "2023-12-31")),
               ("2024-2026", ("2024-01-01", "2026-12-31"))]

    for label, win in periods:
        print(f"\n--- {label} (net {COST}bps) ---")
        print(f"{'variant':<24}{'CAGR':>8}{'Vol':>7}{'Sharpe':>8}{'Sortino':>9}{'MaxDD':>8}{'turn/yr':>9}")
        print(line("SPY buy & hold", spy, win))
        for name in CANDS:
            print(line(name, nets[name], win, turns[name]))

    # Walk-forward: each month pick the regime gate by trailing-252d Sharpe (past-only).
    days = nets["base (no regime)"].index
    mat = pd.DataFrame(nets).reindex(days)
    me = month_ends(days)
    chosen, picks = [], []
    for i, m in enumerate(me[:-1]):
        train = mat[mat.index <= m].tail(252)
        if len(train) < 252:
            continue
        rf_w = rf.reindex(train.index).fillna(0.0)
        sh = {c: stats(train[c], rf_w)["sharpe"] for c in mat.columns}
        best = max(sh, key=lambda c: (-9 if pd.isna(sh[c]) else sh[c]))
        seg = mat[(mat.index > m) & (mat.index <= me[i + 1])][best]
        chosen.append(seg); picks.append((m, best))
    wf = pd.concat(chosen)
    print(f"\n--- WALK-FORWARD regime selection (trailing-252d Sharpe, past-only), net {COST}bps ---")
    print(f"    OOS {wf.index[0].date()}..{wf.index[-1].date()}")
    print(f"    base (no regime)   Sharpe={stats(mat['base (no regime)'].reindex(wf.index), rf)['sharpe']:.2f}")
    print(f"    SPY EMA100->50%    Sharpe={stats(mat['SPY EMA100->50%'].reindex(wf.index), rf)['sharpe']:.2f}")
    wfs = stats(wf, rf)
    print(f"    walk-forward       Sharpe={wfs['sharpe']:.2f}  CAGR={wfs['cagr']*100:.1f}%  "
          f"MaxDD={wfs['maxdd']*100:.1f}%")
    from collections import Counter
    print("    gate chosen (months):", dict(Counter(b for _, b in picks).most_common()))


if __name__ == "__main__":
    main()
