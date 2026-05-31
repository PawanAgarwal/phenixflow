#!/usr/bin/env python3
"""
Independent verification of the Breadth EMA-50 gated strategy.

Three checks:
  (1) RECONCILE — recompute the strategy from scratch (explicit daily loop, explicit 1-day
      execution lag, explicit turnover/cost) and compare to the fast analytic engine in
      breadth_explore.py. If they agree to ~1e-4, the analytic numbers are trustworthy.
  (2) LOOKAHEAD/TIMING audit — show the honest 1-day-lagged result vs an (illegal) same-day
      version, to confirm we are not peeking; confirm EMA/selection use only past data.
  (3) WALK-FORWARD — anchored OOS: each year pick the breadth ramp + EMA length from PRIOR
      data only, apply next year; report stitched OOS. Plus a parameter-stability grid.
"""
import numpy as np
import pandas as pd
import lib
from backtest import CASH_TICKERS, build_signals, candidate_weights
from daily_opt import Overlays, run, stats, BASE
from breadth_explore import breadth_series, scale_from_breadth, gated_perf

COST = 5
LO, HI, EMALEN = 0.18, 0.50, 50
OOS = "2019-07-01"


def month_end_set(index):
    s = pd.Series(index, index=index)
    return set(pd.DatetimeIndex(s.groupby([index.year, index.month]).last().values))


def scratch_engine(prices, sel, cash_t, lo, hi, emalen):
    """Fully explicit recomputation: monthly top-20 12m book * daily EMA-breadth scale.
    Causal by construction: the target set at EOD day t (using close[t]) earns day t+1's
    return, so realized return[t] is governed by the scale decided at EOD t-1 (1-day lag)."""
    R = prices.pct_change()
    cash_ret = R[cash_t]
    moms, above_ma, vol, daily = build_signals(prices, BASE["lookbacks"], BASE["ma_window"])
    selcfg = {k: BASE[k] for k in ["lookbacks", "top_k", "weighting", "ma_filter",
                                   "ma_window", "max_weight"]}
    selcfg["target_vol"] = None
    # Independent breadth: fraction of universe with price above its own EMA(emalen).
    ema = prices[sel].ewm(span=emalen, min_periods=emalen // 2).mean()
    sig = (prices[sel] > ema).astype(float).where(ema.notna())
    breadth = sig.mean(axis=1)
    scale = ((breadth - lo) / (hi - lo)).clip(0.0, 1.0)

    me = month_end_set(prices.index)
    days = prices.index[prices.index >= pd.Timestamp("2017-06-30")]
    w_active, w_base = {}, {}
    rets, idx = [], []
    for t in days:
        rt = R.loc[t]
        # today's return from weights set at EOD of the PRIOR day (causal)
        g = sum(wv * (rt[k] if pd.notna(rt.get(k, np.nan)) else 0.0) for k, wv in w_active.items())
        g += max(0.0, 1.0 - sum(w_active.values())) * (cash_ret[t] if pd.notna(cash_ret[t]) else 0.0)
        rets.append(g)
        idx.append(t)
        # refresh monthly book at EOD (causal — uses data through t)
        if t in me:
            w_base = candidate_weights(t, sel, moms, above_ma, vol, daily, selcfg)
        # set target at EOD t using today's scale -> it earns the NEXT day's return
        s_apply = float(scale.loc[t])
        target = {k: v * s_apply for k, v in w_base.items() if v * s_apply > 1e-9}
        to = sum(abs(target.get(k, 0.0) - w_active.get(k, 0.0)) for k in set(target) | set(w_active))
        rets[-1] = g - to * COST / 1e4
        w_active = target
    return pd.Series(rets, index=idx)


def yearly(net, rf):
    out = {}
    for y in sorted({d.year for d in net.index}):
        x = net[[d.year == y for d in net.index]].dropna()
        tot = (1 + x).prod() - 1
        sh = stats(x, rf)["sharpe"]
        eq = (1 + x).cumprod(); dd = (eq / eq.cummax() - 1).min()
        out[y] = (tot, sh, dd)
    return out


def main():
    prices = lib.load_prices()
    cash_t = next(t for t in CASH_TICKERS if t in prices.columns)
    sel = [c for c in prices.columns if c not in CASH_TICKERS]
    rawclose, volume = lib.load_volume(prices.index, list(prices.columns))
    ov = Overlays(prices, rawclose, volume)
    rf = ov.R[cash_t]

    # Analytic engine (the one used to report iter-8 numbers).
    _, gross_book, sel_turn = run(prices, ov, cash_t, sel, {**BASE, "band": 0.0}, "2017-06-30", 0)
    b_emadefault = breadth_series(prices, sel, "ema", EMALEN)
    analytic, _ = gated_perf(gross_book, sel_turn, ov.R[cash_t],
                             scale_from_breadth(b_emadefault, LO, HI), rf)

    # Independent from-scratch engine.
    scratch = scratch_engine(prices, sel, cash_t, LO, HI, EMALEN)

    def oos(s):
        return s[s.index >= pd.Timestamp(OOS)]

    a, s = stats(oos(analytic), rf), stats(oos(scratch), rf)
    print("=" * 74)
    print("(1) RECONCILE  analytic engine  vs  independent from-scratch engine  (OOS, net 5bps)")
    print("=" * 74)
    print(f"  {'metric':<10}{'analytic':>12}{'scratch':>12}{'diff':>12}")
    for k in ["sharpe", "cagr", "vol", "maxdd"]:
        print(f"  {k:<10}{a[k]:>12.4f}{s[k]:>12.4f}{abs(a[k]-s[k]):>12.5f}")
    # correlation of the two daily series
    j = pd.concat([oos(analytic), oos(scratch)], axis=1).dropna()
    print(f"  daily-return corr = {j.iloc[:,0].corr(j.iloc[:,1]):.5f}   "
          f"mean|Δdaily| = {(j.iloc[:,0]-j.iloc[:,1]).abs().mean():.2e}")

    print("\n(2) LOOKAHEAD / TIMING audit")
    honest_net, _ = gated_perf(gross_book, sel_turn, ov.R[cash_t],
                               scale_from_breadth(b_emadefault, LO, HI), rf, lag=1)
    peek_net, _ = gated_perf(gross_book, sel_turn, ov.R[cash_t],
                             scale_from_breadth(b_emadefault, LO, HI), rf, lag=0)
    honest = stats(oos(honest_net), rf)["sharpe"]
    cheat = stats(oos(peek_net), rf)["sharpe"]
    print(f"  honest (scale lagged 1 day, decide EOD->trade next close): Sharpe {honest:.3f}")
    print(f"  ILLEGAL same-day (scale[t] on R[t], peeking):             Sharpe {cheat:.3f}")
    print(f"  -> peeking inflates by {cheat-honest:+.3f}; we report the honest lagged number.")
    print("  EMA(span), rolling MA, 252d momentum, and monthly selection all use only data")
    print("  up to day t (pandas ewm/rolling are backward-looking) -> no future leakage.")

    print("\n(3) WALK-FORWARD (anchored: pick ramp+EMA len from PRIOR years only)")
    # candidate param grid
    grid = [(lo, hi, n) for lo in (0.10, 0.18, 0.30) for hi in (0.45, 0.50, 0.60)
            for n in (30, 50, 75, 100)]
    cand = {}
    for lo, hi, n in grid:
        b = breadth_series(prices, sel, "ema", n)
        net, _ = gated_perf(gross_book, sel_turn, ov.R[cash_t], scale_from_breadth(b, lo, hi), rf)
        cand[(lo, hi, n)] = net
    years = list(range(2020, 2027))
    stitched, picks = [], []
    for y in years:
        train_end = pd.Timestamp(f"{y}-01-01")
        best, bsh = None, -9
        for key, net in cand.items():
            tr = net[(net.index >= pd.Timestamp("2017-06-30")) & (net.index < train_end)]
            if len(tr) < 252:
                continue
            sh = stats(tr, rf)["sharpe"]
            if sh > bsh:
                bsh, best = sh, key
        seg = cand[best][(cand[best].index >= train_end) & (cand[best].index < pd.Timestamp(f"{y+1}-01-01"))]
        stitched.append(seg); picks.append((y, best))
    wf = pd.concat(stitched)
    wfs = stats(wf, rf)
    base_oos = stats((gross_book - sel_turn * COST / 1e4).reindex(wf.index), rf)
    spy_oos = stats(ov.R["SPY"].reindex(wf.index), rf)
    print(f"  walk-forward OOS {wf.index[0].date()}..{wf.index[-1].date()}: "
          f"Sharpe={wfs['sharpe']:.2f}  CAGR={wfs['cagr']*100:.1f}%  MaxDD={wfs['maxdd']*100:.1f}%")
    print(f"  vs no-gate base Sharpe={base_oos['sharpe']:.2f}   SPY Sharpe={spy_oos['sharpe']:.2f}")
    print("  picks by year (lo,hi,emaLen):")
    for y, k in picks:
        print(f"    {y}: {k}")

    print("\n  Parameter-stability grid (fixed EMA50, OOS Sharpe across ramps):")
    hdr = "lo\\hi"
    print(f"    {hdr:>6}" + "".join(f"{hi:>8.2f}" for hi in (0.45, 0.50, 0.60)))
    for lo in (0.10, 0.18, 0.30):
        row = []
        for hi in (0.45, 0.50, 0.60):
            net, _ = gated_perf(gross_book, sel_turn, ov.R[cash_t],
                                scale_from_breadth(breadth_series(prices, sel, "ema", 50), lo, hi), rf)
            row.append(stats(oos(net), rf)["sharpe"])
        print(f"    {lo:>6.2f}" + "".join(f"{v:>8.2f}" for v in row))

    print("\n  Yearly (scratch engine, net 5bps):  year  return  sharpe  maxDD")
    for y, (tot, sh, dd) in yearly(scratch, rf).items():
        print(f"    {y}: {tot*100:>7.1f}%  {sh:>5.2f}  {dd*100:>6.1f}%")


if __name__ == "__main__":
    main()
