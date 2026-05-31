#!/usr/bin/env python3
"""
Systematic exploration of the BREADTH regime gate:
  (1) multiple uptrend mechanisms (not just SMA): EMA, time-series momentum, Donchian channel
      position, MACD, near-N-day-high;
  (2) multiple MA/lookback lengths (50/100/150/200/252);
  (3) breadth->exposure scaling swept, INCLUDING ramps to 0% (full cash) at low breadth.

Speed: the gate is one scalar multiplier on a fixed monthly book, so we compute the
fully-invested book's daily gross return + selection-turnover ONCE, then apply any daily
scale series analytically. cost = 5 bps one-way.
"""
import numpy as np
import pandas as pd
import lib
from backtest import CASH_TICKERS
from daily_opt import Overlays, run, stats, BASE

COST = 5
OOS = "2019-07-01"
SUBS = [("2020-2021", "2020-01-01", "2021-12-31"),
        ("2022-2023", "2022-01-01", "2023-12-31"),
        ("2024-2026", "2024-01-01", "2026-12-31")]


def breadth_series(px, sel, mech, n):
    """Fraction of the selectable universe in an uptrend by a given mechanism (NaN-safe)."""
    p = px[sel]
    if mech == "sma":
        ref = p.rolling(n, min_periods=n // 2).mean(); sig = p > ref; defined = ref.notna()
    elif mech == "ema":
        ref = p.ewm(span=n, min_periods=n // 2).mean(); sig = p > ref; defined = ref.notna()
    elif mech == "mom":                       # n-day total return > 0 (time-series momentum)
        r = p / p.shift(n) - 1; sig = r > 0; defined = r.notna()
    elif mech == "donch":                     # price above midpoint of n-day high-low channel
        hi = p.rolling(n, min_periods=n // 2).max(); lo = p.rolling(n, min_periods=n // 2).min()
        pos = (p - lo) / (hi - lo); sig = pos > 0.5; defined = pos.notna()
    elif mech == "macd":                      # 12/26 EMA MACD > 0 (n ignored)
        sig = p.ewm(span=12).mean() > p.ewm(span=26).mean()
        defined = p.ewm(span=26, min_periods=26).mean().notna()
    elif mech == "newhigh":                   # within 5% of its trailing n-day high
        hi = p.rolling(n, min_periods=n // 2).max(); sig = p >= 0.95 * hi; defined = hi.notna()
    else:
        raise ValueError(mech)
    sigf = sig.astype(float).where(defined)
    return sigf.mean(axis=1)                   # skipna -> denominator = assets with defined signal


def scale_from_breadth(b, lo, hi):
    """Piecewise-linear: 0% exposure at breadth<=lo, 100% at breadth>=hi, linear between."""
    return ((b - lo) / (hi - lo)).clip(0.0, 1.0)


def gated_perf(gross_book, sel_turn, cash, scale, rf):
    """Apply a daily exposure scale (decided EOD, effective next day) to the book; net of cost."""
    s = scale.reindex(gross_book.index).ffill().fillna(1.0)
    s_prev = s.shift(1).fillna(1.0)
    port_gross = s_prev * gross_book + (1 - s_prev) * cash.reindex(gross_book.index).fillna(0.0)
    gate_turn = s.diff().abs().fillna(0.0)                 # turnover from moving the gross lever
    turn = sel_turn.reindex(gross_book.index).fillna(0.0) * s_prev + gate_turn
    net = port_gross - turn * COST / 1e4
    return net, turn


def perf(net, rf, win=None):
    s = net
    if win:
        s = s[(s.index >= pd.Timestamp(win[0])) & (s.index <= pd.Timestamp(win[1]))]
    else:
        s = s[s.index >= pd.Timestamp(OOS)]
    return stats(s, rf)


def main():
    prices = lib.load_prices()
    cash_t = next(t for t in CASH_TICKERS if t in prices.columns)
    sel = [c for c in prices.columns if c not in CASH_TICKERS]
    rawclose, volume = lib.load_volume(prices.index, list(prices.columns))
    ov = Overlays(prices, rawclose, volume)
    rf = ov.R[cash_t]
    cash = ov.R[cash_t]

    # Fully-invested book (no regime) computed ONCE -> gross daily return + selection turnover.
    _, gross_book, sel_turn = run(prices, ov, cash_t, sel, {**BASE, "band": 0.0},
                                  "2017-06-30", 0)
    base_net = gross_book - sel_turn * COST / 1e4
    spy = ov.R["SPY"]
    print(f"OOS {OOS}..{prices.index[-1].date()}  net {COST}bps  (rf=cash)\n")
    print(f"{'reference':<42}{'Sharpe':>7}{'CAGR':>7}{'MaxDD':>7}{'turn':>6}")
    print(f"{'SPY buy & hold':<42}{perf(spy, rf)['sharpe']:>7.2f}"
          f"{perf(spy, rf)['cagr']*100:>6.0f}%{perf(spy, rf)['maxdd']*100:>6.0f}%{'-':>6}")
    bn = perf(base_net, rf)
    print(f"{'monthly base, no gate (bogey)':<42}{bn['sharpe']:>7.2f}"
          f"{bn['cagr']*100:>6.0f}%{bn['maxdd']*100:>6.0f}%"
          f"{sel_turn[sel_turn.index>=pd.Timestamp(OOS)].sum()/((prices.index>=pd.Timestamp(OOS)).sum()/252):>5.0f}x")

    # ---- (A) Mechanism x length sweep, with a sensible fixed ramp (lo=0.20, hi=0.60) ----
    print("\n(A) UPTREND MECHANISM x LENGTH  (breadth ramp 20%->60%, scale floor 0):")
    print(f"{'mech':<8}{'len':>5}  {'Sharpe':>7}{'CAGR':>7}{'MaxDD':>7}{'turn':>6}")
    mechs = [("sma", [50, 100, 150, 200, 252]), ("ema", [50, 100, 200]),
             ("mom", [63, 126, 252]), ("donch", [100, 200, 252]),
             ("macd", [0]), ("newhigh", [126, 252])]
    results = {}
    for mech, lens in mechs:
        for n in lens:
            b = breadth_series(prices, sel, mech, n)
            net, turn = gated_perf(gross_book, sel_turn, cash, scale_from_breadth(b, 0.20, 0.60), rf)
            st = perf(net, rf)
            typ = turn[turn.index >= pd.Timestamp(OOS)].sum() / ((prices.index >= pd.Timestamp(OOS)).sum() / 252)
            results[(mech, n)] = (b, st)
            print(f"{mech:<8}{n:>5}  {st['sharpe']:>7.2f}{st['cagr']*100:>6.0f}%"
                  f"{st['maxdd']*100:>6.0f}%{typ:>5.0f}x")

    # ---- (B) Scaling-map sweep (lo,hi) on the best couple of mechanisms, incl. scale->0 ----
    print("\n(B) SCALING MAP sweep  scale=clip((breadth-lo)/(hi-lo),0,1)  (lo=full-cash, hi=full-inv):")
    print(f"{'mech/len':<14}{'lo':>5}{'hi':>5}  {'Sharpe':>7}{'CAGR':>7}{'MaxDD':>7}{'turn':>6}")
    best_mechs = sorted(results, key=lambda k: -results[k][1]["sharpe"])[:3]
    for key in best_mechs:
        b = results[key][0]
        for lo, hi in [(0.0, 0.60), (0.10, 0.60), (0.18, 0.60), (0.18, 0.50),
                       (0.18, 0.70), (0.30, 0.60), (0.40, 0.70)]:
            net, turn = gated_perf(gross_book, sel_turn, cash, scale_from_breadth(b, lo, hi), rf)
            st = perf(net, rf)
            typ = turn[turn.index >= pd.Timestamp(OOS)].sum() / ((prices.index >= pd.Timestamp(OOS)).sum() / 252)
            print(f"{key[0]+str(key[1]):<14}{lo:>5.2f}{hi:>5.2f}  {st['sharpe']:>7.2f}"
                  f"{st['cagr']*100:>6.0f}%{st['maxdd']*100:>6.0f}%{typ:>5.0f}x")

    # ---- (C) Sub-period robustness of the overall best config ----
    print("\n(C) SUB-PERIOD robustness of the best (mech,len,lo,hi) vs base & SPY:")
    # find global best over a small grid
    grid_best, best_sh = None, -9
    for key in best_mechs:
        b = results[key][0]
        for lo, hi in [(0.18, 0.60), (0.18, 0.50), (0.0, 0.60), (0.30, 0.60)]:
            net, _ = gated_perf(gross_book, sel_turn, cash, scale_from_breadth(b, lo, hi), rf)
            sh = perf(net, rf)["sharpe"]
            if sh > best_sh:
                best_sh, grid_best = sh, (key, lo, hi, net)
    (mech, n), lo, hi, bnet = grid_best
    print(f"   best = breadth[{mech}{n}] ramp {lo:.2f}->{hi:.2f}  (OOS Sharpe {best_sh:.2f})")
    print(f"   {'period':<14}{'base Sh':>9}{'best Sh':>9}{'SPY Sh':>8}{'best DD':>9}")
    for label, a, c in [("OOS", OOS, "2026-12-31")] + [(s[0], s[1], s[2]) for s in SUBS]:
        w = (a, c)
        print(f"   {label:<14}{perf(base_net, rf, w)['sharpe']:>9.2f}"
              f"{perf(bnet, rf, w)['sharpe']:>9.2f}{perf(spy, rf, w)['sharpe']:>8.2f}"
              f"{perf(bnet, rf, w)['maxdd']*100:>8.0f}%")

    # ---- (D) Walk-forward: pick the uptrend MECHANISM monthly by past-only Sharpe ----
    print("\n(D) WALK-FORWARD mechanism selection (ramp 0.20->0.55, trailing-252d Sharpe, past-only):")
    cand = {f"{m}{n}": gated_perf(gross_book, sel_turn, cash,
                                  scale_from_breadth(breadth_series(prices, sel, m, n), 0.20, 0.55), rf)[0]
            for m, n in [("sma", 200), ("ema", 50), ("ema", 100), ("mom", 252),
                         ("donch", 200), ("donch", 252), ("macd", 0)]}
    mat = pd.DataFrame(cand)
    me = pd.DatetimeIndex(pd.Series(mat.index, index=mat.index)
                          .groupby([mat.index.year, mat.index.month]).last().values)
    chosen, picks = [], []
    for i, m in enumerate(me[:-1]):
        train = mat[mat.index <= m].tail(252)
        if len(train) < 252:
            continue
        rfw = rf.reindex(train.index).fillna(0.0)
        sh = {c: stats(train[c], rfw)["sharpe"] for c in mat.columns}
        best = max(sh, key=lambda c: (-9 if pd.isna(sh[c]) else sh[c]))
        seg = mat[(mat.index > m) & (mat.index <= me[i + 1])][best]
        chosen.append(seg); picks.append((m, best))
    wf = pd.concat(chosen)
    wfs = stats(wf, rf)
    from collections import Counter
    print(f"   walk-forward (adaptive mechanism)  Sharpe={wfs['sharpe']:.2f}  "
          f"CAGR={wfs['cagr']*100:.0f}%  MaxDD={wfs['maxdd']*100:.0f}%")
    print(f"   base no gate Sharpe={stats(base_net.reindex(wf.index), rf)['sharpe']:.2f}   "
          f"SPY Sharpe={stats(spy.reindex(wf.index), rf)['sharpe']:.2f}")
    print("   mechanism chosen (months):", dict(Counter(b for _, b in picks).most_common()))


if __name__ == "__main__":
    main()
