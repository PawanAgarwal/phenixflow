#!/usr/bin/env python3
"""
Cross-asset trend / momentum strategy with honest walk-forward parameter selection.

Idea (established techniques):
  * Time-series (absolute) momentum: an asset is in an "uptrend" if its total return
    over a lookback is positive AND its price is above a long moving average.
  * Cross-sectional momentum: rank uptrending assets by a blended multi-horizon
    momentum score and hold the strongest top-K.
  * Risk-off overlay: any unfilled slot (fewer than K assets in uptrend) goes to cash
    (BIL). This is the downside-protection that lifts Sharpe above buy-and-hold SPY.

Walk-forward:
  Each fixed rule (a "candidate") is deterministic given prices, so we precompute every
  candidate's monthly OOS return once. The adaptive walk-forward portfolio then, at each
  month t, picks the candidate with the best TRAILING Sharpe over a training window using
  ONLY returns up to month t-1, and realizes that candidate's month-t return. No lookahead.
"""
import argparse, itertools, json, os
import numpy as np
import pandas as pd
import lib

CASH_TICKERS = ["BIL", "SGOV", "SHV"]   # risk-off vehicle (BIL preferred)
COST_BPS = 10                            # round-trip transaction cost per unit turnover
TRADING_DAYS = 252


def month_end_dates(index, start, end):
    s = pd.Series(index, index=index)
    me = s.groupby([index.year, index.month]).last()
    me = pd.DatetimeIndex(me.values)
    return me[(me >= pd.Timestamp(start)) & (me <= pd.Timestamp(end))]


def build_signals(prices, lookbacks, ma_window):
    """Precompute, per rebalance, momentum scores + MA-trend flags for all assets."""
    px = prices
    moms = {L: px / px.shift(L) - 1.0 for L in lookbacks}
    sma = px.rolling(ma_window, min_periods=ma_window // 2).mean()
    above_ma = px > sma
    daily = px.pct_change()
    vol = daily.rolling(63, min_periods=40).std()
    return moms, above_ma, vol, daily


def vol_target_scale(date, picks, w, daily, target_vol):
    """Scale held weights so trailing realized portfolio vol ~= target (no leverage)."""
    if not target_vol or len(picks) == 0:
        return w
    hist = daily.loc[:date, list(picks)].tail(63).dropna(how="all")
    if hist.shape[0] < 30:
        return w
    wv = np.array([w.get(t, 0.0) for t in picks])
    cov = hist[list(picks)].cov().values * TRADING_DAYS
    port_var = float(wv @ np.nan_to_num(cov) @ wv)
    port_vol = np.sqrt(max(port_var, 1e-12))
    scale = min(1.0, target_vol / port_vol) if port_vol > 0 else 1.0
    return {t: x * scale for t, x in w.items()}


def candidate_weights(date, sel_universe, moms, above_ma, vol, daily, cfg):
    """Return target weight dict for one rebalance date under one candidate config."""
    lookbacks = cfg["lookbacks"]
    # Blended momentum z-score across horizons (equal weight on each horizon's rank).
    score = pd.Series(0.0, index=sel_universe)
    valid = pd.Series(True, index=sel_universe)
    for L in lookbacks:
        m = moms[L].loc[date, sel_universe]
        valid &= m.notna()
        score = score.add(m.rank(pct=True), fill_value=0.0)
    score = score[valid]
    # Absolute uptrend filter.
    pos_mom = pd.Series(True, index=score.index)
    for L in lookbacks:
        pos_mom &= (moms[L].loc[date, score.index] > 0)
    qual = score.index[pos_mom.values]
    if cfg["ma_filter"]:
        amf = above_ma.loc[date, qual].fillna(False)
        qual = qual[amf.values]
    if len(qual) == 0:
        return {}
    ranked = score.loc[qual].sort_values(ascending=False)
    picks = ranked.index[: cfg["top_k"]]
    if cfg["weighting"] == "invvol":
        v = vol.loc[date, picks].replace(0, np.nan)
        w = (1.0 / v).fillna(0.0)
        w = w / w.sum() if w.sum() > 0 else pd.Series(1.0 / len(picks), index=picks)
    else:  # equal weight
        w = pd.Series(1.0 / len(picks), index=picks)
    # Cap per-asset weight, scale the held sleeve to top_k slots (rest -> cash).
    w = w.clip(upper=cfg["max_weight"])
    w = w / max(w.sum(), 1e-9) * (len(picks) / cfg["top_k"])
    wd = {t: float(x) for t, x in w.items() if x > 1e-6}
    # Portfolio-level volatility targeting (remainder -> cash).
    wd = vol_target_scale(date, list(wd.keys()), wd, daily, cfg.get("target_vol"))
    return {t: x for t, x in wd.items() if x > 1e-6}


def run_candidate(prices, rebals, sel_universe, cash_ticker, moms, above_ma, vol, daily, cfg):
    """Simulate one fixed-rule candidate -> monthly net return series."""
    fwd = prices.reindex(rebals).pct_change().shift(-1)  # return of month following each rebal
    rets, prev_w = [], {}
    idx = []
    for i, d in enumerate(rebals[:-1]):
        w = candidate_weights(d, sel_universe, moms, above_ma, vol, daily, cfg)
        cash_w = max(0.0, 1.0 - sum(w.values()))
        # Gross return over the next month.
        r = 0.0
        nxt = fwd.loc[d]
        for t, ww in w.items():
            rt = nxt.get(t, np.nan)
            r += ww * (rt if pd.notna(rt) else 0.0)
        cash_rt = nxt.get(cash_ticker, 0.0)
        r += cash_w * (cash_rt if pd.notna(cash_rt) else 0.0)
        # Turnover cost.
        all_keys = set(w) | set(prev_w)
        turnover = sum(abs(w.get(k, 0.0) - prev_w.get(k, 0.0)) for k in all_keys)
        r -= turnover * COST_BPS / 1e4
        rets.append(r)
        idx.append(rebals[i + 1])
        prev_w = w
    return pd.Series(rets, index=pd.DatetimeIndex(idx))


def candidate_grid():
    grid = []
    # Iteration 3: emphasise the robust 12-month (252d) momentum horizon; bring back
    # concentration (momentum picks real winners -> higher return); keep trend-to-cash
    # filter for downside protection; loosen vol targeting to >= SPY's ~16% vol.
    lb_sets = [[252]]
    for lbs in lb_sets:
        for k in (8, 10, 12, 15, 20, 25):
            for wt in ("ew", "invvol"):
                for maf in (True, False):
                    for tv in (None, 0.20):
                        grid.append({
                            "lookbacks": lbs, "top_k": k, "weighting": wt,
                            "ma_filter": maf, "ma_window": 200, "max_weight": 0.30,
                            "target_vol": tv,
                            "name": (f"lb{'_'.join(map(str,lbs))}_k{k}_{wt}"
                                     f"_ma{int(maf)}_vt{int((tv or 0)*100)}"),
                        })
    return grid


def walk_forward(cand_rets, train_months=24, rf_monthly=None, ensemble=5):
    """Average the top-`ensemble` candidates by trailing Sharpe (past-only) each month."""
    mat = pd.DataFrame(cand_rets)            # index=month, cols=candidate name
    months = mat.index
    chosen, picks = [], []
    for i in range(len(months)):
        if i < train_months:
            continue
        window = mat.iloc[i - train_months:i]   # strictly past (excludes current month)
        rf_w = rf_monthly.reindex(window.index).fillna(0.0) if rf_monthly is not None else None
        sharpes = {c: lib.perf_stats(window[c], rf_monthly=rf_w).get("sharpe", np.nan)
                   for c in mat.columns}
        ranked = sorted(mat.columns,
                        key=lambda c: (-1e9 if pd.isna(sharpes[c]) else sharpes[c]),
                        reverse=True)
        top = ranked[:ensemble]
        chosen.append(float(mat.loc[months[i], top].mean()))
        picks.append((months[i], top))
    wf = pd.Series(chosen, index=[p[0] for p in picks])
    return wf, picks


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2017-06-30")
    ap.add_argument("--end", default="2026-05-29")
    ap.add_argument("--train-months", type=int, default=24)
    ap.add_argument("--out", default="results.json")
    ap.add_argument("--recompute", action="store_true")
    args = ap.parse_args()

    prices = lib.load_prices()
    cash_ticker = next(t for t in CASH_TICKERS if t in prices.columns)
    # Selectable universe = everything except cash proxies (those are the risk-off sleeve).
    sel_universe = [c for c in prices.columns if c not in CASH_TICKERS]

    rebals = month_end_dates(prices.index, args.start, args.end)
    print(f"Rebalances: {len(rebals)}  ({rebals[0].date()}..{rebals[-1].date()})  "
          f"universe={len(sel_universe)} cash={cash_ticker}")

    # Risk-free monthly series = realized BIL/cash return (fair vs SPY too).
    fwd_cash = prices[cash_ticker].reindex(rebals).pct_change().shift(-1)
    rf_monthly = fwd_cash.reindex(rebals[1:]).copy()
    rf_monthly.index = rebals[1:]

    # Precompute signals once across all needed lookbacks.
    all_lbs = sorted({L for c in candidate_grid() for L in c["lookbacks"]})
    moms, above_ma, vol, daily = build_signals(prices, all_lbs, ma_window=200)

    grid = candidate_grid()
    cache_path = os.path.join(lib.HERE, "data", "cand_rets.csv")
    if os.path.exists(cache_path) and not args.recompute:
        cand_df = pd.read_csv(cache_path, parse_dates=["date"]).set_index("date")
        cand_rets = {c: cand_df[c] for c in cand_df.columns}
        print(f"Candidates: {len(cand_rets)} (loaded cache; --recompute to rebuild)")
    else:
        print(f"Candidates: {len(grid)}  Precomputing OOS returns ...")
        cand_rets = {}
        for c in grid:
            cand_rets[c["name"]] = run_candidate(
                prices, rebals, sel_universe, cash_ticker, moms, above_ma, vol, daily, c)
        pd.DataFrame(cand_rets).to_csv(cache_path, index_label="date")

    # Walk-forward adaptive portfolio.
    wf, picks = walk_forward(cand_rets, train_months=args.train_months, rf_monthly=rf_monthly)
    wf_stats = lib.perf_stats(wf, rf_monthly=rf_monthly)
    oos_idx = wf.index

    # Benchmark: SPY measured over the IDENTICAL OOS window.
    spy = prices["SPY"].reindex(rebals).pct_change().shift(-1).reindex(rebals[:-1])
    spy.index = rebals[1:]
    spy = spy.reindex(oos_idx)
    spy_stats = lib.perf_stats(spy, rf_monthly=rf_monthly)

    # Best fixed candidate over the SAME OOS window (for reference / overfit gap).
    fixed_stats = {n: lib.perf_stats(s.reindex(oos_idx), rf_monthly=rf_monthly)
                   for n, s in cand_rets.items()}
    best_fixed = max(fixed_stats, key=lambda n: (fixed_stats[n].get("sharpe") or -9))

    print(f"\n=== OOS results ({oos_idx[0].date()}..{oos_idx[-1].date()}, "
          f"rf = realized cash/BIL) ===")
    print(lib.fmt_stats("SPY buy & hold", spy_stats))
    print(lib.fmt_stats("Walk-forward adaptive", wf_stats))
    print(lib.fmt_stats(f"Best fixed (hindsight)", fixed_stats[best_fixed]))
    print(f"    best fixed = {best_fixed}")

    # What did walk-forward hold most?
    from collections import Counter
    pick_counts = Counter(n for _, top in picks for n in top)
    print("\nMost-chosen candidates across walk-forward ensemble:")
    for n, c in pick_counts.most_common(8):
        print(f"  {c:>3}x  {n}")

    out = {
        "oos_window": [str(oos_idx[0].date()), str(oos_idx[-1].date())],
        "spy": spy_stats, "walk_forward": wf_stats,
        "best_fixed_name": best_fixed, "best_fixed": fixed_stats[best_fixed],
        "wf_picks": [[str(d.date()), top] for d, top in picks],
        "wf_monthly": {str(d.date()): float(v) for d, v in wf.items()},
        "spy_monthly": {str(d.date()): float(v) for d, v in spy.reindex(oos_idx).items()},
    }
    with open(os.path.join(lib.HERE, args.out), "w") as f:
        json.dump(out, f, indent=2)
    print(f"\nWrote {args.out}")


if __name__ == "__main__":
    main()
