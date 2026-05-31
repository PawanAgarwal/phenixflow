#!/usr/bin/env python3
"""
Validate the FINAL strategy under DAILY rebalancing executed at EOD+10min.

Timing model (causal): target weights are computed from day T's close (12-month momentum,
200d trend filter, top-20 inverse-vol) and the book is set just after the close ("EOD+10min").
Those weights earn the close[T]->close[T+1] return. Turnover cost is charged on each rebalance.

This isolates the real question for daily rebalancing: momentum is slow-moving, so daily
recomputation mostly adds TURNOVER -- does the edge survive realistic trading cost?

We compare, on an identical daily return stream and a common cost model:
  * monthly rebalance (the published rule)   * weekly rebalance
  * daily rebalance                          * daily + no-trade band (turnover throttle)
across one-way cost levels {0, 1, 3, 5, 10} bps, vs SPY buy & hold.
"""
import numpy as np
import pandas as pd
import lib
from backtest import build_signals, candidate_weights, CASH_TICKERS
from strategy import FINAL_CFG

PERIODS = 252


def rebalance_calendar(days, freq):
    if freq == "daily":
        return set(days)
    if freq == "weekly":                      # last trading day of each ISO week
        s = pd.Series(days, index=days)
        return set(s.groupby([days.isocalendar().year, days.isocalendar().week]).last())
    if freq == "monthly":
        s = pd.Series(days, index=days)
        return set(s.groupby([days.year, days.month]).last())
    raise ValueError(freq)


def run_daily(prices, cfg, cash, sel_universe, freq, cost_bps, start, band=0.0):
    """Causal daily-equity engine. Returns (daily_net_ret, daily_gross_ret, turnover_series)."""
    days = prices.index[prices.index >= pd.Timestamp(start)]
    R = prices.pct_change()                   # daily asset returns
    moms, above_ma, vol, daily = build_signals(prices, cfg["lookbacks"], cfg["ma_window"])
    rebal_days = rebalance_calendar(days, freq)

    w_active = {}                             # weights earning today's return (set yesterday/earlier)
    gross, net, turn, idx = [], [], [], []
    cash_ret = R[cash]
    for t in days:
        rt = R.loc[t]
        # Today's portfolio return from weights set at the prior rebalance.
        g = sum(wv * (rt.get(tk, 0.0) if pd.notna(rt.get(tk, np.nan)) else 0.0)
                for tk, wv in w_active.items())
        cw = max(0.0, 1.0 - sum(w_active.values()))
        g += cw * (cash_ret.get(t, 0.0) if pd.notna(cash_ret.get(t, np.nan)) else 0.0)
        cost_today = 0.0
        if t in rebal_days:
            w_new = candidate_weights(t, sel_universe, moms, above_ma, vol, daily, cfg)
            # No-trade band: skip tiny adjustments to throttle turnover.
            if band > 0 and w_active:
                keys = set(w_new) | set(w_active)
                w_adj = dict(w_active)
                for k in keys:
                    nw = w_new.get(k, 0.0)
                    if abs(nw - w_active.get(k, 0.0)) >= band:
                        if nw > 0:
                            w_adj[k] = nw
                        else:
                            w_adj.pop(k, None)
                w_new = w_adj
            to = sum(abs(w_new.get(k, 0.0) - w_active.get(k, 0.0))
                     for k in set(w_new) | set(w_active))      # one-way turnover
            cost_today = to * cost_bps / 1e4
            turn.append(to)
            w_active = w_new
        net.append(g - cost_today)
        gross.append(g)
        idx.append(t)
    return (pd.Series(net, index=idx), pd.Series(gross, index=idx),
            pd.Series(turn))


def stats(daily_ret, rf_daily):
    r = daily_ret.dropna()
    rf = rf_daily.reindex(r.index).fillna(0.0)
    ann_ret = (1 + r).prod() ** (PERIODS / len(r)) - 1
    ann_vol = r.std(ddof=1) * np.sqrt(PERIODS)
    sharpe = (r - rf).mean() * PERIODS / ann_vol if ann_vol > 0 else np.nan
    eq = (1 + r).cumprod()
    dd = (eq / eq.cummax() - 1).min()
    return ann_ret, ann_vol, sharpe, dd


def main():
    prices = lib.load_prices()
    cash = next(t for t in CASH_TICKERS if t in prices.columns)
    sel = [c for c in prices.columns if c not in CASH_TICKERS]
    start = "2017-06-30"
    R = prices.pct_change()
    rf_daily = R[cash]
    days_oos = prices.index[prices.index >= pd.Timestamp("2019-07-01")]

    # SPY benchmark on the daily stream.
    for label, win in (("FULL 2017-07..2026-05", prices.index >= pd.Timestamp(start)),
                       ("OOS  2019-07..2026-05", prices.index >= pd.Timestamp("2019-07-01"))):
        spy = R["SPY"][win].dropna()
        sa = stats(spy, rf_daily)
        print(f"\n### {label}  (annualised; rf=cash)")
        print(f"{'variant':<34}{'CAGR':>8}{'Vol':>7}{'Sharpe':>8}{'MaxDD':>8}"
              f"{'avgTO':>8}{'turn/yr':>8}")
        print(f"{'SPY buy & hold':<34}{sa[0]*100:>7.1f}%{sa[1]*100:>6.1f}%"
              f"{sa[2]:>8.2f}{sa[3]*100:>7.1f}%{'-':>8}{'-':>8}")

    # Precompute each variant once at cost=0, then re-charge cost ex-post via turnover.
    print("\n" + "=" * 96)
    print("DAILY-vs-lower-frequency rebalance, cost sensitivity (one-way bps). OOS 2019-07..2026-05")
    print("=" * 96)
    variants = [("monthly", "monthly", 0.0), ("weekly", "weekly", 0.0),
                ("daily", "daily", 0.0), ("daily+band2%", "daily", 0.02),
                ("daily+band3%", "daily", 0.03)]
    spy_oos = stats(R["SPY"][prices.index >= pd.Timestamp("2019-07-01")].dropna(), rf_daily)

    print(f"\n{'variant':<16}{'cost':>6}{'CAGR':>8}{'Vol':>7}{'Sharpe':>8}{'MaxDD':>8}"
          f"{'turn/yr':>9}{'cost drag':>11}")
    for name, freq, band in variants:
        net0, gross0, turn = run_daily(prices, FINAL_CFG, cash, sel, freq, 0.0,
                                       "2017-06-30", band=band)
        net0 = net0[net0.index >= pd.Timestamp("2019-07-01")]
        gross0 = gross0[gross0.index >= pd.Timestamp("2019-07-01")]
        years = len(net0) / PERIODS
        turn_per_yr = turn.sum() / (len(net0) / PERIODS) if len(turn) else 0.0
        # cost rebuild: need per-day turnover on the OOS window; recompute with each cost.
        for cb in (0, 1, 3, 5, 10):
            net, gross, _ = run_daily(prices, FINAL_CFG, cash, sel, freq, cb,
                                      "2017-06-30", band=band)
            net = net[net.index >= pd.Timestamp("2019-07-01")]
            ca, vo, sh, dd = stats(net, rf_daily)
            drag = (stats(gross0, rf_daily)[0] - ca) * 100
            tag = name if cb == 0 else ""
            print(f"{tag:<16}{cb:>5}b{ca*100:>7.1f}%{vo*100:>6.1f}%{sh:>8.2f}"
                  f"{dd*100:>7.1f}%{turn_per_yr:>8.1f}x{drag:>10.1f}%")
        print(f"{'  (SPY)':<16}{'':>6}{spy_oos[0]*100:>7.1f}%{spy_oos[1]*100:>6.1f}%"
              f"{spy_oos[2]:>8.2f}{spy_oos[3]*100:>7.1f}%")
    print("\nturn/yr = one-way annual turnover (1.0x = replace 100% of book once).")


if __name__ == "__main__":
    main()
