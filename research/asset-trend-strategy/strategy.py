#!/usr/bin/env python3
"""
FINAL cross-asset trend strategy — "Top-20 12-month-momentum, inverse-vol, trend-filtered".

Rule (established techniques, fixed a priori — see NOTES.md for the iteration history):
  * Universe: 245 cross-asset ETF/proxy sleeves from research/asset-universe (ex-cash).
  * Signal:  12-month (252-trading-day) total-return momentum = the asset's established uptrend.
  * Uptrend filter: positive 12-month momentum AND price above its 200-day moving average;
    sleeves failing the filter are dropped, their weight goes to cash (BIL).
  * Selection: hold the top 20 by momentum (diversification removes idiosyncratic risk).
  * Weighting: inverse-volatility (63-day), 30% cap, monthly rebalance, 10bps turnover cost.

Outputs: prints SPY-vs-strategy comparison, an honest walk-forward, a calendar-year robustness
table, and the CURRENT in-uptrend holdings; writes strategy_results.json + equity_curve.csv.
"""
import json, os
import numpy as np
import pandas as pd
import lib
from backtest import (month_end_dates, build_signals, run_candidate, candidate_weights,
                      CASH_TICKERS, walk_forward)

FINAL_CFG = {"lookbacks": [252], "top_k": 20, "weighting": "invvol",
             "ma_filter": True, "ma_window": 200, "max_weight": 0.30,
             "target_vol": None, "name": "FINAL_top20_12m_invvol_trend"}


def yearly_table(strat, spy, rf):
    rows = []
    for yr in sorted({d.year for d in strat.index}):
        ss = strat[[d.year == yr for d in strat.index]]
        bs = spy.reindex(ss.index)
        rr = rf.reindex(ss.index)
        s_strat = lib.perf_stats(ss, rf_monthly=rr)
        s_spy = lib.perf_stats(bs, rf_monthly=rr)
        rows.append((yr, s_strat, s_spy))
    return rows


def main():
    prices = lib.load_prices()
    cash = next(t for t in CASH_TICKERS if t in prices.columns)
    sel_universe = [c for c in prices.columns if c not in CASH_TICKERS]
    rebals = month_end_dates(prices.index, "2017-06-30", "2026-05-29")

    rf = prices[cash].reindex(rebals).pct_change()
    spy = prices["SPY"].reindex(rebals).pct_change()
    moms, above_ma, vol, daily = build_signals(prices, [252], 200)

    # --- Final fixed rule, full sample ---
    strat = run_candidate(prices, rebals, sel_universe, cash, moms, above_ma, vol, daily, FINAL_CFG)
    full_idx = strat.dropna().index
    s_full = lib.perf_stats(strat, rf_monthly=rf)
    spy_full = lib.perf_stats(spy.reindex(full_idx), rf_monthly=rf)

    print("=" * 78)
    print("FINAL STRATEGY: top-20 12-month momentum, inverse-vol, 200d trend-to-cash filter")
    print("=" * 78)
    print(f"\nFull sample {full_idx[0].date()}..{full_idx[-1].date()} "
          f"(rf = realized cash/BIL):")
    print(lib.fmt_stats("  SPY buy & hold", spy_full))
    print(lib.fmt_stats("  Strategy (fixed rule)", s_full))

    # --- Honest walk-forward over the 12-month family (no hindsight on K/weighting) ---
    cand = pd.read_csv(os.path.join(lib.HERE, "data", "cand_rets.csv"),
                       parse_dates=["date"]).set_index("date")
    fam = [c for c in cand.columns if c.startswith("lb252_") and "_ma1_" in c]
    wf, picks = walk_forward({c: cand[c] for c in fam}, train_months=24,
                             rf_monthly=rf, ensemble=5)
    oos = wf.index
    s_wf = lib.perf_stats(wf, rf_monthly=rf)
    s_spy_oos = lib.perf_stats(spy.reindex(oos), rf_monthly=rf)
    print(f"\nWalk-forward (top-5 trailing-Sharpe within 12m+trend family), OOS "
          f"{oos[0].date()}..{oos[-1].date()}:")
    print(lib.fmt_stats("  SPY buy & hold", s_spy_oos))
    print(lib.fmt_stats("  Walk-forward strategy", s_wf))

    # --- Calendar-year robustness (fixed rule vs SPY) ---
    print("\nCalendar-year robustness (strategy vs SPY, annualised Sharpe):")
    print(f"  {'year':<6}{'strat ret':>10}{'spy ret':>10}{'strat Shrp':>12}{'spy Shrp':>10}")
    yt = yearly_table(strat.dropna(), spy, rf)
    wins = 0
    for yr, st, sp in yt:
        win = (st.get("sharpe") or -9) > (sp.get("sharpe") or -9)
        wins += win
        print(f"  {yr:<6}{st['cagr']*100:>9.1f}%{sp['cagr']*100:>9.1f}%"
              f"{st.get('sharpe',0):>12.2f}{sp.get('sharpe',0):>10.2f}{'  *' if win else ''}")
    print(f"  -> strategy Sharpe > SPY in {wins}/{len(yt)} calendar years")

    # --- Current holdings: which asset classes are in uptrend right now ---
    last = rebals[-1]
    w = candidate_weights(last, sel_universe, moms, above_ma, vol, daily, FINAL_CFG)
    acm = lib.asset_class_map()
    cash_w = max(0.0, 1.0 - sum(w.values()))
    print(f"\nCurrent in-uptrend holdings as of {last.date()} "
          f"(cash sleeve {cash_w*100:.0f}%):")
    for t, ww in sorted(w.items(), key=lambda x: -x[1]):
        m12 = moms[252].loc[last, t]
        print(f"  {t:<7} {ww*100:5.1f}%   12m mom {m12*100:6.1f}%   "
              f"[{acm.get(t, 'n/a')}]")

    # --- Persist ---
    equity = (1 + strat.dropna()).cumprod()
    spy_eq = (1 + spy.reindex(full_idx)).cumprod()
    pd.DataFrame({"strategy": equity, "spy": spy_eq}).to_csv(
        os.path.join(lib.HERE, "equity_curve.csv"), index_label="date")
    out = {
        "rule": FINAL_CFG,
        "full_sample": {"window": [str(full_idx[0].date()), str(full_idx[-1].date())],
                        "strategy": s_full, "spy": spy_full},
        "walk_forward": {"window": [str(oos[0].date()), str(oos[-1].date())],
                         "strategy": s_wf, "spy": s_spy_oos},
        "yearly": [{"year": yr, "strategy_sharpe": st.get("sharpe"),
                    "spy_sharpe": sp.get("sharpe"),
                    "strategy_cagr": st.get("cagr"), "spy_cagr": sp.get("cagr")}
                   for yr, st, sp in yt],
        "current_holdings": {t: {"weight": w[t],
                                 "mom_12m": float(moms[252].loc[last, t]),
                                 "asset_class": acm.get(t, "n/a")} for t in w},
        "current_cash_weight": cash_w,
    }
    with open(os.path.join(lib.HERE, "strategy_results.json"), "w") as f:
        json.dump(out, f, indent=2)
    print("\nWrote strategy_results.json + equity_curve.csv")


if __name__ == "__main__":
    main()
