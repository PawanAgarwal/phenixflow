#!/usr/bin/env python3
"""
Emit a strategy-service report.json artifact for the Breadth EMA-50 gated strategy, in the
schema the phenixflow strategy-service consumes (summary / settings / source / equitySeries /
snapshots / openPositions). Daily equity series for the chart; monthly snapshots for holdings.

Numbers come from the SAME verified analytic engine reconciled in verify_breadth_ema50.py.
"""
import json, os
import numpy as np
import pandas as pd
import lib
from backtest import CASH_TICKERS, build_signals, candidate_weights
from daily_opt import Overlays, run, stats, BASE
from breadth_explore import breadth_series, scale_from_breadth, gated_perf

COST, LO, HI, EMALEN = 5, 0.18, 0.50, 50
START = "2017-06-30"          # full history for the equity series
DEFAULT_START = "2019-07-01"  # default chart window (trusted OOS)
OUT = os.path.join(lib.HERE, "..", "..", "projects", "asset-trend-breadth",
                   "artifacts", "breadth-ema50-report.json")


def main():
    prices = lib.load_prices()
    cash_t = next(t for t in CASH_TICKERS if t in prices.columns)
    sel = [c for c in prices.columns if c not in CASH_TICKERS]
    rawclose, volume = lib.load_volume(prices.index, list(prices.columns))
    ov = Overlays(prices, rawclose, volume)
    rf = ov.R[cash_t]
    acm = lib.asset_class_map()
    moms, above_ma, vol, daily = build_signals(prices, BASE["lookbacks"], BASE["ma_window"])
    selcfg = {k: BASE[k] for k in ["lookbacks", "top_k", "weighting", "ma_filter",
                                   "ma_window", "max_weight"]}
    selcfg["target_vol"] = None

    # Daily gated net returns (verified engine) + scale series.
    _, gross_book, sel_turn = run(prices, ov, cash_t, sel, {**BASE, "band": 0.0}, START, 0)
    breadth = breadth_series(prices, sel, "ema", EMALEN)
    scale = scale_from_breadth(breadth, LO, HI)
    net, _ = gated_perf(gross_book, sel_turn, ov.R[cash_t], scale, rf, lag=1)
    net = net[net.index >= pd.Timestamp(START)].dropna()
    spy = ov.R["SPY"].reindex(net.index).fillna(0.0)
    qqq = ov.R["QQQ"].reindex(net.index).fillna(0.0) if "QQQ" in prices.columns else None

    eq = (1 + net).cumprod() * 100.0
    eq_spy = (1 + spy).cumprod() * 100.0
    eq_qqq = (1 + qqq).cumprod() * 100.0 if qqq is not None else None

    # Daily equity series for the chart.
    equity_series = []
    base0 = float(eq.iloc[0])
    for i, d in enumerate(net.index):
        equity_series.append({
            "date": d.strftime("%Y-%m-%d"),
            "signalDate": net.index[i - 1].strftime("%Y-%m-%d") if i > 0 else d.strftime("%Y-%m-%d"),
            "equity": round(float(eq.loc[d]), 4),
            "totalReturn": round(float(eq.loc[d] / base0 - 1), 6),
            "spyReturn": round(float(eq_spy.loc[d] / float(eq_spy.iloc[0]) - 1), 6),
            "qqqReturn": (round(float(eq_qqq.loc[d] / float(eq_qqq.iloc[0]) - 1), 6)
                          if eq_qqq is not None else None),
        })

    # Monthly snapshots (holdings = book * gross scale; realized = that month's net return).
    me = pd.DatetimeIndex(pd.Series(net.index, index=net.index)
                          .groupby([net.index.year, net.index.month]).last().values)
    snapshots, prev_hold = [], {}
    for j, d in enumerate(me):
        book = candidate_weights(d, sel, moms, above_ma, vol, daily, selcfg)
        s = float(scale.loc[d])
        hold = {t: w * s for t, w in book.items() if w * s > 1e-6}
        seg = net[(net.index > (me[j - 1] if j > 0 else net.index[0] - pd.Timedelta(days=1)))
                  & (net.index <= d)]
        seg_spy = spy.reindex(seg.index)
        mret = float((1 + seg).prod() - 1)
        start_eq = float(eq.loc[me[j - 1]]) if j > 0 else base0
        end_eq = float(eq.loc[d])
        turnover = sum(abs(hold.get(k, 0.0) - prev_hold.get(k, 0.0))
                       for k in set(hold) | set(prev_hold))
        top = sorted(hold.items(), key=lambda x: -x[1])
        holdings = [{"ticker": t, "weight": round(w, 6), "weightPct": round(w * 100, 4),
                     "dollars": round(end_eq * w, 2), "assetClass": acm.get(t, "n/a"),
                     "mom12m": round(float(moms[252].loc[d, t]), 4)}
                    for t, w in top]
        snapshots.append({
            "date": d.strftime("%Y-%m-%d"),
            "nextDate": me[j + 1].strftime("%Y-%m-%d") if j + 1 < len(me) else None,
            "equityBeforeNextSession": round(end_eq, 4),
            "grossExposure": round(s, 4),
            "turnover": round(turnover, 6),
            "turnoverPct": round(turnover * 100, 4),
            "topHoldings": ", ".join(t for t, _ in top[:5]),
            "benchmarkReturns": {"spy": round(float((1 + seg_spy).prod() - 1), 6), "qqq": None},
            "holdings": holdings,
            "realized": {
                "netReturn": round(mret, 6), "netReturnPct": round(mret * 100, 4),
                "startEquity": round(start_eq, 4), "endEquity": round(end_eq, 4),
                "pnlDollars": round(end_eq - start_eq, 4),
                "grossReturn": round(mret, 6), "grossReturnPct": round(mret * 100, 4),
                "trades": len(holdings),
            },
        })
        prev_hold = hold

    # Overall stats (annualised daily, rf=cash).
    full = stats(net, rf)
    oos = stats(net[net.index >= pd.Timestamp(DEFAULT_START)], rf)
    monthly = pd.Series([s["realized"]["netReturn"] for s in snapshots])
    total_ret = float(eq.iloc[-1] / base0 - 1)
    summary = {
        "startDate": net.index[0].strftime("%Y-%m-%d"),
        "endDate": net.index[-1].strftime("%Y-%m-%d"),
        "latestRebalanceDate": me[-1].strftime("%Y-%m-%d"),
        "latestCompletedDate": net.index[-1].strftime("%Y-%m-%d"),
        "snapshots": len(snapshots),
        "tradingDays": len(net),
        "totalReturn": round(total_ret, 6),
        "totalReturnPct": round(total_ret * 100, 4),
        "maxDrawdownPct": round(full["maxdd"] * 100, 4),
        "sharpe": round(full["sharpe"], 4),
        "sharpeOos": round(oos["sharpe"], 4),
        "cagrPct": round(full["cagr"] * 100, 4),
        "volPct": round(full["vol"] * 100, 4),
        "hitRatePct": round(float((monthly > 0).mean() * 100), 2),
        "winRate": round(float((monthly > 0).mean()), 4),
        "trades": len(snapshots),
        "benchmarkSpySharpe": round(stats(spy, rf)["sharpe"], 4),
    }
    open_positions = [{"ticker": h["ticker"], "weight": h["weight"], "weightPct": h["weightPct"],
                       "assetClass": h["assetClass"], "mom12m": h["mom12m"]}
                      for h in snapshots[-1]["holdings"]]

    report = {
        "schemaVersion": "phenixflow.strategyReport.v1",
        "generatedAt": pd.Timestamp.utcnow().strftime("%Y-%m-%dT%H:%M:%S.000Z"),
        "source": {
            "provider": "Yahoo Finance daily adjusted close (research/asset-universe tickers)",
            "engine": "research/asset-trend-strategy (verified analytic == from-scratch, 1-day lag)",
            "reportPath": "projects/asset-trend-breadth/artifacts/breadth-ema50-report.json",
        },
        "settings": {
            "startDate": net.index[0].strftime("%Y-%m-%d"),
            "endDate": net.index[-1].strftime("%Y-%m-%d"),
            "selection": "top-20 by 12-month (252d) total-return momentum, >200d SMA, inverse-vol, 30% cap, monthly",
            "gate": f"daily breadth = % of universe > EMA{EMALEN}; exposure ramp {LO:.2f}->{HI:.2f} (0% cash below {LO:.0%}, 100% at {HI:.0%})",
            "costBps": COST, "rebalance": "monthly book + daily exposure gate",
        },
        "summary": summary,
        "latest": snapshots[-1],
        "equitySeries": equity_series,
        "snapshots": snapshots,
        "openPositions": open_positions,
        "trades": [],
    }
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with open(OUT, "w") as f:
        json.dump(report, f, separators=(",", ":"))
    sz = os.path.getsize(OUT) / 1024
    print(f"Wrote {OUT}  ({sz:.0f} KB)")
    print(f"  window {summary['startDate']}..{summary['endDate']}  snapshots={summary['snapshots']}")
    print(f"  Sharpe full={summary['sharpe']}  OOS={summary['sharpeOos']}  "
          f"totalRet={summary['totalReturnPct']}%  maxDD={summary['maxDrawdownPct']}%  "
          f"hitRate={summary['hitRatePct']}%  SPY Sharpe={summary['benchmarkSpySharpe']}")
    print(f"  latest holdings ({snapshots[-1]['date']}): {snapshots[-1]['topHoldings']} "
          f"gross={snapshots[-1]['grossExposure']}")


if __name__ == "__main__":
    main()
