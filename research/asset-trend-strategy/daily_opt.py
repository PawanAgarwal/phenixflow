#!/usr/bin/env python3
"""
Optimised DAILY-rebalance strategy: slow 12-month-momentum SELECTION (monthly) + fast
daily PRICE/VOLUME RISK OVERLAYS, with hysteresis/no-trade bands to control turnover.

Reframe of the iteration-5 failure: recomputing the *selection* daily churns names around the
top-K cutoff for no information gain. Here selection stays monthly; only daily risk overlays
update each day, so the strategy is "daily" where daily data is informative (risk/timing) and
slow where it should be (which trends to own).

Daily overlays (each toggleable):
  * fast_gate : per-sleeve fast trend gate (price vs EMA, with re-enter hysteresis) -> to cash.
  * vol_scale : per-sleeve daily inverse-vol scaling toward a target (de-risk into vol spikes).
  * obv       : volume confirmation -- require on-balance-volume uptrend (accumulation).
  * regime    : market gate -- cut gross exposure when SPY < its EMA (fast crash protection).
  * liq       : dollar-volume liquidity floor on the selectable universe.
Turnover is throttled with a final-weight no-trade band.
"""
import argparse
import numpy as np
import pandas as pd
import lib
from backtest import build_signals, candidate_weights, CASH_TICKERS

PERIODS = 252


def calendar(days, freq):
    s = pd.Series(days, index=days)
    if freq == "daily":
        return set(days)
    if freq == "weekly":
        return set(s.groupby([days.isocalendar().year, days.isocalendar().week]).last())
    if freq == "monthly":
        return set(s.groupby([days.year, days.month]).last())
    raise ValueError(freq)


def stats(daily_ret, rf_daily):
    r = daily_ret.dropna()
    rf = rf_daily.reindex(r.index).fillna(0.0)
    ann_ret = (1 + r).prod() ** (PERIODS / len(r)) - 1
    ann_vol = r.std(ddof=1) * np.sqrt(PERIODS)
    sharpe = (r - rf).mean() * PERIODS / ann_vol if ann_vol > 0 else np.nan
    eq = (1 + r).cumprod()
    dd = (eq / eq.cummax() - 1).min()
    down = r[r < 0].std(ddof=1) * np.sqrt(PERIODS)
    sortino = (r - rf).mean() * PERIODS / down if down > 0 else np.nan
    return {"cagr": ann_ret, "vol": ann_vol, "sharpe": sharpe, "sortino": sortino, "maxdd": dd}


class Overlays:
    """Precompute all daily price/volume signal frames once."""
    def __init__(self, prices, rawclose, volume):
        self.px = prices
        self.R = prices.pct_change()
        # Fast trend gates (price vs EMA) with separate enter/exit spans for hysteresis.
        self.ema = {n: prices.ewm(span=n, min_periods=n // 2).mean()
                    for n in (10, 20, 50, 100, 200)}
        # Daily realized vol (annualised) over a short window.
        self.vol20 = self.R.rolling(20, min_periods=10).std() * np.sqrt(PERIODS)
        # On-balance volume + its slope (accumulation when rising).
        sign = np.sign(prices.diff()).fillna(0.0)
        obv = (sign * volume).cumsum()
        self.obv_up = obv > obv.shift(20)
        # Dollar volume (20d ADV) for liquidity screen.
        self.dollar_adv = (rawclose * volume).rolling(20, min_periods=10).mean()
        # Market regime: SPY vs its EMA (fast crash gate); cache per span.
        self.spy = prices["SPY"]
        self._spy_ema = {}

    def spy_ema(self, span):
        if span not in self._spy_ema:
            self._spy_ema[span] = self.spy.ewm(span=span, min_periods=span // 2).mean()
        return self._spy_ema[span]


def run(prices, ov, cash, sel_universe, cfg, start, cost_bps):
    days = prices.index[prices.index >= pd.Timestamp(start)]
    R = ov.R
    cash_ret = R[cash]
    moms, above_ma, vol_sig, daily = build_signals(prices, cfg["lookbacks"], cfg["ma_window"])
    sel_days = calendar(prices.index, cfg["selection_freq"])

    sel_cfg = {"lookbacks": cfg["lookbacks"], "top_k": cfg["top_k"],
               "weighting": cfg["weighting"], "ma_filter": cfg["ma_filter"],
               "ma_window": cfg["ma_window"], "max_weight": cfg["max_weight"],
               "target_vol": None, "name": "sel"}
    band = cfg.get("band", 0.0)
    w_base = {}          # current slow selection book (held names + base weights)
    w_active = {}        # weights earning today's return (set yesterday)
    gate_on = {}         # stateful hysteresis gate per name (True=held)
    ret_hist = []        # trailing realized portfolio returns for portfolio vol-target
    scale_prev = 1.0
    net, gross, turn = [], [], []
    idx = []

    for t in days:
        rt = R.loc[t]
        g = sum(wv * (rt.get(k, 0.0) if pd.notna(rt.get(k, np.nan)) else 0.0)
                for k, wv in w_active.items())
        g += max(0.0, 1.0 - sum(w_active.values())) * (
            cash_ret.get(t, 0.0) if pd.notna(cash_ret.get(t, np.nan)) else 0.0)
        ret_hist.append(g)

        # 1) Refresh the slow selection book on selection days (optionally liquidity-screened).
        if t in sel_days:
            uni = sel_universe
            if cfg.get("liq_floor"):
                adv = ov.dollar_adv.loc[t]
                uni = [u for u in sel_universe if adv.get(u, 0.0) >= cfg["liq_floor"]]
            w_base = candidate_weights(t, uni, moms, above_ma, vol_sig, daily, sel_cfg)

        # 2) Apply daily overlays to the current book -> target weights.
        w_target = dict(w_base)
        if cfg.get("fast_gate") and w_target:
            ex, en = cfg["fast_gate"]            # exit-EMA span (fast), reenter-EMA span (slow)
            for k in list(w_target):
                px_t = ov.px.at[t, k]
                if pd.isna(px_t):
                    continue
                # Stateful hysteresis: exit below the fast EMA, only re-enter above the slow EMA.
                state = gate_on.get(k, True)
                if state and px_t < ov.ema[ex].at[t, k]:
                    state = False
                elif not state and px_t > ov.ema[en].at[t, k]:
                    state = True
                gate_on[k] = state
                if not state:
                    w_target[k] = 0.0
        if cfg.get("obv") and w_target:
            for k in list(w_target):
                if not bool(ov.obv_up.get(k, pd.Series()).get(t, True)):
                    w_target[k] = 0.0
        if cfg.get("vol_scale") and w_target:      # per-name inverse-vol scaling (high turnover)
            tv = cfg["vol_scale"]
            for k in list(w_target):
                v = ov.vol20.at[t, k] if k in ov.vol20.columns else np.nan
                if pd.notna(v) and v > 0:
                    w_target[k] *= min(1.0, tv / v)
        if cfg.get("regime"):
            sema = ov.spy_ema(cfg.get("regime_win", 50)).at[t]
            risk_on = ov.spy.at[t] >= sema if pd.notna(sema) else True
            if not risk_on:
                w_target = {k: v * cfg["regime"] for k, v in w_target.items()}

        # Portfolio-level vol target: ONE gross-exposure lever (low turnover, vol-timing edge).
        if cfg.get("port_vt") and len(ret_hist) > cfg.get("port_vt_win", 30):
            win = cfg.get("port_vt_win", 30)
            pv = float(np.std(ret_hist[-win:], ddof=1)) * np.sqrt(PERIODS)
            raw = min(cfg.get("port_vt_cap", 1.0), cfg["port_vt"] / pv) if pv > 0 else 1.0
            sm = cfg.get("port_vt_smooth", 0.5)            # EMA smoothing of the scale
            scale = sm * scale_prev + (1 - sm) * raw
            scale_prev = scale
            w_target = {k: v * scale for k, v in w_target.items()}

        w_target = {k: v for k, v in w_target.items() if v > 1e-6}

        # 3) Turnover throttle: skip sub-band adjustments.
        if band > 0 and w_active:
            merged = dict(w_active)
            for k in set(w_target) | set(w_active):
                nw = w_target.get(k, 0.0)
                if abs(nw - w_active.get(k, 0.0)) >= band:
                    if nw > 1e-6:
                        merged[k] = nw
                    else:
                        merged.pop(k, None)
            w_target = merged

        to = sum(abs(w_target.get(k, 0.0) - w_active.get(k, 0.0))
                 for k in set(w_target) | set(w_active))
        net.append(g - to * cost_bps / 1e4)
        gross.append(g)
        turn.append(to)
        idx.append(t)
        w_active = w_target

    return (pd.Series(net, index=idx), pd.Series(gross, index=idx),
            pd.Series(turn, index=idx))


BASE = {"lookbacks": [252], "top_k": 20, "weighting": "invvol", "ma_filter": True,
        "ma_window": 200, "max_weight": 0.30, "selection_freq": "monthly", "band": 0.02}


def variants():
    v = {}
    v["monthly base (bogey)"] = {**BASE, "band": 0.0}
    # Low-turnover portfolio-level overlays (one gross lever).
    v["port vol-target 15%"] = {**BASE, "port_vt": 0.15, "port_vt_win": 30, "port_vt_smooth": 0.5}
    v["port vol-target 18%"] = {**BASE, "port_vt": 0.18, "port_vt_win": 30, "port_vt_smooth": 0.5}
    v["port VT18 smooth0.8"] = {**BASE, "port_vt": 0.18, "port_vt_win": 40, "port_vt_smooth": 0.8}
    v["regime SPY<EMA100->0"] = {**BASE, "regime": 0.0, "regime_win": 100}
    v["regime SPY<EMA200->0"] = {**BASE, "regime": 0.0, "regime_win": 200}
    v["regime EMA100->50%"] = {**BASE, "regime": 0.5, "regime_win": 100}
    v["regime EMA50->50%"] = {**BASE, "regime": 0.5, "regime_win": 50}
    v["regime EMA100->33%"] = {**BASE, "regime": 0.33, "regime_win": 100}
    # Best-of combos: portfolio VT + regime (both low-turnover, complementary).
    v["VT18 + regime EMA100/50%"] = {**BASE, "port_vt": 0.18, "port_vt_win": 30,
                                     "port_vt_smooth": 0.5, "regime": 0.5, "regime_win": 100}
    v["VT15 + regime EMA100/50%"] = {**BASE, "port_vt": 0.15, "port_vt_win": 30,
                                     "port_vt_smooth": 0.5, "regime": 0.5, "regime_win": 100}
    return v


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2017-06-30")
    ap.add_argument("--oos", default="2019-07-01")
    args = ap.parse_args()

    prices = lib.load_prices()
    cash = next(t for t in CASH_TICKERS if t in prices.columns)
    sel = [c for c in prices.columns if c not in CASH_TICKERS]
    rawclose, volume = lib.load_volume(prices.index, list(prices.columns))
    ov = Overlays(prices, rawclose, volume)
    rf_daily = ov.R[cash]
    oos_ts = pd.Timestamp(args.oos)
    spy_series = ov.R["SPY"][ov.R.index >= oos_ts].dropna()
    spy = stats(spy_series, rf_daily)
    print(f"OOS {args.oos}..{prices.index[-1].date()}  (daily basis, rf=cash)\n")
    hdr = f"{'variant':<30}{'cost':>5}{'CAGR':>8}{'Vol':>7}{'Sharpe':>8}{'Sortino':>9}{'MaxDD':>8}{'turn/yr':>9}"
    print(hdr)
    print(f"{'SPY buy & hold':<30}{'-':>5}{spy['cagr']*100:>7.1f}%{spy['vol']*100:>6.1f}%"
          f"{spy['sharpe']:>8.2f}{spy['sortino']:>9.2f}{spy['maxdd']*100:>7.1f}%{'-':>9}")
    print("-" * len(hdr))

    results = {}
    for name, cfg in variants().items():
        for cb in (0, 5, 10):
            net, gross, turn = run(prices, ov, cash, sel, cfg, args.start, cb)
            net_o = net[net.index >= oos_ts]
            st = stats(net_o, rf_daily)
            typ = turn[turn.index >= oos_ts].sum() / (len(net_o) / PERIODS)
            results[(name, cb)] = (st, typ)
            tag = name if cb == 0 else ""
            print(f"{tag:<30}{cb:>4}b{st['cagr']*100:>7.1f}%{st['vol']*100:>6.1f}%"
                  f"{st['sharpe']:>8.2f}{st['sortino']:>9.2f}{st['maxdd']*100:>7.1f}%{typ:>8.1f}x")
        print()


if __name__ == "__main__":
    main()
