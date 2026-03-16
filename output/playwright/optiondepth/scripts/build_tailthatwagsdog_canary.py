#!/usr/bin/env python3

import json
import math
import pathlib
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import date, datetime
from statistics import mean


ROOT = pathlib.Path("/Users/pawanagarwal/github/phenixflow/output/playwright/optiondepth")
OUT_ROOT = ROOT / "tailthatwagsdog_prototype" / "spx_2026-03-13"
RAW_ROOT = OUT_ROOT / "raw" / "thetadata"
SCRIPT_OUT = OUT_ROOT
BASE_URL = "http://127.0.0.1:25503"
TRADE_DATE = "20260313"
TRADE_DATE_DASHED = "2026-03-13"
SYMBOL = "SPX"
RISK_FREE_RATE = 0.042
CONTRACT_MULTIPLIER = 100.0
MIN_TAU = 1e-8
APP_BENCHMARK = {
    "spot": 6632.19,
    "regime": "AMPLIFYING",
    "zero_gamma": 6916.0,
    "zero_gamma_pct": 4.3,
    "one_day_prob_above_6600": 94.9,
    "one_day_prob_below_6650": 74.5,
    "one_week_prob_above_6600": 73.2,
    "one_week_prob_below_6650": 58.4,
}
EXPIRY_BASKETS = {
    "mar_apr_may": ["2026-03-20", "2026-04-17", "2026-05-15"],
    "apr_may_jun": ["2026-04-17", "2026-05-15", "2026-06-19"],
}
ALL_EXPIRIES = sorted({expiry for expiries in EXPIRY_BASKETS.values() for expiry in expiries})
SHOCK_SPOT_PCTS = [pct for pct in range(-10, 11, 1)]
SHOCK_IV_PTS = [pts for pts in range(-10, 41, 1)]


@dataclass
class Contract:
    expiration: str
    strike: float
    right: str
    oi: float
    bid: float
    ask: float
    close: float
    mid: float
    sigma: float
    gamma: float
    vanna: float
    d1: float
    d2: float
    underlying_price: float
    tau: float
    carry: float
    gamma_scale: float
    vanna_scale: float


def norm_pdf(value):
    return math.exp(-0.5 * value * value) / math.sqrt(2.0 * math.pi)


def norm_cdf(value):
    return 0.5 * (1.0 + math.erf(value / math.sqrt(2.0)))


def inverse_norm_cdf(probability):
    if probability <= 0.0:
        return float("-inf")
    if probability >= 1.0:
        return float("inf")

    a = [
        -3.969683028665376e01,
        2.209460984245205e02,
        -2.759285104469687e02,
        1.383577518672690e02,
        -3.066479806614716e01,
        2.506628277459239e00,
    ]
    b = [
        -5.447609879822406e01,
        1.615858368580409e02,
        -1.556989798598866e02,
        6.680131188771972e01,
        -1.328068155288572e01,
    ]
    c = [
        -7.784894002430293e-03,
        -3.223964580411365e-01,
        -2.400758277161838e00,
        -2.549732539343734e00,
        4.374664141464968e00,
        2.938163982698783e00,
    ]
    d = [
        7.784695709041462e-03,
        3.224671290700398e-01,
        2.445134137142996e00,
        3.754408661907416e00,
    ]
    plow = 0.02425
    phigh = 1.0 - plow
    if probability < plow:
        q = math.sqrt(-2.0 * math.log(probability))
        return (
            (((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5]) /
            ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1.0)
        )
    if probability > phigh:
        q = math.sqrt(-2.0 * math.log(1.0 - probability))
        return -(
            (((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5]) /
            ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1.0)
        )

    q = probability - 0.5
    r = q * q
    return (
        (((((a[0] * r + a[1]) * r + a[2]) * r + a[3]) * r + a[4]) * r + a[5]) * q /
        (((((b[0] * r + b[1]) * r + b[2]) * r + b[3]) * r + b[4]) * r + 1.0)
    )


def cornish_fisher_adjusted_z(probability, skewness, kurtosis):
    z = inverse_norm_cdf(probability)
    excess = kurtosis - 3.0
    return (
        z
        + (skewness / 6.0) * (z * z - 1.0)
        + (excess / 24.0) * (z * z * z - 3.0 * z)
        - (skewness * skewness / 36.0) * (2.0 * z * z * z - 5.0 * z)
    )


def edgeworth_cdf(level, mean_value, std_value, skewness, kurtosis):
    if std_value <= 0.0:
        return 1.0 if level >= mean_value else 0.0
    z = (level - mean_value) / std_value
    pdf = norm_pdf(z)
    cdf = norm_cdf(z)
    excess = kurtosis - 3.0
    adjustment = pdf * (
        (skewness / 6.0) * (z * z - 1.0)
        + (excess / 24.0) * (z * z * z - 3.0 * z)
        + (skewness * skewness / 72.0) * (z ** 5 - 10.0 * z ** 3 + 15.0 * z)
    )
    return min(1.0, max(0.0, cdf + adjustment))


def fetch_json(url, out_path, timeout=240):
    out_path.parent.mkdir(parents=True, exist_ok=True)
    started = time.perf_counter()
    request = urllib.request.Request(url, headers={"Accept": "application/json"})
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            payload = response.read()
    except urllib.error.HTTPError as error:
        body = error.read().decode("utf-8", errors="replace")
        if error.code == 472:
            print(f"[fetch-miss] {out_path.name} http=472 body={body[:180]}")
            return None
        raise
    out_path.write_bytes(payload)
    elapsed = time.perf_counter() - started
    print(f"[fetch] {out_path.name} bytes={len(payload)} elapsed_s={elapsed:.1f}")
    return json.loads(payload.decode("utf-8"))


def ensure_json(url, out_path):
    if out_path.exists():
        return json.loads(out_path.read_text())
    return fetch_json(url, out_path)


def to_rows(columnar):
    if not columnar:
        return []
    keys = [key for key, value in columnar.items() if isinstance(value, list)]
    if not keys:
        return []
    row_count = len(columnar[keys[0]])
    rows = []
    for idx in range(row_count):
        row = {}
        for key in keys:
            values = columnar[key]
            if idx < len(values):
                row[key] = values[idx]
        rows.append(row)
    return rows


def request_url(path, **params):
    query = urllib.parse.urlencode(params)
    return f"{BASE_URL}{path}?{query}"


def expiry_compact(expiry):
    return expiry.replace("-", "")


def ensure_raw_payloads():
    payloads = {"greeks": {}, "oi": None, "index": None}
    for expiry in ALL_EXPIRIES:
        out_path = RAW_ROOT / f"spx_greeks_eod_{expiry_compact(expiry)}.json"
        url = request_url(
            "/v3/option/history/greeks/eod",
            symbol=SYMBOL,
            expiration=expiry_compact(expiry),
            start_date=TRADE_DATE,
            end_date=TRADE_DATE,
            format="json",
        )
        payloads["greeks"][expiry] = ensure_json(url, out_path)

    oi_path = RAW_ROOT / "spx_open_interest_snapshot.json"
    oi_url = request_url(
        "/v3/option/snapshot/open_interest",
        symbol=SYMBOL,
        expiration="*",
        format="json",
    )
    payloads["oi"] = ensure_json(oi_url, oi_path)

    index_path = RAW_ROOT / "spx_index_ohlc_1m_20260313.json"
    index_url = request_url(
        "/v3/index/history/ohlc",
        symbol=SYMBOL,
        start_date=TRADE_DATE,
        end_date=TRADE_DATE,
        interval="1m",
        format="json",
    )
    payloads["index"] = ensure_json(index_url, index_path)
    return payloads


def build_oi_lookup(oi_payload):
    lookup = {}
    for row in to_rows(oi_payload):
        key = (row["expiration"], float(row["strike"]), row["right"])
        lookup[key] = float(row.get("open_interest", 0.0) or 0.0)
    return lookup


def bs_gamma(spot, strike, sigma, tau, carry):
    tau = max(tau, MIN_TAU)
    sigma = max(sigma, 1e-8)
    denom = sigma * math.sqrt(tau)
    d1 = (math.log(spot / strike) + (carry + 0.5 * sigma * sigma) * tau) / denom
    return norm_pdf(d1) / (spot * denom)


def bs_vanna(spot, strike, sigma, tau, carry):
    tau = max(tau, MIN_TAU)
    sigma = max(sigma, 1e-8)
    denom = sigma * math.sqrt(tau)
    d1 = (math.log(spot / strike) + (carry + 0.5 * sigma * sigma) * tau) / denom
    d2 = d1 - denom
    return -norm_pdf(d1) * d2 / sigma


def infer_contract(row, oi_lookup):
    expiration = row["expiration"]
    strike = float(row["strike"])
    right = row["right"]
    oi = oi_lookup.get((expiration, strike, right), 0.0)
    if oi <= 0.0:
        return None

    sigma = float(row.get("implied_vol", 0.0) or 0.0)
    gamma = float(row.get("gamma", 0.0) or 0.0)
    vanna = float(row.get("vanna", 0.0) or 0.0)
    d1 = float(row.get("d1", 0.0) or 0.0)
    d2 = float(row.get("d2", 0.0) or 0.0)
    spot = float(row.get("underlying_price", 0.0) or 0.0)
    if sigma <= 0.0 or gamma <= 0.0 or spot <= 0.0:
        return None

    diff = abs(d1 - d2)
    if diff <= 0.0:
        return None
    sqrt_tau = diff / sigma
    tau = max(MIN_TAU, sqrt_tau * sqrt_tau)
    carry = ((d1 * sigma * math.sqrt(tau)) - math.log(spot / strike)) / tau - 0.5 * sigma * sigma

    bid = float(row.get("bid", 0.0) or 0.0)
    ask = float(row.get("ask", 0.0) or 0.0)
    close = float(row.get("close", 0.0) or 0.0)
    if bid > 0.0 and ask > 0.0 and ask >= bid:
        mid = 0.5 * (bid + ask)
    elif close > 0.0:
        mid = close
    else:
        mid = 0.0

    gamma_theory = bs_gamma(spot, strike, sigma, tau, carry)
    vanna_theory = bs_vanna(spot, strike, sigma, tau, carry)
    gamma_scale = gamma / gamma_theory if gamma_theory > 0.0 else 1.0
    if abs(vanna_theory) > 1e-10:
        vanna_scale = vanna / vanna_theory
    else:
        vanna_scale = 1.0

    return Contract(
        expiration=expiration,
        strike=strike,
        right=right,
        oi=oi,
        bid=bid,
        ask=ask,
        close=close,
        mid=mid,
        sigma=sigma,
        gamma=gamma,
        vanna=vanna,
        d1=d1,
        d2=d2,
        underlying_price=spot,
        tau=tau,
        carry=carry,
        gamma_scale=gamma_scale,
        vanna_scale=vanna_scale,
    )


def build_contracts(payloads):
    oi_lookup = build_oi_lookup(payloads["oi"])
    contracts = []
    for expiry, payload in payloads["greeks"].items():
        if payload is None:
            print(f"[load-miss] greeks expiry={expiry} unavailable")
            continue
        rows = to_rows(payload)
        print(f"[load] greeks expiry={expiry} rows={len(rows)}")
        for row in rows:
            contract = infer_contract(row, oi_lookup)
            if contract:
                contracts.append(contract)
    print(f"[load] usable_contracts={len(contracts)}")
    return contracts


def index_close(index_payload):
    closes = index_payload.get("close", [])
    if not closes:
        raise RuntimeError("missing index close rows")
    return float(closes[-1])


def aggregate_gex_per_point(contracts, spot):
    total = 0.0
    for contract in contracts:
        sign = 1.0 if contract.right == "CALL" else -1.0
        total += sign * contract.oi * CONTRACT_MULTIPLIER * spot * contract.gamma
    return total


def aggregate_gex_per_1pct(contracts, spot):
    total = 0.0
    for contract in contracts:
        sign = 1.0 if contract.right == "CALL" else -1.0
        total += sign * contract.oi * CONTRACT_MULTIPLIER * spot * spot * contract.gamma * 0.01
    return total


def aggregate_vex_per_volpt(contracts, spot):
    total = 0.0
    for contract in contracts:
        sign = 1.0 if contract.right == "CALL" else -1.0
        total += sign * contract.oi * CONTRACT_MULTIPLIER * spot * contract.vanna * 0.01
    return total


def contract_gamma_at(contract, spot, sigma_bump=0.0):
    sigma = max(1e-8, contract.sigma + sigma_bump)
    return contract.gamma_scale * bs_gamma(spot, contract.strike, sigma, contract.tau, contract.carry)


def contract_vanna_at(contract, spot, sigma_bump=0.0):
    sigma = max(1e-8, contract.sigma + sigma_bump)
    return contract.vanna_scale * bs_vanna(spot, contract.strike, sigma, contract.tau, contract.carry)


def aggregate_gex_surface_value(contracts, spot):
    total = 0.0
    for contract in contracts:
        sign = 1.0 if contract.right == "CALL" else -1.0
        total += sign * contract.oi * CONTRACT_MULTIPLIER * spot * contract_gamma_at(contract, spot)
    return total


def find_zero_gamma_level(contracts, spot0):
    lower = spot0 * 0.90
    upper = spot0 * 1.10
    step = 5.0
    spots = []
    values = []
    current = lower
    while current <= upper + 0.1:
        spots.append(round(current, 4))
        values.append(aggregate_gex_surface_value(contracts, current))
        current += step

    roots = []
    for idx in range(1, len(spots)):
        left_v = values[idx - 1]
        right_v = values[idx]
        if left_v == 0.0:
            roots.append(spots[idx - 1])
            continue
        if left_v * right_v > 0.0:
            continue
        left_s = spots[idx - 1]
        right_s = spots[idx]
        denom = right_v - left_v
        if abs(denom) <= 1e-12:
            roots.append(left_s)
        else:
            root = left_s - left_v * (right_s - left_s) / denom
            roots.append(root)

    if not roots:
        return None, {"spots": spots, "values": values, "roots": roots}

    preferred = min(roots, key=lambda value: (value < spot0, abs(value - spot0)))
    return preferred, {"spots": spots, "values": values, "roots": roots}


def build_stress_surface(contracts, spot0):
    z_values = []
    current_value = None
    scenarios = {}
    for spot_move in SHOCK_SPOT_PCTS:
        row = []
        pct = spot_move / 100.0
        spot = spot0 * (1.0 + pct)
        delta_spot = spot - spot0
        for iv_pts in SHOCK_IV_PTS:
            sigma_bump = iv_pts / 100.0
            total = 0.0
            for contract in contracts:
                sign = 1.0 if contract.right == "CALL" else -1.0
                gamma_term = spot * contract_gamma_at(contract, spot, sigma_bump) * delta_spot
                vanna_term = spot * contract_vanna_at(contract, spot, sigma_bump) * (iv_pts * 0.01)
                total += sign * contract.oi * CONTRACT_MULTIPLIER * (gamma_term + vanna_term)
            total_m = -(total / 1e6)
            row.append(total_m)
            if spot_move == 0 and iv_pts == 0:
                current_value = total_m
        z_values.append(row)

    key_points = [(-1, 0), (-1, 2), (-2, 2), (-5, 5), (1, -2), (2, -2)]
    for spot_move, iv_pts in key_points:
        try:
            row_idx = SHOCK_SPOT_PCTS.index(spot_move)
            col_idx = SHOCK_IV_PTS.index(iv_pts)
            scenarios[f"spot_{spot_move:+d}_iv_{iv_pts:+d}"] = z_values[row_idx][col_idx]
        except ValueError:
            continue

    return {
        "x": SHOCK_IV_PTS,
        "y": SHOCK_SPOT_PCTS,
        "z": z_values,
        "current": current_value,
        "scenarios_m": scenarios,
    }


def build_otm_price_curve(april_contracts, spot0):
    calls = {}
    puts = {}
    tau = None
    for contract in april_contracts:
        if tau is None:
            tau = contract.tau
        if contract.mid <= 0.0:
            continue
        if contract.right == "CALL":
            calls[contract.strike] = contract.mid
        else:
            puts[contract.strike] = contract.mid

    strikes = sorted(set(calls.keys()) | set(puts.keys()))
    curve = []
    for strike in strikes:
        call_mid = calls.get(strike)
        put_mid = puts.get(strike)
        if strike < spot0 and put_mid:
            curve.append((strike, put_mid))
        elif strike > spot0 and call_mid:
            curve.append((strike, call_mid))
        elif strike == spot0:
            mids = [value for value in [call_mid, put_mid] if value]
            if mids:
                curve.append((strike, sum(mids) / len(mids)))
    return curve, tau


def build_bl_density(april_contracts, spot0):
    curve, tau = build_otm_price_curve(april_contracts, spot0)
    if len(curve) < 5 or tau is None:
        return None

    strikes = [item[0] for item in curve]
    mids = [item[1] for item in curve]
    density = []
    density_strikes = []
    for idx in range(1, len(strikes) - 1):
        left_k = strikes[idx - 1]
        center_k = strikes[idx]
        right_k = strikes[idx + 1]
        h0 = center_k - left_k
        h1 = right_k - center_k
        if h0 <= 0.0 or h1 <= 0.0:
            continue
        second = 2.0 * (
            mids[idx - 1] / (h0 * (h0 + h1))
            - mids[idx] / (h0 * h1)
            + mids[idx + 1] / (h1 * (h0 + h1))
        )
        q = math.exp(RISK_FREE_RATE * tau) * second
        density.append(max(0.0, q))
        density_strikes.append(center_k)

    if len(density_strikes) < 3:
        return None

    weights = []
    for idx, strike in enumerate(density_strikes):
        if idx == 0:
            width = density_strikes[idx + 1] - strike
        elif idx == len(density_strikes) - 1:
            width = strike - density_strikes[idx - 1]
        else:
            width = 0.5 * (density_strikes[idx + 1] - density_strikes[idx - 1])
        weights.append(width)

    mass = sum(value * width for value, width in zip(density, weights))
    if mass <= 0.0:
        return None

    normalized = [value / mass for value in density]
    mean_k = sum(strike * value * width for strike, value, width in zip(density_strikes, normalized, weights))
    variance = sum(((strike - mean_k) ** 2) * value * width for strike, value, width in zip(density_strikes, normalized, weights))
    std = math.sqrt(max(variance, 1e-12))
    skewness = sum((((strike - mean_k) / std) ** 3) * value * width for strike, value, width in zip(density_strikes, normalized, weights))
    kurtosis = sum((((strike - mean_k) / std) ** 4) * value * width for strike, value, width in zip(density_strikes, normalized, weights))

    return {
        "tau_years": tau,
        "dte_days": max(1.0, (datetime.fromisoformat("2026-04-17T16:00:00").date() - date.fromisoformat(TRADE_DATE_DASHED)).days),
        "strikes": density_strikes,
        "density": normalized,
        "mean": mean_k,
        "std": std,
        "skewness": skewness,
        "kurtosis": kurtosis,
    }


def short_horizon_forecast(density_summary, spot0, days):
    dte_days = max(1.0, density_summary["dte_days"])
    scale = math.sqrt(days / dte_days)
    mean_h = spot0 + (density_summary["mean"] - spot0) * (days / dte_days)
    std_h = density_summary["std"] * scale
    skewness = density_summary["skewness"]
    kurtosis = density_summary["kurtosis"]
    levels = [6600.0, 6650.0]
    level_probs = {}
    for level in levels:
        p_below = 100.0 * edgeworth_cdf(level, mean_h, std_h, skewness, kurtosis)
        level_probs[str(int(level))] = {
            "below": p_below,
            "above": 100.0 - p_below,
        }

    percentiles = {}
    for probability in [0.05, 0.25, 0.50, 0.75, 0.95]:
        z = cornish_fisher_adjusted_z(probability, skewness, kurtosis)
        percentiles[f"{int(probability * 100)}"] = mean_h + std_h * z

    return {
        "days": days,
        "mean": mean_h,
        "std": std_h,
        "level_probs": level_probs,
        "percentiles": percentiles,
    }


def classify_intraday_signal(spot0, zero_gamma, stress_surface, gex_per_1pct):
    below_zero = zero_gamma is not None and spot0 < zero_gamma
    down_iv = stress_surface["scenarios_m"].get("spot_-2_iv_+2")
    up_iv = stress_surface["scenarios_m"].get("spot_+2_iv_-2")
    if gex_per_1pct < 0.0:
        regime = "AMPLIFYING"
    elif gex_per_1pct > 0.0:
        regime = "DAMPENING"
    else:
        regime = "NEUTRAL"

    if down_iv is not None and down_iv < 0.0:
        downside = "selloff_with_iv_expansion_should_self_reinforce"
    else:
        downside = "selloff_with_iv_expansion_not_clearly_self_reinforcing"

    if up_iv is not None and up_iv > 0.0:
        upside = "rally_with_iv_compression_should_be_more_two_sided_than_breakout_like"
    else:
        upside = "rally_with_iv_compression_not_clearly_supported"

    if below_zero:
        opening_state = "below_zero_gamma"
    elif zero_gamma is not None:
        opening_state = "above_zero_gamma"
    else:
        opening_state = "zero_gamma_unresolved"

    return {
        "regime": regime,
        "opening_state": opening_state,
        "downside_if_then": downside,
        "upside_if_then": upside,
    }


def basket_metrics(name, basket_expiries, contracts, spot0, density_summary):
    basket_contracts = [contract for contract in contracts if contract.expiration in basket_expiries]
    if not basket_contracts:
        return None
    expiries_used = sorted({contract.expiration for contract in basket_contracts})
    zero_gamma, surface_curve = find_zero_gamma_level(basket_contracts, spot0)
    stress_surface = build_stress_surface(basket_contracts, spot0)
    gex_per_1pct = aggregate_gex_per_1pct(basket_contracts, spot0) / 1e6
    gex_per_point = aggregate_gex_per_point(basket_contracts, spot0) / 1e6
    vex_per_volpt = aggregate_vex_per_volpt(basket_contracts, spot0) / 1e6
    vgr = abs(vex_per_volpt) / max(abs(gex_per_1pct), 1e-9)
    forecasts = {
        "1d": short_horizon_forecast(density_summary, spot0, 1),
        "5d": short_horizon_forecast(density_summary, spot0, 5),
    }
    signal = classify_intraday_signal(spot0, zero_gamma, stress_surface, gex_per_1pct)
    benchmark_diff = {
        "zero_gamma_abs_diff": None if zero_gamma is None else abs(zero_gamma - APP_BENCHMARK["zero_gamma"]),
        "regime_match": signal["regime"] == APP_BENCHMARK["regime"],
        "1d_prob_above_6600_abs_diff": abs(forecasts["1d"]["level_probs"]["6600"]["above"] - APP_BENCHMARK["one_day_prob_above_6600"]),
        "1d_prob_below_6650_abs_diff": abs(forecasts["1d"]["level_probs"]["6650"]["below"] - APP_BENCHMARK["one_day_prob_below_6650"]),
        "5d_prob_above_6600_abs_diff": abs(forecasts["5d"]["level_probs"]["6600"]["above"] - APP_BENCHMARK["one_week_prob_above_6600"]),
        "5d_prob_below_6650_abs_diff": abs(forecasts["5d"]["level_probs"]["6650"]["below"] - APP_BENCHMARK["one_week_prob_below_6650"]),
    }
    return {
        "name": name,
        "expiries": basket_expiries,
        "expiries_used": expiries_used,
        "contract_count": len(basket_contracts),
        "spot": spot0,
        "gex_per_1pct_m": gex_per_1pct,
        "gex_per_point_m": gex_per_point,
        "vex_per_volpt_m": vex_per_volpt,
        "vgr": vgr,
        "zero_gamma": zero_gamma,
        "zero_gamma_pct_above_spot": None if zero_gamma is None else ((zero_gamma / spot0) - 1.0) * 100.0,
        "stress_surface": stress_surface,
        "signal": signal,
        "surface_curve": surface_curve,
        "forecasts": forecasts,
        "benchmark_diff": benchmark_diff,
    }


def render_stress_surface_html(best_metrics):
    out_path = SCRIPT_OUT / "stress_surface_best.html"
    payload = json.dumps(best_metrics["stress_surface"])
    title = f"TailThatWagsDog Prototype Stress Surface ({best_metrics['name']})"
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>{title}</title>
  <script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
  <style>
    body {{
      margin: 0;
      font-family: Menlo, Monaco, monospace;
      background: #090b13;
      color: #f2f4ff;
    }}
    .wrap {{
      max-width: 1200px;
      margin: 0 auto;
      padding: 24px;
    }}
    #chart {{
      height: 760px;
    }}
    .meta {{
      display: flex;
      gap: 24px;
      flex-wrap: wrap;
      margin-bottom: 16px;
    }}
  </style>
</head>
<body>
  <div class="wrap">
    <h1>{title}</h1>
    <div class="meta">
      <div>Spot: {best_metrics['spot']:.2f}</div>
      <div>Basket: {", ".join(best_metrics['expiries'])}</div>
      <div>Regime: {best_metrics['signal']['regime']}</div>
      <div>Zero-Gamma: {"n/a" if best_metrics['zero_gamma'] is None else f"{best_metrics['zero_gamma']:.2f}"}</div>
    </div>
    <div id="chart"></div>
  </div>
  <script>
    const payload = {payload};
    Plotly.newPlot("chart", [{{
      x: payload.x,
      y: payload.y,
      z: payload.z,
      type: "heatmap",
      colorscale: [
        [0.0, "#7f0000"],
        [0.4, "#ff3030"],
        [0.5, "#111111"],
        [0.6, "#2f45ff"],
        [1.0, "#0017b7"]
      ],
      zmid: 0,
      colorbar: {{ title: "$M hedge flow" }}
    }}], {{
      paper_bgcolor: "#090b13",
      plot_bgcolor: "#090b13",
      font: {{ color: "#f2f4ff", family: "Menlo, Monaco, monospace" }},
      xaxis: {{ title: "IV shock (vol pts)" }},
      yaxis: {{ title: "Spot move (%)" }},
      shapes: [
        {{ type: "line", x0: Math.min(...payload.x), x1: Math.max(...payload.x), y0: 0, y1: 0, line: {{ color: "#d7dbff", width: 1 }} }},
        {{ type: "line", x0: 0, x1: 0, y0: Math.min(...payload.y), y1: Math.max(...payload.y), line: {{ color: "#d7dbff", width: 1 }} }}
      ]
    }}, {{ responsive: true }});
  </script>
</body>
</html>
"""
    out_path.write_text(html)


def render_report(spot0, density_summary, metrics_by_basket, best_name):
    out_path = SCRIPT_OUT / "TAILTHATWAGSDOG_SPX_2026-03-13_REPORT.md"
    best = metrics_by_basket[best_name]
    lines = [
        "# TailThatWagsDog Prototype Canary",
        "",
        f"Trade date: `{TRADE_DATE_DASHED}`",
        f"Spot close: `{spot0:.2f}`",
        "",
        "## Benchmark",
        "",
        f"- Public app spot: `{APP_BENCHMARK['spot']:.2f}`",
        f"- Public app regime: `{APP_BENCHMARK['regime']}`",
        f"- Public app zero-gamma: `{APP_BENCHMARK['zero_gamma']:.2f}`",
        f"- Public app 1-day `P(above 6600)`: `{APP_BENCHMARK['one_day_prob_above_6600']:.1f}%`",
        f"- Public app 1-day `P(below 6650)`: `{APP_BENCHMARK['one_day_prob_below_6650']:.1f}%`",
        f"- Public app 1-week `P(above 6600)`: `{APP_BENCHMARK['one_week_prob_above_6600']:.1f}%`",
        f"- Public app 1-week `P(below 6650)`: `{APP_BENCHMARK['one_week_prob_below_6650']:.1f}%`",
        "",
        "## Basket Comparison",
        "",
    ]

    for name, metrics in metrics_by_basket.items():
        if metrics is None:
            lines.extend([
                f"### `{name}`",
                "",
                "- No usable contracts were available for this basket from the current ThetaData pull.",
                "",
            ])
            continue
        lines.extend([
            f"### `{name}`",
            "",
            f"- Expiries: `{', '.join(metrics['expiries'])}`",
            f"- Expiries used: `{', '.join(metrics['expiries_used'])}`",
            f"- Contracts used: `{metrics['contract_count']}`",
            f"- Public `GEX` per `1%` move: `{metrics['gex_per_1pct_m']:.2f}M`",
            f"- Public `GEX` per point: `{metrics['gex_per_point_m']:.2f}M`",
            f"- Public `VEX` hedge per vol point: `{metrics['vex_per_volpt_m']:.2f}M`",
            f"- `VGR`: `{metrics['vgr']:.2f}x`",
            f"- Regime: `{metrics['signal']['regime']}`",
            f"- Zero-gamma: `{('n/a' if metrics['zero_gamma'] is None else f'{metrics['zero_gamma']:.2f}')}`",
            f"- Zero-gamma pct above spot: `{('n/a' if metrics['zero_gamma_pct_above_spot'] is None else f'{metrics['zero_gamma_pct_above_spot']:.2f}%')}`",
            f"- Zero-gamma abs diff vs app: `{('n/a' if metrics['benchmark_diff']['zero_gamma_abs_diff'] is None else f'{metrics['benchmark_diff']['zero_gamma_abs_diff']:.2f}')}`",
            f"- Regime matches app: `{metrics['benchmark_diff']['regime_match']}`",
            f"- 1-day `P(above 6600)`: `{metrics['forecasts']['1d']['level_probs']['6600']['above']:.1f}%`",
            f"- 1-day `P(below 6650)`: `{metrics['forecasts']['1d']['level_probs']['6650']['below']:.1f}%`",
            f"- 1-week `P(above 6600)`: `{metrics['forecasts']['5d']['level_probs']['6600']['above']:.1f}%`",
            f"- 1-week `P(below 6650)`: `{metrics['forecasts']['5d']['level_probs']['6650']['below']:.1f}%`",
            "",
        ])

    lines.extend([
        "## Best Public Match",
        "",
        f"- Best basket by zero-gamma + regime fit: `{best_name}`",
        f"- Best basket regime: `{best['signal']['regime']}`",
        f"- Best basket zero-gamma: `{('n/a' if best['zero_gamma'] is None else f'{best['zero_gamma']:.2f}')}`",
        f"- Best basket stress HTML: `stress_surface_best.html`",
        "",
        "## Intraday If/Then Read",
        "",
        f"- Opening state: `{best['signal']['opening_state']}`",
        f"- If spot drops and IV rises, next effect: `{best['signal']['downside_if_then']}`",
        f"- If spot rallies and IV compresses, next effect: `{best['signal']['upside_if_then']}`",
        f"- `spot -2% / IV +2` stress: `{best['stress_surface']['scenarios_m'].get('spot_-2_iv_+2', float('nan')):.2f}M`",
        f"- `spot -5% / IV +5` stress: `{best['stress_surface']['scenarios_m'].get('spot_-5_iv_+5', float('nan')):.2f}M`",
        f"- `spot +2% / IV -2` stress: `{best['stress_surface']['scenarios_m'].get('spot_+2_iv_-2', float('nan')):.2f}M`",
        "",
        "Interpretation:",
        f"- With spot at `{spot0:.2f}` and zero-gamma at `{('n/a' if best['zero_gamma'] is None else f'{best['zero_gamma']:.2f}')}`, the public model says the market is `{best['signal']['regime'].lower()}` and `{best['signal']['opening_state'].replace('_', ' ')}`.",
        "- If the session shows price weakening together with IV expansion, treat that as the highest-conviction continuation pattern in this framework.",
        "- If price rallies but IV does not compress, the rally is less trustworthy than the raw directional move alone suggests.",
        "- If price reclaims the zero-gamma region and the stress surface turns less negative, the next effect should be volatility compression and more two-sided trade.",
        "",
        "## Forecast Baseline",
        "",
        f"- April BL mean: `{density_summary['mean']:.2f}`",
        f"- April BL std: `{density_summary['std']:.2f}`",
        f"- April BL skewness: `{density_summary['skewness']:.4f}`",
        f"- April BL kurtosis: `{density_summary['kurtosis']:.2f}`",
        "",
        "## Caveats",
        "",
        "- This is a public-data prototype, not a reconstruction of the account's proprietary directional-index logic.",
        "- We are using current snapshot OI on `2026-03-15`, which should still reflect the `2026-03-13` close because the market has not reopened yet.",
        "- We are not using limit-order-book or venue-specific participant/open-close data yet.",
        "- The probability layer is a rough BL + Edgeworth/Cornish-Fisher overlay, not a fully calibrated realized-vol model.",
    ])

    out_path.write_text("\n".join(lines) + "\n")


def choose_best_basket(metrics_by_basket):
    valid_names = [name for name, metrics in metrics_by_basket.items() if metrics is not None]
    if not valid_names:
        raise RuntimeError("no valid baskets available")

    def score(metrics):
        regime_penalty = 0.0 if metrics["benchmark_diff"]["regime_match"] else 1000.0
        zero_penalty = metrics["benchmark_diff"]["zero_gamma_abs_diff"] or 10000.0
        prob_penalty = (
            metrics["benchmark_diff"]["1d_prob_above_6600_abs_diff"]
            + metrics["benchmark_diff"]["1d_prob_below_6650_abs_diff"]
            + metrics["benchmark_diff"]["5d_prob_above_6600_abs_diff"]
            + metrics["benchmark_diff"]["5d_prob_below_6650_abs_diff"]
        )
        return regime_penalty + zero_penalty + prob_penalty

    return min(valid_names, key=lambda name: score(metrics_by_basket[name]))


def main():
    RAW_ROOT.mkdir(parents=True, exist_ok=True)
    payloads = ensure_raw_payloads()
    contracts = build_contracts(payloads)
    spot0 = index_close(payloads["index"])
    april_contracts = [contract for contract in contracts if contract.expiration == "2026-04-17"]
    density_summary = build_bl_density(april_contracts, spot0)
    if density_summary is None:
        raise RuntimeError("unable to build April BL density")

    metrics_by_basket = {}
    for name, expiries in EXPIRY_BASKETS.items():
        metrics_by_basket[name] = basket_metrics(name, expiries, contracts, spot0, density_summary)

    best_name = choose_best_basket(metrics_by_basket)
    render_stress_surface_html(metrics_by_basket[best_name])
    render_report(spot0, density_summary, metrics_by_basket, best_name)

    analysis = {
        "spot_close": spot0,
        "benchmark": APP_BENCHMARK,
        "density_summary": density_summary,
        "baskets": metrics_by_basket,
        "best_basket": best_name,
    }
    (SCRIPT_OUT / "analysis.json").write_text(json.dumps(analysis, indent=2))
    print(f"[done] best_basket={best_name}")
    print(f"[done] report={SCRIPT_OUT / 'TAILTHATWAGSDOG_SPX_2026-03-13_REPORT.md'}")


if __name__ == "__main__":
    main()
