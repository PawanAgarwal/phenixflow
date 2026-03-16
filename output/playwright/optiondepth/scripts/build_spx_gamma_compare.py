#!/usr/bin/env python3

import json
import math
import os
import pathlib
import statistics
import time
from datetime import datetime


ROOT = pathlib.Path("/Users/pawanagarwal/github/phenixflow/output/playwright/optiondepth")
RAW_OD = ROOT / "raw" / "optiondepth" / "od_chart_props_2026-03-06.json"
RAW_THETA = ROOT / "raw" / "thetadata"
RUN_SUFFIX = os.environ.get("RUN_SUFFIX", "default")
OUT_DIR = ROOT / "reconstruction" / "spx_gamma_2026-03-06" / RUN_SUFFIX

INV_SQRT_2PI = 1.0 / math.sqrt(2.0 * math.pi)
CONTRACT_MULTIPLIER = 100.0
GAMMA_LABEL_MOVE = 2.5
BASE_TIME = datetime.fromisoformat(os.environ.get("BASE_TIME_OVERRIDE", "2026-03-05T17:15:00"))
SPX_EXPIRY_TIME = os.environ.get("SPX_EXPIRY_TIME", "16:00:00")
SPXW_EXPIRY_TIME = os.environ.get("SPXW_EXPIRY_TIME", "16:00:00")
MIN_TAU = 1.0 / (24.0 * 365.0)


def load_json(path):
    return json.loads(path.read_text())


def flatten(matrix):
    flat = []
    for row in matrix:
        flat.extend(row)
    return flat


def to_iso_dt(value):
    return datetime.fromisoformat(value.replace("Z", "+00:00").replace("+00:00", ""))


def parse_heatmap_grid(od_payload):
    heatmap = od_payload["data"][0]
    x_vals = [datetime.fromisoformat(item) for item in heatmap["x"]]
    y_vals = [float(item) for item in heatmap["y"]]
    z_vals = [[float(cell) for cell in row] for row in heatmap["z"]]
    return x_vals, y_vals, z_vals


def parse_ohlc_trace(od_payload):
    candles = od_payload["data"][4]
    rows = []
    for idx, ts in enumerate(candles["x"]):
        rows.append({
            "x": ts,
            "open": float(candles["open"][idx]),
            "high": float(candles["high"][idx]),
            "low": float(candles["low"][idx]),
            "close": float(candles["close"][idx]),
        })
    return rows


def build_lookup(columnar, value_field):
    lookup = {}
    n = len(columnar["symbol"])
    for idx in range(n):
        key = (
            columnar["symbol"][idx],
            columnar["expiration"][idx],
            float(columnar["strike"][idx]),
            columnar["right"][idx],
        )
        lookup[key] = float(columnar[value_field][idx])
    return lookup


def right_sign(right):
    return 1.0 if right == "CALL" else -1.0


def strike_bin(abs_distance):
    if abs_distance <= 100.0:
        return "le_100"
    if abs_distance <= 150.0:
        return "101_150"
    if abs_distance <= 200.0:
        return "151_200"
    if abs_distance <= 300.0:
        return "201_300"
    if abs_distance <= 400.0:
        return "301_400"
    if abs_distance <= 500.0:
        return "401_500"
    return "gt_500"


def dte_bin(days_to_expiry):
    if days_to_expiry <= 7:
        return "le_7"
    if days_to_expiry <= 14:
        return "8_14"
    if days_to_expiry <= 30:
        return "15_30"
    if days_to_expiry <= 60:
        return "31_60"
    if days_to_expiry <= 180:
        return "61_180"
    return "gt_180"


def build_contracts(theta_root):
    spxw_greeks = load_json(theta_root / "spxw_greeks_eod_20260305.json")
    spx_greeks = load_json(theta_root / "spx_greeks_eod_20260305.json")
    spxw_oi = build_lookup(load_json(theta_root / "spxw_oi_20260306.json"), "open_interest")
    spx_oi = build_lookup(load_json(theta_root / "spx_oi_20260306.json"), "open_interest")
    chains = [
        (spxw_greeks, spxw_oi),
        (spx_greeks, spx_oi),
    ]

    contracts = []
    skipped = {
        "oi_missing_or_zero": 0,
        "sigma_invalid": 0,
        "gamma_invalid": 0,
        "tte_invalid": 0,
        "mu_invalid": 0,
    }

    for greeks, oi_lookup in chains:
        row_count = len(greeks["symbol"])
        for idx in range(row_count):
            symbol = greeks["symbol"][idx]
            expiration = greeks["expiration"][idx]
            strike = float(greeks["strike"][idx])
            right = greeks["right"][idx]
            key = (symbol, expiration, strike, right)
            oi = float(oi_lookup.get(key, 0.0))
            if oi <= 0.0:
                skipped["oi_missing_or_zero"] += 1
                continue

            sigma = float(greeks["implied_vol"][idx])
            gamma_obs = float(greeks["gamma"][idx])
            d1 = float(greeks["d1"][idx])
            d2 = float(greeks["d2"][idx])
            spot = float(greeks["underlying_price"][idx])
            if sigma <= 0.0 or not math.isfinite(sigma):
                skipped["sigma_invalid"] += 1
                continue
            if gamma_obs <= 0.0 or not math.isfinite(gamma_obs):
                skipped["gamma_invalid"] += 1
                continue

            diff = abs(d1 - d2)
            if diff <= 0.0 or not math.isfinite(diff):
                skipped["tte_invalid"] += 1
                continue

            sqrt_t0 = diff / sigma
            t0 = sqrt_t0 * sqrt_t0
            if t0 <= 0.0 or not math.isfinite(t0):
                skipped["tte_invalid"] += 1
                continue

            try:
                mu = ((d1 * sigma * sqrt_t0) - math.log(spot / strike)) / t0 - (0.5 * sigma * sigma)
            except ValueError:
                skipped["mu_invalid"] += 1
                continue
            if not math.isfinite(mu):
                skipped["mu_invalid"] += 1
                continue

            expiry_time = SPXW_EXPIRY_TIME if symbol == "SPXW" else SPX_EXPIRY_TIME
            expiry_dt = datetime.fromisoformat(f"{expiration}T{expiry_time}")
            days_to_expiry = (expiry_dt.date() - datetime.fromisoformat("2026-03-06T00:00:00").date()).days

            contracts.append({
                "symbol": symbol,
                "expiration": expiration,
                "strike": strike,
                "ln_strike": math.log(strike),
                "right": right,
                "oi": oi,
                "sigma": sigma,
                "mu": mu,
                "t0": t0,
                "expiry_dt": expiry_dt,
                "days_to_expiry": days_to_expiry,
                "strike_bin": strike_bin(abs(strike - spot)),
                "dte_bin": dte_bin(days_to_expiry),
                "public_sign": right_sign(right),
                "dealer_short_sign": -1.0,
            })

    return contracts, skipped


def init_surface(length):
    return [0.0] * length


def add_surface(dst, src):
    return [a + b for a, b in zip(dst, src)]


def surface_to_matrix(flat_surface, ny, nt):
    return [flat_surface[row * nt:(row + 1) * nt] for row in range(ny)]


def mean(values):
    return sum(values) / len(values)


def stdev(values):
    if len(values) < 2:
        return 0.0
    mu = mean(values)
    return math.sqrt(sum((value - mu) ** 2 for value in values) / len(values))


def correlation(a, b):
    ma = mean(a)
    mb = mean(b)
    num = 0.0
    da = 0.0
    db = 0.0
    for av, bv in zip(a, b):
        xa = av - ma
        xb = bv - mb
        num += xa * xb
        da += xa * xa
        db += xb * xb
    if da <= 0.0 or db <= 0.0:
        return 0.0
    return num / math.sqrt(da * db)


def fit_affine(pred, target):
    mp = mean(pred)
    mt = mean(target)
    num = 0.0
    den = 0.0
    for pv, tv in zip(pred, target):
        x = pv - mp
        num += x * (tv - mt)
        den += x * x
    slope = (num / den) if den else 0.0
    intercept = mt - slope * mp
    fitted = [slope * value + intercept for value in pred]
    rmse = math.sqrt(sum((fv - tv) ** 2 for fv, tv in zip(fitted, target)) / len(target))
    target_std = stdev(target) or 1.0
    return slope, intercept, rmse, rmse / target_std, fitted


def nearest_index(values, needle):
    best_idx = 0
    best_abs = None
    for idx, value in enumerate(values):
        delta = abs(value - needle)
        if best_abs is None or delta < best_abs:
            best_idx = idx
            best_abs = delta
    return best_idx


def extract_lines(y_vals, x_vals, surface, ohlc_rows):
    ny = len(y_vals)
    nt = len(x_vals)
    ohlc_lookup = {}
    for row in ohlc_rows:
        ts = row["x"][:16]
        ohlc_lookup[ts] = row["close"]

    peaks = []
    troughs = []
    zeros = []
    previous_zero = None

    for t_idx in range(nt):
        column = [surface[row_idx][t_idx] for row_idx in range(ny)]
        peak_row = max(range(ny), key=lambda idx: column[idx])
        trough_row = min(range(ny), key=lambda idx: column[idx])
        peaks.append(y_vals[peak_row])
        troughs.append(y_vals[trough_row])

        candidates = []
        for row_idx in range(ny - 1):
            z0 = column[row_idx]
            z1 = column[row_idx + 1]
            if z0 == 0.0:
                candidates.append(y_vals[row_idx])
            elif z0 * z1 < 0.0:
                y0 = y_vals[row_idx]
                y1 = y_vals[row_idx + 1]
                weight = abs(z0) / (abs(z0) + abs(z1))
                candidates.append(y0 + (y1 - y0) * weight)
        if not candidates:
            zero_value = y_vals[min(range(ny), key=lambda idx: abs(column[idx]))]
        else:
            minute_key = x_vals[t_idx].strftime("%Y-%m-%dT%H:%M")
            if minute_key in ohlc_lookup:
                target = ohlc_lookup[minute_key]
            elif previous_zero is not None:
                target = previous_zero
            else:
                target = y_vals[peak_row]
            zero_value = min(candidates, key=lambda value: abs(value - target))
        previous_zero = zero_value
        zeros.append(zero_value)

    return {
        "peak": peaks,
        "trough": troughs,
        "zero": zeros,
    }


def line_mae(a, b):
    return sum(abs(x - y) for x, y in zip(a, b)) / len(a)


def compare_ohlc(theta_ohlc, od_ohlc):
    od_lookup = {}
    for row in od_ohlc:
        ts = row["x"][:16]
        od_lookup[ts] = row

    comparisons = []
    shifted_lookup = {}
    for row in od_ohlc:
        minute = datetime.fromisoformat(row["x"].replace("Z", "+00:00"))
        shifted_lookup[(minute.replace(second=0, microsecond=0)).strftime("%Y-%m-%dT%H:%M")] = row

    for idx, ts in enumerate(theta_ohlc["timestamp"]):
        key = ts[:16]
        if key not in od_lookup:
            continue
        theta_row = {
            "open": float(theta_ohlc["open"][idx]),
            "high": float(theta_ohlc["high"][idx]),
            "low": float(theta_ohlc["low"][idx]),
            "close": float(theta_ohlc["close"][idx]),
        }
        od_row = od_lookup[key]
        comparisons.append({
            field: abs(theta_row[field] - od_row[field])
            for field in ("open", "high", "low", "close")
        })

    summary = {}
    for field in ("open", "high", "low", "close"):
        values = [row[field] for row in comparisons]
        summary[field] = {
            "mae": (sum(values) / len(values)) if values else None,
            "max_abs_diff": max(values) if values else None,
        }
    summary["matched_bars"] = len(comparisons)
    return summary


def write_json(path, payload):
    path.write_text(json.dumps(payload, indent=2))


def render_html(path, target_payload, model_payload, diff_payload, metrics, summary_text):
    html = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>SPX Gamma Comparison 2026-03-06</title>
  <script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
  <style>
    :root {{
      --bg: #0c1220;
      --panel: #11192d;
      --panel-alt: #17233c;
      --text: #eef3ff;
      --muted: #b4c0da;
      --accent: #f6c453;
      --line: #243554;
    }}
    body {{
      margin: 0;
      font-family: "Avenir Next", "Segoe UI", sans-serif;
      background:
        radial-gradient(circle at top left, rgba(246,196,83,0.14), transparent 28%),
        radial-gradient(circle at top right, rgba(86,158,255,0.18), transparent 24%),
        linear-gradient(180deg, #0a1020 0%, #11192d 100%);
      color: var(--text);
    }}
    .wrap {{
      max-width: 1600px;
      margin: 0 auto;
      padding: 24px;
    }}
    h1 {{
      margin: 0 0 8px;
      font-size: 30px;
      letter-spacing: 0.02em;
    }}
    p {{
      color: var(--muted);
      line-height: 1.45;
    }}
    .metrics {{
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 12px;
      margin: 18px 0 24px;
    }}
    .metric {{
      background: rgba(17,25,45,0.88);
      border: 1px solid rgba(255,255,255,0.08);
      border-radius: 16px;
      padding: 14px 16px;
      backdrop-filter: blur(16px);
    }}
    .metric .label {{
      display: block;
      color: var(--muted);
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: 0.08em;
      margin-bottom: 6px;
    }}
    .metric .value {{
      font-size: 28px;
      font-weight: 700;
      color: var(--accent);
    }}
    .grid {{
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 16px;
    }}
    .panel {{
      background: rgba(17,25,45,0.92);
      border: 1px solid rgba(255,255,255,0.08);
      border-radius: 20px;
      padding: 14px;
      box-shadow: 0 18px 50px rgba(0,0,0,0.28);
    }}
    .panel h2 {{
      margin: 0 0 10px;
      font-size: 18px;
      font-weight: 600;
    }}
    .chart {{
      height: 560px;
    }}
    .wide {{
      margin-top: 16px;
    }}
    pre {{
      white-space: pre-wrap;
      font-family: "SFMono-Regular", Menlo, monospace;
      color: var(--muted);
      font-size: 13px;
      line-height: 1.45;
      margin: 0;
    }}
  </style>
</head>
<body>
  <div class="wrap">
    <h1>SPX Gamma Heatmap Reconstruction vs OptionDepth</h1>
    <p>{summary_text}</p>
    <div class="metrics">
      <div class="metric"><span class="label">Best Model</span><span class="value">{metrics["best_model"]}</span></div>
      <div class="metric"><span class="label">Correlation</span><span class="value">{metrics["correlation"]:.3f}</span></div>
      <div class="metric"><span class="label">NRMSE</span><span class="value">{metrics["nrmse"]:.3f}</span></div>
      <div class="metric"><span class="label">Gamma Zero MAE</span><span class="value">{metrics["zero_mae"]:.1f}</span></div>
    </div>
    <div class="grid">
      <div class="panel">
        <h2>OptionDepth Target</h2>
        <div id="target" class="chart"></div>
      </div>
      <div class="panel">
        <h2>ThetaData Reconstruction</h2>
        <div id="model" class="chart"></div>
      </div>
    </div>
    <div class="panel wide">
      <h2>Difference Heatmap</h2>
      <div id="diff" class="chart"></div>
    </div>
    <div class="panel wide">
      <h2>Notes</h2>
      <pre>{json.dumps(metrics, indent=2)}</pre>
    </div>
  </div>
  <script>
    const targetPayload = {json.dumps(target_payload)};
    const modelPayload = {json.dumps(model_payload)};
    const diffPayload = {json.dumps(diff_payload)};
    const commonLayout = {{
      paper_bgcolor: "rgba(0,0,0,0)",
      plot_bgcolor: "#ffffff",
      font: {{ family: "Avenir Next, Segoe UI, sans-serif", color: "#eef3ff" }},
      margin: {{ l: 30, r: 60, t: 30, b: 40 }},
      xaxis: {{ title: "Time", tickfont: {{ color: "#eef3ff" }}, showgrid: false }},
      yaxis: {{ title: "Price", side: "right", tickfont: {{ color: "#eef3ff" }}, showgrid: false }},
      legend: {{ bgcolor: "rgba(17,25,45,0.75)", bordercolor: "rgba(255,255,255,0.08)", borderwidth: 1 }},
    }};
    Plotly.newPlot("target", targetPayload.data, {{ ...commonLayout, title: "OptionDepth", annotations: [] }}, {{ responsive: true }});
    Plotly.newPlot("model", modelPayload.data, {{ ...commonLayout, title: "ThetaData Proxy", annotations: [] }}, {{ responsive: true }});
    Plotly.newPlot("diff", diffPayload.data, {{ ...commonLayout, title: "Target minus reconstruction", annotations: [] }}, {{ responsive: true }});
  </script>
</body>
</html>
"""
    path.write_text(html)


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    started = time.perf_counter()
    od_payload = load_json(RAW_OD)
    target_x, target_y, target_z = parse_heatmap_grid(od_payload)
    od_ohlc = parse_ohlc_trace(od_payload)
    target_flat = flatten(target_z)
    theta_ohlc = load_json(RAW_THETA / "spx_index_ohlc_1m_20260306.json")

    contracts, skipped = build_contracts(RAW_THETA)
    contracts.sort(key=lambda item: (item["days_to_expiry"], abs(item["strike"] - 6830.71)))

    y_logs = [math.log(value) for value in target_y]
    y_inverses = [1.0 / value for value in target_y]
    time_offsets = [(value - BASE_TIME).total_seconds() / (365.0 * 24.0 * 3600.0) for value in target_x]

    strike_bins = ["le_100", "101_150", "151_200", "201_300", "301_400", "401_500", "gt_500"]
    dte_bins = ["le_7", "8_14", "15_30", "31_60", "61_180", "gt_180"]
    sign_models = {
        "public_call_plus_put_minus": "public_sign",
        "dealer_short_all_options": "dealer_short_sign",
    }

    surface_bins = {
        sign_key: {
            strike_key: {
                dte_key: init_surface(len(target_x) * len(target_y))
                for dte_key in dte_bins
            }
            for strike_key in strike_bins
        }
        for sign_key in sign_models
    }

    print(f"[build] contracts={len(contracts)} ny={len(target_y)} nt={len(target_x)}")
    for idx, contract in enumerate(contracts, start=1):
        if idx == 1 or idx % 250 == 0:
            elapsed = time.perf_counter() - started
            print(f"[build] contract={idx}/{len(contracts)} elapsed_s={elapsed:.1f}")

        sigma = contract["sigma"]
        strike = contract["strike"]
        ln_strike = contract["ln_strike"]
        mu = contract["mu"]
        expiry_dt = contract["expiry_dt"]
        oi_multiplier = contract["oi"] * CONTRACT_MULTIPLIER * GAMMA_LABEL_MOVE
        strike_key = contract["strike_bin"]
        dte_key = contract["dte_bin"]
        public_scale = oi_multiplier * contract["public_sign"]
        short_scale = oi_multiplier * contract["dealer_short_sign"]

        public_surface = surface_bins["public_call_plus_put_minus"][strike_key][dte_key]
        short_surface = surface_bins["dealer_short_all_options"][strike_key][dte_key]

        for t_idx, time_offset in enumerate(time_offsets):
            if target_x[t_idx] > expiry_dt:
                continue
            tau = contract["t0"] - time_offset
            if tau <= 0.0:
                tau = MIN_TAU
            sqrt_tau = math.sqrt(tau)
            denom = sigma * sqrt_tau
            if denom <= 0.0 or not math.isfinite(denom):
                continue
            drift = (mu + (0.5 * sigma * sigma)) * tau
            common_scale = INV_SQRT_2PI / denom
            public_base = public_scale * common_scale
            short_base = short_scale * common_scale

            for y_idx, ln_spot in enumerate(y_logs):
                d1 = (ln_spot - ln_strike + drift) / denom
                gamma_val = math.exp(-0.5 * d1 * d1) * y_inverses[y_idx]
                flat_idx = (y_idx * len(target_x)) + t_idx
                public_surface[flat_idx] += public_base * gamma_val
                short_surface[flat_idx] += short_base * gamma_val

    cumulative_surfaces = {}
    for sign_key in sign_models:
        cumulative_surfaces[sign_key] = {}
        by_strike = surface_bins[sign_key]
        strike_order = ["le_100", "101_150", "151_200", "201_300", "301_400", "401_500", "gt_500"]
        dte_order = ["le_7", "8_14", "15_30", "31_60", "61_180", "gt_180"]

        cumulative_strike = {}
        running_strike = {dte: init_surface(len(target_x) * len(target_y)) for dte in dte_order}
        for strike_key in strike_order:
            for dte_key in dte_order:
                running_strike[dte_key] = add_surface(running_strike[dte_key], by_strike[strike_key][dte_key])
            cumulative_strike[strike_key] = {dte_key: list(values) for dte_key, values in running_strike.items()}

        for strike_key in strike_order:
            cumulative_surfaces[sign_key][strike_key] = {}
            running = init_surface(len(target_x) * len(target_y))
            for dte_key in dte_order:
                running = add_surface(running, cumulative_strike[strike_key][dte_key])
                cumulative_surfaces[sign_key][strike_key][dte_key] = list(running)

    target_lines = extract_lines(target_y, target_x, target_z, od_ohlc)
    evaluations = []
    for sign_key, strike_map in cumulative_surfaces.items():
        for strike_key, dte_map in strike_map.items():
            for dte_key, flat_surface in dte_map.items():
                corr = correlation(flat_surface, target_flat)
                slope, intercept, rmse, nrmse, fitted = fit_affine(flat_surface, target_flat)
                fitted_matrix = surface_to_matrix(fitted, len(target_y), len(target_x))
                lines = extract_lines(target_y, target_x, fitted_matrix, od_ohlc)
                label = f"{sign_key}|strike={strike_key}|dte={dte_key}"
                evaluations.append({
                    "label": label,
                    "sign_model": sign_key,
                    "strike_scope": strike_key,
                    "dte_scope": dte_key,
                    "correlation": corr,
                    "slope": slope,
                    "intercept": intercept,
                    "rmse": rmse,
                    "nrmse": nrmse,
                    "peak_mae": line_mae(lines["peak"], target_lines["peak"]),
                    "trough_mae": line_mae(lines["trough"], target_lines["trough"]),
                    "zero_mae": line_mae(lines["zero"], target_lines["zero"]),
                    "fitted_surface": fitted_matrix,
                    "fitted_flat": fitted,
                    "lines": lines,
                })

    evaluations.sort(key=lambda item: (-item["correlation"], item["nrmse"], item["zero_mae"]))
    best = evaluations[0]

    diff_matrix = []
    for row_idx in range(len(target_y)):
        diff_row = []
        for col_idx in range(len(target_x)):
            diff_row.append(target_z[row_idx][col_idx] - best["fitted_surface"][row_idx][col_idx])
        diff_matrix.append(diff_row)

    theta_ohlc_summary = compare_ohlc(theta_ohlc, od_ohlc)

    metrics = {
        "best_model": best["label"],
        "correlation": best["correlation"],
        "rmse": best["rmse"],
        "nrmse": best["nrmse"],
        "peak_mae": best["peak_mae"],
        "trough_mae": best["trough_mae"],
        "zero_mae": best["zero_mae"],
        "theta_vs_optiondepth_ohlc": theta_ohlc_summary,
        "contract_count": len(contracts),
        "skipped": skipped,
        "runtime_seconds": time.perf_counter() - started,
    }

    write_json(OUT_DIR / "comparison_metrics.json", {
        "metrics": metrics,
        "top_models": [
            {
                key: value
                for key, value in entry.items()
                if key not in ("fitted_surface", "fitted_flat", "lines")
            }
            for entry in evaluations[:10]
        ],
    })

    write_json(OUT_DIR / "best_surface.json", {
        "x": [value.isoformat() for value in target_x],
        "y": target_y,
        "target_z": target_z,
        "model_z": best["fitted_surface"],
        "difference_z": diff_matrix,
        "target_lines": target_lines,
        "model_lines": best["lines"],
    })

    summary_lines = [
        "# SPX Gamma Reconstruction vs OptionDepth (2026-03-06)",
        "",
        f"- Best model: `{best['label']}`",
        f"- Surface correlation: `{best['correlation']:.4f}`",
        f"- Surface NRMSE after affine fit: `{best['nrmse']:.4f}`",
        f"- Peak line MAE: `{best['peak_mae']:.2f}` points",
        f"- Trough line MAE: `{best['trough_mae']:.2f}` points",
        f"- Zero line MAE: `{best['zero_mae']:.2f}` points",
        f"- ThetaData OHLC matched bars: `{theta_ohlc_summary['matched_bars']}`",
        f"- ThetaData OHLC close MAE vs OptionDepth: `{theta_ohlc_summary['close']['mae']:.4f}`",
        "",
        "## Build Inputs",
        "",
        "- OptionDepth target chart: `raw/optiondepth/od_chart_props_2026-03-06.json`",
        "- ThetaData SPXW greeks: `raw/thetadata/spxw_greeks_eod_20260305.json`",
        "- ThetaData SPX greeks: `raw/thetadata/spx_greeks_eod_20260305.json`",
        "- ThetaData SPXW OI: `raw/thetadata/spxw_oi_20260306.json`",
        "- ThetaData SPX OI: `raw/thetadata/spx_oi_20260306.json`",
        "- ThetaData SPX index OHLC: `raw/thetadata/spx_index_ohlc_1m_20260306.json`",
        "",
        "## Proxy Method",
        "",
        "- Merge ThetaData `EOD` greeks from `2026-03-05` with open interest reported on `2026-03-06`.",
        "- Reprice each contract on the OptionDepth price grid with a Black-Scholes-style gamma formula using the contract's implied volatility and a drift term inferred from ThetaData `d1` / `d2`.",
        "- Aggregate to market-level gamma with two sign conventions: public `call + / put -` and dealer-short `all negative`.",
        "- Fit a simple affine transform to align unit scale before comparing to OptionDepth.",
        "",
        "## Read",
        "",
        "- This is a close public proxy, not an exact clone of OptionDepth's proprietary inventory model.",
        "- The best match here should be interpreted as 'how close we can get with public OI + ThetaData greeks + a transparent sign convention', not as proof of OptionDepth's internal method.",
    ]
    (OUT_DIR / "SPX_GAMMA_2026-03-06_COMPARISON.md").write_text("\n".join(summary_lines) + "\n")

    target_lines_plot = [
        {
            "x": [value.isoformat() for value in target_x],
            "y": target_y,
            "z": target_z,
            "type": "heatmap",
            "name": "OptionDepth Gamma",
            "colorscale": "RdBu",
            "zmid": 0,
            "showscale": True,
            "colorbar": {"title": "Gamma"},
        },
        {
            "x": [value.isoformat() for value in target_x],
            "y": target_lines["peak"],
            "type": "scatter",
            "mode": "lines",
            "name": "Peak",
            "line": {"color": "#f5c451", "width": 2, "dash": "dot"},
        },
        {
            "x": [value.isoformat() for value in target_x],
            "y": target_lines["trough"],
            "type": "scatter",
            "mode": "lines",
            "name": "Trough",
            "line": {"color": "#ef6b73", "width": 2, "dash": "dot"},
        },
        {
            "x": [value.isoformat() for value in target_x],
            "y": target_lines["zero"],
            "type": "scatter",
            "mode": "lines",
            "name": "Zero",
            "line": {"color": "#ffffff", "width": 2},
        },
    ]
    model_lines_plot = [
        {
            "x": [value.isoformat() for value in target_x],
            "y": target_y,
            "z": best["fitted_surface"],
            "type": "heatmap",
            "name": "ThetaData Gamma",
            "colorscale": "RdBu",
            "zmid": 0,
            "showscale": True,
            "colorbar": {"title": "Gamma"},
        },
        {
            "x": [value.isoformat() for value in target_x],
            "y": best["lines"]["peak"],
            "type": "scatter",
            "mode": "lines",
            "name": "Peak",
            "line": {"color": "#f5c451", "width": 2, "dash": "dot"},
        },
        {
            "x": [value.isoformat() for value in target_x],
            "y": best["lines"]["trough"],
            "type": "scatter",
            "mode": "lines",
            "name": "Trough",
            "line": {"color": "#ef6b73", "width": 2, "dash": "dot"},
        },
        {
            "x": [value.isoformat() for value in target_x],
            "y": best["lines"]["zero"],
            "type": "scatter",
            "mode": "lines",
            "name": "Zero",
            "line": {"color": "#ffffff", "width": 2},
        },
    ]
    diff_plot = {
        "data": [{
            "x": [value.isoformat() for value in target_x],
            "y": target_y,
            "z": diff_matrix,
            "type": "heatmap",
            "name": "Difference",
            "colorscale": "RdBu",
            "zmid": 0,
            "showscale": True,
            "colorbar": {"title": "Target - model"},
        }],
    }

    render_html(
        OUT_DIR / "gamma_compare.html",
        {"data": target_lines_plot},
        {"data": model_lines_plot},
        diff_plot,
        metrics,
        "The left panel is the real OptionDepth gamma surface for SPX on 2026-03-06. The right panel is a ThetaData-only proxy built from prior-close EOD greeks plus open interest reported before the session.",
    )

    print(json.dumps(metrics, indent=2))


if __name__ == "__main__":
    main()
