#!/usr/bin/env python3

import json
import pathlib

from compare_od_gamma_legends import build_model_segments, load_json


ROOT = pathlib.Path("/Users/pawanagarwal/github/phenixflow/output/playwright/optiondepth")
OD_PATH = ROOT / "raw" / "optiondepth" / "od_chart_props_2026-03-06.json"
BEST_PATH = ROOT / "reconstruction" / "spx_gamma_2026-03-06" / "default" / "best_surface.json"
COMPARE_PATH = ROOT / "reconstruction" / "spx_gamma_2026-03-06" / "default" / "legend_layer_comparison.json"
THETA_OHLC_PATH = ROOT / "raw" / "thetadata" / "spx_index_ohlc_1m_20260306.json"
OUT_HTML = ROOT / "reconstruction" / "spx_gamma_2026-03-06" / "default" / "gamma_complete_replica.html"


def make_segment_traces(name, segments, line_style):
    traces = []
    for idx, segment in enumerate(segments):
        traces.append({
            "type": "scatter",
            "mode": "lines",
            "name": name,
            "showlegend": idx == 0,
            "x": segment["x"],
            "y": segment["y"],
            "line": line_style,
            "hoverinfo": "skip",
        })
    return traces


def make_heatmap_trace(x, y, z, colorscale, name):
    return {
        "type": "heatmap",
        "name": name,
        "x": x,
        "y": y,
        "z": z,
        "colorscale": colorscale,
        "zmin": -1600,
        "zmax": 1600,
        "zmid": 0,
        "hovertemplate": "Time: %{x}<br>Price: %{y}<br>Value: %{z}<extra></extra>",
        "colorbar": {
            "title": "Gamma / (Δ / 2.5 pts)",
            "titleside": "top",
            "thickness": 15,
            "len": 0.55,
        },
    }


def make_ohlc_trace(x, ohlc):
    return {
        "type": "candlestick",
        "name": "OHLC",
        "x": x,
        "open": ohlc["open"],
        "high": ohlc["high"],
        "low": ohlc["low"],
        "close": ohlc["close"],
        "increasing": {"line": {"color": "#ffffff", "width": 1}},
        "decreasing": {"line": {"color": "#ffffff", "width": 1}},
        "opacity": 0.8,
        "yaxis": "y",
    }


def build_target_traces(od):
    heat = od["data"][0]
    peak = od["data"][1]
    trough = od["data"][2]
    zero = od["data"][3]
    ohlc = od["data"][4]

    traces = [make_heatmap_trace(heat["x"], heat["y"], heat["z"], heat["colorscale"], "Gamma / (Δ / 2.5 pts)")]
    traces.extend(make_segment_traces("Gamma Peak", peak["lines"], peak["line"]))
    traces.extend(make_segment_traces("Gamma Trough", trough["lines"], trough["line"]))
    traces.extend(make_segment_traces("Gamma Zero", zero["lines"], zero["line"]))
    traces.append(make_ohlc_trace(ohlc["x"], {
        "open": ohlc["open"],
        "high": ohlc["high"],
        "low": ohlc["low"],
        "close": ohlc["close"],
    }))
    return traces


def build_model_traces(best_surface, od, theta_ohlc):
    colorscale = od["data"][0]["colorscale"]
    peak_style = od["data"][1]["line"]
    trough_style = od["data"][2]["line"]
    zero_style = od["data"][3]["line"]

    peak_segments = build_model_segments(best_surface, "peak")
    trough_segments = build_model_segments(best_surface, "trough")
    zero_segments = build_model_segments(best_surface, "zero")

    traces = [make_heatmap_trace(best_surface["x"], best_surface["y"], best_surface["model_z"], colorscale, "Gamma / (Δ / 2.5 pts)")]
    traces.extend(make_segment_traces("Gamma Peak", peak_segments, peak_style))
    traces.extend(make_segment_traces("Gamma Trough", trough_segments, trough_style))
    traces.extend(make_segment_traces("Gamma Zero", zero_segments, zero_style))
    traces.append(make_ohlc_trace(theta_ohlc["timestamp"], theta_ohlc))
    return traces


def main():
    od = load_json(OD_PATH)
    best = load_json(BEST_PATH)
    compare = load_json(COMPARE_PATH)
    theta_ohlc = load_json(THETA_OHLC_PATH)

    metrics = compare["region_metrics"]
    line_metrics = compare["line_comparisons"]

    target_traces = build_target_traces(od)
    model_traces = build_model_traces(best, od, theta_ohlc)

    layout = {
        "paper_bgcolor": "rgba(0,0,0,0)",
        "plot_bgcolor": "#ffffff",
        "font": {"family": "Avenir Next, Segoe UI, sans-serif", "color": "#eef3ff"},
        "margin": {"l": 24, "r": 70, "t": 36, "b": 40},
        "xaxis": {"title": "Time", "showgrid": False, "tickfont": {"color": "#eef3ff"}},
        "yaxis": {"title": "SPX Price", "side": "right", "showgrid": False, "tickfont": {"color": "#eef3ff"}},
        "legend": {
            "orientation": "v",
            "x": 1.02,
            "y": 1.0,
            "bgcolor": "rgba(17,25,45,0.78)",
            "bordercolor": "rgba(255,255,255,0.08)",
            "borderwidth": 1,
            "font": {"color": "#eef3ff"},
        },
    }

    summary = {
        "sign_agreement": round(metrics["sign_agreement"], 4),
        "band_agreement": round(metrics["band_agreement"], 4),
        "positive_iou": round(metrics["positive_region_iou"], 4),
        "negative_iou": round(metrics["negative_region_iou"], 4),
        "neutral_iou": round(metrics["neutral_region_iou"], 4),
        "peak_segments": {
            "od": line_metrics["Gamma Peak"]["od"]["segment_count"],
            "model": line_metrics["Gamma Peak"]["model"]["segment_count"],
            "od_to_model_mae": round(line_metrics["Gamma Peak"]["distance"]["od_to_model_mae"], 2),
        },
        "trough_segments": {
            "od": line_metrics["Gamma Trough"]["od"]["segment_count"],
            "model": line_metrics["Gamma Trough"]["model"]["segment_count"],
            "od_to_model_mae": round(line_metrics["Gamma Trough"]["distance"]["od_to_model_mae"], 2),
        },
        "zero_segments": {
            "od": line_metrics["Gamma Zero"]["od"]["segment_count"],
            "model": line_metrics["Gamma Zero"]["model"]["segment_count"],
            "od_to_model_mae": round(line_metrics["Gamma Zero"]["distance"]["od_to_model_mae"], 2),
        },
    }

    html = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>SPX Gamma Complete Replica</title>
  <script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
  <style>
    :root {{
      --bg: #0b1220;
      --panel: rgba(17,25,45,0.92);
      --line: rgba(255,255,255,0.08);
      --text: #eef3ff;
      --muted: #b8c4df;
      --accent: #f5c451;
    }}
    body {{
      margin: 0;
      color: var(--text);
      font-family: "Avenir Next", "Segoe UI", sans-serif;
      background:
        radial-gradient(circle at top left, rgba(245,196,81,0.12), transparent 26%),
        radial-gradient(circle at top right, rgba(68,132,255,0.16), transparent 24%),
        linear-gradient(180deg, #0a1020 0%, #11192d 100%);
    }}
    .wrap {{
      max-width: 1700px;
      margin: 0 auto;
      padding: 24px;
    }}
    h1 {{
      margin: 0 0 8px;
      font-size: 30px;
    }}
    p {{
      margin: 0;
      color: var(--muted);
      line-height: 1.45;
    }}
    .metrics {{
      display: grid;
      grid-template-columns: repeat(5, minmax(0, 1fr));
      gap: 12px;
      margin: 18px 0 22px;
    }}
    .metric {{
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 16px;
      padding: 14px 16px;
      backdrop-filter: blur(14px);
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
      color: var(--accent);
      font-size: 28px;
      font-weight: 700;
    }}
    .grid {{
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 16px;
    }}
    .panel {{
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 20px;
      padding: 14px;
      box-shadow: 0 18px 50px rgba(0,0,0,0.3);
    }}
    .panel h2 {{
      margin: 0 0 10px;
      font-size: 18px;
    }}
    .chart {{
      height: 760px;
    }}
    .notes {{
      margin-top: 16px;
      padding: 16px 18px;
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 18px;
    }}
    pre {{
      margin: 0;
      white-space: pre-wrap;
      font-size: 13px;
      color: var(--muted);
      font-family: "SFMono-Regular", Menlo, monospace;
      line-height: 1.45;
    }}
  </style>
</head>
<body>
  <div class="wrap">
    <h1>SPX Gamma Full-Chart Replica vs OptionDepth</h1>
    <p>The left panel uses the actual captured OptionDepth layers. The right panel reproduces the same structure with ThetaData inputs: gamma heatmap, all detected peak/trough/zero overlays, and OHLC on the SPX price axis.</p>
    <div class="metrics">
      <div class="metric"><span class="label">Sign Agreement</span><span class="value">{summary["sign_agreement"]:.3f}</span></div>
      <div class="metric"><span class="label">Band Agreement</span><span class="value">{summary["band_agreement"]:.3f}</span></div>
      <div class="metric"><span class="label">Peak Segments</span><span class="value">{summary["peak_segments"]["model"]}/{summary["peak_segments"]["od"]}</span></div>
      <div class="metric"><span class="label">Zero Segments</span><span class="value">{summary["zero_segments"]["model"]}/{summary["zero_segments"]["od"]}</span></div>
      <div class="metric"><span class="label">Negative IoU</span><span class="value">{summary["negative_iou"]:.3f}</span></div>
    </div>
    <div class="grid">
      <div class="panel">
        <h2>Actual OptionDepth Chart</h2>
        <div id="target" class="chart"></div>
      </div>
      <div class="panel">
        <h2>ThetaData Full Replica</h2>
        <div id="model" class="chart"></div>
      </div>
    </div>
    <div class="notes">
      <pre>{json.dumps(summary, indent=2)}</pre>
    </div>
  </div>
  <script>
    const targetData = {json.dumps(target_traces)};
    const modelData = {json.dumps(model_traces)};
    const baseLayout = {json.dumps(layout)};
    Plotly.newPlot("target", targetData, {{ ...baseLayout, title: "OptionDepth" }}, {{ responsive: true }});
    Plotly.newPlot("model", modelData, {{ ...baseLayout, title: "ThetaData Replica" }}, {{ responsive: true }});
  </script>
</body>
</html>
"""
    OUT_HTML.write_text(html)
    print(str(OUT_HTML))


if __name__ == "__main__":
    main()
