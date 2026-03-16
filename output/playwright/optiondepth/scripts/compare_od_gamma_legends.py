#!/usr/bin/env python3

import json
import math
import pathlib
from datetime import datetime


ROOT = pathlib.Path("/Users/pawanagarwal/github/phenixflow/output/playwright/optiondepth")
OD_PATH = ROOT / "raw" / "optiondepth" / "od_chart_props_2026-03-06.json"
BEST_PATH = ROOT / "reconstruction" / "spx_gamma_2026-03-06" / "default" / "best_surface.json"
OUT_DIR = ROOT / "reconstruction" / "spx_gamma_2026-03-06" / "default"


def load_json(path):
    return json.loads(path.read_text())


def parse_time(value):
    normalized = value.replace("Z", "+00:00")
    dt = datetime.fromisoformat(normalized)
    if dt.tzinfo is not None:
        dt = dt.replace(tzinfo=None)
    return dt


def flatten(matrix):
    flat = []
    for row in matrix:
        flat.extend(row)
    return flat


def quantile(values, q):
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    pos = q * (len(ordered) - 1)
    lo = int(math.floor(pos))
    hi = int(math.ceil(pos))
    if lo == hi:
        return ordered[lo]
    weight = pos - lo
    return ordered[lo] * (1.0 - weight) + ordered[hi] * weight


def segment_summary(segments):
    lengths = [len(seg["x"]) for seg in segments]
    y_values = [y for seg in segments for y in seg["y"]]
    return {
        "segment_count": len(segments),
        "point_count": sum(lengths),
        "longest_segments": sorted(lengths, reverse=True)[:10],
        "y_min": min(y_values) if y_values else None,
        "y_max": max(y_values) if y_values else None,
    }


def extrema_points_for_column(column, y_vals, mode):
    points = []
    if len(column) >= 2:
        if mode == "peak" and column[0] > column[1]:
            points.append(y_vals[0])
        if mode == "trough" and column[0] < column[1]:
            points.append(y_vals[0])
    for idx in range(1, len(column) - 1):
        left = column[idx - 1]
        center = column[idx]
        right = column[idx + 1]
        if mode == "peak":
            is_extremum = center >= left and center > right
        else:
            is_extremum = center <= left and center < right
        if not is_extremum:
            continue

        denom = left - (2.0 * center) + right
        offset = 0.0
        if denom != 0.0:
            offset = 0.5 * (left - right) / denom
            if offset > 1.0:
                offset = 1.0
            elif offset < -1.0:
                offset = -1.0
        step = y_vals[idx] - y_vals[idx - 1]
        points.append(y_vals[idx] + (offset * step))
    if len(column) >= 2:
        if mode == "peak" and column[-1] > column[-2]:
            points.append(y_vals[-1])
        if mode == "trough" and column[-1] < column[-2]:
            points.append(y_vals[-1])
    return points


def zero_points_for_column(column, y_vals):
    points = []
    for idx in range(len(column) - 1):
        z0 = column[idx]
        z1 = column[idx + 1]
        if z0 == 0.0:
            points.append(y_vals[idx])
            continue
        if z0 * z1 < 0.0:
            y0 = y_vals[idx]
            y1 = y_vals[idx + 1]
            weight = abs(z0) / (abs(z0) + abs(z1))
            points.append(y0 + (y1 - y0) * weight)
    return points


def connect_points_to_segments(points_by_time, x_vals, max_gap):
    segments = []
    active = []
    for t_idx, points in enumerate(points_by_time):
        time_value = x_vals[t_idx]
        remaining = sorted(points)
        next_active = []

        for segment in active:
            best_idx = None
            best_gap = None
            for idx, value in enumerate(remaining):
                gap = abs(value - segment["last_y"])
                if best_gap is None or gap < best_gap:
                    best_gap = gap
                    best_idx = idx
            if best_idx is not None and best_gap is not None and best_gap <= max_gap:
                y_value = remaining.pop(best_idx)
                segment["x"].append(time_value)
                segment["y"].append(y_value)
                segment["last_y"] = y_value
                next_active.append(segment)
            else:
                segments.append({
                    "x": [item.isoformat() for item in segment["x"]],
                    "y": list(segment["y"]),
                })

        for value in remaining:
            next_active.append({
                "x": [time_value],
                "y": [value],
                "last_y": value,
            })
        active = next_active

    for segment in active:
        segments.append({
            "x": [item.isoformat() for item in segment["x"]],
            "y": list(segment["y"]),
        })
    return segments


def build_model_segments(best_surface, mode):
    x_vals = [parse_time(item) for item in best_surface["x"]]
    y_vals = [float(item) for item in best_surface["y"]]
    z_vals = best_surface["model_z"]
    points_by_time = []

    for t_idx in range(len(x_vals)):
        column = [z_vals[row_idx][t_idx] for row_idx in range(len(y_vals))]
        if mode == "zero":
            points = zero_points_for_column(column, y_vals)
        else:
            points = extrema_points_for_column(column, y_vals, mode)
        points_by_time.append(points)

    max_gap = {
        "peak": 40.0,
        "trough": 40.0,
        "zero": 60.0,
    }[mode]
    return connect_points_to_segments(points_by_time, x_vals, max_gap)


def resample_segments_to_grid(segments, target_times):
    time_values = [parse_time(value) if isinstance(value, str) else value for value in target_times]
    bucket = {value.isoformat(): [] for value in time_values}

    for segment in segments:
        seg_x = [parse_time(value) if isinstance(value, str) else value for value in segment["x"]]
        seg_y = [float(value) for value in segment["y"]]
        pairs = sorted(zip(seg_x, seg_y), key=lambda item: item[0])
        if len(pairs) == 1:
            only_x, only_y = pairs[0]
            key = only_x.isoformat()
            if key in bucket:
                bucket[key].append(only_y)
            continue

        seg_x = [item[0] for item in pairs]
        seg_y = [item[1] for item in pairs]
        for target in time_values:
            if target < seg_x[0] or target > seg_x[-1]:
                continue
            for idx in range(len(seg_x) - 1):
                left = seg_x[idx]
                right = seg_x[idx + 1]
                if left <= target <= right:
                    if right == left:
                        value = seg_y[idx]
                    else:
                        span = (right - left).total_seconds()
                        weight = (target - left).total_seconds() / span
                        value = seg_y[idx] + ((seg_y[idx + 1] - seg_y[idx]) * weight)
                    bucket[target.isoformat()].append(value)
                    break
    return bucket


def nearest_distance(source_values, target_values):
    if not source_values or not target_values:
        return []
    return [min(abs(source - target) for target in target_values) for source in source_values]


def compare_trace_points(od_segments, model_segments, target_times):
    od_resampled = resample_segments_to_grid(od_segments, target_times)
    model_resampled = resample_segments_to_grid(model_segments, target_times)

    od_to_model = []
    model_to_od = []
    times_both = 0
    times_od_only = 0
    times_model_only = 0
    times_none = 0

    for target in target_times:
        key = parse_time(target).isoformat() if isinstance(target, str) else target.isoformat()
        od_vals = sorted(od_resampled.get(key, []))
        model_vals = sorted(model_resampled.get(key, []))
        if od_vals and model_vals:
            times_both += 1
            od_to_model.extend(nearest_distance(od_vals, model_vals))
            model_to_od.extend(nearest_distance(model_vals, od_vals))
        elif od_vals:
            times_od_only += 1
        elif model_vals:
            times_model_only += 1
        else:
            times_none += 1

    return {
        "times_with_both": times_both,
        "times_od_only": times_od_only,
        "times_model_only": times_model_only,
        "times_none": times_none,
        "od_point_count": sum(len(values) for values in od_resampled.values()),
        "model_point_count": sum(len(values) for values in model_resampled.values()),
        "od_to_model_mae": (sum(od_to_model) / len(od_to_model)) if od_to_model else None,
        "od_to_model_p90": quantile(od_to_model, 0.90),
        "model_to_od_mae": (sum(model_to_od) / len(model_to_od)) if model_to_od else None,
        "model_to_od_p90": quantile(model_to_od, 0.90),
    }


def sign_label(value):
    return 1 if value >= 0.0 else -1


def gamma_band(value, zmin=-1600.0, zmax=1600.0, bins=10):
    clipped = value
    if clipped < zmin:
        clipped = zmin
    elif clipped > zmax:
        clipped = zmax
    width = (zmax - zmin) / bins
    if width == 0:
        return 0
    idx = int((clipped - zmin) / width)
    if idx >= bins:
        idx = bins - 1
    if idx < 0:
        idx = 0
    return idx


def compute_region_metrics(best_surface):
    target = best_surface["target_z"]
    model = best_surface["model_z"]
    total = 0
    sign_match = 0
    band_match = 0
    positive_inter = positive_union = 0
    negative_inter = negative_union = 0
    neutral_inter = neutral_union = 0

    for row_idx in range(len(target)):
        for col_idx in range(len(target[row_idx])):
            total += 1
            target_value = target[row_idx][col_idx]
            model_value = model[row_idx][col_idx]

            if sign_label(target_value) == sign_label(model_value):
                sign_match += 1
            if gamma_band(target_value) == gamma_band(model_value):
                band_match += 1

            target_pos = target_value > 0.0
            model_pos = model_value > 0.0
            target_neg = target_value < 0.0
            model_neg = model_value < 0.0
            target_neutral = abs(target_value) <= 320.0
            model_neutral = abs(model_value) <= 320.0

            positive_inter += 1 if (target_pos and model_pos) else 0
            positive_union += 1 if (target_pos or model_pos) else 0
            negative_inter += 1 if (target_neg and model_neg) else 0
            negative_union += 1 if (target_neg or model_neg) else 0
            neutral_inter += 1 if (target_neutral and model_neutral) else 0
            neutral_union += 1 if (target_neutral or model_neutral) else 0

    return {
        "cell_count": total,
        "sign_agreement": sign_match / total,
        "band_agreement": band_match / total,
        "positive_region_iou": (positive_inter / positive_union) if positive_union else None,
        "negative_region_iou": (negative_inter / negative_union) if negative_union else None,
        "neutral_region_iou": (neutral_inter / neutral_union) if neutral_union else None,
    }


def main():
    od = load_json(OD_PATH)
    best_surface = load_json(BEST_PATH)

    legend_names = [trace["name"] for trace in od["data"]]
    trace_map = {trace["name"]: trace for trace in od["data"]}

    target_times = best_surface["x"]
    model_segments = {
        "Gamma Peak": build_model_segments(best_surface, "peak"),
        "Gamma Trough": build_model_segments(best_surface, "trough"),
        "Gamma Zero": build_model_segments(best_surface, "zero"),
    }

    od_segments = {
        "Gamma Peak": trace_map["Gamma Peak"]["lines"],
        "Gamma Trough": trace_map["Gamma Trough"]["lines"],
        "Gamma Zero": trace_map["Gamma Zero"]["lines"],
    }

    line_comparisons = {}
    for name in ("Gamma Peak", "Gamma Trough", "Gamma Zero"):
        line_comparisons[name] = {
            "od": segment_summary(od_segments[name]),
            "model": segment_summary(model_segments[name]),
            "distance": compare_trace_points(od_segments[name], model_segments[name], target_times),
        }

    region_metrics = compute_region_metrics(best_surface)

    payload = {
        "legend_items": legend_names,
        "line_comparisons": line_comparisons,
        "region_metrics": region_metrics,
    }

    (OUT_DIR / "legend_layer_comparison.json").write_text(json.dumps(payload, indent=2))

    md_lines = [
        "# OptionDepth Gamma Legend Layer Comparison",
        "",
        "## Legend Items Present",
        "",
    ]
    for item in legend_names:
        md_lines.append(f"- `{item}`")

    md_lines.extend([
        "",
        "## Region Match",
        "",
        f"- Sign agreement on the heatmap grid: `{region_metrics['sign_agreement']:.4f}`",
        f"- Exact clipped color-band agreement: `{region_metrics['band_agreement']:.4f}`",
        f"- Positive-region IoU: `{region_metrics['positive_region_iou']:.4f}`",
        f"- Negative-region IoU: `{region_metrics['negative_region_iou']:.4f}`",
        f"- Near-neutral-region IoU (`|gamma| <= 320`): `{region_metrics['neutral_region_iou']:.4f}`",
        "",
        "## Overlay Layers",
        "",
    ])

    for name in ("Gamma Peak", "Gamma Trough", "Gamma Zero"):
        comp = line_comparisons[name]
        md_lines.extend([
            f"### {name}",
            "",
            f"- OptionDepth segments: `{comp['od']['segment_count']}`",
            f"- ThetaData proxy segments: `{comp['model']['segment_count']}`",
            f"- OptionDepth point count: `{comp['od']['point_count']}`",
            f"- ThetaData proxy point count: `{comp['model']['point_count']}`",
            f"- OD -> proxy MAE at shared times: `{comp['distance']['od_to_model_mae']:.2f}`" if comp["distance"]["od_to_model_mae"] is not None else "- OD -> proxy MAE at shared times: `n/a`",
            f"- OD -> proxy p90 distance: `{comp['distance']['od_to_model_p90']:.2f}`" if comp["distance"]["od_to_model_p90"] is not None else "- OD -> proxy p90 distance: `n/a`",
            f"- Proxy -> OD MAE at shared times: `{comp['distance']['model_to_od_mae']:.2f}`" if comp["distance"]["model_to_od_mae"] is not None else "- Proxy -> OD MAE at shared times: `n/a`",
            f"- Shared 5-minute columns: `{comp['distance']['times_with_both']}`",
            f"- OD-only columns: `{comp['distance']['times_od_only']}`",
            f"- Proxy-only columns: `{comp['distance']['times_model_only']}`",
            "",
        ])

    md_lines.extend([
        "## Read",
        "",
        "- `Gamma / (∆ / 2.5 pts)` is the full heatmap field; the overlay layers are separate contour-segment collections.",
        "- On this day, OptionDepth plotted many more peak/trough/zero branches than a single-path summary would suggest.",
        "- The region map can be directionally close while the individual legend overlays still differ a lot in topology and branching.",
        "",
    ])

    (OUT_DIR / "LEGEND_LAYER_COMPARISON.md").write_text("\n".join(md_lines))
    print(json.dumps(payload, indent=2))


if __name__ == "__main__":
    main()
