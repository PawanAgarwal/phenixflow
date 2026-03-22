#!/usr/bin/env python3

from __future__ import annotations

import json
import math
import re
import urllib.error
import urllib.parse
import urllib.request
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
from PIL import Image


ROOT = Path(__file__).resolve().parents[1]
INVENTORY_PATH = ROOT / "analysis" / "backtests" / "optiondepth-image-backtest" / "inventory.json"
OUT_DIR = ROOT / "analysis" / "backtests" / "optiondepth-zones-feb-march-v2"
YAHOO_SYMBOL = "^GSPC"
YAHOO_INTERVALS = ("1m", "2m", "5m")
ET = ZoneInfo("America/New_York")
UTC_TZ = ZoneInfo("UTC")
USER_AGENT = "Mozilla/5.0"
MONTH_PATTERN = re.compile(r"^2026-(02|03)-")
SAMPLE_X_FRACTIONS = (0.22, 0.35, 0.50, 0.65, 0.80, 0.92)
MIDDAY_X = 0.55
PM_X = 0.82
CLOSE_X = 0.94
CHART_START_HOUR = 8
CHART_START_MINUTE = 30
CHART_END_HOUR = 16
CHART_END_MINUTE = 0
REPORT_DATE = datetime.now(tz=UTC).isoformat()


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def write_json(path: Path, value) -> None:
    ensure_dir(path.parent)
    path.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")


def write_text(path: Path, value: str) -> None:
    ensure_dir(path.parent)
    path.write_text(value, encoding="utf-8")


def normalize_whitespace(value: str = "") -> str:
    return re.sub(r"\s+", " ", (value or "").replace("\u00a0", " ")).strip()


def format_number(value, digits: int = 2) -> str:
    try:
        numeric = float(value)
    except Exception:
        return ""
    if not math.isfinite(numeric):
        return ""
    return f"{numeric:.{digits}f}"


def round_up_10(value: float) -> float:
    return math.ceil(value / 10.0) * 10.0


def round_down_10(value: float) -> float:
    return math.floor(value / 10.0) * 10.0


def dt_utc(date_iso: str, hour: int, minute: int) -> datetime:
    local = datetime.fromisoformat(f"{date_iso}T00:00:00").replace(tzinfo=ET)
    return local.replace(hour=hour, minute=minute, second=0, microsecond=0).astimezone(UTC_TZ)


def parse_time_et(iso_ts: str) -> str:
    dt = datetime.fromisoformat(iso_ts.replace("Z", "+00:00")).astimezone(ET)
    return dt.strftime("%H:%M")


def fetch_yahoo_bars_for_date(date_iso: str) -> tuple[str, list[dict]]:
    start_dt = dt_utc(date_iso, 0, 0)
    next_date = (datetime.fromisoformat(date_iso) + timedelta(days=1)).date().isoformat()
    end_dt = dt_utc(next_date, 0, 0)
    period1 = int(start_dt.timestamp())
    period2 = int(end_dt.timestamp())
    last_error = None

    for interval in YAHOO_INTERVALS:
        params = urllib.parse.urlencode({
            "interval": interval,
            "period1": period1,
            "period2": period2,
            "includePrePost": "true",
            "events": "div,splits",
        })
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{urllib.parse.quote(YAHOO_SYMBOL)}?{params}"
        request = urllib.request.Request(url, headers={
            "User-Agent": USER_AGENT,
            "Accept": "application/json",
            "Referer": "https://finance.yahoo.com/",
        })
        try:
            with urllib.request.urlopen(request, timeout=60) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            last_error = RuntimeError(f"Yahoo fetch failed for {date_iso} at {interval}: {exc.code}")
            if exc.code == 422:
                continue
            raise last_error
        except Exception as exc:
            last_error = exc
            continue

        result = (((payload or {}).get("chart") or {}).get("result") or [None])[0] or {}
        timestamps = result.get("timestamp") or []
        quote = ((result.get("indicators") or {}).get("quote") or [{}])[0]
        bars = []
        for index, ts_seconds in enumerate(timestamps):
            open_v = safe_float((quote.get("open") or [None])[index])
            high_v = safe_float((quote.get("high") or [None])[index])
            low_v = safe_float((quote.get("low") or [None])[index])
            close_v = safe_float((quote.get("close") or [None])[index])
            if not all(math.isfinite(v) and v > 0 for v in (open_v, high_v, low_v, close_v)):
                continue
            ts = datetime.fromtimestamp(int(ts_seconds), tz=UTC_TZ)
            et = ts.astimezone(ET)
            et_date = et.strftime("%Y-%m-%d")
            if et_date != date_iso:
                continue
            et_time = et.strftime("%H:%M")
            if et_time < "09:30" or et_time > "16:00":
                continue
            bars.append({
                "ts": ts.isoformat().replace("+00:00", "Z"),
                "etTime": et_time,
                "open": open_v,
                "high": high_v,
                "low": low_v,
                "close": close_v,
            })
        if bars:
            return interval, bars
    raise RuntimeError(f"Yahoo fetch failed for {date_iso}: {last_error}")


def safe_float(value) -> float:
    try:
        numeric = float(value)
    except Exception:
        return float("nan")
    return numeric


def parse_script_block(content: str) -> dict | None:
    match = re.search(
        r"SCRIPT INPUTS=== SPX closed at ([0-9.]+) ===\n\n([\s\S]*?)(?:\n\n=== ES closed at|$)",
        content,
    )
    if not match:
        return None
    tokens = [token.strip() for token in match.group(2).split(",") if token.strip()]
    if len(tokens) < 18:
        return None
    return {
        "close": safe_float(match.group(1)),
        "upper4": safe_float(tokens[1]),
        "upper3": safe_float(tokens[2]),
        "upper2": safe_float(tokens[4]),
        "upperRisk": safe_float(tokens[6]),
        "upper1": safe_float(tokens[8]),
        "lower1": safe_float(tokens[10]),
        "lowerRisk": safe_float(tokens[12]),
        "lower2": safe_float(tokens[14]),
        "lower3": safe_float(tokens[15]),
        "lower4": safe_float(tokens[17]),
    }


def parse_commentary_section(content: str) -> str:
    if "INTRADAY POST Coding today’s positioning:" not in content:
        return ""
    after = content.split("INTRADAY POST Coding today’s positioning:", 1)[1]
    before_od = after.split("OptionsDepth Heatmap", 1)[0]
    return before_od.strip()


def extract_price_hints(commentary: str, floor_value: float = 6300.0, ceil_value: float = 7100.0) -> list[float]:
    values = []
    for match in re.finditer(r"\b\d{4}(?:\.\d+)?(?:/\d{2,4}(?:\.\d+)?)?\b", commentary):
        token = match.group(0)
        if "/" in token:
            parts = token.split("/")
            base = safe_float(parts[0])
            if floor_value <= base <= ceil_value:
                values.append(base)
            for part in parts[1:]:
                child = safe_float(part)
                if not math.isfinite(child):
                    continue
                if child < 1000:
                    child = math.floor(base / 100.0) * 100.0 + child
                if floor_value <= child <= ceil_value:
                    values.append(child)
        else:
            numeric = safe_float(token)
            if floor_value <= numeric <= ceil_value:
                values.append(numeric)
    return sorted({round(value, 2) for value in values})


def load_inventory_rows() -> list[dict]:
    payload = json.loads(INVENTORY_PATH.read_text(encoding="utf-8"))
    return [
        row for row in payload.get("records", [])
        if row.get("dailyPost") and MONTH_PATTERN.match(row.get("date", ""))
    ]


def crop_chart(image_path: Path) -> tuple[np.ndarray, dict]:
    image = Image.open(image_path).convert("RGB")
    array = np.array(image)
    height, width, _ = array.shape
    top = int(round(height * 0.014))
    bottom = int(round(height * 0.94))
    left = int(round(width * 0.014))
    right = int(round(width * 0.962))
    crop = array[top:bottom, left:right, :]
    return crop, {
        "top": top,
        "bottom": bottom,
        "left": left,
        "right": right,
        "height": crop.shape[0],
        "width": crop.shape[1],
    }


def build_masks(crop: np.ndarray) -> dict[str, np.ndarray]:
    red = crop[:, :, 0].astype(np.int16)
    green = crop[:, :, 1].astype(np.int16)
    blue = crop[:, :, 2].astype(np.int16)
    return {
        "green": (green > 110) & (red < 180) & (blue < 180) & ((green - red) > 20),
        "yellow": (red > 170) & (green > 150) & (blue < 130) & ((red - blue) > 50),
        "zero": (red < 150) & (green < 150) & (blue < 150) & ((red + green + blue) < 320),
    }


def cluster_indices(indices: np.ndarray, max_gap: int = 2) -> list[tuple[int, int]]:
    if len(indices) == 0:
        return []
    clusters = []
    start = prev = int(indices[0])
    for raw in indices[1:]:
        current = int(raw)
        if current <= prev + max_gap:
            prev = current
            continue
        clusters.append((start, prev))
        start = prev = current
    clusters.append((start, prev))
    return clusters


def extract_color_clusters(mask: np.ndarray, x_fraction: float) -> list[float]:
    width = mask.shape[1]
    x = int(round(width * x_fraction))
    left = max(0, x - 3)
    right = min(width, x + 4)
    sample = mask[:, left:right]
    y_indices = np.where(sample.any(axis=1))[0]
    results = []
    for start, end in cluster_indices(y_indices):
        if start < 10 or end > (mask.shape[0] - 20):
            continue
        results.append((start + end) / 2.0)
    return results


def collect_cluster_samples(masks: dict[str, np.ndarray]) -> dict[str, dict[str, list[float]]]:
    samples = {}
    for color, mask in masks.items():
        by_fraction = {}
        for fraction in SAMPLE_X_FRACTIONS:
            by_fraction[f"{fraction:.2f}"] = extract_color_clusters(mask, fraction)
        samples[color] = by_fraction
    return samples


def flatten_cluster_values(samples: dict[str, dict[str, list[float]]], include_zero: bool = True) -> list[float]:
    values = []
    for color, by_fraction in samples.items():
        if color == "zero" and not include_zero:
            continue
        for bucket in by_fraction.values():
            values.extend(bucket)
    return sorted({round(value, 1) for value in values})


def build_axis_candidates(levels: dict) -> tuple[list[float], list[float]]:
    top_candidates = sorted({
        round_up_10(levels["upper2"]),
        round_up_10(levels["upper3"]),
        round_up_10(levels["upper4"]),
        round_up_10(levels["upper4"] + 10),
        round_up_10(levels["upper4"] + 20),
        round_up_10(levels["upper4"] + 30),
        round_up_10(levels["upper3"] + 20),
    })
    bottom_candidates = sorted({
        round_down_10(levels["lower2"]),
        round_down_10(levels["lower3"]),
        round_down_10(levels["lower4"]),
        round_down_10(levels["lower4"] + 10),
        round_down_10(levels["lower4"] + 20),
        round_down_10(levels["lower3"] + 10),
        round_down_10(levels["lower2"] - 20),
    })
    return top_candidates, bottom_candidates


def map_y_to_price(y_value: float, min_y: float, max_y: float, top_price: float, bottom_price: float) -> float:
    if max_y <= min_y:
        return float("nan")
    return top_price - ((y_value - min_y) / (max_y - min_y)) * (top_price - bottom_price)


def calibrate_axis(levels: dict, commentary_numbers: list[float], samples: dict) -> dict | None:
    colored_y = flatten_cluster_values(samples, include_zero=False)
    if len(colored_y) < 6:
        return None
    min_y = min(colored_y)
    max_y = max(colored_y)
    top_candidates, bottom_candidates = build_axis_candidates(levels)
    all_y = flatten_cluster_values(samples, include_zero=True)
    best = None
    for top in top_candidates:
        for bottom in bottom_candidates:
            if top - bottom < 180:
                continue
            mapped = [map_y_to_price(y, min_y, max_y, top, bottom) for y in all_y]
            if commentary_numbers:
                errors = []
                score = 0
                for number in commentary_numbers:
                    nearest = min(mapped, key=lambda price: abs(price - number))
                    error = abs(nearest - number)
                    errors.append(error)
                    if error <= 18:
                        score += 1
                anchor_count = min(8, len(errors))
                avg_error = sum(sorted(errors)[:anchor_count]) / anchor_count
            else:
                score = 0
                avg_error = 999.0
            item = {
                "score": score,
                "avgError": avg_error,
                "topPrice": top,
                "bottomPrice": bottom,
                "minY": min_y,
                "maxY": max_y,
                "mappedAll": mapped,
                "coverageRatio": (score / len(commentary_numbers)) if commentary_numbers else 0.0,
            }
            if (
                best is None
                or item["score"] > best["score"]
                or (item["score"] == best["score"] and item["avgError"] < best["avgError"])
            ):
                best = item
    return best


def map_clusters_to_price(samples: dict, calibration: dict) -> dict[str, dict[str, list[float]]]:
    output: dict[str, dict[str, list[float]]] = {}
    for color, by_fraction in samples.items():
        output[color] = {}
        for key, values in by_fraction.items():
            output[color][key] = [
                round(map_y_to_price(
                    value,
                    calibration["minY"],
                    calibration["maxY"],
                    calibration["topPrice"],
                    calibration["bottomPrice"],
                ), 2)
                for value in values
            ]
    return output


def fraction_key(value: float) -> str:
    return f"{value:.2f}"


def dedupe_sorted(values: list[float], tolerance: float = 2.5) -> list[float]:
    if not values:
        return []
    ordered = sorted(values)
    result = [ordered[0]]
    for value in ordered[1:]:
        if abs(value - result[-1]) <= tolerance:
            result[-1] = round((result[-1] + value) / 2.0, 2)
        else:
            result.append(value)
    return result


def nearest(values: list[float], target: float) -> float | None:
    if not values:
        return None
    return min(values, key=lambda value: abs(value - target))


def nearest_above(values: list[float], target: float, min_gap: float = 0.0) -> float | None:
    candidates = [value for value in values if value > (target + min_gap)]
    if not candidates:
        return None
    return min(candidates)


def nearest_below(values: list[float], target: float, min_gap: float = 0.0) -> float | None:
    candidates = [value for value in values if value < (target - min_gap)]
    if not candidates:
        return None
    return max(candidates)


def pair_band(lower_values: list[float], upper_values: list[float], side: str, anchor_price: float) -> dict | None:
    values = dedupe_sorted(lower_values + upper_values, 3.0)
    if side == "below":
        candidates = [value for value in values if value < anchor_price]
        if not candidates:
            return None
        midpoint = max(candidates)
        band_values = [value for value in values if abs(value - midpoint) <= 12]
    else:
        candidates = [value for value in values if value > anchor_price]
        if not candidates:
            return None
        midpoint = min(candidates)
        band_values = [value for value in values if abs(value - midpoint) <= 12]
    if not band_values:
        return None
    low = min(band_values)
    high = max(band_values)
    return {
        "low": round(low, 2),
        "high": round(high, 2),
        "mid": round((low + high) / 2.0, 2),
        "width": round(high - low, 2),
        "members": [round(value, 2) for value in band_values],
    }


def cluster_values(values: list[float], tolerance: float = 10.0) -> list[list[float]]:
    values = sorted(values)
    if not values:
        return []
    clusters = [[values[0]]]
    for value in values[1:]:
        if abs(value - clusters[-1][-1]) <= tolerance:
            clusters[-1].append(value)
        else:
            clusters.append([value])
    return clusters


def derive_day_levels(mapped_samples: dict, open_price: float, close_price: float) -> dict:
    midday_key = min((fraction_key(value) for value in SAMPLE_X_FRACTIONS), key=lambda key: abs(float(key) - MIDDAY_X))
    pm_key = min((fraction_key(value) for value in SAMPLE_X_FRACTIONS), key=lambda key: abs(float(key) - PM_X))
    close_key = min((fraction_key(value) for value in SAMPLE_X_FRACTIONS), key=lambda key: abs(float(key) - CLOSE_X))

    midday_green = dedupe_sorted(mapped_samples["green"][midday_key], 3.0)
    pm_green = dedupe_sorted(mapped_samples["green"][pm_key], 3.0)
    close_green = dedupe_sorted(mapped_samples["green"][close_key], 3.0)
    midday_yellow = dedupe_sorted(mapped_samples["yellow"][midday_key], 3.0)
    pm_yellow = dedupe_sorted(mapped_samples["yellow"][pm_key], 3.0)
    close_yellow = dedupe_sorted(mapped_samples["yellow"][close_key], 3.0)
    midday_colored = dedupe_sorted(midday_green + midday_yellow, 3.0)
    pm_colored = dedupe_sorted(pm_green + pm_yellow, 3.0)
    close_colored = dedupe_sorted(close_green + close_yellow, 3.0)
    zero_candidates = dedupe_sorted(mapped_samples["zero"][midday_key] + mapped_samples["zero"][pm_key], 3.0)
    breakout_line = nearest(zero_candidates, open_price)
    level_anchor = breakout_line if breakout_line is not None else open_price
    peak_support = pair_band(pm_green, close_green, "below", open_price)
    peak_resistance = pair_band(pm_green, close_green, "above", open_price)
    trough_support = pair_band(pm_yellow, close_yellow, "below", level_anchor)
    trough_resistance = pair_band(pm_yellow, close_yellow, "above", level_anchor)
    all_peak_levels = dedupe_sorted(midday_green + pm_green + close_green, 3.0)
    all_trough_levels = dedupe_sorted(midday_yellow + pm_yellow + close_yellow, 3.0)
    late_values = dedupe_sorted(pm_colored + close_colored + mapped_samples["zero"][close_key], 3.0)
    clusters = cluster_values(late_values, 10.0)
    pin_cluster = None
    if clusters:
        pin_cluster = sorted(
            [
                {
                    "values": [round(value, 2) for value in cluster],
                    "count": len(cluster),
                    "spread": round(max(cluster) - min(cluster), 2),
                    "mid": round(sum(cluster) / len(cluster), 2),
                }
                for cluster in clusters
                if len(cluster) >= 2
            ],
            key=lambda item: (-item["count"], item["spread"], abs(item["mid"] - close_price)),
        )
    pin_zone = pin_cluster[0] if pin_cluster else None
    if pin_zone and pin_zone["spread"] <= 18:
        pin_zone = {
            "low": round(min(pin_zone["values"]), 2),
            "high": round(max(pin_zone["values"]), 2),
            "mid": pin_zone["mid"],
            "count": pin_zone["count"],
            "spread": pin_zone["spread"],
            "members": pin_zone["values"],
        }
    else:
        pin_zone = None
    return {
        "middayLevels": midday_colored,
        "pmLevels": pm_colored,
        "closeLevels": close_colored,
        "middayPeakLevels": midday_green,
        "pmPeakLevels": pm_green,
        "closePeakLevels": close_green,
        "middayTroughLevels": midday_yellow,
        "pmTroughLevels": pm_yellow,
        "closeTroughLevels": close_yellow,
        "allPeakLevels": all_peak_levels,
        "allTroughLevels": all_trough_levels,
        "zeroLevels": zero_candidates,
        "peakSupportZone": peak_support,
        "peakResistanceZone": peak_resistance,
        "troughSupportZone": trough_support,
        "troughResistanceZone": trough_resistance,
        "upperPeakFromBreakout": round(nearest_above(all_peak_levels, level_anchor, 2.0), 2) if nearest_above(all_peak_levels, level_anchor, 2.0) is not None else None,
        "lowerPeakFromBreakout": round(nearest_below(all_peak_levels, level_anchor, 2.0), 2) if nearest_below(all_peak_levels, level_anchor, 2.0) is not None else None,
        "upperTroughFromBreakout": round(nearest_above(all_trough_levels, level_anchor, 2.0), 2) if nearest_above(all_trough_levels, level_anchor, 2.0) is not None else None,
        "lowerTroughFromBreakout": round(nearest_below(all_trough_levels, level_anchor, 2.0), 2) if nearest_below(all_trough_levels, level_anchor, 2.0) is not None else None,
        "breakoutLine": round(breakout_line, 2) if breakout_line is not None else None,
        "pinZone": pin_zone,
    }


def first_index(bars: list[dict], predicate) -> int:
    for index, bar in enumerate(bars):
        if predicate(bar, index):
            return index
    return -1


def trade_path_metrics(bars: list[dict], entry_index: int, exit_index: int, side: str, entry_price: float) -> tuple[float, float]:
    mfe = 0.0
    mae = 0.0
    for index in range(entry_index + 1, min(exit_index + 1, len(bars))):
        bar = bars[index]
        if side == "long":
            mfe = max(mfe, bar["high"] - entry_price)
            mae = max(mae, entry_price - bar["low"])
        else:
            mfe = max(mfe, entry_price - bar["low"])
            mae = max(mae, bar["high"] - entry_price)
    return round(mfe, 2), round(mae, 2)


def finalize_trade(base: dict, bars: list[dict], entry_index: int, exit_index: int, exit_price: float, exit_reason: str) -> dict:
    exit_bar = bars[min(exit_index, len(bars) - 1)]
    side = base["side"]
    pnl = exit_price - base["entryPrice"] if side == "long" else base["entryPrice"] - exit_price
    mfe, mae = trade_path_metrics(bars, entry_index, exit_index, side, base["entryPrice"])
    risk = abs(base["entryPrice"] - base["stopPrice"])
    return {
        **base,
        "exitTs": exit_bar["ts"],
        "exitEt": exit_bar["etTime"],
        "exitPrice": round(exit_price, 2),
        "exitReason": exit_reason,
        "stopTriggered": exit_reason == "stop",
        "pnlPoints": round(pnl, 2),
        "pnlR": round((pnl / risk), 3) if risk > 0 else None,
        "mfePoints": mfe,
        "maePoints": mae,
    }


def exit_trade(bars: list[dict], entry_index: int, side: str, target_price: float, stop_price: float) -> tuple[int, float, str]:
    for index in range(entry_index + 1, len(bars)):
        bar = bars[index]
        if side == "long":
            stop_hit = bar["low"] <= stop_price
            target_hit = bar["high"] >= target_price
        else:
            stop_hit = bar["high"] >= stop_price
            target_hit = bar["low"] <= target_price
        if stop_hit and target_hit:
            return index, stop_price, "stop"
        if stop_hit:
            return index, stop_price, "stop"
        if target_hit:
            return index, target_price, "target"
    last_index = len(bars) - 1
    return last_index, bars[last_index]["close"], "close"


def run_zero_to_peak_continuation(day: dict, bars: list[dict], levels: dict, side: str) -> dict | None:
    breakout = levels.get("breakoutLine")
    if breakout is None:
        return None
    target = levels.get("upperPeakFromBreakout" if side == "long" else "lowerPeakFromBreakout")
    trough = levels.get("upperTroughFromBreakout" if side == "long" else "lowerTroughFromBreakout")
    if target is None:
        return None
    if abs(target - breakout) < 12.0:
        return None
    if trough is not None:
        if side == "long" and not (breakout < trough < target + 8.0):
            return None
        if side == "short" and not (target - 8.0 < trough < breakout):
            return None
    tolerance = 2.0
    open_distance_limit = 30.0
    if abs(bars[0]["open"] - breakout) > open_distance_limit:
        return None
    start_index = first_index(bars, lambda bar, _: bar["etTime"] >= "09:45")
    if start_index == -1:
        return None
    for index in range(max(start_index + 2, 2), len(bars)):
        prior_bar = bars[index - 2]
        prev_bar = bars[index - 1]
        bar = bars[index]
        if side == "long" and (
            prior_bar["close"] <= breakout + tolerance
            and prev_bar["close"] >= breakout + tolerance
            and bar["close"] >= breakout + tolerance
        ):
            if target <= bar["close"] + 4.0:
                continue
            stop = breakout - max(8.0, min(14.0, (target - breakout) * 0.35))
            exit_index, exit_price, exit_reason = exit_trade(bars, index, "long", target, stop)
            return finalize_trade({
                "strategy": "zero_to_peak_continuation",
                "side": "long",
                "rationale": f"Two consecutive closes above OD zero line {format_number(breakout)} with room to the next OD peak at {format_number(target)}.",
                "entryTs": bar["ts"],
                "entryEt": bar["etTime"],
                "entryPrice": round(bar["close"], 2),
                "targetPrice": round(target, 2),
                "stopPrice": round(stop, 2),
                "breakoutLine": breakout,
                "laneTrough": trough,
            }, bars, index, exit_index, exit_price, exit_reason)
        if side == "short" and (
            prior_bar["close"] >= breakout - tolerance
            and prev_bar["close"] <= breakout - tolerance
            and bar["close"] <= breakout - tolerance
        ):
            if target >= bar["close"] - 4.0:
                continue
            stop = breakout + max(8.0, min(14.0, (breakout - target) * 0.35))
            exit_index, exit_price, exit_reason = exit_trade(bars, index, "short", target, stop)
            return finalize_trade({
                "strategy": "zero_to_peak_continuation",
                "side": "short",
                "rationale": f"Two consecutive closes below OD zero line {format_number(breakout)} with room to the next OD peak at {format_number(target)}.",
                "entryTs": bar["ts"],
                "entryEt": bar["etTime"],
                "entryPrice": round(bar["close"], 2),
                "targetPrice": round(target, 2),
                "stopPrice": round(stop, 2),
                "breakoutLine": breakout,
                "laneTrough": trough,
            }, bars, index, exit_index, exit_price, exit_reason)
    return None


def run_peak_rejection(day: dict, bars: list[dict], levels: dict, side: str) -> dict | None:
    zone = levels.get("peakSupportZone" if side == "long" else "peakResistanceZone")
    all_troughs = levels.get("allTroughLevels") or []
    all_peaks = levels.get("allPeakLevels") or []
    breakout = levels.get("breakoutLine")
    if not zone:
        return None
    zone_low = zone["low"]
    zone_high = zone["high"]
    width = max(4.0, zone["width"])
    start_index = first_index(bars, lambda bar, _: bar["etTime"] >= "09:35")
    if start_index == -1:
        return None
    for index in range(start_index, len(bars) - 3):
        bar = bars[index]
        if side == "long":
            touched = bar["low"] <= zone_high + 1.0
            if not touched:
                continue
            for confirm_index in range(index + 1, min(index + 4, len(bars))):
                confirm_bar = bars[confirm_index]
                if confirm_bar["close"] >= zone_high + 1.5:
                    target = nearest_above(all_troughs, confirm_bar["close"], 4.0)
                    if target is None and breakout and breakout > confirm_bar["close"] + 4.0:
                        target = breakout
                    if target is None:
                        target = nearest_above(all_peaks, confirm_bar["close"], 8.0)
                    if target is None or target <= confirm_bar["close"] + 6.0:
                        continue
                    stop = zone_low - max(6.0, width * 0.9)
                    exit_index, exit_price, exit_reason = exit_trade(bars, confirm_index, "long", target, stop)
                    return finalize_trade({
                        "strategy": "peak_rejection",
                        "side": "long",
                        "rationale": f"SPX touched lower OD gamma peak {format_number(zone_low)}-{format_number(zone_high)} and reclaimed it, consistent with peak support.",
                        "entryTs": confirm_bar["ts"],
                        "entryEt": confirm_bar["etTime"],
                        "entryPrice": round(confirm_bar["close"], 2),
                        "targetPrice": round(target, 2),
                        "stopPrice": round(stop, 2),
                        "zoneLow": zone_low,
                        "zoneHigh": zone_high,
                    }, bars, confirm_index, exit_index, exit_price, exit_reason)
        else:
            touched = bar["high"] >= zone_low - 1.0
            if not touched:
                continue
            for confirm_index in range(index + 1, min(index + 4, len(bars))):
                confirm_bar = bars[confirm_index]
                if confirm_bar["close"] <= zone_low - 1.5:
                    target = nearest_below(all_troughs, confirm_bar["close"], 4.0)
                    if target is None and breakout and breakout < confirm_bar["close"] - 4.0:
                        target = breakout
                    if target is None:
                        target = nearest_below(all_peaks, confirm_bar["close"], 8.0)
                    if target is None or target >= confirm_bar["close"] - 6.0:
                        continue
                    stop = zone_high + max(6.0, width * 0.9)
                    exit_index, exit_price, exit_reason = exit_trade(bars, confirm_index, "short", target, stop)
                    return finalize_trade({
                        "strategy": "peak_rejection",
                        "side": "short",
                        "rationale": f"SPX touched upper OD gamma peak {format_number(zone_low)}-{format_number(zone_high)} and rejected it, consistent with peak resistance.",
                        "entryTs": confirm_bar["ts"],
                        "entryEt": confirm_bar["etTime"],
                        "entryPrice": round(confirm_bar["close"], 2),
                        "targetPrice": round(target, 2),
                        "stopPrice": round(stop, 2),
                        "zoneLow": zone_low,
                        "zoneHigh": zone_high,
                    }, bars, confirm_index, exit_index, exit_price, exit_reason)
    return None


def run_late_pin(day: dict, bars: list[dict], levels: dict) -> dict | None:
    pin = levels.get("pinZone")
    if not pin:
        return None
    pin_mid = pin["mid"]
    if pin["spread"] > 14 or pin["count"] < 2:
        return None
    for index, bar in enumerate(bars):
        if bar["etTime"] < "13:30" or bar["etTime"] > "15:15":
            continue
        distance = pin_mid - bar["close"]
        if abs(distance) < 6.0 or abs(distance) > 20.0:
            continue
        side = "long" if distance > 0 else "short"
        target = pin_mid
        stop = bar["close"] - max(6.0, abs(distance) * 0.85) if side == "long" else bar["close"] + max(6.0, abs(distance) * 0.85)
        exit_index, exit_price, exit_reason = exit_trade(bars, index, side, target, stop)
        return finalize_trade({
            "strategy": "late_day_pin",
            "side": side,
            "rationale": f"Late-day OD lines converged into {format_number(pin['low'])}-{format_number(pin['high'])}; traded back toward the pin zone.",
            "entryTs": bar["ts"],
            "entryEt": bar["etTime"],
            "entryPrice": round(bar["close"], 2),
            "targetPrice": round(target, 2),
            "stopPrice": round(stop, 2),
            "pinLow": pin["low"],
            "pinHigh": pin["high"],
        }, bars, index, exit_index, exit_price, exit_reason)
    return None


def summarize_trades(trades: list[dict]) -> dict:
    gross_profit = round(sum(trade["pnlPoints"] for trade in trades if trade["pnlPoints"] > 0), 2)
    gross_loss = round(sum(trade["pnlPoints"] for trade in trades if trade["pnlPoints"] < 0), 2)
    wins = sum(1 for trade in trades if trade["pnlPoints"] > 0)
    losses = sum(1 for trade in trades if trade["pnlPoints"] <= 0)
    equity = 0.0
    peak = 0.0
    max_drawdown = 0.0
    for trade in trades:
        equity += trade["pnlPoints"]
        peak = max(peak, equity)
        max_drawdown = max(max_drawdown, peak - equity)
    return {
        "trades": len(trades),
        "wins": wins,
        "losses": losses,
        "winRate": round((wins / len(trades)), 4) if trades else 0.0,
        "grossProfit": gross_profit,
        "grossLoss": gross_loss,
        "netProfit": round(gross_profit + gross_loss, 2),
        "avgTrade": round((gross_profit + gross_loss) / len(trades), 2) if trades else 0.0,
        "stopsTriggered": sum(1 for trade in trades if trade["stopTriggered"]),
        "profitFactor": round((gross_profit / abs(gross_loss)), 2) if gross_loss else None,
        "maxDrawdown": round(max_drawdown, 2),
    }


def select_candidate_trades(trades: list[dict], diagnostics: list[dict], min_score: int, earliest_et: str, first_per_day: bool) -> list[dict]:
    by_date = {item["date"]: item for item in diagnostics}
    eligible = [
        trade for trade in trades
        if by_date.get(trade["targetDate"], {}).get("calibrationScore", 0) >= min_score
        and trade["entryEt"] >= earliest_et
    ]
    eligible.sort(key=lambda item: (item["targetDate"], item["entryTs"], item["strategy"]))
    if not first_per_day:
        return eligible
    selected = []
    seen_dates = set()
    for trade in eligible:
        if trade["targetDate"] in seen_dates:
            continue
        seen_dates.add(trade["targetDate"])
        selected.append(trade)
    return selected


def build_report(days: list[dict], trades: list[dict], diagnostics: list[dict], candidate_trades: list[dict], strict_candidate_trades: list[dict]) -> str:
    overall = summarize_trades(trades)
    by_strategy = defaultdict(list)
    by_month = defaultdict(list)
    for trade in trades:
        by_strategy[trade["strategy"]].append(trade)
        by_month[trade["targetDate"][:7]].append(trade)
    lines = []
    lines.append("# OptionDepth Zones Feb-March Backtest")
    lines.append("")
    lines.append("This backtest is a second-pass `image-native` POC for Alma's archived `OptionsDepth Heatmap` posts.")
    lines.append("")
    lines.append("Method summary:")
    lines.append("- OD curves are segmented locally from the archived image.")
    lines.append("- The chart is price-calibrated using the same post's OD commentary plus nearby script range candidates.")
    lines.append("- Trade rules are deterministic after extraction and separate gamma peaks from troughs.")
    lines.append("- Signals tested: `peak_rejection`, `zero_to_peak_continuation`, `late_day_pin`.")
    lines.append("")
    lines.append("Candidate tradable subset:")
    lines.append("- `candidate_v1`: calibration score `>= 11`, entry time `>= 10:00 ET`, first qualifying trade per day only.")
    lines.append("- `candidate_v1_strict`: calibration score `>= 12`, entry time `>= 10:00 ET`, first qualifying trade per day only.")
    lines.append("")
    lines.append("## Coverage")
    lines.append("")
    lines.append(f"- Daily OD posts considered: {len(days)}")
    lines.append(f"- Days with usable script-backed calibration: {sum(1 for day in diagnostics if day['usable'])}")
    lines.append(f"- Date range: {days[0]['date']} to {days[-1]['date']}" if days else "- Date range: n/a")
    interval_counts = defaultdict(int)
    for day in diagnostics:
        if day.get("dataInterval"):
            interval_counts[day["dataInterval"]] += 1
    if interval_counts:
        lines.append("- Yahoo interval usage: " + ", ".join(f"{key}={value}" for key, value in sorted(interval_counts.items())))
    lines.append("")
    lines.append("## Candidate Subset")
    lines.append("")
    lines.append("| Bucket | Trades | Wins | Losses | Win rate | Gross profit | Gross loss | Net profit | Max drawdown | Stops | Profit factor |")
    lines.append("| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |")
    for label, subset in [
        ("candidate_v1", candidate_trades),
        ("candidate_v1_strict", strict_candidate_trades),
    ]:
        stats = summarize_trades(subset)
        lines.append(
            f"| {label} | {stats['trades']} | {stats['wins']} | {stats['losses']} | {stats['winRate']*100:.1f}% | {format_number(stats['grossProfit'])} | {format_number(stats['grossLoss'])} | {format_number(stats['netProfit'])} | {format_number(stats['maxDrawdown'])} | {stats['stopsTriggered']} | {format_number(stats['profitFactor'])} |"
        )
    lines.append("")
    lines.append("## Candidate Trades")
    lines.append("")
    lines.append("| Bucket | Date | Strategy | Side | Entry ET | Entry | Stop | Target | Exit ET | Exit | Exit reason | PnL | Source |")
    lines.append("| --- | --- | --- | --- | --- | ---: | ---: | ---: | --- | ---: | --- | ---: | --- |")
    for label, subset in [
        ("candidate_v1", candidate_trades),
        ("candidate_v1_strict", strict_candidate_trades),
    ]:
        for trade in subset:
            lines.append(
                f"| {label} | {trade['targetDate']} | {trade['strategy']} | {trade['side']} | {trade['entryEt']} | {format_number(trade['entryPrice'])} | {format_number(trade['stopPrice'])} | {format_number(trade['targetPrice'])} | {trade['exitEt']} | {format_number(trade['exitPrice'])} | {trade['exitReason']} | {format_number(trade['pnlPoints'])} | [{trade['sourceDir']}]({trade['sourcePath']}) |"
            )
    lines.append("")
    lines.append("## Overall")
    lines.append("")
    lines.append("| Bucket | Trades | Wins | Losses | Win rate | Gross profit | Gross loss | Net profit | Max drawdown | Stops | Profit factor |")
    lines.append("| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |")
    lines.append(
        f"| All trades | {overall['trades']} | {overall['wins']} | {overall['losses']} | {overall['winRate']*100:.1f}% | {format_number(overall['grossProfit'])} | {format_number(overall['grossLoss'])} | {format_number(overall['netProfit'])} | {format_number(overall['maxDrawdown'])} | {overall['stopsTriggered']} | {format_number(overall['profitFactor'])} |"
    )
    lines.append("")
    lines.append("## By Strategy")
    lines.append("")
    lines.append("| Bucket | Trades | Wins | Losses | Win rate | Gross profit | Gross loss | Net profit | Max drawdown | Stops | Profit factor |")
    lines.append("| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |")
    for strategy, strategy_trades in sorted(by_strategy.items()):
        stats = summarize_trades(strategy_trades)
        lines.append(
            f"| {strategy} | {stats['trades']} | {stats['wins']} | {stats['losses']} | {stats['winRate']*100:.1f}% | {format_number(stats['grossProfit'])} | {format_number(stats['grossLoss'])} | {format_number(stats['netProfit'])} | {format_number(stats['maxDrawdown'])} | {stats['stopsTriggered']} | {format_number(stats['profitFactor'])} |"
        )
    lines.append("")
    lines.append("## By Month")
    lines.append("")
    lines.append("| Bucket | Trades | Wins | Losses | Win rate | Gross profit | Gross loss | Net profit | Max drawdown | Stops | Profit factor |")
    lines.append("| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |")
    for month, month_trades in sorted(by_month.items()):
        stats = summarize_trades(month_trades)
        lines.append(
            f"| {month} | {stats['trades']} | {stats['wins']} | {stats['losses']} | {stats['winRate']*100:.1f}% | {format_number(stats['grossProfit'])} | {format_number(stats['grossLoss'])} | {format_number(stats['netProfit'])} | {format_number(stats['maxDrawdown'])} | {stats['stopsTriggered']} | {format_number(stats['profitFactor'])} |"
        )
    stop_trades = [trade for trade in trades if trade["stopTriggered"]]
    if stop_trades:
        lines.append("")
        lines.append("## Stops Triggered")
        lines.append("")
        lines.append("| Date | Data | Strategy | Side | Entry ET | Entry | Stop | Exit ET | Exit | PnL | Source |")
        lines.append("| --- | --- | --- | --- | --- | ---: | ---: | --- | ---: | ---: | --- |")
        for trade in stop_trades:
            lines.append(
                f"| {trade['targetDate']} | {trade['dataInterval']} | {trade['strategy']} | {trade['side']} | {trade['entryEt']} | {format_number(trade['entryPrice'])} | {format_number(trade['stopPrice'])} | {trade['exitEt']} | {format_number(trade['exitPrice'])} | {format_number(trade['pnlPoints'])} | [{trade['sourceDir']}]({trade['sourcePath']}) |"
            )
    lines.append("")
    lines.append("## All Trades")
    lines.append("")
    lines.append("| Date | Data | Strategy | Side | Entry ET | Entry | Stop | Target | Exit ET | Exit | Exit reason | PnL | MAE | MFE | Source |")
    lines.append("| --- | --- | --- | --- | --- | ---: | ---: | ---: | --- | ---: | --- | ---: | ---: | ---: | --- |")
    for trade in trades:
        lines.append(
            f"| {trade['targetDate']} | {trade['dataInterval']} | {trade['strategy']} | {trade['side']} | {trade['entryEt']} | {format_number(trade['entryPrice'])} | {format_number(trade['stopPrice'])} | {format_number(trade['targetPrice'])} | {trade['exitEt']} | {format_number(trade['exitPrice'])} | {trade['exitReason']} | {format_number(trade['pnlPoints'])} | {format_number(trade['maePoints'])} | {format_number(trade['mfePoints'])} | [{trade['sourceDir']}]({trade['sourcePath']}) |"
        )
    lines.append("")
    lines.append("## Calibration Diagnostics")
    lines.append("")
    lines.append("| Date | Data | Usable | Score | Coverage | Avg err | Zero line | Lower peak | Upper peak | Lower trough | Upper trough | Pin zone | Source |")
    lines.append("| --- | --- | --- | ---: | ---: | ---: | ---: | --- | --- | --- | --- | --- | --- |")
    for day in diagnostics:
        lower_peak = day.get("peakSupportZone")
        upper_peak = day.get("peakResistanceZone")
        lower_trough = day.get("troughSupportZone")
        upper_trough = day.get("troughResistanceZone")
        pin = day.get("pinZone")
        lines.append(
            f"| {day['date']} | {day.get('dataInterval','')} | {'yes' if day['usable'] else 'no'} | {day.get('calibrationScore', 0)} | {format_number(day.get('coverageRatio', 0.0) * 100, 1)}% | {format_number(day.get('avgError', 0.0), 1)} | {format_number(day.get('breakoutLine'))} | {format_number(lower_peak['low'])+'-'+format_number(lower_peak['high']) if lower_peak else ''} | {format_number(upper_peak['low'])+'-'+format_number(upper_peak['high']) if upper_peak else ''} | {format_number(lower_trough['low'])+'-'+format_number(lower_trough['high']) if lower_trough else ''} | {format_number(upper_trough['low'])+'-'+format_number(upper_trough['high']) if upper_trough else ''} | {format_number(pin['low'])+'-'+format_number(pin['high']) if pin else ''} | [{day['sourceDir']}]({day['sourcePath']}) |"
        )
    return "\n".join(lines) + "\n"


def main() -> None:
    ensure_dir(OUT_DIR)
    inventory_rows = load_inventory_rows()
    all_days = []
    trades = []
    diagnostics = []

    for row in inventory_rows:
        source_path = Path(row["contentPath"])
        content = source_path.read_text(encoding="utf-8")
        levels = parse_script_block(content)
        day = {
            "date": row["date"],
            "sourceDir": row["dirName"],
            "sourcePath": row["contentPath"],
            "imagePath": row.get("localImagePath"),
            "title": row["title"],
        }
        all_days.append(day)
        if not levels or not row.get("localImagePath"):
            diagnostics.append({
                **day,
                "usable": False,
                "reason": "missing_script_or_image",
            })
            continue

        commentary = parse_commentary_section(content)
        commentary_numbers = extract_price_hints(commentary)
        crop, crop_meta = crop_chart(Path(row["localImagePath"]))
        masks = build_masks(crop)
        samples = collect_cluster_samples(masks)
        calibration = calibrate_axis(levels, commentary_numbers, samples)
        if not calibration:
            diagnostics.append({
                **day,
                "usable": False,
                "reason": "calibration_failed",
            })
            continue

        try:
            data_interval, bars = fetch_yahoo_bars_for_date(row["date"])
        except Exception as exc:
            diagnostics.append({
                **day,
                "usable": False,
                "reason": f"yahoo_failed:{exc}",
            })
            continue
        if len(bars) < 20:
            diagnostics.append({
                **day,
                "usable": False,
                "reason": "insufficient_bars",
            })
            continue

        open_price = bars[0]["open"]
        close_price = bars[-1]["close"]
        mapped_samples = map_clusters_to_price(samples, calibration)
        derived_levels = derive_day_levels(mapped_samples, open_price, close_price)
        calibration_score = calibration["score"]
        usable = calibration_score >= 4 and derived_levels.get("breakoutLine") is not None
        diagnostic = {
            **day,
            "usable": usable,
            "dataInterval": data_interval,
            "openPrice": round(open_price, 2),
            "closePrice": round(close_price, 2),
            "calibrationScore": calibration_score,
            "coverageRatio": round(calibration["coverageRatio"], 4),
            "avgError": round(calibration["avgError"], 2),
            "topPrice": calibration["topPrice"],
            "bottomPrice": calibration["bottomPrice"],
            "commentaryNumbers": commentary_numbers,
            "peakSupportZone": derived_levels.get("peakSupportZone"),
            "peakResistanceZone": derived_levels.get("peakResistanceZone"),
            "troughSupportZone": derived_levels.get("troughSupportZone"),
            "troughResistanceZone": derived_levels.get("troughResistanceZone"),
            "breakoutLine": derived_levels.get("breakoutLine"),
            "pinZone": derived_levels.get("pinZone"),
            "cropMeta": crop_meta,
            "mappedSamples": mapped_samples,
        }
        diagnostics.append(diagnostic)
        if not usable:
            continue

        strategy_results = [
            run_zero_to_peak_continuation(day, bars, derived_levels, "long"),
            run_zero_to_peak_continuation(day, bars, derived_levels, "short"),
            run_peak_rejection(day, bars, derived_levels, "long"),
            run_peak_rejection(day, bars, derived_levels, "short"),
            run_late_pin(day, bars, derived_levels),
        ]
        for trade in strategy_results:
            if not trade:
                continue
            trade["targetDate"] = row["date"]
            trade["month"] = row["date"][:7]
            trade["sourceDir"] = row["dirName"]
            trade["sourcePath"] = row["contentPath"]
            trade["dataInterval"] = data_interval
            trades.append(trade)

    trades.sort(key=lambda item: (item["targetDate"], item["entryTs"], item["strategy"]))
    candidate_trades = select_candidate_trades(trades, diagnostics, min_score=11, earliest_et="10:00", first_per_day=True)
    strict_candidate_trades = select_candidate_trades(trades, diagnostics, min_score=12, earliest_et="10:00", first_per_day=True)
    report = {
        "generatedAt": REPORT_DATE,
        "daysConsidered": len(all_days),
        "usableDays": sum(1 for item in diagnostics if item.get("usable")),
        "overall": summarize_trades(trades),
        "byStrategy": {strategy: summarize_trades(items) for strategy, items in sorted(group_by(trades, "strategy").items())},
        "byMonth": {month: summarize_trades(items) for month, items in sorted(group_by(trades, "month").items())},
        "candidateV1": {
            "rules": {
                "minCalibrationScore": 11,
                "earliestEntryEt": "10:00",
                "firstTradePerDay": True,
            },
            "summary": summarize_trades(candidate_trades),
            "trades": candidate_trades,
        },
        "candidateV1Strict": {
            "rules": {
                "minCalibrationScore": 12,
                "earliestEntryEt": "10:00",
                "firstTradePerDay": True,
            },
            "summary": summarize_trades(strict_candidate_trades),
            "trades": strict_candidate_trades,
        },
        "trades": trades,
        "diagnostics": diagnostics,
    }

    write_json(OUT_DIR / "report.json", report)
    write_json(OUT_DIR / "trades.json", trades)
    write_json(OUT_DIR / "day-diagnostics.json", diagnostics)
    write_json(OUT_DIR / "candidate-trades.json", candidate_trades)
    write_json(OUT_DIR / "candidate-trades-strict.json", strict_candidate_trades)
    write_text(OUT_DIR / "README.md", build_report(all_days, trades, diagnostics, candidate_trades, strict_candidate_trades))
    print(f"Wrote OD image backtest with {len(trades)} trades across {sum(1 for item in diagnostics if item.get('usable'))} usable days.")


def group_by(items: list[dict], key: str) -> dict[str, list[dict]]:
    result = defaultdict(list)
    for item in items:
        result[item[key]].append(item)
    return result


if __name__ == "__main__":
    main()
