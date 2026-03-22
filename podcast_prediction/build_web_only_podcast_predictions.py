#!/usr/bin/env python3

import argparse
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional

PODCAST_ROOT = Path(__file__).resolve().parent
ROOT = PODCAST_ROOT.parent
sys.path.insert(0, str(PODCAST_ROOT))
import prediction_support as support  # noqa: E402


CODEX_ANALYSIS_CACHE = support.CODEX_CACHE / "predictions"
START_DEFAULT = date(2023, 3, 18)
END_DEFAULT = date(2026, 3, 18)


def ensure_dirs() -> None:
    support.ensure_dirs()
    CODEX_ANALYSIS_CACHE.mkdir(parents=True, exist_ok=True)


def analyze_record(record: Dict[str, Any], *, model: str, refresh: bool = False, salvage: bool = False) -> Dict[str, Any]:
    cache_key = support.sha1_hex(
        record["episode_url"]
        + "|"
        + record["content_source"]
        + "|"
        + ("salvage" if salvage else "normal")
        + "|"
        + support.sha1_hex(record["content_text"])
    )
    cache_path = CODEX_ANALYSIS_CACHE / f"{cache_key}.json"
    if cache_path.exists() and not refresh:
        cached = support.read_json(cache_path)
        assert isinstance(cached, dict)
        return cached
    prompt = support.prediction_prompt(record, salvage=salvage)
    last_error: Optional[Exception] = None
    for attempt in range(4):
        try:
            parsed = support.analyze_with_codex_prompt(
                prompt,
                model=model,
                output_path=CODEX_ANALYSIS_CACHE / f"{cache_key}.codex-output.json",
            )
            support.write_json(cache_path, parsed)
            return parsed
        except Exception as exc:
            last_error = exc
            time.sleep(min(2 ** attempt, 12))
    assert last_error is not None
    raise last_error


def attach_analysis(record: Dict[str, Any], analysis: Dict[str, Any]) -> Dict[str, Any]:
    enriched = support.attach_prediction_analysis(record, analysis)
    enriched["summary"] = record.get("summary", "")
    return enriched


def build_content_records(
    metadata_records: List[Dict[str, Any]],
    *,
    workers: int,
    refresh: bool,
) -> List[Dict[str, Any]]:
    records: List[Dict[str, Any]] = []
    failures: List[List[str]] = []
    with ThreadPoolExecutor(max_workers=workers) as executor:
        future_map = {
            executor.submit(support.build_content_record, meta, refresh=refresh): meta
            for meta in metadata_records
        }
        for idx, future in enumerate(as_completed(future_map), start=1):
            meta = future_map[future]
            try:
                records.append(future.result())
            except Exception as exc:
                failures.append([
                    meta.get("episode_url", ""),
                    meta.get("firm", ""),
                    meta.get("series", ""),
                    meta.get("title", ""),
                    str(exc),
                ])
            if idx % 25 == 0:
                support.log(f"[content] completed {idx}/{len(metadata_records)} rows, failures {len(failures)}")
    records.sort(key=lambda row: (row["podcast_date"], row["firm"], row["series"], row["title"]))
    support.write_json(support.MANIFEST_ROOT / "content_failures.json", failures)
    support.write_json(support.MANIFEST_ROOT / "content_records.json", records)
    return records


def analyze_records(
    records: List[Dict[str, Any]],
    *,
    model: str,
    workers: int,
    refresh: bool = False,
    salvage: bool = False,
) -> List[Dict[str, Any]]:
    results: List[Dict[str, Any]] = []
    failures: List[List[str]] = []
    with ThreadPoolExecutor(max_workers=workers) as executor:
        future_map = {
            executor.submit(analyze_record, record, model=model, refresh=refresh, salvage=salvage): record
            for record in records
        }
        for idx, future in enumerate(as_completed(future_map), start=1):
            record = future_map[future]
            try:
                analysis = future.result()
                results.append(attach_analysis(record, analysis))
            except Exception as exc:
                failures.append([
                    record["episode_url"],
                    record.get("firm", ""),
                    record.get("series", ""),
                    record.get("title", ""),
                    str(exc),
                ])
            if idx % 25 == 0:
                support.log(f"[predict] completed {idx}/{len(records)} rows, failures {len(failures)}")
    results.sort(key=lambda row: (row["podcast_date"], row["firm"], row["series"], row["title"]))
    support.write_json(support.MANIFEST_ROOT / "analysis_failures.json", failures)
    support.write_json(support.MANIFEST_ROOT / "analyzed_records.json", results)
    return results


def repair_records(records: List[Dict[str, Any]], *, model: str, workers: int) -> List[Dict[str, Any]]:
    by_url = {row["episode_url"]: dict(row) for row in records}
    raw_records = support.read_json(support.MANIFEST_ROOT / "content_records.json") or []
    raw_by_url = {row["episode_url"]: row for row in raw_records}
    flagged: List[str] = []
    for row in records:
        prediction_count = sum(1 for idx in range(1, 6) if row.get(f"prediction_{idx}"))
        if row.get("market_relevance") not in {"none", ""} and prediction_count == 0:
            flagged.append(row["episode_url"])
        if len(row.get("overall_summary", "")) < 40:
            flagged.append(row["episode_url"])
        if row.get("content_source") == "web_page_summary" and prediction_count == 0:
            flagged.append(row["episode_url"])
    flagged = sorted(set(flagged))
    if not flagged:
        return records
    support.log(f"[repair] re-running {len(flagged)} flagged rows")
    rerun_records = [raw_by_url[url] for url in flagged if url in raw_by_url]
    rerun = analyze_records(
        rerun_records,
        model=model,
        workers=max(1, min(workers, 2)),
        refresh=True,
        salvage=True,
    )
    for row in rerun:
        by_url[row["episode_url"]] = row
    repaired = sorted(by_url.values(), key=lambda row: (row["podcast_date"], row["firm"], row["series"], row["title"]))
    support.write_json(support.MANIFEST_ROOT / "repaired_records.json", repaired)
    return repaired


def firm_slug(firm: str) -> str:
    return "morgan_stanley" if firm == "Morgan Stanley" else "goldman_sachs"


def podcast_text_path(record: Dict[str, Any]) -> Path:
    slug = firm_slug(record["firm"])
    title_slug = support.safe_filename(record["title"].lower())
    return support.PER_PODCAST_ROOT / slug / f"{record['podcast_date']}__{title_slug}.txt"


def write_podcast_text(record: Dict[str, Any]) -> str:
    lines = [
        f"{record['firm']} | {record['series']} | {record['title']}",
        f"Date: {record['podcast_date']}",
        f"Content basis: {record.get('content_source', '')}",
        f"Episode URL: {record.get('episode_url', '')}",
        f"Transcript URL: {record.get('transcript_url', '')}",
        "",
        "Overall summary:",
        record.get("overall_summary", "") or "(none)",
        "",
        "Detailed prediction text:",
        record.get("detailed_prediction_text", "") or "(none)",
        "",
        "Key predictions:",
    ]
    found = False
    for idx in range(1, 6):
        prediction = record.get(f"prediction_{idx}", "")
        if not prediction:
            continue
        found = True
        lines.extend(
            [
                f"{idx}. {prediction}",
                f"   Direction: {record.get(f'prediction_{idx}_direction', '')}",
                f"   Time horizon: {record.get(f'prediction_{idx}_time_horizon', '')}",
                f"   Confidence: {record.get(f'prediction_{idx}_confidence', 0.0):.2f}",
                f"   Rationale: {record.get(f'prediction_{idx}_rationale', '')}",
                f"   Evidence: {record.get(f'prediction_{idx}_evidence', '')}",
            ]
        )
    if not found:
        lines.append("(none)")
    path = podcast_text_path(record)
    support.write_text(path, "\n".join(lines))
    return str(path)


def write_firm_review_files(records: List[Dict[str, Any]], start_date: date, end_date: date) -> Dict[str, str]:
    outputs: Dict[str, str] = {}
    for firm in ["Morgan Stanley", "Goldman Sachs"]:
        firm_rows = [row for row in records if row.get("firm") == firm]
        slug = firm_slug(firm)
        path = support.OUTPUT_ROOT / f"{slug}_podcast_predictions_{start_date.isoformat()}_{end_date.isoformat()}.md"
        lines = [
            f"# {firm} Podcast Predictions",
            "",
            "- Sorted by `podcast_date` ascending.",
            f"- Rows: {len(firm_rows)}",
        ]
        if firm_rows:
            lines.append(f"- Date range in file: {firm_rows[0]['podcast_date']} to {firm_rows[-1]['podcast_date']}")
        else:
            lines.append("- Date range in file: (none)")
        lines.extend([
            "",
            "| Date | Series | Title | Content Basis | Overall Summary | Key Predictions | Episode URL | Transcript URL | Prediction Text |",
            "|---|---|---|---|---|---|---|---|---|",
        ])
        for row in firm_rows:
            title = str(row.get("title", "")).replace("|", "\\|")
            series = str(row.get("series", "")).replace("|", "\\|")
            content_basis = str(row.get("content_source", "")).replace("|", "\\|")
            overall_summary = str(row.get("overall_summary", "")).replace("|", "\\|").replace("\n", "<br>")
            key_predictions = str(row.get("key_predictions", "")).replace("|", "\\|").replace("\n", "<br>")
            episode_url = str(row.get("episode_url", "")).replace("|", "\\|")
            transcript_url = str(row.get("transcript_url", "")).replace("|", "\\|")
            prediction_file = str(row.get("prediction_text_path", "")).replace("|", "\\|")
            lines.append(
                f"| {row.get('podcast_date', '')} | {series} | {title} | {content_basis} | {overall_summary} | {key_predictions} | {episode_url} | {transcript_url} | {prediction_file} |"
            )
        support.write_text(path, "\n".join(lines))
        outputs[f"{slug}_review_md"] = str(path)
    return outputs


def export_outputs(records: List[Dict[str, Any]], start_date: date, end_date: date) -> Dict[str, str]:
    enriched_records: List[Dict[str, Any]] = []
    for record in records:
        enriched = dict(record)
        enriched["prediction_text_path"] = write_podcast_text(record)
        enriched_records.append(enriched)
    outputs = write_firm_review_files(enriched_records, start_date, end_date)
    support.write_json(support.MANIFEST_ROOT / "final_records.json", enriched_records)
    support.write_json(support.MANIFEST_ROOT / "output_paths.json", outputs)
    return outputs


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build web-only podcast predictions.")
    parser.add_argument("--start-date", default=START_DEFAULT.isoformat())
    parser.add_argument("--end-date", default=END_DEFAULT.isoformat())
    parser.add_argument("--max-episodes", type=int, default=None)
    parser.add_argument("--collect-workers", type=int, default=8)
    parser.add_argument("--content-workers", type=int, default=8)
    parser.add_argument("--analysis-workers", type=int, default=2)
    parser.add_argument("--refresh", action="store_true")
    return parser.parse_args()


def main() -> int:
    ensure_dirs()
    support.load_env_file(ROOT / ".env")
    args = parse_args()
    start_date = support.parse_date_string(args.start_date)
    end_date = support.parse_date_string(args.end_date)
    if not start_date or not end_date:
        raise ValueError("Invalid date range")
    stubs = support.candidate_episode_stubs()
    support.log(f"[start] discovered {len(stubs)} candidate episode urls")
    metadata_records = support.collect_metadata_records(
        stubs,
        start_date=start_date,
        end_date=end_date,
        max_episodes=args.max_episodes,
        refresh=args.refresh,
        workers=args.collect_workers,
    )
    support.log(f"[collect-metadata] finished with {len(metadata_records)} in-range episode records")
    records = build_content_records(
        metadata_records,
        workers=args.content_workers,
        refresh=args.refresh,
    )
    support.log(f"[content] prepared {len(records)} rows")
    model = support.configured_model()
    analyzed = analyze_records(records, model=model, workers=args.analysis_workers, refresh=args.refresh)
    repaired = repair_records(analyzed, model=model, workers=args.analysis_workers)
    outputs = export_outputs(repaired, start_date, end_date)
    support.log("[done] outputs")
    for label, path in outputs.items():
        support.log(f"  - {label}: {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
