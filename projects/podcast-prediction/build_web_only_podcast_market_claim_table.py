#!/usr/bin/env python3

import argparse
import json
import os
import sys
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

PODCAST_ROOT = Path(__file__).resolve().parent
ROOT = PODCAST_ROOT.parent
sys.path.insert(0, str(PODCAST_ROOT))
import build_full_podcast_market_claim_table as base  # noqa: E402


WEB_ONLY_OPENAI_CACHE = base.CACHE_ROOT / "openai_web_only"
WEB_ONLY_MANIFEST = base.MANIFEST_DIR / "web_only"
START_DEFAULT = date(2023, 3, 18)
END_DEFAULT = date(2026, 3, 18)


def ensure_dirs() -> None:
    base.ensure_dirs()
    WEB_ONLY_OPENAI_CACHE.mkdir(parents=True, exist_ok=True)
    WEB_ONLY_MANIFEST.mkdir(parents=True, exist_ok=True)


def clean_summary(summary: str) -> str:
    text = base.normalize_space(summary)
    stop_phrases = [
        "This podcast was recorded on",
        "This episode was recorded on",
        "This podcast should not be copied",
        "The information contained in this recording",
        "All price references and market forecasts",
        "Related Tags",
        "Subscribe to Briefings",
    ]
    for phrase in stop_phrases:
        idx = text.find(phrase)
        if idx != -1:
            text = text[:idx].strip()
    return base.normalize_space(text)


def load_records() -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
    metadata = base.read_json(base.MANIFEST_DIR / "collected_metadata_records.json") or []
    transcript_rows = base.read_json(base.MANIFEST_DIR / "transcript_records.json") or []
    direct = {
        row["episode_url"]: row
        for row in transcript_rows
        if row.get("transcript_source") in {"html_transcript", "pdf_transcript"}
    }
    return metadata, direct


def build_content_record(meta: Dict[str, Any], direct_transcripts: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    direct = direct_transcripts.get(meta["episode_url"])
    summary = clean_summary(meta.get("summary", ""))
    if direct:
        content_text = direct.get("transcript_text", "")
        content_source = "direct_web_transcript"
        content_note = "Analysis based on full transcript retrieved directly from the web."
        transcript_url = direct.get("transcript_url") or meta.get("transcript_url", "")
        transcript_path = direct.get("transcript_path", "")
        transcript_chars = direct.get("transcript_chars", 0)
    else:
        content_text = summary or meta.get("title", "")
        content_source = "web_page_summary"
        content_note = (
            "Analysis based on firm web page title/summary because a direct transcript was not "
            "retrievable from this environment."
        )
        transcript_url = meta.get("transcript_url", "")
        transcript_path = ""
        transcript_chars = 0

    record = dict(meta)
    record["summary"] = summary
    record["content_text"] = content_text
    record["content_source"] = content_source
    record["content_note"] = content_note
    record["transcript_url"] = transcript_url
    record["transcript_path"] = transcript_path
    record["transcript_chars"] = transcript_chars
    return record


def openai_prompt(record: Dict[str, Any], *, salvage: bool = False) -> str:
    common = (
        "Extract the top stock-market claims from this investment podcast episode.\n\n"
        "Rules:\n"
        "- Focus only on investable claims about equities, equity regions, sectors, styles, earnings, or macro/rates themes that clearly map to stock-market positioning.\n"
        "- Ignore housekeeping, legal disclaimers, and generic marketing language.\n"
        "- Return 2-3 distinct claims when supported. If the accessible web content only supports 1 clear claim, return 1.\n"
        "- Each claim should be concise and specific.\n"
        "- Map each claim to one liquid US-listed ETF ticker when possible.\n"
        "- Use action labels exactly from: buy, hold, watch, avoid, hedge.\n"
        "- Do not invent details not supported by the accessible content.\n"
        "- If the episode is not meaningfully about equities, return zero claims and set market_relevance to none.\n\n"
        f"Firm: {record['firm']}\n"
        f"Series: {record['series']}\n"
        f"Episode title: {record['title']}\n"
        f"Podcast date: {record['podcast_date']}\n"
        f"Episode URL: {record['episode_url']}\n"
        f"Transcript URL: {record.get('transcript_url', '')}\n"
        f"Summary: {record.get('summary', '') or '(none)'}\n"
        f"Content source: {record['content_source']}\n"
        f"Source note: {record['content_note']}\n\n"
    )

    if salvage and record["content_source"] == "web_page_summary":
        common += (
            "Additional instruction for summary-only rows:\n"
            "- Use the episode title and summary to infer the most defensible market implications of the discussion.\n"
            "- If the summary clearly signals a market theme such as inflation, rates, banks, Europe, AI, Treasuries, commercial real estate, or portfolio stress, convert that into 1-3 conservative equity/ETF implications rather than returning zero claims.\n"
            "- Stay high-level and do not invent precise forecasts, numbers, or sector winners that are not supported by the title/summary.\n\n"
        )

    if record["content_source"] == "direct_web_transcript":
        return common + f"Full transcript:\n{record['content_text']}\n"
    return common + f"Accessible web content:\n{record['content_text']}\n"


def old_direct_cache(record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if record["content_source"] != "direct_web_transcript":
        return None
    cache_path = base.OPENAI_CACHE / f"{base.sha1_hex(record['episode_url'])}.json"
    if cache_path.exists():
        cached = base.read_json(cache_path)
        if isinstance(cached, dict):
            return cached
    return None


def analyze_record(record: Dict[str, Any], *, model: str, refresh: bool = False, salvage: bool = False) -> Dict[str, Any]:
    cache_key = base.sha1_hex(
        record["episode_url"]
        + "|"
        + record["content_source"]
        + "|"
        + ("salvage" if salvage else "normal")
        + "|"
        + base.sha1_hex(record["content_text"])
    )
    cache_path = WEB_ONLY_OPENAI_CACHE / f"{cache_key}.json"
    if cache_path.exists() and not refresh:
        cached = base.read_json(cache_path)
        assert isinstance(cached, dict)
        return cached

    inherited = old_direct_cache(record)
    if inherited is not None and not refresh:
        base.write_json(cache_path, inherited)
        return inherited

    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is not set")

    payload = {
        "model": model,
        "reasoning": {"effort": "low"},
        "text": {
            "verbosity": "low",
            "format": {
                "type": "json_schema",
                "name": "podcast_claims",
                "schema": base.OPENAI_SCHEMA,
                "strict": True,
            },
        },
        "input": openai_prompt(record, salvage=salvage),
    }
    request = urllib.request.Request(
        "https://api.openai.com/v1/responses",
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        method="POST",
    )

    last_error: Optional[Exception] = None
    for attempt in range(4):
        try:
            with urllib.request.urlopen(request, timeout=180) as response:
                response_payload = json.load(response)
            text = base.response_output_text(response_payload)
            parsed = json.loads(text)
            base.write_json(cache_path, parsed)
            return parsed
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            time.sleep(min(2 ** attempt, 12))
    assert last_error is not None
    raise last_error


def attach_analysis(record: Dict[str, Any], analysis: Dict[str, Any]) -> Dict[str, Any]:
    enriched = base.attach_analysis(record, analysis)
    enriched["content_source"] = record["content_source"]
    enriched["content_note"] = record["content_note"]
    enriched["summary"] = record.get("summary", "")
    return enriched


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
            except Exception as exc:  # noqa: BLE001
                failures.append([record["episode_url"], str(exc)])
            if idx % 25 == 0:
                base.log(f"[web-only analyze] completed {idx}/{len(records)} rows, failures {len(failures)}")

    results.sort(key=lambda row: (row["podcast_date"], row["firm"], row["series"], row["title"]))
    base.write_json(WEB_ONLY_MANIFEST / "analysis_failures.json", failures)
    base.write_json(WEB_ONLY_MANIFEST / "analyzed_records.json", results)
    return results


def repair_records(records: List[Dict[str, Any]], *, model: str, workers: int) -> List[Dict[str, Any]]:
    by_url = {row["episode_url"]: dict(row) for row in records}
    raw_records = base.read_json(WEB_ONLY_MANIFEST / "content_records.json") or []
    raw_by_url = {row["episode_url"]: row for row in raw_records}

    flagged = []
    for row in records:
        claim_count = sum(1 for idx in range(1, 4) if row.get(f"claim_{idx}"))
        if claim_count == 0 and row.get("market_relevance") not in {"none", ""}:
            flagged.append(row["episode_url"])
        if row["content_source"] == "web_page_summary" and claim_count == 0:
            flagged.append(row["episode_url"])
    flagged = sorted(set(flagged))
    if not flagged:
        return records

    base.log(f"[web-only repair] re-running {len(flagged)} flagged rows")
    rerun_records = [raw_by_url[url] for url in flagged if url in raw_by_url]
    rerun = analyze_records(
        rerun_records,
        model=model,
        workers=max(1, min(workers, 3)),
        refresh=True,
        salvage=True,
    )
    for row in rerun:
        by_url[row["episode_url"]] = row
    repaired = sorted(by_url.values(), key=lambda row: (row["podcast_date"], row["firm"], row["series"], row["title"]))
    base.write_json(WEB_ONLY_MANIFEST / "repaired_records.json", repaired)
    return repaired


def build_dataframe(records: List[Dict[str, Any]]) -> pd.DataFrame:
    rows = []
    for record in records:
        row = {
            "firm": record["firm"],
            "series": record["series"],
            "podcast_date": record["podcast_date"],
            "title": record["title"],
            "episode_url": record["episode_url"],
            "transcript_url": record.get("transcript_url", ""),
            "source_content_type": record.get("content_source", ""),
            "source_note": record.get("content_note", ""),
            "summary": record.get("summary", ""),
            "market_relevance": record.get("market_relevance", ""),
            "market_relevance_reason": record.get("market_relevance_reason", ""),
            "overall_summary": record.get("overall_summary", ""),
            "top_claims": record.get("top_claims", ""),
            "etf_actions": record.get("etf_actions", ""),
            "claim_1": record.get("claim_1", ""),
            "claim_1_etf_action": record.get("claim_1_etf_action", ""),
            "claim_1_ticker": record.get("claim_1_ticker", ""),
            "claim_1_rationale": record.get("claim_1_rationale", ""),
            "claim_2": record.get("claim_2", ""),
            "claim_2_etf_action": record.get("claim_2_etf_action", ""),
            "claim_2_ticker": record.get("claim_2_ticker", ""),
            "claim_2_rationale": record.get("claim_2_rationale", ""),
            "claim_3": record.get("claim_3", ""),
            "claim_3_etf_action": record.get("claim_3_etf_action", ""),
            "claim_3_ticker": record.get("claim_3_ticker", ""),
            "claim_3_rationale": record.get("claim_3_rationale", ""),
            "transcript_chars": record.get("transcript_chars", 0),
            "transcript_path": record.get("transcript_path", ""),
        }
        rows.append(row)
    return pd.DataFrame(rows)


def export_outputs(records: List[Dict[str, Any]], start_date: date, end_date: date) -> Dict[str, str]:
    stamp = datetime.now().strftime("%Y-%m-%d")
    prefix = f"web_only_podcast_market_claim_table_{start_date.isoformat()}_{end_date.isoformat()}_{stamp}"
    csv_path = base.ARTIFACT_ROOT / f"{prefix}.csv"
    xlsx_path = base.ARTIFACT_ROOT / f"{prefix}.xlsx"
    json_path = base.ARTIFACT_ROOT / f"{prefix}.json"
    summary_path = base.ARTIFACT_ROOT / f"web_only_podcast_market_claim_summary_{start_date.isoformat()}_{end_date.isoformat()}_{stamp}.md"

    df = build_dataframe(records)
    df.to_csv(csv_path, index=False)
    df.to_excel(xlsx_path, index=False)

    payload = {
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "row_count": len(records),
        "source_counts": df.groupby(["firm", "series", "source_content_type"]).size().reset_index(name="count").to_dict(orient="records"),
        "records": records,
    }
    base.write_json(json_path, payload)

    lines = [
        "# Web-Only Podcast Market Claim Table",
        "",
        f"- Date range: {start_date.isoformat()} to {end_date.isoformat()}",
        f"- Rows: {len(records)}",
        "",
        "## Source Counts",
    ]
    for item in payload["source_counts"]:
        lines.append(f"- {item['firm']} | {item['series']} | {item['source_content_type']}: {item['count']}")
    base.write_text(summary_path, "\n".join(lines))

    output_paths = {
        "csv": str(csv_path),
        "xlsx": str(xlsx_path),
        "json": str(json_path),
        "summary_md": str(summary_path),
    }
    base.write_json(WEB_ONLY_MANIFEST / "output_paths.json", output_paths)
    return output_paths


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build the web-only podcast market claim table.")
    parser.add_argument("--start-date", default=START_DEFAULT.isoformat())
    parser.add_argument("--end-date", default=END_DEFAULT.isoformat())
    parser.add_argument("--analysis-workers", type=int, default=6)
    parser.add_argument("--refresh", action="store_true")
    return parser.parse_args()


def main() -> int:
    ensure_dirs()
    base.load_env_file(ROOT / ".env")
    args = parse_args()
    start_date = base.parse_date_string(args.start_date)
    end_date = base.parse_date_string(args.end_date)
    if not start_date or not end_date:
        raise ValueError("Invalid date range")

    metadata, direct = load_records()
    records = []
    for meta in metadata:
        podcast_date = base.parse_date_string(meta.get("podcast_date", ""))
        if podcast_date is None or podcast_date < start_date or podcast_date > end_date:
            continue
        records.append(build_content_record(meta, direct))
    base.write_json(WEB_ONLY_MANIFEST / "content_records.json", records)
    base.log(f"[web-only] prepared {len(records)} rows")

    model = os.environ.get("ALMA_OPENAI_MODEL", "gpt-5-mini")
    analyzed = analyze_records(records, model=model, workers=args.analysis_workers, refresh=args.refresh)
    repaired = repair_records(analyzed, model=model, workers=args.analysis_workers)
    outputs = export_outputs(repaired, start_date, end_date)
    base.log("[web-only] outputs")
    for label, path in outputs.items():
        base.log(f"  - {label}: {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
