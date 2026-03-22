#!/usr/bin/env python3

import io
import hashlib
import json
import os
import re
import shutil
import subprocess
import threading
import time
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from bs4 import BeautifulSoup

try:
    from pypdf import PdfReader
except ImportError:
    PdfReader = None


PODCAST_ROOT = Path(__file__).resolve().parent
ROOT = PODCAST_ROOT.parent
OUTPUT_ROOT = PODCAST_ROOT / "outputs" / "podcast_predictions"
PER_PODCAST_ROOT = OUTPUT_ROOT / "per_podcast"
RUNTIME_ROOT = PODCAST_ROOT / "runtime"
CACHE_ROOT = RUNTIME_ROOT / "prediction_cache"
HTML_CACHE = CACHE_ROOT / "html"
PDF_CACHE = CACHE_ROOT / "pdf"
CODEX_CACHE = CACHE_ROOT / "codex"
MANIFEST_ROOT = RUNTIME_ROOT / "prediction_manifests"
DEFAULT_CODEX_MODEL = "gpt-5.4"
CODEX_TIMEOUT_SECONDS = int(os.environ.get("ALMA_CODEX_TIMEOUT_SECONDS", "900"))
DEFAULT_CODEX_CLI = Path("/Applications/Codex.app/Contents/Resources/codex")
CODEX_SCHEMA_PATH = RUNTIME_ROOT / "codex_prediction_schema.json"
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/137.0.0.0 Safari/537.36"
)
SITEMAP_USER_AGENT = "Mozilla/5.0"
SITEMAP_NS = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
START_DEFAULT = date(2023, 3, 18)
END_DEFAULT = date(2026, 3, 18)
PRINT_LOCK = threading.Lock()
PREDICTION_SCHEMA: Dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "market_relevance": {
            "type": "string",
            "enum": ["high", "medium", "low", "none"],
        },
        "market_relevance_reason": {"type": "string"},
        "overall_summary": {"type": "string"},
        "detailed_prediction_text": {"type": "string"},
        "predictions": {
            "type": "array",
            "minItems": 0,
            "maxItems": 5,
            "items": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "prediction": {"type": "string"},
                    "direction": {
                        "type": "string",
                        "enum": ["bullish", "bearish", "neutral", "conditional", "mixed"],
                    },
                    "time_horizon": {"type": "string"},
                    "rationale": {"type": "string"},
                    "evidence": {"type": "string"},
                    "confidence": {"type": "number"},
                },
                "required": [
                    "prediction",
                    "direction",
                    "time_horizon",
                    "rationale",
                    "evidence",
                    "confidence",
                ],
            },
        },
    },
    "required": [
        "market_relevance",
        "market_relevance_reason",
        "overall_summary",
        "detailed_prediction_text",
        "predictions",
    ],
}


def log(message: str) -> None:
    with PRINT_LOCK:
        print(message, flush=True)


def ensure_dirs() -> None:
    for path in [
        OUTPUT_ROOT,
        PER_PODCAST_ROOT,
        CACHE_ROOT,
        HTML_CACHE,
        PDF_CACHE,
        CODEX_CACHE,
        MANIFEST_ROOT,
    ]:
        path.mkdir(parents=True, exist_ok=True)


def load_env_file(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip("'").strip('"'))


def configured_model() -> str:
    model = os.environ.get("ALMA_OPENAI_MODEL", DEFAULT_CODEX_MODEL).strip() or DEFAULT_CODEX_MODEL
    if model == "gpt-5-mini":
        return DEFAULT_CODEX_MODEL
    return model


def resolve_codex_cli() -> Path:
    configured = os.environ.get("ALMA_CODEX_CLI", "").strip()
    if configured:
        return Path(configured)
    discovered = shutil.which("codex")
    if discovered:
        return Path(discovered)
    return DEFAULT_CODEX_CLI


def sha1_hex(value: str) -> str:
    return hashlib.sha1(value.encode("utf-8")).hexdigest()


def normalize_space(value: str) -> str:
    value = value.replace("\xa0", " ")
    value = re.sub(r"\r\n?", "\n", value)
    value = re.sub(r"[ \t]+", " ", value)
    value = re.sub(r"\n{3,}", "\n\n", value)
    return value.strip()


def normalize_title_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", value.lower())


def safe_filename(value: str) -> str:
    clean = re.sub(r"[^a-zA-Z0-9._-]+", "-", value).strip("-")
    return clean[:180] or "item"


def read_json(path: Path) -> Optional[Any]:
    if not path.exists():
        return None
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def fetch_url(
    url: str,
    cache_path: Path,
    *,
    headers: Optional[Dict[str, str]] = None,
    refresh: bool = False,
    timeout: int = 45,
    max_attempts: int = 3,
) -> bytes:
    if cache_path.exists() and not refresh:
        return cache_path.read_bytes()
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    request_headers = {"User-Agent": USER_AGENT}
    if headers:
        request_headers.update(headers)
    request = urllib.request.Request(url, headers=request_headers)
    last_error: Optional[Exception] = None
    for attempt in range(max_attempts):
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                payload = response.read()
            cache_path.write_bytes(payload)
            return payload
        except Exception as exc:
            last_error = exc
            time.sleep(min(2 ** attempt, 10))
    assert last_error is not None
    raise last_error


def fetch_html(url: str, refresh: bool = False) -> str:
    cache_path = HTML_CACHE / f"{sha1_hex(url)}.html"
    payload = fetch_url(url, cache_path, refresh=refresh)
    return payload.decode("utf-8", errors="replace")


def fetch_pdf(url: str, referer: str, refresh: bool = False) -> bytes:
    cache_path = PDF_CACHE / f"{sha1_hex(url)}.pdf"
    payload = fetch_url(
        url,
        cache_path,
        refresh=refresh,
        headers={"Referer": referer},
        timeout=90,
        max_attempts=3,
    )
    if payload[:4] != b"%PDF":
        if refresh:
            preview = payload[:200].decode("utf-8", errors="replace")
            raise ValueError(f"Expected PDF for {url}, got: {preview!r}")
        cache_path.unlink(missing_ok=True)
        return fetch_pdf(url, referer, refresh=True)
    return payload


def sitemap_entries(url: str) -> List[Dict[str, str]]:
    cache_path = CACHE_ROOT / f"sitemap_{sha1_hex(url)}.xml"
    if cache_path.exists():
        payload = cache_path.read_bytes()
    else:
        last_error: Optional[Exception] = None
        payload = b""
        for attempt in range(5):
            try:
                result = subprocess.run(
                    [
                        "curl",
                        "-fsSL",
                        "--http1.1",
                        "-A",
                        SITEMAP_USER_AGENT,
                        "-H",
                        "Accept: application/xml,text/xml;q=0.9,*/*;q=0.8",
                        url,
                    ],
                    check=True,
                    capture_output=True,
                )
                payload = result.stdout
                cache_path.write_bytes(payload)
                break
            except Exception as exc:
                last_error = exc
                time.sleep(min(2 ** attempt, 20))
        if not payload:
            if cache_path.exists():
                payload = cache_path.read_bytes()
            else:
                assert last_error is not None
                raise last_error
    root = ET.fromstring(payload)
    entries: List[Dict[str, str]] = []
    for node in root.findall(".//sm:url", SITEMAP_NS):
        loc = node.findtext("sm:loc", default="", namespaces=SITEMAP_NS).strip()
        lastmod = node.findtext("sm:lastmod", default="", namespaces=SITEMAP_NS).strip()
        if loc:
            entries.append({"loc": loc, "lastmod": lastmod})
    return entries


def parse_date_string(value: str) -> Optional[date]:
    cleaned = value.strip()
    if not cleaned:
        return None
    formats = [
        "%Y-%m-%d",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S%z",
        "%B %d, %Y",
        "%b %d, %Y",
    ]
    for fmt in formats:
        try:
            return datetime.strptime(cleaned, fmt).date()
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(cleaned.replace("Z", "+00:00")).date()
    except ValueError:
        return None


def extract_first_date_from_text(text: str) -> Optional[date]:
    match = re.search(
        r"\b("
        r"Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|"
        r"Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|"
        r"Dec(?:ember)?"
        r")\.?\s+\d{1,2},\s+\d{4}\b",
        text,
    )
    if not match:
        return None
    return parse_date_string(match.group(0).replace(".", ""))


def url_date_hint(url: str) -> Optional[date]:
    slug = url.rstrip("/").split("/")[-1]
    match = re.match(r"(\d{2})-(\d{2})-(\d{4})-", slug)
    if not match:
        return None
    return date(int(match.group(3)), int(match.group(1)), int(match.group(2)))


def meta_content(soup: BeautifulSoup, *, name: Optional[str] = None, property_name: Optional[str] = None) -> Optional[str]:
    attrs: Dict[str, str] = {}
    if name:
        attrs["name"] = name
    if property_name:
        attrs["property"] = property_name
    tag = soup.find("meta", attrs=attrs)
    if tag and tag.get("content"):
        return str(tag["content"]).strip()
    return None


def parse_json_ld(soup: BeautifulSoup) -> List[Any]:
    payloads: List[Any] = []
    for tag in soup.find_all("script", type="application/ld+json"):
        raw = tag.get_text(strip=True)
        if not raw:
            continue
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            continue
        if isinstance(parsed, list):
            payloads.extend(parsed)
        else:
            payloads.append(parsed)
    return payloads


def best_description(soup: BeautifulSoup) -> str:
    for lookup in [
        {"name": "description"},
        {"property": "og:description"},
        {"name": "twitter:description"},
    ]:
        tag = soup.find("meta", attrs=lookup)
        if tag and tag.get("content"):
            return normalize_space(str(tag["content"]))
    return ""


def clean_summary(summary: str) -> str:
    text = normalize_space(summary)
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
    return normalize_space(text)


def choose_transcript_block(soup: BeautifulSoup) -> str:
    candidates: List[str] = []
    for container in soup.select(".generic-expandable__content.transcript-container"):
        if container.find_parent(class_="episode-card"):
            continue
        transcript_node = container.select_one(".transcript_text") or container
        text = normalize_space(transcript_node.get_text("\n", strip=True))
        if len(text) >= 300:
            candidates.append(text)
    if candidates:
        return max(candidates, key=len)
    return ""


def extract_old_morgan_transcript(soup: BeautifulSoup) -> str:
    root = soup.select_one(".artPod_transcript")
    if not root:
        return ""
    text = normalize_space(root.get_text("\n", strip=True))
    text = re.sub(r"^(View Transcript\s+)?Hide Transcript\s+", "", text, flags=re.IGNORECASE)
    text = re.sub(r"^Transcript\s+", "", text, flags=re.IGNORECASE)
    return text


def parse_morgan_metadata(url: str, html: str, sitemap_lastmod: str) -> Optional[Dict[str, Any]]:
    soup = BeautifulSoup(html, "lxml")
    title_node = soup.find("h1")
    if not title_node:
        return None
    title = normalize_space(title_node.get_text(" ", strip=True))
    if not title:
        return None
    description = clean_summary(best_description(soup))
    published_at = meta_content(soup, name="content_publishedAt")
    podcast_date = parse_date_string(published_at or "")
    if podcast_date is None:
        podcast_date = extract_first_date_from_text(normalize_space(soup.get_text("\n", strip=True)[:4000]))
    if podcast_date is None and sitemap_lastmod:
        podcast_date = parse_date_string(sitemap_lastmod[:10])
    title_key = normalize_title_key(title)
    if title_key in {"thoughtsonthemarketpodcast", "thoughtsonthemarket"}:
        return None
    return {
        "firm": "Morgan Stanley",
        "series": "Thoughts on the Market",
        "episode_url": url,
        "transcript_url": url,
        "title": title,
        "summary": description,
        "podcast_date": podcast_date.isoformat() if podcast_date else "",
        "source_lastmod": sitemap_lastmod,
    }


def resolve_url(base_url: str, href: str) -> str:
    return urllib.parse.urljoin(base_url, href)


def parse_goldman_summary(soup: BeautifulSoup) -> str:
    h1 = soup.find("h1")
    candidates: List[str] = []
    if h1:
        for sibling in h1.parent.find_all_next(["p", "div"], limit=20):
            text = normalize_space(sibling.get_text(" ", strip=True))
            if len(text) < 80:
                continue
            if "This transcript was prepared" in text:
                continue
            if "This episode was recorded on" in text:
                text = normalize_space(text.split("This episode was recorded on", 1)[0])
            if len(text) >= 40:
                candidates.append(text)
    if candidates:
        return clean_summary(candidates[0])
    page_text = normalize_space(soup.get_text("\n", strip=True))
    match = re.search(
        r"Download Transcript\s+(.*?)\s+This episode was recorded on",
        page_text,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if match:
        return clean_summary(match.group(1))
    return ""


def parse_goldman_metadata(url: str, html: str, sitemap_lastmod: str) -> Optional[Dict[str, Any]]:
    soup = BeautifulSoup(html, "lxml")
    json_ld = parse_json_ld(soup)
    title = ""
    podcast_date: Optional[date] = None
    for item in json_ld:
        if not isinstance(item, dict):
            continue
        if not title and item.get("headline"):
            title = normalize_space(str(item["headline"]))
        if podcast_date is None and item.get("datePublished"):
            podcast_date = parse_date_string(str(item["datePublished"]))
    if not title:
        title_node = soup.find("h1")
        if not title_node:
            return None
        title = normalize_space(title_node.get_text(" ", strip=True))
    if podcast_date is None and sitemap_lastmod:
        podcast_date = parse_date_string(sitemap_lastmod[:10])
    transcript_url = ""
    for link in soup.find_all("a", href=True):
        label = normalize_space(link.get_text(" ", strip=True)).lower()
        href = link["href"]
        if "transcript" in label and href:
            transcript_url = resolve_url(url, href)
            break
    if not transcript_url:
        return None
    series = "Exchanges" if "/goldman-sachs-exchanges/" in url else "The Markets"
    return {
        "firm": "Goldman Sachs",
        "series": series,
        "episode_url": url,
        "transcript_url": transcript_url,
        "title": title,
        "summary": parse_goldman_summary(soup),
        "podcast_date": podcast_date.isoformat() if podcast_date else "",
        "source_lastmod": sitemap_lastmod,
    }


def candidate_episode_stubs() -> List[Dict[str, str]]:
    morgan = []
    for entry in sitemap_entries("https://www.morganstanley.com/sitemap.xml"):
        url = entry["loc"].rstrip("/")
        if "thoughts-on-the-market" not in url:
            continue
        if url.endswith("/insights/podcasts/thoughts-on-the-market"):
            continue
        morgan.append({"firm": "Morgan Stanley", "series": "Thoughts on the Market", "url": url, "lastmod": entry["lastmod"]})
    goldman = []
    for entry in sitemap_entries("https://www.goldmansachs.com/sitemap-1.xml"):
        url = entry["loc"].rstrip("/")
        if "/insights/goldman-sachs-exchanges/" in url and not url.endswith("/goldman-sachs-exchanges"):
            goldman.append({"firm": "Goldman Sachs", "series": "Exchanges", "url": url, "lastmod": entry["lastmod"]})
        if "/insights/the-markets/" in url and not url.endswith("/the-markets"):
            goldman.append({"firm": "Goldman Sachs", "series": "The Markets", "url": url, "lastmod": entry["lastmod"]})
    return morgan + goldman


def parse_episode_metadata(stub: Dict[str, str], refresh: bool = False) -> Optional[Dict[str, Any]]:
    url = stub["url"]
    html = fetch_html(url, refresh=refresh)
    if stub["firm"] == "Morgan Stanley":
        return parse_morgan_metadata(url, html, stub["lastmod"])
    return parse_goldman_metadata(url, html, stub["lastmod"])


def collect_metadata_records(
    stubs: List[Dict[str, str]],
    *,
    start_date: date,
    end_date: date,
    max_episodes: Optional[int],
    refresh: bool,
    workers: int,
) -> List[Dict[str, Any]]:
    filtered_stubs: List[Dict[str, str]] = []
    for stub in stubs:
        hinted_date = url_date_hint(stub["url"])
        if hinted_date and (hinted_date < start_date or hinted_date > end_date):
            continue
        filtered_stubs.append(stub)
    records: List[Dict[str, Any]] = []
    failures: List[Tuple[str, str]] = []
    with ThreadPoolExecutor(max_workers=workers) as executor:
        future_map = {executor.submit(parse_episode_metadata, stub, refresh): stub for stub in filtered_stubs}
        for idx, future in enumerate(as_completed(future_map), start=1):
            stub = future_map[future]
            try:
                record = future.result()
                if record is None:
                    continue
                podcast_date = parse_date_string(record.get("podcast_date", ""))
                if not podcast_date:
                    failures.append((stub["url"], "missing podcast date"))
                    continue
                if podcast_date < start_date or podcast_date > end_date:
                    continue
                records.append(record)
            except Exception as exc:
                failures.append((stub["url"], str(exc)))
            if idx % 50 == 0:
                log(f"[collect-metadata] processed {idx}/{len(filtered_stubs)} candidates, kept {len(records)} in range, failures {len(failures)}")
    deduped = dedupe_records(records)
    if max_episodes:
        deduped = deduped[:max_episodes]
    write_json(MANIFEST_ROOT / "collection_failures.json", failures)
    write_json(MANIFEST_ROOT / "collected_metadata_records.json", deduped)
    return deduped


def extract_pdf_text(payload: bytes) -> str:
    if PdfReader is None:
        raise RuntimeError("pypdf is required for PDF transcript extraction")
    reader = PdfReader(io.BytesIO(payload))
    transcript_chunks: List[str] = []
    for page in reader.pages:
        text = page.extract_text() or ""
        if text.strip():
            transcript_chunks.append(text)
    return normalize_space("\n\n".join(transcript_chunks))


def extract_web_transcript(record: Dict[str, Any], refresh: bool = False) -> str:
    if record["firm"] == "Morgan Stanley":
        soup = BeautifulSoup(fetch_html(record["episode_url"], refresh=refresh), "lxml")
        transcript = choose_transcript_block(soup)
        if not transcript:
            transcript = extract_old_morgan_transcript(soup)
        return transcript if len(transcript) >= 300 else ""
    transcript_url = record.get("transcript_url", "")
    if not transcript_url:
        return ""
    if "/pdfs/" in transcript_url or transcript_url.lower().endswith(".pdf"):
        try:
            payload = fetch_pdf(transcript_url, referer=record["episode_url"], refresh=refresh)
            transcript = extract_pdf_text(payload)
            return transcript if len(transcript) >= 300 else ""
        except Exception:
            return ""
    html = fetch_html(transcript_url, refresh=refresh)
    soup = BeautifulSoup(html, "lxml")
    transcript = choose_transcript_block(soup)
    if len(transcript) >= 300:
        return transcript
    body_text = normalize_space(soup.get_text("\n", strip=True))
    return body_text if len(body_text) >= 300 else ""


def build_content_record(meta: Dict[str, Any], refresh: bool = False) -> Dict[str, Any]:
    summary = clean_summary(meta.get("summary", ""))
    transcript_text = extract_web_transcript(meta, refresh=refresh)
    record = dict(meta)
    record["summary"] = summary
    if transcript_text:
        record["content_text"] = transcript_text
        record["content_source"] = "direct_web_transcript"
        record["content_note"] = "Analysis based on full transcript retrieved directly from the web."
        record["content_chars"] = len(transcript_text)
    else:
        record["content_text"] = summary or meta.get("title", "")
        record["content_source"] = "web_page_summary"
        record["content_note"] = "Analysis based on firm web page title/summary because a direct transcript was not retrievable from the web."
        record["content_chars"] = len(record["content_text"])
    return record


def dedupe_records(records: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    best: Dict[Tuple[str, str, str, str], Dict[str, Any]] = {}
    for record in records:
        key = (
            record["firm"],
            record["series"],
            record.get("podcast_date", ""),
            normalize_title_key(record["title"]),
        )
        current = best.get(key)
        if current is None or record.get("content_chars", 0) > current.get("content_chars", 0):
            best[key] = record
    return sorted(best.values(), key=lambda row: (row["podcast_date"], row["firm"], row["series"], row["title"]))


def ensure_codex_schema() -> Path:
    write_json(CODEX_SCHEMA_PATH, PREDICTION_SCHEMA)
    return CODEX_SCHEMA_PATH


def analyze_with_codex_prompt(prompt: str, *, model: str, output_path: Path) -> Dict[str, Any]:
    codex_cli = resolve_codex_cli()
    if not codex_cli.exists():
        raise RuntimeError(
            "Codex CLI not found. Set ALMA_CODEX_CLI or make `codex` available on PATH."
        )
    schema_path = ensure_codex_schema()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.unlink(missing_ok=True)
    command = [
        str(codex_cli),
        "exec",
        "-C",
        str(ROOT),
        "--sandbox",
        "read-only",
        "--ephemeral",
        "-m",
        model,
        "--output-schema",
        str(schema_path),
        "-o",
        str(output_path),
        "-",
    ]
    try:
        result = subprocess.run(
            command,
            input=prompt,
            check=True,
            capture_output=True,
            text=True,
            timeout=CODEX_TIMEOUT_SECONDS,
        )
    except subprocess.CalledProcessError as exc:
        output_path.unlink(missing_ok=True)
        stderr = normalize_space(exc.stderr or "")
        stdout = normalize_space(exc.stdout or "")
        raise RuntimeError(f"Codex exec failed for model={model}. stderr={stderr or '(empty)'} stdout={stdout or '(empty)'}") from exc
    except subprocess.TimeoutExpired as exc:
        output_path.unlink(missing_ok=True)
        raise RuntimeError(f"Codex exec timed out for model={model} after {CODEX_TIMEOUT_SECONDS}s") from exc
    if not output_path.exists():
        raise RuntimeError(f"Codex output file missing. stderr={normalize_space(result.stderr)}")
    if output_path.stat().st_size == 0:
        output_path.unlink(missing_ok=True)
        raise RuntimeError(f"Codex output file empty. stderr={normalize_space(result.stderr)} stdout={normalize_space(result.stdout)}")
    parsed = read_json(output_path)
    if not isinstance(parsed, dict):
        output_path.unlink(missing_ok=True)
        raise ValueError("Codex returned non-object prediction payload")
    return parsed


def prediction_prompt(record: Dict[str, Any], *, salvage: bool = False) -> str:
    common = (
        "Extract the key stock-market predictions from this investment podcast episode.\n\n"
        "Rules:\n"
        "- Focus only on predictions or expectations that matter for equities, equity regions, sectors, styles, earnings, macro, rates, or positioning.\n"
        "- Ignore housekeeping, legal disclaimers, and generic branding language.\n"
        "- Return 1-5 distinct predictions when supported. If the content is not meaningfully about equities, return zero predictions and set market_relevance to none.\n"
        "- Keep each prediction concise, specific, and faithful to the accessible content.\n"
        "- Do not map predictions to ETFs, stocks, or actions. This step is prediction-only.\n"
        "- Provide an overall_summary and a detailed_prediction_text that synthesizes the episode in prose.\n"
        "- For each prediction, provide direction, time_horizon, rationale, evidence, and confidence.\n"
        "- direction must be one of: bullish, bearish, neutral, conditional, mixed.\n"
        "- confidence must be a decimal between 0 and 1.\n"
        "- Do not invent details not supported by the accessible content.\n\n"
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
            "- Use the title and summary to infer only the most defensible high-level equity predictions.\n"
            "- Stay conservative and avoid making precise claims that are not supported by the accessible web content.\n\n"
        )
    label = "Full transcript" if record["content_source"] == "direct_web_transcript" else "Accessible web content"
    return common + f"{label}:\n{record['content_text']}\n"


def clamp_score(value: Any) -> float:
    try:
        score = float(value)
    except (TypeError, ValueError):
        return 0.0
    return max(0.0, min(1.0, score))


def attach_prediction_analysis(record: Dict[str, Any], analysis: Dict[str, Any]) -> Dict[str, Any]:
    predictions = analysis.get("predictions", [])
    cleaned_predictions: List[Dict[str, Any]] = []
    for prediction in predictions[:5]:
        if not isinstance(prediction, dict):
            continue
        cleaned_predictions.append(
            {
                "prediction": normalize_space(str(prediction.get("prediction", ""))),
                "direction": normalize_space(str(prediction.get("direction", "")).lower()),
                "time_horizon": normalize_space(str(prediction.get("time_horizon", ""))),
                "rationale": normalize_space(str(prediction.get("rationale", ""))),
                "evidence": normalize_space(str(prediction.get("evidence", ""))),
                "confidence": clamp_score(prediction.get("confidence", 0.0)),
            }
        )
    enriched = dict(record)
    enriched["market_relevance"] = normalize_space(str(analysis.get("market_relevance", "")))
    enriched["market_relevance_reason"] = normalize_space(str(analysis.get("market_relevance_reason", "")))
    enriched["overall_summary"] = normalize_space(str(analysis.get("overall_summary", "")))
    enriched["detailed_prediction_text"] = normalize_space(str(analysis.get("detailed_prediction_text", "")))
    enriched["predictions_json"] = json.dumps(cleaned_predictions, ensure_ascii=False)
    enriched["key_predictions"] = "\n".join(
        f"{idx}. {item['prediction']} (conf={item['confidence']:.2f})"
        for idx, item in enumerate(cleaned_predictions, start=1)
    )
    for idx in range(1, 6):
        key_prefix = f"prediction_{idx}"
        if idx <= len(cleaned_predictions):
            item = cleaned_predictions[idx - 1]
            enriched[key_prefix] = item["prediction"]
            enriched[f"{key_prefix}_direction"] = item["direction"]
            enriched[f"{key_prefix}_time_horizon"] = item["time_horizon"]
            enriched[f"{key_prefix}_rationale"] = item["rationale"]
            enriched[f"{key_prefix}_evidence"] = item["evidence"]
            enriched[f"{key_prefix}_confidence"] = item["confidence"]
        else:
            enriched[key_prefix] = ""
            enriched[f"{key_prefix}_direction"] = ""
            enriched[f"{key_prefix}_time_horizon"] = ""
            enriched[f"{key_prefix}_rationale"] = ""
            enriched[f"{key_prefix}_evidence"] = ""
            enriched[f"{key_prefix}_confidence"] = 0.0
    return enriched
