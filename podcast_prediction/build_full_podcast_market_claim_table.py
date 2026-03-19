#!/usr/bin/env python3

import argparse
import hashlib
import io
import json
import os
import re
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

import pandas as pd
from bs4 import BeautifulSoup
from pypdf import PdfReader


PODCAST_ROOT = Path(__file__).resolve().parent
ROOT = PODCAST_ROOT.parent
ARTIFACT_ROOT = PODCAST_ROOT / "outputs" / "podcast_claims"
RUNTIME_ROOT = PODCAST_ROOT / "runtime"
CACHE_ROOT = RUNTIME_ROOT / "cache"
HTML_CACHE = CACHE_ROOT / "html"
PDF_CACHE = CACHE_ROOT / "pdf"
MEGAPHONE_CACHE = CACHE_ROOT / "megaphone"
RAW_AUDIO_CACHE = CACHE_ROOT / "audio_raw"
COMPRESSED_AUDIO_CACHE = CACHE_ROOT / "audio_compressed"
TRANSCRIPT_CACHE = CACHE_ROOT / "transcripts"
OPENAI_CACHE = CACHE_ROOT / "openai"
MANIFEST_DIR = RUNTIME_ROOT / "manifests"
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


def log(message: str) -> None:
    with PRINT_LOCK:
        print(message, flush=True)


def ensure_dirs() -> None:
    for path in [
        ARTIFACT_ROOT,
        CACHE_ROOT,
        HTML_CACHE,
        PDF_CACHE,
        MEGAPHONE_CACHE,
        RAW_AUDIO_CACHE,
        COMPRESSED_AUDIO_CACHE,
        TRANSCRIPT_CACHE,
        OPENAI_CACHE,
        MANIFEST_DIR,
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
        key = key.strip()
        value = value.strip().strip("'").strip('"')
        os.environ.setdefault(key, value)


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
    return clean[:160] or "item"


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
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            sleep_seconds = min(2 ** attempt, 10)
            time.sleep(sleep_seconds)
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
            except Exception as exc:  # noqa: BLE001
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
            parsed = datetime.strptime(cleaned, fmt)
            return parsed.date()
        except ValueError:
            continue
    iso_candidate = cleaned.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(iso_candidate).date()
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

    description = best_description(soup)
    published_at = meta_content(soup, name="content_publishedAt")
    podcast_date = parse_date_string(published_at or "")
    if podcast_date is None:
        top_text = normalize_space(soup.get_text("\n", strip=True)[:4000])
        podcast_date = extract_first_date_from_text(top_text)
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
        return candidates[0]
    page_text = normalize_space(soup.get_text("\n", strip=True))
    match = re.search(
        r"Download Transcript\s+(.*?)\s+This episode was recorded on",
        page_text,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if match:
        return normalize_space(match.group(1))
    return ""


def parse_goldman_megaphone_episode_id(html: str) -> str:
    match = re.search(r"https://playlist\.megaphone\.fm/\?e=([A-Z0-9]+)", html)
    if match:
        return match.group(1)
    match = re.search(r"https://megaphone\.link/([A-Z0-9]+)", html)
    return match.group(1) if match else ""


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

    summary = parse_goldman_summary(soup)
    megaphone_episode_id = parse_goldman_megaphone_episode_id(html)
    series = "Exchanges" if "/goldman-sachs-exchanges/" in url else "The Markets"
    return {
        "firm": "Goldman Sachs",
        "series": series,
        "episode_url": url,
        "transcript_url": transcript_url,
        "megaphone_episode_id": megaphone_episode_id,
        "title": title,
        "summary": summary,
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
        morgan.append(
            {
                "firm": "Morgan Stanley",
                "series": "Thoughts on the Market",
                "url": url,
                "lastmod": entry["lastmod"],
            }
        )

    goldman = []
    for entry in sitemap_entries("https://www.goldmansachs.com/sitemap-1.xml"):
        url = entry["loc"].rstrip("/")
        if "/insights/goldman-sachs-exchanges/" in url and not url.endswith("/goldman-sachs-exchanges"):
            goldman.append(
                {
                    "firm": "Goldman Sachs",
                    "series": "Exchanges",
                    "url": url,
                    "lastmod": entry["lastmod"],
                }
            )
        if "/insights/the-markets/" in url and not url.endswith("/the-markets"):
            goldman.append(
                {
                    "firm": "Goldman Sachs",
                    "series": "The Markets",
                    "url": url,
                    "lastmod": entry["lastmod"],
                }
            )

    return morgan + goldman


def parse_episode_metadata(stub: Dict[str, str], refresh: bool = False) -> Optional[Dict[str, Any]]:
    url = stub["url"]
    html = fetch_html(url, refresh=refresh)
    if stub["firm"] == "Morgan Stanley":
        return parse_morgan_metadata(url, html, stub["lastmod"])
    return parse_goldman_metadata(url, html, stub["lastmod"])


def hydrate_morgan_transcript(record: Dict[str, Any], refresh: bool = False) -> Optional[Dict[str, Any]]:
    html = fetch_html(record["episode_url"], refresh=refresh)
    soup = BeautifulSoup(html, "lxml")
    transcript = choose_transcript_block(soup)
    if not transcript:
        transcript = extract_old_morgan_transcript(soup)
    if len(transcript) < 300:
        return None

    slug = record["episode_url"].rstrip("/").split("/")[-1]
    transcript_path = TRANSCRIPT_CACHE / f"{safe_filename(slug)}.txt"
    write_text(transcript_path, transcript)

    enriched = dict(record)
    enriched["transcript_text"] = transcript
    enriched["transcript_path"] = str(transcript_path)
    enriched["transcript_chars"] = len(transcript)
    enriched["transcript_source"] = "html_transcript"
    return enriched


def fetch_megaphone_episode_payload(episode_id: str) -> Dict[str, Any]:
    cache_path = MEGAPHONE_CACHE / f"{safe_filename(episode_id)}.json"
    if cache_path.exists():
        payload = read_json(cache_path)
        assert isinstance(payload, dict)
        return payload
    url = f"https://player.megaphone.fm/playlist/episode/{episode_id}"
    request = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(request, timeout=60) as response:
        payload = json.load(response)
    write_json(cache_path, payload)
    return payload


def compress_audio_for_transcription(raw_audio_path: Path) -> Path:
    if raw_audio_path.stat().st_size <= 24_000_000:
        return raw_audio_path

    output_path = COMPRESSED_AUDIO_CACHE / f"{raw_audio_path.stem}.m4a"
    if output_path.exists() and output_path.stat().st_size <= 24_000_000:
        return output_path

    bitrates = [64000, 48000, 32000]
    for bitrate in bitrates:
        subprocess.run(
            [
                "afconvert",
                "-f",
                "m4af",
                "-d",
                "aac",
                "-b",
                str(bitrate),
                str(raw_audio_path),
                str(output_path),
            ],
            check=True,
            capture_output=True,
        )
        if output_path.exists() and output_path.stat().st_size <= 24_000_000:
            return output_path

    raise ValueError(f"Compressed audio is still too large for transcription: {raw_audio_path}")


def transcribe_audio_with_openai(audio_path: Path, cache_path: Path) -> str:
    if cache_path.exists():
        return normalize_space(cache_path.read_text(encoding="utf-8"))

    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is not set")

    last_error: Optional[Exception] = None
    for attempt in range(3):
        try:
            result = subprocess.run(
                [
                    "curl",
                    "-sS",
                    "--retry",
                    "3",
                    "--retry-delay",
                    "2",
                    "--retry-all-errors",
                    "https://api.openai.com/v1/audio/transcriptions",
                    "-H",
                    f"Authorization: Bearer {api_key}",
                    "-F",
                    "model=gpt-4o-mini-transcribe",
                    "-F",
                    "response_format=json",
                    "-F",
                    "prompt=This is a Goldman Sachs investment podcast transcript about markets, companies, sectors, and macro themes.",
                    "-F",
                    f"file=@{audio_path}",
                ],
                check=True,
                capture_output=True,
                text=True,
            )
            payload = json.loads(result.stdout)
            transcript = normalize_space(str(payload.get("text", "")))
            if len(transcript) < 200:
                raise ValueError(f"OpenAI audio transcription too short for {audio_path}")
            write_text(cache_path, transcript)
            return transcript
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            time.sleep(min(2 ** attempt, 10))
    assert last_error is not None
    raise last_error


def hydrate_goldman_from_audio(record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    episode_id = record.get("megaphone_episode_id", "")
    if not episode_id:
        return None

    payload = fetch_megaphone_episode_payload(episode_id)
    episodes = payload.get("episodes") or []
    if not episodes:
        return None
    episode = episodes[0]
    audio_url = episode.get("audioUrl")
    if not audio_url:
        return None

    raw_audio_path = RAW_AUDIO_CACHE / f"{safe_filename(episode_id)}.mp3"
    fetch_url(
        str(audio_url),
        raw_audio_path,
        headers={"User-Agent": "Mozilla/5.0"},
        timeout=90,
        max_attempts=3,
    )
    upload_path = compress_audio_for_transcription(raw_audio_path)
    transcript_path = TRANSCRIPT_CACHE / f"{safe_filename(record['episode_url'].rstrip('/').split('/')[-1])}.txt"
    transcript = transcribe_audio_with_openai(upload_path, transcript_path)

    enriched = dict(record)
    enriched["summary"] = record.get("summary") or normalize_space(BeautifulSoup(str(episode.get("summary", "")), "lxml").get_text(" ", strip=True))
    enriched["transcript_text"] = transcript
    enriched["transcript_path"] = str(transcript_path)
    enriched["transcript_chars"] = len(transcript)
    enriched["transcript_source"] = "megaphone_audio_transcription"
    return enriched


def hydrate_goldman_transcript(record: Dict[str, Any], refresh: bool = False) -> Optional[Dict[str, Any]]:
    slug = record["episode_url"].rstrip("/").split("/")[-1]
    transcript_path = TRANSCRIPT_CACHE / f"{safe_filename(slug)}.txt"
    if transcript_path.exists() and not refresh:
        transcript = normalize_space(transcript_path.read_text(encoding="utf-8"))
        if len(transcript) >= 300:
            enriched = dict(record)
            enriched["transcript_text"] = transcript
            enriched["transcript_path"] = str(transcript_path)
            enriched["transcript_chars"] = len(transcript)
            enriched["transcript_source"] = "cache"
            return enriched

    prefers_audio = "/pdfs/insights/podcasts/episodes/" in record["transcript_url"]
    if not prefers_audio:
        try:
            pdf_payload = fetch_pdf(record["transcript_url"], referer=record["episode_url"], refresh=refresh)
            reader = PdfReader(io.BytesIO(pdf_payload))
            transcript_chunks: List[str] = []
            for page in reader.pages:
                text = page.extract_text() or ""
                if text.strip():
                    transcript_chunks.append(text)
            transcript = normalize_space("\n\n".join(transcript_chunks))
            if len(transcript) >= 300:
                write_text(transcript_path, transcript)
                enriched = dict(record)
                enriched["transcript_text"] = transcript
                enriched["transcript_path"] = str(transcript_path)
                enriched["transcript_chars"] = len(transcript)
                enriched["transcript_source"] = "pdf_transcript"
                return enriched
        except Exception:  # noqa: BLE001
            pass

    return hydrate_goldman_from_audio(record)


def hydrate_transcript(record: Dict[str, Any], refresh: bool = False) -> Optional[Dict[str, Any]]:
    if record["firm"] == "Morgan Stanley":
        return hydrate_morgan_transcript(record, refresh=refresh)
    return hydrate_goldman_transcript(record, refresh=refresh)


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
        if current is None or record.get("transcript_chars", 0) > current.get("transcript_chars", 0):
            best[key] = record
    return sorted(best.values(), key=lambda row: (row["podcast_date"], row["firm"], row["series"], row["title"]))


OPENAI_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "market_relevance": {
            "type": "string",
            "enum": ["high", "medium", "low", "none"],
        },
        "market_relevance_reason": {"type": "string"},
        "overall_summary": {"type": "string"},
        "claims": {
            "type": "array",
            "minItems": 0,
            "maxItems": 3,
            "items": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "claim": {"type": "string"},
                    "etf_action": {
                        "type": "string",
                        "enum": ["buy", "hold", "watch", "avoid", "hedge"],
                    },
                    "ticker": {"type": "string"},
                    "rationale": {"type": "string"},
                },
                "required": ["claim", "etf_action", "ticker", "rationale"],
            },
        },
    },
    "required": [
        "market_relevance",
        "market_relevance_reason",
        "overall_summary",
        "claims",
    ],
}


def response_output_text(payload: Dict[str, Any]) -> str:
    for item in payload.get("output", []):
        if item.get("type") != "message":
            continue
        for content in item.get("content", []):
            if content.get("type") == "output_text":
                return str(content.get("text", ""))
    if payload.get("output_text"):
        return str(payload["output_text"])
    raise ValueError("No output text found in OpenAI response")


def openai_prompt(record: Dict[str, Any]) -> str:
    summary = record.get("summary") or "(no page summary available)"
    transcript = record["transcript_text"]
    return (
        "Extract the top stock-market claims from this investment podcast episode.\n\n"
        "Rules:\n"
        "- Focus only on investable claims about equities, equity regions, sectors, styles, earnings, rates as they affect equities, or themes that clearly map to stock-market positioning.\n"
        "- Ignore housekeeping, brand language, and generic disclaimers.\n"
        "- Return 2-3 distinct claims when supported. Return fewer only if the episode truly has fewer defensible equity-related claims.\n"
        "- Each claim should be concise and specific.\n"
        "- Map each claim to one liquid US-listed ETF ticker when possible.\n"
        "- Use action labels exactly from: buy, hold, watch, avoid, hedge.\n"
        "- If the episode is not meaningfully about equities, return zero claims and set market_relevance to none.\n\n"
        f"Firm: {record['firm']}\n"
        f"Series: {record['series']}\n"
        f"Episode title: {record['title']}\n"
        f"Podcast date: {record['podcast_date']}\n"
        f"Episode URL: {record['episode_url']}\n"
        f"Page summary: {summary}\n\n"
        "Transcript:\n"
        f"{transcript}\n"
    )


def analyze_with_openai(
    record: Dict[str, Any],
    *,
    model: str,
    refresh: bool = False,
) -> Dict[str, Any]:
    cache_key = sha1_hex(record["episode_url"])
    cache_path = OPENAI_CACHE / f"{cache_key}.json"
    if cache_path.exists() and not refresh:
        cached = read_json(cache_path)
        assert isinstance(cached, dict)
        return cached

    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is not set")

    prompt = openai_prompt(record)
    payload = {
        "model": model,
        "reasoning": {"effort": "low"},
        "text": {
            "verbosity": "low",
            "format": {
                "type": "json_schema",
                "name": "podcast_claims",
                "schema": OPENAI_SCHEMA,
                "strict": True,
            },
        },
        "input": prompt,
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
    for attempt in range(5):
        try:
            with urllib.request.urlopen(request, timeout=180) as response:
                response_payload = json.load(response)
            text = response_output_text(response_payload)
            parsed = json.loads(text)
            write_json(cache_path, parsed)
            return parsed
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            sleep_seconds = min(2 ** attempt, 30)
            time.sleep(sleep_seconds)
    assert last_error is not None
    raise last_error


def attach_analysis(record: Dict[str, Any], analysis: Dict[str, Any]) -> Dict[str, Any]:
    claims = analysis.get("claims", [])
    cleaned_claims: List[Dict[str, str]] = []
    for claim in claims[:3]:
        if not isinstance(claim, dict):
            continue
        cleaned_claims.append(
            {
                "claim": normalize_space(str(claim.get("claim", ""))),
                "etf_action": normalize_space(str(claim.get("etf_action", "")).lower()),
                "ticker": normalize_space(str(claim.get("ticker", "")).upper()),
                "rationale": normalize_space(str(claim.get("rationale", ""))),
            }
        )

    joined_claims = "\n".join(
        f"{idx}. {item['claim']}" for idx, item in enumerate(cleaned_claims, start=1)
    )
    joined_actions = "\n".join(
        f"{idx}. {item['etf_action']} {item['ticker']}".strip()
        for idx, item in enumerate(cleaned_claims, start=1)
    )

    enriched = dict(record)
    enriched["market_relevance"] = analysis.get("market_relevance", "")
    enriched["market_relevance_reason"] = normalize_space(str(analysis.get("market_relevance_reason", "")))
    enriched["overall_summary"] = normalize_space(str(analysis.get("overall_summary", "")))
    enriched["claims_json"] = json.dumps(cleaned_claims, ensure_ascii=False)
    enriched["top_claims"] = joined_claims
    enriched["etf_actions"] = joined_actions

    for idx in range(1, 4):
        key_prefix = f"claim_{idx}"
        if idx <= len(cleaned_claims):
            item = cleaned_claims[idx - 1]
            enriched[key_prefix] = item["claim"]
            enriched[f"{key_prefix}_etf_action"] = item["etf_action"]
            enriched[f"{key_prefix}_ticker"] = item["ticker"]
            enriched[f"{key_prefix}_rationale"] = item["rationale"]
        else:
            enriched[key_prefix] = ""
            enriched[f"{key_prefix}_etf_action"] = ""
            enriched[f"{key_prefix}_ticker"] = ""
            enriched[f"{key_prefix}_rationale"] = ""
    return enriched


def collect_metadata_records(
    stubs: List[Dict[str, str]],
    *,
    start_date: date,
    end_date: date,
    max_episodes: Optional[int],
    refresh: bool,
    workers: int,
) -> List[Dict[str, Any]]:
    filtered_stubs = []
    skipped_by_hint = 0
    for stub in stubs:
        hinted_date = url_date_hint(stub["url"])
        if hinted_date and (hinted_date < start_date or hinted_date > end_date):
            skipped_by_hint += 1
            continue
        filtered_stubs.append(stub)

    if skipped_by_hint:
        log(f"[collect-metadata] prefiltered {skipped_by_hint} obvious out-of-range urls from slug dates")

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
            except Exception as exc:  # noqa: BLE001
                failures.append((stub["url"], str(exc)))
            if idx % 50 == 0:
                log(f"[collect-metadata] processed {idx}/{len(filtered_stubs)} candidates, kept {len(records)} in range, failures {len(failures)}")

    deduped = dedupe_records(records)
    if max_episodes:
        deduped = deduped[:max_episodes]
    write_json(MANIFEST_DIR / "collection_failures.json", failures)
    write_json(MANIFEST_DIR / "collected_metadata_records.json", deduped)
    return deduped[:max_episodes] if max_episodes else deduped


def hydrate_records(
    records: List[Dict[str, Any]],
    *,
    refresh: bool,
    workers: int,
) -> List[Dict[str, Any]]:
    hydrated: List[Dict[str, Any]] = []
    failures: List[Tuple[str, str]] = []
    with ThreadPoolExecutor(max_workers=workers) as executor:
        future_map = {executor.submit(hydrate_transcript, record, refresh): record for record in records}
        for idx, future in enumerate(as_completed(future_map), start=1):
            record = future_map[future]
            try:
                item = future.result()
                if item is None:
                    failures.append((record["episode_url"], "missing transcript"))
                else:
                    hydrated.append(item)
            except Exception as exc:  # noqa: BLE001
                failures.append((record["episode_url"], str(exc)))
            if idx % 25 == 0:
                log(f"[hydrate] completed {idx}/{len(records)} transcripts, failures {len(failures)}")
    hydrated.sort(key=lambda row: (row["podcast_date"], row["firm"], row["series"], row["title"]))
    write_json(MANIFEST_DIR / "transcript_failures.json", failures)
    write_json(MANIFEST_DIR / "transcript_records.json", hydrated)
    return hydrated


def analyze_records(
    records: List[Dict[str, Any]],
    *,
    model: str,
    refresh: bool,
    workers: int,
) -> List[Dict[str, Any]]:
    results: List[Dict[str, Any]] = []
    failures: List[Tuple[str, str]] = []
    with ThreadPoolExecutor(max_workers=workers) as executor:
        future_map = {
            executor.submit(analyze_with_openai, record, model=model, refresh=refresh): record
            for record in records
        }
        for idx, future in enumerate(as_completed(future_map), start=1):
            record = future_map[future]
            try:
                analysis = future.result()
                results.append(attach_analysis(record, analysis))
            except Exception as exc:  # noqa: BLE001
                failures.append((record["episode_url"], str(exc)))
            if idx % 20 == 0:
                log(f"[analyze] completed {idx}/{len(records)} OpenAI analyses, failures {len(failures)}")
    write_json(MANIFEST_DIR / "analysis_failures.json", failures)
    results.sort(key=lambda row: (row["podcast_date"], row["firm"], row["series"], row["title"]))
    return results


def repair_flagged_records(
    records: List[Dict[str, Any]],
    *,
    model: str,
    workers: int,
) -> List[Dict[str, Any]]:
    by_url = {record["episode_url"]: dict(record) for record in records}
    flagged_urls = []
    for record in records:
        claim_count = sum(1 for idx in range(1, 4) if record.get(f"claim_{idx}"))
        weak = (
            claim_count == 0
            or any(len(record.get(f"claim_{idx}", "")) < 20 for idx in range(1, 4) if record.get(f"claim_{idx}"))
            or any(not record.get(f"claim_{idx}_ticker", "") for idx in range(1, 4) if record.get(f"claim_{idx}"))
        )
        if weak:
            flagged_urls.append(record["episode_url"])

    if not flagged_urls:
        return records

    log(f"[repair] re-running {len(flagged_urls)} flagged episodes")
    raw_records = read_json(MANIFEST_DIR / "transcript_records.json") or []
    raw_by_url = {item["episode_url"]: item for item in raw_records}
    flagged_raw = [raw_by_url[url] for url in flagged_urls if url in raw_by_url]
    repaired = analyze_records(flagged_raw, model=model, refresh=True, workers=workers)
    for item in repaired:
        by_url[item["episode_url"]] = item
    repaired_records = sorted(by_url.values(), key=lambda row: (row["podcast_date"], row["firm"], row["series"], row["title"]))
    write_json(MANIFEST_DIR / "repaired_records.json", repaired_records)
    return repaired_records


def build_dataframe(records: List[Dict[str, Any]]) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    for record in records:
        row = {
            "firm": record["firm"],
            "series": record["series"],
            "podcast_date": record["podcast_date"],
            "title": record["title"],
            "episode_url": record["episode_url"],
            "transcript_url": record["transcript_url"],
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
            "transcript_source": record.get("transcript_source", ""),
            "transcript_path": record.get("transcript_path", ""),
        }
        rows.append(row)
    return pd.DataFrame(rows)


def export_outputs(records: List[Dict[str, Any]], start_date: date, end_date: date) -> Dict[str, str]:
    stamp = datetime.now().strftime("%Y-%m-%d")
    csv_path = ARTIFACT_ROOT / f"full_podcast_market_claim_table_{start_date.isoformat()}_{end_date.isoformat()}_{stamp}.csv"
    xlsx_path = ARTIFACT_ROOT / f"full_podcast_market_claim_table_{start_date.isoformat()}_{end_date.isoformat()}_{stamp}.xlsx"
    json_path = ARTIFACT_ROOT / f"full_podcast_market_claim_table_{start_date.isoformat()}_{end_date.isoformat()}_{stamp}.json"
    summary_path = ARTIFACT_ROOT / f"full_podcast_market_claim_summary_{start_date.isoformat()}_{end_date.isoformat()}_{stamp}.md"

    df = build_dataframe(records)
    df.to_csv(csv_path, index=False)
    df.to_excel(xlsx_path, index=False)

    payload = {
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "row_count": len(records),
        "series_counts": df.groupby(["firm", "series"]).size().reset_index(name="count").to_dict(orient="records"),
        "records": records,
    }
    write_json(json_path, payload)

    lines = [
        "# Full Podcast Market Claim Table",
        "",
        f"- Date range: {start_date.isoformat()} to {end_date.isoformat()}",
        f"- Rows: {len(records)}",
        "",
        "## Series Counts",
    ]
    for item in payload["series_counts"]:
        lines.append(f"- {item['firm']} | {item['series']}: {item['count']}")
    lines.extend(["", "## Sample Rows", ""])
    for sample in records[:10]:
        lines.append(f"### {sample['firm']} | {sample['series']} | {sample['title']}")
        lines.append(f"- Date: {sample['podcast_date']}")
        lines.append(f"- URL: {sample['episode_url']}")
        lines.append(f"- Claims: {sample.get('top_claims', '(none)')}")
        lines.append(f"- ETF actions: {sample.get('etf_actions', '(none)')}")
        lines.append("")
    write_text(summary_path, "\n".join(lines))

    return {
        "csv": str(csv_path),
        "xlsx": str(xlsx_path),
        "json": str(json_path),
        "summary_md": str(summary_path),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build the full podcast market-claim table.")
    parser.add_argument("--start-date", default=START_DEFAULT.isoformat())
    parser.add_argument("--end-date", default=END_DEFAULT.isoformat())
    parser.add_argument("--max-episodes", type=int, default=None)
    parser.add_argument("--collect-workers", type=int, default=8)
    parser.add_argument("--analysis-workers", type=int, default=4)
    parser.add_argument("--refresh-html", action="store_true")
    parser.add_argument("--refresh-openai", action="store_true")
    return parser.parse_args()


def main() -> int:
    ensure_dirs()
    load_env_file(ROOT / ".env")
    args = parse_args()
    start_date = parse_date_string(args.start_date)
    end_date = parse_date_string(args.end_date)
    if not start_date or not end_date:
        raise ValueError("Invalid start/end date")

    stubs = candidate_episode_stubs()
    log(f"[start] discovered {len(stubs)} candidate episode urls")
    metadata_records = collect_metadata_records(
        stubs,
        start_date=start_date,
        end_date=end_date,
        max_episodes=args.max_episodes,
        refresh=args.refresh_html,
        workers=args.collect_workers,
    )
    log(f"[collect-metadata] finished with {len(metadata_records)} in-range episode records")

    transcript_records = hydrate_records(
        metadata_records,
        refresh=args.refresh_html,
        workers=args.collect_workers,
    )
    log(f"[hydrate] finished with {len(transcript_records)} transcript records")

    model = os.environ.get("ALMA_OPENAI_MODEL", "gpt-5-mini")
    analyzed = analyze_records(
        transcript_records,
        model=model,
        refresh=args.refresh_openai,
        workers=args.analysis_workers,
    )
    log(f"[analyze] initial analysis produced {len(analyzed)} rows")

    repaired = repair_flagged_records(
        analyzed,
        model=model,
        workers=max(1, min(args.analysis_workers, 2)),
    )
    log(f"[repair] final analyzed row count {len(repaired)}")

    outputs = export_outputs(repaired, start_date, end_date)
    write_json(MANIFEST_DIR / "output_paths.json", outputs)
    log("[done] outputs")
    for label, path in outputs.items():
        log(f"  - {label}: {path}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        log("Interrupted")
        raise
