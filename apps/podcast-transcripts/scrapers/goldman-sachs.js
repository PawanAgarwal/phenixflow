'use strict';

/**
 * Goldman Sachs "Exchanges at Goldman Sachs" scraper
 *
 * Episode listing: Megaphone RSS feed — https://feeds.megaphone.fm/GLD9218176758
 *   Provides titles, dates, and descriptions for all 608 episodes.
 *
 * Transcripts: GS publishes a PDF transcript for each episode at:
 *   https://www.goldmansachs.com/pdfs/insights/goldman-sachs-exchanges/{slug}/transcript.pdf
 *
 *   The PDF URL requires browser cookies from the GS website to access.
 *   Strategy: visit the episode page, then fetch the PDF in-page (uses cookies),
 *   then parse the PDF buffer with pdf-parse.
 *
 * Fallback: RSS description (~2000 chars) when PDF is unavailable.
 *
 * ~608 total episodes; ~209 in a 3-year window.
 */

const { fetchRss } = require('../lib/rss');
const pdfParse = require('pdf-parse');

const RSS_URL = 'https://feeds.megaphone.fm/GLD9218176758';
const GS_BASE = 'https://www.goldmansachs.com';
const GS_EPISODE_BASE = `${GS_BASE}/insights/goldman-sachs-exchanges`;
const GS_PDF_BASE = `${GS_BASE}/pdfs/insights/goldman-sachs-exchanges`;

function slugify(str) {
  return str
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .slice(0, 100);
}

function stripHtml(html) {
  return html
    .replace(/<br\s*\/?>/gi, '\n')
    .replace(/<\/p>/gi, '\n')
    .replace(/<[^>]+>/g, '')
    .replace(/&amp;/g, '&')
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&nbsp;/g, ' ')
    .replace(/&#39;/g, "'")
    .replace(/&quot;/g, '"')
    .replace(/[ \t]+/g, ' ')
    .replace(/\n{3,}/g, '\n\n')
    .trim();
}

/**
 * Visit the GS episode page (to get cookies) then fetch and parse the transcript PDF.
 * Returns the transcript text or null.
 */
async function fetchTranscriptPdf(page, episodeSlug) {
  const epUrl = `${GS_EPISODE_BASE}/${episodeSlug}`;
  const pdfUrl = `${GS_PDF_BASE}/${episodeSlug}/transcript.pdf`;

  // Visit episode page first (establishes session cookies)
  await page.goto(epUrl, { waitUntil: 'domcontentloaded', timeout: 30000 });
  await page.waitForTimeout(2000);

  // Fetch PDF in-page using browser's cookie context
  const pdfBuffer = await page.evaluate(async (url) => {
    const res = await fetch(url);
    if (!res.ok || !res.headers.get('content-type')?.includes('pdf')) return null;
    const buf = await res.arrayBuffer();
    // Chunk the conversion to avoid exceeding the V8 call stack spread limit (~65k args)
    const uint8 = new Uint8Array(buf);
    let binary = '';
    const chunkSize = 8192;
    for (let i = 0; i < uint8.length; i += chunkSize) {
      binary += String.fromCharCode(...uint8.subarray(i, i + chunkSize));
    }
    return btoa(binary);
  }, pdfUrl);

  if (!pdfBuffer) return null;

  // Decode base64 → Buffer → parse PDF
  const buffer = Buffer.from(pdfBuffer, 'base64');
  const parsed = await pdfParse(buffer);
  return parsed.text ? parsed.text.trim() : null;
}

async function scrape({ yearsBack, context, store, log }) {
  const podcastId = 'goldman-sachs';
  const cutoffDate = new Date();
  cutoffDate.setFullYear(cutoffDate.getFullYear() - yearsBack);

  log(`Fetching Megaphone RSS — cutoff: ${cutoffDate.toISOString().slice(0, 10)}`);

  const allEpisodes = await fetchRss(RSS_URL);
  const episodes = allEpisodes.filter((ep) => ep.date && ep.date >= cutoffDate);

  log(`${allEpisodes.length} total episodes in feed; ${episodes.length} within ${yearsBack}-year window`);

  const page = await context.newPage();

  // Prime the browser session by visiting the GS domain once
  try {
    await page.goto('https://www.goldmansachs.com', { waitUntil: 'domcontentloaded', timeout: 20000 });
    await page.waitForTimeout(2000);
  } catch (_) {}

  let downloaded = 0;
  let skipped = 0;
  let usedRssDesc = 0;
  let noContent = 0;

  for (const ep of episodes) {
    const dateStr = ep.date.toISOString().slice(0, 10);
    const id = slugify(ep.title);

    if (store.isDownloaded(podcastId, id)) {
      skipped++;
      continue;
    }

    log(`[${dateStr}] ${ep.title}`);

    let transcript = '';
    let source = '';

    // Try PDF transcript first
    try {
      const pdfText = await fetchTranscriptPdf(page, id);
      if (pdfText && pdfText.length > 200) {
        transcript = pdfText;
        source = 'pdf-transcript';
        log(`  ✓ PDF transcript (${transcript.length} chars)`);
      }
    } catch (err) {
      log(`  ⚠ PDF error: ${err.message.split('\n')[0]}`);
    }

    await page.waitForTimeout(1500); // polite delay

    // Fallback: RSS description (detailed 2–3 paragraph summary)
    if (!transcript && ep.description) {
      transcript = stripHtml(ep.description);
      if (transcript.length > 100) {
        source = 'rss-description';
        log(`  ↩ Using RSS description (${transcript.length} chars)`);
        usedRssDesc++;
      }
    }

    if (!transcript || transcript.length < 50) {
      log(`  ⚠ No content found — skipping`);
      noContent++;
      continue;
    }

    store.saveEpisode(podcastId, {
      id,
      title: ep.title,
      date: dateStr,
      url: `${GS_EPISODE_BASE}/${id}`,
      source,
      transcript,
    });

    downloaded++;
  }

  await page.close();
  log(`Done — downloaded: ${downloaded}, skipped: ${skipped}, used-pdf: ${downloaded - usedRssDesc}, used-rss-desc: ${usedRssDesc}, no-content: ${noContent}`);
  return { downloaded, skipped, usedRssDesc, noContent };
}

module.exports = { scrape };
