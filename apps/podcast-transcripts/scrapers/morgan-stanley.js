'use strict';

/**
 * Morgan Stanley "Thoughts on the Market" scraper
 *
 * Source: Art19 RSS feed — https://rss.art19.com/thoughts-on-the-market
 *
 * The RSS `content:encoded` field contains the full episode transcript
 * (after a "Transcript" separator). No browser automation needed.
 *
 * ~1587 total episodes; ~756 in a 3-year window (published Mon–Fri).
 */

const { fetchRss } = require('../lib/rss');

const RSS_URL = 'https://rss.art19.com/thoughts-on-the-market';

function slugify(str) {
  return str
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .slice(0, 80);
}

/**
 * Extract and clean transcript text from a content:encoded HTML blob.
 * The blob contains the show summary followed by "Transcript" then the dialogue.
 */
function parseTranscript(contentEncoded) {
  if (!contentEncoded) return '';

  // Strip CDATA wrapper
  let html = contentEncoded.replace(/^<!\[CDATA\[/, '').replace(/\]\]>$/, '');

  // Replace <br>, <p>, </p>, <li> etc. with newlines before stripping tags
  html = html.replace(/<br\s*\/?>/gi, '\n').replace(/<\/p>/gi, '\n').replace(/<\/li>/gi, '\n');

  // Decode common HTML entities
  const text = html
    .replace(/<[^>]+>/g, '')
    .replace(/&amp;/g, '&')
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&nbsp;/g, ' ')
    .replace(/&#39;/g, "'")
    .replace(/&quot;/g, '"')
    .replace(/\r\n/g, '\n')
    .replace(/[ \t]+/g, ' ')
    .replace(/\n{3,}/g, '\n\n')
    .trim();

  // Extract the transcript section (everything after the "Transcript" separator line)
  const separatorMatch = /[-–—]{3,}\s*Transcript\s*[-–—]{3,}/i.exec(text);
  if (separatorMatch) {
    return text.slice(separatorMatch.index + separatorMatch[0].length).trim();
  }

  // Fallback: if "Transcript" appears as a standalone label, take everything after it
  const labelMatch = /\bTranscript\b[\s:–-]*/i.exec(text);
  if (labelMatch && labelMatch.index > 50) {
    // Only use if label is not at the very start (that would mean no separator pattern)
    return text.slice(labelMatch.index + labelMatch[0].length).trim();
  }

  return text;
}

async function scrape({ yearsBack, store, log }) {
  const podcastId = 'morgan-stanley';
  const cutoffDate = new Date();
  cutoffDate.setFullYear(cutoffDate.getFullYear() - yearsBack);

  log(`Fetching Art19 RSS — cutoff: ${cutoffDate.toISOString().slice(0, 10)}`);

  const allEpisodes = await fetchRss(RSS_URL);
  const episodes = allEpisodes.filter((ep) => ep.date && ep.date >= cutoffDate);

  log(`${allEpisodes.length} total episodes in feed; ${episodes.length} within ${yearsBack}-year window`);

  let downloaded = 0;
  let skipped = 0;
  let noTranscript = 0;

  for (const ep of episodes) {
    const dateStr = ep.date.toISOString().slice(0, 10);
    const id = slugify(ep.title);

    if (store.isDownloaded(podcastId, id)) {
      skipped++;
      continue;
    }

    const transcript = parseTranscript(ep.rawContentEncoded || ep.description);

    if (!transcript || transcript.length < 80) {
      log(`[${dateStr}] ⚠ No transcript: ${ep.title}`);
      noTranscript++;
      continue;
    }

    store.saveEpisode(podcastId, {
      id,
      title: ep.title,
      date: dateStr,
      url: `https://www.morganstanley.com/insights/podcasts/thoughts-on-the-market/${id}`,
      source: 'rss-transcript',
      transcript,
    });

    downloaded++;
    if (downloaded % 50 === 0) log(`  ... ${downloaded} downloaded so far`);
  }

  log(`Done — downloaded: ${downloaded}, skipped: ${skipped}, no transcript: ${noTranscript}`);
  return { downloaded, skipped, noTranscript };
}

module.exports = { scrape };
