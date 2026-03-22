'use strict';

/**
 * Goldman Sachs "Exchanges at Goldman Sachs" scraper
 *
 * Episode listing: Podbean API (paginated JSON)
 *   https://www.podbean.com/podcast-detail/episode-list?pid=dir216329&page=N&pageSize=20
 *
 * Transcript: Each Podbean episode page contains a text transcript.
 * The official GS site (goldmansachs.com) also hosts PDF transcripts but at
 * unpredictable URL slugs — Podbean text transcripts are more reliable.
 */

const PODBEAN_API = 'https://www.podbean.com/podcast-detail/episode-list';
const PODCAST_ID_PODBEAN = 'dir216329';
const PAGE_SIZE = 20;

function slugify(str) {
  return str
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .slice(0, 80);
}

function parseDate(raw) {
  if (!raw) return null;
  const d = new Date(raw.trim());
  return isNaN(d.getTime()) ? null : d;
}

/**
 * Fetch one page of episode metadata from the Podbean API via the browser
 * (avoids CORS issues and uses the browser's session/cookies).
 */
async function fetchEpisodePage(page, pageNum) {
  const url = `${PODBEAN_API}?pid=${PODCAST_ID_PODBEAN}&page=${pageNum}&pageSize=${PAGE_SIZE}`;

  const response = await page.evaluate(async (fetchUrl) => {
    const res = await fetch(fetchUrl, {
      headers: { 'X-Requested-With': 'XMLHttpRequest' },
    });
    return res.text();
  }, url);

  return response;
}

/**
 * Parse episode cards from the Podbean API HTML response.
 * Returns array of { url, title, dateText }
 */
function parseEpisodeCards(html) {
  // Podbean returns HTML fragments for the episode list
  // We extract hrefs and titles using regex (no DOM available here)
  const episodes = [];

  // Match episode links: /ew/dir-XXXXXX-YYYYYY/...
  const linkRe = /href="(https?:\/\/www\.podbean\.com\/ew\/[^"]+)"/g;
  const titleRe = /<a[^>]*href="https?:\/\/www\.podbean\.com\/ew\/[^"]*"[^>]*>([^<]+)<\/a>/g;
  const dateRe = /(\w+ \d{1,2},\s*\d{4})/g;

  const links = [];
  let m;
  while ((m = linkRe.exec(html)) !== null) {
    if (!links.includes(m[1])) links.push(m[1]);
  }

  const titles = [];
  while ((m = titleRe.exec(html)) !== null) {
    const t = m[1].trim();
    if (t && t.length > 3) titles.push(t);
  }

  const dates = [];
  while ((m = dateRe.exec(html)) !== null) {
    dates.push(m[1]);
  }

  for (let i = 0; i < links.length; i++) {
    episodes.push({
      url: links[i],
      title: titles[i] || '',
      dateText: dates[i] || null,
    });
  }

  return episodes;
}

/**
 * Scrape transcript text from a Podbean episode page.
 */
async function extractTranscript(page, url, log) {
  await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 30000 });

  const transcript = await page.evaluate(() => {
    // Podbean episode pages include a "Transcript" section
    const transcriptSection = document.querySelector(
      '[class*="transcript" i], [id*="transcript" i], .episode-transcript'
    );
    if (transcriptSection) return transcriptSection.innerText.trim();

    // Look for a heading with "Transcript"
    const headings = Array.from(document.querySelectorAll('h2, h3, h4'));
    for (const h of headings) {
      if (/transcript/i.test(h.textContent)) {
        let text = '';
        let el = h.nextElementSibling;
        while (el && !['H2', 'H3', 'H4'].includes(el.tagName)) {
          text += el.innerText + '\n';
          el = el.nextElementSibling;
        }
        if (text.length > 100) return text.trim();
      }
    }

    // Fallback: grab episode description / show notes which often contain the transcript
    const desc = document.querySelector(
      '.episode-description, .show-notes, [class*="description" i], [class*="content" i]'
    );
    return desc ? desc.innerText.trim() : '';
  });

  return transcript;
}

/**
 * Main scrape function.
 */
async function scrape({ yearsBack, context, store, log }) {
  const podcastId = 'goldman-sachs';
  const cutoffDate = new Date();
  cutoffDate.setFullYear(cutoffDate.getFullYear() - yearsBack);

  log(`Scraping Goldman Sachs — cutoff: ${cutoffDate.toISOString().slice(0, 10)}`);

  const page = await context.newPage();

  // First navigate to Podbean to establish session/cookies
  await page.goto('https://www.podbean.com', { waitUntil: 'domcontentloaded', timeout: 30000 });

  // Collect episodes across pages until we pass the cutoff date
  const allEpisodes = [];
  let pageNum = 1;
  let reachedCutoff = false;

  log('Collecting episode listing from Podbean API...');

  while (!reachedCutoff) {
    log(`  Fetching listing page ${pageNum}...`);
    const html = await fetchEpisodePage(page, pageNum);

    if (!html || html.trim() === '' || html.includes('"episodes":[]')) {
      log('  No more episodes returned — end of list');
      break;
    }

    const episodes = parseEpisodeCards(html);
    if (episodes.length === 0) {
      log('  Could not parse episodes from response — stopping');
      break;
    }

    for (const ep of episodes) {
      const date = parseDate(ep.dateText);
      if (date && date < cutoffDate) {
        reachedCutoff = true;
        break;
      }
      allEpisodes.push({ ...ep, date });
    }

    pageNum++;
    await page.waitForTimeout(1000);
  }

  log(`Found ${allEpisodes.length} episodes within date range`);

  let downloaded = 0;
  let skipped = 0;

  for (const ep of allEpisodes) {
    const dateStr = ep.date ? ep.date.toISOString().slice(0, 10) : 'unknown';
    const id = slugify(ep.title) || slugify(ep.url.split('/').pop());

    if (store.isDownloaded(podcastId, id)) {
      skipped++;
      continue;
    }

    log(`Downloading: [${dateStr}] ${ep.title || ep.url}`);

    try {
      const transcript = await extractTranscript(page, ep.url, log);

      if (!transcript || transcript.length < 50) {
        log(`  ⚠ No transcript found at ${ep.url}`);
        continue;
      }

      store.saveEpisode(podcastId, {
        id,
        title: ep.title,
        date: dateStr,
        url: ep.url,
        transcript,
      });

      downloaded++;
      await page.waitForTimeout(1500);
    } catch (err) {
      log(`  ✗ Failed ${ep.url}: ${err.message}`);
    }
  }

  await page.close();
  log(`Goldman Sachs done — downloaded: ${downloaded}, skipped (already had): ${skipped}`);
  return { downloaded, skipped };
}

module.exports = { scrape };
