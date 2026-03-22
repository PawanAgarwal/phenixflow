'use strict';

/**
 * Morgan Stanley "Thoughts on the Market" scraper
 * Source: https://www.morganstanley.com/insights/podcasts/thoughts-on-the-market
 *
 * Episodes are listed on the main page. The page uses a "Load More" button to
 * paginate. Each episode detail page contains the full transcript in HTML.
 */

const LISTING_URL = 'https://www.morganstanley.com/insights/podcasts/thoughts-on-the-market';
const BASE_URL = 'https://www.morganstanley.com';

// Selectors — update here if the site redesigns
const SELECTORS = {
  episodeCard: '[class*="podcast-episode"], [class*="PodcastCard"], article[class*="podcast"]',
  episodeLink: 'a[href*="thoughts-on-the-market"]',
  episodeDate: 'time, [class*="date"], [class*="Date"]',
  loadMore: 'button[class*="load-more"], button[class*="LoadMore"], [aria-label*="load more" i], [class*="load-more"] button',
  transcriptContainer: '[class*="transcript"], [class*="Transcript"], [id*="transcript"]',
  transcriptToggle: 'button[class*="transcript"], a[class*="transcript"], [aria-controls*="transcript"]',
};

/**
 * Parse a date string into a Date object. Handles various formats.
 */
function parseDate(raw) {
  if (!raw) return null;
  const d = new Date(raw.trim());
  return isNaN(d.getTime()) ? null : d;
}

function slugify(str) {
  return str
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .slice(0, 80);
}

/**
 * Collect episode links from the listing page, paginating until cutoffDate.
 */
async function collectEpisodeLinks(page, cutoffDate, log) {
  await page.goto(LISTING_URL, { waitUntil: 'domcontentloaded', timeout: 30000 });

  const episodes = [];
  let iteration = 0;
  const MAX_ITERATIONS = 200; // safety cap

  while (iteration < MAX_ITERATIONS) {
    iteration++;

    // Collect all episode cards currently on page
    const cards = await page.evaluate((selectors) => {
      const results = [];

      // Try multiple selector strategies for episode cards
      const links = Array.from(document.querySelectorAll('a[href]')).filter(
        (a) =>
          a.href.includes('thoughts-on-the-market') &&
          !a.href.endsWith('thoughts-on-the-market') &&
          !a.href.endsWith('thoughts-on-the-market/')
      );

      for (const link of links) {
        // Walk up to find the card container and date
        let container = link;
        let dateText = null;
        for (let i = 0; i < 6; i++) {
          container = container.parentElement;
          if (!container) break;
          const timeEl = container.querySelector('time');
          if (timeEl) {
            dateText = timeEl.getAttribute('datetime') || timeEl.textContent.trim();
            break;
          }
          const dateEl = container.querySelector('[class*="date" i], [class*="Date"]');
          if (dateEl) {
            dateText = dateEl.textContent.trim();
            break;
          }
        }

        // Avoid duplicates
        if (!results.find((r) => r.url === link.href)) {
          results.push({
            url: link.href,
            title: link.textContent.trim() || link.getAttribute('aria-label') || '',
            dateText,
          });
        }
      }

      return results;
    }, SELECTORS);

    let reachedCutoff = false;
    for (const card of cards) {
      const date = parseDate(card.dateText);
      if (date && date < cutoffDate) {
        reachedCutoff = true;
        continue;
      }
      if (!episodes.find((e) => e.url === card.url)) {
        episodes.push({ url: card.url, title: card.title, date });
      }
    }

    if (reachedCutoff) break;

    // Try to click Load More
    const loadMoreBtn = await page.$(SELECTORS.loadMore);
    if (!loadMoreBtn) {
      log('No "Load More" button found — may have reached the end of listing');
      break;
    }

    const prevCount = cards.length;
    await loadMoreBtn.click();
    await page.waitForTimeout(2000);

    const newCount = await page.evaluate((sel) => {
      return Array.from(document.querySelectorAll('a[href]')).filter(
        (a) =>
          a.href.includes('thoughts-on-the-market') &&
          !a.href.endsWith('thoughts-on-the-market') &&
          !a.href.endsWith('thoughts-on-the-market/')
      ).length;
    }, SELECTORS);

    if (newCount <= prevCount) {
      log('No new episodes loaded after clicking Load More — stopping pagination');
      break;
    }
    log(`Loaded more: ${newCount} episodes found so far`);
  }

  return episodes;
}

/**
 * Extract transcript text from an episode detail page.
 */
async function extractTranscript(page, url, log) {
  await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 30000 });

  // Some pages have a toggle to expand the transcript — click it if present
  const toggle = await page.$(SELECTORS.transcriptToggle);
  if (toggle) {
    await toggle.click();
    await page.waitForTimeout(1000);
  }

  const transcript = await page.evaluate((selectors) => {
    // Try named transcript containers first
    const containers = document.querySelectorAll(selectors.transcriptContainer);
    if (containers.length > 0) {
      return Array.from(containers)
        .map((el) => el.innerText.trim())
        .join('\n\n');
    }

    // Fallback: look for a section with the word "transcript" in a heading
    const headings = Array.from(document.querySelectorAll('h2, h3, h4, h5'));
    for (const h of headings) {
      if (/transcript/i.test(h.textContent)) {
        // Collect all sibling/following text until next heading
        let text = '';
        let el = h.nextElementSibling;
        while (el && !['H2', 'H3', 'H4', 'H5'].includes(el.tagName)) {
          text += el.innerText + '\n';
          el = el.nextElementSibling;
        }
        if (text.length > 100) return text.trim();
      }
    }

    // Last resort: grab the main article body
    const article = document.querySelector('article, main, [class*="content"], [class*="body"]');
    return article ? article.innerText.trim() : '';
  }, SELECTORS);

  return transcript;
}

/**
 * Main scrape function.
 * @param {object} opts
 * @param {number} opts.yearsBack
 * @param {object} opts.context - Playwright browser context
 * @param {object} opts.store
 * @param {function} opts.log
 */
async function scrape({ yearsBack, context, store, log }) {
  const podcastId = 'morgan-stanley';
  const cutoffDate = new Date();
  cutoffDate.setFullYear(cutoffDate.getFullYear() - yearsBack);

  log(`Scraping Morgan Stanley — cutoff: ${cutoffDate.toISOString().slice(0, 10)}`);

  const page = await context.newPage();

  log('Collecting episode listing...');
  const episodes = await collectEpisodeLinks(page, cutoffDate, log);
  log(`Found ${episodes.length} episodes within date range`);

  let downloaded = 0;
  let skipped = 0;

  for (const ep of episodes) {
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
      // Polite delay between requests
      await page.waitForTimeout(1500);
    } catch (err) {
      log(`  ✗ Failed to download ${ep.url}: ${err.message}`);
    }
  }

  await page.close();
  log(`Morgan Stanley done — downloaded: ${downloaded}, skipped (already had): ${skipped}`);
  return { downloaded, skipped };
}

module.exports = { scrape };
