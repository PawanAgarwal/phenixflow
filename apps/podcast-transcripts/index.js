'use strict';

/**
 * Podcast Transcript Downloader
 *
 * Usage:
 *   node index.js [options]
 *
 * Options:
 *   --years <n>       How many years back to download (default: 3)
 *   --podcast <id>    Download only this podcast (default: all)
 *                     Use the `id` field from config/podcasts.js
 *   --headless        Run browser in headless mode (default: true)
 *   --visible         Run browser in visible (non-headless) mode for debugging
 *
 * Examples:
 *   node index.js --years 3
 *   node index.js --years 1 --podcast morgan-stanley
 *   node index.js --podcast goldman-sachs --visible
 */

const { launchBrowser } = require('./lib/browser');
const store = require('./lib/store');
const PODCASTS = require('./config/podcasts');

function parseArgs() {
  const args = process.argv.slice(2);
  const opts = { years: 3, podcast: null, headless: true };

  for (let i = 0; i < args.length; i++) {
    if (args[i] === '--years' && args[i + 1]) opts.years = parseInt(args[++i], 10);
    else if (args[i] === '--podcast' && args[i + 1]) opts.podcast = args[++i];
    else if (args[i] === '--visible') opts.headless = false;
    else if (args[i] === '--headless') opts.headless = true;
  }

  return opts;
}

function makeLogger(prefix) {
  return (...args) => console.log(`[${prefix}]`, ...args);
}

async function run() {
  const opts = parseArgs();

  let podcasts = PODCASTS;
  if (opts.podcast) {
    podcasts = PODCASTS.filter((p) => p.id === opts.podcast);
    if (podcasts.length === 0) {
      console.error(`Unknown podcast id: "${opts.podcast}"`);
      console.error(`Available: ${PODCASTS.map((p) => p.id).join(', ')}`);
      process.exit(1);
    }
  }

  console.log(`\nPodcast Transcript Downloader`);
  console.log(`  Years back : ${opts.years}`);
  console.log(`  Podcasts   : ${podcasts.map((p) => p.id).join(', ')}`);
  console.log(`  Headless   : ${opts.headless}\n`);

  const { browser, context } = await launchBrowser({ headless: opts.headless });

  const results = {};

  try {
    for (const podcast of podcasts) {
      const log = makeLogger(podcast.id);
      log(`Starting — "${podcast.name}" by ${podcast.publisher}`);

      let scraper;
      try {
        scraper = require(`./scrapers/${podcast.scraper}`);
      } catch (e) {
        log(`✗ Scraper not found: scrapers/${podcast.scraper}.js`);
        continue;
      }

      try {
        // Pass context only if the scraper needs a browser (scrapers that don't use
        // Playwright simply ignore it via destructuring)
        const result = await scraper.scrape({
          yearsBack: opts.years,
          context,
          store,
          log,
        });
        results[podcast.id] = result;
      } catch (err) {
        log(`✗ Scraper failed: ${err.message}`);
        console.error(err);
      }

      const stats = store.getStats(podcast.id);
      log(`Total in store: ${stats.total} episodes\n`);
    }
  } finally {
    await browser.close();
  }

  // Summary
  console.log('\n=== Summary ===');
  for (const podcast of podcasts) {
    const r = results[podcast.id] || { downloaded: 0, skipped: 0 };
    const stats = store.getStats(podcast.id);
    console.log(
      `${podcast.id}: +${r.downloaded} new, ${r.skipped} already had, ${stats.total} total on disk`
    );
  }
  console.log('Transcripts saved to: apps/podcast-transcripts/data/\n');
}

run().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
