'use strict';

/**
 * Podcast registry.
 * To add a new podcast:
 *   1. Add an entry here with a unique `id` and a `scraper` name
 *   2. Create apps/podcast-transcripts/scrapers/<scraper>.js
 *      exporting: async function scrape({ yearsBack, browser, store, log })
 */
module.exports = [
  {
    id: 'morgan-stanley',
    name: 'Thoughts on the Market',
    publisher: 'Morgan Stanley',
    scraper: 'morgan-stanley',
  },
  {
    id: 'goldman-sachs',
    name: 'Exchanges at Goldman Sachs',
    publisher: 'Goldman Sachs',
    scraper: 'goldman-sachs',
  },
];
