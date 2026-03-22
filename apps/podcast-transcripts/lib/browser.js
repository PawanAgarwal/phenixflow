'use strict';

const { chromium } = require('playwright');

async function launchBrowser({ headless = true } = {}) {
  const browser = await chromium.launch({
    headless,
    args: ['--no-sandbox', '--disable-dev-shm-usage'],
  });

  const context = await browser.newContext({
    userAgent:
      'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 ' +
      '(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    viewport: { width: 1280, height: 900 },
    locale: 'en-US',
  });

  // Abort unnecessary resource types to speed up scraping
  await context.route('**/*', (route) => {
    const blocked = ['image', 'media', 'font', 'stylesheet'];
    if (blocked.includes(route.request().resourceType())) {
      route.abort();
    } else {
      route.continue();
    }
  });

  return { browser, context };
}

module.exports = { launchBrowser };
