/**
 * Lightweight Yahoo Finance HTTP client using the public v8 chart API.
 * No external dependencies — uses Node.js built-in fetch.
 *
 * Uses query2.finance.yahoo.com (no cookie/crumb required for chart data).
 */

const CHART_URL = 'https://query2.finance.yahoo.com/v8/finance/chart';
const SUMMARY_URL = 'https://query2.finance.yahoo.com/v10/finance/quoteSummary';

const HEADERS = {
  'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
  'Accept': 'application/json',
};

const MAX_RETRIES = 2;
const RETRY_DELAY_MS = 1500;

/**
 * Fetch with retry logic for transient failures.
 */
async function fetchWithRetry(url, opts = {}, retries = MAX_RETRIES) {
  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      const res = await fetch(url, { headers: HEADERS, ...opts });
      if (res.status === 429 && attempt < retries) {
        // Rate limited — back off
        await sleep(RETRY_DELAY_MS * (attempt + 1));
        continue;
      }
      return res;
    } catch (err) {
      if (attempt < retries) {
        await sleep(RETRY_DELAY_MS * (attempt + 1));
        continue;
      }
      throw err;
    }
  }
}

/**
 * Fetch weekly chart data for a symbol.
 * @param {string} symbol - Ticker symbol
 * @param {object} opts
 * @param {string} opts.range - Time range (e.g., '5y', '10y')
 * @param {string} opts.interval - Data interval (e.g., '1wk', '1d')
 * @returns {Promise<Array<{date: Date, close: number}>|null>}
 */
export async function fetchChart(symbol, opts = {}) {
  const { range = '5y', interval = '1wk' } = opts;

  const url = `${CHART_URL}/${encodeURIComponent(symbol)}?range=${range}&interval=${interval}&includePrePost=false`;

  const res = await fetchWithRetry(url);
  if (!res.ok) return null;

  const data = await res.json();
  const result = data?.chart?.result?.[0];
  if (!result) return null;

  const timestamps = result.timestamp || [];
  const closes = result.indicators?.quote?.[0]?.close || [];

  // Pair timestamps with closes, filter nulls
  const pairs = [];
  for (let i = 0; i < timestamps.length; i++) {
    if (closes[i] != null) {
      pairs.push({ date: new Date(timestamps[i] * 1000), close: closes[i] });
    }
  }

  return pairs;
}

/**
 * Fetch quote summary (fundamentals) for a symbol.
 * @param {string} symbol
 * @returns {Promise<object|null>}
 */
export async function fetchQuoteSummary(symbol) {
  const modules = 'summaryDetail,defaultKeyStatistics,financialData,price';
  const url = `${SUMMARY_URL}/${encodeURIComponent(symbol)}?modules=${modules}`;

  const res = await fetchWithRetry(url);
  if (!res.ok) return null;

  const data = await res.json();
  const result = data?.quoteSummary?.result?.[0];
  return result || null;
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}
