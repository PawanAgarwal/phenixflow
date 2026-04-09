/**
 * 200-Week Moving Average Screener
 *
 * "If all you ever did was buy high-quality stocks on the 200-week moving
 * average, you would beat the S&P 500 by a large margin."
 *
 * This module fetches weekly price data from Yahoo Finance and identifies
 * stocks trading at or near their 200-week simple moving average (SMA).
 */

import { fetchChart } from './yahoo-client.js';

const WEEKS_200 = 200;

/**
 * Calculate Simple Moving Average from an array of closing prices.
 * @param {number[]} prices - Array of closing prices (oldest first)
 * @param {number} period - SMA period
 * @returns {number|null} - The SMA value, or null if insufficient data
 */
export function calculateSMA(prices, period) {
  if (prices.length < period) return null;
  const slice = prices.slice(prices.length - period);
  return slice.reduce((sum, p) => sum + p, 0) / period;
}

/**
 * Fetch weekly historical data and compute 200-week SMA for a ticker.
 *
 * @param {string} symbol
 * @returns {Promise<object|null>} Result object or null on error
 */
async function analyzeStock(symbol) {
  try {
    const data = await fetchChart(symbol, { range: '5y', interval: '1wk' });
    if (!data || data.length < WEEKS_200) {
      return null;
    }

    const closes = data.map(d => d.close);
    const currentPrice = closes[closes.length - 1];
    const sma200w = calculateSMA(closes, WEEKS_200);

    if (!sma200w || !currentPrice) return null;

    // Distance from 200-week MA as a percentage
    const distancePct = ((currentPrice - sma200w) / sma200w) * 100;

    // 50-week SMA for trend context
    const sma50w = calculateSMA(closes, 50);

    // 52-week high/low (last 52 data points)
    const last52 = closes.slice(-52);
    const high52w = Math.max(...last52);
    const low52w = Math.min(...last52);
    const fromHigh52Pct = ((currentPrice - high52w) / high52w) * 100;

    // Price trend: is price above or below 50w SMA?
    const trendLabel = sma50w
      ? (currentPrice > sma50w ? 'above_50w' : 'below_50w')
      : 'unknown';

    // Was the stock recently above 200w SMA and now converging? (momentum shift)
    const priorClose = closes.length >= 5 ? closes[closes.length - 5] : null;
    const wasAbove = priorClose && sma200w ? priorClose > sma200w : null;
    const isNowBelow = currentPrice < sma200w;
    const convergingFromAbove = wasAbove && isNowBelow;

    return {
      symbol,
      currentPrice: round(currentPrice, 2),
      sma200w: round(sma200w, 2),
      distancePct: round(distancePct, 2),
      absDistancePct: round(Math.abs(distancePct), 2),
      sma50w: sma50w ? round(sma50w, 2) : null,
      trend: trendLabel,
      high52w: round(high52w, 2),
      low52w: round(low52w, 2),
      fromHigh52Pct: round(fromHigh52Pct, 2),
      convergingFromAbove,
      weeksOfData: data.length,
      lastDate: data[data.length - 1].date,
    };
  } catch (_err) {
    return null;
  }
}

/**
 * Screen a list of tickers against the 200-week MA.
 *
 * @param {string[]} tickers - List of ticker symbols
 * @param {object} opts
 * @param {number} opts.maxDistancePct - Max distance from 200w SMA (default 5%)
 * @param {number} opts.concurrency - Max concurrent requests (default 5)
 * @param {function} opts.onProgress - Progress callback(completed, total)
 * @returns {Promise<object[]>} Sorted array of results
 */
export async function screenStocks(tickers, opts = {}) {
  const {
    maxDistancePct = 5,
    concurrency = 5,
    onProgress = null,
  } = opts;

  const results = [];
  let completed = 0;

  // Process in batches to respect rate limits
  for (let i = 0; i < tickers.length; i += concurrency) {
    const batch = tickers.slice(i, i + concurrency);
    const batchResults = await Promise.all(
      batch.map(ticker => analyzeStock(ticker))
    );

    for (const r of batchResults) {
      if (r && r.absDistancePct <= maxDistancePct) {
        results.push(r);
      }
    }

    completed += batch.length;
    if (onProgress) onProgress(completed, tickers.length);

    // Small delay between batches to avoid rate limiting
    if (i + concurrency < tickers.length) {
      await sleep(500);
    }
  }

  // Sort by absolute distance from 200w SMA (closest first)
  results.sort((a, b) => a.absDistancePct - b.absDistancePct);

  return results;
}

function round(num, decimals) {
  return Math.round(num * 10 ** decimals) / 10 ** decimals;
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}
