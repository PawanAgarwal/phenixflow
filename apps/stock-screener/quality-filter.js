/**
 * Quality filter: Enriches screener results with fundamental quality metrics
 * from Yahoo Finance to separate high-quality names from value traps.
 *
 * Quality scoring (max 13 points):
 * - Market cap: mega(3), large(2), mid-large(1)
 * - Profitability: positive EPS(2), high margins(2) or decent margins(1)
 * - Growth: strong(2) or positive(1)
 * - Balance sheet: low debt(2) or moderate(1)
 * - Returns: high ROE(2) or decent(1)
 */

import { fetchQuoteSummary } from './yahoo-client.js';

/**
 * Fetch fundamental quality metrics for a single ticker.
 * @param {string} symbol
 * @returns {Promise<object|null>}
 */
async function getQualityMetrics(symbol) {
  try {
    const quote = await fetchQuoteSummary(symbol);
    if (!quote) return null;

    const price = quote.price || {};
    const keyStats = quote.defaultKeyStatistics || {};
    const financial = quote.financialData || {};
    const summary = quote.summaryDetail || {};

    const marketCap = extractRaw(price.marketCap) || 0;
    const trailingEps = extractRaw(keyStats.trailingEps);
    const forwardEps = extractRaw(keyStats.forwardEps);
    const debtToEquity = extractRaw(financial.debtToEquity);
    const revenueGrowth = extractRaw(financial.revenueGrowth);
    const operatingMargins = extractRaw(financial.operatingMargins);
    const returnOnEquity = extractRaw(financial.returnOnEquity);
    const currentRatio = extractRaw(financial.currentRatio);
    const trailingPE = extractRaw(summary.trailingPE);
    const forwardPE = extractRaw(summary.forwardPE);
    const dividendYield = extractRaw(summary.dividendYield);
    const sector = price.sector || 'Unknown';
    const shortName = price.shortName || symbol;

    // Quality scoring (max 13 points)
    let qualityScore = 0;
    const qualityFlags = [];

    // Market cap
    if (marketCap > 100e9) { qualityScore += 3; qualityFlags.push('mega_cap'); }
    else if (marketCap > 50e9) { qualityScore += 2; qualityFlags.push('large_cap'); }
    else if (marketCap > 10e9) { qualityScore += 1; qualityFlags.push('mid_large_cap'); }
    else { qualityFlags.push('small_cap'); }

    // Profitability
    if (trailingEps > 0) { qualityScore += 2; qualityFlags.push('profitable'); }
    if (operatingMargins > 0.20) { qualityScore += 2; qualityFlags.push('high_margins'); }
    else if (operatingMargins > 0.10) { qualityScore += 1; qualityFlags.push('decent_margins'); }

    // Growth
    if (revenueGrowth > 0.10) { qualityScore += 2; qualityFlags.push('strong_growth'); }
    else if (revenueGrowth > 0) { qualityScore += 1; qualityFlags.push('growing'); }

    // Balance sheet (skip for financials)
    const isFinancial = sector === 'Financial Services';
    if (!isFinancial && debtToEquity != null) {
      if (debtToEquity < 50) { qualityScore += 2; qualityFlags.push('low_debt'); }
      else if (debtToEquity < 100) { qualityScore += 1; qualityFlags.push('moderate_debt'); }
      else { qualityFlags.push('high_debt'); }
    }

    // ROE
    if (returnOnEquity > 0.20) { qualityScore += 2; qualityFlags.push('high_roe'); }
    else if (returnOnEquity > 0.10) { qualityScore += 1; qualityFlags.push('decent_roe'); }

    return {
      symbol,
      shortName,
      sector,
      marketCap,
      marketCapLabel: formatMarketCap(marketCap),
      trailingPE: trailingPE ? round(trailingPE, 1) : null,
      forwardPE: forwardPE ? round(forwardPE, 1) : null,
      trailingEps: trailingEps ? round(trailingEps, 2) : null,
      forwardEps: forwardEps ? round(forwardEps, 2) : null,
      revenueGrowth: revenueGrowth != null ? round(revenueGrowth * 100, 1) : null,
      operatingMargins: operatingMargins != null ? round(operatingMargins * 100, 1) : null,
      returnOnEquity: returnOnEquity != null ? round(returnOnEquity * 100, 1) : null,
      debtToEquity: debtToEquity != null ? round(debtToEquity, 1) : null,
      currentRatio: currentRatio ? round(currentRatio, 2) : null,
      dividendYield: dividendYield != null ? round(dividendYield * 100, 2) : null,
      qualityScore,
      qualityFlags,
    };
  } catch (_err) {
    return null;
  }
}

/**
 * Enrich an array of screener results with quality data.
 * @param {object[]} screenerResults
 * @param {object} opts
 * @param {number} opts.minQualityScore
 * @param {number} opts.concurrency
 * @param {function} opts.onProgress
 * @returns {Promise<object[]>}
 */
export async function enrichWithQuality(screenerResults, opts = {}) {
  const {
    minQualityScore = 0,
    concurrency = 3,
    onProgress = null,
  } = opts;

  const enriched = [];
  let completed = 0;

  for (let i = 0; i < screenerResults.length; i += concurrency) {
    const batch = screenerResults.slice(i, i + concurrency);
    const batchMetrics = await Promise.all(
      batch.map(r => getQualityMetrics(r.symbol))
    );

    for (let j = 0; j < batch.length; j++) {
      const metrics = batchMetrics[j];
      if (metrics && metrics.qualityScore >= minQualityScore) {
        enriched.push({ ...batch[j], ...metrics });
      }
    }

    completed += batch.length;
    if (onProgress) onProgress(completed, screenerResults.length);

    if (i + concurrency < screenerResults.length) {
      await sleep(500);
    }
  }

  // Sort by quality score desc, then by distance from 200w SMA asc
  enriched.sort((a, b) => {
    if (b.qualityScore !== a.qualityScore) return b.qualityScore - a.qualityScore;
    return a.absDistancePct - b.absDistancePct;
  });

  return enriched;
}

/**
 * Yahoo Finance v10 API returns values as {raw: 123, fmt: "123"}.
 * Extract the raw numeric value.
 */
function extractRaw(val) {
  if (val == null) return null;
  if (typeof val === 'number') return val;
  if (typeof val === 'object' && val.raw !== undefined) return val.raw;
  return null;
}

function formatMarketCap(val) {
  if (val >= 1e12) return `$${(val / 1e12).toFixed(1)}T`;
  if (val >= 1e9) return `$${(val / 1e9).toFixed(1)}B`;
  if (val >= 1e6) return `$${(val / 1e6).toFixed(0)}M`;
  return `$${val}`;
}

function round(num, decimals) {
  return Math.round(num * 10 ** decimals) / 10 ** decimals;
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}
