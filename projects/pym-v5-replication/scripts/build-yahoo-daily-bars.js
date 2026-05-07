#!/usr/bin/env node
const fs = require('node:fs');

const { loadConfig, ensureDir, runtimePath } = require('../src/config');
const { resolveEndDate } = require('../src/calendar');
const { collectTickers } = require('../src/symphony');

const YAHOO_CHART_BASE = 'https://query1.finance.yahoo.com/v8/finance/chart';

function parseArgs(argv) {
  const out = {};
  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    if (arg === '--fetch-start') out.fetchStartDate = argv[++index];
    else if (arg === '--start') out.fetchStartDate = argv[++index];
    else if (arg === '--end') out.endDate = argv[++index];
    else if (arg === '--score') out.scorePath = argv[++index];
    else if (arg === '--concurrency') out.concurrency = Number(argv[++index]);
    else if (arg === '--adjustment') out.adjustment = argv[++index];
  }
  return out;
}

function defaultScorePath(config) {
  return runtimePath('source', `composer-${config.source.composerSymphonyId}-score.json`);
}

function epochSeconds(dateIso) {
  return Math.floor(Date.parse(`${dateIso}T00:00:00Z`) / 1000);
}

function addDays(dateIso, days) {
  const date = new Date(`${dateIso}T00:00:00Z`);
  date.setUTCDate(date.getUTCDate() + days);
  return date.toISOString().slice(0, 10);
}

function yahooDate(timestampSeconds) {
  return new Date(timestampSeconds * 1000).toISOString().slice(0, 10);
}

function yahooUrl(ticker, startDate, endDate) {
  const params = new URLSearchParams({
    period1: String(epochSeconds(startDate)),
    period2: String(epochSeconds(addDays(endDate, 2))),
    interval: '1d',
    events: 'history|div|split',
    includeAdjustedClose: 'true',
  });
  return `${YAHOO_CHART_BASE}/${encodeURIComponent(ticker)}?${params.toString()}`;
}

function adjustedRow(ticker, timestamp, quote, adjClose, adjustmentMode) {
  const rawClose = quote.close;
  if (!Number.isFinite(rawClose) || rawClose <= 0) return null;
  const adjustment = adjustmentMode === 'raw' ? 1 : adjClose / rawClose;
  if (!Number.isFinite(adjustment)) return null;
  return {
    date: yahooDate(timestamp),
    ticker,
    open: Number.isFinite(quote.open) ? quote.open * adjustment : null,
    high: Number.isFinite(quote.high) ? quote.high * adjustment : null,
    low: Number.isFinite(quote.low) ? quote.low * adjustment : null,
    close: adjustmentMode === 'raw' ? rawClose : adjClose,
    volume: Number.isFinite(quote.volume) ? quote.volume : 0,
    transactions: 0,
    regularMinuteCount: 390,
    source: adjustmentMode === 'raw' ? 'yahoo-chart-raw-daily' : 'yahoo-chart-adjusted-daily',
  };
}

async function fetchTicker(ticker, startDate, endDate, adjustmentMode, attempt = 1) {
  const response = await fetch(yahooUrl(ticker, startDate, endDate), {
    headers: { 'user-agent': 'phenixflow-pym-v5-replication/1.0' },
  });
  if (!response.ok) {
    if (attempt < 4 && [429, 500, 502, 503, 504].includes(response.status)) {
      await new Promise((resolve) => setTimeout(resolve, 500 * (2 ** (attempt - 1))));
      return fetchTicker(ticker, startDate, endDate, adjustmentMode, attempt + 1);
    }
    throw new Error(`Yahoo chart fetch failed for ${ticker}: ${response.status}`);
  }
  const json = await response.json();
  const result = json.chart?.result?.[0];
  const error = json.chart?.error;
  if (error) throw new Error(`Yahoo chart error for ${ticker}: ${JSON.stringify(error)}`);
  const timestamps = result?.timestamp || [];
  const quote = result?.indicators?.quote?.[0] || {};
  const adjusted = result?.indicators?.adjclose?.[0]?.adjclose || [];
  const rows = [];
  timestamps.forEach((timestamp, index) => {
    const row = adjustedRow(ticker, timestamp, {
      open: quote.open?.[index],
      high: quote.high?.[index],
      low: quote.low?.[index],
      close: quote.close?.[index],
      volume: quote.volume?.[index],
    }, adjusted[index], adjustmentMode);
    if (row && row.date >= startDate && row.date <= endDate) rows.push(row);
  });
  return rows;
}

async function fetchAllTickers(tickers, startDate, endDate, concurrency, adjustmentMode) {
  const queue = [...tickers];
  const rows = [];
  const failures = [];
  let completed = 0;

  async function worker() {
    while (queue.length) {
      const ticker = queue.shift();
      try {
        const tickerRows = await fetchTicker(ticker, startDate, endDate, adjustmentMode);
        rows.push(...tickerRows);
        completed += 1;
        console.log(`fetched ${completed}/${tickers.length} ${ticker}; rows=${tickerRows.length}`);
      } catch (error) {
        completed += 1;
        failures.push({ ticker, error: error.message });
        console.log(`failed ${completed}/${tickers.length} ${ticker}: ${error.message}`);
      }
    }
  }

  await Promise.all(Array.from({ length: concurrency }, () => worker()));
  return { rows, failures };
}

function missingByDate(rows, tickers) {
  const dates = [...new Set(rows.map((row) => row.date))].sort();
  const byDate = new Map(dates.map((date) => [date, new Set()]));
  rows.forEach((row) => byDate.get(row.date)?.add(row.ticker));
  return dates
    .map((date) => ({ date, missing: [...tickers].filter((ticker) => !byDate.get(date).has(ticker)) }))
    .filter((row) => row.missing.length);
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const config = loadConfig();
  const scorePath = args.scorePath || defaultScorePath(config);
  if (!fs.existsSync(scorePath)) {
    throw new Error(`Missing Composer score snapshot: ${scorePath}. Run npm run pym-v5:fetch-sources first.`);
  }
  const score = JSON.parse(fs.readFileSync(scorePath, 'utf8'));
  const tickers = [...collectTickers(score)].sort();
  const fetchStartDate = args.fetchStartDate || '2024-01-01';
  const endDate = resolveEndDate(config, args.endDate || config.windows.endDate);
  const concurrency = Number.isFinite(args.concurrency) ? args.concurrency : 4;
  const adjustmentMode = args.adjustment === 'raw' ? 'raw' : 'adjusted';
  const outPath = runtimePath(`pym-v5-yahoo-${adjustmentMode}-daily-bars-${fetchStartDate}-${endDate}.jsonl`);
  const manifestPath = runtimePath(`pym-v5-yahoo-${adjustmentMode}-daily-bars-${fetchStartDate}-${endDate}.manifest.json`);
  ensureDir(runtimePath());

  const { rows, failures } = await fetchAllTickers(tickers, fetchStartDate, endDate, concurrency, adjustmentMode);
  rows.sort((left, right) => left.date.localeCompare(right.date) || left.ticker.localeCompare(right.ticker));
  fs.writeFileSync(outPath, `${rows.map((row) => JSON.stringify(row)).join('\n')}\n`);
  const manifest = {
    generatedAt: new Date().toISOString(),
    provider: 'Yahoo Finance chart API',
    adjustment: adjustmentMode === 'raw' ? 'Yahoo raw daily OHLC. Close is raw close.' : 'OHLC scaled by adjClose / rawClose; close is Yahoo adjClose.',
    fetchStartDate,
    endDate,
    tickerCount: tickers.length,
    tickers,
    rowsWritten: rows.length,
    failures,
    missingTickerDayCount: missingByDate(rows, tickers).length,
    missingByDay: missingByDate(rows, tickers),
    outputPath: outPath,
  };
  fs.writeFileSync(manifestPath, JSON.stringify(manifest, null, 2));
  console.log(`wrote ${outPath}`);
  console.log(`wrote ${manifestPath}`);
  if (failures.length) process.exitCode = 2;
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
