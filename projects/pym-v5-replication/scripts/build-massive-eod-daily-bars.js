#!/usr/bin/env node
const fs = require('node:fs');

const {
  loadConfig,
  ensureDir,
  runtimePath,
} = require('../src/config');
const { resolveEndDate } = require('../src/calendar');
const { loadMassiveEnv } = require('../src/env');
const { collectTickers } = require('../src/symphony');

const DEFAULT_BASE_URL = 'https://api.massive.com';
const RETRY_STATUSES = new Set([408, 425, 429, 500, 502, 503, 504]);

function parseArgs(argv) {
  const out = {};
  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    if (arg === '--fetch-start') out.fetchStartDate = argv[++index];
    else if (arg === '--start') out.fetchStartDate = argv[++index];
    else if (arg === '--end') out.endDate = argv[++index];
    else if (arg === '--score') out.scorePath = argv[++index];
    else if (arg === '--concurrency') out.concurrency = Number(argv[++index]);
    else if (arg === '--adjusted') out.adjusted = parseBoolean(argv[++index], true);
    else if (arg === '--raw') out.adjusted = false;
    else if (arg === '--api-key') out.apiKey = argv[++index];
    else if (arg === '--base-url') out.baseUrl = argv[++index];
  }
  return out;
}

function parseBoolean(value, fallback) {
  if (value === undefined) return fallback;
  const normalized = String(value).trim().toLowerCase();
  if (['1', 'true', 'yes', 'y'].includes(normalized)) return true;
  if (['0', 'false', 'no', 'n'].includes(normalized)) return false;
  return fallback;
}

function defaultScorePath(config) {
  return runtimePath('source', `composer-${config.source.composerSymphonyId}-score.json`);
}

function resolveApiKey(args, env = process.env) {
  const key = String(
    args.apiKey
    || env.MASSIVE_API_KEY
    || env.POLYGON_API_KEY
    || '',
  ).trim();
  if (!key) {
    throw new Error('Missing Massive REST API key. Set MASSIVE_API_KEY or POLYGON_API_KEY, or pass --api-key.');
  }
  return key;
}

function normalizeBaseUrl(rawValue) {
  return String(rawValue || DEFAULT_BASE_URL).trim().replace(/\/+$/g, '') || DEFAULT_BASE_URL;
}

function buildAggregateUrl({ baseUrl, ticker, startDate, endDate, adjusted }) {
  const params = new URLSearchParams({
    adjusted: adjusted ? 'true' : 'false',
    sort: 'asc',
    limit: '50000',
  });
  return `${baseUrl}/v2/aggs/ticker/${encodeURIComponent(ticker)}/range/1/day/${startDate}/${endDate}?${params.toString()}`;
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function retryDelayMs(attempt) {
  return Math.min(30000, 750 * (2 ** Math.max(0, attempt - 1)));
}

async function fetchJsonWithRetry(url, apiKey, attempt = 1) {
  const requestUrl = new URL(url);
  requestUrl.searchParams.set('apiKey', apiKey);
  const response = await fetch(requestUrl, {
    headers: {
      'user-agent': 'phenixflow-pym-v5-replication/1.0',
    },
  });
  if (!response.ok) {
    if (attempt < 5 && RETRY_STATUSES.has(response.status)) {
      await sleep(retryDelayMs(attempt));
      return fetchJsonWithRetry(url, apiKey, attempt + 1);
    }
    const body = await response.text();
    throw new Error(`Massive aggregate fetch failed: status=${response.status} body=${body.slice(0, 500)}`);
  }
  return response.json();
}

function dateFromTimestampMs(timestamp) {
  if (!Number.isFinite(timestamp)) return null;
  return new Date(timestamp).toISOString().slice(0, 10);
}

function rowFromAggregate(ticker, aggregate, adjusted) {
  const date = dateFromTimestampMs(aggregate.t);
  if (!date || !Number.isFinite(aggregate.c)) return null;
  return {
    date,
    ticker,
    open: Number.isFinite(aggregate.o) ? aggregate.o : null,
    high: Number.isFinite(aggregate.h) ? aggregate.h : null,
    low: Number.isFinite(aggregate.l) ? aggregate.l : null,
    close: aggregate.c,
    volume: Number.isFinite(aggregate.v) ? aggregate.v : 0,
    transactions: Number.isFinite(aggregate.n) ? aggregate.n : 0,
    regularMinuteCount: 390,
    source: adjusted ? 'massive-rest-adjusted-daily' : 'massive-rest-raw-daily',
  };
}

async function fetchTicker({ ticker, startDate, endDate, adjusted, baseUrl, apiKey }) {
  const rows = [];
  let nextUrl = buildAggregateUrl({ baseUrl, ticker, startDate, endDate, adjusted });
  while (nextUrl) {
    const json = await fetchJsonWithRetry(nextUrl, apiKey);
    const status = String(json.status || '').toUpperCase();
    if (status && !['OK', 'DELAYED', 'SUCCESS'].includes(status)) {
      throw new Error(`Massive aggregate fetch returned status=${json.status} ticker=${ticker}`);
    }
    (json.results || []).forEach((aggregate) => {
      const row = rowFromAggregate(ticker, aggregate, adjusted);
      if (row && row.date >= startDate && row.date <= endDate) rows.push(row);
    });
    nextUrl = json.next_url || json.nextUrl || null;
  }
  return rows;
}

async function fetchAllTickers({ tickers, startDate, endDate, concurrency, adjusted, baseUrl, apiKey }) {
  const queue = [...tickers];
  const rows = [];
  const failures = [];
  let completed = 0;

  async function worker() {
    while (queue.length) {
      const ticker = queue.shift();
      try {
        const tickerRows = await fetchTicker({ ticker, startDate, endDate, adjusted, baseUrl, apiKey });
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
  loadMassiveEnv();
  const args = parseArgs(process.argv.slice(2));
  const config = loadConfig();
  const scorePath = args.scorePath || defaultScorePath(config);
  if (!fs.existsSync(scorePath)) {
    throw new Error(`Missing Composer score snapshot: ${scorePath}. Run npm run pym-v5:fetch-sources first.`);
  }
  const apiKey = resolveApiKey(args);
  const score = JSON.parse(fs.readFileSync(scorePath, 'utf8'));
  const tickers = [...collectTickers(score)].sort();
  const fetchStartDate = args.fetchStartDate || '2024-01-01';
  const endDate = resolveEndDate(config, args.endDate || config.windows.endDate);
  const concurrency = Number.isFinite(args.concurrency) ? Math.max(1, Math.min(12, Math.trunc(args.concurrency))) : 4;
  const adjusted = args.adjusted !== false;
  const baseUrl = normalizeBaseUrl(args.baseUrl || process.env.MASSIVE_REST_BASE_URL);
  const adjustmentMode = adjusted ? 'adjusted' : 'raw';
  const outPath = runtimePath(`pym-v5-massive-eod-${adjustmentMode}-daily-bars-${fetchStartDate}-${endDate}.jsonl`);
  const manifestPath = runtimePath(`pym-v5-massive-eod-${adjustmentMode}-daily-bars-${fetchStartDate}-${endDate}.manifest.json`);
  ensureDir(runtimePath());

  const { rows, failures } = await fetchAllTickers({
    tickers,
    startDate: fetchStartDate,
    endDate,
    concurrency,
    adjusted,
    baseUrl,
    apiKey,
  });
  rows.sort((left, right) => left.date.localeCompare(right.date) || left.ticker.localeCompare(right.ticker));
  fs.writeFileSync(outPath, `${rows.map((row) => JSON.stringify(row)).join('\n')}\n`);

  const missingByDay = missingByDate(rows, tickers);
  const manifest = {
    generatedAt: new Date().toISOString(),
    provider: 'Massive REST aggregate bars',
    endpoint: '/v2/aggs/ticker/{ticker}/range/1/day/{from}/{to}',
    baseUrl,
    adjusted,
    adjustment: adjusted
      ? 'Massive adjusted daily aggregate bars requested with adjusted=true.'
      : 'Massive raw daily aggregate bars requested with adjusted=false.',
    fetchStartDate,
    endDate,
    tickerCount: tickers.length,
    tickers,
    rowsWritten: rows.length,
    failures,
    missingTickerDayCount: missingByDay.length,
    missingByDay,
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
