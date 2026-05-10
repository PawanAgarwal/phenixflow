#!/usr/bin/env node
const fs = require('node:fs');
const path = require('node:path');

const { ensureDir, runtimePath } = require('../src/config');
const { loadMassiveEnv } = require('../src/env');

const DEFAULT_BASE_URL = 'https://api.massive.com';
const RETRY_STATUSES = new Set([408, 425, 429, 500, 502, 503, 504]);

const EXTRA_TICKERS = Object.freeze([
  'HYG', 'LQD', 'JNK',
  'XLE', 'XLB', 'XLI', 'XLY', 'XLC', 'XLRE',
]);

function parseArgs(argv) {
  const out = {};
  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    if (arg === '--start') out.startDate = argv[++i];
    else if (arg === '--end') out.endDate = argv[++i];
    else if (arg === '--concurrency') out.concurrency = Number(argv[++i]);
    else if (arg === '--api-key') out.apiKey = argv[++i];
    else if (arg === '--base-url') out.baseUrl = argv[++i];
    else if (arg === '--tickers') out.tickers = argv[++i].split(',').map((t) => t.trim().toUpperCase()).filter(Boolean);
  }
  return out;
}

function resolveApiKey(args, env = process.env) {
  const key = String(args.apiKey || env.MASSIVE_API_KEY || env.POLYGON_API_KEY || '').trim();
  if (!key) throw new Error('Missing Massive REST API key.');
  return key;
}

function buildAggregateUrl({ baseUrl, ticker, startDate, endDate }) {
  const params = new URLSearchParams({ adjusted: 'true', sort: 'asc', limit: '50000' });
  return `${baseUrl}/v2/aggs/ticker/${encodeURIComponent(ticker)}/range/1/day/${startDate}/${endDate}?${params}`;
}

function sleep(ms) { return new Promise((resolve) => setTimeout(resolve, ms)); }

async function fetchJsonWithRetry(url, apiKey, attempt = 1) {
  const requestUrl = new URL(url);
  requestUrl.searchParams.set('apiKey', apiKey);
  const response = await fetch(requestUrl, { headers: { 'user-agent': 'phenixflow-pym-v5-extra/1.0' } });
  if (!response.ok) {
    if (attempt < 5 && RETRY_STATUSES.has(response.status)) {
      await sleep(Math.min(30000, 750 * (2 ** Math.max(0, attempt - 1))));
      return fetchJsonWithRetry(url, apiKey, attempt + 1);
    }
    const body = await response.text();
    throw new Error(`fetch failed status=${response.status} body=${body.slice(0, 300)}`);
  }
  return response.json();
}

async function fetchTicker({ ticker, startDate, endDate, baseUrl, apiKey }) {
  const rows = [];
  let nextUrl = buildAggregateUrl({ baseUrl, ticker, startDate, endDate });
  while (nextUrl) {
    const json = await fetchJsonWithRetry(nextUrl, apiKey);
    (json.results || []).forEach((aggregate) => {
      const date = new Date(aggregate.t).toISOString().slice(0, 10);
      if (date < startDate || date > endDate) return;
      if (!Number.isFinite(aggregate.c)) return;
      rows.push({
        date,
        ticker,
        open: Number.isFinite(aggregate.o) ? aggregate.o : null,
        high: Number.isFinite(aggregate.h) ? aggregate.h : null,
        low: Number.isFinite(aggregate.l) ? aggregate.l : null,
        close: aggregate.c,
        volume: Number.isFinite(aggregate.v) ? aggregate.v : 0,
        transactions: Number.isFinite(aggregate.n) ? aggregate.n : 0,
        regularMinuteCount: 390,
        source: 'massive-rest-adjusted-daily',
      });
    });
    nextUrl = json.next_url || json.nextUrl || null;
  }
  return rows;
}

async function main() {
  loadMassiveEnv();
  const args = parseArgs(process.argv.slice(2));
  const tickers = args.tickers && args.tickers.length ? args.tickers : EXTRA_TICKERS;
  const startDate = args.startDate || '2015-01-01';
  const endDate = args.endDate || '2026-05-08';
  const apiKey = resolveApiKey(args);
  const baseUrl = String(args.baseUrl || process.env.MASSIVE_REST_BASE_URL || DEFAULT_BASE_URL).trim().replace(/\/+$/g, '');
  const concurrency = Math.max(1, Math.min(8, Number.isFinite(args.concurrency) ? args.concurrency : 4));
  const outPath = runtimePath(`pym-v5-extra-eod-daily-bars-${startDate}-${endDate}.jsonl`);
  const manifestPath = outPath.replace(/\.jsonl$/, '.manifest.json');
  ensureDir(path.dirname(outPath));

  const queue = [...tickers];
  const allRows = [];
  const failures = [];
  let completed = 0;
  async function worker() {
    while (queue.length) {
      const ticker = queue.shift();
      try {
        const rows = await fetchTicker({ ticker, startDate, endDate, baseUrl, apiKey });
        allRows.push(...rows);
        completed += 1;
        process.stdout.write(`fetched ${completed}/${tickers.length} ${ticker} rows=${rows.length}\n`);
      } catch (error) {
        completed += 1;
        failures.push({ ticker, error: error.message });
        process.stdout.write(`failed ${completed}/${tickers.length} ${ticker}: ${error.message}\n`);
      }
    }
  }
  await Promise.all(Array.from({ length: concurrency }, () => worker()));
  allRows.sort((a, b) => a.date.localeCompare(b.date) || a.ticker.localeCompare(b.ticker));
  fs.writeFileSync(outPath, `${allRows.map((r) => JSON.stringify(r)).join('\n')}\n`);
  const manifest = {
    generatedAt: new Date().toISOString(),
    provider: 'Massive REST aggregate bars (extra tickers)',
    tickers,
    rowsWritten: allRows.length,
    failures,
    outputPath: outPath,
    startDate,
    endDate,
  };
  fs.writeFileSync(manifestPath, JSON.stringify(manifest, null, 2));
  console.log(`wrote ${outPath}`);
  if (failures.length) process.exitCode = 2;
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
