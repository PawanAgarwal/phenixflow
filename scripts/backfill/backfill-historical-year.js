#!/usr/bin/env node
const fs = require('node:fs');
const path = require('node:path');
const Database = require('better-sqlite3');

const API_BASE = (process.env.PHENIX_API_BASE || 'http://127.0.0.1:3010').replace(/\/$/, '');
const THETA_BASE = (process.env.THETADATA_BASE_URL || 'http://127.0.0.1:25503').replace(/\/$/, '');
const START_DATE = process.env.BACKFILL_START_DATE || '2025-02-17';
const END_DATE = process.env.BACKFILL_END_DATE || '2026-02-17';
const MAX_RETRIES = Number(process.env.BACKFILL_MAX_RETRIES || 3);
const REQUEST_TIMEOUT_MS = Number(process.env.BACKFILL_TIMEOUT_MS || 120000);
const RETRY_BASE_MS = Math.max(250, Number(process.env.BACKFILL_RETRY_BASE_MS || 2000));
const RETRY_MAX_MS = Math.max(RETRY_BASE_MS, Number(process.env.BACKFILL_RETRY_MAX_MS || 30000));
const MODE = (process.env.BACKFILL_MODE || 'base').trim().toLowerCase();
const HEARTBEAT_EVERY = Number(process.env.BACKFILL_HEARTBEAT_EVERY || 25);
const PROBE_FILTERS_IN_BASE = String(process.env.BACKFILL_PROBE_FILTERS || '0') === '1';
const CONCURRENCY = Math.max(1, Number(process.env.BACKFILL_CONCURRENCY || 1));
const SKIP_CALENDAR_CHECK = String(process.env.BACKFILL_SKIP_CALENDAR_CHECK || '1') === '1';
const LOCAL_CACHE_CHECK = String(process.env.BACKFILL_LOCAL_CACHE_CHECK || '0') === '1';
const OUTPUT_PATH = process.env.BACKFILL_REPORT_PATH
  || path.resolve(process.cwd(), 'artifacts', 'reports', 'historical-backfill-year.json');
const DB_PATH = process.env.PHENIX_DB_PATH
  || path.resolve(process.cwd(), 'data', 'phenixflow.sqlite');

const DEFAULT_SYMBOLS = [
  'AAPL', 'MSFT', 'NVDA', 'AMZN', 'GOOGL',
  'META', 'TSLA', 'AVGO', 'NFLX', 'AMD',
  'PLTR', 'SMCI', 'ADBE', 'CRM', 'ORCL',
  'INTC', 'CSCO', 'QCOM', 'MU', 'IBM',
];

const ALL_CHIPS = [
  'calls', 'puts', 'bid', 'ask', 'aa', '100k+', 'sizable', 'whales', 'large-size',
  'leaps', 'weeklies', 'repeat-flow', 'otm', 'vol>oi', 'rising-vol', 'am-spike',
  'bullflow', 'high-sig', 'unusual', 'urgent', 'position-builders', 'grenade',
].join(',');

function toDateOnly(value) {
  const d = new Date(`${value}T00:00:00.000Z`);
  if (Number.isNaN(d.getTime())) throw new Error(`invalid_date:${value}`);
  return d;
}

function toIsoDay(d) {
  return d.toISOString().slice(0, 10);
}

function* iterWeekdays(startIso, endIso) {
  let d = toDateOnly(startIso);
  const end = toDateOnly(endIso);

  while (d <= end) {
    const dow = d.getUTCDay();
    if (dow >= 1 && dow <= 5) {
      yield toIsoDay(d);
    }
    d = new Date(d.getTime() + 86400000);
  }
}

function toYyyymmdd(dayIso) {
  return dayIso.replace(/-/g, '');
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function fetchWithTimeout(url, timeoutMs) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);

  try {
    return await fetch(url, { signal: controller.signal });
  } finally {
    clearTimeout(timer);
  }
}

async function isMarketOpenDay(dayIso) {
  const url = `${THETA_BASE}/v3/calendar/on_date?date=${toYyyymmdd(dayIso)}&format=json`;
  try {
    const response = await fetchWithTimeout(url, REQUEST_TIMEOUT_MS);
    if (!response.ok) return false;
    const body = await response.text();
    const parsed = JSON.parse(body);
    const type = Array.isArray(parsed?.type) ? String(parsed.type[0] || '').toLowerCase() : '';
    return type === 'open';
  } catch {
    return false;
  }
}

function makeHistoricalUrl({ symbol, dayIso, chips }) {
  const from = `${dayIso}T00:00:00.000Z`;
  const to = `${dayIso}T23:59:59.999Z`;
  const url = new URL(`${API_BASE}/api/flow/historical`);
  url.searchParams.set('from', from);
  url.searchParams.set('to', to);
  url.searchParams.set('symbol', symbol);
  if (chips) url.searchParams.set('chips', chips);
  return url.toString();
}

async function callHistorical({ symbol, dayIso, chips, passName }) {
  const url = makeHistoricalUrl({ symbol, dayIso, chips });
  let lastError = null;

  for (let attempt = 1; attempt <= MAX_RETRIES; attempt += 1) {
    try {
      const started = Date.now();
      const response = await fetchWithTimeout(url, REQUEST_TIMEOUT_MS);
      const text = await response.text();
      const durationMs = Date.now() - started;

      let body = null;
      try {
        body = JSON.parse(text);
      } catch {
        body = { parseError: true, raw: text.slice(0, 400) };
      }

      if (response.status === 200) {
        return {
          ok: true,
          status: 200,
          durationMs,
          passName,
          rowCount: Array.isArray(body?.data) ? body.data.length : 0,
          sync: body?.meta?.sync || null,
          enrichment: body?.meta?.enrichment || null,
        };
      }

      lastError = {
        ok: false,
        passName,
        status: response.status,
        durationMs,
        error: body?.error || { code: 'unknown_error', message: String(text).slice(0, 400) },
      };

      const retryable = response.status >= 500 || response.status === 429;
      if (!retryable) break;
    } catch (error) {
      lastError = {
        ok: false,
        passName,
        status: 0,
        error: { code: 'request_failed', message: error.message },
      };
    }

    const exponential = RETRY_BASE_MS * (2 ** (attempt - 1));
    const jitter = Math.floor(Math.random() * RETRY_BASE_MS);
    await sleep(Math.min(RETRY_MAX_MS, exponential + jitter));
  }

  return lastError || {
    ok: false,
    passName,
    status: 0,
    error: { code: 'unknown_error', message: 'unknown' },
  };
}

async function run() {
  const symbols = process.env.BACKFILL_SYMBOLS
    ? process.env.BACKFILL_SYMBOLS.split(',').map((s) => s.trim().toUpperCase()).filter(Boolean)
    : DEFAULT_SYMBOLS;

  const weekdayDays = Array.from(iterWeekdays(START_DATE, END_DATE));
  const days = [];
  if (SKIP_CALENDAR_CHECK) {
    days.push(...weekdayDays);
  } else {
    for (const dayIso of weekdayDays) {
      if (await isMarketOpenDay(dayIso)) {
        days.push(dayIso);
      }
    }
  }
  const totalJobs = symbols.length * days.length;

  const report = {
    startedAt: new Date().toISOString(),
    mode: MODE,
    concurrency: CONCURRENCY,
    apiBase: API_BASE,
    startDate: START_DATE,
    endDate: END_DATE,
    symbols,
    weekdayCount: days.length,
    marketOpenDayCount: days.length,
    totalJobs,
    skippedJobs: 0,
    successJobs: 0,
    failedJobs: 0,
    downloadedJobs: 0,
    downloadedRows: 0,
    fallbackJobs: 0,
    failures: [],
    samples: [],
  };

  const db = LOCAL_CACHE_CHECK && fs.existsSync(DB_PATH)
    ? new Database(DB_PATH, { readonly: true })
    : null;
  if (db) {
    db.pragma('busy_timeout = 1000');
  }
  const requiredMetrics = ['enrichedRows', 'spot', 'otmPct', 'oi', 'volOiRatio'];

  const isCompleteInDb = (symbol, dayIso) => {
    if (!db) return false;
    const day = db.prepare(`
      SELECT cache_status AS cacheStatus
      FROM option_trade_day_cache
      WHERE symbol = ? AND trade_date_utc = ?
    `).get(symbol, dayIso);
    if (!day || day.cacheStatus !== 'full') return false;

    const metricRows = db.prepare(`
      SELECT metric_name AS metricName, cache_status AS cacheStatus
      FROM option_trade_metric_day_cache
      WHERE symbol = ? AND trade_date_utc = ?
        AND metric_name IN (${requiredMetrics.map(() => '?').join(',')})
    `).all(symbol, dayIso, ...requiredMetrics);

    const metricMap = new Map(metricRows.map((row) => [row.metricName, row.cacheStatus]));
    return requiredMetrics.every((metric) => metricMap.get(metric) === 'full');
  };

  const jobs = [];
  for (const symbol of symbols) {
    for (const dayIso of days) {
      jobs.push({ symbol, dayIso });
    }
  }
  let done = 0;
  let nextJobIndex = 0;

  async function runOneJob(job, displayIndex) {
    const { symbol, dayIso } = job;
    const prefix = `[${displayIndex}/${totalJobs}] ${symbol} ${dayIso}`;

    if (isCompleteInDb(symbol, dayIso)) {
      report.skippedJobs += 1;
      console.log(`${prefix} SKIP already_complete`);
      return;
    }

    let base = null;
    let allFilters = null;

    if (MODE === 'filters') {
      allFilters = await callHistorical({ symbol, dayIso, chips: ALL_CHIPS, passName: 'all_filters' });
      if (!allFilters.ok) {
        base = await callHistorical({ symbol, dayIso, chips: null, passName: 'base_fallback' });
      }
    } else {
      base = await callHistorical({ symbol, dayIso, chips: null, passName: 'base_only' });
      if (PROBE_FILTERS_IN_BASE) {
        allFilters = await callHistorical({ symbol, dayIso, chips: ALL_CHIPS, passName: 'all_filters_probe' });
      }
    }

    const effective = base && base.ok ? base : allFilters;
    const ok = Boolean(effective && effective.ok);

    if (ok) {
      report.successJobs += 1;
      if (effective && effective.sync && effective.sync.fetchedRows > 0) {
        report.downloadedJobs += 1;
        report.downloadedRows += Number(effective.sync.fetchedRows || 0);
      }
      if (base && base.ok && allFilters && !allFilters.ok) {
        report.fallbackJobs += 1;
      }
      if (report.samples.length < 50) {
        report.samples.push({
          symbol,
          dayIso,
          effectivePass: effective ? effective.passName : null,
          allFilters,
          base,
        });
      }
      const effectivePass = effective ? effective.passName : 'none';
      const fetchedRows = effective && effective.sync ? Number(effective.sync.fetchedRows || 0) : 0;
      console.log(
        `${prefix} OK pass:${effectivePass} fetched:${fetchedRows} `
        + `all:${allFilters ? allFilters.status : 'n/a'} base:${base ? base.status : 'n/a'}`,
      );
    } else {
      report.failedJobs += 1;
      const failure = {
        symbol,
        dayIso,
        base,
        allFilters,
      };
      report.failures.push(failure);
      console.log(`${prefix} FAIL all:${allFilters ? allFilters.status : 'n/a'} base:${base ? base.status : 'n/a'}`);
    }
  }

  async function workerLoop() {
    while (true) {
      if (nextJobIndex >= jobs.length) return;
      const currentIndex = nextJobIndex;
      nextJobIndex += 1;
      const displayIndex = currentIndex + 1;
      await runOneJob(jobs[currentIndex], displayIndex);
      done += 1;
      if (HEARTBEAT_EVERY > 0 && done % HEARTBEAT_EVERY === 0) {
        console.log(
          `HEARTBEAT done=${done}/${totalJobs} ok=${report.successJobs} fail=${report.failedJobs} `
          + `skip=${report.skippedJobs} downloadedRows=${report.downloadedRows}`,
        );
      }
    }
  }

  const workerCount = Math.min(CONCURRENCY, jobs.length || 1);
  await Promise.all(Array.from({ length: workerCount }, () => workerLoop()));

  report.completedAt = new Date().toISOString();
  if (db) db.close();

  fs.mkdirSync(path.dirname(OUTPUT_PATH), { recursive: true });
  fs.writeFileSync(OUTPUT_PATH, `${JSON.stringify(report, null, 2)}\n`, 'utf8');

  console.log('--- SUMMARY ---');
  console.log(JSON.stringify({
    totalJobs: report.totalJobs,
    skippedJobs: report.skippedJobs,
    successJobs: report.successJobs,
    failedJobs: report.failedJobs,
    downloadedJobs: report.downloadedJobs,
    downloadedRows: report.downloadedRows,
    fallbackJobs: report.fallbackJobs,
    reportPath: OUTPUT_PATH,
  }, null, 2));

  if (report.failedJobs > 0) {
    process.exitCode = 1;
  }
}

run().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
