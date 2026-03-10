#!/usr/bin/env node
const fs = require('node:fs');
const path = require('node:path');

const THETA_BASE_URL = (process.env.THETADATA_BASE_URL || 'http://127.0.0.1:25503').replace(/\/$/, '');
const THETADATA_CALENDAR_PATH = (process.env.THETADATA_CALENDAR_PATH || '/v3/calendar/on_date').trim();
const SYMBOL_FILE = path.resolve(process.env.SYMBOL_FILE || path.join(process.cwd(), 'config', 'top200-universe.json'));
const SYMBOL_LIMIT = Math.max(1, Number(process.env.SYMBOL_LIMIT || 100));
const START_DATE = String(process.env.START_DATE || '').trim();
const END_DATE = String(process.env.END_DATE || '').trim();
const SKIP_CALENDAR_CHECK = String(process.env.SKIP_CALENDAR_CHECK || '0') === '1';
const CALENDAR_TIMEOUT_MS = Math.max(1000, Number(process.env.CALENDAR_TIMEOUT_MS || 30000));
const CALENDAR_CONCURRENCY = Math.max(1, Number(process.env.CALENDAR_CONCURRENCY || 4));
const TS = new Date().toISOString().replace(/[-:.]/g, '').slice(0, 15);
const OUTPUT_PATH = path.resolve(
  process.env.OUTPUT_PATH
    || path.join(
      process.cwd(),
      'artifacts',
      'reports',
      `symbol-days-top${SYMBOL_LIMIT}-${START_DATE.replace(/-/g, '')}-${END_DATE.replace(/-/g, '')}-${TS}.tsv`,
    ),
);
const OPEN_DAYS_OUTPUT_PATH = path.resolve(
  process.env.OPEN_DAYS_OUTPUT_PATH
    || path.join(
      process.cwd(),
      'artifacts',
      'reports',
      `open-days-${START_DATE.replace(/-/g, '')}-${END_DATE.replace(/-/g, '')}-${TS}.tsv`,
    ),
);

function ensureIsoDate(raw) {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(raw)) {
    throw new Error(`invalid_iso_date:${raw}`);
  }
  const d = new Date(`${raw}T00:00:00.000Z`);
  if (Number.isNaN(d.getTime())) {
    throw new Error(`invalid_iso_date:${raw}`);
  }
  return raw;
}

function toYyyymmdd(dayIso) {
  return dayIso.replace(/-/g, '');
}

function listWeekdaysInRange(startIso, endIso) {
  const out = [];
  let cursor = new Date(`${startIso}T00:00:00.000Z`);
  const end = new Date(`${endIso}T00:00:00.000Z`);
  while (cursor <= end) {
    const day = cursor.getUTCDay();
    if (day >= 1 && day <= 5) {
      out.push(cursor.toISOString().slice(0, 10));
    }
    cursor = new Date(cursor.getTime() + 86400000);
  }
  return out;
}

function parseCalendarType(rawBody) {
  const parsed = JSON.parse(rawBody);
  if (Array.isArray(parsed?.type) && parsed.type.length > 0) {
    return String(parsed.type[0] || '').toLowerCase();
  }
  if (typeof parsed?.type === 'string') {
    return parsed.type.toLowerCase();
  }
  if (Array.isArray(parsed?.rows) && parsed.rows.length > 0 && parsed.rows[0]?.type) {
    const value = parsed.rows[0].type;
    if (Array.isArray(value)) return String(value[0] || '').toLowerCase();
    return String(value || '').toLowerCase();
  }
  if (Array.isArray(parsed?.response) && Array.isArray(parsed?.header)) {
    const typeIdx = parsed.header.findIndex((name) => String(name).toLowerCase() === 'type');
    if (typeIdx >= 0 && parsed.response[0]) {
      return String(parsed.response[0][typeIdx] || '').toLowerCase();
    }
  }
  return '';
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

async function isTradableMarketDay(dayIso) {
  const calendarPath = THETADATA_CALENDAR_PATH.startsWith('/')
    ? THETADATA_CALENDAR_PATH
    : `/${THETADATA_CALENDAR_PATH}`;
  const url = `${THETA_BASE_URL}${calendarPath}?date=${toYyyymmdd(dayIso)}&format=json`;
  const started = Date.now();
  const response = await fetchWithTimeout(url, CALENDAR_TIMEOUT_MS);
  const elapsedMs = Date.now() - started;
  if (!response.ok) {
    throw new Error(`calendar_http_${response.status}:${dayIso}:${elapsedMs}ms`);
  }
  const rawBody = await response.text();
  const marketType = parseCalendarType(rawBody);
  return marketType === 'open' || marketType === 'early_close';
}

async function filterOpenDays(days) {
  if (SKIP_CALENDAR_CHECK) return days;
  const open = [];
  let cursor = 0;

  async function worker() {
    while (cursor < days.length) {
      const idx = cursor;
      cursor += 1;
      const dayIso = days[idx];
      if (await isTradableMarketDay(dayIso)) {
        open.push(dayIso);
      }
    }
  }

  const workerCount = Math.min(CALENDAR_CONCURRENCY, days.length || 1);
  await Promise.all(Array.from({ length: workerCount }, () => worker()));
  return open.sort();
}

function loadTopSymbols(symbolFilePath, limit) {
  const parsed = JSON.parse(fs.readFileSync(symbolFilePath, 'utf8'));
  if (!Array.isArray(parsed)) {
    throw new Error(`invalid_symbol_file:${symbolFilePath}`);
  }
  const symbols = [];
  const seen = new Set();
  parsed.forEach((raw) => {
    const symbol = String(raw || '').trim().toUpperCase();
    if (!symbol || seen.has(symbol)) return;
    seen.add(symbol);
    symbols.push(symbol);
  });
  return symbols.slice(0, limit);
}

async function run() {
  if (!START_DATE || !END_DATE) {
    throw new Error('START_DATE and END_DATE are required (YYYY-MM-DD)');
  }
  const startIso = ensureIsoDate(START_DATE);
  const endIso = ensureIsoDate(END_DATE);
  if (startIso > endIso) {
    throw new Error(`START_DATE must be <= END_DATE (got ${startIso} > ${endIso})`);
  }

  const symbols = loadTopSymbols(SYMBOL_FILE, SYMBOL_LIMIT);
  if (symbols.length === 0) {
    throw new Error(`no_symbols_loaded:${SYMBOL_FILE}`);
  }

  const weekdays = listWeekdaysInRange(startIso, endIso);
  if (weekdays.length === 0) {
    throw new Error(`no_weekdays_in_range:${startIso}:${endIso}`);
  }
  const openDays = await filterOpenDays(weekdays);
  if (openDays.length === 0) {
    throw new Error(`no_open_days_in_range:${startIso}:${endIso}`);
  }

  const symbolDayLines = [];
  openDays.forEach((dayIso) => {
    symbols.forEach((symbol) => {
      symbolDayLines.push(`${dayIso}\t${symbol}`);
    });
  });

  fs.mkdirSync(path.dirname(OPEN_DAYS_OUTPUT_PATH), { recursive: true });
  fs.writeFileSync(OPEN_DAYS_OUTPUT_PATH, `${openDays.join('\n')}\n`, 'utf8');
  fs.mkdirSync(path.dirname(OUTPUT_PATH), { recursive: true });
  fs.writeFileSync(OUTPUT_PATH, `${symbolDayLines.join('\n')}\n`, 'utf8');

  console.log(JSON.stringify({
    thetaBaseUrl: THETA_BASE_URL,
    symbolFile: SYMBOL_FILE,
    symbolLimit: SYMBOL_LIMIT,
    symbolCount: symbols.length,
    startDate: startIso,
    endDate: endIso,
    weekdays: weekdays.length,
    openDays: openDays.length,
    symbolDays: symbolDayLines.length,
    skipCalendarCheck: SKIP_CALENDAR_CHECK,
    outputPath: OUTPUT_PATH,
    openDaysOutputPath: OPEN_DAYS_OUTPUT_PATH,
  }, null, 2));
}

run().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
