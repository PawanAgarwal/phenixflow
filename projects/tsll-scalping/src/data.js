const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { spawn } = require('node:child_process');
const readline = require('node:readline');

const {
  datasetCsvPath,
  ensureDir,
  resolveDatasetSource,
  runtimePath,
} = require('./config');
const { loadOpenDates } = require('./calendar');
const { parseOpraTicker, daysBetween } = require('./opra');
const { readGzipCsv, toNumber } = require('./csv');
const {
  nsToMs,
  nsToMinuteMs,
  getEtParts,
  isRegularSessionMs,
  etMinuteToUtcMs,
} = require('./time');

function safeReturn(current, previous) {
  if (!Number.isFinite(current) || !Number.isFinite(previous) || previous <= 0) return 0;
  return (current / previous) - 1;
}

function safeRatio(numerator, denominator) {
  if (!Number.isFinite(numerator) || !Number.isFinite(denominator) || denominator === 0) return 0;
  return numerator / denominator;
}

function createRootToGroup(optionRoots) {
  const out = new Map();
  Object.entries(optionRoots || {}).forEach(([group, roots]) => {
    roots.forEach((root) => out.set(String(root).toUpperCase(), group));
  });
  return out;
}

function createEmptyOptionAgg(groups) {
  const out = {};
  groups.forEach((group) => {
    out[group] = {
      trade_count: 0,
      trade_size: 0,
      trade_call_size: 0,
      trade_put_size: 0,
      trade_premium: 0,
      trade_call_premium: 0,
      trade_put_premium: 0,
      trade_near_dte_size: 0,
      quote_volume: 0,
      quote_call_volume: 0,
      quote_put_volume: 0,
      quote_premium: 0,
      quote_call_premium: 0,
      quote_put_premium: 0,
      quote_near_dte_volume: 0,
    };
  });
  return out;
}

function mergeOptionAgg(target, source, groups) {
  if (!source) return;
  groups.forEach((group) => {
    Object.keys(target[group]).forEach((key) => {
      target[group][key] += source[group]?.[key] || 0;
    });
  });
}

function getOptionAgg(optionByMinute, minuteMs, groups) {
  let agg = optionByMinute.get(minuteMs);
  if (!agg) {
    agg = createEmptyOptionAgg(groups);
    optionByMinute.set(minuteMs, agg);
  }
  return agg;
}

function addOptionTrade(optionByMinute, dayIso, row, rootToGroup, groups, session) {
  const parsed = parseOpraTicker(row.ticker);
  if (!parsed) return;
  const group = rootToGroup.get(parsed.root);
  if (!group) return;
  const tsMs = nsToMs(row.sip_timestamp);
  if (!Number.isFinite(tsMs) || !isRegularSessionMs(tsMs, session)) return;
  const minuteMs = Math.floor(tsMs / 60000) * 60000;
  const size = toNumber(row.size) || 0;
  const price = toNumber(row.price) || 0;
  if (!(size > 0) || !(price > 0)) return;
  const dte = daysBetween(dayIso, parsed.expiration);
  const premium = size * price * 100;
  const target = getOptionAgg(optionByMinute, minuteMs, groups)[group];
  target.trade_count += 1;
  target.trade_size += size;
  target.trade_premium += premium;
  if (dte !== null && dte >= 0 && dte <= 7) target.trade_near_dte_size += size;
  if (parsed.right === 'CALL') {
    target.trade_call_size += size;
    target.trade_call_premium += premium;
  } else {
    target.trade_put_size += size;
    target.trade_put_premium += premium;
  }
}

function addOptionQuote(optionByMinute, dayIso, row, rootToGroup, groups, session) {
  const parsed = parseOpraTicker(row.ticker);
  if (!parsed) return;
  const group = rootToGroup.get(parsed.root);
  if (!group) return;
  const minuteMs = nsToMinuteMs(row.window_start);
  if (!Number.isFinite(minuteMs) || !isRegularSessionMs(minuteMs, session)) return;
  const volume = toNumber(row.volume) || 0;
  const close = toNumber(row.close) || 0;
  if (!(volume > 0) || !(close > 0)) return;
  const dte = daysBetween(dayIso, parsed.expiration);
  const premium = volume * close * 100;
  const target = getOptionAgg(optionByMinute, minuteMs, groups)[group];
  target.quote_volume += volume;
  target.quote_premium += premium;
  if (dte !== null && dte >= 0 && dte <= 7) target.quote_near_dte_volume += volume;
  if (parsed.right === 'CALL') {
    target.quote_call_volume += volume;
    target.quote_call_premium += premium;
  } else {
    target.quote_put_volume += volume;
    target.quote_put_premium += premium;
  }
}

function addStockMinuteFeatures(rows) {
  rows.sort((left, right) => left.minuteMs - right.minuteMs);
  rows.forEach((row, index) => {
    row.ret1 = safeReturn(row.close, rows[index - 1]?.close);
    row.ret5 = safeReturn(row.close, rows[index - 5]?.close);
    row.ret15 = safeReturn(row.close, rows[index - 15]?.close);
  });
  return rows;
}

function parseEnvFileLine(rawLine) {
  const line = String(rawLine || '').trim();
  if (!line || line.startsWith('#')) return null;
  const splitIndex = line.indexOf('=');
  if (splitIndex <= 0) return null;
  const key = line.slice(0, splitIndex).replace(/^export\s+/, '').trim();
  let value = line.slice(splitIndex + 1).trim();
  if ((value.startsWith('"') && value.endsWith('"')) || (value.startsWith("'") && value.endsWith("'"))) value = value.slice(1, -1);
  return key ? { key, value } : null;
}

function loadDotEnv(envPath) {
  if (!envPath || !fs.existsSync(envPath)) return;
  fs.readFileSync(envPath, 'utf8').split(/\r?\n/).forEach((line) => {
    const parsed = parseEnvFileLine(line);
    if (parsed && process.env[parsed.key] === undefined) process.env[parsed.key] = parsed.value;
  });
}

function loadRestApiKey() {
  [
    path.resolve(__dirname, '..', '..', '..', '.env.local'),
    path.join(os.homedir(), 'config', 'massive', '.env.local'),
  ].forEach(loadDotEnv);
  return String(process.env.MASSIVE_API_KEY || process.env.POLYGON_API_KEY || '').trim();
}

function restSecondAggCachePath(symbol, dayIso) {
  return runtimePath('rest-second-aggs', `${String(symbol).toUpperCase()}-${dayIso}-1s-unadjusted.json`);
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function fetchJsonWithRetry(url, maxAttempts = 6) {
  let delay = 750;
  for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
    const response = await fetch(url);
    if (response.ok) return response.json();
    const body = await response.text().catch(() => '');
    const retryable = response.status === 408 || response.status === 425 || response.status === 429 || response.status >= 500;
    if (!retryable || attempt === maxAttempts) throw new Error(`rest_second_aggs_failed:${response.status}:${body.slice(0, 160)}`);
    await sleep(delay);
    delay = Math.min(delay * 1.8, 8000);
  }
  throw new Error('rest_second_aggs_failed:unknown');
}

async function fetchRestSecondAggs(symbol, dayIso) {
  const cachePath = restSecondAggCachePath(symbol, dayIso);
  if (fs.existsSync(cachePath)) {
    const payload = JSON.parse(fs.readFileSync(cachePath, 'utf8'));
    return Array.isArray(payload.results) ? payload.results : [];
  }
  const apiKey = loadRestApiKey();
  if (!apiKey) throw new Error('missing_massive_api_key_for_rest_seconds');
  const results = [];
  let url = new URL(`https://api.massive.com/v2/aggs/ticker/${encodeURIComponent(symbol)}/range/1/second/${dayIso}/${dayIso}`);
  url.searchParams.set('adjusted', 'false');
  url.searchParams.set('sort', 'asc');
  url.searchParams.set('limit', '50000');
  url.searchParams.set('apiKey', apiKey);
  for (let page = 0; page < 10 && url; page += 1) {
    const payload = await fetchJsonWithRetry(url);
    if (Array.isArray(payload.results)) results.push(...payload.results);
    if (!payload.next_url) break;
    url = new URL(payload.next_url);
    url.searchParams.set('apiKey', apiKey);
  }
  ensureDir(path.dirname(cachePath));
  fs.writeFileSync(cachePath, `${JSON.stringify({
    ticker: symbol,
    queryDate: dayIso,
    adjusted: false,
    resultsCount: results.length,
    results,
  })}\n`, 'utf8');
  return results;
}

async function readRestSecondAggTradesForDay(config, dayIso, symbol = config.target) {
  const rows = await fetchRestSecondAggs(String(symbol || '').toUpperCase(), dayIso);
  const trades = [];
  rows.forEach((row) => {
    const tsMs = Number(row.t);
    if (!Number.isFinite(tsMs) || !isRegularSessionMs(tsMs, config.session)) return;
    [
      { offset: 0, price: Number(row.o), size: 1 },
      { offset: 250, price: Number(row.h), size: 1 },
      { offset: 500, price: Number(row.l), size: 1 },
      { offset: 750, price: Number(row.c), size: Number(row.v) || 1 },
    ].filter((point) => point.price > 0).forEach((point) => {
      trades.push({
        tsMs: tsMs + point.offset,
        price: point.price,
        size: point.size,
        exchange: 'REST_1S',
        conditions: 'synthetic_ohlc',
      });
    });
  });
  trades.sort((left, right) => left.tsMs - right.tsMs);
  return trades;
}

async function readTargetTradesForDay(config, dayIso, symbol = config.target, settings = {}) {
  if (settings.useRestSeconds) return readRestSecondAggTradesForDay(config, dayIso, symbol);
  const filePath = datasetCsvPath(config, 'stockTrades', dayIso);
  const trades = [];
  if (!fs.existsSync(filePath)) return readRestSecondAggTradesForDay(config, dayIso, symbol);
  const wanted = String(symbol || '').toUpperCase();
  let seenWanted = false;
  await readGzipCsv(filePath, (row) => {
    const ticker = String(row.ticker || '').toUpperCase();
    if (ticker !== wanted) {
      if (seenWanted && ticker > wanted) return false;
      return undefined;
    }
    seenWanted = true;
    if (String(row.correction || '0') !== '0') return;
    const tsMs = nsToMs(row.sip_timestamp);
    if (!Number.isFinite(tsMs) || !isRegularSessionMs(tsMs, config.session)) return;
    const price = toNumber(row.price);
    const size = toNumber(row.size) || 0;
    if (!(price > 0) || !(size > 0)) return;
    trades.push({
      tsMs,
      price,
      size,
      exchange: row.exchange,
      conditions: row.conditions,
    });
  });
  trades.sort((left, right) => left.tsMs - right.tsMs);
  return trades;
}

function duckdbString(value) {
  return `'${String(value).replace(/'/g, "''")}'`;
}

function stockParquetSql(filePath) {
  return `COPY (
    SELECT ticker, volume, open, close, high, low, window_start, COALESCE(CAST(transactions AS VARCHAR), '0') AS transactions
    FROM read_parquet(${duckdbString(filePath)})
  ) TO STDOUT WITH (FORMAT CSV, HEADER TRUE);`;
}

async function streamParquetRows(filePath, onRow) {
  const child = spawn(process.env.DUCKDB_BIN || 'duckdb', ['-c', stockParquetSql(filePath)], {
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  const stderr = [];
  child.stderr.on('data', (chunk) => stderr.push(String(chunk)));
  const reader = readline.createInterface({ input: child.stdout, crlfDelay: Infinity });
  let headers = null;
  for await (const line of reader) {
    if (!line) continue;
    if (!headers) {
      headers = String(line).split(',');
      continue;
    }
    const values = String(line).split(',');
    const row = {};
    headers.forEach((header, index) => {
      row[header] = values[index] ?? '';
    });
    await onRow(row);
  }
  const code = await new Promise((resolve, reject) => {
    child.on('error', reject);
    child.on('close', resolve);
  });
  if (code !== 0) throw new Error(`duckdb_stock_parquet_read_failed:${filePath}:${stderr.join('').trim() || code}`);
}

async function readStockMinutesForDay(config, dayIso, symbols = config.marketSymbols) {
  const source = resolveDatasetSource(config, 'stockBars', dayIso);
  const wanted = new Set(symbols.map((symbol) => String(symbol).toUpperCase()));
  const bySymbol = new Map();
  if (source.format === 'missing') return bySymbol;
  function onRow(row) {
    const symbol = String(row.ticker || '').toUpperCase();
    if (!wanted.has(symbol)) return;
    const minuteMs = nsToMinuteMs(row.window_start);
    if (!Number.isFinite(minuteMs) || !isRegularSessionMs(minuteMs, config.session)) return;
    const list = bySymbol.get(symbol) || [];
    list.push({
      symbol,
      minuteMs,
      open: toNumber(row.open),
      high: toNumber(row.high),
      low: toNumber(row.low),
      close: toNumber(row.close),
      volume: toNumber(row.volume) || 0,
      transactions: toNumber(row.transactions) || 0,
    });
    bySymbol.set(symbol, list);
  }
  if (source.format === 'parquet') await streamParquetRows(source.filePath, onRow);
  else await readGzipCsv(source.filePath, onRow);
  bySymbol.forEach((rows, symbol) => bySymbol.set(symbol, addStockMinuteFeatures(rows)));
  return bySymbol;
}

function ema(values, period) {
  if (!values.length) return null;
  const alpha = 2 / (period + 1);
  let out = values[0];
  for (let index = 1; index < values.length; index += 1) {
    out = (values[index] * alpha) + (out * (1 - alpha));
  }
  return out;
}

function trueRange(row, previousRow) {
  if (!row) return 0;
  const highLow = row.high - row.low;
  if (!previousRow) return highLow;
  return Math.max(
    highLow,
    Math.abs(row.high - previousRow.close),
    Math.abs(row.low - previousRow.close),
  );
}

function dailyBarFromMinutes(dayIso, rows) {
  if (!rows?.length) return null;
  return {
    date: dayIso,
    open: rows[0].open,
    high: Math.max(...rows.map((row) => row.high)),
    low: Math.min(...rows.map((row) => row.low)),
    close: rows[rows.length - 1].close,
    volume: rows.reduce((sum, row) => sum + (row.volume || 0), 0),
  };
}

async function readDailyBarsForDates(config, dates, symbols = config.marketSymbols) {
  const bySymbol = new Map(symbols.map((symbol) => [symbol, []]));
  for (const dayIso of dates) {
    const stockMinutes = await readStockMinutesForDay(config, dayIso, symbols);
    symbols.forEach((symbol) => {
      const daily = dailyBarFromMinutes(dayIso, stockMinutes.get(symbol));
      if (daily) bySymbol.get(symbol).push(daily);
    });
  }
  return bySymbol;
}

function buildSymbolDailyContext(history) {
  const previous = history[history.length - 1];
  if (!previous) return { ready: 0 };
  const previous2 = history[history.length - 2];
  const closes = history.map((row) => row.close).filter((value) => Number.isFinite(value));
  const last5 = history.slice(-5);
  const ranges = history.map((row) => row.high - row.low);
  const last7Ranges = ranges.slice(-7);
  const trueRanges = history.map((row, index) => trueRange(row, history[index - 1]));
  const atrWindow = trueRanges.slice(-14);
  const atr14 = atrWindow.length
    ? atrWindow.reduce((sum, value) => sum + value, 0) / atrWindow.length
    : previous.high - previous.low;
  const ema8 = ema(closes, 8);
  const ema21 = ema(closes, 21);
  const ret5Base = last5[0]?.close;
  return {
    ready: history.length >= 5 ? 1 : 0,
    prevClose: previous.close,
    prevHigh: previous.high,
    prevLow: previous.low,
    prevOpen: previous.open,
    ret1d: safeReturn(previous.close, previous2?.close),
    ret5d: safeReturn(previous.close, ret5Base),
    atr14,
    atrPct14: safeRatio(atr14, previous.close),
    ema8,
    ema21,
    trendUp: previous.close > ema8 && ema8 > ema21 ? 1 : 0,
    trendDown: previous.close < ema8 && ema8 < ema21 ? 1 : 0,
    prevRangePct: safeRatio(previous.high - previous.low, previous.close),
    nr7: last7Ranges.length >= 7 && last7Ranges[last7Ranges.length - 1] <= Math.min(...last7Ranges) ? 1 : 0,
  };
}

async function buildDailyContextByDate(config, selectedDates, settings = {}) {
  if (!selectedDates.length) return { dailyContextByDate: new Map(), dates: [] };
  const warmupDays = Math.max(5, Math.trunc(settings.warmupDays || config.research?.dailyWarmupDays || 35));
  const symbols = settings.symbols || config.marketSymbols || [config.target];
  const startDate = config.dataPolicy?.firstHistoricalDate || selectedDates[0];
  const endDate = selectedDates[selectedDates.length - 1];
  const openDates = loadOpenDates(config, startDate, endDate);
  const firstIndex = Math.max(0, openDates.indexOf(selectedDates[0]));
  const lastIndex = Math.max(firstIndex, openDates.indexOf(endDate));
  const dates = openDates.slice(Math.max(0, firstIndex - warmupDays), lastIndex + 1);
  const dailyBySymbol = await readDailyBarsForDates(config, dates, symbols);
  const dailyContextByDate = new Map();
  selectedDates.forEach((dayIso) => {
    const context = {};
    symbols.forEach((symbol) => {
      const history = (dailyBySymbol.get(symbol) || []).filter((row) => row.date < dayIso);
      context[symbol] = buildSymbolDailyContext(history);
    });
    dailyContextByDate.set(dayIso, context);
  });
  return { dailyContextByDate, dates, symbols };
}

async function readOptionAggsForDay(config, dayIso, { includeOptionTrades = true, includeOptionQuotes = true } = {}) {
  const groups = Object.keys(config.optionRoots || {});
  const rootToGroup = createRootToGroup(config.optionRoots);
  const optionByMinute = new Map();
  const tradePath = datasetCsvPath(config, 'optionTrades', dayIso);
  if (includeOptionTrades && fs.existsSync(tradePath)) {
    await readGzipCsv(tradePath, (row) => addOptionTrade(optionByMinute, dayIso, row, rootToGroup, groups, config.session));
  }
  const quotePath = datasetCsvPath(config, 'optionBars', dayIso);
  if (includeOptionQuotes && fs.existsSync(quotePath)) {
    await readGzipCsv(quotePath, (row) => addOptionQuote(optionByMinute, dayIso, row, rootToGroup, groups, config.session));
  }
  return { optionByMinute, optionGroups: groups };
}

function completedMinuteAtOrBefore(series, state, currentMs) {
  const rows = series || [];
  while (
    state.index + 1 < rows.length
    && rows[state.index + 1].minuteMs + 60000 <= currentMs
  ) {
    state.index += 1;
  }
  return state.index >= 0 ? rows[state.index] : null;
}

function addOptionFeatureRow(row, optionWindow, groups) {
  groups.forEach((group) => {
    const prefix = `opt_${group}`;
    const current = optionWindow[optionWindow.length - 1]?.[group] || createEmptyOptionAgg([group])[group];
    const tradeCall = optionWindow.reduce((sum, item) => sum + (item[group]?.trade_call_size || 0), 0);
    const tradePut = optionWindow.reduce((sum, item) => sum + (item[group]?.trade_put_size || 0), 0);
    const quoteCall = optionWindow.reduce((sum, item) => sum + (item[group]?.quote_call_volume || 0), 0);
    const quotePut = optionWindow.reduce((sum, item) => sum + (item[group]?.quote_put_volume || 0), 0);
    row[`${prefix}_trade_size_1m`] = current.trade_size || 0;
    row[`${prefix}_trade_count_1m`] = current.trade_count || 0;
    row[`${prefix}_trade_imbalance_5m`] = safeRatio(tradeCall - tradePut, tradeCall + tradePut);
    row[`${prefix}_quote_volume_1m`] = current.quote_volume || 0;
    row[`${prefix}_quote_imbalance_5m`] = safeRatio(quoteCall - quotePut, quoteCall + quotePut);
    row[`${prefix}_near_dte_trade_share_1m`] = safeRatio(current.trade_near_dte_size || 0, current.trade_size || 0);
    row[`${prefix}_near_dte_quote_share_1m`] = safeRatio(current.quote_near_dte_volume || 0, current.quote_volume || 0);
  });
}

function rowsForRegularSession(dayIso, session, barSeconds) {
  const openMs = etMinuteToUtcMs(dayIso, session.regularOpenMinuteEt);
  const closeMs = etMinuteToUtcMs(dayIso, session.regularCloseMinuteEt);
  if (!Number.isFinite(openMs) || !Number.isFinite(closeMs)) return [];
  const stepMs = barSeconds * 1000;
  const out = [];
  for (let ms = openMs; ms < closeMs; ms += stepMs) out.push(ms);
  return out;
}

function addDailyContextFeatures(row, config, dailyContext, sessionOpen, dayHighSoFar, dayLowSoFar) {
  const symbols = config.marketSymbols || [];
  symbols.forEach((symbol) => {
    const context = dailyContext?.[symbol] || { ready: 0 };
    const key = symbol.toLowerCase();
    row[`daily_${key}_ready`] = context.ready || 0;
    row[`daily_${key}_prev_close`] = context.prevClose ?? null;
    row[`daily_${key}_ret_1d`] = context.ret1d || 0;
    row[`daily_${key}_ret_5d`] = context.ret5d || 0;
    row[`daily_${key}_atr_pct14`] = context.atrPct14 || 0;
    row[`daily_${key}_trend_up`] = context.trendUp || 0;
    row[`daily_${key}_trend_down`] = context.trendDown || 0;
    row[`daily_${key}_prev_range_pct`] = context.prevRangePct || 0;
    row[`daily_${key}_nr7`] = context.nr7 || 0;
    row[`daily_${key}_gap_pct`] = safeReturn(sessionOpen, context.prevClose);
  });
  const targetContext = dailyContext?.[config.target] || {};
  row.daily_context_ready = targetContext.ready || 0;
  row.daily_tsll_from_prev_close_atr = safeRatio(row.close - targetContext.prevClose, targetContext.atr14);
  row.daily_tsll_open_gap_atr = safeRatio(sessionOpen - targetContext.prevClose, targetContext.atr14);
  row.daily_tsll_range_so_far_atr = safeRatio(dayHighSoFar - dayLowSoFar, targetContext.atr14);
  row.daily_macro_trend_up = (
    (dailyContext?.TSLA?.trendUp || 0)
    + (dailyContext?.QQQ?.trendUp || 0)
    + (dailyContext?.SPY?.trendUp || 0)
  ) >= 2 ? 1 : 0;
}

function addOpeningRangeFeatures(bars) {
  const windows = [5, 15, 30];
  windows.forEach((minutes) => {
    const openingRows = bars.filter((row) => row.minutes_from_open < minutes);
    const high = openingRows.length ? Math.max(...openingRows.map((row) => row.high)) : null;
    const low = openingRows.length ? Math.min(...openingRows.map((row) => row.low)) : null;
    const close = openingRows[openingRows.length - 1]?.close ?? null;
    bars.forEach((row) => {
      const prefix = `orb${minutes}`;
      row[`${prefix}_complete`] = row.minutes_from_open >= minutes ? 1 : 0;
      row[`${prefix}_high`] = high;
      row[`${prefix}_low`] = low;
      row[`${prefix}_range_cents`] = high && low ? (high - low) * 100 : 0;
      row[`${prefix}_breakout_cents`] = row[`${prefix}_complete`] && high ? (row.close - high) * 100 : 0;
      row[`${prefix}_breakdown_cents`] = row[`${prefix}_complete`] && low ? (low - row.close) * 100 : 0;
      row[`${prefix}_return`] = row[`${prefix}_complete`] ? safeReturn(close, openingRows[0]?.open) : 0;
    });
  });
}

function buildFiveSecondBars({ config, dayIso, trades, stockMinutes, optionByMinute, optionGroups, barSeconds, dailyContext }) {
  if (!trades.length) return [];
  const stepMs = Math.max(1, Math.trunc(barSeconds || 5)) * 1000;
  const bucketToTicks = new Map();
  trades.forEach((trade) => {
    const bucketMs = Math.floor(trade.tsMs / stepMs) * stepMs;
    const list = bucketToTicks.get(bucketMs) || [];
    list.push(trade);
    bucketToTicks.set(bucketMs, list);
  });

  const states = new Map();
  (config.marketSymbols || []).forEach((symbol) => states.set(symbol, { index: -1 }));
  const optionWindow = [];
  const optionMinutesSeen = new Set();
  const bars = [];
  let previousClose = null;
  let cumulativeDollarVolume = 0;
  let cumulativeVolume = 0;
  let sessionOpen = null;
  let dayHighSoFar = -Infinity;
  let dayLowSoFar = Infinity;

  rowsForRegularSession(dayIso, config.session, Math.max(1, Math.trunc(barSeconds || 5))).forEach((bucketMs) => {
    const ticks = bucketToTicks.get(bucketMs) || [];
    let open = previousClose;
    let high = previousClose;
    let low = previousClose;
    let close = previousClose;
    let volume = 0;
    if (ticks.length) {
      open = ticks[0].price;
      close = ticks[ticks.length - 1].price;
      high = Math.max(...ticks.map((tick) => tick.price));
      low = Math.min(...ticks.map((tick) => tick.price));
      volume = ticks.reduce((sum, tick) => sum + tick.size, 0);
      cumulativeDollarVolume += ticks.reduce((sum, tick) => sum + (tick.price * tick.size), 0);
      cumulativeVolume += volume;
    }
    if (!Number.isFinite(close)) return;
    if (!Number.isFinite(sessionOpen)) sessionOpen = open;
    dayHighSoFar = Math.max(dayHighSoFar, high);
    dayLowSoFar = Math.min(dayLowSoFar, low);
    previousClose = close;

    const et = getEtParts(bucketMs);
    const row = {
      tradeDate: dayIso,
      tsUtc: new Date(bucketMs).toISOString(),
      tsMs: bucketMs,
      minuteOfDayEt: et.minuteOfDayEt,
      secondOfDayEt: et.secondOfDayEt,
      open,
      high,
      low,
      close,
      volume,
      trade_count: ticks.length,
      vwap: cumulativeVolume > 0 ? cumulativeDollarVolume / cumulativeVolume : close,
      minutes_from_open: et.minuteOfDayEt - config.session.regularOpenMinuteEt,
      minutes_to_close: config.session.regularCloseMinuteEt - et.minuteOfDayEt - 1,
    };
    addDailyContextFeatures(row, config, dailyContext, sessionOpen, dayHighSoFar, dayLowSoFar);

    (config.marketSymbols || []).forEach((symbol) => {
      const current = completedMinuteAtOrBefore(stockMinutes.get(symbol), states.get(symbol), bucketMs);
      const key = symbol.toLowerCase();
      row[`${key}_minute_close`] = current?.close ?? null;
      row[`${key}_ret_1m`] = current?.ret1 || 0;
      row[`${key}_ret_5m`] = current?.ret5 || 0;
      row[`${key}_ret_15m`] = current?.ret15 || 0;
      row[`${key}_minute_volume_log`] = Math.log1p(current?.volume || 0);
    });

    if (optionGroups.length) {
      const completedMinuteMs = Math.floor((bucketMs - 1) / 60000) * 60000;
      if (!optionMinutesSeen.has(completedMinuteMs)) {
        optionWindow.push(optionByMinute.get(completedMinuteMs) || createEmptyOptionAgg(optionGroups));
        optionMinutesSeen.add(completedMinuteMs);
        if (optionWindow.length > 5) optionWindow.shift();
      }
      addOptionFeatureRow(row, optionWindow, optionGroups);
    }
    bars.push(row);
  });

  bars.forEach((row, index) => {
    const prev1 = bars[index - 1];
    const prev3 = bars[index - 3];
    const prev12 = bars[index - Math.max(1, Math.round(60 / (barSeconds || 5)))];
    const prev36 = bars[index - Math.max(1, Math.round(180 / (barSeconds || 5)))];
    const last3 = bars.slice(Math.max(0, index - 2), index + 1);
    const last6 = bars.slice(Math.max(0, index - 5), index + 1);
    const last12Prev = bars.slice(Math.max(0, index - 12), index);
    const last36Prev = bars.slice(Math.max(0, index - 36), index);
    row.ret_1bar_cents = Number.isFinite(prev1?.close) ? (row.close - prev1.close) * 100 : 0;
    row.ret_3bar_cents = Number.isFinite(prev3?.close) ? (row.close - prev3.close) * 100 : 0;
    row.ret_60s_cents = Number.isFinite(prev12?.close) ? (row.close - prev12.close) * 100 : 0;
    row.ret_180s_cents = Number.isFinite(prev36?.close) ? (row.close - prev36.close) * 100 : 0;
    row.volume_15s = last3.reduce((sum, item) => sum + item.volume, 0);
    row.volume_30s = last6.reduce((sum, item) => sum + item.volume, 0);
    row.prev_high_60s = last12Prev.length ? Math.max(...last12Prev.map((item) => item.high)) : row.high;
    row.prev_low_60s = last12Prev.length ? Math.min(...last12Prev.map((item) => item.low)) : row.low;
    row.prev_high_180s = last36Prev.length ? Math.max(...last36Prev.map((item) => item.high)) : row.high;
    row.prev_low_180s = last36Prev.length ? Math.min(...last36Prev.map((item) => item.low)) : row.low;
    row.vwap_dist_cents = (row.close - row.vwap) * 100;
    row.range_60s_cents = row.prev_high_60s && row.prev_low_60s ? (row.prev_high_60s - row.prev_low_60s) * 100 : 0;
    row.market_ok_1m = row.spy_ret_1m > -0.0005 && row.qqq_ret_1m > -0.0007 ? 1 : 0;
    row.tsla_confirm_1m = row.tsla_ret_1m > 0 ? 1 : 0;
  });
  addOpeningRangeFeatures(bars);
  return bars;
}

async function buildScalpingBarsForDay(config, dayIso, settings = {}) {
  const barSeconds = settings.barSeconds || config.execution?.barSeconds || 5;
  const includeOptions = settings.includeOptions === true;
  const [trades, stockMinutes, optionAggs] = await Promise.all([
    readTargetTradesForDay(config, dayIso, config.target, settings),
    readStockMinutesForDay(config, dayIso, config.marketSymbols),
    includeOptions
      ? readOptionAggsForDay(config, dayIso, settings)
      : Promise.resolve({ optionByMinute: new Map(), optionGroups: [] }),
  ]);
  const dailyContext = settings.dailyContextByDate?.get(dayIso) || {};
  const rows = buildFiveSecondBars({
    config,
    dayIso,
    trades,
    stockMinutes,
    optionByMinute: optionAggs.optionByMinute,
    optionGroups: optionAggs.optionGroups,
    barSeconds,
    dailyContext,
  });
  return {
    dayIso,
    rows,
    counts: {
      trades: trades.length,
      bars: rows.length,
      stockMinuteSymbols: stockMinutes.size,
      optionMinutes: optionAggs.optionByMinute.size,
      includeOptions,
      targetTradeSource: settings.useRestSeconds ? 'massive_rest_1s_aggs' : 'massive_stock_trades_or_rest_1s_fallback',
    },
  };
}

module.exports = {
  safeReturn,
  safeRatio,
  createRootToGroup,
  createEmptyOptionAgg,
  mergeOptionAgg,
  readTargetTradesForDay,
  readStockMinutesForDay,
  readDailyBarsForDates,
  buildDailyContextByDate,
  readOptionAggsForDay,
  buildFiveSecondBars,
  buildScalpingBarsForDay,
};
