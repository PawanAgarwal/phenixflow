#!/usr/bin/env node
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');

const { availableDates } = require('../src/calendar');
const { artifactPath, ensureDir, loadConfig, runtimePath } = require('../src/config');
const { readStockMinutesForDay } = require('../src/data');
const { etMinuteToUtcMs, getEtParts, sessionBounds } = require('../src/time');

function parseArgs(argv = process.argv.slice(2)) {
  const out = {};
  for (let index = 0; index < argv.length; index += 1) {
    const token = argv[index];
    if (!token.startsWith('--')) continue;
    const key = token.slice(2);
    const next = argv[index + 1];
    if (!next || next.startsWith('--')) {
      out[key] = true;
      continue;
    }
    out[key] = next;
    index += 1;
  }
  return out;
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

function round(value, digits = 4) {
  return Number.isFinite(value) ? Number(value.toFixed(digits)) : value;
}

function monthKey(dayIso) {
  return dayIso.slice(0, 7);
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function mapWithConcurrency(items, limit, mapper) {
  const cappedLimit = Math.max(1, Math.floor(limit || 1));
  const results = new Array(items.length);
  let nextIndex = 0;
  const workers = Array.from({ length: Math.min(cappedLimit, items.length) }, async () => {
    while (nextIndex < items.length) {
      const currentIndex = nextIndex;
      nextIndex += 1;
      results[currentIndex] = await mapper(items[currentIndex], currentIndex);
    }
  });
  await Promise.all(workers);
  return results;
}

function sessionSecondMs(dayIso, session, sessionName = 'regular') {
  const bounds = sessionBounds(session, sessionName);
  const openMs = etMinuteToUtcMs(dayIso, bounds.openMinuteEt);
  const closeMs = etMinuteToUtcMs(dayIso, bounds.closeMinuteEt);
  const out = [];
  for (let ms = openMs; ms < closeMs; ms += 1000) out.push(ms);
  return { rows: out, bounds };
}

function completedMinuteAtOrBefore(series, state, currentMs) {
  const rows = series || [];
  while (state.index + 1 < rows.length && rows[state.index + 1].minuteMs + 60000 <= currentMs) {
    state.index += 1;
  }
  return state.index >= 0 ? rows[state.index] : null;
}

function cachePathFor(symbol, dayIso) {
  return runtimePath('rest-second-aggs-cross', `${symbol}-${dayIso}-1s-unadjusted.json`);
}

function legacyTsllCachePath(dayIso) {
  return runtimePath('rest-second-aggs', `TSLL-${dayIso}-1s-unadjusted.json`);
}

async function fetchJsonWithRetry(url, { maxAttempts = 6 } = {}) {
  let delay = 750;
  for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
    const response = await fetch(url);
    if (response.ok) return response.json();
    const body = await response.text().catch(() => '');
    const retryable = response.status === 408 || response.status === 425 || response.status === 429 || response.status >= 500;
    if (!retryable || attempt === maxAttempts) throw new Error(`rest_fetch_failed:${response.status}:${body.slice(0, 160)}`);
    await sleep(delay);
    delay = Math.min(delay * 1.8, 8000);
  }
  throw new Error('rest_fetch_failed:unknown');
}

async function fetchRestAggs(symbol, dayIso, apiKey) {
  const outPath = cachePathFor(symbol, dayIso);
  if (fs.existsSync(outPath)) {
    const payload = JSON.parse(fs.readFileSync(outPath, 'utf8'));
    return Array.isArray(payload.results) ? payload.results : [];
  }
  if (symbol === 'TSLL' && fs.existsSync(legacyTsllCachePath(dayIso))) {
    const payload = JSON.parse(fs.readFileSync(legacyTsllCachePath(dayIso), 'utf8'));
    return Array.isArray(payload.results) ? payload.results : [];
  }
  if (!apiKey) throw new Error('missing_massive_api_key');

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

  ensureDir(path.dirname(outPath));
  fs.writeFileSync(outPath, `${JSON.stringify({
    ticker: symbol,
    queryDate: dayIso,
    adjusted: false,
    resultsCount: results.length,
    results,
  })}\n`);
  return results;
}

function safeReturn(current, previous) {
  if (!Number.isFinite(current) || !Number.isFinite(previous) || previous <= 0) return 0;
  return (current / previous) - 1;
}

function addRollingRange(rows, windowSeconds, highKey, lowKey, rangeKey) {
  const maxDeque = [];
  const minDeque = [];
  for (let index = 0; index < rows.length; index += 1) {
    const previousIndex = index - 1;
    if (previousIndex >= 0) {
      while (maxDeque.length && rows[maxDeque[maxDeque.length - 1]].high <= rows[previousIndex].high) maxDeque.pop();
      while (minDeque.length && rows[minDeque[minDeque.length - 1]].low >= rows[previousIndex].low) minDeque.pop();
      maxDeque.push(previousIndex);
      minDeque.push(previousIndex);
    }
    const minIndex = index - windowSeconds;
    while (maxDeque.length && maxDeque[0] < minIndex) maxDeque.shift();
    while (minDeque.length && minDeque[0] < minIndex) minDeque.shift();
    rows[index][highKey] = maxDeque.length ? rows[maxDeque[0]].high : rows[index].high;
    rows[index][lowKey] = minDeque.length ? rows[minDeque[0]].low : rows[index].low;
    rows[index][rangeKey] = (rows[index][highKey] - rows[index][lowKey]) * 100;
  }
}

function addSecondFeatures(rows) {
  let cumulativeVolume = 0;
  let cumulativeDollarVolume = 0;
  let rollingVolume60 = 0;
  let rollingDollar60 = 0;
  rows.forEach((row, index) => {
    const typical = (row.high + row.low + row.close) / 3;
    cumulativeVolume += row.volume || 0;
    cumulativeDollarVolume += typical * (row.volume || 0);
    rollingVolume60 += row.volume || 0;
    rollingDollar60 += row.close * (row.volume || 0);
    if (index >= 60) {
      rollingVolume60 -= rows[index - 60].volume || 0;
      rollingDollar60 -= rows[index - 60].close * (rows[index - 60].volume || 0);
    }
    row.index = index;
    row.ret_1s = safeReturn(row.close, rows[index - 1]?.close);
    row.ret_30s = safeReturn(row.close, rows[index - 30]?.close);
    row.ret_60s = safeReturn(row.close, rows[index - 60]?.close);
    row.ret_180s = safeReturn(row.close, rows[index - 180]?.close);
    row.ret_300s = safeReturn(row.close, rows[index - 300]?.close);
    row.ret_900s = safeReturn(row.close, rows[index - 900]?.close);
    row.ret_1800s = safeReturn(row.close, rows[index - 1800]?.close);
    row.vwap = cumulativeVolume > 0 ? cumulativeDollarVolume / cumulativeVolume : row.close;
    row.volume_60s = rollingVolume60;
    row.dollar_volume_60s = rollingDollar60;
    row.minuteRangeCents = (row.high - row.low) * 100;
  });
  addRollingRange(rows, 30, 'prior_high_30s', 'prior_low_30s', 'prior_range_30s_cents');
  addRollingRange(rows, 60, 'prior_high_60s', 'prior_low_60s', 'prior_range_60s_cents');
  addRollingRange(rows, 180, 'prior_high_180s', 'prior_low_180s', 'prior_range_180s_cents');
  addRollingRange(rows, 300, 'prior_high_300s', 'prior_low_300s', 'prior_range_300s_cents');
  addRollingRange(rows, 900, 'prior_high_900s', 'prior_low_900s', 'prior_range_900s_cents');
}

async function buildRowsForDay({ config, symbol, dayIso, apiKey, sessionName }) {
  const restRows = await fetchRestAggs(symbol, dayIso, apiKey);
  const stockMinutes = await readStockMinutesForDay(config, dayIso, ['SPY', 'QQQ'], { sessionName });
  const bySecond = new Map(restRows.map((row) => [row.t, row]));
  const spyState = { index: -1 };
  const qqqState = { index: -1 };
  const rows = [];
  let previousClose = null;
  const sessionRows = sessionSecondMs(dayIso, config.session, sessionName);
  sessionRows.rows.forEach((ms) => {
    const tick = bySecond.get(ms);
    let open = previousClose;
    let high = previousClose;
    let low = previousClose;
    let close = previousClose;
    let volume = 0;
    let tradeCount = 0;
    if (tick) {
      open = Number(tick.o);
      high = Number(tick.h);
      low = Number(tick.l);
      close = Number(tick.c);
      volume = Number(tick.v) || 0;
      tradeCount = Number(tick.n) || 0;
    }
    if (!Number.isFinite(close)) return;
    previousClose = close;
    const et = getEtParts(ms);
    const spy = completedMinuteAtOrBefore(stockMinutes.get('SPY'), spyState, ms);
    const qqq = completedMinuteAtOrBefore(stockMinutes.get('QQQ'), qqqState, ms);
    rows.push({
      tradeDate: dayIso,
      symbol,
      tsMs: ms,
      tsUtc: new Date(ms).toISOString(),
      minuteOfDayEt: et.minuteOfDayEt,
      secondOfDayEt: et.secondOfDayEt,
      secondsFromOpen: et.secondOfDayEt - (sessionRows.bounds.openMinuteEt * 60),
      secondsToClose: (sessionRows.bounds.closeMinuteEt * 60) - et.secondOfDayEt - 1,
      open,
      high,
      low,
      close,
      volume,
      trade_count: tradeCount,
      spy_ret_1m: spy?.ret1 || 0,
      spy_ret_5m: spy?.ret5 || 0,
      qqq_ret_1m: qqq?.ret1 || 0,
      qqq_ret_5m: qqq?.ret5 || 0,
    });
  });
  addSecondFeatures(rows);
  return { rows, restRows: restRows.length };
}

function centsFromBps(price, bps, minCents, maxCents) {
  return Math.min(maxCents, Math.max(minCents, price * 100 * bps / 10000));
}

function buildStrategies() {
  const shapes = [
    { name: 's15', bps: 4, targetMult: 1.15, stopMult: 1.7, holdSeconds: 15, minRange: 4, rangeKey: 'prior_range_30s_cents', rangeMult: 0.25 },
    { name: 's30', bps: 6, targetMult: 1.25, stopMult: 1.8, holdSeconds: 30, minRange: 6, rangeKey: 'prior_range_60s_cents', rangeMult: 0.25 },
    { name: 's60', bps: 10, targetMult: 1.35, stopMult: 1.9, holdSeconds: 60, minRange: 8, rangeKey: 'prior_range_180s_cents', rangeMult: 0.22 },
    { name: 'm5', bps: 16, targetMult: 1.4, stopMult: 1.9, holdSeconds: 300, minRange: 15, rangeKey: 'prior_range_300s_cents', rangeMult: 0.2 },
    { name: 'm15', bps: 20, targetMult: 1.5, stopMult: 2.0, holdSeconds: 900, minRange: 25, rangeKey: 'prior_range_900s_cents', rangeMult: 0.18 },
  ];
  const filters = ['trend_pullback', 'strong_trend_dip', 'vwap_dip', 'opening_trend_dip', 'deep_dip', 'market_rebound'];
  return shapes.flatMap((shape) => filters.map((filterName) => ({
    id: `${filterName}_${shape.name}_${shape.bps}bps`,
    shape,
    filterName,
  })));
}

function resolvePlan(row, strategy) {
  const base = centsFromBps(row.close, strategy.shape.bps, 3, 300);
  const range = Math.max(base, (row[strategy.shape.rangeKey] || 0) * strategy.shape.rangeMult, strategy.shape.minRange);
  return {
    buyCents: range,
    targetCents: Math.max(3, range * strategy.shape.targetMult),
    stopCents: Math.max(5, range * strategy.shape.stopMult),
    holdSeconds: strategy.shape.holdSeconds,
    cooldownSeconds: Math.min(15, Math.max(2, Math.round(strategy.shape.holdSeconds * 0.15))),
  };
}

function passesFilter(row, strategy) {
  if (row.secondsFromOpen < 10 * 60 || row.secondsToClose < 15 * 60) return false;
  if (row.dollar_volume_60s < 50000) return false;
  if ((row[strategy.shape.rangeKey] || 0) < strategy.shape.minRange) return false;
  if (row.spy_ret_1m < -0.002 || row.qqq_ret_1m < -0.0025) return false;
  if (strategy.filterName === 'trend_pullback') {
    return row.ret_900s > -0.003 && row.ret_60s > -0.012 && row.ret_30s <= 0.002;
  }
  if (strategy.filterName === 'strong_trend_dip') {
    return row.ret_900s > 0.004 && row.ret_1800s > -0.002 && row.ret_30s <= 0.003 && row.qqq_ret_5m > -0.0015;
  }
  if (strategy.filterName === 'vwap_dip') {
    return row.close > row.vwap && row.low <= row.vwap * 1.01 && row.ret_900s > 0 && row.ret_300s > -0.01 && row.spy_ret_5m > -0.0025;
  }
  if (strategy.filterName === 'opening_trend_dip') {
    return row.secondsFromOpen >= 20 * 60 && row.secondsFromOpen <= 150 * 60 && row.ret_900s > 0.006 && row.ret_30s <= 0.003 && row.qqq_ret_5m > -0.001;
  }
  if (strategy.filterName === 'deep_dip') {
    return row.ret_60s < -0.001 && row.ret_300s > -0.025 && row.spy_ret_5m > -0.004;
  }
  if (strategy.filterName === 'market_rebound') {
    return row.qqq_ret_5m > -0.002 && row.ret_180s < 0.002 && row.ret_300s > -0.02;
  }
  return true;
}

function simulateDay(rows, strategy, runConfig) {
  const trades = [];
  let index = 1800;
  while (index < rows.length - 3) {
    const signal = rows[index];
    const entryBar = rows[index + 1];
    if (!passesFilter(signal, strategy)) {
      index += 1;
      continue;
    }
    const plan = resolvePlan(signal, strategy);
    const entryLimit = signal.close - (plan.buyCents / 100);
    if ((entryBar.trade_count || 0) <= 0 || entryBar.low > entryLimit) {
      index += 1;
      continue;
    }
    const target = entryLimit + (plan.targetCents / 100);
    const stop = entryLimit - (plan.stopCents / 100);
    const lastIndex = Math.min(rows.length - 1, index + 1 + plan.holdSeconds);
    const firstExitIndex = Math.min(lastIndex, index + runConfig.exitStartOffsetSeconds);
    let exitIndex = firstExitIndex;
    let exitPrice = rows[firstExitIndex].close;
    let exitReason = 'timeout';
    for (let cursor = firstExitIndex; cursor <= lastIndex; cursor += 1) {
      const row = rows[cursor];
      exitIndex = cursor;
      exitPrice = row.close;
      if ((row.trade_count || 0) > 0 && row.low <= stop && row.high >= target) {
        exitPrice = stop;
        exitReason = 'stop_same_second';
        break;
      }
      if ((row.trade_count || 0) > 0 && row.low <= stop) {
        exitPrice = stop;
        exitReason = 'stop';
        break;
      }
      if ((row.trade_count || 0) > 0 && row.high >= target) {
        exitPrice = target;
        exitReason = 'target';
        break;
      }
      if (cursor - index >= Math.max(6, Math.round(plan.holdSeconds * 0.45)) && (row.close - entryLimit) * 100 >= plan.targetCents * 0.45) {
        exitPrice = row.close;
        exitReason = 'profit_lock';
        break;
      }
    }
    trades.push({
      grossCents: (exitPrice - entryLimit) * 100,
      entryPrice: entryLimit,
      exitReason,
    });
    index = exitIndex + plan.cooldownSeconds + 1;
  }
  return trades;
}

function createAgg(symbol, strategyId) {
  return {
    symbol,
    strategyId,
    trades: 0,
    wins: 0,
    buyCapital: 0,
    dayStats: [],
    exitReasons: {},
  };
}

function updateAgg(agg, dayIso, trades, costCentsPerSide) {
  const netCents = trades.reduce((sum, trade) => sum + trade.grossCents - (2 * costCentsPerSide), 0);
  const buyCapital = trades.reduce((sum, trade) => sum + trade.entryPrice, 0);
  agg.trades += trades.length;
  agg.wins += trades.filter((trade) => trade.grossCents - (2 * costCentsPerSide) > 0).length;
  agg.buyCapital += buyCapital;
  trades.forEach((trade) => {
    agg.exitReasons[trade.exitReason] = (agg.exitReasons[trade.exitReason] || 0) + 1;
  });
  agg.dayStats.push({
    date: dayIso,
    month: monthKey(dayIso),
    trades: trades.length,
    netCents: round(netCents, 4),
    buyCapital: round(buyCapital, 6),
  });
}

function mean(values) {
  return values.length ? values.reduce((sum, value) => sum + value, 0) / values.length : 0;
}

function stdev(values) {
  if (values.length < 2) return 0;
  const avg = mean(values);
  return Math.sqrt(values.reduce((sum, value) => sum + ((value - avg) ** 2), 0) / (values.length - 1));
}

function sharpe(values) {
  const sd = stdev(values);
  return sd ? mean(values) / sd * Math.sqrt(252) : null;
}

function maxDrawdown(values) {
  let equity = 0;
  let peak = 0;
  let drawdown = 0;
  values.forEach((value) => {
    equity += value;
    peak = Math.max(peak, equity);
    drawdown = Math.min(drawdown, equity - peak);
  });
  return drawdown;
}

function summarizeDays(dayStats, buyCapital, trades) {
  const values = dayStats.map((day) => day.netCents || 0);
  const netCents = values.reduce((sum, value) => sum + value, 0);
  const avgEntry = trades ? buyCapital / trades : 0;
  return {
    days: dayStats.length,
    tradedDays: dayStats.filter((day) => day.trades > 0).length,
    positiveDays: dayStats.filter((day) => day.netCents > 0).length,
    trades,
    netCents: round(netCents, 2),
    pnlPer1000Shares: round(netCents * 10, 2),
    avgCentsPerTrade: trades ? round(netCents / trades, 4) : 0,
    returnPct: avgEntry ? round(((netCents / 100) / avgEntry) * 100, 3) : 0,
    maxDrawdownCents: round(maxDrawdown(values), 2),
    sharpe: round(sharpe(values), 3),
  };
}

function summarizeAgg(agg) {
  const trainDays = agg.dayStats.filter((day) => day.date < '2026-01-01');
  const testDays = agg.dayStats.filter((day) => day.date >= '2026-01-01');
  const sumBuy = (days) => days.reduce((sum, day) => sum + (day.buyCapital || 0), 0);
  const sumTrades = (days) => days.reduce((sum, day) => sum + (day.trades || 0), 0);
  const months = [...new Set(agg.dayStats.map((day) => day.month))].sort().map((month) => {
    const days = agg.dayStats.filter((day) => day.month === month);
    return { month, ...summarizeDays(days, sumBuy(days), sumTrades(days)) };
  });
  const overall = summarizeDays(agg.dayStats, agg.buyCapital, agg.trades);
  overall.winRate = agg.trades ? round(agg.wins / agg.trades, 6) : 0;
  return {
    symbol: agg.symbol,
    strategyId: agg.strategyId,
    overall,
    train2025: summarizeDays(trainDays, sumBuy(trainDays), sumTrades(trainDays)),
    test2026: summarizeDays(testDays, sumBuy(testDays), sumTrades(testDays)),
    months,
    exitReasons: agg.exitReasons,
  };
}

function rankResults(results) {
  return [...results].filter((item) => item.overall.trades >= 25).sort((left, right) => {
    const leftPass = left.overall.netCents > 0 && left.test2026.netCents > 0 ? 1 : 0;
    const rightPass = right.overall.netCents > 0 && right.test2026.netCents > 0 ? 1 : 0;
    if (rightPass !== leftPass) return rightPass - leftPass;
    const leftScore = (left.test2026.sharpe || -99) * 2 + (left.overall.sharpe || -99);
    const rightScore = (right.test2026.sharpe || -99) * 2 + (right.overall.sharpe || -99);
    if (rightScore !== leftScore) return rightScore - leftScore;
    return right.overall.netCents - left.overall.netCents;
  });
}

function renderMarkdown(payload) {
  const lines = [
    '# Adaptive 1-Second Scalp Backtest',
    '',
    `Window: ${payload.startDate} to ${payload.endDate}`,
    `Symbols: ${payload.symbols.join(', ')}`,
    `Cost: ${payload.costCentsPerSide} cents/side`,
    `Exit start offset: ${payload.exitStartOffsetSeconds} seconds after signal`,
    '',
    '## Best By Symbol',
    '',
    '| Symbol | Strategy | Trades | Net c/share | Sharpe | 2026 net | 2026 Sharpe | Max DD c/share |',
    '| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |',
    ...payload.bestBySymbol.map((item) => `| ${item.symbol} | ${item.strategyId} | ${item.overall.trades} | ${item.overall.netCents.toFixed(2)} | ${item.overall.sharpe?.toFixed(3) ?? ''} | ${item.test2026.netCents.toFixed(2)} | ${item.test2026.sharpe?.toFixed(3) ?? ''} | ${item.overall.maxDrawdownCents.toFixed(2)} |`),
    '',
    '## Top Runs',
    '',
    '| Rank | Symbol | Strategy | Trades | Net c/share | Sharpe | 2026 net | 2026 Sharpe |',
    '| ---: | --- | --- | ---: | ---: | ---: | ---: | ---: |',
    ...payload.topRuns.slice(0, 50).map((item, index) => `| ${index + 1} | ${item.symbol} | ${item.strategyId} | ${item.overall.trades} | ${item.overall.netCents.toFixed(2)} | ${item.overall.sharpe?.toFixed(3) ?? ''} | ${item.test2026.netCents.toFixed(2)} | ${item.test2026.sharpe?.toFixed(3) ?? ''} |`),
    '',
    '## Method Note',
    '',
    'This uses Massive unadjusted 1-second aggregate trade bars. It requires a traded second for passive entry and disallows same-second exits, but it still is not NBBO queue proof.',
    '',
  ];
  return `${lines.join('\n')}\n`;
}

async function main() {
  const args = parseArgs();
  const config = loadConfig();
  const startDate = args.start || '2025-01-02';
  const endDate = args.end || '2026-05-08';
  const symbols = String(args.symbols || 'SNDK,UPST,PSIX,HYMC,APP,SOXL,TEM,TNXP,TSLL,TSLA,NVDA,MSTR')
    .split(',')
    .map((symbol) => symbol.trim().toUpperCase())
    .filter(Boolean);
  const costCentsPerSide = Number(args.costCentsPerSide ?? 0.5);
  const sessionName = String(args.session || args.sessionName || 'regular').trim().toLowerCase();
  const concurrency = Number(args.concurrency ?? 5);
  const batchSize = Number(args.batchSize ?? Math.max(20, concurrency * 8));
  const exitStartOffsetSeconds = Number(args.exitStartOffsetSeconds ?? 2);
  const apiKey = loadRestApiKey();
  const dates = availableDates(config, startDate, endDate, ['stockBars']);
  const strategyIds = args.strategyIds
    ? new Set(String(args.strategyIds).split(',').map((item) => item.trim()).filter(Boolean))
    : null;
  const strategies = buildStrategies().filter((strategy) => !strategyIds || strategyIds.has(strategy.id));
  console.log(`[seconds-scalp] symbols=${symbols.join(',')} dates=${dates.length} session=${sessionName} strategies=${strategies.length} concurrency=${concurrency}`);

  const errors = [];
  const allResults = [];
  const startedAt = Date.now();
  for (let symbolIndex = 0; symbolIndex < symbols.length; symbolIndex += 1) {
    const symbol = symbols[symbolIndex];
    const aggs = new Map(strategies.map((strategy) => [strategy.id, createAgg(symbol, strategy.id)]));
    for (let batchStart = 0; batchStart < dates.length; batchStart += batchSize) {
      const batch = dates.slice(batchStart, batchStart + batchSize).map((dayIso, offset) => ({
        dayIso,
        dateIndex: batchStart + offset,
      }));
      const dayResults = await mapWithConcurrency(batch, concurrency, async ({ dayIso, dateIndex }) => {
        try {
          const { rows, restRows } = await buildRowsForDay({ config, symbol, dayIso, apiKey, sessionName });
          const tradesByStrategy = strategies.map((strategy) => ({
            strategyId: strategy.id,
            trades: simulateDay(rows, strategy, { exitStartOffsetSeconds }),
          }));
          return { ok: true, dayIso, dateIndex, rowsLength: rows.length, restRows, tradesByStrategy };
        } catch (error) {
          return { ok: false, dayIso, dateIndex, error: error.message };
        }
      });
      dayResults.sort((left, right) => left.dateIndex - right.dateIndex).forEach((result) => {
        if (!result.ok) {
          errors.push({ symbol, dayIso: result.dayIso, error: result.error });
          console.warn(`[seconds-scalp] error symbol=${symbol} date=${result.dayIso} ${result.error}`);
          return;
        }
        result.tradesByStrategy.forEach(({ strategyId, trades }) => {
          updateAgg(aggs.get(strategyId), result.dayIso, trades, costCentsPerSide);
        });
        if ((result.dateIndex + 1) % 20 === 0 || result.dateIndex === dates.length - 1) {
          const elapsedMin = ((Date.now() - startedAt) / 60000).toFixed(1);
          console.log(`[seconds-scalp] ${symbolIndex + 1}/${symbols.length} ${symbol} ${result.dateIndex + 1}/${dates.length} ${result.dayIso} rows=${result.rowsLength} restRows=${result.restRows} elapsed=${elapsedMin}m`);
        }
      });
    }
    allResults.push(...[...aggs.values()].map(summarizeAgg));
  }

  const ranked = rankResults(allResults);
  const bestBySymbol = symbols.map((symbol) => ranked.find((item) => item.symbol === symbol)).filter(Boolean);
  const payload = {
    generatedAt: new Date().toISOString(),
    startDate,
    endDate,
    sessionName,
    dates: dates.length,
    symbols,
    costCentsPerSide,
    exitStartOffsetSeconds,
    assumptions: {
      data: 'Massive REST unadjusted 1-second stock aggregates plus local Massive 1-minute SPY/QQQ context.',
      session: sessionName === 'regular' ? 'Regular trading hours only.' : 'Extended-hours run using the configured session window.',
      execution: 'Passive buy limit can fill only in a traded second; target/stop checks start after the entry second.',
      caveat: '1-second aggregate OHLC does not prove NBBO queue fill quality.',
    },
    bestBySymbol,
    topRuns: ranked.slice(0, 100),
    results: allResults,
    errors,
  };
  const tag = args.tag ? `-${String(args.tag).replace(/[^A-Za-z0-9._-]/g, '-')}` : '';
  const sessionTag = sessionName !== 'regular' ? `-${sessionName}` : '';
  const slug = `${startDate}-${endDate}${sessionTag}${tag}`;
  const outJson = artifactPath(`adaptive-second-scalp-backtest-${slug}.json`);
  const outMd = artifactPath(`adaptive-second-scalp-backtest-${slug}.md`);
  ensureDir(path.dirname(outJson));
  fs.writeFileSync(outJson, `${JSON.stringify(payload, null, 2)}\n`);
  fs.writeFileSync(outMd, renderMarkdown(payload));
  console.log(JSON.stringify({
    outJson,
    outMd,
    errors: errors.length,
    bestBySymbol: bestBySymbol.map((item) => ({
      symbol: item.symbol,
      strategyId: item.strategyId,
      trades: item.overall.trades,
      netCents: item.overall.netCents,
      sharpe: item.overall.sharpe,
      test2026NetCents: item.test2026.netCents,
      test2026Sharpe: item.test2026.sharpe,
    })),
  }, null, 2));
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
