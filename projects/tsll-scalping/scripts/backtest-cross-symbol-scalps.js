#!/usr/bin/env node
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');

const { availableDates } = require('../src/calendar');
const { artifactPath, ensureDir, loadConfig, runtimePath } = require('../src/config');
const { readStockMinutesForDay } = require('../src/data');
const { etMinuteToUtcMs, getEtParts } = require('../src/time');

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
  if ((value.startsWith('"') && value.endsWith('"')) || (value.startsWith("'") && value.endsWith("'"))) {
    value = value.slice(1, -1);
  }
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

function regularSecondMs(dayIso, session) {
  const openMs = etMinuteToUtcMs(dayIso, session.regularOpenMinuteEt);
  const closeMs = etMinuteToUtcMs(dayIso, session.regularCloseMinuteEt);
  const out = [];
  for (let ms = openMs; ms < closeMs; ms += 1000) out.push(ms);
  return out;
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
    const retryable = response.status === 429 || response.status >= 500;
    if (!retryable || attempt === maxAttempts) {
      throw new Error(`rest_fetch_failed:${response.status}:${body.slice(0, 160)}`);
    }
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

function addRollingFeatures(rows) {
  rows.forEach((row, index) => {
    const prev1 = rows[index - 1];
    const prev60 = rows[index - 60];
    const last12Prev = rows.slice(Math.max(0, index - 12), index);
    row.ret_1bar_cents = Number.isFinite(prev1?.close) ? (row.close - prev1.close) * 100 : 0;
    row.ret_60s_cents = Number.isFinite(prev60?.close) ? (row.close - prev60.close) * 100 : 0;
    row.prev_high_12s = last12Prev.length ? Math.max(...last12Prev.map((item) => item.high)) : row.high;
    row.prev_low_12s = last12Prev.length ? Math.min(...last12Prev.map((item) => item.low)) : row.low;
    row.range_12s_cents = row.prev_high_12s && row.prev_low_12s
      ? (row.prev_high_12s - row.prev_low_12s) * 100
      : 0;
    row.market_ok_1m = row.spy_ret_1m > -0.0005 && row.qqq_ret_1m > -0.0007 ? 1 : 0;
  });
}

async function buildRowsForDay({ config, symbol, dayIso, apiKey }) {
  const restRows = await fetchRestAggs(symbol, dayIso, apiKey);
  const stockMinutes = await readStockMinutesForDay(config, dayIso, ['SPY', 'QQQ', symbol]);
  const bySecond = new Map(restRows.map((row) => [row.t, row]));
  const states = new Map(['SPY', 'QQQ', symbol].map((item) => [item, { index: -1 }]));
  const rows = [];
  let previousClose = null;

  regularSecondMs(dayIso, config.session).forEach((ms) => {
    const tick = bySecond.get(ms);
    let open = previousClose;
    let high = previousClose;
    let low = previousClose;
    let close = previousClose;
    let volume = 0;
    let tradeCount = 0;
    if (tick) {
      open = tick.o;
      high = tick.h;
      low = tick.l;
      close = tick.c;
      volume = tick.v || 0;
      tradeCount = tick.n || 0;
    }
    if (!Number.isFinite(close)) return;
    previousClose = close;
    const et = getEtParts(ms);
    const row = {
      tradeDate: dayIso,
      symbol,
      tsMs: ms,
      tsUtc: new Date(ms).toISOString(),
      minuteOfDayEt: et.minuteOfDayEt,
      open,
      high,
      low,
      close,
      volume,
      trade_count: tradeCount,
      minutes_from_open: et.minuteOfDayEt - config.session.regularOpenMinuteEt,
      minutes_to_close: config.session.regularCloseMinuteEt - et.minuteOfDayEt - 1,
    };
    ['SPY', 'QQQ', symbol].forEach((item) => {
      const current = completedMinuteAtOrBefore(stockMinutes.get(item), states.get(item), ms);
      const key = item === symbol ? 'target' : item.toLowerCase();
      row[`${key}_ret_1m`] = current?.ret1 || 0;
      row[`${key}_ret_5m`] = current?.ret5 || 0;
      row[`${key}_ret_15m`] = current?.ret15 || 0;
    });
    rows.push(row);
  });

  addRollingFeatures(rows);
  return { rows, restRows: restRows.length };
}

function basePasses(row, settings) {
  if (!row) return false;
  if (row.minutes_from_open < settings.noEntryFirstMinutes) return false;
  if (row.minutes_to_close < settings.noEntryLastMinutes) return false;
  if ((row.trade_count || 0) < settings.minTradeCount) return false;
  if ((row.range_12s_cents || 0) < settings.minRangeCents) return false;
  if ((row.ret_60s_cents || 0) < settings.minRet60sCents) return false;
  if ((row.ret_1bar_cents || 0) > settings.maxLastBarUpCents) return false;
  if (settings.requireMarketOk && row.market_ok_1m !== 1) return false;
  if ((row.spy_ret_1m || 0) < settings.minSpyRet1m) return false;
  if ((row.qqq_ret_1m || 0) < settings.minQqqRet1m) return false;
  if ((row.target_ret_1m || 0) < settings.minTargetRet1m) return false;
  return true;
}

function simulateDay(rows, candidate, runConfig = {}) {
  const trades = [];
  const exitStartOffsetBars = Math.max(1, runConfig.exitStartOffsetBars || 1);
  let index = 0;
  while (index < rows.length - 2) {
    const signal = rows[index];
    const entryBar = rows[index + 1];
    const settings = candidate.resolveSettings ? candidate.resolveSettings(signal) : candidate.settings;
    if (!basePasses(signal, settings) || !candidate.filter(signal)) {
      index += 1;
      continue;
    }
    const entryLimit = signal.close - (settings.buyBelowCloseCents / 100);
    if (entryBar.low > entryLimit) {
      index += 1;
      continue;
    }
    const target = entryLimit + (settings.targetCents / 100);
    const stop = entryLimit - (settings.stopCents / 100);
    const lastIndex = Math.min(rows.length - 1, index + 1 + settings.maxHoldBars);
    const firstExitIndex = Math.min(lastIndex, index + exitStartOffsetBars);
    let exitIndex = firstExitIndex;
    let exitPrice = rows[firstExitIndex].close;
    let exitReason = 'timeout';

    for (let cursor = firstExitIndex; cursor <= lastIndex; cursor += 1) {
      const row = rows[cursor];
      exitIndex = cursor;
      exitPrice = row.close;
      if (row.low <= stop && row.high >= target) {
        exitPrice = stop;
        exitReason = 'stop_same_bar';
        break;
      }
      if (row.low <= stop) {
        exitPrice = stop;
        exitReason = 'stop';
        break;
      }
      if (row.high >= target) {
        exitPrice = target;
        exitReason = 'target';
        break;
      }
      const earlyExit = candidate.earlyExit?.({ row, signal, entryLimit, holdBars: cursor - index, settings });
      if (earlyExit) {
        exitPrice = earlyExit.price ?? row.close;
        exitReason = earlyExit.reason || 'early_exit';
        break;
      }
    }
    trades.push({
      grossCents: (exitPrice - entryLimit) * 100,
      entryPrice: entryLimit,
      exitReason,
    });
    index = exitIndex + settings.cooldownBars + 1;
  }
  return trades;
}

function strategySettings({ buy, target, stop, hold, minRange }) {
  return {
    buyBelowCloseCents: buy,
    targetCents: target,
    stopCents: stop,
    maxHoldBars: hold,
    cooldownBars: 2,
    noEntryFirstMinutes: 5,
    noEntryLastMinutes: 10,
    minTradeCount: 1,
    minRangeCents: minRange,
    minRet60sCents: -50,
    maxLastBarUpCents: Math.max(12, target * 3),
    requireMarketOk: true,
    minSpyRet1m: -0.001,
    minQqqRet1m: -0.0012,
    minTargetRet1m: -0.003,
  };
}

function clamp(value, min, max) {
  return Math.min(max, Math.max(min, value));
}

function buildCandidates() {
  const fixed = [
    ['fixed_4_4_6_15', strategySettings({ buy: 4, target: 4, stop: 6, hold: 15, minRange: 3 })],
    ['profit_lock_4_4_6_15', strategySettings({ buy: 4, target: 4, stop: 6, hold: 15, minRange: 3 })],
    ['qqq_not_weak_4_4_6_15', strategySettings({ buy: 4, target: 4, stop: 6, hold: 15, minRange: 3 })],
    ['fixed_6_6_9_20', strategySettings({ buy: 6, target: 6, stop: 9, hold: 20, minRange: 5 })],
    ['profit_lock_6_6_9_20', strategySettings({ buy: 6, target: 6, stop: 9, hold: 20, minRange: 5 })],
    ['fixed_8_8_12_25', strategySettings({ buy: 8, target: 8, stop: 12, hold: 25, minRange: 7 })],
    ['profit_lock_8_8_12_25', strategySettings({ buy: 8, target: 8, stop: 12, hold: 25, minRange: 7 })],
    ['fixed_10_10_15_30', strategySettings({ buy: 10, target: 10, stop: 15, hold: 30, minRange: 8 })],
    ['profit_lock_10_10_15_30', strategySettings({ buy: 10, target: 10, stop: 15, hold: 30, minRange: 8 })],
  ];
  const candidates = fixed.map(([id, settings]) => ({
    id,
    settings,
    filter: id.startsWith('qqq_not_weak') ? ((row) => row.qqq_ret_5m >= -0.0005) : (() => true),
    earlyExit: id.startsWith('profit_lock')
      ? ({ row, entryLimit, holdBars, settings: live }) => (
        holdBars >= Math.max(6, Math.round(live.maxHoldBars * 0.4))
        && (row.close - entryLimit) * 100 >= live.targetCents * 0.5
          ? { price: row.close, reason: 'profit_lock' }
          : null
      )
      : null,
  }));
  candidates.push({
    id: 'price_scaled_2bps_profit_lock',
    filter: () => true,
    resolveSettings: (row) => {
      const cents = clamp(Math.round(row.close * 100 * 0.0002), 4, 25);
      return strategySettings({
        buy: cents,
        target: cents,
        stop: clamp(Math.round(cents * 1.5), cents + 2, 40),
        hold: clamp(12 + cents, 15, 35),
        minRange: Math.max(3, Math.round(cents * 0.8)),
      });
    },
    earlyExit: ({ row, entryLimit, holdBars, settings }) => (
      holdBars >= Math.max(6, Math.round(settings.maxHoldBars * 0.4))
      && (row.close - entryLimit) * 100 >= settings.targetCents * 0.5
        ? { price: row.close, reason: 'profit_lock' }
        : null
    ),
  });
  candidates.push({
    id: 'price_scaled_4bps_profit_lock',
    filter: () => true,
    resolveSettings: (row) => {
      const cents = clamp(Math.round(row.close * 100 * 0.0004), 4, 50);
      return strategySettings({
        buy: cents,
        target: cents,
        stop: clamp(Math.round(cents * 1.5), cents + 2, 75),
        hold: clamp(12 + cents, 15, 45),
        minRange: Math.max(3, Math.round(cents * 0.8)),
      });
    },
    earlyExit: ({ row, entryLimit, holdBars, settings }) => (
      holdBars >= Math.max(6, Math.round(settings.maxHoldBars * 0.4))
      && (row.close - entryLimit) * 100 >= settings.targetCents * 0.5
        ? { price: row.close, reason: 'profit_lock' }
        : null
    ),
  });
  return candidates;
}

function createAgg(symbol, strategyId) {
  return {
    symbol,
    strategyId,
    days: 0,
    tradedDays: 0,
    positiveDays: 0,
    trades: 0,
    wins: 0,
    grossCents: 0,
    costCents: 0,
    buyCapital: 0,
    equity: 0,
    peak: 0,
    maxDrawdownCents: 0,
    exitReasons: {},
    dayStats: [],
    months: new Map(),
  };
}

function updateAgg(agg, dayIso, trades, costPerSideCents) {
  const dayGross = trades.reduce((sum, trade) => sum + trade.grossCents, 0);
  const dayCost = dayGross - (trades.length * costPerSideCents * 2);
  const dayBuyCapital = trades.reduce((sum, trade) => sum + trade.entryPrice, 0);
  agg.days += 1;
  if (trades.length) agg.tradedDays += 1;
  if (dayCost > 0) agg.positiveDays += 1;
  agg.trades += trades.length;
  agg.wins += trades.filter((trade) => trade.grossCents - (costPerSideCents * 2) > 0).length;
  agg.grossCents += dayGross;
  agg.costCents += dayCost;
  agg.buyCapital += dayBuyCapital;
  trades.forEach((trade) => {
    const pnl = trade.grossCents - (costPerSideCents * 2);
    agg.equity += pnl;
    agg.peak = Math.max(agg.peak, agg.equity);
    agg.maxDrawdownCents = Math.min(agg.maxDrawdownCents, agg.equity - agg.peak);
    agg.exitReasons[trade.exitReason] = (agg.exitReasons[trade.exitReason] || 0) + 1;
  });
  const day = {
    date: dayIso,
    month: monthKey(dayIso),
    trades: trades.length,
    costCents: round(dayCost, 4),
    buyCapital: round(dayBuyCapital, 6),
  };
  agg.dayStats.push(day);
  const month = day.month;
  const current = agg.months.get(month) || { month, days: 0, tradedDays: 0, positiveDays: 0, trades: 0, costCents: 0, buyCapital: 0 };
  current.days += 1;
  if (trades.length) current.tradedDays += 1;
  if (dayCost > 0) current.positiveDays += 1;
  current.trades += trades.length;
  current.costCents += dayCost;
  current.buyCapital += dayBuyCapital;
  agg.months.set(month, current);
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
  for (const value of values) {
    equity += value;
    peak = Math.max(peak, equity);
    drawdown = Math.min(drawdown, equity - peak);
  }
  return drawdown;
}

function summarizeStats(days, totalBuyCapital, totalTrades) {
  const cents = days.map((day) => day.costCents || 0);
  const netCents = cents.reduce((sum, value) => sum + value, 0);
  const avgEntry = totalTrades ? totalBuyCapital / totalTrades : 0;
  const ddCents = maxDrawdown(cents);
  return {
    days: days.length,
    tradedDays: days.filter((day) => day.trades > 0).length,
    positiveDays: days.filter((day) => day.costCents > 0).length,
    trades: totalTrades,
    netCents: round(netCents, 2),
    pnlPer1000Shares: round(netCents * 10, 2),
    avgCentsPerTrade: totalTrades ? round(netCents / totalTrades, 4) : 0,
    winRate: null,
    returnRecycled: avgEntry ? round((netCents / 100) / avgEntry, 8) : 0,
    returnPct: avgEntry ? round(((netCents / 100) / avgEntry) * 100, 3) : 0,
    maxDrawdownCents: round(ddCents, 2),
    maxDrawdownPct: avgEntry ? round(((ddCents / 100) / avgEntry) * 100, 3) : 0,
    sharpe: round(sharpe(cents), 3),
  };
}

function summarizeAgg(agg) {
  const overall = summarizeStats(agg.dayStats, agg.buyCapital, agg.trades);
  overall.winRate = agg.trades ? round(agg.wins / agg.trades, 6) : 0;
  overall.maxDrawdownCentsTradeSequence = round(agg.maxDrawdownCents, 2);
  const months = [...agg.months.values()].sort((left, right) => left.month.localeCompare(right.month)).map((month) => {
    const days = agg.dayStats.filter((day) => day.month === month.month);
    return {
      month: month.month,
      ...summarizeStats(days, month.buyCapital, month.trades),
    };
  });
  return {
    symbol: agg.symbol,
    strategyId: agg.strategyId,
    overall,
    exitReasons: agg.exitReasons,
    months,
  };
}

function renderMarkdown(payload) {
  const top = [...payload.results]
    .filter((item) => item.overall.trades >= 25)
    .sort((left, right) => {
      if (right.overall.sharpe !== left.overall.sharpe) return right.overall.sharpe - left.overall.sharpe;
      return right.overall.netCents - left.overall.netCents;
    })
    .slice(0, 30);
  const bySymbol = new Map();
  payload.bestBySymbol.forEach((item) => bySymbol.set(item.symbol, item));
  const lines = [
    '# Cross-Symbol Passive Scalp Backtest',
    '',
    `Window: ${payload.startDate} to ${payload.endDate}`,
    `Cost: ${payload.costCentsPerSide} cents/side hidden cost`,
    `Exit start offset: ${payload.exitStartOffsetBars ?? 1} bar(s) after signal`,
    '',
    '## Best By Symbol',
    '',
    '| Symbol | Best strategy | Trades | Net c/share | P/L per 1k shares | Return | Max DD c/share | Sharpe |',
    '| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |',
    ...payload.bestBySymbol.map((item) => `| ${item.symbol} | ${item.strategyId} | ${item.overall.trades} | ${item.overall.netCents.toFixed(2)} | $${item.overall.pnlPer1000Shares.toLocaleString()} | ${item.overall.returnPct.toFixed(3)}% | ${item.overall.maxDrawdownCents.toFixed(2)} | ${item.overall.sharpe?.toFixed(3) ?? ''} |`),
    '',
    '## Top Strategy Runs',
    '',
    '| Rank | Symbol | Strategy | Trades | Net c/share | Return | Max DD c/share | Sharpe |',
    '| ---: | --- | --- | ---: | ---: | ---: | ---: | ---: |',
    ...top.map((item, index) => `| ${index + 1} | ${item.symbol} | ${item.strategyId} | ${item.overall.trades} | ${item.overall.netCents.toFixed(2)} | ${item.overall.returnPct.toFixed(3)}% | ${item.overall.maxDrawdownCents.toFixed(2)} | ${item.overall.sharpe?.toFixed(3) ?? ''} |`),
    '',
    '## Caveat',
    '',
    'This uses 1-second traded-price aggregates, not quote/NBBO queue simulation. Wider-spread names need quote-level validation before any live use.',
    '',
  ];
  return `${lines.join('\n')}\n`;
}

async function main() {
  const args = parseArgs();
  const config = loadConfig();
  const startDate = args.start || '2025-01-02';
  const endDate = args.end || '2026-05-08';
  const symbols = String(args.symbols || 'TSLL,SOXL,TQQQ,SQQQ,NVDL,NVDA,MSTU,MSTR,COIN,AMDL,TSLA')
    .split(',')
    .map((symbol) => symbol.trim().toUpperCase())
    .filter(Boolean);
  const costCentsPerSide = Number(args.costCentsPerSide ?? 0.5);
  const concurrency = Number(args.concurrency ?? 4);
  const batchSize = Number(args.batchSize ?? Math.max(20, concurrency * 10));
  const exitStartOffsetBars = Number(args.exitStartOffsetBars ?? 1);
  const apiKey = loadRestApiKey();
  const dates = availableDates(config, startDate, endDate, ['stockBars']);
  const strategyIds = args.strategyIds
    ? new Set(String(args.strategyIds).split(',').map((item) => item.trim()).filter(Boolean))
    : null;
  const candidates = buildCandidates().filter((candidate) => !strategyIds || strategyIds.has(candidate.id));
  console.log(`[cross-scalp] symbols=${symbols.join(',')} dates=${dates.length} candidates=${candidates.length} concurrency=${concurrency} exitStartOffsetBars=${exitStartOffsetBars}`);
  const allResults = [];
  const errors = [];
  const startedAt = Date.now();

  for (let symbolIndex = 0; symbolIndex < symbols.length; symbolIndex += 1) {
    const symbol = symbols[symbolIndex];
    const aggs = new Map(candidates.map((candidate) => [candidate.id, createAgg(symbol, candidate.id)]));
    for (let batchStart = 0; batchStart < dates.length; batchStart += batchSize) {
      const batch = dates.slice(batchStart, batchStart + batchSize).map((dayIso, offset) => ({
        dayIso,
        dateIndex: batchStart + offset,
      }));
      const dayResults = await mapWithConcurrency(batch, concurrency, async ({ dayIso, dateIndex }) => {
        try {
          const { rows, restRows } = await buildRowsForDay({ config, symbol, dayIso, apiKey });
          const tradesByStrategy = candidates.map((candidate) => ({
            strategyId: candidate.id,
            trades: simulateDay(rows, candidate, { exitStartOffsetBars }),
          }));
          return { ok: true, dayIso, dateIndex, rowsLength: rows.length, restRows, tradesByStrategy };
        } catch (error) {
          return { ok: false, dayIso, dateIndex, error: error.message };
        }
      });

      dayResults.sort((left, right) => left.dateIndex - right.dateIndex).forEach((result) => {
        if (!result.ok) {
          errors.push({ symbol, dayIso: result.dayIso, error: result.error });
          console.warn(`[cross-scalp] error symbol=${symbol} date=${result.dayIso} ${result.error}`);
          return;
        }
        result.tradesByStrategy.forEach(({ strategyId, trades }) => {
          updateAgg(aggs.get(strategyId), result.dayIso, trades, costCentsPerSide);
        });
        if ((result.dateIndex + 1) % 20 === 0 || result.dateIndex === dates.length - 1) {
          const elapsedMin = ((Date.now() - startedAt) / 60000).toFixed(1);
          console.log(`[cross-scalp] ${symbolIndex + 1}/${symbols.length} ${symbol} ${result.dateIndex + 1}/${dates.length} ${result.dayIso} rows=${result.rowsLength} restRows=${result.restRows} elapsed=${elapsedMin}m`);
        }
      });
    }
    allResults.push(...[...aggs.values()].map(summarizeAgg));
  }

  const bestBySymbol = symbols.map((symbol) => (
    allResults
      .filter((item) => item.symbol === symbol && item.overall.trades >= 25)
      .sort((left, right) => {
        if (right.overall.sharpe !== left.overall.sharpe) return right.overall.sharpe - left.overall.sharpe;
        return right.overall.netCents - left.overall.netCents;
      })[0]
  )).filter(Boolean);

  const payload = {
    generatedAt: new Date().toISOString(),
    startDate,
    endDate,
    symbols,
    dates: dates.length,
    costCentsPerSide,
    concurrency,
    exitStartOffsetBars,
    assumptions: {
      data: 'Massive REST unadjusted 1-second stock aggregates plus local Massive stock_quotes_1m SPY/QQQ context.',
      ranking: 'Best-by-symbol ranks by annualized daily P/L Sharpe, then net cents.',
      caveat: '1-second traded-price proxy only; passive queue and NBBO fill quality are not proven.',
    },
    bestBySymbol,
    results: allResults,
    errors,
  };
  const tag = args.tag ? `-${String(args.tag).replace(/[^A-Za-z0-9._-]/g, '-')}` : '';
  const slug = `${startDate}-${endDate}${tag}`;
  const outJson = artifactPath(`cross-symbol-scalp-backtest-${slug}.json`);
  const outMd = artifactPath(`cross-symbol-scalp-backtest-${slug}.md`);
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
      maxDrawdownCents: item.overall.maxDrawdownCents,
      sharpe: item.overall.sharpe,
    })),
  }, null, 2));
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
