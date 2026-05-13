#!/usr/bin/env node
const fs = require('node:fs');
const path = require('node:path');
const zlib = require('node:zlib');
const readline = require('node:readline');

const { availableDates } = require('../src/calendar');
const { artifactPath, datasetCsvPath, ensureDir, loadConfig } = require('../src/config');
const { getEtParts, isRegularSessionMs, nsToMinuteMs } = require('../src/time');

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

function round(value, digits = 4) {
  return Number.isFinite(value) ? Number(value.toFixed(digits)) : value;
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

function monthKey(dayIso) {
  return dayIso.slice(0, 7);
}

function toNumber(value) {
  const out = Number(value);
  return Number.isFinite(out) ? out : 0;
}

function parseCsvLine(line, headers) {
  const values = String(line).split(',');
  const row = {};
  headers.forEach((header, index) => {
    row[header] = values[index] ?? '';
  });
  return row;
}

async function streamStockRows(config, dayIso, onRow) {
  const filePath = datasetCsvPath(config, 'stockBars', dayIso);
  if (!fs.existsSync(filePath)) return 0;
  const input = fs.createReadStream(filePath).pipe(zlib.createGunzip());
  const reader = readline.createInterface({ input, crlfDelay: Infinity });
  let headers = null;
  let count = 0;
  for await (const line of reader) {
    if (!line) continue;
    if (!headers) {
      headers = String(line).split(',');
      continue;
    }
    count += 1;
    await onRow(parseCsvLine(line, headers));
  }
  return count;
}

function isPlainTicker(symbol) {
  return /^[A-Z]{1,5}$/.test(symbol);
}

function addUniverseMetric(dayMap, row, config) {
  const symbol = String(row.ticker || '').toUpperCase();
  if (!isPlainTicker(symbol)) return;
  const minuteMs = nsToMinuteMs(row.window_start);
  if (!Number.isFinite(minuteMs) || !isRegularSessionMs(minuteMs, config.session)) return;
  const open = toNumber(row.open);
  const close = toNumber(row.close);
  const high = toNumber(row.high);
  const low = toNumber(row.low);
  const volume = toNumber(row.volume);
  const transactions = toNumber(row.transactions);
  if (!(open > 0) || !(close > 0) || !(high > 0) || !(low > 0) || !(volume > 0)) return;
  const current = dayMap.get(symbol) || {
    symbol,
    open,
    close,
    high,
    low,
    volume: 0,
    transactions: 0,
    minutes: 0,
  };
  current.close = close;
  current.high = Math.max(current.high, high);
  current.low = Math.min(current.low, low);
  current.volume += volume;
  current.transactions += transactions;
  current.minutes += 1;
  dayMap.set(symbol, current);
}

async function screenUniverse(config, dates, {
  sampleEvery = 8,
  minPrice = 5,
  minDollarVolume = 15000000,
  minRangePct = 0.025,
  topN = 120,
  seeds = [],
} = {}) {
  const sampleDates = dates.filter((_, index) => index % sampleEvery === 0);
  const summaries = new Map();
  for (let index = 0; index < sampleDates.length; index += 1) {
    const dayIso = sampleDates[index];
    const dayMap = new Map();
    await streamStockRows(config, dayIso, (row) => addUniverseMetric(dayMap, row, config));
    dayMap.forEach((day) => {
      if (day.minutes < 120) return;
      const mid = (day.high + day.low + day.close) / 3;
      const dollarVolume = day.volume * mid;
      const rangePct = day.low > 0 ? (day.high / day.low) - 1 : 0;
      const current = summaries.get(day.symbol) || {
        symbol: day.symbol,
        days: 0,
        dollarVolume: 0,
        rangePct: 0,
        transactions: 0,
        avgPrice: 0,
      };
      current.days += 1;
      current.dollarVolume += dollarVolume;
      current.rangePct += rangePct;
      current.transactions += day.transactions;
      current.avgPrice += day.close;
      summaries.set(day.symbol, current);
    });
    if ((index + 1) % 10 === 0 || index === sampleDates.length - 1) {
      console.log(`[adaptive-search] screened ${index + 1}/${sampleDates.length} sample days ${dayIso} tickers=${summaries.size}`);
    }
  }
  const seedSet = new Set(seeds.map((symbol) => String(symbol).toUpperCase()).filter(Boolean));
  const ranked = [...summaries.values()].map((item) => {
    const avgDollarVolume = item.dollarVolume / item.days;
    const avgRangePct = item.rangePct / item.days;
    const avgTransactions = item.transactions / item.days;
    const avgPrice = item.avgPrice / item.days;
    return {
      symbol: item.symbol,
      sampleDays: item.days,
      avgDollarVolume,
      avgRangePct,
      avgTransactions,
      avgPrice,
      score: avgRangePct * Math.log10(Math.max(10, avgDollarVolume)) * Math.log10(Math.max(10, avgTransactions)),
    };
  }).filter((item) => (
    item.sampleDays >= Math.max(8, Math.floor(sampleDates.length * 0.45))
    && item.avgPrice >= minPrice
    && item.avgDollarVolume >= minDollarVolume
    && item.avgRangePct >= minRangePct
  )).sort((left, right) => right.score - left.score);

  const selected = [];
  const seen = new Set();
  [...seeds.map((symbol) => String(symbol).toUpperCase()), ...ranked.map((item) => item.symbol)].forEach((symbol) => {
    if (!symbol || seen.has(symbol)) return;
    const metric = ranked.find((item) => item.symbol === symbol) || {
      symbol,
      sampleDays: 0,
      avgDollarVolume: null,
      avgRangePct: null,
      avgTransactions: null,
      avgPrice: null,
      score: seedSet.has(symbol) ? Number.MAX_SAFE_INTEGER : 0,
    };
    selected.push(metric);
    seen.add(symbol);
  });
  return { sampleDates, ranked, selected: selected.slice(0, topN) };
}

function safeReturn(current, previous) {
  if (!Number.isFinite(current) || !Number.isFinite(previous) || previous <= 0) return 0;
  return (current / previous) - 1;
}

function addMinuteFeatures(rows) {
  rows.sort((left, right) => left.minuteMs - right.minuteMs);
  let cumulativeVolume = 0;
  let cumulativeDollarVolume = 0;
  rows.forEach((row, index) => {
    const typical = (row.high + row.low + row.close) / 3;
    cumulativeVolume += row.volume || 0;
    cumulativeDollarVolume += typical * (row.volume || 0);
    row.index = index;
    row.ret1 = safeReturn(row.close, rows[index - 1]?.close);
    row.ret3 = safeReturn(row.close, rows[index - 3]?.close);
    row.ret5 = safeReturn(row.close, rows[index - 5]?.close);
    row.ret15 = safeReturn(row.close, rows[index - 15]?.close);
    row.ret30 = safeReturn(row.close, rows[index - 30]?.close);
    const prior = rows.slice(Math.max(0, index - 5), index);
    const prior15 = rows.slice(Math.max(0, index - 15), index);
    row.priorRangeCents5 = prior.length
      ? (Math.max(...prior.map((item) => item.high)) - Math.min(...prior.map((item) => item.low))) * 100
      : 0;
    row.priorRangeCents15 = prior15.length
      ? (Math.max(...prior15.map((item) => item.high)) - Math.min(...prior15.map((item) => item.low))) * 100
      : 0;
    row.priorHigh15 = prior15.length ? Math.max(...prior15.map((item) => item.high)) : row.high;
    row.priorLow15 = prior15.length ? Math.min(...prior15.map((item) => item.low)) : row.low;
    row.vwap = cumulativeVolume > 0 ? cumulativeDollarVolume / cumulativeVolume : row.close;
    row.minuteRangeCents = (row.high - row.low) * 100;
    row.dollarVolume = row.close * row.volume;
  });
  return rows;
}

function createDayState(config, symbols) {
  return {
    wanted: new Set([...symbols, 'SPY', 'QQQ'].map((symbol) => String(symbol).toUpperCase())),
    rowsBySymbol: new Map(),
    config,
  };
}

function addBacktestRow(state, row) {
  const symbol = String(row.ticker || '').toUpperCase();
  if (!state.wanted.has(symbol)) return;
  const minuteMs = nsToMinuteMs(row.window_start);
  if (!Number.isFinite(minuteMs) || !isRegularSessionMs(minuteMs, state.config.session)) return;
  const open = toNumber(row.open);
  const close = toNumber(row.close);
  const high = toNumber(row.high);
  const low = toNumber(row.low);
  if (!(open > 0) || !(close > 0) || !(high > 0) || !(low > 0)) return;
  const et = getEtParts(minuteMs);
  const list = state.rowsBySymbol.get(symbol) || [];
  list.push({
    symbol,
    minuteMs,
    minuteOfDayEt: et.minuteOfDayEt,
    minutesFromOpen: et.minuteOfDayEt - state.config.session.regularOpenMinuteEt,
    minutesToClose: state.config.session.regularCloseMinuteEt - et.minuteOfDayEt - 1,
    open,
    high,
    low,
    close,
    volume: toNumber(row.volume),
    transactions: toNumber(row.transactions),
  });
  state.rowsBySymbol.set(symbol, list);
}

function marketContextAt(row, spyRows, qqqRows) {
  const spy = spyRows?.[row.index] || null;
  const qqq = qqqRows?.[row.index] || null;
  return {
    spyRet1: spy?.ret1 || 0,
    spyRet5: spy?.ret5 || 0,
    qqqRet1: qqq?.ret1 || 0,
    qqqRet5: qqq?.ret5 || 0,
  };
}

function centsFromBps(price, bps, minCents, maxCents) {
  return Math.min(maxCents, Math.max(minCents, price * 100 * bps / 10000));
}

function buildStrategies() {
  const strategies = [];
  [
    { name: 'fast', bps: 4, targetMult: 1.2, stopMult: 1.7, hold: 6, minRange: 6 },
    { name: 'base', bps: 6, targetMult: 1.4, stopMult: 1.9, hold: 8, minRange: 8 },
    { name: 'wide', bps: 10, targetMult: 1.5, stopMult: 2.0, hold: 12, minRange: 12 },
    { name: 'mstr_wide', bps: 16, targetMult: 1.4, stopMult: 1.8, hold: 16, minRange: 20 },
    { name: 'trend_hold', bps: 20, targetMult: 1.5, stopMult: 1.9, hold: 24, minRange: 25 },
  ].forEach((shape) => {
    ['trend_pullback', 'market_rebound', 'deep_dip', 'strong_trend_dip', 'vwap_dip', 'opening_trend_dip'].forEach((filterName) => {
      strategies.push({
        id: `${filterName}_${shape.name}_${shape.bps}bps`,
        shape,
        filterName,
      });
    });
  });
  return strategies;
}

function resolvePlan(row, strategy) {
  const base = centsFromBps(row.close, strategy.shape.bps, 4, 250);
  const range = Math.max(base, row.priorRangeCents5 * 0.35, strategy.shape.minRange);
  return {
    buyCents: range,
    targetCents: Math.max(5, range * strategy.shape.targetMult),
    stopCents: Math.max(7, range * strategy.shape.stopMult),
    holdMinutes: strategy.shape.hold,
    cooldownMinutes: 2,
  };
}

function passesFilter(row, strategy, market) {
  if (row.minutesFromOpen < 10 || row.minutesToClose < 15) return false;
  if (row.volume <= 0 || row.transactions <= 0) return false;
  if (row.dollarVolume < 10000) return false;
  if (row.priorRangeCents5 < strategy.shape.minRange) return false;
  if (market.spyRet1 < -0.002 || market.qqqRet1 < -0.0025) return false;
  if (strategy.filterName === 'trend_pullback') {
    return row.ret15 > -0.003 && row.ret1 <= 0.0008 && row.ret5 > -0.015;
  }
  if (strategy.filterName === 'market_rebound') {
    return market.qqqRet5 > -0.002 && row.ret3 < 0.001 && row.ret5 > -0.02;
  }
  if (strategy.filterName === 'deep_dip') {
    return row.ret1 < -0.001 && row.ret5 > -0.025 && market.spyRet5 > -0.004;
  }
  if (strategy.filterName === 'strong_trend_dip') {
    return row.ret15 > 0.004 && row.ret30 > -0.002 && row.ret1 <= 0.0015 && market.qqqRet5 > -0.0015;
  }
  if (strategy.filterName === 'vwap_dip') {
    return row.close > row.vwap && row.low <= row.vwap * 1.01 && row.ret15 > 0 && row.ret5 > -0.01 && market.spyRet5 > -0.0025;
  }
  if (strategy.filterName === 'opening_trend_dip') {
    return row.minutesFromOpen >= 20 && row.minutesFromOpen <= 150 && row.ret15 > 0.006 && row.ret1 <= 0.002 && market.qqqRet5 > -0.001;
  }
  return true;
}

function simulateDay(rows, spyRows, qqqRows, strategy) {
  const trades = [];
  let index = 15;
  while (index < rows.length - 3) {
    const signal = rows[index];
    const entryBar = rows[index + 1];
    const market = marketContextAt(signal, spyRows, qqqRows);
    if (!passesFilter(signal, strategy, market)) {
      index += 1;
      continue;
    }
    const plan = resolvePlan(signal, strategy);
    const entryLimit = signal.close - (plan.buyCents / 100);
    if (entryBar.low > entryLimit) {
      index += 1;
      continue;
    }
    const target = entryLimit + (plan.targetCents / 100);
    const stop = entryLimit - (plan.stopCents / 100);
    const lastIndex = Math.min(rows.length - 1, index + 1 + plan.holdMinutes);
    const firstExitIndex = Math.min(lastIndex, index + 2);
    let exitIndex = firstExitIndex;
    let exitPrice = rows[firstExitIndex].close;
    let exitReason = 'timeout';
    for (let cursor = firstExitIndex; cursor <= lastIndex; cursor += 1) {
      const row = rows[cursor];
      exitIndex = cursor;
      exitPrice = row.close;
      if (row.low <= stop && row.high >= target) {
        exitPrice = stop;
        exitReason = 'stop_same_minute';
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
      if (cursor - index >= Math.max(3, Math.round(plan.holdMinutes * 0.5)) && (row.close - entryLimit) * 100 >= plan.targetCents * 0.45) {
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
    index = exitIndex + plan.cooldownMinutes + 1;
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
    year: dayIso.slice(0, 4),
    trades: trades.length,
    netCents: round(netCents, 4),
    buyCapital: round(buyCapital, 6),
  });
}

function summarizeDays(dayStats, buyCapital, trades) {
  const values = dayStats.map((day) => day.netCents || 0);
  const netCents = values.reduce((sum, value) => sum + value, 0);
  const avgEntry = trades ? buyCapital / trades : 0;
  const dd = maxDrawdown(values);
  return {
    days: dayStats.length,
    tradedDays: dayStats.filter((day) => day.trades > 0).length,
    positiveDays: dayStats.filter((day) => day.netCents > 0).length,
    trades,
    netCents: round(netCents, 2),
    pnlPer1000Shares: round(netCents * 10, 2),
    avgCentsPerTrade: trades ? round(netCents / trades, 4) : 0,
    returnPct: avgEntry ? round(((netCents / 100) / avgEntry) * 100, 3) : 0,
    maxDrawdownCents: round(dd, 2),
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
  return [...results].filter((item) => item.overall.trades >= 50).sort((left, right) => {
    const leftPass = left.overall.netCents > 0 && left.test2026.netCents > 0 ? 1 : 0;
    const rightPass = right.overall.netCents > 0 && right.test2026.netCents > 0 ? 1 : 0;
    if (rightPass !== leftPass) return rightPass - leftPass;
    const leftScore = (left.test2026.sharpe || -99) + (left.overall.sharpe || -99) * 0.35;
    const rightScore = (right.test2026.sharpe || -99) + (right.overall.sharpe || -99) * 0.35;
    if (rightScore !== leftScore) return rightScore - leftScore;
    return right.overall.netCents - left.overall.netCents;
  });
}

function renderMarkdown(payload) {
  const lines = [
    '# Adaptive High-Vol Passive Scalp Search',
    '',
    `Window: ${payload.startDate} to ${payload.endDate}`,
    `Universe candidates tested: ${payload.symbols.length}`,
    `Cost: ${payload.costCentsPerSide} cents/side`,
    '',
    '## Best Candidates',
    '',
    '| Symbol | Strategy | Trades | Net c/share | Test 2026 net | Test Sharpe | Max DD c/share |',
    '| --- | --- | ---: | ---: | ---: | ---: | ---: |',
    ...payload.bestBySymbol.slice(0, 30).map((item) => `| ${item.symbol} | ${item.strategyId} | ${item.overall.trades} | ${item.overall.netCents.toFixed(2)} | ${item.test2026.netCents.toFixed(2)} | ${item.test2026.sharpe?.toFixed(3) ?? ''} | ${item.overall.maxDrawdownCents.toFixed(2)} |`),
    '',
    '## Top Strategy Runs',
    '',
    '| Rank | Symbol | Strategy | Trades | Net c/share | Overall Sharpe | Test 2026 net | Test Sharpe |',
    '| ---: | --- | --- | ---: | ---: | ---: | ---: | ---: |',
    ...payload.topRuns.slice(0, 50).map((item, index) => `| ${index + 1} | ${item.symbol} | ${item.strategyId} | ${item.overall.trades} | ${item.overall.netCents.toFixed(2)} | ${item.overall.sharpe?.toFixed(3) ?? ''} | ${item.test2026.netCents.toFixed(2)} | ${item.test2026.sharpe?.toFixed(3) ?? ''} |`),
    '',
    '## Method Note',
    '',
    'This is a conservative 1-minute OHLC search. Entry is a passive dip limit in the next minute, and exits cannot occur until the following minute. It is useful for finding candidates, but true passive scalping still needs tick quote validation.',
    '',
  ];
  return `${lines.join('\n')}\n`;
}

async function main() {
  const args = parseArgs();
  const config = loadConfig();
  const startDate = args.start || '2025-01-02';
  const endDate = args.end || '2026-05-08';
  const dates = availableDates(config, startDate, endDate, ['stockBars']);
  const costCentsPerSide = Number(args.costCentsPerSide ?? 0.5);
  const seeds = String(args.seeds || 'TSLL,TSLA,NVDA,SOXL,TQQQ,SQQQ,MSTR,MSTU,MSTX,NVDL,NVDS,COIN,AMDL,SOXS,LABU,LABD,UVXY,BITX,CONL,FNGU,TECL,TECS,BOIL')
    .split(',')
    .map((symbol) => symbol.trim().toUpperCase())
    .filter(Boolean);
  const topN = Number(args.topN ?? 120);
  const sampleEvery = Number(args.sampleEvery ?? 8);
  const strategies = buildStrategies();

  console.log(`[adaptive-search] dates=${dates.length} start=${startDate} end=${endDate}`);
  const universe = await screenUniverse(config, dates, {
    sampleEvery,
    topN,
    seeds,
    minPrice: Number(args.minPrice ?? 5),
    minDollarVolume: Number(args.minDollarVolume ?? 15000000),
    minRangePct: Number(args.minRangePct ?? 0.025),
  });
  const symbols = universe.selected.map((item) => item.symbol);
  console.log(`[adaptive-search] selected=${symbols.length} strategies=${strategies.length} top=${symbols.slice(0, 20).join(',')}`);

  const aggs = new Map();
  symbols.forEach((symbol) => {
    strategies.forEach((strategy) => {
      aggs.set(`${symbol}|${strategy.id}`, createAgg(symbol, strategy.id));
    });
  });

  const startedAt = Date.now();
  for (let dateIndex = 0; dateIndex < dates.length; dateIndex += 1) {
    const dayIso = dates[dateIndex];
    const state = createDayState(config, symbols);
    await streamStockRows(config, dayIso, (row) => addBacktestRow(state, row));
    state.rowsBySymbol.forEach((rows, symbol) => state.rowsBySymbol.set(symbol, addMinuteFeatures(rows)));
    const spyRows = state.rowsBySymbol.get('SPY') || [];
    const qqqRows = state.rowsBySymbol.get('QQQ') || [];
    symbols.forEach((symbol) => {
      const rows = state.rowsBySymbol.get(symbol);
      if (!rows || rows.length < 120) {
        strategies.forEach((strategy) => updateAgg(aggs.get(`${symbol}|${strategy.id}`), dayIso, [], costCentsPerSide));
        return;
      }
      strategies.forEach((strategy) => {
        const trades = simulateDay(rows, spyRows, qqqRows, strategy);
        updateAgg(aggs.get(`${symbol}|${strategy.id}`), dayIso, trades, costCentsPerSide);
      });
    });
    if ((dateIndex + 1) % 20 === 0 || dateIndex === dates.length - 1) {
      const elapsedMin = ((Date.now() - startedAt) / 60000).toFixed(1);
      console.log(`[adaptive-search] backtested ${dateIndex + 1}/${dates.length} ${dayIso} elapsed=${elapsedMin}m`);
    }
  }

  const results = [...aggs.values()].map(summarizeAgg);
  const ranked = rankResults(results);
  const bestBySymbol = symbols.map((symbol) => ranked.find((item) => item.symbol === symbol)).filter(Boolean);
  const payload = {
    generatedAt: new Date().toISOString(),
    startDate,
    endDate,
    dates: dates.length,
    costCentsPerSide,
    sampleEvery,
    symbols,
    universeTop: universe.ranked.slice(0, 100),
    selectedUniverse: universe.selected,
    strategies: strategies.map((strategy) => strategy.id),
    assumptions: {
      data: 'Massive stock_quotes_1m local aggregate bars.',
      execution: 'Passive next-minute dip entry; exits begin one minute after entry; same-minute entry/exit is disallowed.',
      split: '2025 is reported as train/reference, 2026 through end date as out-of-sample style check.',
    },
    bestBySymbol,
    topRuns: ranked.slice(0, 100),
    results,
  };
  const slug = `${startDate}-${endDate}`;
  const outJson = artifactPath(`adaptive-high-vol-scalp-search-${slug}.json`);
  const outMd = artifactPath(`adaptive-high-vol-scalp-search-${slug}.md`);
  ensureDir(path.dirname(outJson));
  fs.writeFileSync(outJson, `${JSON.stringify(payload, null, 2)}\n`);
  fs.writeFileSync(outMd, renderMarkdown(payload));
  console.log(JSON.stringify({
    outJson,
    outMd,
    bestBySymbol: bestBySymbol.slice(0, 12).map((item) => ({
      symbol: item.symbol,
      strategyId: item.strategyId,
      trades: item.overall.trades,
      netCents: item.overall.netCents,
      test2026NetCents: item.test2026.netCents,
      test2026Sharpe: item.test2026.sharpe,
    })),
  }, null, 2));
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
