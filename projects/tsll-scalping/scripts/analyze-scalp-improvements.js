#!/usr/bin/env node
const fs = require('node:fs');
const path = require('node:path');
const readline = require('node:readline');
const zlib = require('node:zlib');

const { availableDates } = require('../src/calendar');
const {
  artifactPath,
  datasetCsvPath,
  ensureDir,
  loadConfig,
  runtimePath,
} = require('../src/config');
const {
  buildDailyContextByDate,
  readStockMinutesForDay,
  safeRatio,
  safeReturn,
} = require('../src/data');
const { parseOpraTicker, daysBetween } = require('../src/opra');
const { etMinuteToUtcMs, getEtParts, isRegularSessionMs, nsToMs } = require('../src/time');

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

function monthKey(dayIso) {
  return dayIso.slice(0, 7);
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

function addDailyContextFeatures(row, config, dailyContext, sessionOpen, dayHighSoFar, dayLowSoFar) {
  (config.marketSymbols || []).forEach((symbol) => {
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
  [5, 15, 30].forEach((minutes) => {
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

function addRollingFeatures(bars) {
  bars.forEach((row, index) => {
    const prev1 = bars[index - 1];
    const prev3 = bars[index - 3];
    const prev60 = bars[index - 60];
    const prev180 = bars[index - 180];
    const last12Prev = bars.slice(Math.max(0, index - 12), index);
    const last36Prev = bars.slice(Math.max(0, index - 36), index);
    row.ret_1bar_cents = Number.isFinite(prev1?.close) ? (row.close - prev1.close) * 100 : 0;
    row.ret_3bar_cents = Number.isFinite(prev3?.close) ? (row.close - prev3.close) * 100 : 0;
    row.ret_60s_cents = Number.isFinite(prev60?.close) ? (row.close - prev60.close) * 100 : 0;
    row.ret_180s_cents = Number.isFinite(prev180?.close) ? (row.close - prev180.close) * 100 : 0;
    row.prev_high_60s = last12Prev.length ? Math.max(...last12Prev.map((item) => item.high)) : row.high;
    row.prev_low_60s = last12Prev.length ? Math.min(...last12Prev.map((item) => item.low)) : row.low;
    row.prev_high_180s = last36Prev.length ? Math.max(...last36Prev.map((item) => item.high)) : row.high;
    row.prev_low_180s = last36Prev.length ? Math.min(...last36Prev.map((item) => item.low)) : row.low;
    row.range_60s_cents = row.prev_high_60s && row.prev_low_60s
      ? (row.prev_high_60s - row.prev_low_60s) * 100
      : 0;
    row.market_ok_1m = row.spy_ret_1m > -0.0005 && row.qqq_ret_1m > -0.0007 ? 1 : 0;
    row.tsla_confirm_1m = row.tsla_ret_1m > 0 ? 1 : 0;
  });
  addOpeningRangeFeatures(bars);
}

function loadRestAggs(dayIso) {
  const filePath = runtimePath('rest-second-aggs', `TSLL-${dayIso}-1s-unadjusted.json`);
  if (!fs.existsSync(filePath)) return null;
  const payload = JSON.parse(fs.readFileSync(filePath, 'utf8'));
  return Array.isArray(payload.results) ? payload.results : [];
}

function emptyOptionFeatures() {
  return {
    count: 0,
    size: 0,
    callSize: 0,
    putSize: 0,
    premium: 0,
    callPremium: 0,
    putPremium: 0,
    nearDteSize: 0,
  };
}

function combineOptionFeatures(items) {
  return items.reduce((acc, item) => {
    acc.count += item.count || 0;
    acc.size += item.size || 0;
    acc.callSize += item.callSize || 0;
    acc.putSize += item.putSize || 0;
    acc.premium += item.premium || 0;
    acc.callPremium += item.callPremium || 0;
    acc.putPremium += item.putPremium || 0;
    acc.nearDteSize += item.nearDteSize || 0;
    return acc;
  }, emptyOptionFeatures());
}

async function readTslaOptionTradesForDay(config, dayIso) {
  const filePath = datasetCsvPath(config, 'optionTrades', dayIso);
  const byMinute = new Map();
  if (!fs.existsSync(filePath)) return byMinute;
  let started = false;
  const fileStream = fs.createReadStream(filePath);
  const gunzip = zlib.createGunzip();
  const reader = readline.createInterface({
    input: fileStream.pipe(gunzip),
    crlfDelay: Infinity,
  });
  let isHeader = true;
  for await (const line of reader) {
    if (!line) continue;
    if (isHeader) {
      isHeader = false;
      continue;
    }
    if (!started && line < 'O:TSL') continue;
    started = true;
    if (line >= 'O:TSM') {
      reader.close();
      fileStream.destroy();
      gunzip.destroy();
      break;
    }
    if (!line.startsWith('O:TSLA')) continue;
    const values = line.split(',');
    const ticker = values[0];
    const parsed = parseOpraTicker(ticker);
    if (!parsed || parsed.root !== 'TSLA') continue;
    if (String(values[2] || '0') !== '0') continue;
    const tsMs = nsToMs(values[5]);
    if (!Number.isFinite(tsMs) || !isRegularSessionMs(tsMs, config.session)) continue;
    const price = Number(values[4]);
    const size = Number(values[6]);
    if (!(size > 0) || !(price > 0)) continue;
    const minuteMs = Math.floor(tsMs / 60000) * 60000;
    const agg = byMinute.get(minuteMs) || emptyOptionFeatures();
    const premium = size * price * 100;
    const dte = daysBetween(dayIso, parsed.expiration);
    agg.count += 1;
    agg.size += size;
    agg.premium += premium;
    if (dte !== null && dte >= 0 && dte <= 7) agg.nearDteSize += size;
    if (parsed.right === 'CALL') {
      agg.callSize += size;
      agg.callPremium += premium;
    } else {
      agg.putSize += size;
      agg.putPremium += premium;
    }
    byMinute.set(minuteMs, agg);
  }
  return byMinute;
}

function addOptionFeatures(row, tslaOptionByMinute) {
  if (!tslaOptionByMinute) return;
  const completedMinuteMs = Math.floor((row.tsMs - 1) / 60000) * 60000;
  const current = tslaOptionByMinute.get(completedMinuteMs) || emptyOptionFeatures();
  const window = [];
  for (let offset = 0; offset < 5; offset += 1) {
    window.push(tslaOptionByMinute.get(completedMinuteMs - (offset * 60000)) || emptyOptionFeatures());
  }
  const five = combineOptionFeatures(window);
  row.opt_tsla_trade_count_1m = current.count;
  row.opt_tsla_premium_1m = current.premium;
  row.opt_tsla_trade_count_5m = five.count;
  row.opt_tsla_premium_5m = five.premium;
  row.opt_tsla_call_put_size_imb_5m = safeRatio(five.callSize - five.putSize, five.callSize + five.putSize);
  row.opt_tsla_call_put_premium_imb_5m = safeRatio(five.callPremium - five.putPremium, five.callPremium + five.putPremium);
  row.opt_tsla_near_dte_share_5m = safeRatio(five.nearDteSize, five.size);
}

async function buildRowsForDay(config, dayIso, dailyContext, includeOptions) {
  const restRows = loadRestAggs(dayIso);
  if (!restRows) return { rows: [], restRows: 0 };
  const stockMinutes = await readStockMinutesForDay(config, dayIso, config.marketSymbols);
  const optionByMinute = includeOptions ? await readTslaOptionTradesForDay(config, dayIso) : null;
  const bySecond = new Map(restRows.map((row) => [row.t, row]));
  const states = new Map((config.marketSymbols || []).map((symbol) => [symbol, { index: -1 }]));
  const bars = [];
  let previousClose = null;
  let sessionOpen = null;
  let dayHighSoFar = -Infinity;
  let dayLowSoFar = Infinity;

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
    if (!Number.isFinite(sessionOpen)) sessionOpen = open;
    dayHighSoFar = Math.max(dayHighSoFar, high);
    dayLowSoFar = Math.min(dayLowSoFar, low);
    previousClose = close;

    const et = getEtParts(ms);
    const row = {
      tradeDate: dayIso,
      tsUtc: new Date(ms).toISOString(),
      tsMs: ms,
      minuteOfDayEt: et.minuteOfDayEt,
      secondOfDayEt: et.secondOfDayEt,
      open,
      high,
      low,
      close,
      volume,
      trade_count: tradeCount,
      minutes_from_open: et.minuteOfDayEt - config.session.regularOpenMinuteEt,
      minutes_to_close: config.session.regularCloseMinuteEt - et.minuteOfDayEt - 1,
    };
    addDailyContextFeatures(row, config, dailyContext, sessionOpen, dayHighSoFar, dayLowSoFar);
    (config.marketSymbols || []).forEach((symbol) => {
      const current = completedMinuteAtOrBefore(stockMinutes.get(symbol), states.get(symbol), ms);
      const key = symbol.toLowerCase();
      row[`${key}_minute_close`] = current?.close ?? null;
      row[`${key}_ret_1m`] = current?.ret1 || 0;
      row[`${key}_ret_5m`] = current?.ret5 || 0;
      row[`${key}_ret_15m`] = current?.ret15 || 0;
      row[`${key}_minute_volume_log`] = Math.log1p(current?.volume || 0);
    });
    addOptionFeatures(row, optionByMinute);
    bars.push(row);
  });
  addRollingFeatures(bars);
  return { rows: bars, restRows: restRows.length, optionMinutes: optionByMinute?.size || 0 };
}

function basePasses(row, settings) {
  if (!row) return false;
  if (row.minutes_from_open < settings.noEntryFirstMinutes) return false;
  if (row.minutes_to_close < settings.noEntryLastMinutes) return false;
  if ((row.trade_count || 0) < settings.minTradeCount) return false;
  if ((row.range_60s_cents || 0) < settings.minRange60sCents) return false;
  if ((row.ret_60s_cents || 0) < settings.minRet60sCents) return false;
  if ((row.ret_1bar_cents || 0) > settings.maxLastBarUpCents) return false;
  if (settings.requireMarketOk && row.market_ok_1m !== 1) return false;
  if ((row.spy_ret_1m || 0) < settings.minSpyRet1m) return false;
  if ((row.qqq_ret_1m || 0) < settings.minQqqRet1m) return false;
  if ((row.tsla_ret_1m || 0) < settings.minTslaRet1m) return false;
  return true;
}

function simulateDay(rows, candidate) {
  const trades = [];
  const settings = candidate.settings;
  let index = 0;
  while (index < rows.length - 2) {
    const signal = rows[index];
    const entryBar = rows[index + 1];
    if (!basePasses(signal, settings) || !candidate.filter(signal)) {
      index += 1;
      continue;
    }
    const tradeSettings = candidate.resolveSettings
      ? { ...settings, ...candidate.resolveSettings(signal) }
      : settings;
    const entryLimit = signal.close - (tradeSettings.buyBelowCloseCents / 100);
    if (entryBar.low > entryLimit - ((tradeSettings.throughCents || 0) / 100)) {
      index += 1;
      continue;
    }
    const target = entryLimit + (tradeSettings.targetCents / 100);
    const stop = entryLimit - (tradeSettings.stopCents / 100);
    const lastIndex = Math.min(rows.length - 1, index + 1 + tradeSettings.maxHoldBars);
    let exitIndex = index + 1;
    let exitPrice = entryBar.close;
    let exitReason = 'timeout';
    for (let cursor = index + 1; cursor <= lastIndex; cursor += 1) {
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
      const earlyExit = candidate.earlyExit?.({
        row,
        signal,
        entryLimit,
        target,
        stop,
        holdBars: cursor - index,
        tradeSettings,
      });
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
    index = exitIndex + tradeSettings.cooldownBars + 1;
  }
  return trades;
}

function createAgg(id, label, settings, filterId) {
  return {
    id,
    label,
    filterId,
    settings,
    days: 0,
    tradedDays: 0,
    positiveDays0: 0,
    positiveDaysCost0p5: 0,
    trades: 0,
    wins0: 0,
    winsCost0p5: 0,
    grossCents: 0,
    cost0p5Cents: 0,
    buyCapital: 0,
    peak0: 0,
    equity0: 0,
    maxDrawdown0: 0,
    peakCost0p5: 0,
    equityCost0p5: 0,
    maxDrawdownCost0p5: 0,
    exitReasons: {},
    dayStats: [],
    train: createSplitAgg(),
    test: createSplitAgg(),
    months: new Map(),
  };
}

function createSplitAgg() {
  return {
    days: 0,
    tradedDays: 0,
    positiveDays0: 0,
    positiveDaysCost0p5: 0,
    trades: 0,
    wins0: 0,
    winsCost0p5: 0,
    grossCents: 0,
    cost0p5Cents: 0,
    buyCapital: 0,
  };
}

function updateEquity(agg, grossCents, cost0p5Cents) {
  agg.equity0 += grossCents;
  agg.peak0 = Math.max(agg.peak0, agg.equity0);
  agg.maxDrawdown0 = Math.min(agg.maxDrawdown0, agg.equity0 - agg.peak0);
  agg.equityCost0p5 += cost0p5Cents;
  agg.peakCost0p5 = Math.max(agg.peakCost0p5, agg.equityCost0p5);
  agg.maxDrawdownCost0p5 = Math.min(agg.maxDrawdownCost0p5, agg.equityCost0p5 - agg.peakCost0p5);
}

function updateSplit(split, trades, dayGross, dayCost0p5) {
  split.days += 1;
  if (trades.length) split.tradedDays += 1;
  if (dayGross > 0) split.positiveDays0 += 1;
  if (dayCost0p5 > 0) split.positiveDaysCost0p5 += 1;
  split.trades += trades.length;
  split.wins0 += trades.filter((trade) => trade.grossCents > 0).length;
  split.winsCost0p5 += trades.filter((trade) => trade.grossCents - 1 > 0).length;
  split.grossCents += dayGross;
  split.cost0p5Cents += dayCost0p5;
  split.buyCapital += trades.reduce((sum, trade) => sum + trade.entryPrice, 0);
}

function updateAggForDay(agg, dayIso, trades) {
  const dayGross = trades.reduce((sum, trade) => sum + trade.grossCents, 0);
  const dayCost0p5 = dayGross - trades.length;
  const buyCapital = trades.reduce((sum, trade) => sum + trade.entryPrice, 0);
  const avgEntry = trades.length ? buyCapital / trades.length : 0;
  agg.days += 1;
  if (trades.length) agg.tradedDays += 1;
  if (dayGross > 0) agg.positiveDays0 += 1;
  if (dayCost0p5 > 0) agg.positiveDaysCost0p5 += 1;
  agg.trades += trades.length;
  agg.wins0 += trades.filter((trade) => trade.grossCents > 0).length;
  agg.winsCost0p5 += trades.filter((trade) => trade.grossCents - 1 > 0).length;
  agg.grossCents += dayGross;
  agg.cost0p5Cents += dayCost0p5;
  agg.buyCapital += buyCapital;
  agg.dayStats.push({
    date: dayIso,
    month: monthKey(dayIso),
    trades: trades.length,
    grossCents: round(dayGross, 4),
    cost0p5Cents: round(dayCost0p5, 4),
    avgEntry: round(avgEntry, 6),
    buyCapital: round(buyCapital, 6),
    returnRecycled0: avgEntry ? round((dayGross / 100) / avgEntry, 8) : 0,
    returnRecycledCost0p5: avgEntry ? round((dayCost0p5 / 100) / avgEntry, 8) : 0,
    returnBuyTurnover0: buyCapital ? round((dayGross / 100) / buyCapital, 8) : 0,
    returnBuyTurnoverCost0p5: buyCapital ? round((dayCost0p5 / 100) / buyCapital, 8) : 0,
  });
  trades.forEach((trade) => {
    agg.exitReasons[trade.exitReason] = (agg.exitReasons[trade.exitReason] || 0) + 1;
    updateEquity(agg, trade.grossCents, trade.grossCents - 1);
  });
  const split = dayIso < '2026-01-01' ? agg.train : agg.test;
  updateSplit(split, trades, dayGross, dayCost0p5);
  const month = monthKey(dayIso);
  if (!agg.months.has(month)) agg.months.set(month, createSplitAgg());
  updateSplit(agg.months.get(month), trades, dayGross, dayCost0p5);
}

function summarizeSplit(split) {
  const pnlDollars = split.grossCents / 100;
  const pnlCostDollars = split.cost0p5Cents / 100;
  const avgEntry = split.trades ? split.buyCapital / split.trades : 0;
  return {
    days: split.days,
    tradedDays: split.tradedDays,
    positiveDays0: split.positiveDays0,
    positiveDaysCost0p5: split.positiveDaysCost0p5,
    trades: split.trades,
    netCents0: round(split.grossCents, 2),
    netCentsCost0p5: round(split.cost0p5Cents, 2),
    avgCents0: split.trades ? round(split.grossCents / split.trades, 4) : 0,
    avgCentsCost0p5: split.trades ? round(split.cost0p5Cents / split.trades, 4) : 0,
    winRate0: split.trades ? round(split.wins0 / split.trades, 6) : 0,
    winRateCost0p5: split.trades ? round(split.winsCost0p5 / split.trades, 6) : 0,
    pnlPer1000Shares0: round(pnlDollars * 1000, 2),
    pnlPer1000SharesCost0p5: round(pnlCostDollars * 1000, 2),
    returnOnBuyTurnover0: split.buyCapital ? round(pnlDollars / split.buyCapital, 8) : 0,
    returnOnBuyTurnoverCost0p5: split.buyCapital ? round(pnlCostDollars / split.buyCapital, 8) : 0,
    returnOnRecycledCapital0: avgEntry ? round(pnlDollars / avgEntry, 8) : 0,
    returnOnRecycledCapitalCost0p5: avgEntry ? round(pnlCostDollars / avgEntry, 8) : 0,
  };
}

function summarizeAgg(agg) {
  return {
    id: agg.id,
    label: agg.label,
    filterId: agg.filterId,
    settings: agg.settings,
    overall: {
      ...summarizeSplit(agg),
      maxDrawdownCents0: round(agg.maxDrawdown0, 2),
      maxDrawdownCentsCost0p5: round(agg.maxDrawdownCost0p5, 2),
      exitReasons: agg.exitReasons,
    },
    train2025: summarizeSplit(agg.train),
    test2026Ytd: summarizeSplit(agg.test),
    months: [...agg.months.entries()].sort().map(([month, split]) => ({
      month,
      ...summarizeSplit(split),
    })),
    dayStats: agg.dayStats,
  };
}

const BASE_SETTINGS = {
  buyBelowCloseCents: 3,
  targetCents: 3,
  stopCents: 5,
  maxHoldBars: 10,
  cooldownBars: 2,
  throughCents: 0,
  noEntryFirstMinutes: 5,
  noEntryLastMinutes: 10,
  minTradeCount: 1,
  minRange60sCents: 3,
  minRet60sCents: -20,
  maxLastBarUpCents: 12,
  requireMarketOk: true,
  minSpyRet1m: -0.001,
  minQqqRet1m: -0.0012,
  minTslaRet1m: -0.002,
};

function clamp(value, min, max) {
  return Math.min(max, Math.max(min, value));
}

function rangeBucketSettings(row) {
  if ((row.range_60s_cents || 0) >= 8) {
    return {
      buyBelowCloseCents: 5,
      targetCents: 5,
      stopCents: 8,
      maxHoldBars: 20,
    };
  }
  if ((row.range_60s_cents || 0) >= 5) {
    return {
      buyBelowCloseCents: 4,
      targetCents: 4,
      stopCents: 6,
      maxHoldBars: 15,
    };
  }
  return {
    buyBelowCloseCents: 3,
    targetCents: 3,
    stopCents: 5,
    maxHoldBars: 10,
  };
}

function continuousRangeSettings(row) {
  const range = row.range_60s_cents || 0;
  const cents = clamp(Math.round(range * 0.55), 3, 6);
  return {
    buyBelowCloseCents: cents,
    targetCents: cents,
    stopCents: clamp(Math.round(cents * 1.5), 5, 9),
    maxHoldBars: clamp(Math.round(8 + range), 10, 24),
  };
}

function momentumWideSettings(row) {
  if ((row.range_60s_cents || 0) >= 5 && row.qqq_ret_5m >= 0 && row.tsla_ret_5m >= 0) {
    return {
      buyBelowCloseCents: 5,
      targetCents: 5,
      stopCents: 8,
      maxHoldBars: 20,
    };
  }
  return {
    buyBelowCloseCents: 4,
    targetCents: 4,
    stopCents: 6,
    maxHoldBars: 15,
  };
}

function buildCandidates(includeOptions) {
  const settingVariants = [
    ['b3_t3_s5_h10', {}],
    ['b3_t4_s5_h15', { targetCents: 4, maxHoldBars: 15 }],
    ['b3_t5_s6_h20', { targetCents: 5, stopCents: 6, maxHoldBars: 20 }],
    ['b4_t4_s6_h15', { buyBelowCloseCents: 4, targetCents: 4, stopCents: 6, maxHoldBars: 15 }],
    ['b4_t5_s6_h20', { buyBelowCloseCents: 4, targetCents: 5, stopCents: 6, maxHoldBars: 20 }],
    ['b5_t5_s8_h20', { buyBelowCloseCents: 5, targetCents: 5, stopCents: 8, maxHoldBars: 20 }],
  ];
  const filters = [
    ['base', () => true],
    ['tsla_green_1m', (row) => row.tsla_ret_1m >= 0],
    ['qqq_green_1m', (row) => row.qqq_ret_1m >= 0],
    ['spy_qqq_green_1m', (row) => row.spy_ret_1m >= 0 && row.qqq_ret_1m >= 0],
    ['tsla_qqq_green_1m', (row) => row.tsla_ret_1m >= 0 && row.qqq_ret_1m >= 0],
    ['tsla_not_weak_5m', (row) => row.tsla_ret_5m >= -0.001],
    ['qqq_not_weak_5m', (row) => row.qqq_ret_5m >= -0.0005],
    ['tsla_qqq_not_weak_5m', (row) => row.tsla_ret_5m >= -0.001 && row.qqq_ret_5m >= -0.0005],
    ['all_market_positive_5m', (row) => row.spy_ret_5m >= 0 && row.qqq_ret_5m >= 0 && row.tsla_ret_5m >= 0],
    ['range_ge_5c', (row) => row.range_60s_cents >= 5],
    ['range_ge_7c', (row) => row.range_60s_cents >= 7],
    ['not_overextended_60s', (row) => row.ret_60s_cents <= 8 && row.ret_60s_cents >= -12],
    ['range5_not_overextended', (row) => row.range_60s_cents >= 5 && row.ret_60s_cents <= 8 && row.ret_60s_cents >= -12],
    ['avoid_lunch', (row) => row.minuteOfDayEt < 690 || row.minuteOfDayEt >= 810],
    ['morning_5_to_120m', (row) => row.minutes_from_open >= 5 && row.minutes_from_open < 120],
    ['after_orb15_above_low', (row) => row.orb15_complete === 1 && row.close >= row.orb15_low],
    ['after_orb30_inside', (row) => row.orb30_complete === 1 && row.close >= row.orb30_low && row.close <= row.orb30_high],
    ['daily_macro_trend_up', (row) => row.daily_context_ready === 1 && row.daily_macro_trend_up === 1],
    ['daily_tsla_qqq_trend_up', (row) => row.daily_tsla_trend_up === 1 && row.daily_qqq_trend_up === 1],
    ['daily_no_big_tsll_gap', (row) => row.daily_context_ready === 1 && Math.abs(row.daily_tsll_open_gap_atr || 0) <= 0.7],
    ['daily_not_extended', (row) => row.daily_context_ready === 1 && Math.abs(row.daily_tsll_from_prev_close_atr || 0) <= 1.2],
    ['daily_gap_and_qqq_not_weak', (row) => row.daily_context_ready === 1 && Math.abs(row.daily_tsll_open_gap_atr || 0) <= 0.7 && row.qqq_ret_5m >= -0.0005],
    ['daily_gap_and_tsla_qqq_not_weak', (row) => row.daily_context_ready === 1 && Math.abs(row.daily_tsll_open_gap_atr || 0) <= 0.7 && row.tsla_ret_5m >= -0.001 && row.qqq_ret_5m >= -0.0005],
    ['daily_not_extended_and_qqq_not_weak', (row) => row.daily_context_ready === 1 && Math.abs(row.daily_tsll_from_prev_close_atr || 0) <= 1.2 && row.qqq_ret_5m >= -0.0005],
    ['range5_and_qqq_not_weak', (row) => row.range_60s_cents >= 5 && row.qqq_ret_5m >= -0.0005],
    ['range5_and_daily_gap', (row) => row.daily_context_ready === 1 && row.range_60s_cents >= 5 && Math.abs(row.daily_tsll_open_gap_atr || 0) <= 0.7],
    ['avoid_lunch_and_qqq_not_weak', (row) => (row.minuteOfDayEt < 690 || row.minuteOfDayEt >= 810) && row.qqq_ret_5m >= -0.0005],
    ['morning_and_qqq_not_weak', (row) => row.minutes_from_open >= 5 && row.minutes_from_open < 120 && row.qqq_ret_5m >= -0.0005],
  ];
  if (includeOptions) {
    filters.push(
      ['opt_tsla_active_5m', (row) => (row.opt_tsla_trade_count_5m || 0) >= 100],
      ['opt_tsla_call_premium_not_put_heavy', (row) => (row.opt_tsla_call_put_premium_imb_5m || 0) >= -0.1],
      ['opt_tsla_call_premium_positive', (row) => (row.opt_tsla_call_put_premium_imb_5m || 0) >= 0],
      ['opt_tsla_near_dte_active', (row) => (row.opt_tsla_trade_count_5m || 0) >= 100 && (row.opt_tsla_near_dte_share_5m || 0) >= 0.45],
      ['opt_tsla_call_positive_and_market_ok', (row) => (row.opt_tsla_call_put_premium_imb_5m || 0) >= 0 && row.qqq_ret_5m >= -0.0005 && row.tsla_ret_5m >= -0.001],
    );
  }
  const out = [];
  settingVariants.forEach(([settingsId, overrides]) => {
    filters.forEach(([filterId, filter]) => {
      const id = `${settingsId}__${filterId}`;
      out.push({
        id,
        label: `${settingsId} + ${filterId}`,
        filterId,
        settings: { ...BASE_SETTINGS, ...overrides },
        filter,
      });
    });
  });
  if (!includeOptions) {
    [
      {
        id: 'dyn_range_bucket__base',
        label: 'dynamic range bucket + base',
        filterId: 'dynamic_range_bucket_base',
        filter: () => true,
        resolveSettings: rangeBucketSettings,
      },
      {
        id: 'dyn_range_bucket__qqq_not_weak_5m',
        label: 'dynamic range bucket + QQQ not weak 5m',
        filterId: 'dynamic_range_bucket_qqq_not_weak_5m',
        filter: (row) => row.qqq_ret_5m >= -0.0005,
        resolveSettings: rangeBucketSettings,
      },
      {
        id: 'dyn_range_bucket__daily_gap',
        label: 'dynamic range bucket + no big daily gap',
        filterId: 'dynamic_range_bucket_daily_gap',
        filter: (row) => row.daily_context_ready === 1 && Math.abs(row.daily_tsll_open_gap_atr || 0) <= 0.7,
        resolveSettings: rangeBucketSettings,
      },
      {
        id: 'dyn_continuous_range__base',
        label: 'dynamic continuous range + base',
        filterId: 'dynamic_continuous_range_base',
        filter: () => true,
        resolveSettings: continuousRangeSettings,
      },
      {
        id: 'dyn_momentum_wide__base',
        label: 'dynamic market-confirmed wider scalp',
        filterId: 'dynamic_momentum_wide_base',
        filter: () => true,
        resolveSettings: momentumWideSettings,
      },
      {
        id: 'dyn_b4_market_early_exit__base',
        label: 'b4/t4/s6/h15 + market early exit',
        filterId: 'dynamic_b4_market_early_exit_base',
        filter: () => true,
        resolveSettings: () => ({
          buyBelowCloseCents: 4,
          targetCents: 4,
          stopCents: 6,
          maxHoldBars: 15,
        }),
        earlyExit: ({ row }) => (
          row.qqq_ret_1m < -0.0012 || row.tsla_ret_1m < -0.0025
            ? { price: row.close, reason: 'market_weak_exit' }
            : null
        ),
      },
      {
        id: 'dyn_b4_profit_lock__base',
        label: 'b4/t4/s6/h15 + profit lock',
        filterId: 'dynamic_b4_profit_lock_base',
        filter: () => true,
        resolveSettings: () => ({
          buyBelowCloseCents: 4,
          targetCents: 4,
          stopCents: 6,
          maxHoldBars: 15,
        }),
        earlyExit: ({ row, entryLimit, holdBars }) => (
          holdBars >= 6 && (row.close - entryLimit) * 100 >= 2
            ? { price: row.close, reason: 'profit_lock' }
            : null
        ),
      },
      {
        id: 'dyn_b4_stale_loss_exit__base',
        label: 'b4/t4/s6/h15 + stale-loss exit',
        filterId: 'dynamic_b4_stale_loss_exit_base',
        filter: () => true,
        resolveSettings: () => ({
          buyBelowCloseCents: 4,
          targetCents: 4,
          stopCents: 6,
          maxHoldBars: 15,
        }),
        earlyExit: ({ row, entryLimit, holdBars }) => (
          holdBars >= 8 && row.close < entryLimit && row.qqq_ret_1m < 0
            ? { price: row.close, reason: 'stale_loss_exit' }
            : null
        ),
      },
    ].forEach((candidate) => {
      out.push({
        ...candidate,
        settings: BASE_SETTINGS,
      });
    });
  }
  return out;
}

function renderMarkdown(payload) {
  const top = payload.topByCost0p5.slice(0, 15);
  const rows = top.map((item, index) => [
    index + 1,
    item.id,
    item.overall.trades,
    item.overall.netCentsCost0p5,
    `$${item.overall.pnlPer1000SharesCost0p5.toLocaleString()}`,
    `${(item.overall.winRateCost0p5 * 100).toFixed(1)}%`,
    `${(item.overall.returnOnBuyTurnoverCost0p5 * 100).toFixed(4)}%`,
    item.test2026Ytd.netCentsCost0p5,
    item.test2026Ytd.trades,
  ]);
  const lines = [
    '# TSLL Scalp Improvement Analysis',
    '',
    `Window: ${payload.startDate} to ${payload.endDate}`,
    `Trading days: ${payload.days}`,
    `Option features included: ${payload.includeOptions ? 'yes, TSLA option trades only' : 'no'}`,
    '',
    'Ranking below uses the 0.5 cent/side hidden-cost sensitivity, because the gross edge is too small to trust without it.',
    '',
    '| Rank | Candidate | Trades | Net c/share after 0.5c/side | P/L per 1k shares | Win rate | Buy-turnover return | 2026 YTD net c/share | 2026 trades |',
    '| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |',
    ...rows.map((row) => `| ${row.join(' | ')} |`),
    '',
    '## Baseline',
    '',
    `Baseline candidate: ${payload.baseline.id}`,
    `- Gross/no-cost net: ${payload.baseline.overall.netCents0} c/share`,
    `- 0.5c/side net: ${payload.baseline.overall.netCentsCost0p5} c/share`,
    `- 0.5c/side P/L per 1k shares: $${payload.baseline.overall.pnlPer1000SharesCost0p5.toLocaleString()}`,
    '',
    '## Caveats',
    '',
    '- Seconds bars show traded prices, not passive queue priority or NBBO availability.',
    '- TSLA option-flow filters here use call-vs-put trade/premium mix, not true buyer/seller aggressor side.',
    '- Treat this as a research filter screen; quote-level validation is still required before live use.',
    '',
  ];
  return `${lines.join('\n')}\n`;
}

async function main() {
  const args = parseArgs();
  const config = loadConfig();
  const startDate = args.start || '2025-01-02';
  const endDate = args.end || '2026-05-08';
  const includeOptions = Boolean(args.options);
  const dates = availableDates(config, startDate, endDate, ['stockBars'])
    .filter((dayIso) => fs.existsSync(runtimePath('rest-second-aggs', `TSLL-${dayIso}-1s-unadjusted.json`)));
  const candidates = buildCandidates(includeOptions);
  const aggs = new Map(candidates.map((candidate) => [
    candidate.id,
    createAgg(candidate.id, candidate.label, candidate.settings, candidate.filterId),
  ]));
  const { dailyContextByDate } = await buildDailyContextByDate(config, dates, {
    symbols: config.marketSymbols,
  });
  console.log(`[tsll-improve] dates=${dates.length} candidates=${candidates.length} options=${includeOptions}`);
  const startedAt = Date.now();
  for (let dateIndex = 0; dateIndex < dates.length; dateIndex += 1) {
    const dayIso = dates[dateIndex];
    const { rows, restRows, optionMinutes } = await buildRowsForDay(
      config,
      dayIso,
      dailyContextByDate.get(dayIso),
      includeOptions,
    );
    candidates.forEach((candidate) => {
      const trades = simulateDay(rows, candidate);
      updateAggForDay(aggs.get(candidate.id), dayIso, trades);
    });
    const elapsed = (Date.now() - startedAt) / 1000;
    console.log(`[tsll-improve] ${dateIndex + 1}/${dates.length} ${dayIso} rows=${rows.length} restRows=${restRows} optionMinutes=${optionMinutes} elapsed=${elapsed.toFixed(1)}s`);
  }
  const results = [...aggs.values()].map(summarizeAgg);
  const baseline = results.find((item) => item.id === 'b3_t3_s5_h10__base');
  const topByCost0p5 = [...results]
    .filter((item) => item.overall.trades >= 25)
    .sort((left, right) => {
      const testDiff = right.test2026Ytd.netCentsCost0p5 - left.test2026Ytd.netCentsCost0p5;
      if (Math.abs(testDiff) > 1e-9) return testDiff;
      return right.overall.netCentsCost0p5 - left.overall.netCentsCost0p5;
    });
  const payload = {
    generatedAt: new Date().toISOString(),
    startDate,
    endDate,
    days: dates.length,
    includeOptions,
    assumptions: {
      data: 'Massive REST TSLL unadjusted 1-second aggregates plus local Massive stock_quotes_1m SPY/QQQ/TSLA/TSLL context.',
      optionData: includeOptions ? 'Massive option_trades_all TSLA OPRA trades, aggregated to completed 1-minute and rolling 5-minute windows.' : null,
      ranking: 'Primary ranking uses 0.5 cent/side hidden-cost sensitivity and 2026 YTD test net cents, with full-period net as tie-breaker.',
      train: '2025-01-02 through 2025-12-31',
      test: '2026-01-01 through 2026-05-08',
    },
    baseline,
    topByCost0p5: topByCost0p5.slice(0, 25),
    results,
  };
  const suffix = includeOptions ? 'with-tsla-options' : 'market-filters';
  const outJson = artifactPath(`tsll-scalp-improvement-analysis-${suffix}-${startDate}-${endDate}.json`);
  const outMd = artifactPath(`tsll-scalp-improvement-analysis-${suffix}-${startDate}-${endDate}.md`);
  ensureDir(path.dirname(outJson));
  fs.writeFileSync(outJson, `${JSON.stringify(payload, null, 2)}\n`);
  fs.writeFileSync(outMd, renderMarkdown(payload));
  console.log(JSON.stringify({
    outJson,
    outMd,
    baseline: {
      trades: baseline.overall.trades,
      netCents0: baseline.overall.netCents0,
      netCentsCost0p5: baseline.overall.netCentsCost0p5,
      testNetCentsCost0p5: baseline.test2026Ytd.netCentsCost0p5,
    },
    top: topByCost0p5.slice(0, 5).map((item) => ({
      id: item.id,
      trades: item.overall.trades,
      netCentsCost0p5: item.overall.netCentsCost0p5,
      testNetCentsCost0p5: item.test2026Ytd.netCentsCost0p5,
      avgCentsCost0p5: item.overall.avgCentsCost0p5,
    })),
  }, null, 2));
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
