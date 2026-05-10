const fs = require('node:fs');

const { datasetCsvPath } = require('./config');
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

async function readTargetTradesForDay(config, dayIso, symbol = config.target) {
  const filePath = datasetCsvPath(config, 'stockTrades', dayIso);
  const trades = [];
  if (!fs.existsSync(filePath)) return trades;
  const wanted = String(symbol || '').toUpperCase();
  await readGzipCsv(filePath, (row) => {
    if (String(row.ticker || '').toUpperCase() !== wanted) return;
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

async function readStockMinutesForDay(config, dayIso, symbols = config.marketSymbols) {
  const filePath = datasetCsvPath(config, 'stockBars', dayIso);
  const wanted = new Set(symbols.map((symbol) => String(symbol).toUpperCase()));
  const bySymbol = new Map();
  if (!fs.existsSync(filePath)) return bySymbol;
  await readGzipCsv(filePath, (row) => {
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
  });
  bySymbol.forEach((rows, symbol) => bySymbol.set(symbol, addStockMinuteFeatures(rows)));
  return bySymbol;
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

function buildFiveSecondBars({ config, dayIso, trades, stockMinutes, optionByMinute, optionGroups, barSeconds }) {
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

    (config.marketSymbols || []).forEach((symbol) => {
      const current = completedMinuteAtOrBefore(stockMinutes.get(symbol), states.get(symbol), bucketMs);
      const key = symbol.toLowerCase();
      row[`${key}_minute_close`] = current?.close ?? null;
      row[`${key}_ret_1m`] = current?.ret1 || 0;
      row[`${key}_ret_5m`] = current?.ret5 || 0;
      row[`${key}_ret_15m`] = current?.ret15 || 0;
      row[`${key}_minute_volume_log`] = Math.log1p(current?.volume || 0);
    });

    const completedMinuteMs = Math.floor((bucketMs - 1) / 60000) * 60000;
    if (!optionMinutesSeen.has(completedMinuteMs)) {
      optionWindow.push(optionByMinute.get(completedMinuteMs) || createEmptyOptionAgg(optionGroups));
      optionMinutesSeen.add(completedMinuteMs);
      if (optionWindow.length > 5) optionWindow.shift();
    }
    addOptionFeatureRow(row, optionWindow, optionGroups);
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
  return bars;
}

async function buildScalpingBarsForDay(config, dayIso, settings = {}) {
  const barSeconds = settings.barSeconds || config.execution?.barSeconds || 5;
  const [trades, stockMinutes, optionAggs] = await Promise.all([
    readTargetTradesForDay(config, dayIso, config.target),
    readStockMinutesForDay(config, dayIso, config.marketSymbols),
    readOptionAggsForDay(config, dayIso, settings),
  ]);
  const rows = buildFiveSecondBars({
    config,
    dayIso,
    trades,
    stockMinutes,
    optionByMinute: optionAggs.optionByMinute,
    optionGroups: optionAggs.optionGroups,
    barSeconds,
  });
  return {
    dayIso,
    rows,
    counts: {
      trades: trades.length,
      bars: rows.length,
      stockMinuteSymbols: stockMinutes.size,
      optionMinutes: optionAggs.optionByMinute.size,
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
  readOptionAggsForDay,
  buildFiveSecondBars,
  buildScalpingBarsForDay,
};
