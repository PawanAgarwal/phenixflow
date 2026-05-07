const fs = require('node:fs');
const readline = require('node:readline');

const { datasetCsvPath } = require('./config');
const { readGzipCsv, toNumber } = require('./csv');
const { parseOpraTicker, daysBetween } = require('./opra');
const { nsToMinuteMs, minuteMsToIso, getEtParts, isRegularSessionMinute } = require('./time');
const { assignForwardLabels } = require('./labels');

const OPTION_GROUPS = Object.freeze(['spy', 'spx', 'vix', 'mag7']);
const OPTION_METRICS = Object.freeze([
  'quote_volume',
  'quote_call_volume',
  'quote_put_volume',
  'quote_premium',
  'quote_call_premium',
  'quote_put_premium',
  'quote_transactions',
  'quote_near_dte_volume',
  'quote_0dte_volume',
  'quote_1dte_volume',
  'quote_2_7dte_volume',
  'quote_8_30dte_volume',
  'quote_atm_volume',
  'quote_otm_call_volume',
  'quote_otm_put_volume',
  'trade_count',
  'trade_size',
  'trade_call_size',
  'trade_put_size',
  'trade_premium',
  'trade_call_premium',
  'trade_put_premium',
  'trade_near_dte_size',
  'trade_0dte_size',
  'trade_1dte_size',
  'trade_2_7dte_size',
  'trade_8_30dte_size',
  'trade_atm_size',
  'trade_otm_call_size',
  'trade_otm_put_size',
]);

function sanitizeSymbol(symbol) {
  return String(symbol || '').replace(/^I:/, '').toLowerCase().replace(/[^a-z0-9]+/g, '_');
}

function safeReturn(current, previous) {
  if (!Number.isFinite(current) || !Number.isFinite(previous) || previous <= 0) return 0;
  return (current / previous) - 1;
}

function safeRatio(numerator, denominator) {
  if (!Number.isFinite(numerator) || !Number.isFinite(denominator) || denominator === 0) return 0;
  return numerator / denominator;
}

function createOptionRootToGroup(optionRoots) {
  const out = new Map();
  Object.entries(optionRoots || {}).forEach(([group, roots]) => {
    roots.forEach((root) => out.set(String(root).toUpperCase(), group));
  });
  return out;
}

function allCrossAssetSymbols(config) {
  return (config.stockSymbols || []).filter((symbol) => symbol !== config.target);
}

function groupSymbols(config, groupName) {
  if (groupName === 'mag7') return config.mag7Symbols || [];
  if (groupName === 'market') return config.marketEtfSymbols || [];
  if (groupName === 'sector') return config.sectorEtfSymbols || [];
  if (groupName === 'risk') return config.riskProxySymbols || [];
  if (groupName === 'cross') return allCrossAssetSymbols(config);
  return [];
}

function createEmptyGroupAgg() {
  const out = {};
  OPTION_METRICS.forEach((metric) => {
    out[metric] = 0;
  });
  return out;
}

function createEmptyOptionAgg() {
  const out = {};
  OPTION_GROUPS.forEach((group) => {
    out[group] = createEmptyGroupAgg();
  });
  return out;
}

function getOptionAgg(optionByMinute, minuteMs) {
  let agg = optionByMinute.get(minuteMs);
  if (!agg) {
    agg = createEmptyOptionAgg();
    optionByMinute.set(minuteMs, agg);
  }
  return agg;
}

function dteBucket(dte) {
  if (dte === 0) return '0dte';
  if (dte === 1) return '1dte';
  if (dte >= 2 && dte <= 7) return '2_7dte';
  if (dte >= 8 && dte <= 30) return '8_30dte';
  return null;
}

function optionMoneynessBucket(parsed, underlyingClose) {
  if (!parsed || !(underlyingClose > 0)) return null;
  const ratio = parsed.strike / underlyingClose;
  if (Math.abs(ratio - 1) <= 0.005) return 'atm';
  if (parsed.right === 'CALL') return ratio > 1 ? 'otm_call' : 'itm_call';
  return ratio < 1 ? 'otm_put' : 'itm_put';
}

function buildCloseLookup(seriesBySymbol) {
  const out = new Map();
  seriesBySymbol.forEach((rows, symbol) => {
    const byMinute = new Map();
    rows.forEach((row) => byMinute.set(row.minuteMs, row.close));
    out.set(symbol, byMinute);
  });
  return out;
}

function lookupUnderlyingClose(parsed, minuteMs, closeLookup) {
  if (!parsed) return null;
  let symbol = parsed.root;
  if (parsed.root === 'SPXW') symbol = 'I:SPX';
  else if (parsed.root === 'VIXW') symbol = 'I:VIX';
  else if (parsed.root === 'SPX') symbol = 'I:SPX';
  else if (parsed.root === 'VIX') symbol = 'I:VIX';
  return closeLookup.get(symbol)?.get(minuteMs) ?? null;
}

function addDteAndMoneyness(target, prefix, dte, moneyness) {
  const bucket = dteBucket(dte);
  if (bucket) {
    if (prefix === 'quote') target[`quote_${bucket}_volume`] += target.__lastVolume || 0;
    if (prefix === 'trade') target[`trade_${bucket}_size`] += target.__lastSize || 0;
  }
  if (moneyness === 'atm') {
    if (prefix === 'quote') target.quote_atm_volume += target.__lastVolume || 0;
    if (prefix === 'trade') target.trade_atm_size += target.__lastSize || 0;
  }
  if (moneyness === 'otm_call') {
    if (prefix === 'quote') target.quote_otm_call_volume += target.__lastVolume || 0;
    if (prefix === 'trade') target.trade_otm_call_size += target.__lastSize || 0;
  }
  if (moneyness === 'otm_put') {
    if (prefix === 'quote') target.quote_otm_put_volume += target.__lastVolume || 0;
    if (prefix === 'trade') target.trade_otm_put_size += target.__lastSize || 0;
  }
}

function addOptionQuote(optionByMinute, dayIso, row, rootToGroup, closeLookup) {
  const parsed = parseOpraTicker(row.ticker);
  if (!parsed) return;
  const group = rootToGroup.get(parsed.root);
  if (!group) return;
  const minuteMs = nsToMinuteMs(row.window_start);
  if (minuteMs === null) return;
  const volume = toNumber(row.volume) || 0;
  const close = toNumber(row.close) || 0;
  const transactions = toNumber(row.transactions) || 0;
  const premium = volume * close * 100;
  const dte = daysBetween(dayIso, parsed.expiration);
  const target = getOptionAgg(optionByMinute, minuteMs)[group];
  const moneyness = optionMoneynessBucket(parsed, lookupUnderlyingClose(parsed, minuteMs, closeLookup));
  target.quote_volume += volume;
  target.quote_premium += premium;
  target.quote_transactions += transactions;
  if (dte !== null && dte >= 0 && dte <= 7) target.quote_near_dte_volume += volume;
  if (parsed.right === 'CALL') {
    target.quote_call_volume += volume;
    target.quote_call_premium += premium;
  } else {
    target.quote_put_volume += volume;
    target.quote_put_premium += premium;
  }
  target.__lastVolume = volume;
  addDteAndMoneyness(target, 'quote', dte, moneyness);
  delete target.__lastVolume;
}

function addOptionTrade(optionByMinute, dayIso, row, rootToGroup, closeLookup) {
  const parsed = parseOpraTicker(row.ticker);
  if (!parsed) return;
  const group = rootToGroup.get(parsed.root);
  if (!group) return;
  const minuteMs = nsToMinuteMs(row.sip_timestamp);
  if (minuteMs === null) return;
  const size = toNumber(row.size) || 0;
  const price = toNumber(row.price) || 0;
  const premium = size * price * 100;
  const dte = daysBetween(dayIso, parsed.expiration);
  const target = getOptionAgg(optionByMinute, minuteMs)[group];
  const moneyness = optionMoneynessBucket(parsed, lookupUnderlyingClose(parsed, minuteMs, closeLookup));
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
  target.__lastSize = size;
  addDteAndMoneyness(target, 'trade', dte, moneyness);
  delete target.__lastSize;
}

function preprocessSeries(rows) {
  rows.sort((left, right) => left.minuteMs - right.minuteMs);
  rows.forEach((row, index) => {
    row.ret1 = safeReturn(row.close, rows[index - 1]?.close);
    row.ret5 = safeReturn(row.close, rows[index - 5]?.close);
    row.ret15 = safeReturn(row.close, rows[index - 15]?.close);
    row.ret60 = safeReturn(row.close, rows[index - 60]?.close);
    row.rv5 = Math.sqrt(rows.slice(Math.max(0, index - 4), index + 1).reduce((sum, item) => sum + ((item.ret1 || 0) ** 2), 0));
    row.rv15 = Math.sqrt(rows.slice(Math.max(0, index - 14), index + 1).reduce((sum, item) => sum + ((item.ret1 || 0) ** 2), 0));
    row.rv30 = Math.sqrt(rows.slice(Math.max(0, index - 29), index + 1).reduce((sum, item) => sum + ((item.ret1 || 0) ** 2), 0));
    row.rv60 = Math.sqrt(rows.slice(Math.max(0, index - 59), index + 1).reduce((sum, item) => sum + ((item.ret1 || 0) ** 2), 0));
  });
  return rows;
}

async function readStockSeriesForDay(config, dayIso) {
  const filePath = datasetCsvPath(config, 'stockBars', dayIso);
  const selected = new Set(config.stockSymbols.map((symbol) => symbol.toUpperCase()));
  const bySymbol = new Map();
  if (!fs.existsSync(filePath)) return bySymbol;
  await readGzipCsv(filePath, (row) => {
    const symbol = String(row.ticker || '').toUpperCase();
    if (!selected.has(symbol)) return;
    const minuteMs = nsToMinuteMs(row.window_start);
    if (minuteMs === null || !isRegularSessionMinute(minuteMs, config.session)) return;
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
  bySymbol.forEach((rows, symbol) => bySymbol.set(symbol, preprocessSeries(rows)));
  return bySymbol;
}

async function readIndexSeriesForDay(config, dayIso) {
  const filePath = datasetCsvPath(config, 'indexBars', dayIso);
  const selected = new Set(config.indexSymbols.map((symbol) => symbol.toUpperCase()));
  const bySymbol = new Map();
  if (!fs.existsSync(filePath)) return bySymbol;
  await readGzipCsv(filePath, (row) => {
    const symbol = String(row.ticker || '').toUpperCase();
    if (!selected.has(symbol)) return;
    const minuteMs = nsToMinuteMs(row.window_start);
    if (minuteMs === null || !isRegularSessionMinute(minuteMs, config.session)) return;
    const list = bySymbol.get(symbol) || [];
    list.push({
      symbol,
      minuteMs,
      open: toNumber(row.open),
      high: toNumber(row.high),
      low: toNumber(row.low),
      close: toNumber(row.close),
    });
    bySymbol.set(symbol, list);
  });
  bySymbol.forEach((rows, symbol) => bySymbol.set(symbol, preprocessSeries(rows)));
  return bySymbol;
}

async function readOptionAggsForDay(config, dayIso, { includeOptions = true, stockBySymbol = new Map(), indexBySymbol = new Map() } = {}) {
  const optionByMinute = new Map();
  if (!includeOptions) return optionByMinute;
  const rootToGroup = createOptionRootToGroup(config.optionRoots);
  const closeLookup = buildCloseLookup(new Map([...stockBySymbol, ...indexBySymbol]));
  const quotePath = datasetCsvPath(config, 'optionBars', dayIso);
  if (fs.existsSync(quotePath)) {
    await readGzipCsv(quotePath, (row) => addOptionQuote(optionByMinute, dayIso, row, rootToGroup, closeLookup));
  }
  const tradePath = datasetCsvPath(config, 'optionTrades', dayIso);
  if (fs.existsSync(tradePath)) {
    await readGzipCsv(tradePath, (row) => addOptionTrade(optionByMinute, dayIso, row, rootToGroup, closeLookup));
  }
  return optionByMinute;
}

function getForwardFilled(series, pointerState, minuteMs) {
  const rows = series || [];
  while (pointerState.index + 1 < rows.length && rows[pointerState.index + 1].minuteMs <= minuteMs) {
    pointerState.index += 1;
  }
  return pointerState.index >= 0 ? rows[pointerState.index] : null;
}

function addOptionFeatures(row, currentAgg, rollingHistory) {
  const flatCurrent = {};
  OPTION_GROUPS.forEach((group) => {
    const agg = currentAgg?.[group] || createEmptyGroupAgg();
    const prefix = `opt_${group}`;
    OPTION_METRICS.forEach((metric) => {
      flatCurrent[`${prefix}_${metric}`] = agg[metric] || 0;
      row[`${prefix}_${metric}`] = agg[metric] || 0;
    });
    row[`${prefix}_quote_volume_imbalance`] = safeRatio(
      (agg.quote_call_volume || 0) - (agg.quote_put_volume || 0),
      (agg.quote_call_volume || 0) + (agg.quote_put_volume || 0),
    );
    row[`${prefix}_quote_premium_imbalance`] = safeRatio(
      (agg.quote_call_premium || 0) - (agg.quote_put_premium || 0),
      (agg.quote_call_premium || 0) + (agg.quote_put_premium || 0),
    );
    row[`${prefix}_trade_size_imbalance`] = safeRatio(
      (agg.trade_call_size || 0) - (agg.trade_put_size || 0),
      (agg.trade_call_size || 0) + (agg.trade_put_size || 0),
    );
    row[`${prefix}_trade_premium_imbalance`] = safeRatio(
      (agg.trade_call_premium || 0) - (agg.trade_put_premium || 0),
      (agg.trade_call_premium || 0) + (agg.trade_put_premium || 0),
    );
    row[`${prefix}_quote_near_dte_share`] = safeRatio(agg.quote_near_dte_volume || 0, agg.quote_volume || 0);
    row[`${prefix}_trade_near_dte_share`] = safeRatio(agg.trade_near_dte_size || 0, agg.trade_size || 0);
    row[`${prefix}_quote_0dte_share`] = safeRatio(agg.quote_0dte_volume || 0, agg.quote_volume || 0);
    row[`${prefix}_trade_0dte_share`] = safeRatio(agg.trade_0dte_size || 0, agg.trade_size || 0);
    row[`${prefix}_quote_1dte_share`] = safeRatio(agg.quote_1dte_volume || 0, agg.quote_volume || 0);
    row[`${prefix}_trade_1dte_share`] = safeRatio(agg.trade_1dte_size || 0, agg.trade_size || 0);
    row[`${prefix}_quote_atm_share`] = safeRatio(agg.quote_atm_volume || 0, agg.quote_volume || 0);
    row[`${prefix}_trade_atm_share`] = safeRatio(agg.trade_atm_size || 0, agg.trade_size || 0);
  });

  rollingHistory.push(flatCurrent);
  if (rollingHistory.length > 15) rollingHistory.shift();

  [5, 15].forEach((windowSize) => {
    const window = rollingHistory.slice(-windowSize);
    OPTION_GROUPS.forEach((group) => {
      const prefix = `opt_${group}`;
      const quoteCall = window.reduce((sum, item) => sum + (item[`${prefix}_quote_call_volume`] || 0), 0);
      const quotePut = window.reduce((sum, item) => sum + (item[`${prefix}_quote_put_volume`] || 0), 0);
      const tradeCall = window.reduce((sum, item) => sum + (item[`${prefix}_trade_call_size`] || 0), 0);
      const tradePut = window.reduce((sum, item) => sum + (item[`${prefix}_trade_put_size`] || 0), 0);
      row[`${prefix}_quote_volume_${windowSize}m`] = quoteCall + quotePut;
      row[`${prefix}_quote_imbalance_${windowSize}m`] = safeRatio(quoteCall - quotePut, quoteCall + quotePut);
      row[`${prefix}_trade_size_${windowSize}m`] = tradeCall + tradePut;
      row[`${prefix}_trade_imbalance_${windowSize}m`] = safeRatio(tradeCall - tradePut, tradeCall + tradePut);
    });
  });
}

function addOpeningOptionFeatures(row, openingOptionTotals, ready) {
  row.opening_option_proxy_ready = ready ? 1 : 0;
  OPTION_GROUPS.forEach((group) => {
    const agg = openingOptionTotals[group] || createEmptyGroupAgg();
    const prefix = `opening_opt_${group}`;
    row[`${prefix}_quote_volume`] = agg.quote_volume || 0;
    row[`${prefix}_quote_premium`] = agg.quote_premium || 0;
    row[`${prefix}_trade_size`] = agg.trade_size || 0;
    row[`${prefix}_trade_premium`] = agg.trade_premium || 0;
    row[`${prefix}_quote_premium_imbalance`] = safeRatio(
      (agg.quote_call_premium || 0) - (agg.quote_put_premium || 0),
      (agg.quote_call_premium || 0) + (agg.quote_put_premium || 0),
    );
    row[`${prefix}_trade_premium_imbalance`] = safeRatio(
      (agg.trade_call_premium || 0) - (agg.trade_put_premium || 0),
      (agg.trade_call_premium || 0) + (agg.trade_put_premium || 0),
    );
    row[`${prefix}_quote_0dte_share`] = safeRatio(agg.quote_0dte_volume || 0, agg.quote_volume || 0);
    row[`${prefix}_trade_0dte_share`] = safeRatio(agg.trade_0dte_size || 0, agg.trade_size || 0);
  });
}

function mergeOptionAgg(target, source) {
  OPTION_GROUPS.forEach((group) => {
    const targetGroup = target[group];
    const sourceGroup = source?.[group] || createEmptyGroupAgg();
    OPTION_METRICS.forEach((metric) => {
      targetGroup[metric] += sourceGroup[metric] || 0;
    });
  });
}

function addGroupStats(row, groupName, returns) {
  const finite = returns.filter((value) => Number.isFinite(value));
  row[`${groupName}_ret_1m_mean`] = finite.length ? finite.reduce((sum, value) => sum + value, 0) / finite.length : 0;
  row[`${groupName}_breadth_1m`] = finite.length ? finite.filter((value) => value > 0).length / finite.length : 0;
}

function buildRowsForDay({ config, dayIso, stockBySymbol, indexBySymbol, optionByMinute }) {
  const targetRows = stockBySymbol.get(config.target) || [];
  const pointers = new Map();
  [...allCrossAssetSymbols(config), ...config.indexSymbols].forEach((symbol) => {
    pointers.set(symbol, { index: -1 });
  });
  const rollingOptions = [];
  const openingOptionTotals = createEmptyOptionAgg();
  const firstSpy = targetRows[0] || null;
  const open30Row = targetRows[29] || null;
  const preCloseStart = targetRows.find((row) => getEtParts(row.minuteMs).minuteOfDayEt === (config.research?.preCloseWindowStartMinuteEt || 900));
  const lastThirtyEntry = targetRows.find((row) => getEtParts(row.minuteMs).minuteOfDayEt === (config.research?.lastThirtyEntryMinuteEt || 930));
  const rows = [];

  targetRows.forEach((spy, index) => {
    const et = getEtParts(spy.minuteMs);
    const row = {
      rowId: `${dayIso}|${minuteMsToIso(spy.minuteMs)}`,
      tradeDate: dayIso,
      minuteUtc: minuteMsToIso(spy.minuteMs),
      minuteMs: spy.minuteMs,
      minuteOfDayEt: et.minuteOfDayEt,
      spy_open: spy.open,
      spy_high: spy.high,
      spy_low: spy.low,
      spy_close: spy.close,
      spy_volume: spy.volume,
      spy_transactions: spy.transactions,
      spy_ret_1m: safeReturn(spy.close, targetRows[index - 1]?.close),
      spy_ret_5m: safeReturn(spy.close, targetRows[index - 5]?.close),
      spy_ret_15m: safeReturn(spy.close, targetRows[index - 15]?.close),
      spy_ret_60m: safeReturn(spy.close, targetRows[index - 60]?.close),
      spy_rv_5m: spy.rv5 || 0,
      spy_rv_15m: spy.rv15 || 0,
      spy_rv_30m: spy.rv30 || 0,
      spy_rv_60m: spy.rv60 || 0,
      spy_range_pct: safeRatio((spy.high || spy.close) - (spy.low || spy.close), spy.close),
      spy_volume_log: Math.log1p(spy.volume || 0),
      minutes_from_open: et.minuteOfDayEt - config.session.regularOpenMinuteEt,
      minutes_to_close: config.session.regularCloseMinuteEt - et.minuteOfDayEt - 1,
    };

    row.opening_30m_return = firstSpy?.close && spy.close ? safeReturn((index >= 29 ? open30Row?.close : spy.close), firstSpy.close) : 0;
    row.opening_30m_complete = index >= 29 ? 1 : 0;
    row.opening_30m_range_pct = index >= 29 && open30Row
      ? safeRatio(
        Math.max(...targetRows.slice(0, 30).map((item) => item.high || item.close)) - Math.min(...targetRows.slice(0, 30).map((item) => item.low || item.close)),
        open30Row.close,
      )
      : 0;
    row.preclose_1500_1530_return = et.minuteOfDayEt >= (config.research?.lastThirtyEntryMinuteEt || 930) && preCloseStart?.close && lastThirtyEntry?.close
      ? safeReturn(lastThirtyEntry.close, preCloseStart.close)
      : 0;
    row.preclose_1500_1530_complete = et.minuteOfDayEt >= (config.research?.lastThirtyEntryMinuteEt || 930) ? 1 : 0;

    const groupReturns = {
      mag7: [],
      market: [],
      sector: [],
      risk: [],
      cross: [],
    };
    allCrossAssetSymbols(config).forEach((symbol) => {
      const featureSymbol = sanitizeSymbol(symbol);
      const current = getForwardFilled(stockBySymbol.get(symbol), pointers.get(symbol), spy.minuteMs);
      row[`${featureSymbol}_close`] = current?.close ?? null;
      row[`${featureSymbol}_ret_1m`] = current?.ret1 ?? 0;
      row[`${featureSymbol}_ret_5m`] = current?.ret5 ?? 0;
      row[`${featureSymbol}_ret_15m`] = current?.ret15 ?? 0;
      row[`${featureSymbol}_ret_60m`] = current?.ret60 ?? 0;
      row[`${featureSymbol}_rel_spy_ret_1m`] = (current?.ret1 || 0) - (row.spy_ret_1m || 0);
      row[`${featureSymbol}_rel_spy_ret_5m`] = (current?.ret5 || 0) - (row.spy_ret_5m || 0);
      row[`${featureSymbol}_volume_log`] = Math.log1p(current?.volume || 0);
      if (current) {
        groupReturns.cross.push(current.ret1 || 0);
        if (groupSymbols(config, 'mag7').includes(symbol)) groupReturns.mag7.push(current.ret1 || 0);
        if (groupSymbols(config, 'market').includes(symbol)) groupReturns.market.push(current.ret1 || 0);
        if (groupSymbols(config, 'sector').includes(symbol)) groupReturns.sector.push(current.ret1 || 0);
        if (groupSymbols(config, 'risk').includes(symbol)) groupReturns.risk.push(current.ret1 || 0);
      }
    });
    Object.entries(groupReturns).forEach(([groupName, returns]) => addGroupStats(row, groupName, returns));

    config.indexSymbols.forEach((symbol) => {
      const featureSymbol = sanitizeSymbol(symbol);
      const current = getForwardFilled(indexBySymbol.get(symbol), pointers.get(symbol), spy.minuteMs);
      row[`${featureSymbol}_close`] = current?.close ?? null;
      row[`${featureSymbol}_ret_1m`] = current?.ret1 ?? 0;
      row[`${featureSymbol}_ret_5m`] = current?.ret5 ?? 0;
      row[`${featureSymbol}_ret_15m`] = current?.ret15 ?? 0;
      row[`${featureSymbol}_ret_60m`] = current?.ret60 ?? 0;
    });

    row.vix1d_over_vix = safeRatio(row.vix1d_close, row.vix_close);
    row.vix9d_over_vix = safeRatio(row.vix9d_close, row.vix_close);
    row.vix3m_over_vix = safeRatio(row.vix3m_close, row.vix_close);
    row.vix_term_1d_9d = row.vix1d_close && row.vix9d_close ? row.vix1d_close - row.vix9d_close : 0;
    row.vix_term_9d_3m = row.vix9d_close && row.vix3m_close ? row.vix9d_close - row.vix3m_close : 0;
    row.spx_spy_ret_spread_1m = (row.spx_ret_1m || 0) - (row.spy_ret_1m || 0);

    const currentOptionAgg = optionByMinute.get(spy.minuteMs);
    if (index < (config.research?.openingWindowMinutes || 30)) mergeOptionAgg(openingOptionTotals, currentOptionAgg);
    addOptionFeatures(row, currentOptionAgg, rollingOptions);
    addOpeningOptionFeatures(row, openingOptionTotals, index >= ((config.research?.openingWindowMinutes || 30) - 1));
    row.gamma_proxy_spx_spy_0dte_share = safeRatio(
      (row.opt_spx_quote_0dte_volume || 0) + (row.opt_spy_quote_0dte_volume || 0),
      (row.opt_spx_quote_volume || 0) + (row.opt_spy_quote_volume || 0),
    );
    row.gamma_proxy_short_dte_pressure = safeRatio(
      (row.opt_spx_quote_0dte_volume || 0) + (row.opt_spy_quote_0dte_volume || 0) + (row.opt_spx_quote_1dte_volume || 0) + (row.opt_spy_quote_1dte_volume || 0),
      (row.opt_spx_quote_volume || 0) + (row.opt_spy_quote_volume || 0),
    );
    row.gamma_proxy_atm_pressure = safeRatio(
      (row.opt_spx_quote_atm_volume || 0) + (row.opt_spy_quote_atm_volume || 0),
      (row.opt_spx_quote_volume || 0) + (row.opt_spy_quote_volume || 0),
    );
    rows.push(row);
  });

  return rows;
}

async function buildDatasetRows(config, dates, settings = {}) {
  const includeOptions = settings.includeOptions !== false;
  const allRows = [];
  for (const dayIso of dates) {
    const stockBySymbol = await readStockSeriesForDay(config, dayIso);
    const indexBySymbol = await readIndexSeriesForDay(config, dayIso);
    const optionByMinute = await readOptionAggsForDay(config, dayIso, { includeOptions, stockBySymbol, indexBySymbol });
    const dayRows = buildRowsForDay({ config, dayIso, stockBySymbol, indexBySymbol, optionByMinute });
    allRows.push(...dayRows);
  }
  return assignForwardLabels(allRows, config.horizons, {
    lastThirtyEntryMinuteEt: config.research?.lastThirtyEntryMinuteEt,
    tripleBarrier: config.research?.tripleBarrier,
  });
}

async function buildDatasetToJsonl(config, dates, filePath, settings = {}) {
  const includeOptions = settings.includeOptions !== false;
  const stream = fs.createWriteStream(filePath, { encoding: 'utf8' });
  let rowCount = 0;
  let dayCount = 0;
  try {
    for (const dayIso of dates) {
      const startedAt = Date.now();
      const stockBySymbol = await readStockSeriesForDay(config, dayIso);
      const indexBySymbol = await readIndexSeriesForDay(config, dayIso);
      const optionByMinute = await readOptionAggsForDay(config, dayIso, { includeOptions, stockBySymbol, indexBySymbol });
      const dayRows = buildRowsForDay({ config, dayIso, stockBySymbol, indexBySymbol, optionByMinute });
      assignForwardLabels(dayRows, config.horizons, {
        lastThirtyEntryMinuteEt: config.research?.lastThirtyEntryMinuteEt,
        tripleBarrier: config.research?.tripleBarrier,
      });
      dayRows.forEach((row) => {
        stream.write(`${JSON.stringify(row)}\n`);
      });
      rowCount += dayRows.length;
      dayCount += 1;
      if (settings.onDayComplete) {
        settings.onDayComplete({
          dayIso,
          dayCount,
          rowCount,
          dayRows: dayRows.length,
          elapsedMs: Date.now() - startedAt,
        });
      }
    }
  } finally {
    await new Promise((resolve, reject) => {
      stream.end((error) => (error ? reject(error) : resolve()));
    });
  }
  return { rowCount, dayCount };
}

function writeJsonl(filePath, rows) {
  const stream = fs.createWriteStream(filePath, { encoding: 'utf8' });
  rows.forEach((row) => {
    stream.write(`${JSON.stringify(row)}\n`);
  });
  stream.end();
}

function readJsonl(filePath) {
  return fs.readFileSync(filePath, 'utf8')
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean)
    .map((line) => JSON.parse(line));
}

async function readJsonlStreaming(filePath) {
  const rows = [];
  const reader = readline.createInterface({
    input: fs.createReadStream(filePath, { encoding: 'utf8' }),
    crlfDelay: Infinity,
  });
  for await (const line of reader) {
    const trimmed = line.trim();
    if (trimmed) rows.push(JSON.parse(trimmed));
  }
  return rows;
}

module.exports = {
  OPTION_GROUPS,
  OPTION_METRICS,
  sanitizeSymbol,
  safeReturn,
  safeRatio,
  createOptionRootToGroup,
  dteBucket,
  optionMoneynessBucket,
  allCrossAssetSymbols,
  buildDatasetRows,
  buildDatasetToJsonl,
  writeJsonl,
  readJsonl,
  readJsonlStreaming,
};
