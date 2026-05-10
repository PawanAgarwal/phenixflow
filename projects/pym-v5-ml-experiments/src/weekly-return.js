const fs = require('node:fs');
const path = require('node:path');

const { readDailyBarsJsonl } = require('../../pym-v5-replication/src/backtest');
const { ensureDir, runtimePath } = require('../../pym-v5-replication/src/config');
const { findLatestMassiveEodBarsPath } = require('../../pym-v5-replication/src/rebalance-report');
const { artifactPath, fitRidgeMulti, predictLinearMulti } = require('./experiment');

const DEFAULT_TARGET_TICKER = 'SPY';
const DEFAULT_SAFE_TICKER = 'BIL';
const DEFAULT_PREDICT_START = '2025-01-01';
const DEFAULT_VALIDATION_START = '2025-01-01';
const DEFAULT_VALIDATION_END = '2025-12-31';
const DEFAULT_TEST_START = '2026-01-01';
const DEFAULT_INITIAL_CAPITAL = 10000;
const DEFAULT_COST_BPS = 2;
const DEFAULT_MIN_TRAIN_WEEKS = 35;
const DEFAULT_MIN_LOOKBACK_DAYS = 63;
const DEFAULT_MIN_ANCHOR_WEEKS = 13;
const DEFAULT_TARGET_MODE = 'close_to_close';

const DAILY_RETURN_WINDOWS = Object.freeze([1, 2, 3, 5, 10, 21, 42, 63, 126, 252]);
const DAILY_VOL_WINDOWS = Object.freeze([5, 10, 21, 42, 63]);
const MA_WINDOWS = Object.freeze([10, 21, 50, 100, 200]);
const DRAWDOWN_WINDOWS = Object.freeze([21, 63, 126, 252]);
const RSI_WINDOWS = Object.freeze([2, 5, 14]);
const WEEKLY_RETURN_WINDOWS = Object.freeze([1, 2, 4, 8, 13, 26, 52]);
const CROSS_DAILY_WINDOWS = Object.freeze([5, 21, 63]);
const CROSS_WEEKLY_WINDOWS = Object.freeze([1, 4, 13, 26]);
const DEFAULT_CORE_TICKERS = Object.freeze([
  'SPY', 'QQQ', 'IWM', 'TLT', 'IEF', 'GLD', 'UUP', 'USO',
  'VIXY', 'UVXY', 'SVIX', 'SVXY', 'XLK', 'XLF', 'XLU', 'XLP',
  'XLV', 'XLE', 'EEM', 'KMLM',
]);

function pct(value) {
  return Number.isFinite(value) ? value * 100 : null;
}

function finite(value, fallback = 0) {
  return Number.isFinite(value) ? value : fallback;
}

function mean(values) {
  return values.length ? values.reduce((sum, value) => sum + value, 0) / values.length : 0;
}

function standardDeviation(values) {
  if (values.length < 2) return 0;
  const avg = mean(values);
  return Math.sqrt(mean(values.map((value) => (value - avg) ** 2)));
}

function correlation(leftValues, rightValues) {
  const pairs = leftValues
    .map((left, index) => [left, rightValues[index]])
    .filter(([left, right]) => Number.isFinite(left) && Number.isFinite(right));
  if (pairs.length < 3) return 0;
  const leftMean = mean(pairs.map(([left]) => left));
  const rightMean = mean(pairs.map(([, right]) => right));
  let numerator = 0;
  let leftVariance = 0;
  let rightVariance = 0;
  pairs.forEach(([left, right]) => {
    const leftCentered = left - leftMean;
    const rightCentered = right - rightMean;
    numerator += leftCentered * rightCentered;
    leftVariance += leftCentered ** 2;
    rightVariance += rightCentered ** 2;
  });
  const denominator = Math.sqrt(leftVariance * rightVariance);
  return denominator > 0 ? numerator / denominator : 0;
}

function clamp(value, low = 0, high = 1) {
  return Math.max(low, Math.min(high, finite(value)));
}

function dateToUtc(date) {
  return new Date(`${date}T00:00:00Z`);
}

function isoDate(date) {
  return date.toISOString().slice(0, 10);
}

function weekStartKey(date) {
  const value = dateToUtc(date);
  const day = value.getUTCDay() || 7;
  value.setUTCDate(value.getUTCDate() - day + 1);
  return isoDate(value);
}

function nextCalendarFriday(date) {
  const value = dateToUtc(date);
  const day = value.getUTCDay();
  const daysUntilFriday = (5 - day + 7) % 7 || 7;
  value.setUTCDate(value.getUTCDate() + daysUntilFriday);
  return isoDate(value);
}

function findPreferredMassiveEodBarsPath() {
  const explicit = process.env.PYM_V5_DAILY_BARS_PATH;
  if (explicit) return path.resolve(explicit);
  const runtimeRoot = runtimePath();
  if (!fs.existsSync(runtimeRoot)) return findLatestMassiveEodBarsPath();
  const matches = fs.readdirSync(runtimeRoot)
    .map((name) => {
      const match = name.match(/^pym-v5-massive-eod-adjusted-daily-bars-(\d{4}-\d{2}-\d{2})-(\d{4}-\d{2}-\d{2})\.jsonl$/);
      return match ? { name, startDate: match[1], endDate: match[2] } : null;
    })
    .filter(Boolean)
    .sort((left, right) => (
      left.endDate.localeCompare(right.endDate)
      || right.startDate.localeCompare(left.startDate)
      || left.name.localeCompare(right.name)
    ));
  return matches.length ? path.join(runtimeRoot, matches.at(-1).name) : findLatestMassiveEodBarsPath();
}

function rowAt(market, ticker, index) {
  return market.byDate.get(market.dates[index])?.get(ticker) || null;
}

function closeAt(market, ticker, index) {
  const values = market.closes.get(ticker) || [];
  return values[index];
}

function openAt(market, ticker, index) {
  const row = rowAt(market, ticker, index);
  return Number.isFinite(row?.open) && row.open > 0 ? row.open : closeAt(market, ticker, index);
}

function dailyReturn(market, ticker, index, window = 1) {
  const current = closeAt(market, ticker, index);
  const previous = closeAt(market, ticker, index - window);
  if (!Number.isFinite(current) || !Number.isFinite(previous) || previous <= 0) return 0;
  return (current / previous) - 1;
}

function realizedVolatility(market, ticker, index, window) {
  const returns = [];
  for (let cursor = Math.max(1, index - window + 1); cursor <= index; cursor += 1) {
    returns.push(dailyReturn(market, ticker, cursor));
  }
  return standardDeviation(returns) * Math.sqrt(252);
}

function movingAverageDistance(market, ticker, index, window) {
  const closes = market.closes.get(ticker) || [];
  const current = closes[index];
  const start = Math.max(0, index - window + 1);
  const values = closes.slice(start, index + 1).filter(Number.isFinite);
  if (!Number.isFinite(current) || !values.length) return 0;
  const avg = mean(values);
  return avg > 0 ? (current / avg) - 1 : 0;
}

function rollingDrawdown(market, ticker, index, window) {
  const closes = market.closes.get(ticker) || [];
  const current = closes[index];
  if (!Number.isFinite(current)) return 0;
  let peak = current;
  for (let cursor = Math.max(0, index - window + 1); cursor <= index; cursor += 1) {
    if (Number.isFinite(closes[cursor])) peak = Math.max(peak, closes[cursor]);
  }
  return peak > 0 ? (current / peak) - 1 : 0;
}

function rsi(market, ticker, index, window) {
  if (index < window) return 50;
  let gains = 0;
  let losses = 0;
  for (let cursor = index - window + 1; cursor <= index; cursor += 1) {
    const ret = dailyReturn(market, ticker, cursor);
    if (ret > 0) gains += ret;
    else losses -= ret;
  }
  const avgGain = gains / window;
  const avgLoss = losses / window;
  if (avgGain === 0 && avgLoss === 0) return 50;
  if (avgLoss === 0) return 100;
  return 100 - (100 / (1 + (avgGain / avgLoss)));
}

function buildWeeklyAnchors(market) {
  const anchors = [];
  let current = null;
  market.dates.forEach((date, index) => {
    const key = weekStartKey(date);
    if (!current || current.weekKey !== key) {
      if (current) anchors.push(current);
      current = { weekKey: key, startIndex: index, index, date };
    } else {
      current.index = index;
      current.date = date;
    }
  });
  if (current) anchors.push(current);
  return anchors;
}

function weeklyReturn(market, ticker, anchors, anchorPosition, window = 1) {
  const current = closeAt(market, ticker, anchors[anchorPosition]?.index);
  const previous = closeAt(market, ticker, anchors[anchorPosition - window]?.index);
  if (!Number.isFinite(current) || !Number.isFinite(previous) || previous <= 0) return 0;
  return (current / previous) - 1;
}

function weeklyVolatility(market, ticker, anchors, anchorPosition, window) {
  const returns = [];
  for (let cursor = Math.max(1, anchorPosition - window + 1); cursor <= anchorPosition; cursor += 1) {
    returns.push(weeklyReturn(market, ticker, anchors, cursor));
  }
  return standardDeviation(returns) * Math.sqrt(52);
}

function weeklyBarFeature(market, ticker, anchor) {
  let high = -Infinity;
  let low = Infinity;
  let volume = 0;
  for (let index = anchor.startIndex; index <= anchor.index; index += 1) {
    const row = rowAt(market, ticker, index);
    if (!row) continue;
    if (Number.isFinite(row.high)) high = Math.max(high, row.high);
    if (Number.isFinite(row.low)) low = Math.min(low, row.low);
    if (Number.isFinite(row.volume)) volume += row.volume;
  }
  const open = openAt(market, ticker, anchor.startIndex);
  const close = closeAt(market, ticker, anchor.index);
  const weekReturn = Number.isFinite(open) && open > 0 && Number.isFinite(close) ? (close / open) - 1 : 0;
  const range = Number.isFinite(high) && Number.isFinite(low) && low > 0 ? (high / low) - 1 : 0;
  const closeLocation = Number.isFinite(high) && Number.isFinite(low) && high > low && Number.isFinite(close)
    ? ((close - low) / (high - low)) - 0.5
    : 0;
  return { weekReturn, range, closeLocation, logVolume: volume > 0 ? Math.log(volume) : 0 };
}

function appendFeature(values, names, name, value) {
  values.push(finite(value));
  names?.push(name);
}

function appendTargetFeatures(values, names, market, anchors, anchorPosition, targetTicker) {
  const anchor = anchors[anchorPosition];
  DAILY_RETURN_WINDOWS.forEach((window) => {
    appendFeature(values, names, `${targetTicker}_daily_ret_${window}`, dailyReturn(market, targetTicker, anchor.index, window));
  });
  DAILY_VOL_WINDOWS.forEach((window) => {
    appendFeature(values, names, `${targetTicker}_daily_vol_${window}`, realizedVolatility(market, targetTicker, anchor.index, window));
  });
  MA_WINDOWS.forEach((window) => {
    appendFeature(values, names, `${targetTicker}_ma_dist_${window}`, movingAverageDistance(market, targetTicker, anchor.index, window));
  });
  DRAWDOWN_WINDOWS.forEach((window) => {
    appendFeature(values, names, `${targetTicker}_drawdown_${window}`, rollingDrawdown(market, targetTicker, anchor.index, window));
  });
  RSI_WINDOWS.forEach((window) => {
    appendFeature(values, names, `${targetTicker}_rsi_${window}`, (rsi(market, targetTicker, anchor.index, window) - 50) / 50);
  });
  WEEKLY_RETURN_WINDOWS.forEach((window) => {
    appendFeature(values, names, `${targetTicker}_weekly_ret_${window}`, weeklyReturn(market, targetTicker, anchors, anchorPosition, window));
  });
  [4, 13, 26].forEach((window) => {
    appendFeature(values, names, `${targetTicker}_weekly_vol_${window}`, weeklyVolatility(market, targetTicker, anchors, anchorPosition, window));
  });
  const weeklyBar = weeklyBarFeature(market, targetTicker, anchor);
  appendFeature(values, names, `${targetTicker}_current_week_return`, weeklyBar.weekReturn);
  appendFeature(values, names, `${targetTicker}_current_week_range`, weeklyBar.range);
  appendFeature(values, names, `${targetTicker}_current_week_close_location`, weeklyBar.closeLocation);
}

function appendCrossAssetFeatures(values, names, market, anchors, anchorPosition, coreTickers, targetTicker) {
  const anchor = anchors[anchorPosition];
  coreTickers.forEach((ticker) => {
    if (!market.closes.has(ticker)) return;
    CROSS_DAILY_WINDOWS.forEach((window) => {
      appendFeature(values, names, `${ticker}_daily_ret_${window}`, dailyReturn(market, ticker, anchor.index, window));
    });
    CROSS_WEEKLY_WINDOWS.forEach((window) => {
      appendFeature(values, names, `${ticker}_weekly_ret_${window}`, weeklyReturn(market, ticker, anchors, anchorPosition, window));
    });
    appendFeature(values, names, `${ticker}_daily_vol_21`, realizedVolatility(market, ticker, anchor.index, 21));
    appendFeature(values, names, `${ticker}_drawdown_63`, rollingDrawdown(market, ticker, anchor.index, 63));
    const relative4 = weeklyReturn(market, ticker, anchors, anchorPosition, 4)
      - weeklyReturn(market, targetTicker, anchors, anchorPosition, 4);
    appendFeature(values, names, `${ticker}_vs_${targetTicker}_weekly_ret_4`, relative4);
  });
}

function appendRegimeFeatures(values, names, market, anchors, anchorPosition, targetTicker) {
  const anchor = anchors[anchorPosition];
  const qqqSpy = weeklyReturn(market, 'QQQ', anchors, anchorPosition, 4)
    - weeklyReturn(market, targetTicker, anchors, anchorPosition, 4);
  const iwmSpy = weeklyReturn(market, 'IWM', anchors, anchorPosition, 4)
    - weeklyReturn(market, targetTicker, anchors, anchorPosition, 4);
  const tltSpy = weeklyReturn(market, 'TLT', anchors, anchorPosition, 4)
    - weeklyReturn(market, targetTicker, anchors, anchorPosition, 4);
  appendFeature(values, names, 'regime_qqq_vs_spy_4w', qqqSpy);
  appendFeature(values, names, 'regime_iwm_vs_spy_4w', iwmSpy);
  appendFeature(values, names, 'regime_tlt_vs_spy_4w', tltSpy);
  appendFeature(values, names, 'regime_vixy_ret_1w', weeklyReturn(market, 'VIXY', anchors, anchorPosition, 1));
  appendFeature(values, names, 'regime_vixy_ret_4w', weeklyReturn(market, 'VIXY', anchors, anchorPosition, 4));
  appendFeature(values, names, 'regime_uvxy_ret_1w', weeklyReturn(market, 'UVXY', anchors, anchorPosition, 1));
  appendFeature(values, names, 'regime_gold_vs_spy_4w', weeklyReturn(market, 'GLD', anchors, anchorPosition, 4)
    - weeklyReturn(market, targetTicker, anchors, anchorPosition, 4));
  appendFeature(values, names, 'regime_dollar_ret_4w', weeklyReturn(market, 'UUP', anchors, anchorPosition, 4));
  appendFeature(values, names, 'regime_spy_vol_21', realizedVolatility(market, targetTicker, anchor.index, 21));
  appendFeature(values, names, 'regime_spy_drawdown_126', rollingDrawdown(market, targetTicker, anchor.index, 126));
}

function appendCalendarFeatures(values, names, date) {
  const value = dateToUtc(date);
  const month = value.getUTCMonth();
  const start = Date.UTC(value.getUTCFullYear(), 0, 1);
  const dayOfYear = Math.floor((value.getTime() - start) / 86400000) + 1;
  appendFeature(values, names, 'calendar_month_sin', Math.sin(2 * Math.PI * month / 12));
  appendFeature(values, names, 'calendar_month_cos', Math.cos(2 * Math.PI * month / 12));
  appendFeature(values, names, 'calendar_year_sin', Math.sin(2 * Math.PI * dayOfYear / 366));
  appendFeature(values, names, 'calendar_year_cos', Math.cos(2 * Math.PI * dayOfYear / 366));
}

function buildFeatureGroups({ market, anchors, anchorPosition, targetTicker, coreTickers, includeNames = false }) {
  const out = {};
  const names = {};
  const makeGroup = (name, writer) => {
    const values = [];
    const groupNames = includeNames ? [] : null;
    writer(values, groupNames);
    out[name] = values;
    if (includeNames) names[name] = groupNames;
  };
  makeGroup('target', (values, groupNames) => appendTargetFeatures(values, groupNames, market, anchors, anchorPosition, targetTicker));
  makeGroup('cross', (values, groupNames) => appendCrossAssetFeatures(values, groupNames, market, anchors, anchorPosition, coreTickers, targetTicker));
  makeGroup('regime', (values, groupNames) => appendRegimeFeatures(values, groupNames, market, anchors, anchorPosition, targetTicker));
  makeGroup('calendar', (values, groupNames) => appendCalendarFeatures(values, groupNames, anchors[anchorPosition].date));
  return includeNames ? { groups: out, names } : out;
}

function concatFeatureGroups(sample, featureGroups) {
  return featureGroups.flatMap((group) => sample.featureGroups[group] || []);
}

function sampleTargetReturn(sample, targetMode) {
  if (targetMode === 'next_open_to_week_close') return sample.nextOpenToWeekCloseReturn;
  if (targetMode === 'close_to_close') return sample.closeToCloseReturn;
  throw new Error(`unknown target mode: ${targetMode}`);
}

function sampleSafeReturn(sample, targetMode) {
  if (targetMode === 'next_open_to_week_close') return sample.safeNextOpenToWeekCloseReturn;
  if (targetMode === 'close_to_close') return sample.safeCloseToCloseReturn;
  throw new Error(`unknown target mode: ${targetMode}`);
}

function buildWeeklySamples({
  market,
  targetTicker = DEFAULT_TARGET_TICKER,
  safeTicker = DEFAULT_SAFE_TICKER,
  coreTickers = DEFAULT_CORE_TICKERS,
  minLookbackDays = DEFAULT_MIN_LOOKBACK_DAYS,
} = {}) {
  if (!market?.dates?.length) throw new Error('missing market dates');
  if (!market.closes.has(targetTicker)) throw new Error(`missing target ticker ${targetTicker}`);
  const availableCoreTickers = coreTickers.filter((ticker) => market.closes.has(ticker));
  const anchors = buildWeeklyAnchors(market);
  const samples = [];
  const latestAnchorPosition = anchors.length - 1;
  const featurePreview = buildFeatureGroups({
    market,
    anchors,
    anchorPosition: Math.min(Math.max(DEFAULT_MIN_ANCHOR_WEEKS, 0), latestAnchorPosition),
    targetTicker,
    coreTickers: availableCoreTickers,
    includeNames: true,
  }).names;

  for (let anchorPosition = 0; anchorPosition < anchors.length - 1; anchorPosition += 1) {
    const anchor = anchors[anchorPosition];
    const nextAnchor = anchors[anchorPosition + 1];
    if (anchor.index < minLookbackDays || anchorPosition < DEFAULT_MIN_ANCHOR_WEEKS) continue;
    const nextOpenIndex = anchor.index + 1;
    if (nextOpenIndex > nextAnchor.index) continue;
    const signalClose = closeAt(market, targetTicker, anchor.index);
    const nextClose = closeAt(market, targetTicker, nextAnchor.index);
    const nextOpen = openAt(market, targetTicker, nextOpenIndex);
    if (!Number.isFinite(signalClose) || !Number.isFinite(nextClose) || !Number.isFinite(nextOpen) || signalClose <= 0 || nextOpen <= 0) continue;
    const safeSignalClose = closeAt(market, safeTicker, anchor.index);
    const safeNextClose = closeAt(market, safeTicker, nextAnchor.index);
    const safeNextOpen = openAt(market, safeTicker, nextOpenIndex);
    samples.push({
      date: anchor.date,
      weekKey: anchor.weekKey,
      index: anchor.index,
      nextWeekDate: nextAnchor.date,
      executionDate: market.dates[nextOpenIndex],
      nextOpenIndex,
      nextWeekIndex: nextAnchor.index,
      targetTicker,
      safeTicker,
      signalClose,
      nextOpen,
      nextWeekClose: nextClose,
      closeToCloseReturn: (nextClose / signalClose) - 1,
      nextOpenToWeekCloseReturn: (nextClose / nextOpen) - 1,
      safeCloseToCloseReturn: Number.isFinite(safeSignalClose) && Number.isFinite(safeNextClose) && safeSignalClose > 0
        ? (safeNextClose / safeSignalClose) - 1
        : 0,
      safeNextOpenToWeekCloseReturn: Number.isFinite(safeNextOpen) && Number.isFinite(safeNextClose) && safeNextOpen > 0
        ? (safeNextClose / safeNextOpen) - 1
        : 0,
      featureGroups: buildFeatureGroups({
        market,
        anchors,
        anchorPosition,
        targetTicker,
        coreTickers: availableCoreTickers,
      }),
    });
  }

  const latestAnchor = anchors.at(-1);
  const latestPredictionSample = latestAnchor && latestAnchor.index >= minLookbackDays ? {
    date: latestAnchor.date,
    weekKey: latestAnchor.weekKey,
    index: latestAnchor.index,
    targetTicker,
    safeTicker,
    signalClose: closeAt(market, targetTicker, latestAnchor.index),
    expectedNextFriday: nextCalendarFriday(latestAnchor.date),
    featureGroups: buildFeatureGroups({
      market,
      anchors,
      anchorPosition: latestAnchorPosition,
      targetTicker,
      coreTickers: availableCoreTickers,
    }),
  } : null;

  return {
    anchors,
    samples,
    latestPredictionSample,
    metadata: {
      targetTicker,
      safeTicker,
      coreTickers: availableCoreTickers,
      featureNames: featurePreview,
      marketStartDate: market.dates[0],
      marketEndDate: market.dates.at(-1),
      weeklyAnchorCount: anchors.length,
    },
  };
}

function standardize(trainX, predictX) {
  const columns = trainX[0]?.length || 0;
  const means = Array(columns).fill(0);
  const stds = Array(columns).fill(1);
  for (let column = 0; column < columns; column += 1) {
    const values = trainX.map((row) => row[column]);
    means[column] = mean(values);
    stds[column] = standardDeviation(values) || 1;
  }
  const transform = (rows) => rows.map((row) => row.map((value, column) => (finite(value) - means[column]) / stds[column]));
  return { trainX: transform(trainX), predictX: transform(predictX), means, stds };
}

function fitRidgeReturnModel(trainSamples, predictSample, spec, targetMode) {
  const rawTrainX = trainSamples.map((sample) => concatFeatureGroups(sample, spec.featureGroups));
  const rawPredictX = [concatFeatureGroups(predictSample, spec.featureGroups)];
  const { trainX, predictX } = standardize(rawTrainX, rawPredictX);
  const y = trainSamples.map((sample) => [sampleTargetReturn(sample, targetMode)]);
  const beta = fitRidgeMulti(trainX, y, spec.lambda);
  return predictLinearMulti(beta, predictX[0])[0];
}

function featureValue(sample, metadata, group, name) {
  const names = metadata.featureNames[group] || [];
  const index = names.indexOf(name);
  if (index === -1) return 0;
  return finite(sample.featureGroups[group]?.[index]);
}

function rulePrediction(sample, spec, metadata) {
  if (spec.id === 'rule_13w_momentum') {
    return featureValue(sample, metadata, 'target', `${sample.targetTicker}_weekly_ret_13`);
  }
  if (spec.id === 'rule_sma_200') {
    return featureValue(sample, metadata, 'target', `${sample.targetTicker}_ma_dist_200`);
  }
  if (spec.id === 'rule_risk_on_trend') {
    const trend = featureValue(sample, metadata, 'target', `${sample.targetTicker}_ma_dist_100`);
    const volShock = featureValue(sample, metadata, 'regime', 'regime_vixy_ret_1w');
    return trend - Math.max(0, volShock) * 0.25;
  }
  return 0;
}

function exposureFromPrediction(predictedReturn, spec) {
  if (spec.exposureMode === 'scaled') {
    return clamp((predictedReturn - (spec.threshold || 0)) / (spec.scale || 0.02), 0, spec.maxExposure || 1);
  }
  return predictedReturn > (spec.threshold || 0) ? (spec.maxExposure || 1) : 0;
}

function makeStrategySpecs() {
  const modelSpecs = [
    { id: 'ridge_target_l10_t0', kind: 'ridge', featureGroups: ['target', 'calendar'], lambda: 10, threshold: 0 },
    { id: 'ridge_target_l100_t15bps', kind: 'ridge', featureGroups: ['target', 'calendar'], lambda: 100, threshold: 0.0015 },
    { id: 'ridge_target_regime_l100_t0', kind: 'ridge', featureGroups: ['target', 'regime', 'calendar'], lambda: 100, threshold: 0 },
    { id: 'ridge_target_regime_l250_t15bps', kind: 'ridge', featureGroups: ['target', 'regime', 'calendar'], lambda: 250, threshold: 0.0015 },
    { id: 'ridge_cross_l500_t15bps', kind: 'ridge', featureGroups: ['target', 'cross', 'regime', 'calendar'], lambda: 500, threshold: 0.0015 },
    { id: 'ridge_cross_l1000_t30bps', kind: 'ridge', featureGroups: ['target', 'cross', 'regime', 'calendar'], lambda: 1000, threshold: 0.003 },
    { id: 'ridge_scaled_cross_l750', kind: 'ridge', featureGroups: ['target', 'cross', 'regime', 'calendar'], lambda: 750, threshold: -0.001, exposureMode: 'scaled', scale: 0.018 },
  ];
  const ruleSpecs = [
    { id: 'rule_13w_momentum', kind: 'rule', threshold: 0 },
    { id: 'rule_sma_200', kind: 'rule', threshold: 0 },
    { id: 'rule_risk_on_trend', kind: 'rule', threshold: 0 },
  ];
  return [...modelSpecs, ...ruleSpecs];
}

function weightsForExposure(exposure, targetTicker, safeTicker) {
  const targetWeight = clamp(exposure);
  const safeWeight = 1 - targetWeight;
  const weights = {};
  if (targetWeight > 1e-10) weights[targetTicker] = targetWeight;
  if (safeWeight > 1e-10) weights[safeTicker] = safeWeight;
  return weights;
}

function turnover(previousWeights, nextWeights) {
  const tickers = new Set([...Object.keys(previousWeights || {}), ...Object.keys(nextWeights || {})]);
  let out = 0;
  tickers.forEach((ticker) => {
    out += Math.abs((nextWeights[ticker] || 0) - (previousWeights[ticker] || 0));
  });
  return out;
}

function maxDrawdown(equityCurve) {
  let peak = equityCurve[0]?.equity || DEFAULT_INITIAL_CAPITAL;
  let drawdown = 0;
  equityCurve.forEach((point) => {
    if (point.equity > peak) peak = point.equity;
    if (peak > 0) drawdown = Math.min(drawdown, (point.equity / peak) - 1);
  });
  return drawdown;
}

function monthlyReturns(equityCurve) {
  const months = new Map();
  equityCurve.forEach((point) => {
    const month = point.date.slice(0, 7);
    if (!months.has(month)) months.set(month, { startEquity: point.startEquity, endEquity: point.equity });
    months.get(month).endEquity = point.equity;
  });
  return [...months.entries()].map(([month, value]) => {
    const ret = value.startEquity > 0 ? (value.endEquity / value.startEquity) - 1 : 0;
    return { month, return: ret, returnPct: pct(ret) };
  });
}

function summarizePoints(points, initialCapital = DEFAULT_INITIAL_CAPITAL) {
  const returns = points.map((point) => point.netReturn);
  const finalEquity = points.at(-1)?.equity || initialCapital;
  const totalReturn = initialCapital > 0 ? (finalEquity / initialCapital) - 1 : 0;
  const weeklyVol = standardDeviation(returns) * Math.sqrt(52);
  const avgWeekly = mean(returns);
  const cagr = points.length && totalReturn > -1 ? ((1 + totalReturn) ** (52 / points.length)) - 1 : 0;
  const drawdown = maxDrawdown(points);
  const wins = returns.filter((value) => value > 0).length;
  return {
    startSignalDate: points[0]?.signalDate || null,
    endDate: points.at(-1)?.date || null,
    weeks: points.length,
    finalEquity,
    totalReturn,
    totalReturnPct: pct(totalReturn),
    cagr,
    cagrPct: pct(cagr),
    maxDrawdown: drawdown,
    maxDrawdownPct: pct(drawdown),
    annualizedVolatility: weeklyVol,
    annualizedVolatilityPct: pct(weeklyVol),
    sharpe: weeklyVol > 0 ? (avgWeekly * 52) / weeklyVol : 0,
    winRate: points.length ? wins / points.length : 0,
    winRatePct: points.length ? pct(wins / points.length) : null,
    averageWeeklyTurnover: mean(points.map((point) => point.turnover)),
    averageWeeklyTurnoverPct: pct(mean(points.map((point) => point.turnover))),
    averageExposure: mean(points.map((point) => point.exposure)),
    averageExposurePct: pct(mean(points.map((point) => point.exposure))),
    monthlyReturns: monthlyReturns(points),
    equityCurve: points,
  };
}

function predictiveMetrics(points) {
  const scored = points.filter((point) => Number.isFinite(point.predictedReturn));
  const predictions = scored.map((point) => point.predictedReturn);
  const actuals = scored.map((point) => point.targetReturn);
  const errors = scored.map((point) => point.predictedReturn - point.targetReturn);
  const correctSigns = scored.filter((point) => (
    (point.predictedReturn >= 0 && point.targetReturn >= 0)
    || (point.predictedReturn < 0 && point.targetReturn < 0)
  )).length;
  const longDecisions = scored.filter((point) => point.exposure > 0.001);
  const profitableLongs = longDecisions.filter((point) => point.targetReturn > point.safeReturn).length;
  return {
    predictions: scored.length,
    rmse: errors.length ? Math.sqrt(mean(errors.map((error) => error ** 2))) : 0,
    rmsePct: pct(errors.length ? Math.sqrt(mean(errors.map((error) => error ** 2))) : 0),
    mae: errors.length ? mean(errors.map((error) => Math.abs(error))) : 0,
    maePct: pct(errors.length ? mean(errors.map((error) => Math.abs(error))) : 0),
    correlation: correlation(predictions, actuals),
    signAccuracy: scored.length ? correctSigns / scored.length : 0,
    signAccuracyPct: scored.length ? pct(correctSigns / scored.length) : null,
    longDecisionCount: longDecisions.length,
    longDecisionHitRate: longDecisions.length ? profitableLongs / longDecisions.length : 0,
    longDecisionHitRatePct: longDecisions.length ? pct(profitableLongs / longDecisions.length) : null,
    averagePredictedReturn: mean(predictions),
    averagePredictedReturnPct: pct(mean(predictions)),
    averageActualReturn: mean(actuals),
    averageActualReturnPct: pct(mean(actuals)),
  };
}

function scoreStrategy(summary) {
  return (summary.sharpe || 0) + (summary.totalReturn || 0) * 2 + (summary.maxDrawdown || 0) * 0.75;
}

function buildPoint({ sample, targetMode, exposure, predictedReturn, previousWeights, equity, targetTicker, safeTicker, costBps }) {
  const targetReturn = sampleTargetReturn(sample, targetMode);
  const safeReturn = sampleSafeReturn(sample, targetMode);
  const weights = weightsForExposure(exposure, targetTicker, safeTicker);
  const weekTurnover = turnover(previousWeights, weights);
  const grossReturn = exposure * targetReturn + (1 - exposure) * safeReturn;
  const costReturn = weekTurnover * costBps / 10000;
  const netReturn = grossReturn - costReturn;
  const startEquity = equity;
  const endEquity = equity * (1 + netReturn);
  return {
    date: sample.nextWeekDate,
    signalDate: sample.date,
    executionDate: targetMode === 'next_open_to_week_close' ? sample.executionDate : sample.date,
    startEquity,
    equity: endEquity,
    targetReturn,
    targetReturnPct: pct(targetReturn),
    safeReturn,
    safeReturnPct: pct(safeReturn),
    predictedReturn,
    predictedReturnPct: pct(predictedReturn),
    grossReturn,
    grossReturnPct: pct(grossReturn),
    costReturn,
    costReturnPct: pct(costReturn),
    netReturn,
    netReturnPct: pct(netReturn),
    turnover: weekTurnover,
    exposure,
    holdings: weights,
    signalClose: sample.signalClose,
    nextOpen: sample.nextOpen,
    nextWeekClose: sample.nextWeekClose,
  };
}

function runStrategyWalkForward({ spec, samples, trainingPool = samples, metadata, targetMode, trainStart, minTrainWeeks, initialCapital, costBps }) {
  let equity = initialCapital;
  let previousWeights = {};
  const points = [];
  const skipped = [];

  samples.forEach((sample) => {
    const trainSamples = trainingPool.filter((row) => row.date < sample.date && (!trainStart || row.date >= trainStart));
    if (spec.kind === 'ridge' && trainSamples.length < minTrainWeeks) {
      skipped.push(sample.date);
      return;
    }
    const predictedReturn = spec.kind === 'ridge'
      ? fitRidgeReturnModel(trainSamples, sample, spec, targetMode)
      : rulePrediction(sample, spec, metadata);
    const exposure = exposureFromPrediction(predictedReturn, spec);
    const point = buildPoint({
      sample,
      targetMode,
      exposure,
      predictedReturn,
      previousWeights,
      equity,
      targetTicker: metadata.targetTicker,
      safeTicker: metadata.safeTicker,
      costBps,
    });
    equity = point.equity;
    previousWeights = point.holdings;
    points.push(point);
  });

  return {
    id: spec.id,
    spec,
    skippedSignals: skipped,
    summary: summarizePoints(points, initialCapital),
    predictive: predictiveMetrics(points),
  };
}

function runFixedExposureBenchmark({ id, exposure, samples, metadata, targetMode, initialCapital, costBps }) {
  let equity = initialCapital;
  let previousWeights = {};
  const points = samples.map((sample) => {
    const point = buildPoint({
      sample,
      targetMode,
      exposure,
      predictedReturn: null,
      previousWeights,
      equity,
      targetTicker: metadata.targetTicker,
      safeTicker: metadata.safeTicker,
      costBps,
    });
    equity = point.equity;
    previousWeights = point.holdings;
    return point;
  });
  return {
    id,
    spec: { id, kind: 'benchmark', exposure },
    skippedSignals: [],
    summary: summarizePoints(points, initialCapital),
    predictive: predictiveMetrics(points),
  };
}

function filterByDate(samples, startDate, endDate = null) {
  return samples.filter((sample) => sample.date >= startDate && (!endDate || sample.date <= endDate));
}

function compactSummary(report) {
  return {
    id: report.id,
    weeks: report.summary.weeks,
    totalReturnPct: report.summary.totalReturnPct,
    cagrPct: report.summary.cagrPct,
    sharpe: report.summary.sharpe,
    maxDrawdownPct: report.summary.maxDrawdownPct,
    averageExposurePct: report.summary.averageExposurePct,
    signAccuracyPct: report.predictive.signAccuracyPct,
    correlation: report.predictive.correlation,
    score: scoreStrategy(report.summary),
  };
}

function rebasePoints(points, initialCapital) {
  let equity = initialCapital;
  return points.map((point) => {
    const startEquity = equity;
    equity *= (1 + point.netReturn);
    return {
      ...point,
      startEquity,
      equity,
    };
  });
}

function summarizeExistingWindow(strategy, startDate, endDate, initialCapital) {
  const points = strategy.summary.equityCurve.filter((point) => (
    point.signalDate >= startDate && (!endDate || point.signalDate <= endDate)
  ));
  const rebased = rebasePoints(points, initialCapital);
  return {
    id: strategy.id,
    spec: strategy.spec,
    skippedSignals: strategy.skippedSignals.filter((date) => date >= startDate && (!endDate || date <= endDate)),
    summary: summarizePoints(rebased, initialCapital),
    predictive: predictiveMetrics(rebased),
  };
}

function latestPredictionForSpec({ spec, labeledSamples, latestPredictionSample, metadata, targetMode, minTrainWeeks }) {
  if (!latestPredictionSample) return null;
  const closeProjection = (predictedReturn) => (targetMode === 'close_to_close'
    ? latestPredictionSample.signalClose * (1 + predictedReturn)
    : null);
  const projectionNote = targetMode === 'close_to_close'
    ? 'Projected next Friday close is signal close times predicted close-to-close return.'
    : 'Target is next-session-open to next-Friday-close, so next Friday price cannot be projected until the next open is known.';
  if (spec.kind === 'ridge') {
    const trainSamples = labeledSamples.filter((sample) => sample.date < latestPredictionSample.date);
    if (trainSamples.length < minTrainWeeks) return null;
    const predictedReturn = fitRidgeReturnModel(trainSamples, latestPredictionSample, spec, targetMode);
    return {
      strategyId: spec.id,
      signalDate: latestPredictionSample.date,
      expectedNextFriday: latestPredictionSample.expectedNextFriday,
      targetMode,
      signalClose: latestPredictionSample.signalClose,
      predictedReturn,
      predictedReturnPct: pct(predictedReturn),
      predictedNextFridayCloseFromSignalClose: closeProjection(predictedReturn),
      projectionNote,
      exposure: exposureFromPrediction(predictedReturn, spec),
      holdings: weightsForExposure(exposureFromPrediction(predictedReturn, spec), metadata.targetTicker, metadata.safeTicker),
      trainedOnWeeks: trainSamples.length,
    };
  }
  const predictedReturn = rulePrediction(latestPredictionSample, spec, metadata);
  return {
    strategyId: spec.id,
    signalDate: latestPredictionSample.date,
    expectedNextFriday: latestPredictionSample.expectedNextFriday,
    targetMode,
    signalClose: latestPredictionSample.signalClose,
    predictedReturn,
    predictedReturnPct: pct(predictedReturn),
    predictedNextFridayCloseFromSignalClose: closeProjection(predictedReturn),
    projectionNote,
    exposure: exposureFromPrediction(predictedReturn, spec),
    holdings: weightsForExposure(exposureFromPrediction(predictedReturn, spec), metadata.targetTicker, metadata.safeTicker),
    trainedOnWeeks: null,
  };
}

function runWeeklyReturnExperiment(options = {}) {
  const dailyBarsPath = options.dailyBarsPath || findPreferredMassiveEodBarsPath();
  if (!dailyBarsPath || !fs.existsSync(dailyBarsPath)) throw new Error('Missing Massive adjusted EOD daily bars.');
  const market = readDailyBarsJsonl(dailyBarsPath);
  const targetTicker = options.targetTicker || DEFAULT_TARGET_TICKER;
  const safeTicker = options.safeTicker || (market.closes.has(DEFAULT_SAFE_TICKER) ? DEFAULT_SAFE_TICKER : targetTicker);
  const targetMode = options.targetMode || DEFAULT_TARGET_MODE;
  const predictStart = options.predictStart || DEFAULT_PREDICT_START;
  const validationStart = options.validationStart || DEFAULT_VALIDATION_START;
  const validationEnd = options.validationEnd || DEFAULT_VALIDATION_END;
  const testStart = options.testStart || DEFAULT_TEST_START;
  const testEnd = options.testEnd || null;
  const trainStart = options.trainStart || null;
  const minTrainWeeks = options.minTrainWeeks || DEFAULT_MIN_TRAIN_WEEKS;
  const initialCapital = options.initialCapital || DEFAULT_INITIAL_CAPITAL;
  const costBps = options.costBps ?? DEFAULT_COST_BPS;

  const { samples, latestPredictionSample, metadata } = buildWeeklySamples({
    market,
    targetTicker,
    safeTicker,
    coreTickers: options.coreTickers || DEFAULT_CORE_TICKERS,
    minLookbackDays: options.minLookbackDays || DEFAULT_MIN_LOOKBACK_DAYS,
  });
  const predictionSamples = filterByDate(samples, predictStart, testEnd);
  if (!predictionSamples.length) throw new Error('No weekly prediction samples in requested range.');
  const specs = makeStrategySpecs().filter((spec) => !options.strategyIds || options.strategyIds.includes(spec.id));
  const strategies = [
    runFixedExposureBenchmark({
      id: `${targetTicker.toLowerCase()}_buy_hold`,
      exposure: 1,
      samples: predictionSamples,
      metadata,
      targetMode,
      initialCapital,
      costBps,
    }),
    runFixedExposureBenchmark({
      id: `${safeTicker.toLowerCase()}_cash_proxy`,
      exposure: 0,
      samples: predictionSamples,
      metadata,
      targetMode,
      initialCapital,
      costBps,
    }),
    ...specs.map((spec) => runStrategyWalkForward({
      spec,
      samples: predictionSamples,
      trainingPool: samples,
      metadata,
      targetMode,
      trainStart,
      minTrainWeeks,
      initialCapital,
      costBps,
    })),
  ];

  const validationSamples = filterByDate(predictionSamples, validationStart, validationEnd);
  const testSamples = filterByDate(predictionSamples, testStart, testEnd);
  const validationReports = Object.fromEntries(strategies.map((strategy) => [
    strategy.id,
    validationSamples.length
      ? summarizeExistingWindow(strategy, validationStart, validationEnd, initialCapital)
      : null,
  ]));
  const testReports = Object.fromEntries(strategies.map((strategy) => [
    strategy.id,
    testSamples.length
      ? summarizeExistingWindow(strategy, testStart, testEnd, initialCapital)
      : null,
  ]));

  const modelIds = new Set(specs.filter((spec) => spec.kind === 'ridge').map((spec) => spec.id));
  const candidateIds = new Set(specs.map((spec) => spec.id));
  const validationRanking = strategies
    .filter((strategy) => validationReports[strategy.id]?.summary?.weeks)
    .map((strategy) => ({ id: strategy.id, ...compactSummary(validationReports[strategy.id]) }))
    .sort((left, right) => right.score - left.score || right.totalReturnPct - left.totalReturnPct);
  const validationCandidateRanking = validationRanking.filter((row) => candidateIds.has(row.id));
  const modelValidationRanking = validationRanking.filter((row) => modelIds.has(row.id));
  const selectedOverallId = validationCandidateRanking[0]?.id || validationRanking[0]?.id || strategies[0].id;
  const selectedModelId = modelValidationRanking[0]?.id || [...modelIds][0] || null;
  const latestPredictions = Object.fromEntries(specs.map((spec) => [
    spec.id,
    latestPredictionForSpec({
      spec,
      labeledSamples: samples,
      latestPredictionSample,
      metadata,
      targetMode,
      minTrainWeeks,
    }),
  ]));

  return {
    generatedAt: new Date().toISOString(),
    source: {
      dailyBarsPath,
      provider: 'Massive adjusted EOD aggregate bars',
    },
    settings: {
      targetTicker,
      safeTicker,
      targetMode,
      timing: targetMode === 'next_open_to_week_close'
        ? 'features_at_week_close_action_next_session_open_exit_next_week_close'
        : 'features_at_week_close_research_close_to_close_weekly_return',
      predictStart,
      trainStart,
      minTrainWeeks,
      validationStart,
      validationEnd,
      testStart,
      testEnd,
      initialCapital,
      costBps,
    },
    data: {
      marketStartDate: metadata.marketStartDate,
      marketEndDate: metadata.marketEndDate,
      weeklyAnchorCount: metadata.weeklyAnchorCount,
      labeledWeeklySamples: samples.length,
      predictionWeeks: predictionSamples.length,
      firstPredictionSignalDate: predictionSamples[0]?.date || null,
      lastPredictionSignalDate: predictionSamples.at(-1)?.date || null,
      validationWeeks: validationSamples.length,
      testWeeks: testSamples.length,
      targetTicker,
      safeTicker,
      coreTickers: metadata.coreTickers,
      featureGroupSizes: Object.fromEntries(Object.entries(metadata.featureNames).map(([group, names]) => [group, names.length])),
    },
    selected: {
      overallByValidation: selectedOverallId,
      modelByValidation: selectedModelId,
      latestOverall: latestPredictions[selectedOverallId] || null,
      latestModel: selectedModelId ? latestPredictions[selectedModelId] : null,
      selectionNote: 'Selected ids are chosen by validation-period score only; test-period rows are kept separate.',
    },
    rankings: {
      validation: validationRanking,
      test: strategies
        .filter((strategy) => testReports[strategy.id]?.summary?.weeks)
        .map((strategy) => ({ id: strategy.id, ...compactSummary(testReports[strategy.id]) }))
        .sort((left, right) => right.score - left.score || right.totalReturnPct - left.totalReturnPct),
      fullPeriod: strategies
        .map((strategy) => ({ id: strategy.id, ...compactSummary(strategy) }))
        .sort((left, right) => right.score - left.score || right.totalReturnPct - left.totalReturnPct),
    },
    strategies: Object.fromEntries(strategies.map((strategy) => [strategy.id, strategy])),
    validation: validationReports,
    test: testReports,
    latestPredictions,
  };
}

function writeWeeklyReport(report, outPath = null) {
  const target = outPath || artifactPath(`weekly-${report.settings.targetTicker.toLowerCase()}-${report.settings.targetMode}-${report.settings.predictStart}-${report.data.marketEndDate}.json`);
  ensureDir(path.dirname(target));
  fs.writeFileSync(target, JSON.stringify(report, null, 2));
  return target;
}

module.exports = {
  DEFAULT_CORE_TICKERS,
  buildWeeklyAnchors,
  buildWeeklySamples,
  runWeeklyReturnExperiment,
  writeWeeklyReport,
  sampleTargetReturn,
  sampleSafeReturn,
  makeStrategySpecs,
  nextCalendarFriday,
  findPreferredMassiveEodBarsPath,
};
