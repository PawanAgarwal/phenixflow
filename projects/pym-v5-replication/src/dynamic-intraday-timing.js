const fs = require('node:fs');
const path = require('node:path');

const { openCalendarDays } = require('./calendar');
const { loadConfig, runtimePath } = require('./config');
const { readMinuteBarsForDay, closeAt } = require('./intraday-minute-data');
const { buildTargetReport } = require('./intraday-suite');
const { readOptionFeatureJsonl } = require('./option-features');
const { tickerFeatures } = require('./multitimeframe-prediction');

const ENTRY_START_MINUTE = 675; // 11:15 ET, after 20 completed 5m bars.
const DEFAULT_EXIT_MINUTE = 955; // 15:55 ET.
const DEFAULT_CHECK_INTERVAL = 5;
const SAFE_TICKERS = new Set(['AGG', 'BIL', 'BND', 'BSV', 'IEF', 'IEI', 'IGIB', 'MUB', 'SHV', 'SHY', 'TIP']);

function mean(values) {
  return values.length ? values.reduce((sum, value) => sum + value, 0) / values.length : 0;
}

function standardDeviation(values) {
  if (values.length < 2) return 0;
  const avg = mean(values);
  return Math.sqrt(mean(values.map((value) => (value - avg) ** 2)));
}

function maxDrawdown(equityCurve) {
  let peak = equityCurve[0]?.equity || 1;
  let drawdown = 0;
  equityCurve.forEach((point) => {
    if (point.equity > peak) peak = point.equity;
    if (peak > 0) drawdown = Math.min(drawdown, (point.equity / peak) - 1);
  });
  return drawdown;
}

function cleanWeights(weights) {
  const out = new Map();
  let total = 0;
  weights.forEach((weight, ticker) => {
    if (Number.isFinite(weight) && weight > 1e-10) {
      out.set(ticker, weight);
      total += weight;
    }
  });
  if (total <= 0) return new Map();
  out.forEach((weight, ticker) => out.set(ticker, weight / total));
  return out;
}

function targetWeights(snapshot, limit = 8) {
  const rows = (snapshot?.holdings || [])
    .filter((holding) => !SAFE_TICKERS.has(holding.ticker))
    .sort((left, right) => right.weight - left.weight || left.ticker.localeCompare(right.ticker));
  return cleanWeights(new Map((limit ? rows.slice(0, limit) : rows).map((holding) => [holding.ticker, holding.weight])));
}

function targetTickers(snapshot) {
  return new Set((snapshot?.holdings || []).map((holding) => holding.ticker));
}

function weightTurnover(previous, next) {
  const tickers = new Set([...previous.keys(), ...next.keys()]);
  let turnover = 0;
  tickers.forEach((ticker) => {
    turnover += Math.abs((next.get(ticker) || 0) - (previous.get(ticker) || 0));
  });
  return turnover;
}

function countTrades(previous, next) {
  const tickers = new Set([...previous.keys(), ...next.keys()]);
  let trades = 0;
  tickers.forEach((ticker) => {
    if (Math.abs((next.get(ticker) || 0) - (previous.get(ticker) || 0)) > 1e-6) trades += 1;
  });
  return trades;
}

function intradayReturn(dayBars, ticker, previousMinute, minute) {
  const previous = closeAt(dayBars, ticker, previousMinute);
  const current = closeAt(dayBars, ticker, minute);
  if (!Number.isFinite(previous) || !Number.isFinite(current) || previous <= 0) return null;
  return (current / previous) - 1;
}

function applyMinuteReturn(state, dayBars, previousMinute, minute) {
  let grossReturn = 0;
  state.weights.forEach((weight, ticker) => {
    const ret = intradayReturn(dayBars, ticker, previousMinute, minute);
    if (ret === null) {
      state.missingReturnEvents += 1;
      return;
    }
    grossReturn += weight * ret;
  });
  state.equity *= (1 + grossReturn);
  return grossReturn;
}

function featureScore(feature) {
  const parts = [
    Number.isFinite(feature.rsi1m20) ? (feature.rsi1m20 - 50) / 25 : null,
    Number.isFinite(feature.rsi5m20) ? (feature.rsi5m20 - 50) / 25 : null,
    Number.isFinite(feature.ret1m20) ? feature.ret1m20 * 100 : null,
    Number.isFinite(feature.ret5m20) ? feature.ret5m20 * 40 : null,
    feature.aboveVwap ? 0.25 : -0.25,
  ].filter(Number.isFinite);
  return parts.length ? mean(parts) : null;
}

function clamp(value, min, max) {
  if (!Number.isFinite(value)) return null;
  return Math.min(max, Math.max(min, value));
}

function stockVolumeScore(feature) {
  const parts = [
    Number.isFinite(feature.volumeRatio20) ? clamp(Math.log(feature.volumeRatio20), -1.25, 1.25) : null,
    Number.isFinite(feature.dollarVolumeRatio20) ? clamp(Math.log(feature.dollarVolumeRatio20), -1.25, 1.25) : null,
    Number.isFinite(feature.signedVolumeImbalance20) ? feature.signedVolumeImbalance20 * 1.5 : null,
    Number.isFinite(feature.volumeWeightedReturn20) ? feature.volumeWeightedReturn20 * 300 : null,
  ].filter(Number.isFinite);
  return parts.length ? mean(parts) : null;
}

function stockVolumeConfirm(feature) {
  const burst = (
    feature.volumeRatio20 >= 1.08
    || feature.dollarVolumeRatio20 >= 1.08
  );
  return (
    burst
    && feature.signedVolumeImbalance20 >= -0.05
    && feature.volumeWeightedReturn20 >= -0.0004
  );
}

function stockVolumeVetoPass(feature) {
  return (
    (!Number.isFinite(feature.volumeRatio20) || feature.volumeRatio20 >= 0.35)
    && (!Number.isFinite(feature.signedVolumeImbalance20) || feature.signedVolumeImbalance20 >= -0.35)
    && (!Number.isFinite(feature.volumeWeightedReturn20) || feature.volumeWeightedReturn20 >= -0.0015)
  );
}

function optionRootFeature(dayState, ticker) {
  return dayState.priorOptionRow?.roots?.[ticker] || null;
}

function optionVolumeScore(feature, dayState) {
  const root = optionRootFeature(dayState, feature.ticker);
  if (!root) return 0;
  const rolling = root.rolling || {};
  const callMomentum = rolling.callPremiumMomentum5 || 0;
  const putMomentum = rolling.putPremiumMomentum5 || 0;
  const parts = [
    Number.isFinite(root.premiumImbalance) ? root.premiumImbalance : null,
    Number.isFinite(root.volumeImbalance) ? root.volumeImbalance * 0.5 : null,
    Number.isFinite(rolling.premiumImbalanceZ20) ? clamp(rolling.premiumImbalanceZ20 / 2, -1.5, 1.5) : null,
    Number.isFinite(rolling.volumeImbalanceZ20) ? clamp(rolling.volumeImbalanceZ20 / 2, -1.5, 1.5) : null,
    Number.isFinite(rolling.totalPremiumLogZ20) ? clamp(rolling.totalPremiumLogZ20 / 3, -1, 1) : null,
    clamp((callMomentum - putMomentum) / 3, -1, 1),
  ].filter(Number.isFinite);
  return parts.length ? mean(parts) : 0;
}

function optionVolumeConfirm(feature, dayState) {
  const root = optionRootFeature(dayState, feature.ticker);
  if (!root) return false;
  const rolling = root.rolling || {};
  const callMomentum = rolling.callPremiumMomentum5 || 0;
  const putMomentum = rolling.putPremiumMomentum5 || 0;
  const liquidEnough = root.totalPremium >= 25000 || rolling.totalPremiumLogZ20 >= -0.75;
  const bullishEnough = (
    root.premiumImbalance >= -0.1
    || rolling.premiumImbalanceZ20 >= 0.35
    || (callMomentum - putMomentum) >= 0.25
  );
  const notPutHeavy = (
    !Number.isFinite(rolling.putCallPremiumRatioZ20)
    || rolling.putCallPremiumRatioZ20 <= 1.75
  );
  return liquidEnough && bullishEnough && notPutHeavy;
}

function optionBearishVetoPass(feature, dayState) {
  const root = optionRootFeature(dayState, feature.ticker);
  if (!root) return true;
  const rolling = root.rolling || {};
  const extremePutPressure = (
    root.premiumImbalance <= -0.75
    || rolling.premiumImbalanceZ20 <= -2
    || rolling.putCallPremiumRatioZ20 >= 2.5
    || rolling.putPremiumMomentum5 - rolling.callPremiumMomentum5 >= 2.5
  );
  const thinAndBearish = root.totalPremium < 10000 && root.premiumImbalance < -0.25;
  return !(extremePutPressure || thinAndBearish);
}

function stockOptionVolumeScore(feature, dayState) {
  return (
    (feature.score || 0)
    + ((stockVolumeScore(feature) || 0) * 0.35)
    + (optionVolumeScore(feature, dayState) * 0.3)
  );
}

function rankedFeatures(ctx, limit) {
  const base = targetWeights(ctx.targetSnapshot, limit);
  return [...base.keys()]
    .map((ticker) => ({ ...tickerFeatures(ctx.dayBars, ticker, ctx.minuteEt), baseWeight: base.get(ticker) || 0 }))
    .filter((feature) => Number.isFinite(feature.close) && Number.isFinite(feature.rsi1m20))
    .map((feature) => ({ ...feature, score: featureScore(feature), stockVolumeScore: stockVolumeScore(feature) }))
    .sort((left, right) => (right.score || -Infinity) - (left.score || -Infinity) || right.baseWeight - left.baseWeight);
}

function latestOptionFeaturesPath() {
  const explicit = process.env.PYM_V5_OPTION_FEATURES_PATH;
  if (explicit) return path.resolve(explicit);
  const root = runtimePath();
  if (!fs.existsSync(root)) return null;
  const matches = fs.readdirSync(root)
    .map((name) => {
      const match = name.match(/^pym-v5-option-bar-features-(\d{4}-\d{2}-\d{2})-(\d{4}-\d{2}-\d{2})\.jsonl$/);
      return match ? { name, startDate: match[1], endDate: match[2] } : null;
    })
    .filter(Boolean)
    .sort((left, right) => (
      right.endDate.localeCompare(left.endDate)
      || left.startDate.localeCompare(right.startDate)
      || left.name.localeCompare(right.name)
    ));
  return matches.length ? path.join(root, matches[0].name) : null;
}

function spyPutPressureFromOptionRow(row) {
  const spy = row?.roots?.SPY;
  if (!spy) return null;
  const putCallZ = spy.rolling?.putCallPremiumRatioZ20;
  const imbalanceZ = spy.rolling?.premiumImbalanceZ20;
  return Math.max(
    Number.isFinite(putCallZ) ? putCallZ : 0,
    Number.isFinite(imbalanceZ) ? -imbalanceZ : 0,
  );
}

function optionRowsByDate(optionFeaturesPath) {
  if (!optionFeaturesPath || !fs.existsSync(optionFeaturesPath)) return new Map();
  return new Map(readOptionFeatureJsonl(optionFeaturesPath).map((row) => [row.date, row]));
}

function priorOptionRow(optionByDate, date, dates) {
  const index = dates.indexOf(date);
  if (index <= 0) return null;
  for (let cursor = index - 1; cursor >= 0; cursor -= 1) {
    const row = optionByDate.get(dates[cursor]);
    if (row) return row;
  }
  return null;
}

function hasBaseTrend(feature) {
  return (
    feature.rsi5m20 >= 52
    && feature.ret5m20 > 0
  );
}

function hasStrongConfirmation(feature) {
  return (
    feature.rsi1m20 >= 52
    && feature.rsi5m20 >= 52
    && feature.ret1m20 > 0
    && feature.ret5m20 > 0
    && feature.aboveVwap
  );
}

function reclaimEntry(feature, priorFeature) {
  return (
    hasBaseTrend(feature)
    && feature.aboveVwap
    && feature.rsi1m20 >= 52
    && (!priorFeature || priorFeature.rsi1m20 < 52 || !priorFeature.aboveVwap)
  );
}

function breakoutEntry(feature, dayState) {
  const rangeHigh = dayState.openingRangeHigh.get(feature.ticker);
  return (
    hasBaseTrend(feature)
    && feature.aboveVwap
    && Number.isFinite(rangeHigh)
    && feature.close > rangeHigh * 1.001
  );
}

function pullbackRecoveryEntry(feature, dayState) {
  return (
    dayState.pullbackArmed.has(feature.ticker)
    && hasBaseTrend(feature)
    && feature.rsi1m20 >= 50
    && feature.aboveVwap
  );
}

function rsiVwapExit(feature, position) {
  return (
    feature.rsi1m20 < 45
    || feature.rsi5m20 < 48
    || !feature.aboveVwap
    || ((feature.close / position.entryPrice) - 1) <= -0.012
  );
}

function trailingProfitExit(feature, position) {
  const fromEntry = (feature.close / position.entryPrice) - 1;
  const fromHigh = (feature.close / position.highPrice) - 1;
  return (
    fromEntry <= -0.012
    || fromEntry >= 0.026
    || fromHigh <= -0.009
    || feature.rsi5m20 < 47
  );
}

function softExit(feature, position) {
  const fromEntry = (feature.close / position.entryPrice) - 1;
  const fromHigh = (feature.close / position.highPrice) - 1;
  return (
    fromEntry <= -0.015
    || fromEntry >= 0.035
    || fromHigh <= -0.012
    || (feature.rsi1m20 < 42 && feature.rsi5m20 < 50)
  );
}

function makeStrategy({
  id,
  name,
  description,
  entryMode,
  exitMode,
  targetLimit = 8,
  maxHoldings = 3,
  fixedEntryMinute = null,
  lastEntryMinute = 915,
  checkInterval = DEFAULT_CHECK_INTERVAL,
  singleEntry = false,
  rankMode = 'price',
  optionRiskFilter = false,
  optionRiskThreshold = 2.5,
}) {
  return {
    id,
    name,
    description,
    targetLimit,
    maxHoldings,
    fixedEntryMinute,
    lastEntryMinute,
    checkInterval,
    singleEntry,
    rankMode,
    optionRiskFilter,
    optionRiskThreshold,
    entryMode,
    exitMode,
  };
}

const BASE_DYNAMIC_STRATEGIES = Object.freeze([
  makeStrategy({
    id: 'fixed_1115_confirm_dynamic_rsi_vwap_exit',
    name: 'Fixed 11:15 confirm, dynamic RSI/VWAP exit',
    description: 'Enter once at 11:15 on 1m/5m confirmation, then exit on RSI/VWAP/stop invalidation.',
    entryMode: 'confirm',
    exitMode: 'rsi_vwap',
    fixedEntryMinute: ENTRY_START_MINUTE,
    maxHoldings: 5,
    targetLimit: 5,
  }),
  makeStrategy({
    id: 'fixed_1115_confirm_trailing_exit',
    name: 'Fixed 11:15 confirm, trailing/profit exit',
    description: 'Enter once at 11:15 on confirmation, exit by stop, trailing stop, profit target, or 5m rollover.',
    entryMode: 'confirm',
    exitMode: 'trailing_profit',
    fixedEntryMinute: ENTRY_START_MINUTE,
    maxHoldings: 5,
    targetLimit: 5,
  }),
  makeStrategy({
    id: 'dynamic_reclaim_fixed_exit',
    name: 'Dynamic VWAP/RSI reclaim, fixed 15:55 exit',
    description: 'After 11:15, enter on 1m RSI/VWAP reclaim inside positive 5m trend; exit at 15:55.',
    entryMode: 'reclaim',
    exitMode: 'eod',
    maxHoldings: 3,
    targetLimit: 8,
    checkInterval: 5,
  }),
  makeStrategy({
    id: 'first_confirm_after_1115_fixed_exit',
    name: 'First 1m/5m confirm after 11:15, fixed exit',
    description: 'Check every five minutes after 11:15, take the first confirmed PYM target set, then hold to 15:55.',
    entryMode: 'confirm',
    exitMode: 'eod',
    maxHoldings: 5,
    targetLimit: 5,
    singleEntry: true,
    checkInterval: 5,
  }),
  makeStrategy({
    id: 'first_confirm_stock_volume_after_1115_fixed_exit',
    name: 'First 1m/5m confirm + stock volume after 11:15',
    description: 'Take the first confirmed PYM target set only when 1m relative volume and signed volume support the move.',
    entryMode: 'confirm_stock_volume',
    exitMode: 'eod',
    maxHoldings: 5,
    targetLimit: 5,
    singleEntry: true,
    rankMode: 'stock_volume',
    checkInterval: 5,
  }),
  makeStrategy({
    id: 'first_confirm_stock_volume_veto_after_1115_fixed_exit',
    name: 'First 1m/5m confirm + stock volume veto after 11:15',
    description: 'Takes the first confirmed basket unless same-day stock volume shows weak or bearish participation.',
    entryMode: 'confirm_stock_volume_veto',
    exitMode: 'eod',
    maxHoldings: 5,
    targetLimit: 5,
    singleEntry: true,
    rankMode: 'stock_volume',
    checkInterval: 5,
  }),
  makeStrategy({
    id: 'first_confirm_stock_option_veto_after_1115_fixed_exit',
    name: 'First 1m/5m confirm + stock/option veto after 11:15',
    description: 'Takes the first confirmed basket unless stock volume is weak/bearish or prior-day root option flow is extremely put-heavy.',
    entryMode: 'confirm_stock_option_veto',
    exitMode: 'eod',
    maxHoldings: 5,
    targetLimit: 5,
    singleEntry: true,
    rankMode: 'stock_option_volume',
    checkInterval: 5,
  }),
  makeStrategy({
    id: 'first_confirm_stock_option_volume_after_1115_fixed_exit',
    name: 'First 1m/5m confirm + stock/option volume after 11:15',
    description: 'Requires price confirmation, supportive same-day stock volume, and prior-day option flow not dominated by puts.',
    entryMode: 'confirm_stock_option_volume',
    exitMode: 'eod',
    maxHoldings: 5,
    targetLimit: 5,
    singleEntry: true,
    rankMode: 'stock_option_volume',
    checkInterval: 5,
  }),
  makeStrategy({
    id: 'first_score_rank_top3_after_1115_fixed_exit',
    name: 'First score-rank top 3 after 11:15, fixed exit',
    description: 'Check every five minutes after 11:15, take the first positive 1m/5m score-ranked top-three basket, then hold to 15:55.',
    entryMode: 'score_rank',
    exitMode: 'eod',
    maxHoldings: 3,
    targetLimit: 8,
    singleEntry: true,
    checkInterval: 5,
  }),
  makeStrategy({
    id: 'first_volume_score_rank_top3_after_1115_fixed_exit',
    name: 'First stock/option volume score top 3 after 11:15',
    description: 'Ranks top PYM targets by price momentum plus same-day stock volume and prior-day option-flow features.',
    entryMode: 'stock_option_volume_score',
    exitMode: 'eod',
    maxHoldings: 3,
    targetLimit: 8,
    singleEntry: true,
    rankMode: 'stock_option_volume',
    checkInterval: 5,
  }),
  makeStrategy({
    id: 'first_reclaim_after_1115_fixed_exit',
    name: 'First VWAP/RSI reclaim after 11:15, fixed exit',
    description: 'Take the first VWAP/RSI reclaim inside a positive 5m trend, then hold that basket to 15:55.',
    entryMode: 'reclaim',
    exitMode: 'eod',
    maxHoldings: 3,
    targetLimit: 8,
    singleEntry: true,
    checkInterval: 5,
  }),
  makeStrategy({
    id: 'first_reclaim_stock_volume_after_1115_fixed_exit',
    name: 'First VWAP/RSI reclaim + stock volume after 11:15',
    description: 'Take the first reclaim only when recent stock volume confirms participation.',
    entryMode: 'reclaim_stock_volume',
    exitMode: 'eod',
    maxHoldings: 3,
    targetLimit: 8,
    singleEntry: true,
    rankMode: 'stock_volume',
    checkInterval: 5,
  }),
  makeStrategy({
    id: 'first_reclaim_stock_volume_veto_after_1115_fixed_exit',
    name: 'First VWAP/RSI reclaim + stock volume veto after 11:15',
    description: 'Takes the first reclaim unless same-day stock volume shows weak or bearish participation.',
    entryMode: 'reclaim_stock_volume_veto',
    exitMode: 'eod',
    maxHoldings: 3,
    targetLimit: 8,
    singleEntry: true,
    rankMode: 'stock_volume',
    checkInterval: 5,
  }),
  makeStrategy({
    id: 'first_reclaim_stock_option_veto_after_1115_fixed_exit',
    name: 'First VWAP/RSI reclaim + stock/option veto after 11:15',
    description: 'Takes the first reclaim unless stock volume is weak/bearish or prior-day root option flow is extremely put-heavy.',
    entryMode: 'reclaim_stock_option_veto',
    exitMode: 'eod',
    maxHoldings: 3,
    targetLimit: 8,
    singleEntry: true,
    rankMode: 'stock_option_volume',
    checkInterval: 5,
  }),
  makeStrategy({
    id: 'first_reclaim_stock_option_volume_after_1115_fixed_exit',
    name: 'First VWAP/RSI reclaim + stock/option volume after 11:15',
    description: 'Take the first reclaim with same-day stock-volume support and prior-day option-flow confirmation.',
    entryMode: 'reclaim_stock_option_volume',
    exitMode: 'eod',
    maxHoldings: 3,
    targetLimit: 8,
    singleEntry: true,
    rankMode: 'stock_option_volume',
    checkInterval: 5,
  }),
  makeStrategy({
    id: 'dynamic_reclaim_dynamic_exit',
    name: 'Dynamic reclaim, dynamic RSI/VWAP exit',
    description: 'After 11:15, enter on 1m RSI/VWAP reclaim and exit on signal invalidation or stop.',
    entryMode: 'reclaim',
    exitMode: 'rsi_vwap',
    maxHoldings: 3,
    targetLimit: 8,
    checkInterval: 5,
  }),
  makeStrategy({
    id: 'first_breakout_after_1115_fixed_exit',
    name: 'First opening range breakout after 11:15, fixed exit',
    description: 'Take the first confirmed opening-range breakout after 11:15 and hold the basket to 15:55.',
    entryMode: 'breakout',
    exitMode: 'eod',
    maxHoldings: 3,
    targetLimit: 8,
    singleEntry: true,
    checkInterval: 5,
  }),
  makeStrategy({
    id: 'opening_range_breakout_fixed_exit',
    name: 'Opening range breakout, fixed exit',
    description: 'Enter when a PYM target breaks the 9:30-10:30 range with 5m confirmation; exit at 15:55.',
    entryMode: 'breakout',
    exitMode: 'eod',
    maxHoldings: 3,
    targetLimit: 8,
    checkInterval: 5,
  }),
  makeStrategy({
    id: 'opening_range_breakout_dynamic_exit',
    name: 'Opening range breakout, dynamic exit',
    description: 'Opening range breakout entry with trailing/profit and 5m rollover exits.',
    entryMode: 'breakout',
    exitMode: 'trailing_profit',
    maxHoldings: 3,
    targetLimit: 8,
    checkInterval: 5,
  }),
  makeStrategy({
    id: 'fixed_1200_confirm_fixed_exit',
    name: 'Fixed 12:00 confirm, fixed exit',
    description: 'Enter once at noon on 1m/5m confirmation, then hold to 15:55.',
    entryMode: 'confirm',
    exitMode: 'eod',
    fixedEntryMinute: 720,
    maxHoldings: 5,
    targetLimit: 5,
  }),
  makeStrategy({
    id: 'fixed_1300_confirm_fixed_exit',
    name: 'Fixed 13:00 confirm, fixed exit',
    description: 'Enter once at 13:00 on 1m/5m confirmation, then hold to 15:55.',
    entryMode: 'confirm',
    exitMode: 'eod',
    fixedEntryMinute: 780,
    maxHoldings: 5,
    targetLimit: 5,
  }),
  makeStrategy({
    id: 'pullback_recovery_fixed_exit',
    name: 'Pullback recovery, fixed exit',
    description: 'Arm on 1m RSI pullback inside 5m uptrend, enter on recovery, exit at 15:55.',
    entryMode: 'pullback_recovery',
    exitMode: 'eod',
    maxHoldings: 3,
    targetLimit: 8,
    checkInterval: 5,
  }),
  makeStrategy({
    id: 'pullback_recovery_dynamic_exit',
    name: 'Pullback recovery, dynamic exit',
    description: 'Pullback recovery entry with soft stop, trailing stop, profit target, and trend rollover exits.',
    entryMode: 'pullback_recovery',
    exitMode: 'soft',
    maxHoldings: 3,
    targetLimit: 8,
    checkInterval: 5,
  }),
]);

function withOptionFilter(strategy) {
  return {
    ...strategy,
    id: `${strategy.id}_prev_option_filter`,
    name: `${strategy.name} + prior SPY option filter`,
    description: `${strategy.description} Skips new entries when prior-day SPY put-pressure z-score is at least 2.5.`,
    optionRiskFilter: true,
  };
}

const DYNAMIC_INTRADAY_STRATEGIES = Object.freeze([
  ...BASE_DYNAMIC_STRATEGIES,
  ...BASE_DYNAMIC_STRATEGIES.map(withOptionFilter),
]);

function shouldCheck(strategy, minute) {
  if (minute < ENTRY_START_MINUTE || minute > DEFAULT_EXIT_MINUTE) return false;
  if (strategy.fixedEntryMinute !== null) return minute === strategy.fixedEntryMinute || minute === DEFAULT_EXIT_MINUTE;
  return ((minute - ENTRY_START_MINUTE) % strategy.checkInterval) === 0 || minute === DEFAULT_EXIT_MINUTE;
}

function positionWeights(dayState) {
  return cleanWeights(new Map([...dayState.positions.keys()].map((ticker) => [ticker, 1])));
}

function updatePositionHighs(dayState, features) {
  features.forEach((feature) => {
    const position = dayState.positions.get(feature.ticker);
    if (!position || !Number.isFinite(feature.close)) return;
    position.highPrice = Math.max(position.highPrice, feature.close);
  });
}

function markPullbacks(dayState, features) {
  features.forEach((feature) => {
    if (hasBaseTrend(feature) && feature.rsi1m20 >= 34 && feature.rsi1m20 <= 46) {
      dayState.pullbackArmed.add(feature.ticker);
    }
  });
}

function entryPredicate(strategy, feature, dayState, priorFeature) {
  if (strategy.entryMode === 'confirm') return hasStrongConfirmation(feature);
  if (strategy.entryMode === 'confirm_stock_volume') return hasStrongConfirmation(feature) && stockVolumeConfirm(feature);
  if (strategy.entryMode === 'confirm_stock_volume_veto') return hasStrongConfirmation(feature) && stockVolumeVetoPass(feature);
  if (strategy.entryMode === 'confirm_stock_option_veto') {
    return hasStrongConfirmation(feature) && stockVolumeVetoPass(feature) && optionBearishVetoPass(feature, dayState);
  }
  if (strategy.entryMode === 'confirm_stock_option_volume') {
    return hasStrongConfirmation(feature) && stockVolumeConfirm(feature) && optionVolumeConfirm(feature, dayState);
  }
  if (strategy.entryMode === 'score_rank') return Number.isFinite(feature.score) && feature.score > 0.15 && feature.rsi5m20 >= 48;
  if (strategy.entryMode === 'stock_option_volume_score') {
    return stockOptionVolumeScore(feature, dayState) > 0.35 && feature.rsi5m20 >= 50 && stockVolumeConfirm(feature);
  }
  if (strategy.entryMode === 'reclaim') return reclaimEntry(feature, priorFeature);
  if (strategy.entryMode === 'reclaim_stock_volume') return reclaimEntry(feature, priorFeature) && stockVolumeConfirm(feature);
  if (strategy.entryMode === 'reclaim_stock_volume_veto') return reclaimEntry(feature, priorFeature) && stockVolumeVetoPass(feature);
  if (strategy.entryMode === 'reclaim_stock_option_veto') {
    return reclaimEntry(feature, priorFeature) && stockVolumeVetoPass(feature) && optionBearishVetoPass(feature, dayState);
  }
  if (strategy.entryMode === 'reclaim_stock_option_volume') {
    return reclaimEntry(feature, priorFeature) && stockVolumeConfirm(feature) && optionVolumeConfirm(feature, dayState);
  }
  if (strategy.entryMode === 'breakout') return breakoutEntry(feature, dayState);
  if (strategy.entryMode === 'pullback_recovery') return pullbackRecoveryEntry(feature, dayState);
  throw new Error(`unknown_entry_mode:${strategy.entryMode}`);
}

function rankScore(strategy, feature, dayState) {
  if (strategy.rankMode === 'stock_volume') return (feature.score || 0) + ((feature.stockVolumeScore || 0) * 0.45);
  if (strategy.rankMode === 'stock_option_volume') return stockOptionVolumeScore(feature, dayState);
  return feature.score || -Infinity;
}

function exitPredicate(strategy, feature, position) {
  if (strategy.exitMode === 'eod') return false;
  if (strategy.exitMode === 'rsi_vwap') return rsiVwapExit(feature, position);
  if (strategy.exitMode === 'trailing_profit') return trailingProfitExit(feature, position);
  if (strategy.exitMode === 'soft') return softExit(feature, position);
  throw new Error(`unknown_exit_mode:${strategy.exitMode}`);
}

function buildOpeningRange(dayBars, tickers) {
  const out = new Map();
  tickers.forEach((ticker) => {
    let high = null;
    const bars = dayBars.barsByTicker.get(ticker);
    if (!bars) return;
    bars.forEach((bar, minute) => {
      if (minute < 570 || minute > 630) return;
      if (!Number.isFinite(bar.high) || bar.high <= 0) return;
      high = high === null ? bar.high : Math.max(high, bar.high);
    });
    if (Number.isFinite(high)) out.set(ticker, high);
  });
  return out;
}

function applyRebalance(state, nextWeights, costBps) {
  const next = cleanWeights(nextWeights);
  const turnover = weightTurnover(state.weights, next);
  if (turnover <= 1e-10) return { turnover: 0, trades: 0, cost: 0 };
  const trades = countTrades(state.weights, next);
  const cost = state.equity * turnover * costBps / 10000;
  state.equity -= cost;
  state.weights = next;
  state.totalTurnover += turnover;
  state.tradeCount += trades;
  state.tradeEvents += 1;
  return { turnover, trades, cost };
}

function emptyState(strategy, initialCapital) {
  return {
    strategy,
    equity: initialCapital,
    weights: new Map(),
    dailyReturns: [],
    equityCurve: [],
    daySummaries: [],
    totalTurnover: 0,
    tradeCount: 0,
    tradeEvents: 0,
    missingReturnEvents: 0,
    activeDays: 0,
    skippedByOptionRisk: 0,
  };
}

function emptyDayState(dayBars, tickers, priorOptionRowForDay = null) {
  return {
    positions: new Map(),
    exitedTickers: new Set(),
    pullbackArmed: new Set(),
    previousFeatures: new Map(),
    openingRangeHigh: buildOpeningRange(dayBars, tickers),
    entryLocked: false,
    priorOptionRow: priorOptionRowForDay,
  };
}

function selectEntries({ strategy, features, dayState, minute, optionRiskOff }) {
  if (optionRiskOff) return [];
  if (strategy.singleEntry && dayState.entryLocked) return [];
  if (strategy.fixedEntryMinute !== null && minute !== strategy.fixedEntryMinute) return [];
  if (minute > strategy.lastEntryMinute) return [];
  const slots = strategy.maxHoldings - dayState.positions.size;
  if (slots <= 0) return [];
  return features
    .filter((feature) => (
      !dayState.positions.has(feature.ticker)
      && !dayState.exitedTickers.has(feature.ticker)
      && entryPredicate(strategy, feature, dayState, dayState.previousFeatures.get(feature.ticker))
    ))
    .sort((left, right) => rankScore(strategy, right, dayState) - rankScore(strategy, left, dayState) || right.baseWeight - left.baseWeight)
    .slice(0, slots);
}

function runDecision({ state, strategy, dayState, ctx, costBps }) {
  const features = rankedFeatures(ctx, strategy.targetLimit);
  updatePositionHighs(dayState, features);
  markPullbacks(dayState, features);

  features.forEach((feature) => {
    const position = dayState.positions.get(feature.ticker);
    if (!position) return;
    if (ctx.minuteEt === DEFAULT_EXIT_MINUTE || exitPredicate(strategy, feature, position)) {
      dayState.positions.delete(feature.ticker);
      dayState.exitedTickers.add(feature.ticker);
    }
  });

  const optionRiskOff = strategy.optionRiskFilter && Number.isFinite(ctx.priorSpyPutPressure)
    && ctx.priorSpyPutPressure >= strategy.optionRiskThreshold;
  if (optionRiskOff) state.skippedByOptionRisk += 1;
  const entries = selectEntries({ strategy, features, dayState, minute: ctx.minuteEt, optionRiskOff });
  entries.forEach((feature) => {
    dayState.positions.set(feature.ticker, {
      entryMinute: ctx.minuteEt,
      entryPrice: feature.close,
      highPrice: feature.close,
    });
  });
  if (entries.length && strategy.singleEntry) dayState.entryLocked = true;
  const event = applyRebalance(state, positionWeights(dayState), costBps);
  features.forEach((feature) => dayState.previousFeatures.set(feature.ticker, feature));
  return { ...event, entries: entries.length, exits: event.trades - entries.length };
}

function simulateStrategyDay({ state, strategy, day, dayBars, minutes, targetSnapshot, costBps, priorSpyPutPressure, priorOptionRowForDay }) {
  const tickers = targetWeights(targetSnapshot, strategy.targetLimit);
  const dayState = emptyDayState(dayBars, new Set(tickers.keys()), priorOptionRowForDay);
  const startEquity = state.equity;
  const startTrades = state.tradeCount;
  const startTurnover = state.totalTurnover;
  const events = [];
  let previousMinute = null;

  for (const minute of minutes) {
    if (previousMinute !== null) applyMinuteReturn(state, dayBars, previousMinute, minute);
    if (shouldCheck(strategy, minute)) {
      const event = runDecision({
        state,
        strategy,
        dayState,
        costBps,
        ctx: {
          day,
          dayBars,
          minuteEt: minute,
          targetSnapshot,
          priorSpyPutPressure,
        },
      });
      if (event.turnover > 0) events.push({ minuteEt: minute, ...event });
    }
    previousMinute = minute;
  }

  if (state.weights.size) {
    dayState.positions.clear();
    const event = applyRebalance(state, new Map(), costBps);
    if (event.turnover > 0) events.push({ minuteEt: minutes.at(-1), exit: true, ...event });
  }

  const dailyReturn = startEquity > 0 ? (state.equity / startEquity) - 1 : 0;
  state.dailyReturns.push(dailyReturn);
  state.equityCurve.push({ date: day.date, equity: state.equity, dailyReturn });
  if (events.length) state.activeDays += 1;
  state.daySummaries.push({
    date: day.date,
    targetSignalDate: targetSnapshot.date,
    priorSpyPutPressure,
    startEquity,
    endEquity: state.equity,
    dailyReturn,
    turnover: state.totalTurnover - startTurnover,
    tradeCount: state.tradeCount - startTrades,
    events: events.length,
    firstEventMinute: events[0]?.minuteEt || null,
    lastEventMinute: events.at(-1)?.minuteEt || null,
  });
}

function summarizeState(state, initialCapital) {
  const totalReturn = (state.equity / initialCapital) - 1;
  const annualizedVolatility = standardDeviation(state.dailyReturns) * Math.sqrt(252);
  const avgDaily = mean(state.dailyReturns);
  return {
    id: state.strategy.id,
    name: state.strategy.name,
    description: state.strategy.description,
    finalEquity: state.equity,
    totalReturn,
    totalReturnPct: totalReturn * 100,
    cagr: state.dailyReturns.length ? ((1 + totalReturn) ** (252 / state.dailyReturns.length)) - 1 : 0,
    maxDrawdown: maxDrawdown(state.equityCurve),
    maxDrawdownPct: maxDrawdown(state.equityCurve) * 100,
    annualizedVolatility,
    sharpe: annualizedVolatility > 0 ? (avgDaily * 252) / annualizedVolatility : 0,
    tradingDays: state.dailyReturns.length,
    activeDays: state.activeDays,
    activeShare: state.dailyReturns.length ? state.activeDays / state.dailyReturns.length : 0,
    winRate: state.dailyReturns.length ? state.dailyReturns.filter((value) => value > 0).length / state.dailyReturns.length : 0,
    averageDailyTurnover: state.dailyReturns.length ? state.totalTurnover / state.dailyReturns.length : 0,
    averageTradesPerDay: state.dailyReturns.length ? state.tradeCount / state.dailyReturns.length : 0,
    averageTradeEventsPerDay: state.dailyReturns.length ? state.tradeEvents / state.dailyReturns.length : 0,
    totalTurnover: state.totalTurnover,
    totalTrades: state.tradeCount,
    tradeEvents: state.tradeEvents,
    missingReturnEvents: state.missingReturnEvents,
    skippedByOptionRisk: state.skippedByOptionRisk,
  };
}

function benchmarkIntradayReturn(dayBars, ticker, entryMinute, exitMinute) {
  const entry = closeAt(dayBars, ticker, entryMinute);
  const exit = closeAt(dayBars, ticker, exitMinute);
  if (!Number.isFinite(entry) || !Number.isFinite(exit) || entry <= 0) return null;
  return (exit / entry) - 1;
}

async function runDynamicIntradayTiming(settings = {}) {
  const config = settings.config || loadConfig();
  const startDate = settings.startDate || '2025-01-02';
  const endDate = settings.endDate || '2026-05-08';
  const costBps = Number.isFinite(settings.costBps) ? settings.costBps : 2;
  const initialCapital = Number.isFinite(settings.initialCapital) ? settings.initialCapital : 10000;
  const strategies = settings.strategies || DYNAMIC_INTRADAY_STRATEGIES;
  const targetReport = settings.targetReport || buildTargetReport({
    config,
    dailyBarsPath: settings.dailyBarsPath,
    scorePath: settings.scorePath,
    startDate,
    rsiMode: settings.rsiMode || 'wilder',
  });
  const optionPath = settings.optionFeaturesPath || latestOptionFeaturesPath();
  const optionByDate = optionRowsByDate(optionPath);
  const targetByDate = new Map(targetReport.snapshots.map((snapshot) => [snapshot.date, snapshot]));
  const days = openCalendarDays(config.roots.calendar, startDate, endDate);
  const marketDates = targetReport.snapshots.map((snapshot) => snapshot.date);
  const states = strategies.map((strategy) => emptyState(strategy, initialCapital));
  const skippedDays = [];
  const benchmarks = { spy: [], qqq: [] };
  let previousSnapshot = null;

  for (const day of days) {
    const todaysSnapshot = targetByDate.get(day.date);
    if (!previousSnapshot) {
      previousSnapshot = todaysSnapshot || previousSnapshot;
      skippedDays.push({ date: day.date, reason: 'no_prior_eod_target' });
      continue;
    }
    const tickers = new Set([...targetTickers(previousSnapshot), 'SPY', 'QQQ']);
    const dayBars = await readMinuteBarsForDay(config, day, tickers);
    const minutes = dayBars.minutes.filter((minute) => minute >= 570 && minute <= DEFAULT_EXIT_MINUTE);
    if (!minutes.length) {
      skippedDays.push({ date: day.date, reason: 'missing_minute_bars' });
      previousSnapshot = todaysSnapshot || previousSnapshot;
      continue;
    }
    const priorOptionRowForDay = priorOptionRow(optionByDate, day.date, marketDates);
    const priorSpyPutPressure = spyPutPressureFromOptionRow(priorOptionRowForDay);
    states.forEach((state) => simulateStrategyDay({
      state,
      strategy: state.strategy,
      day,
      dayBars,
      minutes,
      targetSnapshot: previousSnapshot,
      costBps,
      priorSpyPutPressure,
      priorOptionRowForDay,
    }));
    const spy = benchmarkIntradayReturn(dayBars, 'SPY', ENTRY_START_MINUTE, DEFAULT_EXIT_MINUTE);
    const qqq = benchmarkIntradayReturn(dayBars, 'QQQ', ENTRY_START_MINUTE, DEFAULT_EXIT_MINUTE);
    if (spy !== null) benchmarks.spy.push(spy);
    if (qqq !== null) benchmarks.qqq.push(qqq);
    previousSnapshot = todaysSnapshot || previousSnapshot;
    if (settings.onProgress) settings.onProgress({ day, processedDays: states[0].dailyReturns.length, skippedDays: skippedDays.length });
  }

  const summaries = states.map((state) => summarizeState(state, initialCapital))
    .sort((left, right) => right.totalReturn - left.totalReturn);
  return {
    generatedAt: new Date().toISOString(),
    settings: {
      startDate,
      endDate,
      costBps,
      initialCapital,
      rsiMode: settings.rsiMode || 'wilder',
      timing: 'causal_dynamic_intraday_entries_and_exits_flat_overnight',
      featureWindows: { oneMinuteBars: 20, fiveMinuteBars: 20 },
      entryStartMinute: ENTRY_START_MINUTE,
      exitMinute: DEFAULT_EXIT_MINUTE,
      optionFeaturesPath: optionPath,
    },
    targetSource: targetReport.source,
    skippedDays,
    benchmarks: {
      window: '11:15-15:55 ET',
      spyEntryToExitReturn: benchmarks.spy.reduce((equity, value) => equity * (1 + value), 1) - 1,
      qqqEntryToExitReturn: benchmarks.qqq.reduce((equity, value) => equity * (1 + value), 1) - 1,
      spyObservations: benchmarks.spy.length,
      qqqObservations: benchmarks.qqq.length,
    },
    summaries,
    strategies: states.map((state) => ({
      summary: summarizeState(state, initialCapital),
      daySummaries: state.daySummaries,
      equityCurve: state.equityCurve,
    })),
  };
}

module.exports = {
  DYNAMIC_INTRADAY_STRATEGIES,
  BASE_DYNAMIC_STRATEGIES,
  breakoutEntry,
  latestOptionFeaturesPath,
  reclaimEntry,
  runDynamicIntradayTiming,
  spyPutPressureFromOptionRow,
  summarizeState,
};
