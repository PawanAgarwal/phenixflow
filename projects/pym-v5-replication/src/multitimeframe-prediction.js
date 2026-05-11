const { openCalendarDays } = require('./calendar');
const { loadConfig } = require('./config');
const { readMinuteBarsForDay, closeAt } = require('./intraday-minute-data');
const { buildTargetReport } = require('./intraday-suite');

const DEFAULT_INTERVAL_MINUTES = 5;
const DEFAULT_START_MINUTE = 675;
const DEFAULT_END_MINUTE = 955;
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

function cleanWeights(weights, maxExposure = 1) {
  const out = new Map();
  let total = 0;
  weights.forEach((weight, ticker) => {
    if (Number.isFinite(weight) && weight > 1e-10) {
      out.set(ticker, weight);
      total += weight;
    }
  });
  if (total <= 0) return new Map();
  out.forEach((weight, ticker) => out.set(ticker, (weight / total) * maxExposure));
  return out;
}

function targetWeights(snapshot, limit = null) {
  const rows = (snapshot?.holdings || [])
    .filter((holding) => !SAFE_TICKERS.has(holding.ticker))
    .sort((left, right) => right.weight - left.weight || left.ticker.localeCompare(right.ticker));
  return cleanWeights(new Map((limit ? rows.slice(0, limit) : rows).map((holding) => [holding.ticker, holding.weight])));
}

function tickerSetForSnapshot(snapshot) {
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

function closeSeriesAtOrBefore(dayBars, ticker, minute, stepMinutes = 1) {
  const bars = dayBars.barsByTicker.get(ticker);
  if (!bars) return [];
  const rows = [];
  for (let cursor = 570; cursor <= minute; cursor += stepMinutes) {
    const bar = bars.get(cursor);
    const close = bar?.close ?? closeAt(dayBars, ticker, cursor);
    if (Number.isFinite(close) && close > 0) rows.push({ minute: cursor, close });
  }
  return rows;
}

function barSeriesAtOrBefore(dayBars, ticker, minute, stepMinutes = 1) {
  const bars = dayBars.barsByTicker.get(ticker);
  if (!bars) return [];
  const rows = [];
  for (let cursor = 570; cursor <= minute; cursor += stepMinutes) {
    const bar = bars.get(cursor);
    if (!bar || !Number.isFinite(bar.close) || bar.close <= 0) continue;
    rows.push({
      minute: cursor,
      open: Number.isFinite(bar.open) ? bar.open : null,
      high: Number.isFinite(bar.high) ? bar.high : null,
      low: Number.isFinite(bar.low) ? bar.low : null,
      close: bar.close,
      volume: Number.isFinite(bar.volume) && bar.volume > 0 ? bar.volume : 0,
    });
  }
  return rows;
}

function cumulativeReturn(rows, bars) {
  if (rows.length <= bars) return null;
  const previous = rows[rows.length - 1 - bars]?.close;
  const current = rows.at(-1)?.close;
  if (!Number.isFinite(previous) || !Number.isFinite(current) || previous <= 0) return null;
  return (current / previous) - 1;
}

function rsiFromRows(rows, window) {
  if (rows.length <= window) return null;
  const slice = rows.slice(-(window + 1));
  let gains = 0;
  let losses = 0;
  for (let index = 1; index < slice.length; index += 1) {
    const previous = slice[index - 1].close;
    const current = slice[index].close;
    if (!Number.isFinite(previous) || !Number.isFinite(current) || previous <= 0) return null;
    const change = (current / previous) - 1;
    if (change > 0) gains += change;
    else losses -= change;
  }
  const avgGain = gains / window;
  const avgLoss = losses / window;
  if (avgGain === 0 && avgLoss === 0) return 50;
  if (avgLoss === 0) return 100;
  const rs = avgGain / avgLoss;
  return 100 - (100 / (1 + rs));
}

function volumeRatioFromRows(rows, window) {
  if (rows.length <= window) return null;
  const slice = rows.slice(-(window + 1));
  const current = slice.at(-1)?.volume;
  const prior = slice.slice(0, -1).map((row) => row.volume).filter((value) => Number.isFinite(value) && value > 0);
  const avg = mean(prior);
  return avg > 0 && Number.isFinite(current) ? current / avg : null;
}

function dollarVolumeRatioFromRows(rows, window) {
  if (rows.length <= window) return null;
  const slice = rows.slice(-(window + 1));
  const current = slice.at(-1);
  const prior = slice.slice(0, -1)
    .map((row) => row.close * row.volume)
    .filter((value) => Number.isFinite(value) && value > 0);
  const avg = mean(prior);
  const currentDollarVolume = current?.close * current?.volume;
  return avg > 0 && Number.isFinite(currentDollarVolume) ? currentDollarVolume / avg : null;
}

function signedVolumeImbalanceFromRows(rows, window) {
  if (rows.length <= window) return null;
  const slice = rows.slice(-(window + 1));
  let signed = 0;
  let total = 0;
  for (let index = 1; index < slice.length; index += 1) {
    const previous = slice[index - 1];
    const current = slice[index];
    if (!Number.isFinite(current.volume) || current.volume <= 0) continue;
    const direction = Math.sign(current.close - previous.close);
    signed += direction * current.volume;
    total += current.volume;
  }
  return total > 0 ? signed / total : null;
}

function volumeWeightedReturnFromRows(rows, window) {
  if (rows.length <= window) return null;
  const slice = rows.slice(-(window + 1));
  let weightedReturn = 0;
  let totalVolume = 0;
  for (let index = 1; index < slice.length; index += 1) {
    const previous = slice[index - 1];
    const current = slice[index];
    if (!Number.isFinite(current.volume) || current.volume <= 0 || previous.close <= 0) continue;
    weightedReturn += ((current.close / previous.close) - 1) * current.volume;
    totalVolume += current.volume;
  }
  return totalVolume > 0 ? weightedReturn / totalVolume : null;
}

function intradayVwap(dayBars, ticker, minute) {
  const bars = dayBars.barsByTicker.get(ticker);
  if (!bars) return null;
  let volume = 0;
  let dollars = 0;
  bars.forEach((bar, barMinute) => {
    if (barMinute > minute) return;
    if (!Number.isFinite(bar.close) || !Number.isFinite(bar.volume) || bar.volume <= 0) return;
    volume += bar.volume;
    dollars += bar.close * bar.volume;
  });
  return volume > 0 ? dollars / volume : null;
}

function tickerFeatures(dayBars, ticker, minute, window = 20) {
  const oneMinuteBars = barSeriesAtOrBefore(dayBars, ticker, minute, 1);
  const oneMinute = oneMinuteBars.map(({ minute: rowMinute, close }) => ({ minute: rowMinute, close }));
  const fiveMinute = closeSeriesAtOrBefore(dayBars, ticker, minute, 5);
  const close = oneMinute.at(-1)?.close ?? null;
  const vwap = intradayVwap(dayBars, ticker, minute);
  return {
    ticker,
    close,
    rsi1m20: rsiFromRows(oneMinute, window),
    rsi5m20: rsiFromRows(fiveMinute, window),
    ret1m20: cumulativeReturn(oneMinute, window),
    ret5m20: cumulativeReturn(fiveMinute, window),
    aboveVwap: Number.isFinite(close) && Number.isFinite(vwap) && vwap > 0 ? close >= vwap : false,
    volumeRatio20: volumeRatioFromRows(oneMinuteBars, window),
    dollarVolumeRatio20: dollarVolumeRatioFromRows(oneMinuteBars, window),
    signedVolumeImbalance20: signedVolumeImbalanceFromRows(oneMinuteBars, window),
    volumeWeightedReturn20: volumeWeightedReturnFromRows(oneMinuteBars, window),
  };
}

function scoreFeature(feature) {
  const parts = [
    Number.isFinite(feature.rsi1m20) ? (feature.rsi1m20 - 50) / 25 : null,
    Number.isFinite(feature.rsi5m20) ? (feature.rsi5m20 - 50) / 25 : null,
    Number.isFinite(feature.ret1m20) ? feature.ret1m20 * 100 : null,
    Number.isFinite(feature.ret5m20) ? feature.ret5m20 * 40 : null,
    feature.aboveVwap ? 0.25 : -0.25,
  ].filter(Number.isFinite);
  return parts.length ? mean(parts) : null;
}

function rankedFeatures(ctx, limit = null) {
  const base = targetWeights(ctx.targetSnapshot, limit);
  return [...base.keys()]
    .map((ticker) => ({ ...tickerFeatures(ctx.dayBars, ticker, ctx.minuteEt), baseWeight: base.get(ticker) || 0 }))
    .filter((feature) => Number.isFinite(feature.close) && Number.isFinite(feature.rsi1m20))
    .map((feature) => ({ ...feature, score: scoreFeature(feature) }))
    .sort((left, right) => (right.score || -Infinity) - (left.score || -Infinity) || right.baseWeight - left.baseWeight);
}

function weightsFromFeatures(features, { equalWeight = false, maxExposure = 1 } = {}) {
  if (!features.length) return new Map();
  const raw = new Map(features.map((feature) => [feature.ticker, equalWeight ? 1 : Math.max(feature.baseWeight, 0.0001)]));
  return cleanWeights(raw, maxExposure);
}

function makeStrategy({
  id,
  name,
  description,
  targetLimit = 5,
  predicate,
  maxHoldings = null,
  equalWeight = false,
  intervalMinutes = DEFAULT_INTERVAL_MINUTES,
  startMinute = DEFAULT_START_MINUTE,
  endMinute = DEFAULT_END_MINUTE,
}) {
  return {
    id,
    name,
    description,
    startMinute,
    endMinute,
    intervalMinutes,
    decide: (ctx) => {
      const selected = rankedFeatures(ctx, targetLimit).filter(predicate);
      return weightsFromFeatures(maxHoldings ? selected.slice(0, maxHoldings) : selected, { equalWeight });
    },
  };
}

const MULTITIMEFRAME_STRATEGIES = Object.freeze([
  makeStrategy({
    id: 'top5_1m_rsi20_trend',
    name: 'Top 5 PYM, 1m RSI20 trend',
    description: 'Trades top PYM risk holdings whose 1m RSI20 and 20-minute momentum are positive.',
    targetLimit: 5,
    predicate: (feature) => feature.rsi1m20 >= 55 && feature.rsi1m20 <= 80 && feature.ret1m20 > 0 && feature.aboveVwap,
  }),
  makeStrategy({
    id: 'top5_5m_rsi20_trend',
    name: 'Top 5 PYM, 5m RSI20 trend',
    description: 'Trades top PYM risk holdings whose 5m RSI20 and 20x5m momentum are positive.',
    targetLimit: 5,
    predicate: (feature) => feature.rsi5m20 >= 52 && feature.rsi5m20 <= 80 && feature.ret5m20 > 0,
  }),
  makeStrategy({
    id: 'top5_1m_5m_rsi20_confirm',
    name: 'Top 5 PYM, 1m/5m RSI20 confirm',
    description: 'Requires both 1m and 5m RSI20/momentum confirmation.',
    targetLimit: 5,
    predicate: (feature) => (
      feature.rsi1m20 >= 52
      && feature.rsi5m20 >= 52
      && feature.ret1m20 > 0
      && feature.ret5m20 > 0
      && feature.aboveVwap
    ),
  }),
  makeStrategy({
    id: 'top8_1m_5m_rsi20_confirm',
    name: 'Top 8 PYM, 1m/5m RSI20 confirm',
    description: 'Same multi-timeframe confirmation across the top eight PYM risk holdings.',
    targetLimit: 8,
    predicate: (feature) => (
      feature.rsi1m20 >= 52
      && feature.rsi5m20 >= 52
      && feature.ret1m20 > 0
      && feature.ret5m20 > 0
      && feature.aboveVwap
    ),
  }),
  makeStrategy({
    id: 'top8_score_rank_top3',
    name: 'Top 8 PYM, 1m/5m score rank top 3',
    description: 'Ranks top PYM risk holdings by combined 1m and 5m RSI/momentum score; trades best three.',
    targetLimit: 8,
    maxHoldings: 3,
    equalWeight: true,
    predicate: (feature) => Number.isFinite(feature.score) && feature.score > 0.15 && feature.rsi5m20 >= 48,
  }),
  makeStrategy({
    id: 'top8_pullback_in_5m_uptrend',
    name: 'Top 8 PYM, 1m pullback in 5m uptrend',
    description: 'Mean-reversion variant: 1m RSI20 pullback while 5m RSI20 and 20x5m momentum stay positive.',
    targetLimit: 8,
    maxHoldings: 3,
    equalWeight: true,
    predicate: (feature) => (
      feature.rsi1m20 >= 35
      && feature.rsi1m20 <= 48
      && feature.rsi5m20 >= 55
      && feature.ret5m20 > 0
    ),
  }),
  makeStrategy({
    id: 'top8_overbought_fade_to_cash',
    name: 'Top 8 PYM, avoid 1m overbought',
    description: 'Trades positive 5m trend only when 1m RSI20 is not overbought.',
    targetLimit: 8,
    predicate: (feature) => (
      feature.rsi1m20 >= 45
      && feature.rsi1m20 <= 68
      && feature.rsi5m20 >= 52
      && feature.ret5m20 > 0
    ),
  }),
  makeStrategy({
    id: 'top5_1m_5m_confirm_once_1115',
    name: 'Top 5 PYM, 1m/5m confirm once at 11:15',
    description: 'One daily decision after 20 completed 5m bars are available; holds until 15:55.',
    targetLimit: 5,
    intervalMinutes: 999,
    predicate: (feature) => (
      feature.rsi1m20 >= 52
      && feature.rsi5m20 >= 52
      && feature.ret1m20 > 0
      && feature.ret5m20 > 0
      && feature.aboveVwap
    ),
  }),
  makeStrategy({
    id: 'top8_score_rank_top3_once_1115',
    name: 'Top 8 PYM, score rank top 3 once at 11:15',
    description: 'Ranks by combined 1m/5m RSI and momentum once per day; holds best three until 15:55.',
    targetLimit: 8,
    maxHoldings: 3,
    equalWeight: true,
    intervalMinutes: 999,
    predicate: (feature) => Number.isFinite(feature.score) && feature.score > 0.15 && feature.rsi5m20 >= 48,
  }),
  makeStrategy({
    id: 'top8_pullback_once_1115',
    name: 'Top 8 PYM, pullback once at 11:15',
    description: 'One daily pullback-in-uptrend decision using 1m RSI20 and 5m RSI20.',
    targetLimit: 8,
    maxHoldings: 3,
    equalWeight: true,
    intervalMinutes: 999,
    predicate: (feature) => (
      feature.rsi1m20 >= 35
      && feature.rsi1m20 <= 48
      && feature.rsi5m20 >= 55
      && feature.ret5m20 > 0
    ),
  }),
  makeStrategy({
    id: 'top5_1m_5m_confirm_hourly',
    name: 'Top 5 PYM, 1m/5m confirm hourly',
    description: 'Multi-timeframe confirmation checked hourly to reduce rebalance churn.',
    targetLimit: 5,
    intervalMinutes: 60,
    predicate: (feature) => (
      feature.rsi1m20 >= 52
      && feature.rsi5m20 >= 52
      && feature.ret1m20 > 0
      && feature.ret5m20 > 0
      && feature.aboveVwap
    ),
  }),
  makeStrategy({
    id: 'top8_score_rank_top3_hourly',
    name: 'Top 8 PYM, score rank top 3 hourly',
    description: 'Ranks by combined 1m/5m RSI and momentum once per hour.',
    targetLimit: 8,
    maxHoldings: 3,
    equalWeight: true,
    intervalMinutes: 60,
    predicate: (feature) => Number.isFinite(feature.score) && feature.score > 0.15 && feature.rsi5m20 >= 48,
  }),
]);

function shouldDecide(strategy, minute) {
  if (minute < strategy.startMinute || minute > strategy.endMinute) return false;
  return ((minute - strategy.startMinute) % strategy.intervalMinutes) === 0;
}

function priceReturn(dayBars, ticker, previousMinute, minute) {
  const previous = closeAt(dayBars, ticker, previousMinute);
  const current = closeAt(dayBars, ticker, minute);
  if (!Number.isFinite(previous) || !Number.isFinite(current) || previous <= 0) return null;
  return (current / previous) - 1;
}

function applyMinuteReturn(state, dayBars, previousMinute, minute) {
  let grossReturn = 0;
  state.weights.forEach((weight, ticker) => {
    const ret = priceReturn(dayBars, ticker, previousMinute, minute);
    if (ret === null) {
      state.missingReturnEvents += 1;
      return;
    }
    grossReturn += weight * ret;
  });
  state.equity *= (1 + grossReturn);
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
  };
}

function simulateStrategyDay({ state, strategy, day, dayBars, minutes, targetSnapshot, costBps }) {
  const startEquity = state.equity;
  const startTurnover = state.totalTurnover;
  const startTrades = state.tradeCount;
  const rebalanceEvents = [];
  let previousMinute = null;

  for (const minute of minutes) {
    if (previousMinute !== null) applyMinuteReturn(state, dayBars, previousMinute, minute);
    if (shouldDecide(strategy, minute)) {
      const desired = strategy.decide({ day, dayBars, minuteEt: minute, minutes, targetSnapshot });
      const event = applyRebalance(state, desired, costBps);
      if (event.turnover > 0) rebalanceEvents.push({ minuteEt: minute, ...event });
    }
    if (minute === strategy.endMinute && state.weights.size) {
      const event = applyRebalance(state, new Map(), costBps);
      if (event.turnover > 0) rebalanceEvents.push({ minuteEt: minute, exit: true, ...event });
    }
    previousMinute = minute;
  }

  if (state.weights.size) {
    const event = applyRebalance(state, new Map(), costBps);
    if (event.turnover > 0) rebalanceEvents.push({ minuteEt: minutes.at(-1), exit: true, ...event });
  }

  const dailyReturn = startEquity > 0 ? (state.equity / startEquity) - 1 : 0;
  state.dailyReturns.push(dailyReturn);
  state.equityCurve.push({ date: day.date, equity: state.equity, dailyReturn });
  if (rebalanceEvents.length) state.activeDays += 1;
  state.daySummaries.push({
    date: day.date,
    targetSignalDate: targetSnapshot.date,
    startEquity,
    endEquity: state.equity,
    dailyReturn,
    turnover: state.totalTurnover - startTurnover,
    tradeCount: state.tradeCount - startTrades,
    rebalanceEvents: rebalanceEvents.length,
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
  };
}

function benchmarkIntradayReturn(dayBars, ticker, entryMinute, exitMinute) {
  const entry = closeAt(dayBars, ticker, entryMinute);
  const exit = closeAt(dayBars, ticker, exitMinute);
  if (!Number.isFinite(entry) || !Number.isFinite(exit) || entry <= 0) return null;
  return (exit / entry) - 1;
}

async function runMultiTimeframePrediction(settings = {}) {
  const config = settings.config || loadConfig();
  const startDate = settings.startDate || '2025-01-02';
  const endDate = settings.endDate || '2026-05-06';
  const costBps = Number.isFinite(settings.costBps) ? settings.costBps : 2;
  const initialCapital = Number.isFinite(settings.initialCapital) ? settings.initialCapital : 10000;
  const strategies = settings.strategies || MULTITIMEFRAME_STRATEGIES;
  const targetReport = settings.targetReport || buildTargetReport({
    config,
    dailyBarsPath: settings.dailyBarsPath,
    scorePath: settings.scorePath,
    startDate,
    rsiMode: settings.rsiMode || 'wilder',
  });
  const targetByDate = new Map(targetReport.snapshots.map((snapshot) => [snapshot.date, snapshot]));
  const days = openCalendarDays(config.roots.calendar, startDate, endDate);
  const states = strategies.map((strategy) => emptyState(strategy, initialCapital));
  const skippedDays = [];
  const benchmarks = {
    spyEntryToExit: [],
    qqqEntryToExit: [],
  };
  let previousSnapshot = null;

  for (const day of days) {
    const todaysSnapshot = targetByDate.get(day.date);
    if (!previousSnapshot) {
      previousSnapshot = todaysSnapshot || previousSnapshot;
      skippedDays.push({ date: day.date, reason: 'no_prior_eod_target' });
      continue;
    }
    const tickers = new Set([...tickerSetForSnapshot(previousSnapshot), 'SPY', 'QQQ']);
    const dayBars = await readMinuteBarsForDay(config, day, tickers);
    const minutes = dayBars.minutes.filter((minute) => minute >= 570 && minute <= DEFAULT_END_MINUTE);
    if (!minutes.length) {
      skippedDays.push({ date: day.date, reason: 'missing_minute_bars' });
      previousSnapshot = todaysSnapshot || previousSnapshot;
      continue;
    }

    states.forEach((state) => simulateStrategyDay({
      state,
      strategy: state.strategy,
      day,
      dayBars,
      minutes,
      targetSnapshot: previousSnapshot,
      costBps,
    }));
    const spy = benchmarkIntradayReturn(dayBars, 'SPY', DEFAULT_START_MINUTE, DEFAULT_END_MINUTE);
    const qqq = benchmarkIntradayReturn(dayBars, 'QQQ', DEFAULT_START_MINUTE, DEFAULT_END_MINUTE);
    if (spy !== null) benchmarks.spyEntryToExit.push(spy);
    if (qqq !== null) benchmarks.qqqEntryToExit.push(qqq);
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
      timing: 'previous_eod_pym_target_with_causal_1m_and_completed_5m_features_flat_overnight',
      featureWindows: {
        oneMinuteBars: 20,
        fiveMinuteBars: 20,
      },
      decisionIntervalMinutes: DEFAULT_INTERVAL_MINUTES,
      startMinute: DEFAULT_START_MINUTE,
      endMinute: DEFAULT_END_MINUTE,
    },
    targetSource: targetReport.source,
    skippedDays,
    benchmarks: {
      entryMinute: DEFAULT_START_MINUTE,
      exitMinute: DEFAULT_END_MINUTE,
      spyEntryToExitReturn: benchmarks.spyEntryToExit.reduce((equity, value) => equity * (1 + value), 1) - 1,
      qqqEntryToExitReturn: benchmarks.qqqEntryToExit.reduce((equity, value) => equity * (1 + value), 1) - 1,
      spyObservations: benchmarks.spyEntryToExit.length,
      qqqObservations: benchmarks.qqqEntryToExit.length,
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
  MULTITIMEFRAME_STRATEGIES,
  barSeriesAtOrBefore,
  rsiFromRows,
  tickerFeatures,
  signedVolumeImbalanceFromRows,
  runMultiTimeframePrediction,
  summarizeState,
  volumeRatioFromRows,
  volumeWeightedReturnFromRows,
};
