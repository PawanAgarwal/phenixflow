const fs = require('node:fs');

const { openCalendarDays } = require('./calendar');
const { loadConfig } = require('./config');
const { readDailyBarsJsonl } = require('./backtest');
const { readMinuteBarsForDay, closeAt } = require('./intraday-minute-data');
const { buildDailyRebalanceReport, defaultScorePath, findLatestMassiveEodBarsPath } = require('./rebalance-report');
const { STRATEGIES } = require('./intraday-strategies');

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

function weightTurnover(previous, next) {
  const keys = new Set([...previous.keys(), ...next.keys()]);
  let turnover = 0;
  keys.forEach((ticker) => {
    turnover += Math.abs((next.get(ticker) || 0) - (previous.get(ticker) || 0));
  });
  return turnover;
}

function countTrades(previous, next) {
  const keys = new Set([...previous.keys(), ...next.keys()]);
  let trades = 0;
  keys.forEach((ticker) => {
    if (Math.abs((next.get(ticker) || 0) - (previous.get(ticker) || 0)) > 1e-6) trades += 1;
  });
  return trades;
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
  if (total > 0 && Math.abs(total - 1) > 1e-6) {
    out.forEach((weight, ticker) => out.set(ticker, weight / total));
  }
  return out;
}

function emptyState(strategy, initialCapital) {
  return {
    strategy,
    equity: initialCapital,
    weights: new Map(),
    dailyReturns: [],
    equityCurve: [],
    totalTurnover: 0,
    tradeCount: 0,
    tradeEvents: 0,
    missingReturnEvents: 0,
    activeDays: 0,
    daySummaries: [],
  };
}

function updateObservedPrices(dayBars, minute, lastPrices, lastSeenMinutes) {
  dayBars.barsByTicker.forEach((bars, ticker) => {
    const close = bars.get(minute)?.close;
    if (!Number.isFinite(close) || close <= 0) return;
    lastPrices.set(ticker, close);
    lastSeenMinutes.set(ticker, minute);
  });
}

function applyMinuteReturn(state, dayBars, minute, lastPrices, lastSeenMinutes) {
  let grossReturn = 0;
  state.weights.forEach((weight, ticker) => {
    const bars = dayBars.barsByTicker.get(ticker);
    const current = bars?.get(minute)?.close;
    if (!Number.isFinite(current) || current <= 0) return;
    const previous = lastPrices.get(ticker);
    if (Number.isFinite(previous) && previous > 0) {
      grossReturn += weight * ((current / previous) - 1);
    }
    lastPrices.set(ticker, current);
    lastSeenMinutes.set(ticker, minute);
  });
  state.equity *= (1 + grossReturn);
  return grossReturn;
}

function tradableWeights(desiredWeights, lastPrices, lastSeenMinutes, minute, maxStaleMinutes = 30) {
  const next = new Map();
  desiredWeights.forEach((weight, ticker) => {
    const lastPrice = lastPrices.get(ticker);
    const lastSeen = lastSeenMinutes.get(ticker);
    if (!Number.isFinite(lastPrice) || lastPrice <= 0) return;
    if (!Number.isFinite(lastSeen) || minute - lastSeen > maxStaleMinutes) return;
    next.set(ticker, weight);
  });
  return cleanWeights(next);
}

function applyRebalance(state, desiredWeights, costBps, lastPrices, lastSeenMinutes, minute) {
  const next = tradableWeights(desiredWeights, lastPrices, lastSeenMinutes, minute);
  const turnover = weightTurnover(state.weights, next);
  if (turnover <= 1e-10) return { turnover: 0, trades: 0, cost: 0 };
  const trades = countTrades(state.weights, next);
  const cost = state.equity * turnover * costBps / 10000;
  state.equity -= cost;
  state.totalTurnover += turnover;
  state.tradeCount += trades;
  state.tradeEvents += 1;
  state.weights = next;
  return { turnover, trades, cost };
}

function shouldDecide(strategy, minute) {
  if (minute < strategy.startMinute || minute > strategy.endMinute) return false;
  return ((minute - strategy.startMinute) % strategy.intervalMinutes) === 0;
}

function targetTickers(snapshot) {
  return new Set((snapshot?.holdings || []).map((holding) => holding.ticker));
}

function requiredTickersForDay(snapshot, strategies) {
  const tickers = new Set([...targetTickers(snapshot), 'SPY', 'QQQ']);
  strategies.forEach((strategy) => {
    (strategy.requiredTickers?.(snapshot) || []).forEach((ticker) => tickers.add(ticker));
  });
  return tickers;
}

function benchmarkReturn(dayBars, ticker, entryMinute, exitMinute) {
  const entry = closeAt(dayBars, ticker, entryMinute);
  const exit = closeAt(dayBars, ticker, exitMinute);
  if (!Number.isFinite(entry) || !Number.isFinite(exit) || entry <= 0) return null;
  return (exit / entry) - 1;
}

function simulateStrategyDay({ state, strategy, day, dayBars, minutes, targetSnapshot, costBps }) {
  const startEquity = state.equity;
  const dayStartTradeCount = state.tradeCount;
  const dayStartTurnover = state.totalTurnover;
  const rebalanceEvents = [];
  const lastPrices = new Map();
  const lastSeenMinutes = new Map();

  for (const minute of minutes) {
    applyMinuteReturn(state, dayBars, minute, lastPrices, lastSeenMinutes);
    updateObservedPrices(dayBars, minute, lastPrices, lastSeenMinutes);
    if (shouldDecide(strategy, minute)) {
      const desired = strategy.decide({
        day,
        dayBars,
        minuteEt: minute,
        minutes,
        targetSnapshot,
      });
      const event = applyRebalance(state, desired, costBps, lastPrices, lastSeenMinutes, minute);
      if (event.turnover > 0) rebalanceEvents.push({ minuteEt: minute, ...event });
    }
    if (minute === strategy.endMinute && state.weights.size) {
      const event = applyRebalance(state, new Map(), costBps, lastPrices, lastSeenMinutes, minute);
      if (event.turnover > 0) rebalanceEvents.push({ minuteEt: minute, exit: true, ...event });
    }
  }

  if (state.weights.size) {
    const event = applyRebalance(state, new Map(), costBps, lastPrices, lastSeenMinutes, minutes.at(-1));
    if (event.turnover > 0) rebalanceEvents.push({ minuteEt: minutes.at(-1), exit: true, ...event });
  }

  const dailyReturn = startEquity > 0 ? (state.equity / startEquity) - 1 : 0;
  state.dailyReturns.push(dailyReturn);
  state.equityCurve.push({
    date: day.date,
    equity: state.equity,
    dailyReturn,
  });
  if (rebalanceEvents.length) state.activeDays += 1;
  state.daySummaries.push({
    date: day.date,
    targetSignalDate: targetSnapshot.date,
    startEquity,
    endEquity: state.equity,
    dailyReturn,
    tradeCount: state.tradeCount - dayStartTradeCount,
    turnover: state.totalTurnover - dayStartTurnover,
    rebalanceEvents: rebalanceEvents.length,
    spyIntradayReturn: benchmarkReturn(dayBars, 'SPY', strategy.startMinute, strategy.endMinute),
    qqqIntradayReturn: benchmarkReturn(dayBars, 'QQQ', strategy.startMinute, strategy.endMinute),
  });
}

function summarizeState(state, initialCapital) {
  const totalReturn = (state.equity / initialCapital) - 1;
  const dailyVolatility = standardDeviation(state.dailyReturns) * Math.sqrt(252);
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
    annualizedVolatility: dailyVolatility,
    sharpe: dailyVolatility > 0 ? (avgDaily * 252) / dailyVolatility : 0,
    tradingDays: state.dailyReturns.length,
    activeDays: state.activeDays,
    winRate: state.dailyReturns.length ? state.dailyReturns.filter((value) => value > 0).length / state.dailyReturns.length : 0,
    averageTradesPerDay: state.dailyReturns.length ? state.tradeCount / state.dailyReturns.length : 0,
    averageTradeEventsPerDay: state.dailyReturns.length ? state.tradeEvents / state.dailyReturns.length : 0,
    averageDailyTurnover: state.dailyReturns.length ? state.totalTurnover / state.dailyReturns.length : 0,
    totalTurnover: state.totalTurnover,
    totalTrades: state.tradeCount,
    tradeEvents: state.tradeEvents,
    missingReturnEvents: state.missingReturnEvents,
  };
}

function buildTargetReport({ config, dailyBarsPath, scorePath, startDate, rsiMode }) {
  const resolvedScorePath = scorePath || defaultScorePath(config);
  const resolvedDailyBarsPath = dailyBarsPath || findLatestMassiveEodBarsPath();
  if (!resolvedDailyBarsPath || !fs.existsSync(resolvedDailyBarsPath)) {
    throw new Error('missing_massive_eod_bars: run npm run pym-v5:massive-eod-build first');
  }
  if (!fs.existsSync(resolvedScorePath)) throw new Error(`missing_score_snapshot:${resolvedScorePath}`);
  const score = JSON.parse(fs.readFileSync(resolvedScorePath, 'utf8'));
  const market = readDailyBarsJsonl(resolvedDailyBarsPath);
  return buildDailyRebalanceReport({
    market,
    score,
    startDate,
    rsiMode,
    initialCapital: 10000,
    transactionCostBps: 0,
    slippageBps: 0,
    source: {
      scorePath: resolvedScorePath,
      barsPath: resolvedDailyBarsPath,
    },
  });
}

async function runIntradaySuite(settings = {}) {
  const config = settings.config || loadConfig();
  const startDate = settings.startDate || '2025-01-02';
  const endDate = settings.endDate || '2026-05-06';
  const costBps = Number.isFinite(settings.costBps) ? settings.costBps : 4;
  const initialCapital = Number.isFinite(settings.initialCapital) ? settings.initialCapital : 10000;
  const rsiMode = settings.rsiMode || 'wilder';
  const strategies = settings.strategies || STRATEGIES;
  const targetReport = settings.targetReport || buildTargetReport({
    config,
    dailyBarsPath: settings.dailyBarsPath,
    scorePath: settings.scorePath,
    startDate,
    rsiMode,
  });
  const targetByDate = new Map(targetReport.snapshots.map((snapshot) => [snapshot.date, snapshot]));
  const days = openCalendarDays(config.roots.calendar, startDate, endDate);
  const states = strategies.map((strategy) => emptyState(strategy, initialCapital));
  const skippedDays = [];
  let previousSnapshot = null;

  for (const day of days) {
    const todaysSnapshot = targetByDate.get(day.date);
    if (!previousSnapshot) {
      previousSnapshot = todaysSnapshot || previousSnapshot;
      skippedDays.push({ date: day.date, reason: 'no_prior_eod_target' });
      continue;
    }
    const tickers = requiredTickersForDay(previousSnapshot, strategies);
    const dayBars = await readMinuteBarsForDay(config, day, tickers);
    const minutes = dayBars.minutes.filter((minute) => minute >= 570 && minute <= 959);
    if (!minutes.length) {
      skippedDays.push({ date: day.date, reason: 'missing_minute_bars' });
      previousSnapshot = todaysSnapshot || previousSnapshot;
      continue;
    }

    states.forEach((state) => {
      simulateStrategyDay({
        state,
        strategy: state.strategy,
        day,
        dayBars,
        minutes,
        targetSnapshot: previousSnapshot,
        costBps,
      });
    });

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
      rsiMode,
      timing: 'previous_eod_pym_target_intraday_flat_overnight',
    },
    targetSource: targetReport.source,
    skippedDays,
    summaries,
    strategies: states.map((state) => ({
      summary: summarizeState(state, initialCapital),
      daySummaries: state.daySummaries,
      equityCurve: state.equityCurve,
    })),
  };
}

module.exports = {
  buildTargetReport,
  runIntradaySuite,
  summarizeState,
  simulateStrategyDay,
  weightTurnover,
};
