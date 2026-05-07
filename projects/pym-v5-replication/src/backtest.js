const fs = require('node:fs');

const SPLIT_CANDIDATES = Object.freeze([
  1 / 20,
  1 / 15,
  1 / 12,
  1 / 10,
  1 / 8,
  1 / 5,
  1 / 4,
  1 / 3,
  1 / 2,
  2,
  3,
  4,
  5,
  8,
  10,
  12,
  15,
  20,
]);

function detectSplitRatio(previousClose, currentClose) {
  if (!Number.isFinite(previousClose) || !Number.isFinite(currentClose) || previousClose <= 0 || currentClose <= 0) return null;
  const rawRatio = currentClose / previousClose;
  if (Math.abs(rawRatio - 1) < 0.4) return null;
  let best = null;
  SPLIT_CANDIDATES.forEach((candidate) => {
    const correctedReturn = (currentClose / (previousClose * candidate)) - 1;
    const candidateDistance = Math.abs(Math.log(rawRatio / candidate));
    if (Math.abs(correctedReturn) > 0.25 || candidateDistance > 0.12) return;
    if (!best || candidateDistance < best.candidateDistance) {
      best = { ratio: candidate, rawRatio, correctedReturn, candidateDistance };
    }
  });
  return best;
}

function adjustedClosesForTicker(dates, byDate, ticker) {
  const raw = dates.map((date) => byDate.get(date)?.get(ticker)?.close ?? null);
  const adjusted = [];
  const splitAdjustments = [];
  let factor = 1;
  for (let index = 0; index < raw.length; index += 1) {
    const previous = raw[index - 1];
    const current = raw[index];
    const split = index > 0 ? detectSplitRatio(previous, current) : null;
    if (split) {
      factor *= split.ratio;
      splitAdjustments.push({
        ticker,
        date: dates[index],
        previousDate: dates[index - 1],
        previousClose: previous,
        currentClose: current,
        detectedRatio: split.ratio,
        rawRatio: split.rawRatio,
        correctedReturn: split.correctedReturn,
      });
    }
    adjusted.push(Number.isFinite(current) ? current / factor : null);
  }
  return { adjusted, splitAdjustments };
}

function readDailyBarsJsonl(filePath) {
  const rows = fs.readFileSync(filePath, 'utf8')
    .split('\n')
    .filter(Boolean)
    .map((line) => JSON.parse(line));
  const dates = [...new Set(rows.map((row) => row.date))].sort();
  const tickers = [...new Set(rows.map((row) => row.ticker))].sort();
  const byDate = new Map(dates.map((date) => [date, new Map()]));
  rows.forEach((row) => byDate.get(row.date)?.set(row.ticker, row));
  const closes = new Map();
  const splitAdjustments = [];
  tickers.forEach((ticker) => {
    const adjusted = adjustedClosesForTicker(dates, byDate, ticker);
    closes.set(ticker, adjusted.adjusted);
    splitAdjustments.push(...adjusted.splitAdjustments);
  });
  return { rows, dates, tickers, byDate, closes, splitAdjustments };
}

function cleanWeights(weights) {
  const out = new Map();
  weights.forEach((weight, ticker) => {
    if (Number.isFinite(weight) && weight > 1e-10) out.set(ticker, weight);
  });
  return out;
}

function weightDeltaTurnover(previous, next) {
  const keys = new Set([...previous.keys(), ...next.keys()]);
  let turnover = 0;
  keys.forEach((ticker) => {
    turnover += Math.abs((next.get(ticker) || 0) - (previous.get(ticker) || 0));
  });
  return turnover;
}

function tickerReturn(closes, ticker, index) {
  const values = closes.get(ticker) || [];
  const previous = values[index - 1];
  const current = values[index];
  if (!Number.isFinite(previous) || !Number.isFinite(current) || previous <= 0) return null;
  return (current / previous) - 1;
}

function maxDrawdownFromEquity(equityCurve) {
  let peak = equityCurve[0]?.equity || 1;
  let maxDrawdown = 0;
  equityCurve.forEach((point) => {
    if (point.equity > peak) peak = point.equity;
    if (peak > 0) maxDrawdown = Math.min(maxDrawdown, (point.equity / peak) - 1);
  });
  return maxDrawdown;
}

function mean(values) {
  if (!values.length) return 0;
  return values.reduce((sum, value) => sum + value, 0) / values.length;
}

function standardDeviation(values) {
  if (values.length < 2) return 0;
  const avg = mean(values);
  return Math.sqrt(mean(values.map((value) => (value - avg) ** 2)));
}

function monthlyReturns(equityCurve) {
  const months = new Map();
  equityCurve.forEach((point) => {
    const month = point.date.slice(0, 7);
    if (!months.has(month)) months.set(month, { startEquity: point.startEquity, endEquity: point.equity });
    months.get(month).endEquity = point.equity;
  });
  return [...months.entries()].map(([month, value]) => ({
    month,
    return: value.startEquity > 0 ? (value.endEquity / value.startEquity) - 1 : 0,
  }));
}

function topAverageWeights(weightSnapshots, limit = 15) {
  const totals = new Map();
  weightSnapshots.forEach((weights) => {
    weights.forEach((weight, ticker) => totals.set(ticker, (totals.get(ticker) || 0) + weight));
  });
  const count = Math.max(1, weightSnapshots.length);
  return [...totals.entries()]
    .map(([ticker, total]) => ({ ticker, averageWeight: total / count }))
    .sort((left, right) => right.averageWeight - left.averageWeight)
    .slice(0, limit);
}

function benchmarkReturn(closes, ticker, startIndex, endIndex) {
  const values = closes.get(ticker) || [];
  const start = values[startIndex - 1];
  const end = values[endIndex];
  if (!Number.isFinite(start) || !Number.isFinite(end) || start <= 0) return null;
  return (end / start) - 1;
}

function runBacktest({ market, score, evaluateSymphony, startDate, timing, transactionCostBps, slippageBps, initialCapital }) {
  const totalCostBps = (transactionCostBps || 0) + (slippageBps || 0);
  const equityCurve = [];
  const dailyReturns = [];
  const missingReturnEvents = [];
  const weightSnapshots = [];
  let equity = initialCapital || 10000;
  let previousWeights = new Map();
  let totalTurnover = 0;
  let investedDays = 0;
  let firstTradeIndex = null;
  let lastTradeIndex = null;

  for (let index = 1; index < market.dates.length; index += 1) {
    const date = market.dates[index];
    if (date < startDate) continue;
    const signalIndex = timing === 'same_close' ? index : index - 1;
    const weights = cleanWeights(evaluateSymphony(score, market, signalIndex));
    const grossExposure = [...weights.values()].reduce((sum, weight) => sum + weight, 0);
    const turnover = weightDeltaTurnover(previousWeights, weights);
    let grossReturn = 0;
    weights.forEach((weight, ticker) => {
      const ret = tickerReturn(market.closes, ticker, index);
      if (ret === null) {
        missingReturnEvents.push({ date, ticker, weight });
        return;
      }
      grossReturn += weight * ret;
    });
    const costReturn = turnover * totalCostBps / 10000;
    const netReturn = grossReturn - costReturn;
    const startEquity = equity;
    equity *= (1 + netReturn);
    equityCurve.push({
      date,
      signalDate: market.dates[signalIndex],
      startEquity,
      equity,
      grossReturn,
      costReturn,
      netReturn,
      turnover,
      grossExposure,
      holdings: Object.fromEntries(weights),
    });
    dailyReturns.push(netReturn);
    weightSnapshots.push(weights);
    totalTurnover += turnover;
    if (grossExposure > 0.001) investedDays += 1;
    if (firstTradeIndex === null) firstTradeIndex = index;
    lastTradeIndex = index;
    previousWeights = weights;
  }

  const totalReturn = equityCurve.length ? (equity / (initialCapital || 10000)) - 1 : 0;
  const volatility = standardDeviation(dailyReturns) * Math.sqrt(252);
  const avgDaily = mean(dailyReturns);
  const cagr = equityCurve.length ? ((1 + totalReturn) ** (252 / equityCurve.length)) - 1 : 0;
  return {
    summary: {
      startDate,
      endDate: equityCurve.at(-1)?.date || null,
      timing,
      initialCapital: initialCapital || 10000,
      finalEquity: equity,
      totalReturn,
      cagr,
      maxDrawdown: maxDrawdownFromEquity(equityCurve),
      annualizedVolatility: volatility,
      sharpe: volatility > 0 ? (avgDaily * 252) / volatility : 0,
      tradingDays: equityCurve.length,
      investedDays,
      investedShare: equityCurve.length ? investedDays / equityCurve.length : 0,
      averageDailyTurnover: equityCurve.length ? totalTurnover / equityCurve.length : 0,
      totalTurnover,
      transactionCostBps,
      slippageBps,
      missingReturnEventCount: missingReturnEvents.length,
      spyBuyHoldReturn: firstTradeIndex === null ? null : benchmarkReturn(market.closes, 'SPY', firstTradeIndex, lastTradeIndex),
    },
    monthlyReturns: monthlyReturns(equityCurve),
    topAverageWeights: topAverageWeights(weightSnapshots),
    missingReturnEvents: missingReturnEvents.slice(0, 200),
    splitAdjustments: market.splitAdjustments || [],
    equityCurve,
  };
}

module.exports = {
  readDailyBarsJsonl,
  runBacktest,
  tickerReturn,
  detectSplitRatio,
};
