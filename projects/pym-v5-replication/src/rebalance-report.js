const fs = require('node:fs');
const path = require('node:path');

const { artifactPath, runtimePath } = require('./config');
const { tickerReturn } = require('./backtest');
const { evaluateSymphony } = require('./symphony');

const DEFAULT_RSI_MODE = 'wilder';
const DEFAULT_START_DATE = '2025-01-01';

function pct(value) {
  return Number.isFinite(value) ? value * 100 : null;
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

function weightsToHoldings(weights, equity, previousWeights = new Map()) {
  return [...weights.entries()]
    .map(([ticker, weight]) => ({
      ticker,
      weight,
      weightPct: pct(weight),
      previousWeight: previousWeights.get(ticker) || 0,
      weightChange: weight - (previousWeights.get(ticker) || 0),
      weightChangePct: pct(weight - (previousWeights.get(ticker) || 0)),
      dollars: Number.isFinite(equity) ? equity * weight : null,
    }))
    .sort((left, right) => right.weight - left.weight || left.ticker.localeCompare(right.ticker));
}

function topHoldingsLabel(holdings, limit = 4) {
  return holdings.slice(0, limit).map((holding) => holding.ticker).join(', ');
}

function benchmarkPoint(market, ticker, baseIndex, index) {
  const closes = market.closes.get(ticker);
  if (!closes) return null;
  const base = closes[baseIndex];
  const current = closes[index];
  if (!Number.isFinite(base) || !Number.isFinite(current) || base <= 0) return null;
  return (current / base) - 1;
}

function portfolioReturn(weights, market, nextIndex) {
  let grossReturn = 0;
  const missing = [];
  weights.forEach((weight, ticker) => {
    const ret = tickerReturn(market.closes, ticker, nextIndex);
    if (ret === null) {
      missing.push({ ticker, weight });
      return;
    }
    grossReturn += weight * ret;
  });
  return { grossReturn, missing };
}

function firstSignalIndexOnOrAfter(dates, startDate) {
  const index = dates.findIndex((date) => date >= startDate);
  return index === -1 ? null : index;
}

function maxDrawdown(equitySeries) {
  let peak = equitySeries[0]?.equity || 1;
  let drawdown = 0;
  equitySeries.forEach((point) => {
    if (point.equity > peak) peak = point.equity;
    if (peak > 0) drawdown = Math.min(drawdown, (point.equity / peak) - 1);
  });
  return drawdown;
}

function buildDailyRebalanceReport({
  market,
  score,
  startDate = DEFAULT_START_DATE,
  rsiMode = DEFAULT_RSI_MODE,
  initialCapital = 10000,
  transactionCostBps = 1,
  slippageBps = 1,
  generatedAt = new Date().toISOString(),
  source = {},
} = {}) {
  if (!market?.dates?.length) throw new Error('missing_market_dates');
  if (!score) throw new Error('missing_composer_score');
  const startIndex = firstSignalIndexOnOrAfter(market.dates, startDate);
  if (startIndex === null) throw new Error(`no_market_dates_on_or_after:${startDate}`);

  const totalCostBps = (transactionCostBps || 0) + (slippageBps || 0);
  const snapshots = [];
  const equitySeries = [];
  let equity = initialCapital;
  let previousWeights = new Map();
  let totalTurnover = 0;
  let investedDays = 0;
  let missingReturnEventCount = 0;

  for (let index = startIndex; index < market.dates.length; index += 1) {
    const date = market.dates[index];
    const nextDate = market.dates[index + 1] || null;
    const weights = cleanWeights(evaluateSymphony(score, market, index, { rsiMode }));
    const holdings = weightsToHoldings(weights, equity, previousWeights);
    const grossExposure = [...weights.values()].reduce((sum, weight) => sum + weight, 0);
    const turnover = weightDeltaTurnover(previousWeights, weights);
    const costReturn = turnover * totalCostBps / 10000;
    const estimatedRebalanceCost = equity * costReturn;
    const snapshot = {
      date,
      rebalanceDate: date,
      execution: 'eod_close',
      nextDate,
      equityBeforeNextSession: equity,
      grossExposure,
      turnover,
      turnoverPct: pct(turnover),
      estimatedRebalanceCost,
      estimatedRebalanceCostPct: pct(costReturn),
      holdings,
      topHoldings: topHoldingsLabel(holdings),
      benchmarkReturns: {
        spy: benchmarkPoint(market, 'SPY', startIndex, index),
        qqq: benchmarkPoint(market, 'QQQ', startIndex, index),
      },
      realized: null,
    };

    if (nextDate) {
      const { grossReturn, missing } = portfolioReturn(weights, market, index + 1);
      missingReturnEventCount += missing.length;
      const netReturn = grossReturn - costReturn;
      const startEquity = equity;
      equity *= (1 + netReturn);
      totalTurnover += turnover;
      if (grossExposure > 0.001) investedDays += 1;
      snapshot.realized = {
        date: nextDate,
        startEquity,
        endEquity: equity,
        grossReturn,
        grossReturnPct: pct(grossReturn),
        netReturn,
        netReturnPct: pct(netReturn),
        costReturn,
        costReturnPct: pct(costReturn),
        missingReturnCount: missing.length,
      };
      equitySeries.push({
        date: nextDate,
        signalDate: date,
        equity,
        totalReturn: (equity / initialCapital) - 1,
        spyReturn: benchmarkPoint(market, 'SPY', startIndex, index + 1),
        qqqReturn: benchmarkPoint(market, 'QQQ', startIndex, index + 1),
      });
    }

    snapshots.push(snapshot);
    previousWeights = weights;
  }

  const completedSnapshots = snapshots.filter((snapshot) => snapshot.realized);
  const latest = snapshots.at(-1);
  const latestCompleted = completedSnapshots.at(-1) || null;
  const finalEquity = latestCompleted?.realized?.endEquity ?? initialCapital;
  const totalReturn = (finalEquity / initialCapital) - 1;
  return {
    generatedAt,
    source,
    settings: {
      startDate,
      rsiMode,
      timing: 'signal_eod_close_then_next_close',
      initialCapital,
      transactionCostBps,
      slippageBps,
    },
    summary: {
      startDate,
      firstRebalanceDate: snapshots[0]?.date || null,
      latestRebalanceDate: latest?.date || null,
      latestCompletedDate: latestCompleted?.realized?.date || null,
      snapshots: snapshots.length,
      completedSessions: completedSnapshots.length,
      finalEquity,
      totalReturn,
      totalReturnPct: pct(totalReturn),
      maxDrawdown: maxDrawdown(equitySeries),
      maxDrawdownPct: pct(maxDrawdown(equitySeries)),
      investedDays,
      investedShare: completedSnapshots.length ? investedDays / completedSnapshots.length : 0,
      averageDailyTurnover: completedSnapshots.length ? totalTurnover / completedSnapshots.length : 0,
      missingReturnEventCount,
      spyReturn: equitySeries.at(-1)?.spyReturn ?? null,
      qqqReturn: equitySeries.at(-1)?.qqqReturn ?? null,
    },
    latest,
    snapshots,
    equitySeries,
  };
}

function findLatestMassiveEodBarsPath() {
  const explicit = process.env.PYM_V5_DAILY_BARS_PATH;
  if (explicit) return path.resolve(explicit);
  const runtimeRoot = runtimePath();
  if (!fs.existsSync(runtimeRoot)) return null;
  const matches = fs.readdirSync(runtimeRoot)
    .map((name) => {
      const match = name.match(/^pym-v5-massive-eod-adjusted-daily-bars-(\d{4}-\d{2}-\d{2})-(\d{4}-\d{2}-\d{2})\.jsonl$/);
      return match ? { name, startDate: match[1], endDate: match[2] } : null;
    })
    .filter(Boolean)
    .sort((left, right) => (
      left.endDate.localeCompare(right.endDate)
      || left.startDate.localeCompare(right.startDate)
      || left.name.localeCompare(right.name)
    ));
  return matches.length ? path.join(runtimeRoot, matches.at(-1).name) : null;
}

function defaultScorePath(config) {
  return runtimePath('source', `composer-${config.source.composerSymphonyId}-score.json`);
}

function defaultReportPath() {
  return artifactPath('pym-v5-rebalance-report.json');
}

function serializeSnapshotForList(snapshot) {
  return {
    date: snapshot.date,
    nextDate: snapshot.nextDate,
    equityBeforeNextSession: snapshot.equityBeforeNextSession,
    turnover: snapshot.turnover,
    turnoverPct: snapshot.turnoverPct,
    grossExposure: snapshot.grossExposure,
    topHoldings: snapshot.topHoldings,
    holdingCount: snapshot.holdings.length,
    netReturn: snapshot.realized?.netReturn ?? null,
    netReturnPct: snapshot.realized?.netReturnPct ?? null,
    endEquity: snapshot.realized?.endEquity ?? null,
    spyReturn: snapshot.benchmarkReturns.spy,
    qqqReturn: snapshot.benchmarkReturns.qqq,
  };
}

module.exports = {
  buildDailyRebalanceReport,
  findLatestMassiveEodBarsPath,
  defaultScorePath,
  defaultReportPath,
  serializeSnapshotForList,
  weightsToHoldings,
};
