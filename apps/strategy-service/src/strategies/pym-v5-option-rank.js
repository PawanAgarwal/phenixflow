const fs = require('node:fs');
const path = require('node:path');

const { readDailyBarsJsonl, tickerReturn } = require('../../../../projects/pym-v5-replication/src/backtest');
const { loadConfig, runtimePath } = require('../../../../projects/pym-v5-replication/src/config');
const { loadMassiveEnv } = require('../../../../projects/pym-v5-replication/src/env');
const { readOptionFeatureJsonl } = require('../../../../projects/pym-v5-replication/src/option-features');
const {
  OPTION_OVERLAY_STRATEGIES,
  cleanWeights,
  optionMomentumScore,
} = require('../../../../projects/pym-v5-replication/src/option-overlay-suite');
const {
  buildDailyRebalanceReport,
  defaultScorePath,
  findLatestMassiveEodBarsPath,
  weightsToHoldings,
} = require('../../../../projects/pym-v5-replication/src/rebalance-report');
const { collectTickers } = require('../../../../projects/pym-v5-replication/src/symphony');

const DEFAULT_STRATEGY_ID = 'grid_pym_option_rank_top8_zm0p5';
const SPY_PUT_PRESSURE_STRATEGY_ID = 'grid_pym_spy_put_z2p5_to_bil';

const OPTION_RANK_DESCRIPTION = 'Starts from the normal PYM V5 target portfolio, ranks those candidates by Massive option-flow momentum, and keeps the strongest top 8 names.';
const OPTION_RANK_RULE_SUMMARY = Object.freeze([
  'Start with the normal PYM V5 holdings for the day.',
  'Rank candidates by option-flow momentum z-scores.',
  'Hold the top 8 passing z >= -0.5, then renormalize weights.',
]);

const SPY_PUT_PRESSURE_RULE_SUMMARY = Object.freeze([
  'if SPY option put-pressure z-score >= 2.5: hold 100% BIL',
  'else: hold normal PYM V5 portfolio',
]);

function pct(value) {
  return Number.isFinite(value) ? value * 100 : null;
}

function mean(values) {
  return values.length ? values.reduce((sum, value) => sum + value, 0) / values.length : 0;
}

function standardDeviation(values) {
  if (values.length < 2) return 0;
  const avg = mean(values);
  return Math.sqrt(mean(values.map((value) => (value - avg) ** 2)));
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

function weightDeltaTurnover(previous, next) {
  const keys = new Set([...previous.keys(), ...next.keys()]);
  let turnover = 0;
  keys.forEach((ticker) => {
    turnover += Math.abs((next.get(ticker) || 0) - (previous.get(ticker) || 0));
  });
  return turnover;
}

function grossExposure(weights) {
  return [...weights.values()].reduce((sum, weight) => sum + weight, 0);
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

function parseRangeFromName(filePath, prefix) {
  const basename = path.basename(filePath || '');
  const match = basename.match(new RegExp(`^${prefix}-(\\d{4}-\\d{2}-\\d{2})-(\\d{4}-\\d{2}-\\d{2})\\.jsonl$`));
  return match ? { startDate: match[1], endDate: match[2] } : {};
}

function latestBarsMetadata(filePath) {
  if (!filePath || !fs.existsSync(filePath)) return null;
  const stats = fs.statSync(filePath);
  return {
    path: filePath,
    name: path.basename(filePath),
    ...parseRangeFromName(filePath, 'pym-v5-massive-eod-adjusted-daily-bars'),
    updatedAt: stats.mtime.toISOString(),
  };
}

function safeReadJson(filePath) {
  try {
    return JSON.parse(fs.readFileSync(filePath, 'utf8'));
  } catch {
    return null;
  }
}

function optionFeaturesMetadata(filePath) {
  if (!filePath || !fs.existsSync(filePath)) return null;
  const stats = fs.statSync(filePath);
  const manifestPath = filePath.replace(/\.jsonl$/, '.manifest.json');
  const manifest = fs.existsSync(manifestPath) ? safeReadJson(manifestPath) : null;
  return {
    path: filePath,
    name: path.basename(filePath),
    ...parseRangeFromName(filePath, 'pym-v5-option-bar-features'),
    updatedAt: stats.mtime.toISOString(),
    manifestPath: manifest ? manifestPath : null,
    processedDays: manifest?.processedDays ?? null,
    totalRowsRead: manifest?.totalRowsRead ?? null,
    totalRowsUsed: manifest?.totalRowsUsed ?? null,
    missingFileDays: manifest?.missingFileDays || [],
  };
}

function findLatestOptionFeaturesPath() {
  const explicit = process.env.PYM_V5_OPTION_FEATURES_PATH;
  if (explicit) return path.resolve(explicit);
  const runtimeRoot = runtimePath();
  if (!fs.existsSync(runtimeRoot)) return null;
  const matches = fs.readdirSync(runtimeRoot)
    .map((name) => {
      const match = name.match(/^pym-v5-option-bar-features-(\d{4}-\d{2}-\d{2})-(\d{4}-\d{2}-\d{2})\.jsonl$/);
      return match ? { name, startDate: match[1], endDate: match[2] } : null;
    })
    .filter(Boolean);
  if (!matches.length) return null;
  const latestEndDate = matches.reduce((latest, item) => (item.endDate > latest ? item.endDate : latest), matches[0].endDate);
  const latestCandidates = matches
    .filter((item) => item.endDate === latestEndDate)
    .sort((left, right) => left.startDate.localeCompare(right.startDate) || left.name.localeCompare(right.name));
  return path.join(runtimeRoot, latestCandidates[0].name);
}

function firstDateOnOrAfter(dates, startDate) {
  const index = dates.findIndex((date) => date >= startDate);
  return index === -1 ? null : index;
}

function optionUniverseFromScore(score, market) {
  const marketTickers = new Set(market.tickers);
  return [...collectTickers(score)]
    .filter((ticker) => marketTickers.has(ticker))
    .sort();
}

function buildTargetReport({ market, score, scorePath, startDate, rsiMode, initialCapital }) {
  return buildDailyRebalanceReport({
    market,
    score,
    startDate,
    rsiMode,
    initialCapital,
    transactionCostBps: 0,
    slippageBps: 0,
    source: { scorePath },
  });
}

function buildOptionRankReport({
  metadata,
  market,
  score,
  scorePath,
  barsPath,
  optionFeaturesPath,
  startDate,
  endDate,
  rsiMode,
  initialCapital,
  transactionCostBps,
  slippageBps,
  optionStrategyId,
  generatedAt = new Date().toISOString(),
}) {
  const optionStrategy = OPTION_OVERLAY_STRATEGIES.find((strategy) => strategy.id === optionStrategyId);
  if (!optionStrategy) throw new Error(`missing_option_overlay_strategy:${optionStrategyId}`);

  const targetReport = buildTargetReport({ market, score, scorePath, startDate, rsiMode, initialCapital });
  const targetByDate = new Map(targetReport.snapshots.map((snapshot) => [snapshot.date, snapshot]));
  const optionRows = readOptionFeatureJsonl(optionFeaturesPath);
  const featuresByDate = new Map(optionRows.map((row) => [row.date, row]));
  const optionUniverse = optionUniverseFromScore(score, market);
  const totalCostBps = (transactionCostBps || 0) + (slippageBps || 0);
  const firstIndex = firstDateOnOrAfter(market.dates, startDate);
  if (firstIndex === null) throw new Error(`no_market_dates_on_or_after:${startDate}`);

  const snapshots = [];
  const equitySeries = [];
  const dailyReturns = [];
  const skippedDays = [];
  let equity = initialCapital;
  let previousWeights = new Map();
  let firstSignalIndex = null;
  let totalTurnover = 0;
  let investedDays = 0;
  let missingReturnEventCount = 0;

  for (let signalIndex = firstIndex; signalIndex < market.dates.length; signalIndex += 1) {
    const signalDate = market.dates[signalIndex];
    if (signalDate > endDate) break;
    const targetSnapshot = targetByDate.get(signalDate);
    if (!targetSnapshot) {
      skippedDays.push({ date: signalDate, reason: 'missing_pym_target' });
      continue;
    }
    const optionFeatures = featuresByDate.get(signalDate);
    if (!optionFeatures) {
      skippedDays.push({ date: signalDate, reason: 'missing_option_features' });
      continue;
    }
    if (firstSignalIndex === null) firstSignalIndex = signalIndex;

    const context = {
      market,
      signalIndex,
      signalDate,
      targetSnapshot,
      optionFeatures,
      optionUniverse,
    };
    const desired = cleanWeights(optionStrategy.weights(context));
    const turnover = weightDeltaTurnover(previousWeights, desired);
    const costReturn = turnover * totalCostBps / 10000;
    const holdings = weightsToHoldings(desired, equity, previousWeights);
    const exposure = grossExposure(desired);
    const nextDate = market.dates[signalIndex + 1] || null;
    const snapshot = {
      date: signalDate,
      rebalanceDate: signalDate,
      execution: 'eod_close',
      nextDate,
      equityBeforeNextSession: equity,
      grossExposure: exposure,
      turnover,
      turnoverPct: pct(turnover),
      estimatedRebalanceCost: equity * costReturn,
      estimatedRebalanceCostPct: pct(costReturn),
      holdings,
      topHoldings: topHoldingsLabel(holdings),
      benchmarkReturns: {
        spy: benchmarkPoint(market, 'SPY', firstSignalIndex, signalIndex),
        qqq: benchmarkPoint(market, 'QQQ', firstSignalIndex, signalIndex),
      },
      optionDiagnostics: {
        spyOptionScore: optionMomentumScore(context, 'SPY'),
        qqqOptionScore: optionMomentumScore(context, 'QQQ'),
      },
      realized: null,
    };

    if (nextDate) {
      const { grossReturn, missing } = portfolioReturn(desired, market, signalIndex + 1);
      const netReturn = grossReturn - costReturn;
      const startEquity = equity;
      equity *= (1 + netReturn);
      totalTurnover += turnover;
      missingReturnEventCount += missing.length;
      if (exposure > 0.001) investedDays += 1;
      dailyReturns.push(netReturn);
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
        signalDate,
        equity,
        dailyReturn: netReturn,
        totalReturn: (equity / initialCapital) - 1,
        spyReturn: benchmarkPoint(market, 'SPY', firstSignalIndex, signalIndex + 1),
        qqqReturn: benchmarkPoint(market, 'QQQ', firstSignalIndex, signalIndex + 1),
      });
    }

    snapshots.push(snapshot);
    previousWeights = desired;
  }

  const completedSnapshots = snapshots.filter((snapshot) => snapshot.realized);
  const latest = snapshots.at(-1) || null;
  const latestCompleted = completedSnapshots.at(-1) || null;
  const finalEquity = latestCompleted?.realized?.endEquity ?? initialCapital;
  const totalReturn = (finalEquity / initialCapital) - 1;
  const annualizedVolatility = standardDeviation(dailyReturns) * Math.sqrt(252);
  const avgDailyReturn = mean(dailyReturns);
  const drawdown = maxDrawdown(equitySeries);

  return {
    generatedAt,
    source: {
      ...metadata,
      scorePath,
      bars: latestBarsMetadata(barsPath),
      optionFeatures: optionFeaturesMetadata(optionFeaturesPath),
      optionOverlayStrategy: {
        id: optionStrategy.id,
        name: optionStrategy.name,
        family: optionStrategy.family,
        description: optionStrategy.description,
      },
      note: 'Uses local Massive option aggregate bars. Historical Greeks/open-interest files were not present locally, so option pressure is derived from option-flow proxies.',
    },
    settings: {
      startDate,
      endDate,
      rsiMode,
      timing: 'option_flow_and_pym_signal_at_day_x_eod_then_next_close',
      initialCapital,
      transactionCostBps,
      slippageBps,
      optionRollingWindow: 20,
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
      maxDrawdown: drawdown,
      maxDrawdownPct: pct(drawdown),
      annualizedVolatility,
      annualizedVolatilityPct: pct(annualizedVolatility),
      sharpe: annualizedVolatility > 0 ? (avgDailyReturn * 252) / annualizedVolatility : 0,
      investedDays,
      investedShare: completedSnapshots.length ? investedDays / completedSnapshots.length : 0,
      averageDailyTurnover: completedSnapshots.length ? totalTurnover / completedSnapshots.length : 0,
      missingReturnEventCount,
      skippedDays: skippedDays.length,
      spyReturn: equitySeries.at(-1)?.spyReturn ?? null,
      qqqReturn: equitySeries.at(-1)?.qqqReturn ?? null,
    },
    latest,
    snapshots,
    equitySeries,
    skippedDays,
  };
}

function createPymV5OptionOverlayStrategy(options = {}) {
  loadMassiveEnv();
  const config = options.config || loadConfig();
  const optionStrategyId = options.optionStrategyId || (
    options.useOptionStudyEnv === false ? DEFAULT_STRATEGY_ID : process.env.PYM_V5_OPTION_STUDY_ID
  ) || DEFAULT_STRATEGY_ID;
  const metadata = {
    id: options.id || 'pym-v5-option-rank-top8',
    name: options.name || 'PYM Option-Rank Top 8',
    displayName: options.displayName || options.name || 'PYM Option-Rank Top 8',
    family: options.family || 'pym_option_flow',
    dataProvider: options.dataProvider || 'Massive adjusted EOD + Massive option aggregates',
    strategySource: options.strategySource || 'PYM V5 Composer tree plus local option-flow ranking overlay',
    description: options.description || null,
    ruleSummary: options.ruleSummary || [],
    sourceLinks: options.sourceLinks || [],
  };

  const state = {
    report: null,
    loadedAt: null,
    refresh: null,
  };

  function getMetadata() {
    return {
      id: metadata.id,
      name: metadata.name,
      displayName: metadata.displayName,
      family: metadata.family,
      cadence: 'daily_eod',
      actionType: 'rebalance',
      dataProvider: metadata.dataProvider,
      strategySource: metadata.strategySource,
      description: metadata.description,
      ruleSummary: metadata.ruleSummary,
      sourceLinks: metadata.sourceLinks,
      composerSymphonyId: config.source.composerSymphonyId,
      baseStrategyId: 'pym-v5',
      optionOverlayStrategyId: optionStrategyId,
      defaultStartDate: options.startDate || process.env.PYM_V5_REBALANCE_START || '2025-01-01',
      supports: ['chart', 'values', 'latest_portfolio', 'portfolio_change'],
    };
  }

  function resolvePaths() {
    return {
      scorePath: options.scorePath || process.env.PYM_V5_SCORE_PATH || defaultScorePath(config),
      barsPath: options.barsPath || process.env.PYM_V5_DAILY_BARS_PATH || findLatestMassiveEodBarsPath(),
      optionFeaturesPath: options.optionFeaturesPath || process.env.PYM_V5_OPTION_FEATURES_PATH || findLatestOptionFeaturesPath(),
    };
  }

  function recompute() {
    const { scorePath, barsPath, optionFeaturesPath } = resolvePaths();
    if (!scorePath || !fs.existsSync(scorePath)) throw new Error(`missing_score_snapshot:${scorePath}`);
    if (!barsPath || !fs.existsSync(barsPath)) throw new Error('missing_massive_eod_bars: mount runtime data or run pym-v5:massive-eod-build');
    if (!optionFeaturesPath || !fs.existsSync(optionFeaturesPath)) {
      throw new Error('missing_option_features: mount runtime data or run pym-v5:build-option-features');
    }
    const score = JSON.parse(fs.readFileSync(scorePath, 'utf8'));
    const market = readDailyBarsJsonl(barsPath);
    const featureMetadata = optionFeaturesMetadata(optionFeaturesPath);
    const marketLastDate = market.dates.at(-1);
    const featureEndDate = featureMetadata?.endDate || marketLastDate;
    const report = buildOptionRankReport({
      metadata: getMetadata(),
      market,
      score,
      scorePath,
      barsPath,
      optionFeaturesPath,
      startDate: options.startDate || process.env.PYM_V5_REBALANCE_START || '2025-01-01',
      endDate: options.endDate || process.env.PYM_V5_OPTION_STUDY_END || (featureEndDate < marketLastDate ? featureEndDate : marketLastDate),
      rsiMode: options.rsiMode || process.env.PYM_V5_RSI_MODE || 'wilder',
      initialCapital: config.execution.initialCapital,
      transactionCostBps: config.execution.transactionCostBps,
      slippageBps: config.execution.slippageBps,
      optionStrategyId,
    });
    state.report = report;
    state.loadedAt = new Date().toISOString();
    return report;
  }

  function getReport() {
    if (!state.report) recompute();
    return state.report;
  }

  function refreshData() {
    const { refreshEodInputsStep, runRefreshSequence } = require('./refresh-helpers');
    return runRefreshSequence(state, [
      refreshEodInputsStep({ label: 'refresh-eod-option-inputs', withOptionFeatures: true }),
    ], recompute);
  }

  return {
    state,
    getMetadata,
    getReport,
    recompute,
    refreshData,
  };
}

function createPymV5OptionRankStrategy(options = {}) {
  return createPymV5OptionOverlayStrategy({
    ...options,
    description: options.description || OPTION_RANK_DESCRIPTION,
    ruleSummary: options.ruleSummary || OPTION_RANK_RULE_SUMMARY,
  });
}

function createPymV5SpyPutPressureStrategy(options = {}) {
  return createPymV5OptionOverlayStrategy({
    ...options,
    id: options.id || 'pym-v5-spy-put-pressure-bil',
    name: options.name || 'PYM SPY Put-Pressure to BIL',
    displayName: options.displayName || 'PYM SPY Put-Pressure',
    family: options.family || 'pym_option_risk_overlay',
    strategySource: options.strategySource || 'PYM V5 Composer tree plus SPY option put-pressure risk-off overlay',
    description: options.description || 'Holds the replicated PYM V5 ETF portfolio unless SPY option put-pressure z-score is at least 2.5, then rotates to BIL at EOD.',
    ruleSummary: options.ruleSummary || SPY_PUT_PRESSURE_RULE_SUMMARY,
    optionStrategyId: options.optionStrategyId || SPY_PUT_PRESSURE_STRATEGY_ID,
    useOptionStudyEnv: false,
  });
}

module.exports = {
  DEFAULT_STRATEGY_ID,
  SPY_PUT_PRESSURE_STRATEGY_ID,
  buildOptionRankReport,
  createPymV5OptionOverlayStrategy,
  createPymV5OptionRankStrategy,
  createPymV5SpyPutPressureStrategy,
  findLatestOptionFeaturesPath,
  optionFeaturesMetadata,
};
