const fs = require('node:fs');
const path = require('node:path');

const { weightsToHoldings } = require('../../../../projects/pym-v5-replication/src/rebalance-report');

const REPO_ROOT = path.resolve(__dirname, '..', '..', '..', '..');
const DEFAULT_ML_REPORT_PATH = 'projects/pym-v5-ml-experiments/artifacts/pym-v5-daily-walkforward-micro-features-2025-02-01-2026-05-08.json';
const DEFAULT_OPTION_REPORT_PATH = 'projects/pym-v5-replication/artifacts/pym-v5-option-overlay-suite-grid-top8-zm0p5-2025-01-02-2026-05-08.json';
const DEFAULT_DATASET_PATH = 'projects/pym-v5-ml-experiments/artifacts/pym-v5-walkforward-dataset-micro-features-2025-01-02-2026-05-08.jsonl';
const DEFAULT_INITIAL_CAPITAL = 10000;
const DEFAULT_COST_BPS = 2;
const DEFAULT_TWO_SPEED_STRATEGY_ID = 'two_speed_attention_pym_light_governed';
const DEFAULT_OPTION_STRATEGY_ID = 'grid_pym_option_rank_top8_zm0p5';
const META_LOOKBACK = 21;
const SELECTOR_LOOKBACKS = Object.freeze([5, 10, 15, 21, 30, 42, 50, 63, 84, 126]);

const TWO_SPEED_RULE_SUMMARY = Object.freeze([
  'Train each day using only prior labeled trading days.',
  'Blend a long-memory model with a 63-day recent-memory model.',
  'Hold top predicted PYM candidates after the turnover governor clears the edge.',
]);

const META21_RULE_SUMMARY = Object.freeze([
  'Build candidate selectors between ML two-speed and option top-8.',
  'Each day pick the selector with the best prior 21-day realized score.',
  'Hold the chosen underlying strategy portfolio for the next session.',
]);

function resolvePath(filePath) {
  if (!filePath) return null;
  return path.isAbsolute(filePath) ? filePath : path.join(REPO_ROOT, filePath);
}

function finite(value, fallback = 0) {
  const number = Number(value);
  return Number.isFinite(number) ? number : fallback;
}

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

function cleanHoldings(holdings) {
  const out = new Map();
  Object.entries(holdings || {}).forEach(([ticker, weight]) => {
    const value = finite(weight);
    if (value > 1e-10) out.set(ticker, value);
  });
  return out;
}

function weightTurnover(previous, next) {
  const keys = new Set([...previous.keys(), ...next.keys()]);
  let total = 0;
  keys.forEach((ticker) => {
    total += Math.abs((next.get(ticker) || 0) - (previous.get(ticker) || 0));
  });
  return total;
}

function grossExposure(weights) {
  return [...weights.values()].reduce((sum, weight) => sum + weight, 0);
}

function topHoldingsLabel(holdings, limit = 4) {
  return holdings.slice(0, limit).map((holding) => holding.ticker).join(', ');
}

function fileMetadata(filePath) {
  if (!filePath || !fs.existsSync(filePath)) return null;
  const stats = fs.statSync(filePath);
  return {
    path: filePath,
    name: path.basename(filePath),
    updatedAt: stats.mtime.toISOString(),
  };
}

function readJson(filePath) {
  if (!filePath || !fs.existsSync(filePath)) throw new Error(`missing_artifact:${filePath}`);
  return JSON.parse(fs.readFileSync(filePath, 'utf8'));
}

function loadBenchmarkSamples(datasetPath) {
  if (!datasetPath || !fs.existsSync(datasetPath)) return new Map();
  const samples = new Map();
  fs.readFileSync(datasetPath, 'utf8').split(/\r?\n/).forEach((line) => {
    if (!line.trim()) return;
    const row = JSON.parse(line);
    if (row.type === 'sample') samples.set(row.date, row);
  });
  return samples;
}

function loadMlRawPoints(reportPath, strategyId) {
  const report = readJson(reportPath);
  const strategy = report.strategies?.[strategyId];
  if (!strategy?.equityCurve) throw new Error(`missing_ml_strategy:${strategyId}`);
  return {
    sourceReport: report,
    rawPoints: strategy.equityCurve.map((point) => ({
      signalDate: point.signalDate,
      date: point.date,
      grossReturn: finite(point.grossReturn, finite(point.netReturn) + finite(point.costReturn)),
      costReturn: finite(point.costReturn),
      netReturn: finite(point.netReturn),
      turnover: finite(point.turnover),
      holdings: point.holdings || {},
    })),
  };
}

function loadOptionRawPoints(reportPath, strategyId) {
  const report = readJson(reportPath);
  const strategy = (report.strategies || []).find((entry) => entry.summary?.id === strategyId);
  if (!strategy?.daySummaries) throw new Error(`missing_option_strategy:${strategyId}`);
  return {
    sourceReport: report,
    rawPoints: strategy.daySummaries.map((point) => ({
      signalDate: point.signalDate,
      date: point.realizedDate,
      grossReturn: finite(point.grossReturn, finite(point.netReturn) + finite(point.costReturn)),
      costReturn: finite(point.costReturn),
      netReturn: finite(point.netReturn),
      turnover: finite(point.turnover),
      holdings: point.holdings || {},
    })),
  };
}

function intersectDates(series) {
  const sets = series.map((item) => new Set(item.rawPoints.map((point) => point.date)));
  const [first, ...rest] = sets;
  return [...first].filter((date) => rest.every((set) => set.has(date))).sort();
}

function rawSeriesToPoints({ id, rawPoints, commonDates, costBps, initialCapital }) {
  const byDate = new Map(rawPoints.map((point) => [point.date, point]));
  let equity = initialCapital;
  let previousWeights = new Map();
  return commonDates.map((date) => {
    const raw = byDate.get(date);
    const weights = cleanHoldings(raw.holdings);
    const turnover = weightTurnover(previousWeights, weights);
    const costReturn = turnover * costBps / 10000;
    const grossReturn = finite(raw.grossReturn);
    const netReturn = grossReturn - costReturn;
    const startEquity = equity;
    equity *= 1 + netReturn;
    const point = {
      id,
      signalDate: raw.signalDate,
      date,
      startEquity,
      equity,
      grossReturn,
      costReturn,
      netReturn,
      turnover,
      holdings: Object.fromEntries(weights),
    };
    previousWeights = weights;
    return point;
  });
}

function recentStrategyScore(points, currentIndex, lookback) {
  const start = Number.isFinite(lookback) ? Math.max(0, currentIndex - lookback) : 0;
  const recent = points.slice(start, currentIndex);
  if (!recent.length) return 0;
  let logReturn = 0;
  let equity = 1;
  let peak = 1;
  let drawdown = 0;
  recent.forEach((point) => {
    logReturn += Math.log(Math.max(1e-9, 1 + point.netReturn));
    equity *= 1 + point.netReturn;
    peak = Math.max(peak, equity);
    drawdown = Math.min(drawdown, equity / peak - 1);
  });
  return logReturn + (0.5 * drawdown);
}

function buildTwoStrategySelector({ id, candidateSeries, candidateIds, commonDates, lookback, initialCapital, costBps, seedId }) {
  let equity = initialCapital;
  let previousWeights = new Map();
  const selectionCounts = {};
  const points = commonDates.map((date, index) => {
    let chosenId = seedId;
    if (index > 0) {
      chosenId = candidateIds
        .map((candidateId) => ({
          candidateId,
          score: recentStrategyScore(candidateSeries[candidateId], index, lookback),
        }))
        .sort((left, right) => right.score - left.score || left.candidateId.localeCompare(right.candidateId))[0].candidateId;
    }
    const raw = candidateSeries[chosenId][index];
    const weights = cleanHoldings(raw.holdings);
    const turnover = weightTurnover(previousWeights, weights);
    const costReturn = turnover * costBps / 10000;
    const grossReturn = finite(raw.grossReturn);
    const netReturn = grossReturn - costReturn;
    const startEquity = equity;
    equity *= 1 + netReturn;
    previousWeights = weights;
    selectionCounts[chosenId] = (selectionCounts[chosenId] || 0) + 1;
    return {
      id,
      signalDate: raw.signalDate,
      date,
      startEquity,
      equity,
      grossReturn,
      costReturn,
      netReturn,
      turnover,
      holdings: Object.fromEntries(weights),
      chosenStrategy: chosenId,
      lookback,
    };
  });
  return { points, selectionCounts };
}

function buildMeta21Selector({ candidateSelectors, selectorIds, commonDates, initialCapital, costBps, seedId }) {
  let equity = initialCapital;
  let previousWeights = new Map();
  const selectionCounts = {};
  const selectedLookbackCounts = {};
  const points = commonDates.map((date, index) => {
    let chosenSelectorId = seedId;
    if (index > 0) {
      chosenSelectorId = selectorIds
        .map((selectorId) => ({
          selectorId,
          score: recentStrategyScore(candidateSelectors[selectorId].points, index, META_LOOKBACK),
        }))
        .sort((left, right) => right.score - left.score || left.selectorId.localeCompare(right.selectorId))[0].selectorId;
    }
    const raw = candidateSelectors[chosenSelectorId].points[index];
    const weights = cleanHoldings(raw.holdings);
    const turnover = weightTurnover(previousWeights, weights);
    const costReturn = turnover * costBps / 10000;
    const grossReturn = finite(raw.grossReturn);
    const netReturn = grossReturn - costReturn;
    const startEquity = equity;
    equity *= 1 + netReturn;
    previousWeights = weights;
    selectionCounts[chosenSelectorId] = (selectionCounts[chosenSelectorId] || 0) + 1;
    selectedLookbackCounts[String(raw.lookback)] = (selectedLookbackCounts[String(raw.lookback)] || 0) + 1;
    return {
      id: 'walkforward_lookback_best_of_two_speed_or_option_meta21',
      signalDate: raw.signalDate,
      date,
      startEquity,
      equity,
      grossReturn,
      costReturn,
      netReturn,
      turnover,
      holdings: Object.fromEntries(weights),
      chosenSelector: chosenSelectorId,
      chosenStrategy: raw.chosenStrategy,
      chosenLookback: raw.lookback,
    };
  });
  return { points, selectionCounts, selectedLookbackCounts };
}

function summaryFromPoints(points, initialCapital, extra = {}) {
  const dailyReturns = points.map((point) => point.netReturn);
  const equitySeries = points.map((point) => ({ equity: point.equity }));
  const finalEquity = points.at(-1)?.equity || initialCapital;
  const totalReturn = finalEquity / initialCapital - 1;
  const annualizedVolatility = standardDeviation(dailyReturns) * Math.sqrt(252);
  const avgDailyReturn = mean(dailyReturns);
  const drawdown = maxDrawdown(equitySeries);
  const totalTurnover = points.reduce((sum, point) => sum + point.turnover, 0);
  return {
    startDate: points[0]?.signalDate || null,
    firstRebalanceDate: points[0]?.signalDate || null,
    latestRebalanceDate: points.at(-1)?.signalDate || null,
    latestCompletedDate: points.at(-1)?.date || null,
    snapshots: points.length,
    completedSessions: points.length,
    finalEquity,
    totalReturn,
    totalReturnPct: pct(totalReturn),
    maxDrawdown: drawdown,
    maxDrawdownPct: pct(drawdown),
    annualizedVolatility,
    annualizedVolatilityPct: pct(annualizedVolatility),
    sharpe: annualizedVolatility > 0 ? (avgDailyReturn * 252) / annualizedVolatility : 0,
    investedDays: points.filter((point) => grossExposure(cleanHoldings(point.holdings)) > 0.001).length,
    investedShare: points.length ? points.filter((point) => grossExposure(cleanHoldings(point.holdings)) > 0.001).length / points.length : 0,
    averageDailyTurnover: points.length ? totalTurnover / points.length : 0,
    missingReturnEventCount: 0,
    skippedDays: 0,
    spyReturn: null,
    qqqReturn: null,
    ...extra,
  };
}

function reportFromPoints({ metadata, points, samplesBySignalDate, source, settings, extraSummary = {} }) {
  const initialCapital = settings.initialCapital || DEFAULT_INITIAL_CAPITAL;
  let previousWeights = new Map();
  let spyEquity = 1;
  let qqqEquity = 1;
  const snapshots = [];
  const equitySeries = [];

  points.forEach((point) => {
    const weights = cleanHoldings(point.holdings);
    const holdings = weightsToHoldings(weights, point.startEquity, previousWeights);
    const sample = samplesBySignalDate.get(point.signalDate);
    if (sample?.nextReturns) {
      spyEquity *= 1 + finite(sample.nextReturns.SPY);
      qqqEquity *= 1 + finite(sample.nextReturns.QQQ);
    }
    const spyReturn = spyEquity - 1;
    const qqqReturn = qqqEquity - 1;
    const snapshot = {
      date: point.signalDate,
      rebalanceDate: point.signalDate,
      execution: 'eod_close',
      nextDate: point.date,
      equityBeforeNextSession: point.startEquity,
      grossExposure: grossExposure(weights),
      turnover: point.turnover,
      turnoverPct: pct(point.turnover),
      estimatedRebalanceCost: point.startEquity * point.costReturn,
      estimatedRebalanceCostPct: pct(point.costReturn),
      holdings,
      topHoldings: topHoldingsLabel(holdings),
      benchmarkReturns: {
        spy: spyReturn,
        qqq: qqqReturn,
      },
      selectorDiagnostics: {
        chosenStrategy: point.chosenStrategy || null,
        chosenSelector: point.chosenSelector || null,
        chosenLookback: point.chosenLookback || point.lookback || null,
      },
      realized: {
        date: point.date,
        startEquity: point.startEquity,
        endEquity: point.equity,
        grossReturn: point.grossReturn,
        grossReturnPct: pct(point.grossReturn),
        netReturn: point.netReturn,
        netReturnPct: pct(point.netReturn),
        costReturn: point.costReturn,
        costReturnPct: pct(point.costReturn),
        missingReturnCount: 0,
      },
    };
    snapshots.push(snapshot);
    equitySeries.push({
      date: point.date,
      signalDate: point.signalDate,
      equity: point.equity,
      dailyReturn: point.netReturn,
      totalReturn: (point.equity / initialCapital) - 1,
      spyReturn,
      qqqReturn,
    });
    previousWeights = weights;
  });

  const summary = summaryFromPoints(points, initialCapital, {
    spyReturn: equitySeries.at(-1)?.spyReturn ?? null,
    qqqReturn: equitySeries.at(-1)?.qqqReturn ?? null,
    ...extraSummary,
  });

  return {
    generatedAt: new Date().toISOString(),
    source,
    settings,
    summary,
    latest: snapshots.at(-1) || null,
    snapshots,
    equitySeries,
    skippedDays: [],
    metadata,
  };
}

function createArtifactStrategy(options = {}) {
  const metadata = {
    id: options.id,
    name: options.name,
    displayName: options.displayName || options.name,
    family: options.family || 'pym_ml_research',
    cadence: 'daily_eod',
    actionType: 'rebalance',
    dataProvider: 'Massive adjusted EOD + Massive option aggregates',
    strategySource: options.strategySource || 'PYM V5 ML walk-forward artifact',
    description: options.description || null,
    ruleSummary: options.ruleSummary || [],
    sourceLinks: options.sourceLinks || [],
    artifactStrategyId: options.artifactStrategyId || null,
    defaultStartDate: options.defaultStartDate || '2025-02-03',
    supports: ['chart', 'values', 'latest_portfolio', 'portfolio_change'],
  };
  const state = {
    report: null,
    loadedAt: null,
    refresh: null,
  };

  function getMetadata() {
    return metadata;
  }

  function getReport() {
    if (!state.report) {
      state.report = options.buildReport(getMetadata());
      state.loadedAt = new Date().toISOString();
    }
    return state.report;
  }

  function recompute() {
    state.report = options.buildReport(getMetadata());
    state.loadedAt = new Date().toISOString();
    return state.report;
  }

  return {
    state,
    getMetadata,
    getReport,
    recompute,
  };
}

function createPymV5MlTwoSpeedStrategy(options = {}) {
  const mlReportPath = resolvePath(options.mlReportPath || process.env.PYM_V5_ML_REPORT_PATH || DEFAULT_ML_REPORT_PATH);
  const datasetPath = resolvePath(options.datasetPath || process.env.PYM_V5_ML_DATASET_PATH || DEFAULT_DATASET_PATH);
  const strategyId = options.artifactStrategyId || DEFAULT_TWO_SPEED_STRATEGY_ID;
  return createArtifactStrategy({
    id: options.id || 'pym-v5-ml-two-speed-attention',
    name: options.name || 'PYM ML Two-Speed Attention',
    displayName: options.displayName || 'PYM ML Two-Speed',
    family: options.family || 'pym_ml_research',
    strategySource: 'PYM V5 daily walk-forward ML artifact',
    description: options.description || 'Daily walk-forward ridge return model using attention/PYM features with long-memory and recent-memory blending.',
    ruleSummary: options.ruleSummary || TWO_SPEED_RULE_SUMMARY,
    artifactStrategyId: strategyId,
    buildReport: (metadata) => {
      const { sourceReport, rawPoints } = loadMlRawPoints(mlReportPath, strategyId);
      const samplesBySignalDate = loadBenchmarkSamples(datasetPath);
      const initialCapital = sourceReport.settings?.initialCapital || DEFAULT_INITIAL_CAPITAL;
      const costBps = sourceReport.settings?.costBps || DEFAULT_COST_BPS;
      const points = rawSeriesToPoints({
        id: strategyId,
        rawPoints,
        commonDates: rawPoints.map((point) => point.date),
        costBps,
        initialCapital,
      });
      return reportFromPoints({
        metadata,
        points,
        samplesBySignalDate,
        source: {
          ...metadata,
          mlReport: fileMetadata(mlReportPath),
          dataset: fileMetadata(datasetPath),
          source: sourceReport.source,
        },
        settings: {
          ...sourceReport.settings,
          artifactStrategyId: strategyId,
          timing: sourceReport.settings?.timing || 'train_on_prior_labeled_days_signal_eod_close_then_next_close',
        },
      });
    },
  });
}

function createPymV5TwoSpeedOptionMeta21Strategy(options = {}) {
  const mlReportPath = resolvePath(options.mlReportPath || process.env.PYM_V5_ML_REPORT_PATH || DEFAULT_ML_REPORT_PATH);
  const optionReportPath = resolvePath(options.optionReportPath || process.env.PYM_V5_ML_OPTION_REPORT_PATH || DEFAULT_OPTION_REPORT_PATH);
  const datasetPath = resolvePath(options.datasetPath || process.env.PYM_V5_ML_DATASET_PATH || DEFAULT_DATASET_PATH);
  const twoSpeedId = options.twoSpeedStrategyId || DEFAULT_TWO_SPEED_STRATEGY_ID;
  const optionId = options.optionStrategyId || DEFAULT_OPTION_STRATEGY_ID;
  return createArtifactStrategy({
    id: options.id || 'pym-v5-two-speed-option-meta21',
    name: options.name || 'PYM Two-Speed / Option Meta21',
    displayName: options.displayName || 'PYM ML + Option Meta21',
    family: options.family || 'pym_selector_research',
    strategySource: 'PYM V5 ML walk-forward artifact plus option top-8 overlay artifact',
    description: options.description || 'Walk-forward selector that chooses among two-speed-vs-option lookback selectors using only the prior 21 realized trading days.',
    ruleSummary: options.ruleSummary || META21_RULE_SUMMARY,
    artifactStrategyId: 'walkforward_lookback_best_of_two_speed_or_option_meta21',
    buildReport: (metadata) => {
      const { sourceReport, rawPoints: twoRawPoints } = loadMlRawPoints(mlReportPath, twoSpeedId);
      const { rawPoints: optionRawPoints } = loadOptionRawPoints(optionReportPath, optionId);
      const samplesBySignalDate = loadBenchmarkSamples(datasetPath);
      const commonDates = intersectDates([
        { rawPoints: twoRawPoints },
        { rawPoints: optionRawPoints },
      ]);
      const initialCapital = sourceReport.settings?.initialCapital || DEFAULT_INITIAL_CAPITAL;
      const costBps = sourceReport.settings?.costBps || DEFAULT_COST_BPS;
      const twoPoints = rawSeriesToPoints({
        id: twoSpeedId,
        rawPoints: twoRawPoints,
        commonDates,
        costBps,
        initialCapital,
      });
      const optionPoints = rawSeriesToPoints({
        id: optionId,
        rawPoints: optionRawPoints,
        commonDates,
        costBps,
        initialCapital,
      });
      const candidateSeries = {
        [twoSpeedId]: twoPoints,
        [optionId]: optionPoints,
      };
      const candidateSelectors = {};
      SELECTOR_LOOKBACKS.forEach((lookback) => {
        candidateSelectors[`best_of_two_speed_or_option_recent${lookback}`] = buildTwoStrategySelector({
          id: `best_of_two_speed_or_option_recent${lookback}`,
          candidateSeries,
          candidateIds: [twoSpeedId, optionId],
          commonDates,
          lookback,
          initialCapital,
          costBps,
          seedId: twoSpeedId,
        });
      });
      const selectorIds = SELECTOR_LOOKBACKS.map((lookback) => `best_of_two_speed_or_option_recent${lookback}`);
      const meta = buildMeta21Selector({
        candidateSelectors,
        selectorIds,
        commonDates,
        initialCapital,
        costBps,
        seedId: 'best_of_two_speed_or_option_recent21',
      });
      return reportFromPoints({
        metadata,
        points: meta.points,
        samplesBySignalDate,
        source: {
          ...metadata,
          mlReport: fileMetadata(mlReportPath),
          optionReport: fileMetadata(optionReportPath),
          dataset: fileMetadata(datasetPath),
          underlyingStrategies: [twoSpeedId, optionId],
          selectorLookbacks: SELECTOR_LOOKBACKS,
        },
        settings: {
          ...sourceReport.settings,
          artifactStrategyId: 'walkforward_lookback_best_of_two_speed_or_option_meta21',
          underlyingStrategies: [twoSpeedId, optionId],
          selectorLookbacks: SELECTOR_LOOKBACKS,
          metaLookback: META_LOOKBACK,
          timing: 'EOD signal date X, close-to-close realized date X+1. Selector choices use only prior realized strategy returns.',
        },
        extraSummary: {
          selectionCounts: meta.selectionCounts,
          selectedLookbackCounts: meta.selectedLookbackCounts,
        },
      });
    },
  });
}

module.exports = {
  createPymV5MlTwoSpeedStrategy,
  createPymV5TwoSpeedOptionMeta21Strategy,
};
