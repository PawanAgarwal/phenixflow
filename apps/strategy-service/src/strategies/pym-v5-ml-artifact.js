const fs = require('node:fs');
const path = require('node:path');

const { weightsToHoldings } = require('../../../../projects/pym-v5-replication/src/rebalance-report');
const { dailyEodExecution } = require('./execution');

const REPO_ROOT = path.resolve(__dirname, '..', '..', '..', '..');
const DEFAULT_ML_ARTIFACT_DIR = 'projects/pym-v5-ml-experiments/artifacts';
const DEFAULT_OPTION_ARTIFACT_DIR = 'projects/pym-v5-replication/artifacts';
const DEFAULT_ML_REPORT_PATH = 'projects/pym-v5-ml-experiments/artifacts/pym-v5-daily-walkforward-micro-features-2025-02-01-2026-05-08.json';
const DEFAULT_OPTION_REPORT_PATH = 'projects/pym-v5-replication/artifacts/pym-v5-option-overlay-suite-grid-top8-zm0p5-2025-01-02-2026-05-08.json';
const DEFAULT_DATASET_PATH = 'projects/pym-v5-ml-experiments/artifacts/pym-v5-walkforward-dataset-micro-features-2025-01-02-2026-05-08.jsonl';
const DEFAULT_RISK_OVERLAY_REPORT_DIR = 'projects/pym-v5-ml-experiments/artifacts';
const DEFAULT_RISK_OVERLAY_REPORT_NAME = 'pym-v5-two-speed-risk-overlays-2025-02-01-2026-05-08.json';
const DEFAULT_INITIAL_CAPITAL = 10000;
const DEFAULT_COST_BPS = 2;
const DEFAULT_TWO_SPEED_STRATEGY_ID = 'two_speed_attention_pym_light_governed';
const DEFAULT_OPTION_STRATEGY_ID = 'grid_pym_option_rank_top8_zm0p5';
const DEFAULT_ML_OPTION_BLEND_STRATEGY_ID = 'blend_50_ml_50_option_top8';
const META_LOOKBACK = 21;
const SELECTOR_LOOKBACKS = Object.freeze([5, 10, 15, 21, 30, 42, 50, 63, 84, 126]);
const CALM_TREND_RAW_WEIGHT = 0.35;
const CALM_TREND_OPTION_WEIGHT = 0.65;
const CALM_TREND_MIN_PRIOR_SAMPLES = 40;
const CALM_TREND_STRESS_MAX = 0.25;
const CALM_TREND_SPY_RET_21_MIN = 0;
const CALM_TREND_QQQ_RET_21_MIN = 0;
const CALM_TREND_SPY_VOL_21_MAX = 0.22;

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

const ML_OPTION_BLEND_RULE_SUMMARY = Object.freeze([
  'Hold 50% two-speed ML target and 50% option top-8 target.',
  'Use day-X EOD ML holdings and Massive option-flow rankings available through that same day.',
  'Rebalance at EOD close and realize the close-to-close return into X+1.',
]);

const CALM_TREND_ROUTER_RULE_SUMMARY = Object.freeze([
  'If prior labeled training days >= 40 and calm-trend conditions hold: hold 35% raw ML and 65% option top-8.',
  'Calm-trend conditions: stress < 0.25, SPY 21d return > 0, QQQ 21d return > 0, and SPY 21d volatility < 22%.',
  'Otherwise hold the raw two-speed ML target unchanged.',
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

function normalizeWeights(weights) {
  const total = [...weights.values()].reduce((sum, weight) => sum + weight, 0);
  if (total <= 1e-10) return new Map([['BIL', 1]]);
  const out = new Map();
  weights.forEach((weight, ticker) => {
    const normalized = weight / total;
    if (normalized > 1e-10) out.set(ticker, normalized);
  });
  return out;
}

function addScaledWeights(target, source, scale) {
  cleanHoldings(source).forEach((weight, ticker) => {
    const value = weight * scale;
    if (Math.abs(value) > 1e-12) target.set(ticker, (target.get(ticker) || 0) + value);
  });
  return target;
}

function blendHoldings(left, right, leftWeight) {
  const weight = Math.max(0, Math.min(1, finite(leftWeight)));
  const out = new Map();
  addScaledWeights(out, left, weight);
  addScaledWeights(out, right, 1 - weight);
  return normalizeWeights(out);
}

function weightTurnover(previous, next) {
  const keys = new Set([...previous.keys(), ...next.keys()]);
  let total = 0;
  keys.forEach((ticker) => {
    total += Math.abs((next.get(ticker) || 0) - (previous.get(ticker) || 0));
  });
  return total;
}

function portfolioReturn(weights, nextReturns) {
  let total = 0;
  weights.forEach((weight, ticker) => {
    total += weight * finite(nextReturns?.[ticker]);
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

function findLatestDatedArtifactPath({ explicit, dir, fallback, pattern }) {
  if (explicit) return resolvePath(explicit);
  const absoluteDir = resolvePath(dir);
  if (!fs.existsSync(absoluteDir)) return resolvePath(fallback);
  const matches = fs.readdirSync(absoluteDir)
    .map((name) => {
      const match = name.match(pattern);
      return match ? { name, startDate: match[1], endDate: match[2] } : null;
    })
    .filter(Boolean)
    .sort((left, right) => right.endDate.localeCompare(left.endDate)
      || right.startDate.localeCompare(left.startDate)
      || right.name.localeCompare(left.name));
  return matches.length
    ? path.join(absoluteDir, matches[0].name)
    : resolvePath(fallback);
}

function findLatestMlReportPath() {
  return findLatestDatedArtifactPath({
    explicit: process.env.PYM_V5_ML_REPORT_PATH,
    dir: DEFAULT_ML_ARTIFACT_DIR,
    fallback: DEFAULT_ML_REPORT_PATH,
    pattern: /^pym-v5-daily-walkforward-micro-features-(\d{4}-\d{2}-\d{2})-(\d{4}-\d{2}-\d{2})\.json$/,
  });
}

function findLatestOptionReportPath() {
  return findLatestDatedArtifactPath({
    explicit: process.env.PYM_V5_ML_OPTION_REPORT_PATH,
    dir: DEFAULT_OPTION_ARTIFACT_DIR,
    fallback: DEFAULT_OPTION_REPORT_PATH,
    pattern: /^pym-v5-option-overlay-suite-grid-top8-zm0p5-(\d{4}-\d{2}-\d{2})-(\d{4}-\d{2}-\d{2})\.json$/,
  });
}

function findLatestMlDatasetPath() {
  return findLatestDatedArtifactPath({
    explicit: process.env.PYM_V5_ML_DATASET_PATH,
    dir: DEFAULT_ML_ARTIFACT_DIR,
    fallback: DEFAULT_DATASET_PATH,
    pattern: /^pym-v5-walkforward-dataset-micro-features-(\d{4}-\d{2}-\d{2})-(\d{4}-\d{2}-\d{2})\.jsonl$/,
  });
}

function findLatestRiskOverlayReportPath() {
  return findLatestDatedArtifactPath({
    explicit: process.env.PYM_V5_ML_RISK_OVERLAY_REPORT_PATH,
    dir: DEFAULT_RISK_OVERLAY_REPORT_DIR,
    fallback: path.join(DEFAULT_RISK_OVERLAY_REPORT_DIR, DEFAULT_RISK_OVERLAY_REPORT_NAME),
    pattern: /^pym-v5-two-speed-risk-overlays-(\d{4}-\d{2}-\d{2})-(\d{4}-\d{2}-\d{2})\.json$/,
  });
}

function loadBenchmarkSamples(datasetPath) {
  return loadDatasetRows(datasetPath).samplesBySignalDate;
}

function loadDatasetRows(datasetPath) {
  if (!datasetPath || !fs.existsSync(datasetPath)) {
    return {
      metadata: {},
      samples: [],
      predictionSamples: [],
      samplesBySignalDate: new Map(),
      predictionSamplesBySignalDate: new Map(),
    };
  }
  let metadata = {};
  const samples = [];
  const predictionSamples = [];
  fs.readFileSync(datasetPath, 'utf8').split(/\r?\n/).forEach((line) => {
    if (!line.trim()) return;
    const row = JSON.parse(line);
    if (row.type === 'metadata') metadata = row;
    else if (row.type === 'sample') samples.push(row);
    else if (row.type === 'prediction_sample') predictionSamples.push(row);
  });
  samples.sort((left, right) => left.date.localeCompare(right.date));
  predictionSamples.sort((left, right) => left.date.localeCompare(right.date));
  return {
    metadata,
    samples,
    predictionSamples,
    samplesBySignalDate: new Map(samples.map((sample) => [sample.date, sample])),
    predictionSamplesBySignalDate: new Map(predictionSamples.map((sample) => [sample.date, sample])),
  };
}

function featureValue(datasetMetadata, sample, group, name) {
  const names = datasetMetadata?.featureNames?.[group] || [];
  const index = names.indexOf(name);
  if (index < 0) return 0;
  return finite(sample?.featureGroups?.[group]?.[index]);
}

function clamp(value, min = 0, max = 1) {
  return Math.max(min, Math.min(max, finite(value)));
}

function regimeStress(datasetMetadata, sample) {
  const spyVol21 = featureValue(datasetMetadata, sample, 'price', 'SPY_vol_21');
  const spyDrawdown21 = featureValue(datasetMetadata, sample, 'price', 'SPY_drawdown_21');
  const qqqDrawdown21 = featureValue(datasetMetadata, sample, 'price', 'QQQ_drawdown_21');
  const vixyRet5 = featureValue(datasetMetadata, sample, 'price', 'VIXY_ret_5');
  const uvxyRet5 = featureValue(datasetMetadata, sample, 'price', 'UVXY_ret_5');
  const spyPutCall = featureValue(datasetMetadata, sample, 'options', 'option_SPY_putCallPremiumRatio');
  const spxPutCall = Math.max(
    featureValue(datasetMetadata, sample, 'options', 'option_SPX_putCallPremiumRatio'),
    featureValue(datasetMetadata, sample, 'options', 'option_SPXW_putCallPremiumRatio'),
  );
  const spyPutPressure = 1 - featureValue(datasetMetadata, sample, 'options', 'option_SPY_callPremiumShare');
  const spxPutPressure = Math.max(
    1 - featureValue(datasetMetadata, sample, 'options', 'option_SPX_callPremiumShare'),
    1 - featureValue(datasetMetadata, sample, 'options', 'option_SPXW_callPremiumShare'),
  );
  const liquidityStress = Math.max(
    featureValue(datasetMetadata, sample, 'liquidity', 'SPY_rangePct_z_21'),
    featureValue(datasetMetadata, sample, 'liquidity', 'QQQ_rangePct_z_21'),
  );
  return clamp([
    0.18 * clamp((spyVol21 - 0.16) / 0.20),
    0.25 * clamp(Math.max(-spyDrawdown21, -qqqDrawdown21) / 0.10),
    0.17 * clamp(vixyRet5 / 0.18),
    0.13 * clamp(uvxyRet5 / 0.30),
    0.08 * clamp((Math.max(spyPutCall, spxPutCall) - 1.1) / 2.5),
    0.08 * clamp((Math.max(spyPutPressure, spxPutPressure) - 0.50) / 0.35),
    0.11 * clamp(liquidityStress / 3.0),
  ].reduce((sum, value) => sum + value, 0));
}

function calmTrendDiagnostics({ datasetMetadata, sample, priorLabeledSamples }) {
  const stress = regimeStress(datasetMetadata, sample);
  const spyRet21 = featureValue(datasetMetadata, sample, 'price', 'SPY_ret_21');
  const qqqRet21 = featureValue(datasetMetadata, sample, 'price', 'QQQ_ret_21');
  const spyVol21 = featureValue(datasetMetadata, sample, 'price', 'SPY_vol_21');
  const ready = priorLabeledSamples >= CALM_TREND_MIN_PRIOR_SAMPLES;
  const calmTrend = ready
    && stress < CALM_TREND_STRESS_MAX
    && spyRet21 > CALM_TREND_SPY_RET_21_MIN
    && qqqRet21 > CALM_TREND_QQQ_RET_21_MIN
    && spyVol21 < CALM_TREND_SPY_VOL_21_MAX;
  return {
    ready,
    calmTrend,
    priorLabeledSamples,
    stress,
    spyRet21,
    qqqRet21,
    spyVol21,
    thresholds: {
      minPriorLabeledSamples: CALM_TREND_MIN_PRIOR_SAMPLES,
      stressMax: CALM_TREND_STRESS_MAX,
      spyRet21Min: CALM_TREND_SPY_RET_21_MIN,
      qqqRet21Min: CALM_TREND_QQQ_RET_21_MIN,
      spyVol21Max: CALM_TREND_SPY_VOL_21_MAX,
    },
  };
}

function loadMlRawPoints(reportPath, strategyId) {
  const report = readJson(reportPath);
  const strategy = report.strategies?.[strategyId];
  if (!strategy?.equityCurve) throw new Error(`missing_ml_strategy:${strategyId}`);
  return {
    sourceReport: report,
    latestPrediction: report.latestPredictions?.[strategyId] || null,
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

function weightsObjectFromHoldings(holdings = []) {
  const out = {};
  holdings.forEach((holding) => {
    const ticker = holding.ticker || holding.symbol;
    const weight = finite(holding.weight);
    if (ticker && weight > 1e-10) out[ticker] = weight;
  });
  return out;
}

function predictionFromSnapshot(snapshot, strategyId) {
  if (!snapshot?.date) return null;
  return {
    strategyId,
    signalDate: snapshot.date,
    nextDate: snapshot.nextDate || null,
    predictionOnly: true,
    startEquity: finite(snapshot.equityBeforeNextSession),
    turnover: finite(snapshot.turnover),
    holdings: weightsObjectFromHoldings(snapshot.holdings),
  };
}

function loadLatestOptionTargetPrediction(optionStrategyId = DEFAULT_OPTION_STRATEGY_ID) {
  try {
    const { createPymV5OptionOverlayStrategy } = require('./pym-v5-option-rank');
    const strategy = createPymV5OptionOverlayStrategy({
      optionStrategyId,
      useOptionStudyEnv: false,
    });
    const report = strategy.getReport();
    const latest = report.latest || report.snapshots?.at(-1) || null;
    if (!latest || latest.realized) return null;
    return predictionFromSnapshot(latest, optionStrategyId);
  } catch {
    return null;
  }
}

function loadRiskOverlayRawPoints(reportPath, strategyId) {
  const report = readJson(reportPath);
  const overlay = report.overlays?.[strategyId];
  if (!overlay?.points) throw new Error(`missing_risk_overlay_strategy:${strategyId}`);
  return {
    sourceReport: report,
    overlay,
    rawPoints: overlay.points.map((point) => ({
      signalDate: point.signalDate,
      date: point.date,
      startEquity: finite(point.startEquity),
      equity: finite(point.equity),
      grossReturn: finite(point.grossReturn, finite(point.netReturn) + finite(point.costReturn)),
      costReturn: finite(point.costReturn),
      netReturn: finite(point.netReturn),
      turnover: finite(point.turnover),
      holdings: point.holdings || {},
      diagnostics: point.diagnostics || null,
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

function selectBestCandidateId({ candidateSeries, candidateIds, currentIndex, lookback, seedId }) {
  if (currentIndex <= 0) return seedId;
  return candidateIds
    .map((candidateId) => ({
      candidateId,
      score: recentStrategyScore(candidateSeries[candidateId], currentIndex, lookback),
    }))
    .sort((left, right) => right.score - left.score || left.candidateId.localeCompare(right.candidateId))[0].candidateId;
}

function selectBestSelectorId({ candidateSelectors, selectorIds, currentIndex, lookback, seedId }) {
  if (currentIndex <= 0) return seedId;
  return selectorIds
    .map((selectorId) => ({
      selectorId,
      score: recentStrategyScore(candidateSelectors[selectorId].points, currentIndex, lookback),
    }))
    .sort((left, right) => right.score - left.score || left.selectorId.localeCompare(right.selectorId))[0].selectorId;
}

function lookbackFromSelectorId(selectorId) {
  const match = String(selectorId || '').match(/recent(\d+)$/);
  return match ? Number(match[1]) : null;
}

function buildTwoStrategySelector({ id, candidateSeries, candidateIds, commonDates, lookback, initialCapital, costBps, seedId }) {
  let equity = initialCapital;
  let previousWeights = new Map();
  const selectionCounts = {};
  const points = commonDates.map((date, index) => {
    const chosenId = selectBestCandidateId({
      candidateSeries,
      candidateIds,
      currentIndex: index,
      lookback,
      seedId,
    });
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
    const chosenSelectorId = selectBestSelectorId({
      candidateSelectors,
      selectorIds,
      currentIndex: index,
      lookback: META_LOOKBACK,
      seedId,
    });
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

function sameSignalDate(...predictions) {
  const dates = predictions.map((prediction) => prediction?.signalDate).filter(Boolean);
  return dates.length && dates.every((date) => date === dates[0]) ? dates[0] : null;
}

function latestBlendedPrediction({ strategyId, left, right, leftWeight, startEquity, extra = {} }) {
  const signalDate = sameSignalDate(left, right);
  if (!signalDate) return null;
  return {
    strategyId,
    signalDate,
    nextDate: left.nextDate || right.nextDate || null,
    predictionOnly: true,
    startEquity,
    holdings: Object.fromEntries(blendHoldings(left.holdings, right.holdings, leftWeight)),
    ...extra,
  };
}

function latestCalmRouterPrediction({ mlPrediction, optionPrediction, dataset, trainStart, startEquity, twoSpeedId, optionId }) {
  const signalDate = sameSignalDate(mlPrediction, optionPrediction);
  if (!signalDate) return null;
  const sample = dataset.predictionSamplesBySignalDate?.get(signalDate)
    || dataset.samplesBySignalDate?.get(signalDate);
  const priorLabeledSamples = dataset.samples
    .filter((row) => row.date >= trainStart && row.date < signalDate)
    .length;
  const diagnostics = calmTrendDiagnostics({
    datasetMetadata: dataset.metadata,
    sample,
    priorLabeledSamples,
  });
  const weights = diagnostics.calmTrend
    ? blendHoldings(mlPrediction.holdings, optionPrediction.holdings, CALM_TREND_RAW_WEIGHT)
    : cleanHoldings(mlPrediction.holdings);
  return {
    strategyId: 'calm_trend_router_35_ml_65_option_top8',
    signalDate,
    nextDate: mlPrediction.nextDate || optionPrediction.nextDate || null,
    predictionOnly: true,
    startEquity,
    holdings: Object.fromEntries(weights),
    routerDiagnostics: {
      ...diagnostics,
      rawMlWeight: diagnostics.calmTrend ? CALM_TREND_RAW_WEIGHT : 1,
      optionTop8Weight: diagnostics.calmTrend ? CALM_TREND_OPTION_WEIGHT : 0,
      rawMlStrategy: twoSpeedId,
      optionTop8Strategy: optionId,
    },
  };
}

function latestMeta21Prediction({
  mlPrediction,
  optionPrediction,
  candidateSeries,
  candidateSelectors,
  selectorIds,
  currentIndex,
  startEquity,
  twoSpeedId,
  optionId,
}) {
  const signalDate = sameSignalDate(mlPrediction, optionPrediction);
  if (!signalDate) return null;
  const seedSelectorId = 'best_of_two_speed_or_option_recent21';
  const chosenSelectorId = selectBestSelectorId({
    candidateSelectors,
    selectorIds,
    currentIndex,
    lookback: META_LOOKBACK,
    seedId: seedSelectorId,
  });
  const chosenLookback = lookbackFromSelectorId(chosenSelectorId) || 21;
  const chosenStrategy = selectBestCandidateId({
    candidateSeries,
    candidateIds: [twoSpeedId, optionId],
    currentIndex,
    lookback: chosenLookback,
    seedId: twoSpeedId,
  });
  const selectedPrediction = chosenStrategy === optionId ? optionPrediction : mlPrediction;
  return {
    strategyId: 'walkforward_lookback_best_of_two_speed_or_option_meta21',
    signalDate,
    nextDate: selectedPrediction.nextDate || null,
    predictionOnly: true,
    startEquity,
    holdings: Object.fromEntries(cleanHoldings(selectedPrediction.holdings)),
    chosenSelector: chosenSelectorId,
    chosenStrategy,
    chosenLookback,
  };
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

function reportFromPoints({
  metadata,
  points,
  samplesBySignalDate,
  source,
  settings,
  extraSummary = {},
  provisionalPredictions = [],
}) {
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
        calmTrendRouter: point.routerDiagnostics || null,
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

  provisionalPredictions.forEach((prediction) => {
    if (!prediction?.signalDate) return;
    const lastRealizedSignalDate = points.at(-1)?.signalDate || null;
    if (lastRealizedSignalDate && prediction.signalDate <= lastRealizedSignalDate) return;
    const weights = cleanHoldings(prediction.holdings);
    const equityBeforeNextSession = finite(prediction.startEquity, points.at(-1)?.equity || initialCapital);
    const turnoverValue = Number.isFinite(prediction.turnover)
      ? prediction.turnover
      : weightTurnover(previousWeights, weights);
    const costReturn = Number.isFinite(prediction.costReturn)
      ? prediction.costReturn
      : turnoverValue * (settings.costBps || DEFAULT_COST_BPS) / 10000;
    const holdings = weightsToHoldings(weights, equityBeforeNextSession, previousWeights);
    const snapshot = {
      date: prediction.signalDate,
      rebalanceDate: prediction.signalDate,
      execution: 'eod_close',
      nextDate: prediction.nextDate || null,
      predictionOnly: true,
      equityBeforeNextSession,
      grossExposure: grossExposure(weights),
      turnover: turnoverValue,
      turnoverPct: pct(turnoverValue),
      estimatedRebalanceCost: equityBeforeNextSession * costReturn,
      estimatedRebalanceCostPct: pct(costReturn),
      holdings,
      topHoldings: topHoldingsLabel(holdings),
      benchmarkReturns: {
        spy: equitySeries.at(-1)?.spyReturn ?? null,
        qqq: equitySeries.at(-1)?.qqqReturn ?? null,
      },
      selectorDiagnostics: {
        chosenStrategy: prediction.chosenStrategy || null,
        chosenSelector: prediction.chosenSelector || null,
        chosenLookback: prediction.chosenLookback || prediction.lookback || null,
        calmTrendRouter: prediction.routerDiagnostics || null,
      },
      realized: null,
    };
    snapshots.push(snapshot);
    previousWeights = weights;
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
    execution: options.execution || dailyEodExecution(),
    dataProvider: 'Massive adjusted EOD + Massive option aggregates',
    strategySource: options.strategySource || 'PYM V5 ML walk-forward artifact',
    description: options.description || null,
    ruleSummary: options.ruleSummary || [],
    sourceLinks: options.sourceLinks || [],
    artifactStrategyId: options.artifactStrategyId || null,
    defaultStartDate: options.defaultStartDate || '2025-02-03',
    supports: ['chart', 'values', 'latest_portfolio', 'portfolio_change', 'refresh_data'],
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

  // ML/option-overlay strategies are artifact-backed: their underlying
  // walk-forward predictions and overlay reports are precomputed by separate
  // out-of-band workflows. Refresh just re-reads the artifact and recomputes
  // (cheap; no external data fetch).
  function refreshData() {
    const { noopRefresh } = require('./refresh-helpers');
    return noopRefresh(state, recompute);
  }

  return {
    state,
    getMetadata,
    getReport,
    recompute,
    refreshData,
  };
}

function createPymV5MlTwoSpeedStrategy(options = {}) {
  const mlReportPath = resolvePath(options.mlReportPath || findLatestMlReportPath());
  const datasetPath = resolvePath(options.datasetPath || findLatestMlDatasetPath());
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
      const { sourceReport, rawPoints, latestPrediction } = loadMlRawPoints(mlReportPath, strategyId);
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
        provisionalPredictions: latestPrediction ? [latestPrediction] : [],
      });
    },
  });
}

function createPymV5TwoSpeedOptionMeta21Strategy(options = {}) {
  const mlReportPath = resolvePath(options.mlReportPath || findLatestMlReportPath());
  const optionReportPath = resolvePath(options.optionReportPath || findLatestOptionReportPath());
  const datasetPath = resolvePath(options.datasetPath || findLatestMlDatasetPath());
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
      const { sourceReport, rawPoints: twoRawPoints, latestPrediction: mlLatestPrediction } = loadMlRawPoints(mlReportPath, twoSpeedId);
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
      const optionLatestPrediction = loadLatestOptionTargetPrediction(optionId);
      const latestPrediction = latestMeta21Prediction({
        mlPrediction: mlLatestPrediction,
        optionPrediction: optionLatestPrediction,
        candidateSeries,
        candidateSelectors,
        selectorIds,
        currentIndex: commonDates.length,
        startEquity: meta.points.at(-1)?.equity || initialCapital,
        twoSpeedId,
        optionId,
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
        provisionalPredictions: latestPrediction ? [latestPrediction] : [],
      });
    },
  });
}

function createPymV5MlOptionTop85050Strategy(options = {}) {
  const riskOverlayReportPath = resolvePath(options.riskOverlayReportPath || findLatestRiskOverlayReportPath());
  const mlReportPath = resolvePath(options.mlReportPath || findLatestMlReportPath());
  const datasetPath = resolvePath(options.datasetPath || findLatestMlDatasetPath());
  const strategyId = options.artifactStrategyId || DEFAULT_ML_OPTION_BLEND_STRATEGY_ID;
  return createArtifactStrategy({
    id: options.id || 'pym-v5-ml-option-top8-50-50',
    name: options.name || 'PYM 50/50 ML + Option Top 8',
    displayName: options.displayName || 'PYM 50/50 ML+Option',
    family: options.family || 'pym_ml_option_blend',
    strategySource: 'PYM V5 two-speed ML risk-overlay artifact',
    description: options.description || 'Static 50/50 blend of the two-speed ML target and the Massive option-flow top-8 target.',
    ruleSummary: options.ruleSummary || ML_OPTION_BLEND_RULE_SUMMARY,
    artifactStrategyId: strategyId,
    buildReport: (metadata) => {
      const { sourceReport, overlay, rawPoints } = loadRiskOverlayRawPoints(riskOverlayReportPath, strategyId);
      const sourceMlReportPath = resolvePath(options.mlReportPath || sourceReport.source?.mlReport) || mlReportPath;
      const mlSourceReport = readJson(sourceMlReportPath);
      const twoSpeedId = sourceReport.source?.strategy || DEFAULT_TWO_SPEED_STRATEGY_ID;
      const optionId = sourceReport.source?.optionOverlayStrategy || DEFAULT_OPTION_STRATEGY_ID;
      const mlLatestPrediction = mlSourceReport.latestPredictions?.[twoSpeedId] || null;
      const optionLatestPrediction = loadLatestOptionTargetPrediction(optionId);
      const latestPrediction = latestBlendedPrediction({
        strategyId,
        left: mlLatestPrediction,
        right: optionLatestPrediction,
        leftWeight: 0.5,
        startEquity: rawPoints.at(-1)?.equity || overlay.summary?.finalEquity || DEFAULT_INITIAL_CAPITAL,
      });
      const samplesBySignalDate = loadBenchmarkSamples(datasetPath);
      return reportFromPoints({
        metadata,
        points: rawPoints,
        samplesBySignalDate,
        source: {
          ...metadata,
          riskOverlayReport: fileMetadata(riskOverlayReportPath),
          dataset: fileMetadata(datasetPath),
          source: sourceReport.source,
          underlyingStrategies: [
            sourceReport.source?.strategy || DEFAULT_TWO_SPEED_STRATEGY_ID,
            sourceReport.source?.optionOverlayStrategy || DEFAULT_OPTION_STRATEGY_ID,
          ],
        },
        settings: {
          ...sourceReport.settings,
          artifactStrategyId: strategyId,
          timing: sourceReport.settings?.timing || 'EOD signal date X; close-to-close realized date X+1.',
          blendWeights: {
            ml: 0.5,
            optionTop8: 0.5,
          },
        },
        extraSummary: {
          overlayDescription: overlay.summary?.description || null,
          sourceSummary: overlay.summary || null,
        },
        provisionalPredictions: latestPrediction ? [latestPrediction] : [],
      });
    },
  });
}

function createPymV5MlCalmTrendRouterStrategy(options = {}) {
  const mlReportPath = resolvePath(options.mlReportPath || findLatestMlReportPath());
  const optionReportPath = resolvePath(options.optionReportPath || findLatestOptionReportPath());
  const datasetPath = resolvePath(options.datasetPath || findLatestMlDatasetPath());
  const twoSpeedId = options.twoSpeedStrategyId || DEFAULT_TWO_SPEED_STRATEGY_ID;
  const optionId = options.optionStrategyId || DEFAULT_OPTION_STRATEGY_ID;
  return createArtifactStrategy({
    id: options.id || 'pym-v5-ml-calm-trend-router',
    name: options.name || 'PYM ML Calm-Trend Router',
    displayName: options.displayName || 'PYM Calm Router',
    family: options.family || 'pym_ml_option_router',
    strategySource: 'PYM V5 ML walk-forward artifact plus option top-8 overlay artifact',
    description: options.description || 'Uses raw PYM two-speed ML in stress/dispersion regimes, but blends toward option top-8 during calm positive-trend tapes.',
    ruleSummary: options.ruleSummary || CALM_TREND_ROUTER_RULE_SUMMARY,
    artifactStrategyId: 'calm_trend_router_35_ml_65_option_top8',
    buildReport: (metadata) => {
      const { sourceReport, rawPoints: mlRawPoints, latestPrediction: mlLatestPrediction } = loadMlRawPoints(mlReportPath, twoSpeedId);
      const { rawPoints: optionRawPoints } = loadOptionRawPoints(optionReportPath, optionId);
      const dataset = loadDatasetRows(datasetPath);
      const samplesBySignalDate = dataset.samplesBySignalDate;
      const commonDates = intersectDates([
        { rawPoints: mlRawPoints },
        { rawPoints: optionRawPoints },
      ]);
      const mlByDate = new Map(mlRawPoints.map((point) => [point.date, point]));
      const optionByDate = new Map(optionRawPoints.map((point) => [point.date, point]));
      const initialCapital = sourceReport.settings?.initialCapital || DEFAULT_INITIAL_CAPITAL;
      const costBps = sourceReport.settings?.costBps || DEFAULT_COST_BPS;
      const trainStart = sourceReport.settings?.trainStart || '2025-01-02';
      let equity = initialCapital;
      let previousWeights = new Map();
      let activatedDays = 0;
      const points = commonDates.map((date) => {
        const rawMl = mlByDate.get(date);
        const rawOption = optionByDate.get(date);
        const sample = samplesBySignalDate.get(rawMl.signalDate);
        const priorLabeledSamples = dataset.samples
          .filter((row) => row.date >= trainStart && row.date < rawMl.signalDate)
          .length;
        const diagnostics = calmTrendDiagnostics({
          datasetMetadata: dataset.metadata,
          sample,
          priorLabeledSamples,
        });
        const weights = diagnostics.calmTrend
          ? blendHoldings(rawMl.holdings, rawOption.holdings, CALM_TREND_RAW_WEIGHT)
          : cleanHoldings(rawMl.holdings);
        if (diagnostics.calmTrend) activatedDays += 1;
        const turnover = weightTurnover(previousWeights, weights);
        const costReturn = turnover * costBps / 10000;
        const grossReturn = sample?.nextReturns
          ? portfolioReturn(weights, sample.nextReturns)
          : finite(rawMl.grossReturn);
        const netReturn = grossReturn - costReturn;
        const startEquity = equity;
        equity *= 1 + netReturn;
        previousWeights = weights;
        return {
          id: 'calm_trend_router_35_ml_65_option_top8',
          signalDate: rawMl.signalDate,
          date,
          startEquity,
          equity,
          grossReturn,
          costReturn,
          netReturn,
          turnover,
          holdings: Object.fromEntries(weights),
          routerDiagnostics: {
            ...diagnostics,
            rawMlWeight: diagnostics.calmTrend ? CALM_TREND_RAW_WEIGHT : 1,
            optionTop8Weight: diagnostics.calmTrend ? CALM_TREND_OPTION_WEIGHT : 0,
            rawMlStrategy: twoSpeedId,
            optionTop8Strategy: optionId,
          },
        };
      });
      const optionLatestPrediction = loadLatestOptionTargetPrediction(optionId);
      const latestPrediction = latestCalmRouterPrediction({
        mlPrediction: mlLatestPrediction,
        optionPrediction: optionLatestPrediction,
        dataset,
        trainStart,
        startEquity: points.at(-1)?.equity || initialCapital,
        twoSpeedId,
        optionId,
      });
      return reportFromPoints({
        metadata,
        points,
        samplesBySignalDate,
        source: {
          ...metadata,
          mlReport: fileMetadata(mlReportPath),
          optionReport: fileMetadata(optionReportPath),
          dataset: fileMetadata(datasetPath),
          underlyingStrategies: [twoSpeedId, optionId],
        },
        settings: {
          ...sourceReport.settings,
          artifactStrategyId: 'calm_trend_router_35_ml_65_option_top8',
          underlyingStrategies: [twoSpeedId, optionId],
          timing: 'EOD signal date X; close-to-close realized date X+1. Calm-trend routing uses only signal-date features.',
          calmTrendRouter: {
            rawMlWeight: CALM_TREND_RAW_WEIGHT,
            optionTop8Weight: CALM_TREND_OPTION_WEIGHT,
            minPriorLabeledSamples: CALM_TREND_MIN_PRIOR_SAMPLES,
            stressMax: CALM_TREND_STRESS_MAX,
            spyRet21Min: CALM_TREND_SPY_RET_21_MIN,
            qqqRet21Min: CALM_TREND_QQQ_RET_21_MIN,
            spyVol21Max: CALM_TREND_SPY_VOL_21_MAX,
          },
        },
        extraSummary: {
          routerActivationDays: activatedDays,
          routerActivationShare: points.length ? activatedDays / points.length : 0,
        },
        provisionalPredictions: latestPrediction ? [latestPrediction] : [],
      });
    },
  });
}

module.exports = {
  createPymV5MlTwoSpeedStrategy,
  createPymV5MlOptionTop85050Strategy,
  createPymV5MlCalmTrendRouterStrategy,
  createPymV5TwoSpeedOptionMeta21Strategy,
};
