const { computePolicyBacktest } = require('./backtest');
const { compactMetrics, computePredictionMetrics } = require('./metrics');
const {
  filterFeatureColumns,
  fitLogisticRegression,
  fitScaler,
  isFiniteNumber,
  predictBaseline,
  predictExamples,
  predictLinear,
  selectNumericFeatureColumns,
  sigmoid,
  trainExperiment,
  transformExample,
} = require('./models');
const { rowsInWindow, splitRowsByConfig } = require('./splits');

const PHASE2_STRATEGIES = Object.freeze([
  'volatility_gated',
  'threshold_policy',
  'regime_diagnostics',
  'enhanced_meta_labeling',
  'walk_forward',
]);

const THRESHOLD_CANDIDATES = Object.freeze([0, 0.5, 0.55, 0.6, 0.65, 0.7, 0.75, 0.8, 0.85, 0.9]);
const CONFIDENCE_CANDIDATES = Object.freeze([0.5, 0.52, 0.54, 0.56, 0.58, 0.6, 0.65]);
const META_CANDIDATES = Object.freeze([0.45, 0.5, 0.55, 0.6, 0.65, 0.7, 0.75]);
const MAX_DIRECTION_FEATURES = 90;
const MAX_MAGNITUDE_FEATURES = 120;
const MAX_META_FEATURES = 140;

function round(value, digits = 6) {
  return typeof value === 'number' && Number.isFinite(value) ? Number(value.toFixed(digits)) : value;
}

function unique(values) {
  return [...new Set(values)];
}

function featurePriority(column) {
  if (['minuteOfDayEt', 'minutes_from_open', 'minutes_to_close'].includes(column)) return 0;
  if (column.startsWith('spy_')) return 1;
  if (column.startsWith('vix')) return 2;
  if (column.startsWith('gamma_proxy_')) return 3;
  if (column.startsWith('opening_')) return 4;
  if (column.startsWith('opt_spy_')) return 5;
  if (column.startsWith('opt_spx_')) return 6;
  if (column.includes('_breadth_')) return 7;
  if (column.includes('_rel_spy_')) return 8;
  if (column.includes('_ret_')) return 9;
  if (column.includes('_volume_log')) return 10;
  return 99;
}

function capFeatureColumns(columns, maxColumns) {
  if (columns.length <= maxColumns) return columns;
  return columns
    .slice()
    .sort((left, right) => {
      const priority = featurePriority(left) - featurePriority(right);
      if (priority !== 0) return priority;
      return left.localeCompare(right);
    })
    .slice(0, maxColumns)
    .sort();
}

function trainFast(rows, horizonName, featureColumns) {
  return trainExperiment(rows, horizonName, featureColumns, {
    logistic: { iterations: 80, learningRate: 0.055, l2: 0.0015 },
    linear: { iterations: 90, learningRate: 0.022, l2: 0.0015 },
  });
}

function finiteRows(rows, labelName) {
  const labelKey = `label_${labelName}_return`;
  return rows.filter((row) => isFiniteNumber(row[labelKey]));
}

function rowsAtMinute(rows, minuteOfDayEt) {
  return rows.filter((row) => row.minuteOfDayEt === minuteOfDayEt);
}

function rowsFromMinute(rows, minuteOfDayEt) {
  return rows.filter((row) => row.minuteOfDayEt >= minuteOfDayEt);
}

function rowKey(row) {
  return row.rowId || `${row.tradeDate}|${row.minuteUtc}`;
}

function uniqueSortedDates(rows) {
  return unique(rows.map((row) => row.tradeDate).filter(Boolean)).sort();
}

function splitRowsForValidation(rows, validationShare = 0.25) {
  const dates = uniqueSortedDates(rows);
  if (dates.length < 4) return { fitRows: rows, validationRows: rows };
  const validationDateCount = Math.max(1, Math.floor(dates.length * validationShare));
  const validationDates = new Set(dates.slice(-validationDateCount));
  const fitRows = rows.filter((row) => !validationDates.has(row.tradeDate));
  const validationRows = rows.filter((row) => validationDates.has(row.tradeDate));
  if (!fitRows.length || !validationRows.length) return { fitRows: rows, validationRows: rows };
  return { fitRows, validationRows };
}

function quantile(sortedValues, percentile) {
  if (!sortedValues.length) return null;
  const index = Math.min(sortedValues.length - 1, Math.max(0, Math.floor(sortedValues.length * percentile)));
  return sortedValues[index];
}

function makeHighMagnitudeRows(rows, sourceLabel, threshold, targetLabel) {
  return rows
    .filter((row) => isFiniteNumber(row[`label_${sourceLabel}_return`]))
    .map((row) => ({
      ...row,
      [`label_${targetLabel}_return`]: row[`label_${sourceLabel}_return`] >= threshold ? 1 : 0,
    }));
}

function magnitudeSourceForHorizon(horizonName) {
  if (horizonName === 'next_5m') return 'abs_return_5m';
  if (horizonName === 'next_60m') return 'abs_return_60m';
  if (horizonName === 'last_30m') return 'abs_return_30m';
  return 'abs_return_eod';
}

function phase2Setups(config) {
  return [
    {
      strategy: 'cross_sectional',
      featureSet: 'cross_sectional_research',
      horizons: ['next_5m', 'next_60m'],
      rowFilter: (rows) => rows,
    },
    {
      strategy: 'gamma_regime',
      featureSet: 'gamma_regime_research',
      horizons: ['next_5m', 'next_60m', 'eod_close'],
      rowFilter: (rows) => rowsFromMinute(rows, config.session.regularOpenMinuteEt + (config.research?.openingWindowMinutes || 30)),
    },
    {
      strategy: 'eod_momentum',
      featureSet: 'eod_momentum_research',
      horizons: ['last_30m', 'eod_close'],
      rowFilter: (rows) => rowsAtMinute(rows, config.research?.lastThirtyEntryMinuteEt || 930),
    },
    {
      strategy: 'opening_option_flow',
      featureSet: 'opening_option_flow_research',
      horizons: ['eod_close'],
      rowFilter: (rows) => rowsAtMinute(rows, config.session.regularOpenMinuteEt + (config.research?.openingWindowMinutes || 30)),
    },
  ];
}

function magnitudeFeatureColumns(allFeatureColumns) {
  return capFeatureColumns(unique([
    ...filterFeatureColumns(allFeatureColumns, 'cross_sectional_research'),
    ...filterFeatureColumns(allFeatureColumns, 'gamma_regime_research'),
    ...filterFeatureColumns(allFeatureColumns, 'eod_momentum_research'),
    ...filterFeatureColumns(allFeatureColumns, 'opening_option_flow_research'),
  ]), MAX_MAGNITUDE_FEATURES);
}

function buildMagnitudeModel(rows, sourceLabel, featureColumns, targetPrefix = 'phase2_high') {
  const values = rows
    .map((row) => row[`label_${sourceLabel}_return`])
    .filter(isFiniteNumber)
    .sort((left, right) => left - right);
  if (values.length < 20) return null;
  const threshold = quantile(values, 0.7);
  const targetLabel = `${targetPrefix}_${sourceLabel}`;
  const preparedRows = makeHighMagnitudeRows(rows, sourceLabel, threshold, targetLabel);
  if (preparedRows.length < 20) return null;
  return {
    threshold,
    targetLabel,
    model: trainFast(preparedRows, targetLabel, featureColumns),
  };
}

function predictionsByRow(predictions) {
  const out = new Map();
  predictions.forEach((prediction) => out.set(rowKey(prediction.row), prediction));
  return out;
}

function applyMagnitudeGate(basePredictions, magnitudePredictions, magnitudeThreshold) {
  const magnitudeByRow = predictionsByRow(magnitudePredictions);
  return basePredictions.map((prediction) => {
    const magnitude = magnitudeByRow.get(rowKey(prediction.row));
    const magnitudeProbability = magnitude?.directionProbability ?? 0;
    const accepted = magnitudeProbability >= magnitudeThreshold;
    return {
      ...prediction,
      directionProbability: accepted ? prediction.directionProbability : 0.5,
      predictedReturn: accepted ? prediction.predictedReturn : 0,
      predictedDirection: accepted ? prediction.predictedDirection : 0,
      confidence: accepted ? Math.max(prediction.confidence, magnitudeProbability) : 0.5,
      magnitudeProbability,
      magnitudeThreshold,
      accepted,
    };
  });
}

function compactBacktest(result) {
  return {
    observations: result.observations,
    inputObservations: result.inputObservations,
    executionPolicy: result.executionPolicy,
    confidenceThreshold: round(result.confidenceThreshold),
    totalReturn: round(result.totalReturn),
    buyAndHoldReturn: round(result.buyAndHoldReturn),
    excessReturn: round(result.excessReturn),
    maxDrawdown: round(result.maxDrawdown),
    turnover: round(result.turnover),
    averageTurnover: round(result.averageTurnover),
    longShare: round(result.longShare),
    shortShare: round(result.shortShare),
    cashShare: round(result.cashShare),
  };
}

function acceptedShare(predictions) {
  if (!predictions.length) return 0;
  return predictions.filter((prediction) => prediction.accepted !== false).length / predictions.length;
}

function chooseByBacktest(candidates, predictionFactory, config, horizonName, { minExposureShare = 0.02 } = {}) {
  let best = null;
  const evaluated = candidates.map((threshold) => {
    const predictions = predictionFactory(threshold);
    const exposureShare = acceptedShare(predictions);
    const backtest = computePolicyBacktest(predictions, { ...config.execution, horizonName });
    const score = exposureShare >= minExposureShare ? backtest.totalReturn - (Math.abs(backtest.maxDrawdown) * 0.1) : -Infinity;
    const item = {
      threshold,
      exposureShare,
      score,
      totalReturn: backtest.totalReturn,
      maxDrawdown: backtest.maxDrawdown,
      observations: backtest.observations,
    };
    if (!best || item.score > best.score) best = item;
    return item;
  });
  if (!best || best.score === -Infinity) {
    best = evaluated.sort((left, right) => right.totalReturn - left.totalReturn)[0] || { threshold: candidates[0] };
  }
  return {
    selectedThreshold: best.threshold,
    selected: {
      ...best,
      totalReturn: round(best.totalReturn),
      maxDrawdown: round(best.maxDrawdown),
      score: round(best.score),
      exposureShare: round(best.exposureShare),
    },
    candidates: evaluated.map((item) => ({
      threshold: item.threshold,
      exposureShare: round(item.exposureShare),
      totalReturn: round(item.totalReturn),
      maxDrawdown: round(item.maxDrawdown),
      score: round(item.score),
      observations: item.observations,
    })),
  };
}

function predictionRecord({ resultKey, prediction }) {
  return {
    resultKey,
    rowId: prediction.row.rowId,
    tradeDate: prediction.row.tradeDate,
    minuteUtc: prediction.row.minuteUtc,
    spyClose: prediction.row.spy_close,
    actualReturn: prediction.actualReturn,
    actualDirection: prediction.actualDirection,
    predictedDirection: prediction.predictedDirection,
    directionProbability: round(prediction.directionProbability, 8),
    predictedReturn: round(prediction.predictedReturn, 8),
    confidence: round(prediction.confidence, 8),
    magnitudeProbability: round(prediction.magnitudeProbability, 8),
    metaAcceptProbability: round(prediction.metaAcceptProbability, 8),
    accepted: prediction.accepted,
  };
}

function addPhase2Result(out, {
  config,
  trainMode,
  phase2Strategy,
  baseStrategy,
  experiment,
  horizonName,
  featureSet,
  trainRows,
  testWindow,
  predictions,
  baselinePredictions,
  ungatedPredictions = null,
  model,
  selection = null,
  policySettings = null,
  includeDiagnostics = false,
}) {
  const policy = { ...config.execution, ...(policySettings || {}), horizonName };
  const backtest = computePolicyBacktest(predictions, policy);
  const baselineBacktest = baselinePredictions ? computePolicyBacktest(baselinePredictions, policy) : null;
  const ungatedBacktest = ungatedPredictions ? computePolicyBacktest(ungatedPredictions, { ...config.execution, horizonName }) : null;
  const resultKey = `${trainMode}|${phase2Strategy}|${experiment}|${horizonName}|${testWindow.name}`;
  const record = {
    resultKey,
    trainMode,
    phase2Strategy,
    baseStrategy,
    experiment,
    horizon: horizonName,
    featureSet,
    split: testWindow.name,
    trainRows: model?.trainRowCount || trainRows.length,
    testRows: predictions.length,
    featureCount: model?.featureColumns?.length || 0,
    selectedPolicy: selection,
    acceptedShare: round(acceptedShare(predictions)),
    metrics: compactMetrics(computePredictionMetrics(predictions)),
    baselineMetrics: baselinePredictions ? compactMetrics(computePredictionMetrics(baselinePredictions)) : null,
    ungatedMetrics: ungatedPredictions ? compactMetrics(computePredictionMetrics(ungatedPredictions)) : null,
    backtest: compactBacktest(backtest),
    baselineBacktest: baselineBacktest ? compactBacktest(baselineBacktest) : null,
    ungatedBacktest: ungatedBacktest ? compactBacktest(ungatedBacktest) : null,
  };
  if (includeDiagnostics) {
    record.regimeDiagnostics = computeRegimeDiagnostics(predictions, config, horizonName);
  }
  out.results.push(record);
  predictions.forEach((prediction) => out.predictions.push(predictionRecord({ resultKey, prediction })));
  return record;
}

function trainRowsForSetup(rows, setup, horizonName) {
  return finiteRows(setup.rowFilter(rows), horizonName);
}

function testRowsForSetup(rows, setup, horizonName) {
  return finiteRows(setup.rowFilter(rows), horizonName);
}

function runVolatilityGatedSetup({ out, config, trainMode, trainRows, tests, allFeatureColumns, setup, horizonName, includeDiagnostics }) {
  const featureColumns = capFeatureColumns(filterFeatureColumns(allFeatureColumns, setup.featureSet), MAX_DIRECTION_FEATURES);
  const magColumns = magnitudeFeatureColumns(allFeatureColumns);
  const filteredTrain = trainRowsForSetup(trainRows, setup, horizonName);
  if (filteredTrain.length < 50 || featureColumns.length === 0 || magColumns.length === 0) return;

  const sourceLabel = magnitudeSourceForHorizon(horizonName);
  const { fitRows, validationRows } = splitRowsForValidation(filteredTrain);
  const fitMagnitude = buildMagnitudeModel(fitRows, sourceLabel, magColumns, 'phase2_validation_high');
  if (!fitMagnitude) return;
  const fitBaseModel = trainFast(fitRows, horizonName, featureColumns);
  const validationBase = predictExamples(fitBaseModel, validationRows);
  const validationMagnitudeRows = makeHighMagnitudeRows(validationRows, sourceLabel, fitMagnitude.threshold, fitMagnitude.targetLabel);
  const validationMagnitude = predictExamples(fitMagnitude.model, validationMagnitudeRows);
  const selection = chooseByBacktest(
    THRESHOLD_CANDIDATES,
    (threshold) => applyMagnitudeGate(validationBase, validationMagnitude, threshold),
    config,
    horizonName,
  );

  const finalMagnitude = buildMagnitudeModel(filteredTrain, sourceLabel, magColumns, 'phase2_high');
  if (!finalMagnitude) return;
  const finalBaseModel = trainFast(filteredTrain, horizonName, featureColumns);
  tests.forEach(({ window, rows }) => {
    const filteredTest = testRowsForSetup(rows, setup, horizonName);
    if (!filteredTest.length) return;
    const basePredictions = predictExamples(finalBaseModel, filteredTest);
    const baselinePredictions = predictBaseline(finalBaseModel, filteredTest);
    const magnitudeRows = makeHighMagnitudeRows(filteredTest, sourceLabel, finalMagnitude.threshold, finalMagnitude.targetLabel);
    const magnitudePredictions = predictExamples(finalMagnitude.model, magnitudeRows);
    const gatedPredictions = applyMagnitudeGate(basePredictions, magnitudePredictions, selection.selectedThreshold);
    addPhase2Result(out, {
      config,
      trainMode,
      phase2Strategy: 'volatility_gated',
      baseStrategy: setup.strategy,
      experiment: `volatility_gated_${setup.strategy}_${setup.featureSet}`,
      horizonName,
      featureSet: `${setup.featureSet}+volatility_magnitude`,
      trainRows: filteredTrain,
      testWindow: window,
      predictions: gatedPredictions,
      baselinePredictions,
      ungatedPredictions: basePredictions,
      model: finalBaseModel,
      selection: {
        type: 'validation_magnitude_threshold',
        sourceLabel,
        labelThreshold: round(finalMagnitude.threshold, 8),
        ...selection,
      },
      includeDiagnostics,
    });
  });
}

function runThresholdPolicySetup({ out, config, trainMode, trainRows, tests, allFeatureColumns, setup, horizonName, includeDiagnostics }) {
  const featureColumns = capFeatureColumns(filterFeatureColumns(allFeatureColumns, setup.featureSet), MAX_DIRECTION_FEATURES);
  const filteredTrain = trainRowsForSetup(trainRows, setup, horizonName);
  if (filteredTrain.length < 50 || featureColumns.length === 0) return;

  const { fitRows, validationRows } = splitRowsForValidation(filteredTrain);
  const fitModel = trainFast(fitRows, horizonName, featureColumns);
  const validationPredictions = predictExamples(fitModel, validationRows);
  const selection = chooseByBacktest(
    CONFIDENCE_CANDIDATES,
    (threshold) => validationPredictions.map((prediction) => ({ ...prediction, accepted: prediction.confidence >= threshold })),
    config,
    horizonName,
    { minExposureShare: 0.05 },
  );
  const finalModel = trainFast(filteredTrain, horizonName, featureColumns);
  tests.forEach(({ window, rows }) => {
    const filteredTest = testRowsForSetup(rows, setup, horizonName);
    if (!filteredTest.length) return;
    const predictions = predictExamples(finalModel, filteredTest);
    const thresholded = predictions.map((prediction) => {
      const accepted = prediction.confidence >= selection.selectedThreshold;
      return accepted ? { ...prediction, accepted } : {
        ...prediction,
        directionProbability: 0.5,
        predictedReturn: 0,
        predictedDirection: 0,
        confidence: 0.5,
        accepted,
      };
    });
    addPhase2Result(out, {
      config,
      trainMode,
      phase2Strategy: 'threshold_policy',
      baseStrategy: setup.strategy,
      experiment: `threshold_policy_${setup.strategy}_${setup.featureSet}`,
      horizonName,
      featureSet: setup.featureSet,
      trainRows: filteredTrain,
      testWindow: window,
      predictions: thresholded,
      baselinePredictions: predictBaseline(finalModel, filteredTest),
      ungatedPredictions: predictions,
      model: finalModel,
      selection: {
        type: 'validation_confidence_threshold',
        ...selection,
      },
      policySettings: { confidenceThreshold: 0.500001 },
      includeDiagnostics,
    });
  });
}

function timeBucket(minuteOfDayEt) {
  if (!Number.isFinite(minuteOfDayEt)) return 'unknown';
  if (minuteOfDayEt < 630) return 'open_0930_1030';
  if (minuteOfDayEt < 780) return 'midday_1030_1300';
  if (minuteOfDayEt < 900) return 'afternoon_1300_1500';
  return 'last_hour_1500_1600';
}

function signBucket(value) {
  if (!isFiniteNumber(value)) return 'missing';
  if (value > 0) return 'positive';
  if (value < 0) return 'negative';
  return 'flat';
}

function quantileBucket(value, low, high) {
  if (!isFiniteNumber(value)) return 'missing';
  if (low === null || high === null) return 'all';
  if (value <= low) return 'low';
  if (value >= high) return 'high';
  return 'middle';
}

function compactDiagnosticBacktest(predictions, config, horizonName) {
  const backtest = computePolicyBacktest(predictions, { ...config.execution, horizonName });
  return {
    observations: backtest.observations,
    totalReturn: round(backtest.totalReturn),
    maxDrawdown: round(backtest.maxDrawdown),
    longShare: round(backtest.longShare),
    shortShare: round(backtest.shortShare),
    cashShare: round(backtest.cashShare),
  };
}

function computeRegimeDiagnostics(predictions, config, horizonName) {
  const rows = predictions.map((prediction) => prediction.row);
  const quantileFeatures = [
    { name: 'vix_close', column: 'vix_close' },
    { name: 'vix1d_over_vix', column: 'vix1d_over_vix' },
    { name: 'vix9d_over_vix', column: 'vix9d_over_vix' },
    { name: 'realized_vol_30m', column: 'spy_rv_30m' },
    { name: 'gamma_short_dte_pressure', column: 'gamma_proxy_short_dte_pressure' },
  ];
  const quantiles = new Map();
  quantileFeatures.forEach((feature) => {
    const values = rows
      .map((row) => row[feature.column])
      .filter(isFiniteNumber)
      .sort((left, right) => left - right);
    quantiles.set(feature.column, {
      low: quantile(values, 1 / 3),
      high: quantile(values, 2 / 3),
    });
  });
  const specs = [
    { name: 'time_of_day', bucket: (row) => timeBucket(row.minuteOfDayEt) },
    { name: 'opening_range_direction', bucket: (row) => signBucket(row.opening_30m_return) },
    { name: 'option_flow_imbalance', bucket: (row) => signBucket(row.opt_spy_trade_premium_imbalance) },
    ...quantileFeatures.map((feature) => ({
      name: feature.name,
      bucket: (row) => {
        const cuts = quantiles.get(feature.column);
        return quantileBucket(row[feature.column], cuts.low, cuts.high);
      },
    })),
  ];
  const minimumCount = horizonName === 'eod_close' || horizonName === 'last_30m' ? 5 : 25;
  return specs.flatMap((spec) => {
    const groups = new Map();
    predictions.forEach((prediction) => {
      const bucket = spec.bucket(prediction.row);
      const list = groups.get(bucket) || [];
      list.push(prediction);
      groups.set(bucket, list);
    });
    return [...groups.entries()]
      .filter(([, list]) => list.length >= minimumCount)
      .map(([bucket, list]) => ({
        regime: spec.name,
        bucket,
        count: list.length,
        metrics: compactMetrics(computePredictionMetrics(list)),
        backtest: compactDiagnosticBacktest(list, config, horizonName),
      }));
  });
}

function buildMetaExamples({ basePredictions, magnitudePredictions, featureColumns, costRate }) {
  const magnitudeByRow = predictionsByRow(magnitudePredictions);
  return basePredictions.map((prediction) => {
    const magnitude = magnitudeByRow.get(rowKey(prediction.row));
    const magnitudeProbability = magnitude?.directionProbability ?? 0;
    const position = prediction.predictedDirection === 1 ? 1 : -1;
    const signedReturn = (position * prediction.actualReturn) - costRate;
    return {
      row: prediction.row,
      primary: prediction,
      magnitudeProbability,
      x: featureColumns
        .map((column) => (isFiniteNumber(prediction.row[column]) ? prediction.row[column] : 0))
        .concat([
          prediction.directionProbability,
          prediction.confidence,
          prediction.predictedReturn,
          magnitudeProbability,
        ]),
      yDirection: signedReturn > 0 ? 1 : 0,
      yReturn: signedReturn,
    };
  });
}

function trainEnhancedMetaModel({ basePredictions, magnitudePredictions, featureColumns, costRate }) {
  const rawExamples = buildMetaExamples({ basePredictions, magnitudePredictions, featureColumns, costRate });
  if (rawExamples.length < 30) return null;
  const scaler = fitScaler(rawExamples);
  const examples = rawExamples.map((example) => ({ ...example, z: transformExample(example, scaler) }));
  const logistic = fitLogisticRegression(examples, { iterations: 90, learningRate: 0.05, l2: 0.0015 });
  return { scaler, logistic, featureColumns, examples };
}

function applyEnhancedMeta({ basePredictions, magnitudePredictions, metaModel, threshold }) {
  const magnitudeByRow = predictionsByRow(magnitudePredictions);
  return basePredictions.map((prediction) => {
    const magnitude = magnitudeByRow.get(rowKey(prediction.row));
    const magnitudeProbability = magnitude?.directionProbability ?? 0;
    const x = metaModel.featureColumns
      .map((column) => (isFiniteNumber(prediction.row[column]) ? prediction.row[column] : 0))
      .concat([
        prediction.directionProbability,
        prediction.confidence,
        prediction.predictedReturn,
        magnitudeProbability,
      ]);
    const z = transformExample({ x }, metaModel.scaler);
    const metaAcceptProbability = sigmoid(predictLinear(metaModel.logistic.weights, z));
    const accepted = metaAcceptProbability >= threshold;
    return accepted ? {
      ...prediction,
      confidence: Math.max(prediction.confidence, metaAcceptProbability, magnitudeProbability),
      magnitudeProbability,
      metaAcceptProbability,
      accepted,
    } : {
      ...prediction,
      directionProbability: 0.5,
      predictedReturn: 0,
      predictedDirection: 0,
      confidence: 0.5,
      magnitudeProbability,
      metaAcceptProbability,
      accepted,
    };
  });
}

function runEnhancedMetaSetup({ out, config, trainMode, trainRows, tests, allFeatureColumns, setup, horizonName, includeDiagnostics }) {
  const directionColumns = capFeatureColumns(filterFeatureColumns(allFeatureColumns, setup.featureSet), MAX_DIRECTION_FEATURES);
  const metaColumns = capFeatureColumns(unique([
    ...directionColumns,
    ...filterFeatureColumns(allFeatureColumns, 'gamma_regime_research'),
    ...filterFeatureColumns(allFeatureColumns, 'eod_momentum_research'),
  ]), MAX_META_FEATURES);
  const magColumns = magnitudeFeatureColumns(allFeatureColumns);
  const filteredTrain = trainRowsForSetup(trainRows, setup, horizonName);
  if (filteredTrain.length < 80 || directionColumns.length === 0 || metaColumns.length === 0 || magColumns.length === 0) return;
  const sourceLabel = magnitudeSourceForHorizon(horizonName);
  const { fitRows, validationRows } = splitRowsForValidation(filteredTrain, 0.35);
  const fitBaseModel = trainFast(fitRows, horizonName, directionColumns);
  const fitMagnitude = buildMagnitudeModel(fitRows, sourceLabel, magColumns, 'phase2_meta_high');
  if (!fitMagnitude) return;
  const validationBase = predictExamples(fitBaseModel, validationRows);
  const validationMagnitudeRows = makeHighMagnitudeRows(validationRows, sourceLabel, fitMagnitude.threshold, fitMagnitude.targetLabel);
  const validationMagnitude = predictExamples(fitMagnitude.model, validationMagnitudeRows);
  const costRate = ((config.execution?.transactionCostBps || 1) + (config.execution?.slippageBps || 1)) / 10_000;
  const metaModel = trainEnhancedMetaModel({
    basePredictions: validationBase,
    magnitudePredictions: validationMagnitude,
    featureColumns: metaColumns,
    costRate,
  });
  if (!metaModel) return;
  const selection = chooseByBacktest(
    META_CANDIDATES,
    (threshold) => applyEnhancedMeta({
      basePredictions: validationBase,
      magnitudePredictions: validationMagnitude,
      metaModel,
      threshold,
    }),
    config,
    horizonName,
    { minExposureShare: 0.03 },
  );
  const finalBaseModel = trainFast(filteredTrain, horizonName, directionColumns);
  const finalMagnitude = buildMagnitudeModel(filteredTrain, sourceLabel, magColumns, 'phase2_meta_final_high');
  if (!finalMagnitude) return;
  tests.forEach(({ window, rows }) => {
    const filteredTest = testRowsForSetup(rows, setup, horizonName);
    if (!filteredTest.length) return;
    const basePredictions = predictExamples(finalBaseModel, filteredTest);
    const magnitudeRows = makeHighMagnitudeRows(filteredTest, sourceLabel, finalMagnitude.threshold, finalMagnitude.targetLabel);
    const magnitudePredictions = predictExamples(finalMagnitude.model, magnitudeRows);
    const metaPredictions = applyEnhancedMeta({
      basePredictions,
      magnitudePredictions,
      metaModel,
      threshold: selection.selectedThreshold,
    });
    addPhase2Result(out, {
      config,
      trainMode,
      phase2Strategy: 'enhanced_meta_labeling',
      baseStrategy: setup.strategy,
      experiment: `enhanced_meta_${setup.strategy}_${setup.featureSet}`,
      horizonName,
      featureSet: `${setup.featureSet}+meta+volatility_magnitude`,
      trainRows: filteredTrain,
      testWindow: window,
      predictions: metaPredictions,
      baselinePredictions: predictBaseline(finalBaseModel, filteredTest),
      ungatedPredictions: basePredictions,
      model: {
        ...finalBaseModel,
        featureColumns: metaColumns.concat(['primary_probability', 'primary_confidence', 'primary_return', 'magnitude_probability']),
      },
      selection: {
        type: 'validation_meta_accept_threshold',
        sourceLabel,
        labelThreshold: round(finalMagnitude.threshold, 8),
        ...selection,
      },
      policySettings: { confidenceThreshold: 0.500001 },
      includeDiagnostics,
    });
  });
}

function walkForwardTrainRows(rows, config, testIndex) {
  const startDate = config.windows.sensitivityTrain?.startDate || config.windows.train.startDate;
  const previousEnd = testIndex === 0
    ? config.windows.train.endDate
    : config.windows.tests[testIndex - 1].endDate;
  return rows.filter((row) => row.tradeDate >= startDate && row.tradeDate <= previousEnd);
}

function runForTrainMode({ out, rows, config, trainMode, trainRows, tests, requestedStrategies, includeDiagnostics }) {
  const allFeatureColumns = selectNumericFeatureColumns(trainRows);
  phase2Setups(config).forEach((setup) => {
    setup.horizons.forEach((horizonName) => {
      if (process.env.SPY_PHASE2_PROGRESS !== '0') {
        process.stderr.write(`[spy-intraday-phase2] trainMode=${trainMode} setup=${setup.strategy} horizon=${horizonName} trainRows=${trainRows.length} tests=${tests.length}\n`);
      }
      if (requestedStrategies.includes('volatility_gated')) {
        runVolatilityGatedSetup({ out, config, trainMode, trainRows, tests, allFeatureColumns, setup, horizonName, includeDiagnostics });
      }
      if (requestedStrategies.includes('threshold_policy')) {
        runThresholdPolicySetup({ out, config, trainMode, trainRows, tests, allFeatureColumns, setup, horizonName, includeDiagnostics });
      }
      if (requestedStrategies.includes('enhanced_meta_labeling') && ['cross_sectional', 'gamma_regime'].includes(setup.strategy)) {
        runEnhancedMetaSetup({ out, config, trainMode, trainRows, tests, allFeatureColumns, setup, horizonName, includeDiagnostics });
      }
    });
  });
  void rows;
}

function markPhase2Promising(results, config) {
  const minLift = config.research?.promisingThresholds?.minBalancedAccuracyLift ?? 0.02;
  const minPositiveMonths = config.research?.promisingThresholds?.minPositiveHoldoutMonths ?? 2;
  const grouped = new Map();
  results.forEach((result) => {
    const key = `${result.trainMode}|${result.phase2Strategy}|${result.baseStrategy}|${result.experiment}|${result.horizon}`;
    const list = grouped.get(key) || [];
    list.push(result);
    grouped.set(key, list);
  });
  const promising = [];
  grouped.forEach((items, key) => {
    const positiveMonths = items.filter((item) => item.backtest.totalReturn > 0).length;
    const liftMonths = items.filter((item) => {
      const baseBalanced = item.ungatedMetrics?.balancedAccuracy ?? item.baselineMetrics?.balancedAccuracy ?? 0;
      return (item.metrics.balancedAccuracy || 0) - baseBalanced >= minLift;
    }).length;
    const improvesPolicyMonths = items.filter((item) => (
      item.ungatedBacktest && item.backtest.totalReturn > item.ungatedBacktest.totalReturn
    )).length;
    if (positiveMonths >= minPositiveMonths && (liftMonths >= minPositiveMonths || improvesPolicyMonths >= minPositiveMonths)) {
      promising.push({ key, positiveMonths, liftMonths, improvesPolicyMonths, items });
    }
  });
  return promising;
}

function runPhase2Suite(rows, config, requestedStrategies = PHASE2_STRATEGIES) {
  const splitBundle = splitRowsByConfig(rows, config);
  const out = { results: [], predictions: [] };
  const includeDiagnostics = requestedStrategies.includes('regime_diagnostics');
  const trainModes = [
    { name: config.research?.officialTrainName || 'jan_only', rows: splitBundle.train, tests: splitBundle.tests },
  ];
  if (splitBundle.sensitivityTrain?.length) {
    trainModes.push({
      name: config.research?.sensitivityTrainName || 'history_plus_jan',
      rows: splitBundle.sensitivityTrain,
      tests: splitBundle.tests,
    });
  }
  trainModes.forEach((trainMode) => {
    runForTrainMode({
      out,
      rows,
      config,
      trainMode: trainMode.name,
      trainRows: trainMode.rows,
      tests: trainMode.tests,
      requestedStrategies,
      includeDiagnostics,
    });
  });
  if (requestedStrategies.includes('walk_forward')) {
    splitBundle.tests.forEach((test, index) => {
      const trainRows = walkForwardTrainRows(rows, config, index);
      runForTrainMode({
        out,
        rows,
        config,
        trainMode: 'walk_forward_expanding',
        trainRows,
        tests: [test],
        requestedStrategies: requestedStrategies.filter((strategy) => strategy !== 'walk_forward'),
        includeDiagnostics,
      });
    });
  }
  return {
    ...out,
    promising: markPhase2Promising(out.results, config),
  };
}

module.exports = {
  PHASE2_STRATEGIES,
  applyMagnitudeGate,
  computeRegimeDiagnostics,
  runPhase2Suite,
  splitRowsForValidation,
};
