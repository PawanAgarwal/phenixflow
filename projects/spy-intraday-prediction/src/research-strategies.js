const { computePolicyBacktest } = require('./backtest');
const {
  computePredictionMetrics,
  compactMetrics,
} = require('./metrics');
const {
  createExamples,
  filterFeatureColumns,
  fitLogisticRegression,
  fitScaler,
  predictBaseline,
  predictExamples,
  predictLinear,
  sigmoid,
  trainExperiment,
  transformExample,
} = require('./models');
const { splitRowsByConfig } = require('./splits');

const STRATEGIES = Object.freeze([
  'cross_sectional',
  'eod_momentum',
  'opening_option_flow',
  'gamma_regime',
  'meta_labeling',
  'volatility_magnitude',
]);

function predictionRecord({ experiment, horizonName, featureSet, splitName, trainMode, prediction }) {
  return {
    experiment,
    horizon: horizonName,
    featureSet,
    split: splitName,
    trainMode,
    rowId: prediction.row.rowId,
    tradeDate: prediction.row.tradeDate,
    minuteUtc: prediction.row.minuteUtc,
    spyClose: prediction.row.spy_close,
    actualReturn: prediction.actualReturn,
    actualDirection: prediction.actualDirection,
    predictedDirection: prediction.predictedDirection,
    directionProbability: prediction.directionProbability,
    predictedReturn: prediction.predictedReturn,
    confidence: prediction.confidence,
  };
}

function finiteRows(rows, labelName) {
  const labelKey = `label_${labelName}_return`;
  return rows.filter((row) => Number.isFinite(row[labelKey]));
}

function rowsAtMinute(rows, minuteOfDayEt) {
  return rows.filter((row) => row.minuteOfDayEt === minuteOfDayEt);
}

function rowsFromMinute(rows, minuteOfDayEt) {
  return rows.filter((row) => row.minuteOfDayEt >= minuteOfDayEt);
}

function addExperimentResult(out, {
  config,
  trainMode,
  strategy,
  experiment,
  horizonName,
  featureSet,
  trainRows,
  testWindow,
  testRows,
  model,
  predictions,
  baselinePredictions,
  runPolicy = true,
}) {
  const metrics = compactMetrics(computePredictionMetrics(predictions));
  const baselineMetrics = compactMetrics(computePredictionMetrics(baselinePredictions));
  const backtest = runPolicy ? computePolicyBacktest(predictions, { ...config.execution, horizonName }) : null;
  const baselineBacktest = runPolicy ? computePolicyBacktest(baselinePredictions, { ...config.execution, horizonName }) : null;
  const record = {
    trainMode,
    strategy,
    experiment,
    horizon: horizonName,
    featureSet,
    split: testWindow.name,
    trainRows: model.trainRowCount,
    testRows: predictions.length,
    featureCount: model.featureColumns.length,
    metrics,
    baselineMetrics,
    backtest: backtest ? compactBacktest(backtest) : null,
    baselineBacktest: baselineBacktest ? compactBacktest(baselineBacktest) : null,
  };
  out.results.push(record);
  predictions.forEach((prediction) => out.predictions.push(predictionRecord({
    experiment,
    horizonName,
    featureSet,
    splitName: testWindow.name,
    trainMode,
    prediction,
  })));
  baselinePredictions.forEach((prediction) => out.predictions.push(predictionRecord({
    experiment: `${experiment}_baseline`,
    horizonName,
    featureSet,
    splitName: testWindow.name,
    trainMode,
    prediction,
  })));
  return record;
}

function compactBacktest(result) {
  function round(value) {
    return typeof value === 'number' && Number.isFinite(value) ? Number(value.toFixed(6)) : value;
  }
  return {
    observations: result.observations,
    inputObservations: result.inputObservations,
    executionPolicy: result.executionPolicy,
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

function runStandardStrategy({ out, config, splitBundle, allFeatureColumns, trainMode, trainRows, strategy, featureSet, horizons, rowFilter = (rows) => rows }) {
  const featureColumns = filterFeatureColumns(allFeatureColumns, featureSet);
  horizons.forEach((horizonName) => {
    const filteredTrain = finiteRows(rowFilter(trainRows), horizonName);
    if (!filteredTrain.length || !featureColumns.length) return;
    const model = trainExperiment(filteredTrain, horizonName, featureColumns);
    splitBundle.tests.forEach(({ window, rows }) => {
      const filteredTest = finiteRows(rowFilter(rows), horizonName);
      if (!filteredTest.length) return;
      const predictions = predictExamples(model, filteredTest);
      const baselinePredictions = predictBaseline(model, filteredTest);
      addExperimentResult(out, {
        config,
        trainMode,
        strategy,
        experiment: `${strategy}_${featureSet}_logistic_ridge`,
        horizonName,
        featureSet,
        trainRows: filteredTrain,
        testWindow: window,
        testRows: filteredTest,
        model,
        predictions,
        baselinePredictions,
      });
    });
  });
}

function makeHighMagnitudeRows(rows, sourceLabel, threshold, targetLabel) {
  return rows
    .filter((row) => Number.isFinite(row[`label_${sourceLabel}_return`]))
    .map((row) => ({
      ...row,
      [`label_${targetLabel}_return`]: row[`label_${sourceLabel}_return`] >= threshold ? 1 : 0,
    }));
}

function runVolatilityMagnitude({ out, config, splitBundle, allFeatureColumns, trainMode, trainRows }) {
  const featureColumns = filterFeatureColumns(allFeatureColumns, 'gamma_regime_research')
    .concat(filterFeatureColumns(allFeatureColumns, 'cross_sectional_research'))
    .filter((column, index, array) => array.indexOf(column) === index);
  ['abs_return_5m', 'abs_return_30m', 'abs_return_60m', 'abs_return_eod'].forEach((sourceLabel) => {
    const trainAbs = trainRows
      .map((row) => row[`label_${sourceLabel}_return`])
      .filter((value) => Number.isFinite(value))
      .sort((left, right) => left - right);
    if (trainAbs.length < 20 || !featureColumns.length) return;
    const threshold = trainAbs[Math.floor(trainAbs.length * 0.7)];
    const targetLabel = `high_${sourceLabel}`;
    const preparedTrain = makeHighMagnitudeRows(trainRows, sourceLabel, threshold, targetLabel);
    const model = trainExperiment(preparedTrain, targetLabel, featureColumns);
    splitBundle.tests.forEach(({ window, rows }) => {
      const preparedTest = makeHighMagnitudeRows(rows, sourceLabel, threshold, targetLabel);
      if (!preparedTest.length) return;
      const predictions = predictExamples(model, preparedTest);
      const baselinePredictions = predictBaseline(model, preparedTest);
      addExperimentResult(out, {
        config,
        trainMode,
        strategy: 'volatility_magnitude',
        experiment: `volatility_magnitude_${sourceLabel}_high_move`,
        horizonName: targetLabel,
        featureSet: 'volatility_magnitude_research',
        trainRows: preparedTrain,
        testWindow: window,
        testRows: preparedTest,
        model,
        predictions,
        baselinePredictions,
        runPolicy: false,
      });
    });
  });
}

function trainMetaModel(primaryModel, trainRows, featureColumns) {
  const primaryTrain = predictExamples(primaryModel, trainRows);
  const rawExamples = primaryTrain.map((prediction) => ({
    row: prediction.row,
    x: featureColumns
      .map((column) => (Number.isFinite(prediction.row[column]) ? prediction.row[column] : 0))
      .concat([prediction.directionProbability, prediction.confidence, prediction.predictedReturn]),
    yDirection: prediction.predictedDirection === prediction.actualDirection ? 1 : 0,
    yReturn: prediction.predictedDirection === prediction.actualDirection ? 1 : 0,
    primary: prediction,
  }));
  const scaler = fitScaler(rawExamples);
  const examples = rawExamples.map((example) => ({ ...example, z: transformExample(example, scaler) }));
  const logistic = fitLogisticRegression(examples, { iterations: 120, learningRate: 0.05, l2: 0.001 });
  return { scaler, logistic, featureColumns, horizonName: primaryModel.horizonName };
}

function predictMetaModel(primaryModel, metaModel, rows, threshold = 0.55) {
  const primaryPredictions = predictExamples(primaryModel, rows);
  return primaryPredictions.map((prediction) => {
    const x = metaModel.featureColumns
      .map((column) => (Number.isFinite(prediction.row[column]) ? prediction.row[column] : 0))
      .concat([prediction.directionProbability, prediction.confidence, prediction.predictedReturn]);
    const z = transformExample({ x }, metaModel.scaler);
    const acceptProbability = sigmoid(predictLinear(metaModel.logistic.weights, z));
    const accepted = acceptProbability >= threshold;
    return {
      ...prediction,
      directionProbability: accepted ? prediction.directionProbability : 0.5,
      predictedReturn: accepted ? prediction.predictedReturn : 0,
      predictedDirection: accepted ? prediction.predictedDirection : 0,
      confidence: accepted ? Math.max(prediction.confidence, acceptProbability) : 0.5,
      metaAcceptProbability: acceptProbability,
    };
  });
}

function runMetaLabeling({ out, config, splitBundle, allFeatureColumns, trainMode, trainRows }) {
  const featureColumns = filterFeatureColumns(allFeatureColumns, 'cross_sectional_research');
  const filteredTrain = finiteRows(trainRows, 'next_5m');
  if (filteredTrain.length < 50 || !featureColumns.length) return;
  const primaryModel = trainExperiment(filteredTrain, 'next_5m', featureColumns);
  const metaModel = trainMetaModel(primaryModel, filteredTrain, featureColumns);
  splitBundle.tests.forEach(({ window, rows }) => {
    const filteredTest = finiteRows(rows, 'next_5m');
    if (!filteredTest.length) return;
    const predictions = predictMetaModel(primaryModel, metaModel, filteredTest);
    const baselinePredictions = predictBaseline(primaryModel, filteredTest);
    const model = {
      ...primaryModel,
      featureColumns: featureColumns.concat(['primary_probability', 'primary_confidence', 'primary_return']),
    };
    addExperimentResult(out, {
      config,
      trainMode,
      strategy: 'meta_labeling',
      experiment: 'meta_labeling_cross_sectional_next_5m',
      horizonName: 'next_5m',
      featureSet: 'meta_labeling_research',
      trainRows: filteredTrain,
      testWindow: window,
      testRows: filteredTest,
      model,
      predictions,
      baselinePredictions,
    });
  });
}

function markPromising(results, config) {
  const thresholds = config.research?.promisingThresholds || {};
  const minLift = thresholds.minBalancedAccuracyLift ?? 0.02;
  const minMonths = thresholds.minPositiveHoldoutMonths ?? 2;
  const grouped = new Map();
  results.forEach((result) => {
    const key = `${result.trainMode}|${result.strategy}|${result.experiment}|${result.horizon}`;
    const list = grouped.get(key) || [];
    list.push(result);
    grouped.set(key, list);
  });
  const promising = [];
  grouped.forEach((items, key) => {
    const accuracyMonths = items.filter((item) => (
      (item.metrics.balancedAccuracy || 0) - (item.baselineMetrics.balancedAccuracy || 0) >= minLift
    )).length;
    const policyItems = items.filter((item) => item.backtest);
    const positiveMonths = policyItems.filter((item) => (item.backtest.totalReturn || 0) > 0).length;
    const highConfidenceEdge = items.some((item) => (item.metrics.confidenceBuckets || []).some((bucket) => (
      bucket.count >= (thresholds.minHighConfidenceBucketCount || 25)
      && bucket.accuracy !== null
      && bucket.accuracy >= 0.57
    )));
    if (policyItems.length && (accuracyMonths >= minMonths || highConfidenceEdge) && positiveMonths >= minMonths) {
      promising.push({ key, accuracyMonths, positiveMonths, highConfidenceEdge, items });
    }
  });
  return promising;
}

function runResearchSuite(rows, config, requestedStrategies = STRATEGIES) {
  const splitBundle = splitRowsByConfig(rows, config);
  const allFeatureColumns = require('./models').selectNumericFeatureColumns(splitBundle.train);
  const trainModes = [
    { name: config.research?.officialTrainName || 'jan_only', rows: splitBundle.train },
  ];
  if (splitBundle.sensitivityTrain?.length) {
    trainModes.push({ name: config.research?.sensitivityTrainName || 'history_plus_jan', rows: splitBundle.sensitivityTrain });
  }
  const out = { results: [], predictions: [] };
  trainModes.forEach(({ name: trainMode, rows: trainRows }) => {
    if (requestedStrategies.includes('cross_sectional')) {
      runStandardStrategy({
        out,
        config,
        splitBundle,
        allFeatureColumns,
        trainMode,
        trainRows,
        strategy: 'cross_sectional',
        featureSet: 'cross_sectional_research',
        horizons: ['next_5m', 'next_60m'],
      });
    }
    if (requestedStrategies.includes('eod_momentum')) {
      runStandardStrategy({
        out,
        config,
        splitBundle,
        allFeatureColumns,
        trainMode,
        trainRows,
        strategy: 'eod_momentum',
        featureSet: 'eod_momentum_research',
        horizons: ['last_30m', 'eod_close'],
        rowFilter: (inputRows) => rowsAtMinute(inputRows, config.research?.lastThirtyEntryMinuteEt || 930),
      });
    }
    if (requestedStrategies.includes('opening_option_flow')) {
      runStandardStrategy({
        out,
        config,
        splitBundle,
        allFeatureColumns,
        trainMode,
        trainRows,
        strategy: 'opening_option_flow',
        featureSet: 'opening_option_flow_research',
        horizons: ['eod_close'],
        rowFilter: (inputRows) => rowsAtMinute(inputRows, config.session.regularOpenMinuteEt + (config.research?.openingWindowMinutes || 30)),
      });
    }
    if (requestedStrategies.includes('gamma_regime')) {
      runStandardStrategy({
        out,
        config,
        splitBundle,
        allFeatureColumns,
        trainMode,
        trainRows,
        strategy: 'gamma_regime',
        featureSet: 'gamma_regime_research',
        horizons: ['next_5m', 'next_60m', 'eod_close'],
        rowFilter: (inputRows) => rowsFromMinute(inputRows, config.session.regularOpenMinuteEt + (config.research?.openingWindowMinutes || 30)),
      });
    }
    if (requestedStrategies.includes('meta_labeling')) {
      runMetaLabeling({ out, config, splitBundle, allFeatureColumns, trainMode, trainRows });
    }
    if (requestedStrategies.includes('volatility_magnitude')) {
      runVolatilityMagnitude({ out, config, splitBundle, allFeatureColumns, trainMode, trainRows });
    }
  });
  return {
    ...out,
    promising: markPromising(out.results, config),
  };
}

module.exports = {
  STRATEGIES,
  runResearchSuite,
  markPromising,
};
