const { computePolicyBacktest, selectPredictionsByHorizon } = require('./backtest');
const {
  filterFeatureColumns,
  isFiniteNumber,
  predictExamples,
  selectNumericFeatureColumns,
  trainExperiment,
} = require('./models');
const { applyMagnitudeGate, splitRowsForValidation } = require('./phase2-research');
const { runValidationBacktest } = require('./signal-validation');

const HORIZON_NAME = 'next_60m';
const MAGNITUDE_LABEL = 'abs_return_60m';
const FEATURE_SET = 'gamma_regime_research';
const THRESHOLD_CANDIDATES = Object.freeze([0.5, 0.55, 0.6, 0.65, 0.7, 0.75, 0.8, 0.85, 0.9]);
const MAX_DIRECTION_FEATURES = 90;
const MAX_MAGNITUDE_FEATURES = 120;

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

function trainFast(rows, horizonName, featureColumns, settings = {}) {
  return trainExperiment(rows, horizonName, featureColumns, {
    logistic: {
      iterations: settings.logisticIterations || 80,
      learningRate: settings.logisticLearningRate || 0.055,
      l2: settings.logisticL2 || 0.0015,
    },
    linear: {
      iterations: settings.linearIterations || 90,
      learningRate: settings.linearLearningRate || 0.022,
      l2: settings.linearL2 || 0.0015,
    },
  });
}

function rowsFromMinute(rows, minuteOfDayEt) {
  return rows.filter((row) => row.minuteOfDayEt >= minuteOfDayEt);
}

function finiteRows(rows, labelName) {
  const labelKey = `label_${labelName}_return`;
  return rows.filter((row) => isFiniteNumber(row[labelKey]));
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

function magnitudeFeatureColumns(allFeatureColumns) {
  return capFeatureColumns(unique([
    ...filterFeatureColumns(allFeatureColumns, 'cross_sectional_research'),
    ...filterFeatureColumns(allFeatureColumns, 'gamma_regime_research'),
    ...filterFeatureColumns(allFeatureColumns, 'eod_momentum_research'),
    ...filterFeatureColumns(allFeatureColumns, 'opening_option_flow_research'),
  ]), MAX_MAGNITUDE_FEATURES);
}

function buildMagnitudeModel(rows, sourceLabel, featureColumns, targetPrefix = 'monthly_best_high', settings = {}) {
  const values = rows
    .map((row) => row[`label_${sourceLabel}_return`])
    .filter(isFiniteNumber)
    .sort((left, right) => left - right);
  if (values.length < 20) return null;
  const threshold = quantile(values, 0.7);
  const targetLabel = `${targetPrefix}_${sourceLabel}`;
  const preparedRows = makeHighMagnitudeRows(rows, sourceLabel, threshold, targetLabel);
  return {
    threshold,
    targetLabel,
    model: trainFast(preparedRows, targetLabel, featureColumns, settings),
  };
}

function chooseMagnitudeThreshold(basePredictions, magnitudePredictions, config) {
  let best = null;
  const candidates = THRESHOLD_CANDIDATES.map((threshold) => {
    const gated = applyMagnitudeGate(basePredictions, magnitudePredictions, threshold);
    const backtest = computePolicyBacktest(gated, { ...config.execution, horizonName: HORIZON_NAME });
    const activeShare = activePredictionStats(gated, HORIZON_NAME, config.execution.confidenceThreshold).activePredictionCount
      / Math.max(1, selectPredictionsByHorizon(gated, HORIZON_NAME).selected.length);
    const score = activeShare >= 0.02 ? backtest.totalReturn - (Math.abs(backtest.maxDrawdown) * 0.1) : -Infinity;
    const item = {
      threshold,
      score,
      activeShare,
      totalReturn: backtest.totalReturn,
      maxDrawdown: backtest.maxDrawdown,
    };
    if (!best || item.score > best.score) best = item;
    return item;
  });
  if (!best || best.score === -Infinity) {
    best = candidates.slice().sort((left, right) => right.totalReturn - left.totalReturn)[0];
  }
  return {
    selectedThreshold: best.threshold,
    validation: {
      selected: {
        threshold: best.threshold,
        score: round(best.score),
        activeShare: round(best.activeShare),
        totalReturn: round(best.totalReturn),
        maxDrawdown: round(best.maxDrawdown),
      },
      candidates: candidates.map((candidate) => ({
        threshold: candidate.threshold,
        score: round(candidate.score),
        activeShare: round(candidate.activeShare),
        totalReturn: round(candidate.totalReturn),
        maxDrawdown: round(candidate.maxDrawdown),
      })),
    },
  };
}

function monthKey(tradeDate) {
  return tradeDate.slice(0, 7);
}

function monthStart(month) {
  return `${month}-01`;
}

function nextMonth(month) {
  const [year, rawMonth] = month.split('-').map(Number);
  const date = new Date(Date.UTC(year, rawMonth - 1, 1));
  date.setUTCMonth(date.getUTCMonth() + 1);
  return `${date.getUTCFullYear()}-${String(date.getUTCMonth() + 1).padStart(2, '0')}`;
}

function monthsInRows(rows) {
  return unique(rows.map((row) => monthKey(row.tradeDate)).filter(Boolean)).sort();
}

function rowsBeforeMonth(rows, month) {
  return rows.filter((row) => row.tradeDate < monthStart(month));
}

function rowsForMonth(rows, month) {
  const start = monthStart(month);
  const end = monthStart(nextMonth(month));
  return rows.filter((row) => row.tradeDate >= start && row.tradeDate < end);
}

function rowsForDate(rows, tradeDate) {
  return rows.filter((row) => row.tradeDate === tradeDate);
}

function rowsBeforeDate(rows, tradeDate) {
  return rows.filter((row) => row.tradeDate < tradeDate);
}

function rowsInDateRange(rows, startDate, endDate) {
  return rows.filter((row) => row.tradeDate >= startDate && row.tradeDate <= endDate);
}

function uniqueTradeDates(rows) {
  return unique(rows.map((row) => row.tradeDate).filter(Boolean)).sort();
}

function sortRows(rows) {
  return rows.slice().sort((left, right) => {
    if (left.tradeDate !== right.tradeDate) return left.tradeDate.localeCompare(right.tradeDate);
    return String(left.minuteUtc).localeCompare(String(right.minuteUtc));
  });
}

function capTrainingRows(rows, maxTrainRows) {
  if (!maxTrainRows || rows.length <= maxTrainRows) return rows;
  const sorted = sortRows(rows);
  const step = Math.ceil(sorted.length / maxTrainRows);
  return sorted.filter((_, index) => index % step === 0).slice(0, maxTrainRows);
}

function positionFor(prediction, confidenceThreshold, positionMode = 'long_short') {
  let position = 0;
  if (prediction.directionProbability >= confidenceThreshold) position = 1;
  if (prediction.directionProbability <= 1 - confidenceThreshold) position = -1;
  if (positionMode === 'long_cash' && position < 0) position = 0;
  return position;
}

function activePredictionStats(predictions, horizonName, confidenceThreshold, positionMode = 'long_short') {
  const selected = selectPredictionsByHorizon(predictions, horizonName).selected;
  const active = selected
    .map((prediction) => ({ prediction, position: positionFor(prediction, confidenceThreshold, positionMode) }))
    .filter((item) => item.position !== 0);
  const success = active.filter((item) => item.position * item.prediction.actualReturn > 0).length;
  const failed = active.length - success;
  const longCount = active.filter((item) => item.position === 1).length;
  const shortCount = active.filter((item) => item.position === -1).length;
  return {
    observations: selected.length,
    activePredictionCount: active.length,
    succeeded: success,
    failed,
    successRatePct: active.length ? round((success / active.length) * 100, 2) : null,
    failureRatePct: active.length ? round((failed / active.length) * 100, 2) : null,
    abstained: selected.length - active.length,
    activeSharePct: selected.length ? round((active.length / selected.length) * 100, 2) : null,
    longCount,
    shortCount,
  };
}

function compactBacktest(result) {
  return {
    observations: result.observations,
    totalReturn: round(result.totalReturn),
    buyAndHoldReturn: round(result.buyAndHoldReturn),
    maxDrawdown: round(result.maxDrawdown),
    turnover: round(result.turnover),
    longShare: round(result.longShare),
    shortShare: round(result.shortShare),
    cashShare: round(result.cashShare),
  };
}

function runLongOnlyBacktest(predictions, config) {
  const result = runValidationBacktest(predictions, {
    ...config.execution,
    horizonName: HORIZON_NAME,
    positionMode: 'long_cash',
  });
  return {
    observations: result.observations,
    totalReturn: round(result.totalReturn),
    maxDrawdown: round(result.maxDrawdown),
    turnover: round(result.turnover),
    longShare: round(result.longShare),
    shortShare: round(result.shortShare),
    cashShare: round(result.cashShare),
  };
}

function trainBestSignalModel(trainRowsRaw, config, settings = {}) {
  const startMinute = config.session.regularOpenMinuteEt + (config.research?.openingWindowMinutes || 30);
  const filteredTrainAll = finiteRows(rowsFromMinute(trainRowsRaw, startMinute), HORIZON_NAME);
  const filteredTrain = capTrainingRows(filteredTrainAll, settings.maxTrainRows);
  const minTrainRows = settings.minTrainRows || 5_000;
  if (filteredTrain.length < minTrainRows) {
    return {
      status: 'skipped',
      reason: `insufficient_training_rows:${filteredTrain.length}`,
      trainRows: filteredTrain.length,
      uncappedTrainRows: filteredTrainAll.length,
    };
  }
  const modelSettings = settings.modelSettings || {};
  const allFeatureColumns = selectNumericFeatureColumns(filteredTrain);
  const directionColumns = capFeatureColumns(filterFeatureColumns(allFeatureColumns, FEATURE_SET), MAX_DIRECTION_FEATURES);
  const magColumns = magnitudeFeatureColumns(allFeatureColumns);
  let selected = null;
  if (Number.isFinite(settings.fixedMagnitudeThreshold)) {
    selected = {
      selectedThreshold: settings.fixedMagnitudeThreshold,
      validation: {
        mode: 'fixed_threshold',
        selected: { threshold: settings.fixedMagnitudeThreshold },
        candidates: [],
      },
    };
  } else {
    const { fitRows, validationRows } = splitRowsForValidation(filteredTrain, settings.validationShare || 0.25);
    const validationMagnitude = buildMagnitudeModel(
      fitRows,
      MAGNITUDE_LABEL,
      magColumns,
      'monthly_validation_high',
      modelSettings,
    );
    if (!validationMagnitude) {
      return {
        status: 'skipped',
        reason: 'could_not_train_validation_magnitude_model',
        trainRows: filteredTrain.length,
        uncappedTrainRows: filteredTrainAll.length,
      };
    }
    const validationBase = trainFast(fitRows, HORIZON_NAME, directionColumns, modelSettings);
    const validationBasePredictions = predictExamples(validationBase, validationRows);
    const validationMagnitudeRows = makeHighMagnitudeRows(
      validationRows,
      MAGNITUDE_LABEL,
      validationMagnitude.threshold,
      validationMagnitude.targetLabel,
    );
    const validationMagnitudePredictions = predictExamples(validationMagnitude.model, validationMagnitudeRows);
    selected = chooseMagnitudeThreshold(validationBasePredictions, validationMagnitudePredictions, config);
  }

  const finalBase = trainFast(filteredTrain, HORIZON_NAME, directionColumns, modelSettings);
  const finalMagnitude = buildMagnitudeModel(
    filteredTrain,
    MAGNITUDE_LABEL,
    magColumns,
    'monthly_final_high',
    modelSettings,
  );
  if (!finalMagnitude) {
    return {
      status: 'skipped',
      reason: 'could_not_train_final_magnitude_model',
      trainRows: filteredTrain.length,
      uncappedTrainRows: filteredTrainAll.length,
    };
  }
  return {
    status: 'trained',
    trainRows: filteredTrain.length,
    uncappedTrainRows: filteredTrainAll.length,
    directionFeatureCount: directionColumns.length,
    magnitudeFeatureCount: magColumns.length,
    selectedMagnitudeThreshold: selected.selectedThreshold,
    magnitudeLabelThreshold: round(finalMagnitude.threshold, 8),
    validation: selected.validation,
    baseModel: finalBase,
    magnitudeModel: finalMagnitude.model,
    magnitudeTargetLabel: finalMagnitude.targetLabel,
    magnitudeSourceThreshold: finalMagnitude.threshold,
  };
}

function scoreBestSignalModel(modelBundle, testRowsRaw, config) {
  const startMinute = config.session.regularOpenMinuteEt + (config.research?.openingWindowMinutes || 30);
  const filteredTest = finiteRows(rowsFromMinute(testRowsRaw, startMinute), HORIZON_NAME);
  if (!filteredTest.length) {
    return {
      status: 'skipped',
      reason: 'no_test_rows',
      testRows: 0,
      predictions: [],
    };
  }
  const basePredictions = predictExamples(modelBundle.baseModel, filteredTest);
  const magnitudeRows = makeHighMagnitudeRows(
    filteredTest,
    MAGNITUDE_LABEL,
    modelBundle.magnitudeSourceThreshold,
    modelBundle.magnitudeTargetLabel,
  );
  const magnitudePredictions = predictExamples(modelBundle.magnitudeModel, magnitudeRows);
  const predictions = applyMagnitudeGate(basePredictions, magnitudePredictions, modelBundle.selectedMagnitudeThreshold);
  return {
    status: 'scored',
    testRows: filteredTest.length,
    predictions,
  };
}

function summarizePredictionSet(predictions, config) {
  const backtest = computePolicyBacktest(predictions, { ...config.execution, horizonName: HORIZON_NAME });
  return {
    longShort: activePredictionStats(predictions, HORIZON_NAME, config.execution.confidenceThreshold, 'long_short'),
    longOnly: activePredictionStats(predictions, HORIZON_NAME, config.execution.confidenceThreshold, 'long_cash'),
    backtest: compactBacktest(backtest),
    longOnlyBacktest: runLongOnlyBacktest(predictions, config),
  };
}

function summarizeTotals(months) {
  const tested = months.filter((month) => month.status === 'tested');
  const sum = (selector) => tested.reduce((total, month) => total + selector(month), 0);
  const active = sum((month) => month.longShort.activePredictionCount);
  const succeeded = sum((month) => month.longShort.succeeded);
  const failed = sum((month) => month.longShort.failed);
  const longOnlyActive = sum((month) => month.longOnly.activePredictionCount);
  const longOnlySucceeded = sum((month) => month.longOnly.succeeded);
  const longOnlyFailed = sum((month) => month.longOnly.failed);
  return {
    testedMonths: tested.length,
    skippedMonths: months.length - tested.length,
    activePredictionCount: active,
    succeeded,
    failed,
    successRatePct: active ? round((succeeded / active) * 100, 2) : null,
    failureRatePct: active ? round((failed / active) * 100, 2) : null,
    positiveReturnMonths: tested.filter((month) => month.backtest.totalReturn > 0).length,
    totalReturnSimpleSum: round(tested.reduce((total, month) => total + month.backtest.totalReturn, 0)),
    longOnlyActivePredictionCount: longOnlyActive,
    longOnlySucceeded,
    longOnlyFailed,
    longOnlySuccessRatePct: longOnlyActive ? round((longOnlySucceeded / longOnlyActive) * 100, 2) : null,
    longOnlyFailureRatePct: longOnlyActive ? round((longOnlyFailed / longOnlyActive) * 100, 2) : null,
    longOnlyPositiveReturnMonths: tested.filter((month) => month.longOnlyBacktest.totalReturn > 0).length,
    longOnlyTotalReturnSimpleSum: round(tested.reduce((total, month) => total + month.longOnlyBacktest.totalReturn, 0)),
  };
}

function testMonth(rows, config, month, settings = {}) {
  const trainRowsRaw = settings.trainRowsOverride || rowsBeforeMonth(rows, month);
  const testRowsRaw = rowsForMonth(rows, month);
  const modelBundle = trainBestSignalModel(trainRowsRaw, config, settings);
  if (modelBundle.status !== 'trained') {
    return {
      month,
      status: 'skipped',
      reason: modelBundle.reason,
      trainRows: modelBundle.trainRows,
      uncappedTrainRows: modelBundle.uncappedTrainRows,
      testRows: testRowsRaw.length,
    };
  }
  const scored = scoreBestSignalModel(modelBundle, testRowsRaw, config);
  if (scored.status !== 'scored') {
    return {
      month,
      status: 'skipped',
      reason: scored.reason,
      trainRows: modelBundle.trainRows,
      uncappedTrainRows: modelBundle.uncappedTrainRows,
      testRows: 0,
    };
  }
  const summary = summarizePredictionSet(scored.predictions, config);

  return {
    month,
    status: 'tested',
    trainRows: modelBundle.trainRows,
    uncappedTrainRows: modelBundle.uncappedTrainRows,
    testRows: scored.testRows,
    directionFeatureCount: modelBundle.directionFeatureCount,
    magnitudeFeatureCount: modelBundle.magnitudeFeatureCount,
    selectedMagnitudeThreshold: modelBundle.selectedMagnitudeThreshold,
    magnitudeLabelThreshold: modelBundle.magnitudeLabelThreshold,
    validation: modelBundle.validation,
    ...summary,
  };
}

function runMonthlyBestSignal(rows, config, settings = {}) {
  const startMonth = settings.startMonth || '2025-01';
  const months = monthsInRows(rows).filter((month) => month >= startMonth);
  const monthly = months.map((month, index) => {
    if (settings.onMonthStart) settings.onMonthStart({ month, index: index + 1, total: months.length });
    const result = testMonth(rows, config, month, settings);
    if (settings.onMonthComplete) settings.onMonthComplete(result);
    return result;
  });
  return {
    generatedAt: new Date().toISOString(),
    signal: 'volatility_gated_gamma_regime_next_60m',
    protocol: 'expanding monthly walk-forward; train rows strictly before test month',
    startMonth,
    monthCount: monthly.length,
    summary: summarizeTotals(monthly),
    monthly,
  };
}

function runFrozenModelSweep(rows, config, protocol) {
  const trainRows = rowsInDateRange(rows, protocol.trainStartDate, protocol.trainEndDate);
  const modelBundle = trainBestSignalModel(trainRows, config, protocol.settings || {});
  const months = monthsInRows(rows).filter((month) => month >= (protocol.startMonth || '2025-01'));
  if (modelBundle.status !== 'trained') {
    return {
      protocol: protocol.name,
      status: 'skipped',
      reason: modelBundle.reason,
      trainStartDate: protocol.trainStartDate,
      trainEndDate: protocol.trainEndDate,
      monthly: months.map((month) => ({ month, status: 'skipped', reason: modelBundle.reason })),
      summary: summarizeTotals([]),
    };
  }
  const monthly = months.map((month) => {
    const scored = scoreBestSignalModel(modelBundle, rowsForMonth(rows, month), config);
    if (scored.status !== 'scored') {
      return {
        month,
        status: 'skipped',
        reason: scored.reason,
        trainRows: modelBundle.trainRows,
        uncappedTrainRows: modelBundle.uncappedTrainRows,
        testRows: scored.testRows,
      };
    }
    return {
      month,
      status: 'tested',
      trainRows: modelBundle.trainRows,
      uncappedTrainRows: modelBundle.uncappedTrainRows,
      testRows: scored.testRows,
      selectedMagnitudeThreshold: modelBundle.selectedMagnitudeThreshold,
      magnitudeLabelThreshold: modelBundle.magnitudeLabelThreshold,
      ...summarizePredictionSet(scored.predictions, config),
    };
  });
  return {
    protocol: protocol.name,
    status: 'tested',
    note: protocol.note,
    trainStartDate: protocol.trainStartDate,
    trainEndDate: protocol.trainEndDate,
    trainRows: modelBundle.trainRows,
    uncappedTrainRows: modelBundle.uncappedTrainRows,
    selectedMagnitudeThreshold: modelBundle.selectedMagnitudeThreshold,
    magnitudeLabelThreshold: modelBundle.magnitudeLabelThreshold,
    validation: modelBundle.validation,
    summary: summarizeTotals(monthly),
    monthly,
  };
}

function compactDailyResult(result, modelBundle, scored, config) {
  if (result.status !== 'tested') return result;
  const summary = summarizePredictionSet(scored.predictions, config);
  return {
    tradeDate: result.tradeDate,
    status: 'tested',
    trainRows: modelBundle.trainRows,
    uncappedTrainRows: modelBundle.uncappedTrainRows,
    testRows: scored.testRows,
    selectedMagnitudeThreshold: modelBundle.selectedMagnitudeThreshold,
    longShort: summary.longShort,
    longOnly: summary.longOnly,
    backtest: summary.backtest,
    longOnlyBacktest: summary.longOnlyBacktest,
  };
}

function monthlyFromDaily(daily) {
  const grouped = new Map();
  daily.forEach((day) => {
    const month = monthKey(day.tradeDate);
    const list = grouped.get(month) || [];
    list.push(day);
    grouped.set(month, list);
  });
  return [...grouped.entries()].sort((left, right) => left[0].localeCompare(right[0])).map(([month, days]) => {
    const tested = days.filter((day) => day.status === 'tested');
    const sum = (selector) => tested.reduce((total, day) => total + selector(day), 0);
    const active = sum((day) => day.longShort.activePredictionCount);
    const succeeded = sum((day) => day.longShort.succeeded);
    const failed = sum((day) => day.longShort.failed);
    const longOnlyActive = sum((day) => day.longOnly.activePredictionCount);
    const longOnlySucceeded = sum((day) => day.longOnly.succeeded);
    const longOnlyFailed = sum((day) => day.longOnly.failed);
    return {
      month,
      status: tested.length ? 'tested' : 'skipped',
      testedDays: tested.length,
      skippedDays: days.length - tested.length,
      longShort: {
        activePredictionCount: active,
        succeeded,
        failed,
        successRatePct: active ? round((succeeded / active) * 100, 2) : null,
        failureRatePct: active ? round((failed / active) * 100, 2) : null,
        longCount: sum((day) => day.longShort.longCount),
        shortCount: sum((day) => day.longShort.shortCount),
        abstained: sum((day) => day.longShort.abstained),
      },
      longOnly: {
        activePredictionCount: longOnlyActive,
        succeeded: longOnlySucceeded,
        failed: longOnlyFailed,
        successRatePct: longOnlyActive ? round((longOnlySucceeded / longOnlyActive) * 100, 2) : null,
        failureRatePct: longOnlyActive ? round((longOnlyFailed / longOnlyActive) * 100, 2) : null,
      },
      backtest: {
        totalReturn: round(sum((day) => day.backtest.totalReturn)),
        maxDrawdownWorstDay: round(Math.min(...tested.map((day) => day.backtest.maxDrawdown))),
        positiveDays: tested.filter((day) => day.backtest.totalReturn > 0).length,
      },
      longOnlyBacktest: {
        totalReturn: round(sum((day) => day.longOnlyBacktest.totalReturn)),
        maxDrawdownWorstDay: round(Math.min(...tested.map((day) => day.longOnlyBacktest.maxDrawdown))),
        positiveDays: tested.filter((day) => day.longOnlyBacktest.totalReturn > 0).length,
      },
    };
  });
}

function summarizeDailyMonthly(monthly) {
  const tested = monthly.filter((month) => month.status === 'tested');
  const sum = (selector) => tested.reduce((total, month) => total + selector(month), 0);
  const active = sum((month) => month.longShort.activePredictionCount);
  const succeeded = sum((month) => month.longShort.succeeded);
  const failed = sum((month) => month.longShort.failed);
  const longOnlyActive = sum((month) => month.longOnly.activePredictionCount);
  const longOnlySucceeded = sum((month) => month.longOnly.succeeded);
  const longOnlyFailed = sum((month) => month.longOnly.failed);
  return {
    testedMonths: tested.length,
    activePredictionCount: active,
    succeeded,
    failed,
    successRatePct: active ? round((succeeded / active) * 100, 2) : null,
    failureRatePct: active ? round((failed / active) * 100, 2) : null,
    positiveReturnMonths: tested.filter((month) => month.backtest.totalReturn > 0).length,
    totalReturnSimpleSum: round(sum((month) => month.backtest.totalReturn)),
    longOnlyActivePredictionCount: longOnlyActive,
    longOnlySucceeded,
    longOnlyFailed,
    longOnlySuccessRatePct: longOnlyActive ? round((longOnlySucceeded / longOnlyActive) * 100, 2) : null,
    longOnlyFailureRatePct: longOnlyActive ? round((longOnlyFailed / longOnlyActive) * 100, 2) : null,
    longOnlyPositiveReturnMonths: tested.filter((month) => month.longOnlyBacktest.totalReturn > 0).length,
    longOnlyTotalReturnSimpleSum: round(sum((month) => month.longOnlyBacktest.totalReturn)),
  };
}

function runDailyRetrainSweep(rows, config, protocol) {
  const startDate = protocol.startDate || '2025-02-01';
  const dates = uniqueTradeDates(rows).filter((date) => date >= startDate);
  const daily = [];
  dates.forEach((tradeDate, index) => {
    if (protocol.onDayStart) protocol.onDayStart({ tradeDate, index: index + 1, total: dates.length, protocol: protocol.name });
    const trainRows = rowsBeforeDate(rows, tradeDate);
    const testRows = rowsForDate(rows, tradeDate);
    const modelBundle = trainBestSignalModel(trainRows, config, protocol.settings || {});
    if (modelBundle.status !== 'trained') {
      const result = {
        tradeDate,
        status: 'skipped',
        reason: modelBundle.reason,
        trainRows: modelBundle.trainRows,
        uncappedTrainRows: modelBundle.uncappedTrainRows,
      };
      daily.push(result);
      if (protocol.onDayComplete) protocol.onDayComplete(result);
      return;
    }
    const scored = scoreBestSignalModel(modelBundle, testRows, config);
    if (scored.status !== 'scored') {
      const result = {
        tradeDate,
        status: 'skipped',
        reason: scored.reason,
        trainRows: modelBundle.trainRows,
        uncappedTrainRows: modelBundle.uncappedTrainRows,
        testRows: scored.testRows,
      };
      daily.push(result);
      if (protocol.onDayComplete) protocol.onDayComplete(result);
      return;
    }
    const result = compactDailyResult({ tradeDate, status: 'tested' }, modelBundle, scored, config);
    daily.push(result);
    if (protocol.onDayComplete) protocol.onDayComplete(result);
  });
  const monthly = monthlyFromDaily(daily);
  return {
    protocol: protocol.name,
    status: 'tested',
    note: protocol.note,
    startDate,
    settings: {
      minTrainRows: protocol.settings?.minTrainRows,
      maxTrainRows: protocol.settings?.maxTrainRows,
      fixedMagnitudeThreshold: protocol.settings?.fixedMagnitudeThreshold,
      modelSettings: protocol.settings?.modelSettings,
    },
    summary: summarizeDailyMonthly(monthly),
    monthly,
    daily,
  };
}

function runBestSignalFullHistory(rows, config, settings = {}) {
  const frozenProtocols = settings.frozenProtocols || [
    {
      name: 'frozen_history_plus_jan_anchor',
      trainStartDate: '2025-01-02',
      trainEndDate: '2026-01-30',
      startMonth: '2025-01',
      note: 'Retrospective frozen model trained on 2025-01-02 through 2026-01-30; months before the train end are pattern-stability checks, not causal live claims.',
      settings: {},
    },
    {
      name: 'frozen_walk_forward_final_anchor',
      trainStartDate: '2025-01-02',
      trainEndDate: '2026-03-31',
      startMonth: '2025-01',
      note: 'Retrospective frozen model using the final expanding walk-forward anchor through 2026-03-31.',
      settings: {},
    },
  ];
  const dailyModelSettings = settings.dailyModelSettings || {
    logisticIterations: 35,
    linearIterations: 40,
  };
  const dailyProtocols = settings.dailyProtocols || [
    {
      name: 'daily_expanding_selected_threshold',
      startDate: '2025-02-01',
      note: 'Daily expanding retrain using only prior trading days; threshold is reselected from prior-history validation. Training rows may be capped deterministically for runtime.',
      settings: {
        minTrainRows: 5_000,
        maxTrainRows: settings.dailyMaxTrainRows || 25_000,
        modelSettings: dailyModelSettings,
      },
    },
    {
      name: 'daily_expanding_fixed_0_60_threshold',
      startDate: '2025-02-01',
      note: 'Daily expanding retrain using only prior trading days; gate probability threshold fixed at 0.60 to mirror the survivor threshold.',
      settings: {
        minTrainRows: 5_000,
        maxTrainRows: settings.dailyMaxTrainRows || 25_000,
        fixedMagnitudeThreshold: 0.6,
        modelSettings: dailyModelSettings,
      },
    },
  ];
  return {
    generatedAt: new Date().toISOString(),
    signal: 'volatility_gated_gamma_regime_next_60m',
    note: 'Success/failure percentages count only active long/short predictions after 60-minute horizon sampling and volatility gating; cash/abstain observations are reported separately.',
    frozenSweeps: frozenProtocols.map((protocol) => runFrozenModelSweep(rows, config, protocol)),
    dailyRetrainSweeps: dailyProtocols.map((protocol) => runDailyRetrainSweep(rows, config, {
      ...protocol,
      onDayStart: settings.onDayStart,
      onDayComplete: settings.onDayComplete,
    })),
  };
}

module.exports = {
  activePredictionStats,
  runBestSignalFullHistory,
  runDailyRetrainSweep,
  runFrozenModelSweep,
  runMonthlyBestSignal,
  scoreBestSignalModel,
  trainBestSignalModel,
  testMonth,
};
