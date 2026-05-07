function isFiniteNumber(value) {
  return typeof value === 'number' && Number.isFinite(value);
}

function sigmoid(value) {
  if (value >= 0) {
    const z = Math.exp(-value);
    return 1 / (1 + z);
  }
  const z = Math.exp(value);
  return z / (1 + z);
}

function selectNumericFeatureColumns(rows, { minFiniteShare = 0.2 } = {}) {
  const counts = new Map();
  const total = rows.length || 1;
  rows.forEach((row) => {
    Object.entries(row).forEach(([key, value]) => {
      if (
        key === 'rowId'
        || key === 'tradeDate'
        || key === 'minuteUtc'
        || key === 'minuteMs'
        || key.startsWith('label_')
      ) {
        return;
      }
      if (isFiniteNumber(value)) counts.set(key, (counts.get(key) || 0) + 1);
    });
  });
  return [...counts.entries()]
    .filter(([, count]) => count / total >= minFiniteShare)
    .map(([key]) => key)
    .sort();
}

function filterFeatureColumns(columns, featureSet) {
  const keepTime = (column) => ['minuteOfDayEt', 'minutes_from_open', 'minutes_to_close'].includes(column);
  const keepSpyMicro = (column) => [
    'spy_ret_1m',
    'spy_ret_5m',
    'spy_ret_15m',
    'spy_ret_60m',
    'spy_range_pct',
    'spy_volume_log',
  ].includes(column);

  if (featureSet === 'price_only') {
    return columns.filter((column) => keepTime(column) || column.startsWith('spy_'));
  }
  if (featureSet === 'cross_asset') {
    return columns.filter((column) => !column.startsWith('opt_'));
  }
  if (featureSet === 'options_plus_spy') {
    return columns.filter((column) => keepTime(column) || keepSpyMicro(column) || column.startsWith('opt_'));
  }
  if (featureSet === 'cross_sectional_research') {
    return columns.filter((column) => (
      keepTime(column)
      || column.includes('_ret_')
      || column.includes('_breadth_')
      || column.includes('_rel_spy_')
      || column.includes('_volume_log')
    ) && !column.startsWith('opt_') && !column.startsWith('opening_opt_') && !column.startsWith('gamma_'));
  }
  if (featureSet === 'eod_momentum_research') {
    return columns.filter((column) => (
      keepTime(column)
      || [
        'opening_30m_return',
        'opening_30m_complete',
        'opening_30m_range_pct',
        'preclose_1500_1530_return',
        'preclose_1500_1530_complete',
        'spy_ret_60m',
        'spy_rv_30m',
        'spy_rv_60m',
        'vix_ret_1m',
        'vix1d_over_vix',
        'vix9d_over_vix',
      ].includes(column)
    ));
  }
  if (featureSet === 'opening_option_flow_research') {
    return columns.filter((column) => (
      keepTime(column)
      || column.startsWith('opening_opt_')
      || [
        'opening_option_proxy_ready',
        'opening_30m_return',
        'opening_30m_range_pct',
        'vix1d_over_vix',
        'vix9d_over_vix',
        'spy_rv_30m',
      ].includes(column)
    ));
  }
  if (featureSet === 'gamma_regime_research') {
    return columns.filter((column) => (
      keepTime(column)
      || column.startsWith('gamma_proxy_')
      || column.startsWith('opt_spx_')
      || column.startsWith('opt_spy_')
      || column.startsWith('vix')
      || [
        'spy_rv_5m',
        'spy_rv_15m',
        'spy_rv_30m',
        'spy_rv_60m',
        'opening_30m_return',
      ].includes(column)
    ));
  }
  return columns.slice();
}

function createExamples(rows, horizonName, featureColumns) {
  const labelKey = `label_${horizonName}_return`;
  return rows
    .filter((row) => isFiniteNumber(row[labelKey]))
    .map((row) => ({
      row,
      x: featureColumns.map((column) => (isFiniteNumber(row[column]) ? row[column] : 0)),
      yReturn: row[labelKey],
      yDirection: row[labelKey] > 0 ? 1 : 0,
    }));
}

function fitScaler(examples) {
  const width = examples[0]?.x.length || 0;
  const mean = Array(width).fill(0);
  const variance = Array(width).fill(0);
  examples.forEach((example) => {
    example.x.forEach((value, index) => {
      mean[index] += value;
    });
  });
  for (let index = 0; index < width; index += 1) mean[index] /= Math.max(1, examples.length);
  examples.forEach((example) => {
    example.x.forEach((value, index) => {
      const delta = value - mean[index];
      variance[index] += delta * delta;
    });
  });
  const scale = variance.map((value) => {
    const std = Math.sqrt(value / Math.max(1, examples.length));
    return std > 1e-12 ? std : 1;
  });
  return { mean, scale };
}

function transformExample(example, scaler) {
  return example.x.map((value, index) => (value - scaler.mean[index]) / scaler.scale[index]);
}

function transformRows(rows, horizonName, featureColumns, scaler) {
  return createExamples(rows, horizonName, featureColumns).map((example) => ({
    ...example,
    z: transformExample(example, scaler),
  }));
}

function fitLogisticRegression(examples, {
  iterations = 140,
  learningRate = 0.06,
  l2 = 0.001,
} = {}) {
  const width = examples[0]?.z.length || 0;
  const weights = Array(width + 1).fill(0);
  if (!examples.length) return { weights };

  for (let iteration = 0; iteration < iterations; iteration += 1) {
    const grad = Array(width + 1).fill(0);
    examples.forEach((example) => {
      let score = weights[0];
      for (let index = 0; index < width; index += 1) score += weights[index + 1] * example.z[index];
      const error = sigmoid(score) - example.yDirection;
      grad[0] += error;
      for (let index = 0; index < width; index += 1) grad[index + 1] += error * example.z[index];
    });
    const step = learningRate / examples.length;
    weights[0] -= step * grad[0];
    for (let index = 0; index < width; index += 1) {
      weights[index + 1] -= learningRate * ((grad[index + 1] / examples.length) + (l2 * weights[index + 1]));
    }
  }

  return { weights };
}

function fitLinearReturnModel(examples, {
  iterations = 180,
  learningRate = 0.025,
  l2 = 0.001,
  responseScale = 10_000,
} = {}) {
  const width = examples[0]?.z.length || 0;
  const weights = Array(width + 1).fill(0);
  if (!examples.length) return { weights, responseScale };

  for (let iteration = 0; iteration < iterations; iteration += 1) {
    const grad = Array(width + 1).fill(0);
    examples.forEach((example) => {
      let score = weights[0];
      for (let index = 0; index < width; index += 1) score += weights[index + 1] * example.z[index];
      const error = score - (example.yReturn * responseScale);
      grad[0] += error;
      for (let index = 0; index < width; index += 1) grad[index + 1] += error * example.z[index];
    });
    weights[0] -= learningRate * (grad[0] / examples.length);
    for (let index = 0; index < width; index += 1) {
      weights[index + 1] -= learningRate * ((grad[index + 1] / examples.length) + (l2 * weights[index + 1]));
    }
  }

  return { weights, responseScale };
}

function predictLinear(weights, z) {
  let score = weights[0] || 0;
  for (let index = 0; index < z.length; index += 1) score += (weights[index + 1] || 0) * z[index];
  return score;
}

function fitBaseline(examples) {
  const positive = examples.filter((example) => example.yDirection === 1).length;
  const meanReturn = examples.reduce((sum, example) => sum + example.yReturn, 0) / Math.max(1, examples.length);
  return {
    positiveProbability: examples.length ? positive / examples.length : 0.5,
    meanReturn: Number.isFinite(meanReturn) ? meanReturn : 0,
  };
}

function trainExperiment(trainRows, horizonName, featureColumns, settings = {}) {
  const rawTrain = createExamples(trainRows, horizonName, featureColumns);
  const scaler = fitScaler(rawTrain);
  const train = rawTrain.map((example) => ({ ...example, z: transformExample(example, scaler) }));
  const baseline = fitBaseline(train);
  const logistic = fitLogisticRegression(train, settings.logistic);
  const linear = fitLinearReturnModel(train, settings.linear);
  return {
    horizonName,
    featureColumns,
    scaler,
    baseline,
    logistic,
    linear,
    trainRowCount: train.length,
  };
}

function predictExamples(model, rows) {
  const examples = transformRows(rows, model.horizonName, model.featureColumns, model.scaler);
  return examples.map((example) => {
    const directionProbability = sigmoid(predictLinear(model.logistic.weights, example.z));
    const predictedReturn = predictLinear(model.linear.weights, example.z) / model.linear.responseScale;
    return {
      row: example.row,
      actualReturn: example.yReturn,
      actualDirection: example.yDirection,
      directionProbability,
      predictedReturn,
      predictedDirection: directionProbability >= 0.5 ? 1 : 0,
      confidence: Math.max(directionProbability, 1 - directionProbability),
    };
  });
}

function predictBaseline(model, rows) {
  const examples = createExamples(rows, model.horizonName, model.featureColumns);
  return examples.map((example) => ({
    row: example.row,
    actualReturn: example.yReturn,
    actualDirection: example.yDirection,
    directionProbability: model.baseline.positiveProbability,
    predictedReturn: model.baseline.meanReturn,
    predictedDirection: model.baseline.positiveProbability >= 0.5 ? 1 : 0,
    confidence: Math.max(model.baseline.positiveProbability, 1 - model.baseline.positiveProbability),
  }));
}

module.exports = {
  isFiniteNumber,
  sigmoid,
  selectNumericFeatureColumns,
  filterFeatureColumns,
  createExamples,
  fitScaler,
  transformExample,
  transformRows,
  fitLogisticRegression,
  fitLinearReturnModel,
  predictLinear,
  trainExperiment,
  predictExamples,
  predictBaseline,
};
