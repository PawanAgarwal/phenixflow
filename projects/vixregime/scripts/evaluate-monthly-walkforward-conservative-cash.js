#!/usr/bin/env node
const fs = require('node:fs');
const path = require('node:path');

const ARTIFACT_PATH = path.resolve(
  process.env.ARTIFACT_PATH
    || path.join(process.cwd(), 'vixregime', 'artifacts', 'reports', 'vixregime-backtest-2025-01-02-2026-03-20.json'),
);
const MODEL_PATH = path.resolve(
  process.env.MODEL_PATH
    || path.join(process.cwd(), 'vixregime', 'artifacts', 'reports', 'vixregime-feature-subset-ordinal-model-search.json'),
);
const OUTPUT_DIR = path.resolve(
  process.env.OUTPUT_DIR
    || path.join(process.cwd(), 'vixregime', 'artifacts', 'reports'),
);
const TRAIN_ROWS = Math.max(40, Math.trunc(Number(process.env.TRAIN_ROWS || 63)));

const FEATURE_GROUPS = Object.freeze({
  volCore: ['vix', 'vixPctRank', 'delta5', 'delta10', 'ts9d30', 'ts30d90d', 'ts1d9d', 'vix1dOverVix'],
  priceTrend: [
    'spyRet1', 'spyRet5', 'spyRet10',
    'spyReturn1d', 'spyReturn3d', 'spyReturn5d', 'spyReturn10d',
    'spyMaGap5', 'spyMaGap10', 'spyMaGap20',
  ],
  realized: [
    'spyRealizedVol5', 'spyRealizedVol10', 'spyRealizedVol20',
    'spyDownsideVol10', 'spyDownsideVol20',
    'spyIntradayReturn', 'spyRangePct', 'spyGapFromPrevClose', 'spyCloseLocation',
  ],
  volDelta: [
    'vixChange1', 'termSlopeGap', 'vixChange3d',
    'vix9dChange1d', 'vix1dChange1d', 'vix3mChange1d',
    'ts9d30Delta1', 'ts30d90dDelta1', 'ts1d9dDelta1',
    'vixRiskPremium10', 'vixRiskPremium20',
  ],
  event: [
    'isFomcDay', 'isMonthlyOpex', 'isQuarterlyOpex', 'isCpiDay', 'isPpiDay',
    'isNfpDay', 'isJoltsDay', 'isMacroEventDay', 'eventScore',
  ],
});

const CONSERVATIVE_CASH_POLICY = Object.freeze({
  Calm: 'SPXL',
  Normal: 'SPY',
  Stress: 'CASH',
  Crash: 'CASH',
});

function toNumber(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function mean(values = []) {
  if (!values.length) return 0;
  return values.reduce((sum, value) => sum + value, 0) / values.length;
}

function stdDev(values = []) {
  if (values.length < 2) return 1;
  const avg = mean(values);
  const variance = values.reduce((sum, value) => sum + ((value - avg) ** 2), 0) / (values.length - 1);
  const out = Math.sqrt(Math.max(variance, 0));
  return out > 1e-9 ? out : 1;
}

function computeReturn(currentPrice, nextPrice) {
  const current = toNumber(currentPrice);
  const next = toNumber(nextPrice);
  if (current === null || next === null || current <= 0) return 0;
  return (next / current) - 1;
}

function labelReturn(nextReturn, labelConfig) {
  const ret = toNumber(nextReturn);
  if (ret === null) return null;
  if (ret <= labelConfig.crashMax) return 'Crash';
  if (ret <= labelConfig.stressMax) return 'Stress';
  if (ret < labelConfig.calmMin) return 'Normal';
  return 'Calm';
}

function monthKey(dateString) {
  return String(dateString || '').slice(0, 7);
}

function buildFeatureMap(current, prev1, prev5, prev10) {
  const spyRet1 = computeReturn(prev1.spyClose, current.spyClose);
  const spyRet5 = computeReturn(prev5.spyClose, current.spyClose);
  const spyRet10 = computeReturn(prev10.spyClose, current.spyClose);
  const vixChange1 = toNumber(current.vix) !== null && toNumber(prev1.vix) !== null ? current.vix - prev1.vix : null;
  const termSlopeGap = (toNumber(current.ts9d30) !== null && toNumber(current.ts30d90d) !== null)
    ? current.ts9d30 - current.ts30d90d
    : null;
  return {
    vix: current.vix,
    vixPctRank: current.vixPctRank,
    delta5: current.delta5,
    delta10: current.delta10,
    ts9d30: current.ts9d30,
    ts30d90d: current.ts30d90d,
    ts1d9d: current.ts1d9d,
    vix1dOverVix: current.vix1dOverVix,
    spyRet1,
    spyRet5,
    spyRet10,
    vixChange1,
    termSlopeGap,
    spyReturn1d: current.spyReturn1d,
    spyReturn3d: current.spyReturn3d,
    spyReturn5d: current.spyReturn5d,
    spyReturn10d: current.spyReturn10d,
    spyMaGap5: current.spyMaGap5,
    spyMaGap10: current.spyMaGap10,
    spyMaGap20: current.spyMaGap20,
    spyRealizedVol5: current.spyRealizedVol5,
    spyRealizedVol10: current.spyRealizedVol10,
    spyRealizedVol20: current.spyRealizedVol20,
    spyDownsideVol10: current.spyDownsideVol10,
    spyDownsideVol20: current.spyDownsideVol20,
    spyIntradayReturn: current.spyIntradayReturn,
    spyRangePct: current.spyRangePct,
    spyGapFromPrevClose: current.spyGapFromPrevClose,
    spyCloseLocation: current.spyCloseLocation,
    vixChange3d: current.vixChange3d,
    vix9dChange1d: current.vix9dChange1d,
    vix1dChange1d: current.vix1dChange1d,
    vix3mChange1d: current.vix3mChange1d,
    ts9d30Delta1: current.ts9d30Delta1,
    ts30d90dDelta1: current.ts30d90dDelta1,
    ts1d9dDelta1: current.ts1d9dDelta1,
    vixRiskPremium10: current.vixRiskPremium10,
    vixRiskPremium20: current.vixRiskPremium20,
    isFomcDay: current.isFomcDay ? 1 : 0,
    isMonthlyOpex: current.isMonthlyOpex ? 1 : 0,
    isQuarterlyOpex: current.isQuarterlyOpex ? 1 : 0,
    isCpiDay: current.isCpiDay ? 1 : 0,
    isPpiDay: current.isPpiDay ? 1 : 0,
    isNfpDay: current.isNfpDay ? 1 : 0,
    isJoltsDay: current.isJoltsDay ? 1 : 0,
    isMacroEventDay: current.isMacroEventDay ? 1 : 0,
    eventScore: current.eventScore || 0,
  };
}

function buildVector(featureMap, selectedGroups = [], featureMode = 'base') {
  const names = selectedGroups.flatMap((group) => FEATURE_GROUPS[group] || []);
  const base = names.map((name) => toNumber(featureMap[name]) ?? 0);
  if (featureMode !== 'poly2') return { names, values: base };
  const extras = [];
  const extraNames = [];
  if (selectedGroups.includes('volCore') && selectedGroups.includes('priceTrend')) {
    extras.push((featureMap.vix ?? 0) * (featureMap.spyReturn5d ?? 0));
    extraNames.push('vix_x_spyReturn5d');
    extras.push((featureMap.vixPctRank ?? 0) * (featureMap.spyRet10 ?? 0));
    extraNames.push('vixPctRank_x_spyRet10');
  }
  if (selectedGroups.includes('volCore') && selectedGroups.includes('volDelta')) {
    extras.push((featureMap.vix ?? 0) * (featureMap.ts9d30 ?? 0));
    extraNames.push('vix_x_ts9d30');
    extras.push((featureMap.vix1dOverVix ?? 0) * (featureMap.ts30d90d ?? 0));
    extraNames.push('vix1dOverVix_x_ts30d90d');
  }
  if (selectedGroups.includes('priceTrend') && selectedGroups.includes('realized')) {
    extras.push((featureMap.spyReturn1d ?? 0) * (featureMap.spyRealizedVol10 ?? 0));
    extraNames.push('spyReturn1d_x_spyRealizedVol10');
    extras.push((featureMap.spyMaGap10 ?? 0) * (featureMap.spyIntradayReturn ?? 0));
    extraNames.push('spyMaGap10_x_spyIntradayReturn');
  }
  if (selectedGroups.includes('event') && selectedGroups.includes('volCore')) {
    extras.push((featureMap.eventScore ?? 0) * (featureMap.vixPctRank ?? 0));
    extraNames.push('eventScore_x_vixPctRank');
  }
  return { names: names.concat(extraNames), values: base.concat(extras) };
}

function buildDataset(rows = [], labelConfig, selectedGroups = [], featureMode = 'base') {
  const out = [];
  for (let i = 10; i < rows.length - 1; i += 1) {
    const current = rows[i];
    const next = rows[i + 1];
    const prev1 = rows[i - 1];
    const prev5 = rows[i - 5];
    const prev10 = rows[i - 10];
    const actualLabel = labelReturn(computeReturn(current.spyClose, next.spyClose), labelConfig);
    if (!actualLabel) continue;
    const featureMap = buildFeatureMap(current, prev1, prev5, prev10);
    const vector = buildVector(featureMap, selectedGroups, featureMode);
    out.push({
      tradeDateUtc: current.tradeDateUtc,
      nextTradeDateUtc: next.tradeDateUtc,
      actualLabel,
      spyReturnNext: computeReturn(current.spyClose, next.spyClose),
      spxlReturnNext: computeReturn(current.spxlClose, next.spxlClose),
      spxsReturnNext: computeReturn(current.spxsClose, next.spxsClose),
      vector: vector.values,
      featureNames: vector.names,
    });
  }
  return out;
}

function standardize(trainRows = [], otherRows = []) {
  const dims = trainRows[0]?.vector?.length || 0;
  const means = [];
  const scales = [];
  for (let i = 0; i < dims; i += 1) {
    const col = trainRows.map((row) => row.vector[i]);
    means[i] = mean(col);
    scales[i] = stdDev(col);
  }
  const transform = (rows) => rows.map((row) => ({
    ...row,
    x: row.vector.map((value, index) => (value - means[index]) / scales[index]),
  }));
  return [transform(trainRows), ...otherRows.map(transform)];
}

function sigmoid(value) {
  if (value >= 0) {
    const z = Math.exp(-value);
    return 1 / (1 + z);
  }
  const z = Math.exp(value);
  return z / (1 + z);
}

function trainBinaryLogistic(trainRows = [], options = {}) {
  const dims = trainRows[0]?.x?.length || 0;
  const weights = Array.from({ length: dims }, () => 0);
  let bias = 0;
  const learningRate = options.learningRate ?? 0.03;
  const epochs = options.epochs ?? 600;
  const reg = options.reg ?? 0.001;
  const positives = trainRows.filter((row) => row.binaryTarget === 1).length;
  const negatives = Math.max(1, trainRows.length - positives);
  const posWeight = trainRows.length / (2 * Math.max(1, positives)) * (options.posMultiplier ?? 1);
  const negWeight = trainRows.length / (2 * negatives);

  for (let epoch = 0; epoch < epochs; epoch += 1) {
    const grad = Array.from({ length: dims }, () => 0);
    let gradBias = 0;
    trainRows.forEach((row) => {
      const logit = row.x.reduce((sum, value, index) => sum + (weights[index] * value), bias);
      const prob = sigmoid(logit);
      const sampleWeight = row.binaryTarget === 1 ? posWeight : negWeight;
      const error = (prob - row.binaryTarget) * sampleWeight;
      gradBias += error;
      row.x.forEach((value, index) => { grad[index] += error * value; });
    });
    for (let i = 0; i < dims; i += 1) {
      grad[i] = (grad[i] / trainRows.length) + (reg * weights[i]);
      weights[i] -= learningRate * grad[i];
    }
    bias -= learningRate * (gradBias / trainRows.length);
  }

  return { weights, bias };
}

function predictBinary(model, rows = []) {
  return rows.map((row) => {
    const logit = row.x.reduce((sum, value, index) => sum + (model.weights[index] * value), model.bias);
    return sigmoid(logit);
  });
}

function fitOrdinalModel(trainRows = [], spec = {}) {
  const upRows = trainRows.map((row) => ({
    ...row,
    binaryTarget: row.actualLabel === 'Calm' ? 1 : 0,
  }));
  const downRows = trainRows.map((row) => ({
    ...row,
    binaryTarget: (row.actualLabel === 'Stress' || row.actualLabel === 'Crash') ? 1 : 0,
  }));
  const crashRows = trainRows
    .filter((row) => row.actualLabel === 'Stress' || row.actualLabel === 'Crash')
    .map((row) => ({
      ...row,
      binaryTarget: row.actualLabel === 'Crash' ? 1 : 0,
    }));

  return {
    upModel: trainBinaryLogistic(upRows, {
      learningRate: spec.learningRate,
      reg: spec.reg,
      epochs: spec.epochs,
      posMultiplier: spec.upPosMultiplier,
    }),
    downModel: trainBinaryLogistic(downRows, {
      learningRate: spec.learningRate,
      reg: spec.reg,
      epochs: spec.epochs,
      posMultiplier: spec.downPosMultiplier,
    }),
    crashModel: crashRows.length >= 8
      ? trainBinaryLogistic(crashRows, {
        learningRate: spec.learningRate,
        reg: spec.reg,
        epochs: spec.epochs,
        posMultiplier: spec.crashPosMultiplier,
      })
      : null,
  };
}

function predictOrdinal(model, rows = [], spec = {}) {
  const upProb = predictBinary(model.upModel, rows);
  const downProb = predictBinary(model.downModel, rows);
  const crashProb = model.crashModel ? predictBinary(model.crashModel, rows) : rows.map(() => 0);
  return rows.map((row, index) => {
    const up = upProb[index];
    const down = downProb[index];
    const crash = crashProb[index];
    let predictedLabel = 'Normal';
    if (up >= spec.upThreshold && up >= down) {
      predictedLabel = 'Calm';
    } else if (down >= spec.downThreshold) {
      predictedLabel = crash >= spec.crashThreshold ? 'Crash' : 'Stress';
    }
    return {
      ...row,
      predictedLabel,
    };
  });
}

function summarizeClassification(rows = []) {
  let right = 0;
  let wrong = 0;
  rows.forEach((row) => {
    if (row.predictedLabel === row.actualLabel) right += 1;
    else wrong += 1;
  });
  return {
    total: rows.length,
    right,
    wrong,
    accuracy: rows.length ? right / rows.length : null,
  };
}

function applyPolicy(row, policy = {}) {
  const asset = policy[row.predictedLabel] || 'CASH';
  const strategyReturn = asset === 'SPXL'
    ? row.spxlReturnNext
    : asset === 'SPXS'
      ? row.spxsReturnNext
      : asset === 'SPY'
        ? row.spyReturnNext
        : 0;
  return { asset, strategyReturn };
}

function summarizePolicy(rows = [], policy = {}) {
  let endingEquity = 1;
  let benchmarkEndingEquity = 1;
  rows.forEach((row) => {
    const trade = applyPolicy(row, policy);
    endingEquity *= (1 + trade.strategyReturn);
    benchmarkEndingEquity *= (1 + row.spyReturnNext);
  });
  return {
    endingEquity,
    totalReturn: endingEquity - 1,
    benchmarkEndingEquity,
    benchmarkTotalReturn: benchmarkEndingEquity - 1,
    relativeEdge: endingEquity - benchmarkEndingEquity,
  };
}

function main() {
  const artifact = JSON.parse(fs.readFileSync(ARTIFACT_PATH, 'utf8'));
  const modelReport = JSON.parse(fs.readFileSync(MODEL_PATH, 'utf8'));
  const bestModel = modelReport.bestModel;
  const dailyFeatures = artifact.daily?.features || artifact.dailyFeatureRows || [];
  const dataset = buildDataset(
    dailyFeatures,
    bestModel.labelConfig,
    bestModel.spec.selectedGroups,
    bestModel.spec.featureMode,
  );

  const monthRows = new Map();
  dataset.forEach((row) => {
    const key = monthKey(row.tradeDateUtc);
    const rows = monthRows.get(key) || [];
    rows.push(row);
    monthRows.set(key, rows);
  });
  const months = Array.from(monthRows.keys()).sort();
  const monthlyResults = [];

  months.forEach((month) => {
    const testRows = monthRows.get(month) || [];
    if (!testRows.length) return;
    const monthStart = `${month}-01`;
    const monthIndex = dataset.findIndex((row) => row.tradeDateUtc >= monthStart);
    if (monthIndex < TRAIN_ROWS) return;
    const trainRows = dataset.slice(Math.max(0, monthIndex - TRAIN_ROWS), monthIndex);
    if (trainRows.length < TRAIN_ROWS) return;
    const [trainStd, testStd] = standardize(trainRows, [testRows]);
    const model = fitOrdinalModel(trainStd, bestModel.spec);
    const predictedRows = predictOrdinal(model, testStd, bestModel.spec);
    monthlyResults.push({
      month,
      trainStartDate: trainRows[0].tradeDateUtc,
      trainEndDate: trainRows[trainRows.length - 1].tradeDateUtc,
      testStartDate: testRows[0].tradeDateUtc,
      testEndDate: testRows[testRows.length - 1].tradeDateUtc,
      classification: summarizeClassification(predictedRows),
      conservativeCash: summarizePolicy(predictedRows, CONSERVATIVE_CASH_POLICY),
    });
  });

  const aggregateClassification = {
    total: monthlyResults.reduce((sum, month) => sum + (month.classification.total || 0), 0),
    right: monthlyResults.reduce((sum, month) => sum + (month.classification.right || 0), 0),
    wrong: monthlyResults.reduce((sum, month) => sum + (month.classification.wrong || 0), 0),
  };
  aggregateClassification.accuracy = aggregateClassification.total
    ? aggregateClassification.right / aggregateClassification.total
    : null;

  let aggregateEndingEquity = 1;
  let aggregateBenchmarkEndingEquity = 1;
  monthlyResults.forEach((month) => {
    aggregateEndingEquity *= month.conservativeCash.endingEquity;
    aggregateBenchmarkEndingEquity *= month.conservativeCash.benchmarkEndingEquity;
  });

  const output = {
    generatedAt: new Date().toISOString(),
    artifactPath: ARTIFACT_PATH,
    modelPath: MODEL_PATH,
    trainRows: TRAIN_ROWS,
    note: 'Uses the fixed best feature-subset ordinal model spec. Retrains monthly on the prior TRAIN_ROWS labeled days and tests on the calendar month.',
    monthlyResults,
    aggregate: {
      classification: aggregateClassification,
      conservativeCash: {
        endingEquity: aggregateEndingEquity,
        totalReturn: aggregateEndingEquity - 1,
        benchmarkEndingEquity: aggregateBenchmarkEndingEquity,
        benchmarkTotalReturn: aggregateBenchmarkEndingEquity - 1,
        relativeEdge: aggregateEndingEquity - aggregateBenchmarkEndingEquity,
      },
    },
  };

  fs.mkdirSync(OUTPUT_DIR, { recursive: true });
  const outputPath = path.join(OUTPUT_DIR, 'vixregime-monthly-walkforward-conservative-cash.json');
  fs.writeFileSync(outputPath, JSON.stringify(output, null, 2));

  console.log(JSON.stringify({
    outputPath,
    monthlyResults: monthlyResults.map((month) => ({
      month: month.month,
      classification: month.classification,
      conservativeCash: month.conservativeCash,
    })),
    aggregate: output.aggregate,
  }, null, 2));
}

main();
