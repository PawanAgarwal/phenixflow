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

function standardize(trainRows = [], predictVector = []) {
  const dims = trainRows[0]?.vector?.length || 0;
  const means = [];
  const scales = [];
  for (let i = 0; i < dims; i += 1) {
    const col = trainRows.map((row) => row.vector[i]);
    means[i] = mean(col);
    scales[i] = stdDev(col);
  }
  const transformRow = (row) => ({
    ...row,
    x: row.vector.map((value, index) => (value - means[index]) / scales[index]),
  });
  return {
    trainRows: trainRows.map(transformRow),
    predictX: predictVector.map((value, index) => (value - means[index]) / scales[index]),
  };
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

function predictBinary(model, x = []) {
  const logit = x.reduce((sum, value, index) => sum + (model.weights[index] * value), model.bias);
  return sigmoid(logit);
}

function topContributions(weights = [], x = [], featureNames = [], count = 6) {
  return featureNames
    .map((name, index) => ({
      name,
      contribution: weights[index] * x[index],
      weight: weights[index],
      zScore: x[index],
    }))
    .sort((a, b) => Math.abs(b.contribution) - Math.abs(a.contribution))
    .slice(0, count);
}

function main() {
  const artifact = JSON.parse(fs.readFileSync(ARTIFACT_PATH, 'utf8'));
  const report = JSON.parse(fs.readFileSync(MODEL_PATH, 'utf8'));
  const bestModel = report.bestModel;
  const rows = (artifact.daily?.features || artifact.dailyFeatureRows || [])
    .filter((row) => row && row.tradeDateUtc && row.spyClose > 0 && row.spxlClose > 0 && row.spxsClose > 0);

  if (rows.length < 20) {
    throw new Error(`insufficient_rows:${rows.length}`);
  }

  const latestIndex = rows.length - 1;
  const latest = rows[latestIndex];
  const prev1 = rows[latestIndex - 1];
  const prev5 = rows[latestIndex - 5];
  const prev10 = rows[latestIndex - 10];
  const predictFeatureMap = buildFeatureMap(latest, prev1, prev5, prev10);
  const predictVector = buildVector(
    predictFeatureMap,
    bestModel.spec.selectedGroups,
    bestModel.spec.featureMode,
  );

  const trainRows = [];
  for (let i = 10; i < rows.length - 1; i += 1) {
    const current = rows[i];
    const next = rows[i + 1];
    const localPrev1 = rows[i - 1];
    const localPrev5 = rows[i - 5];
    const localPrev10 = rows[i - 10];
    const actualLabel = labelReturn(computeReturn(current.spyClose, next.spyClose), bestModel.labelConfig);
    if (!actualLabel) continue;
    const featureMap = buildFeatureMap(current, localPrev1, localPrev5, localPrev10);
    const vector = buildVector(
      featureMap,
      bestModel.spec.selectedGroups,
      bestModel.spec.featureMode,
    );
    trainRows.push({
      tradeDateUtc: current.tradeDateUtc,
      actualLabel,
      vector: vector.values,
    });
  }

  const { trainRows: standardizedTrainRows, predictX } = standardize(trainRows, predictVector.values);

  const upRows = standardizedTrainRows.map((row) => ({
    ...row,
    binaryTarget: row.actualLabel === 'Calm' ? 1 : 0,
  }));
  const downRows = standardizedTrainRows.map((row) => ({
    ...row,
    binaryTarget: (row.actualLabel === 'Stress' || row.actualLabel === 'Crash') ? 1 : 0,
  }));
  const crashRows = standardizedTrainRows
    .filter((row) => row.actualLabel === 'Stress' || row.actualLabel === 'Crash')
    .map((row) => ({
      ...row,
      binaryTarget: row.actualLabel === 'Crash' ? 1 : 0,
    }));

  const upModel = trainBinaryLogistic(upRows, {
    learningRate: bestModel.spec.learningRate,
    reg: bestModel.spec.reg,
    epochs: bestModel.spec.epochs,
    posMultiplier: bestModel.spec.upPosMultiplier,
  });
  const downModel = trainBinaryLogistic(downRows, {
    learningRate: bestModel.spec.learningRate,
    reg: bestModel.spec.reg,
    epochs: bestModel.spec.epochs,
    posMultiplier: bestModel.spec.downPosMultiplier,
  });
  const crashModel = crashRows.length >= 8
    ? trainBinaryLogistic(crashRows, {
      learningRate: bestModel.spec.learningRate,
      reg: bestModel.spec.reg,
      epochs: bestModel.spec.epochs,
      posMultiplier: bestModel.spec.crashPosMultiplier,
    })
    : null;

  const upProb = predictBinary(upModel, predictX);
  const downProb = predictBinary(downModel, predictX);
  const crashProb = crashModel ? predictBinary(crashModel, predictX) : 0;

  let predictedLabel = 'Normal';
  if (upProb >= bestModel.spec.upThreshold && upProb >= downProb) {
    predictedLabel = 'Calm';
  } else if (downProb >= bestModel.spec.downThreshold) {
    predictedLabel = crashProb >= bestModel.spec.crashThreshold ? 'Crash' : 'Stress';
  }

  const output = {
    predictionDateUtc: latest.tradeDateUtc,
    targetNextSessionLabelDateUtc: 'next_trading_day',
    labelConfig: bestModel.labelConfig,
    spec: bestModel.spec,
    predictedLabel,
    conservativeCashAction: CONSERVATIVE_CASH_POLICY[predictedLabel],
    stageProbabilities: {
      calmVsRest: Number(upProb.toFixed(4)),
      stressOrCrashVsRest: Number(downProb.toFixed(4)),
      crashVsStress: Number(crashProb.toFixed(4)),
    },
    stageThresholds: {
      calmThreshold: bestModel.spec.upThreshold,
      stressThreshold: bestModel.spec.downThreshold,
      crashThreshold: bestModel.spec.crashThreshold,
    },
    latestInputs: {
      tradeDateUtc: latest.tradeDateUtc,
      spyClose: latest.spyClose,
      spxlClose: latest.spxlClose,
      spxsClose: latest.spxsClose,
      vix: latest.vix,
      vix9d: latest.vix9d,
      vix1d: latest.vix1d,
      vix3m: latest.vix3m,
      ts1d9d: latest.ts1d9d,
      vix1dOverVix: latest.vix1dOverVix,
      vixPctRank: latest.vixPctRank,
      delta10: latest.delta10,
      spyReturn1d: latest.spyReturn1d,
      spyReturn5d: latest.spyReturn5d,
      spyReturn10d: latest.spyReturn10d,
      spyMaGap5: latest.spyMaGap5,
      spyMaGap10: latest.spyMaGap10,
    },
    topContributions: {
      calmVsRest: topContributions(upModel.weights, predictX, predictVector.names),
      stressOrCrashVsRest: topContributions(downModel.weights, predictX, predictVector.names),
      crashVsStress: topContributions(crashModel?.weights || Array.from({ length: predictX.length }, () => 0), predictX, predictVector.names),
    },
  };

  console.log(JSON.stringify(output, null, 2));
}

main();
