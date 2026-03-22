#!/usr/bin/env node
const fs = require('node:fs');
const path = require('node:path');

const ARTIFACT_PATH = path.resolve(
  process.env.ARTIFACT_PATH
    || path.join(process.cwd(), 'vixregime', 'artifacts', 'reports', 'vixregime-backtest-2025-01-02-2026-03-20.json'),
);
const OUTPUT_DIR = path.resolve(
  process.env.OUTPUT_DIR
    || path.join(process.cwd(), 'vixregime', 'artifacts', 'reports'),
);
const WINDOW_DAYS = Math.max(40, Math.trunc(Number(process.env.WINDOW_DAYS || 63)));
const SEARCH_ITERATIONS = Math.max(100, Math.trunc(Number(process.env.SEARCH_ITERATIONS || 900)));

const CLASS_NAMES = Object.freeze(['Crash', 'Stress', 'Normal', 'Calm']);
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

function createRng(seed = 20260321) {
  let state = seed >>> 0;
  return () => {
    state = (Math.imul(1664525, state) + 1013904223) >>> 0;
    return state / 0x100000000;
  };
}

function pick(rng, values = []) {
  return values[Math.floor(rng() * values.length)];
}

function computeReturn(currentPrice, nextPrice) {
  const current = toNumber(currentPrice);
  const next = toNumber(nextPrice);
  if (current === null || next === null || current <= 0) return null;
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

function emptyConfusion() {
  const out = {};
  CLASS_NAMES.forEach((actual) => {
    out[actual] = {};
    CLASS_NAMES.forEach((predicted) => {
      out[actual][predicted] = 0;
    });
  });
  return out;
}

function evaluateClassification(rows = []) {
  const confusion = emptyConfusion();
  let right = 0;
  let total = 0;
  const byLabel = {};
  CLASS_NAMES.forEach((label) => {
    byLabel[label] = { total: 0, right: 0 };
  });
  rows.forEach((row) => {
    if (!row.actualLabel || !row.predictedLabel) return;
    total += 1;
    byLabel[row.actualLabel].total += 1;
    confusion[row.actualLabel][row.predictedLabel] += 1;
    if (row.actualLabel === row.predictedLabel) {
      right += 1;
      byLabel[row.actualLabel].right += 1;
    }
  });
  const recalls = CLASS_NAMES
    .map((label) => byLabel[label].total > 0 ? (byLabel[label].right / byLabel[label].total) : null)
    .filter((value) => value !== null);
  const macroF1 = CLASS_NAMES
    .map((label) => {
      const tp = confusion[label][label];
      const predicted = CLASS_NAMES.reduce((sum, actual) => sum + confusion[actual][label], 0);
      const actual = CLASS_NAMES.reduce((sum, predicted) => sum + confusion[label][predicted], 0);
      if (!actual || !predicted || !tp) return actual > 0 ? 0 : null;
      const precision = tp / predicted;
      const recall = tp / actual;
      return (2 * precision * recall) / (precision + recall);
    })
    .filter((value) => value !== null);
  return {
    right,
    total,
    accuracy: total > 0 ? right / total : null,
    balancedAccuracy: recalls.length ? mean(recalls) : null,
    macroF1: macroF1.length ? mean(macroF1) : null,
    byLabel,
    confusion,
  };
}

function labelGrid() {
  const out = [];
  [-0.015, -0.02, -0.025].forEach((crashMax) => {
    [-0.0025, -0.005, -0.0075].forEach((stressMax) => {
      [0.0025, 0.005, 0.0075].forEach((calmMin) => {
        if (!(crashMax < stressMax && stressMax < calmMin)) return;
        out.push({ crashMax, stressMax, calmMin });
      });
    });
  });
  return out;
}

function buildSequentialWindows(features = []) {
  const rows = features.filter((row) => row && row.tradeDateUtc && row.spyClose > 0 && row.spxlClose > 0 && row.spxsClose > 0);
  const needed = (WINDOW_DAYS * 4) + 1;
  if (rows.length < needed) throw new Error(`insufficient_daily_rows:${rows.length}:${needed}`);
  const source = rows.slice(-needed);
  const buildWindow = (name, start) => {
    const segment = source.slice(start, start + WINDOW_DAYS + 1);
    return {
      rows: segment,
      range: {
        startDate: segment[0].tradeDateUtc,
        endDate: segment[segment.length - 2].tradeDateUtc,
      },
      name,
    };
  };
  return {
    train: buildWindow('train', 0),
    selection: buildWindow('selection', WINDOW_DAYS),
    holdout1: buildWindow('holdout1', WINDOW_DAYS * 2),
    holdout2: buildWindow('holdout2', WINDOW_DAYS * 3),
  };
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
  if (featureMode !== 'poly2') return base;
  const extras = [];
  if (selectedGroups.includes('volCore') && selectedGroups.includes('priceTrend')) {
    extras.push((featureMap.vix ?? 0) * (featureMap.spyReturn5d ?? 0));
    extras.push((featureMap.vixPctRank ?? 0) * (featureMap.spyRet10 ?? 0));
  }
  if (selectedGroups.includes('volCore') && selectedGroups.includes('volDelta')) {
    extras.push((featureMap.vix ?? 0) * (featureMap.ts9d30 ?? 0));
    extras.push((featureMap.vix1dOverVix ?? 0) * (featureMap.ts30d90d ?? 0));
  }
  if (selectedGroups.includes('priceTrend') && selectedGroups.includes('realized')) {
    extras.push((featureMap.spyReturn1d ?? 0) * (featureMap.spyRealizedVol10 ?? 0));
    extras.push((featureMap.spyMaGap10 ?? 0) * (featureMap.spyIntradayReturn ?? 0));
  }
  if (selectedGroups.includes('event') && selectedGroups.includes('volCore')) {
    extras.push((featureMap.eventScore ?? 0) * (featureMap.vixPctRank ?? 0));
  }
  return base.concat(extras);
}

function buildDataset(features = [], labelConfig, selectedGroups = [], featureMode = 'base') {
  const rows = [];
  for (let i = 10; i < features.length - 1; i += 1) {
    const current = features[i];
    const next = features[i + 1];
    const prev1 = features[i - 1];
    const prev5 = features[i - 5];
    const prev10 = features[i - 10];
    const nextSpyReturn = computeReturn(current.spyClose, next.spyClose);
    const actualLabel = labelReturn(nextSpyReturn, labelConfig);
    if (!actualLabel) continue;
    const featureMap = buildFeatureMap(current, prev1, prev5, prev10);
    rows.push({
      tradeDateUtc: current.tradeDateUtc,
      nextTradeDateUtc: next.tradeDateUtc,
      actualLabel,
      vector: buildVector(featureMap, selectedGroups, featureMode),
    });
  }
  return rows;
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

function candidateSpecs() {
  const out = [];
  const groupSets = [
    ['volCore', 'priceTrend'],
    ['volCore', 'priceTrend', 'realized'],
    ['volCore', 'priceTrend', 'volDelta'],
    ['volCore', 'priceTrend', 'event'],
    ['volCore', 'priceTrend', 'realized', 'volDelta'],
    ['volCore', 'priceTrend', 'realized', 'event'],
    ['volCore', 'priceTrend', 'volDelta', 'event'],
    ['volCore', 'priceTrend', 'realized', 'volDelta', 'event'],
  ];
  [0.015, 0.03].forEach((learningRate) => {
    [0.0005, 0.001].forEach((reg) => {
      [350, 450, 650].forEach((epochs) => {
        ['base', 'poly2'].forEach((featureMode) => {
          [1.0, 1.5, 2.0].forEach((upPosMultiplier) => {
            [1.0, 1.5, 2.0].forEach((downPosMultiplier) => {
              [1.0, 1.5, 2.0].forEach((crashPosMultiplier) => {
                [0.45, 0.5, 0.55, 0.6].forEach((upThreshold) => {
                  [0.45, 0.5, 0.55, 0.6].forEach((downThreshold) => {
                    [0.35, 0.4, 0.45, 0.5].forEach((crashThreshold) => {
                      groupSets.forEach((selectedGroups) => {
                        out.push({
                          featureMode,
                          learningRate,
                          reg,
                          epochs,
                          upPosMultiplier,
                          downPosMultiplier,
                          crashPosMultiplier,
                          upThreshold,
                          downThreshold,
                          crashThreshold,
                          selectedGroups,
                        });
                      });
                    });
                  });
                });
              });
            });
          });
        });
      });
    });
  });
  return out;
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
      tradeDateUtc: row.tradeDateUtc,
      actualLabel: row.actualLabel,
      predictedLabel,
      upProb: Number(up.toFixed(4)),
      downProb: Number(down.toFixed(4)),
      crashProb: Number(crash.toFixed(4)),
    };
  });
}

function scoreModel(metrics = {}) {
  return (
    (metrics.selection.accuracy || 0) * 0.4
    + (metrics.holdout1.accuracy || 0) * 0.3
    + (metrics.holdout2.accuracy || 0) * 0.3
  );
}

function main() {
  const artifact = JSON.parse(fs.readFileSync(ARTIFACT_PATH, 'utf8'));
  const dailyFeatures = artifact.daily?.features || artifact.dailyFeatureRows || [];
  const windows = buildSequentialWindows(dailyFeatures);
  const rng = createRng();
  const labelConfigs = labelGrid();
  const specs = candidateSpecs();
  let bestModel = null;

  for (let iteration = 0; iteration < SEARCH_ITERATIONS; iteration += 1) {
    const labelConfig = pick(rng, labelConfigs);
    const spec = pick(rng, specs);
    const trainRows = buildDataset(windows.train.rows, labelConfig, spec.selectedGroups, spec.featureMode);
    const selectionRows = buildDataset(windows.selection.rows, labelConfig, spec.selectedGroups, spec.featureMode);
    const holdout1Rows = buildDataset(windows.holdout1.rows, labelConfig, spec.selectedGroups, spec.featureMode);
    const holdout2Rows = buildDataset(windows.holdout2.rows, labelConfig, spec.selectedGroups, spec.featureMode);
    const [trainStd, selectionStd, holdout1Std, holdout2Std] = standardize(trainRows, [selectionRows, holdout1Rows, holdout2Rows]);
    const model = fitOrdinalModel(trainStd, spec);
    const selection = evaluateClassification(predictOrdinal(model, selectionStd, spec));
    const holdout1 = evaluateClassification(predictOrdinal(model, holdout1Std, spec));
    const holdout2 = evaluateClassification(predictOrdinal(model, holdout2Std, spec));
    const score = scoreModel({ selection, holdout1, holdout2 });

    if (!bestModel || score > bestModel.score) {
      bestModel = {
        score,
        labelConfig,
        spec,
        selection,
        holdout1,
        holdout2,
      };
    }
  }

  fs.mkdirSync(OUTPUT_DIR, { recursive: true });
  const outputPath = path.join(OUTPUT_DIR, 'vixregime-feature-subset-ordinal-model-search.json');
  fs.writeFileSync(outputPath, JSON.stringify({
    generatedAt: new Date().toISOString(),
    sourceArtifact: ARTIFACT_PATH,
    windowDays: WINDOW_DAYS,
    searchIterations: SEARCH_ITERATIONS,
    windows: {
      train: windows.train.range,
      selection: windows.selection.range,
      holdout1: windows.holdout1.range,
      holdout2: windows.holdout2.range,
    },
    bestModel,
  }, null, 2));
  console.log(JSON.stringify({ outputPath, bestModel }, null, 2));
}

main();
