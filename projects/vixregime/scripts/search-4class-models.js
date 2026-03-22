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
const SEARCH_ITERATIONS = Math.max(20, Math.trunc(Number(process.env.SEARCH_ITERATIONS || 80)));

const CLASS_NAMES = Object.freeze(['Crash', 'Stress', 'Normal', 'Calm']);

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

function argMax(values = []) {
  let bestIndex = 0;
  let bestValue = values[0];
  for (let i = 1; i < values.length; i += 1) {
    if (values[i] > bestValue) {
      bestValue = values[i];
      bestIndex = i;
    }
  }
  return bestIndex;
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

function classIndex(label) {
  return CLASS_NAMES.indexOf(label);
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
      const actual = CLASS_NAMES.reduce((sum, predictedLabel) => sum + confusion[label][predictedLabel], 0);
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
      name,
      rows: segment,
      range: {
        startDate: segment[0].tradeDateUtc,
        endDate: segment[segment.length - 2].tradeDateUtc,
      },
    };
  };
  return {
    sourceStartDate: source[0].tradeDateUtc,
    sourceEndDate: source[source.length - 1].tradeDateUtc,
    train: buildWindow('train', 0),
    selection: buildWindow('selection', WINDOW_DAYS),
    holdout1: buildWindow('holdout1', WINDOW_DAYS * 2),
    holdout2: buildWindow('holdout2', WINDOW_DAYS * 3),
  };
}

function buildDataset(features = [], labelConfig, featureMode = 'base') {
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

    const spyRet1 = computeReturn(prev1.spyClose, current.spyClose);
    const spyRet5 = computeReturn(prev5.spyClose, current.spyClose);
    const spyRet10 = computeReturn(prev10.spyClose, current.spyClose);
    const vixChange1 = toNumber(current.vix) !== null && toNumber(prev1.vix) !== null ? current.vix - prev1.vix : null;
    const termSlopeGap = (toNumber(current.ts9d30) !== null && toNumber(current.ts30d90d) !== null)
      ? current.ts9d30 - current.ts30d90d
      : null;

    const baseVector = [
      current.vix,
      current.vixPctRank,
      current.delta5,
      current.delta10,
      current.ts9d30,
      current.ts30d90d,
      current.ts1d9d,
      current.vix1dOverVix,
      spyRet1,
      spyRet5,
      spyRet10,
      vixChange1,
      termSlopeGap,
      current.spyReturn1d,
      current.spyReturn3d,
      current.spyReturn5d,
      current.spyReturn10d,
      current.spyMaGap5,
      current.spyMaGap10,
      current.spyMaGap20,
      current.spyRealizedVol5,
      current.spyRealizedVol10,
      current.spyRealizedVol20,
      current.spyDownsideVol10,
      current.spyDownsideVol20,
      current.spyIntradayReturn,
      current.spyRangePct,
      current.spyGapFromPrevClose,
      current.spyCloseLocation,
      current.vixChange3d,
      current.vix9dChange1d,
      current.vix1dChange1d,
      current.vix3mChange1d,
      current.ts9d30Delta1,
      current.ts30d90dDelta1,
      current.ts1d9dDelta1,
      current.vixRiskPremium10,
      current.vixRiskPremium20,
      current.isFomcDay ? 1 : 0,
      current.isMonthlyOpex ? 1 : 0,
      current.isQuarterlyOpex ? 1 : 0,
      current.isCpiDay ? 1 : 0,
      current.isPpiDay ? 1 : 0,
      current.isNfpDay ? 1 : 0,
      current.isJoltsDay ? 1 : 0,
      current.isMacroEventDay ? 1 : 0,
      current.isPreFomcDay ? 1 : 0,
      current.isPostFomcDay ? 1 : 0,
      current.isPreMonthlyOpexDay ? 1 : 0,
      current.isPostMonthlyOpexDay ? 1 : 0,
      current.eventScore,
      current.avoidSuggested ? 1 : 0,
    ];
    const usableBase = baseVector.map((value) => toNumber(value) ?? 0);
    let vector = usableBase;
    if (featureMode === 'poly2') {
      vector = usableBase.concat([
        usableBase[0] * usableBase[1],
        usableBase[0] * usableBase[4],
        usableBase[0] * usableBase[5],
        usableBase[7] * usableBase[4],
        usableBase[1] * usableBase[10],
        usableBase[0] ** 2,
        usableBase[1] ** 2,
        usableBase[4] ** 2,
        usableBase[5] ** 2,
        usableBase[7] ** 2,
        usableBase[13] * usableBase[20],
        usableBase[14] * usableBase[21],
        usableBase[17] * usableBase[29],
        usableBase[18] * usableBase[33],
        usableBase[20] ** 2,
        usableBase[21] ** 2,
        usableBase[24] ** 2,
        usableBase[35] ** 2,
        usableBase[38] * usableBase[51],
        usableBase[42] * usableBase[51],
        usableBase[50] * usableBase[51],
      ]);
    }
    rows.push({
      tradeDateUtc: current.tradeDateUtc,
      nextTradeDateUtc: next.tradeDateUtc,
      actualLabel,
      actualClass: classIndex(actualLabel),
      nextSpyReturn,
      vector,
    });
  }
  return rows;
}

function standardizeDatasets(trainRows = [], otherRowSets = []) {
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
  return [transform(trainRows), ...otherRowSets.map(transform), { means, scales }];
}

function softmax(logits = []) {
  const maxLogit = Math.max(...logits);
  const exps = logits.map((value) => Math.exp(value - maxLogit));
  const denom = exps.reduce((sum, value) => sum + value, 0) || 1;
  return exps.map((value) => value / denom);
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
  const posWeight = trainRows.length / (2 * Math.max(1, positives));
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
      row.x.forEach((value, index) => {
        grad[index] += error * value;
      });
    });
    for (let i = 0; i < dims; i += 1) {
      grad[i] = (grad[i] / trainRows.length) + (reg * weights[i]);
      weights[i] -= learningRate * grad[i];
    }
    bias -= learningRate * (gradBias / trainRows.length);
  }

  return { modelType: 'binary_logistic', weights, bias };
}

function trainSoftmax(trainRows = [], options = {}) {
  const dims = trainRows[0]?.x?.length || 0;
  const classes = CLASS_NAMES.length;
  const rng = createRng(options.seed || 7);
  const weights = Array.from({ length: classes }, () => Array.from({ length: dims }, () => (rng() - 0.5) * 0.02));
  const bias = Array.from({ length: classes }, () => 0);
  const learningRate = options.learningRate ?? 0.03;
  const epochs = options.epochs ?? 800;
  const reg = options.reg ?? 0.001;
  const classPower = options.classWeightPower ?? 1;
  const labelCounts = Array.from({ length: classes }, () => 0);
  trainRows.forEach((row) => {
    labelCounts[row.actualClass] += 1;
  });
  const classWeights = labelCounts.map((count) => (count > 0 ? (trainRows.length / (classes * count)) ** classPower : 0));

  for (let epoch = 0; epoch < epochs; epoch += 1) {
    const gradW = Array.from({ length: classes }, () => Array.from({ length: dims }, () => 0));
    const gradB = Array.from({ length: classes }, () => 0);

    trainRows.forEach((row) => {
      const logits = weights.map((classWeightsRow, classIndexValue) => (
        classWeightsRow.reduce((sum, weight, dimIndex) => sum + (weight * row.x[dimIndex]), bias[classIndexValue])
      ));
      const probs = softmax(logits);
      const sampleWeight = classWeights[row.actualClass] || 1;
      for (let classIdx = 0; classIdx < classes; classIdx += 1) {
        const target = classIdx === row.actualClass ? 1 : 0;
        const error = (probs[classIdx] - target) * sampleWeight;
        gradB[classIdx] += error;
        for (let dimIndex = 0; dimIndex < dims; dimIndex += 1) {
          gradW[classIdx][dimIndex] += error * row.x[dimIndex];
        }
      }
    });

    for (let classIdx = 0; classIdx < classes; classIdx += 1) {
      for (let dimIndex = 0; dimIndex < dims; dimIndex += 1) {
        gradW[classIdx][dimIndex] = (gradW[classIdx][dimIndex] / trainRows.length) + (reg * weights[classIdx][dimIndex]);
        weights[classIdx][dimIndex] -= learningRate * gradW[classIdx][dimIndex];
      }
      bias[classIdx] -= learningRate * (gradB[classIdx] / trainRows.length);
    }
  }

  return {
    modelType: 'softmax',
    weights,
    bias,
    options,
    classBias: options.classBias || Array.from({ length: CLASS_NAMES.length }, () => 0),
  };
}

function predictSoftmax(model, rows = []) {
  return rows.map((row) => {
    const logits = model.weights.map((classWeightsRow, classIdx) => (
      classWeightsRow.reduce((sum, weight, dimIndex) => sum + (weight * row.x[dimIndex]), model.bias[classIdx] + (model.classBias?.[classIdx] || 0))
    ));
    const probs = softmax(logits);
    const predictedClass = argMax(probs);
    return {
      ...row,
      predictedClass,
      predictedLabel: CLASS_NAMES[predictedClass],
      probabilities: probs,
    };
  });
}

function trainNearestCentroid(trainRows = [], options = {}) {
  const dims = trainRows[0]?.x?.length || 0;
  const centroids = Array.from({ length: CLASS_NAMES.length }, () => Array.from({ length: dims }, () => 0));
  const counts = Array.from({ length: CLASS_NAMES.length }, () => 0);
  trainRows.forEach((row) => {
    counts[row.actualClass] += 1;
    row.x.forEach((value, index) => {
      centroids[row.actualClass][index] += value;
    });
  });
  centroids.forEach((centroid, classIdx) => {
    const denom = counts[classIdx] || 1;
    centroid.forEach((_, index) => {
      centroid[index] /= denom;
    });
  });
  return {
    modelType: 'centroid',
    centroids,
    counts,
    priorStrength: options.priorStrength ?? 0.15,
    classBias: options.classBias || Array.from({ length: CLASS_NAMES.length }, () => 0),
  };
}

function predictNearestCentroid(model, rows = []) {
  return rows.map((row) => {
    const scores = model.centroids.map((centroid, classIdx) => {
      const distance = centroid.reduce((sum, value, index) => sum + ((row.x[index] - value) ** 2), 0);
      const priorPenalty = model.counts[classIdx] > 0 ? (-Math.log(model.counts[classIdx]) * model.priorStrength) : 999;
      return -distance + priorPenalty + (model.classBias?.[classIdx] || 0);
    });
    const predictedClass = argMax(scores);
    return {
      ...row,
      predictedClass,
      predictedLabel: CLASS_NAMES[predictedClass],
      scores,
    };
  });
}

function trainHierarchicalModel(trainRows = [], options = {}) {
  const binaryRows = trainRows.map((row) => ({ ...row, binaryTarget: row.actualLabel === 'Normal' ? 1 : 0 }));
  const stage1 = trainBinaryLogistic(binaryRows, options);
  const minorityRows = trainRows.filter((row) => row.actualLabel !== 'Normal');
  const stage2 = trainNearestCentroid(minorityRows, { priorStrength: options.minorityPriorStrength ?? 0.1, classBias: options.classBias });
  return {
    modelType: 'hierarchical',
    stage1,
    stage2,
    normalThreshold: options.normalThreshold ?? 0.55,
  };
}

function predictHierarchical(model, rows = []) {
  return rows.map((row) => {
    const normalLogit = row.x.reduce((sum, value, index) => sum + (model.stage1.weights[index] * value), model.stage1.bias);
    const normalProb = sigmoid(normalLogit);
    if (normalProb >= model.normalThreshold) {
      return {
        ...row,
        predictedClass: classIndex('Normal'),
        predictedLabel: 'Normal',
        normalProb,
      };
    }

    const scores = model.stage2.centroids.map((centroid, classIdx) => {
      const distance = centroid.reduce((sum, value, index) => sum + ((row.x[index] - value) ** 2), 0);
      const priorPenalty = model.stage2.counts[classIdx] > 0 ? (-Math.log(model.stage2.counts[classIdx]) * model.stage2.priorStrength) : 999;
      return -distance + priorPenalty + (model.stage2.classBias?.[classIdx] || 0);
    });
    scores[classIndex('Normal')] = -999999;
    const predictedClass = argMax(scores);
    return {
      ...row,
      predictedClass,
      predictedLabel: CLASS_NAMES[predictedClass],
      normalProb,
      scores,
    };
  });
}

function trainModel(modelSpec, trainRows) {
  if (modelSpec.type === 'softmax') return trainSoftmax(trainRows, modelSpec);
  if (modelSpec.type === 'centroid') return trainNearestCentroid(trainRows, modelSpec);
  if (modelSpec.type === 'hierarchical') return trainHierarchicalModel(trainRows, modelSpec);
  throw new Error(`unknown_model_type:${modelSpec.type}`);
}

function predictModel(model, rows) {
  if (model.modelType === 'softmax') return predictSoftmax(model, rows);
  if (model.modelType === 'centroid') return predictNearestCentroid(model, rows);
  if (model.modelType === 'hierarchical') return predictHierarchical(model, rows);
  throw new Error(`unknown_prediction_model:${model.modelType}`);
}

function candidateModels() {
  const out = [];
  const classBiasSets = [
    [0, 0, 0, 0],
    [0.2, 0.2, 0, 0.2],
    [0.1, 0.1, 0.2, 0.1],
    [0, 0.15, 0.3, 0],
    [-0.1, 0.1, 0.25, -0.1],
  ];
  [0.5, 1.0, 1.5].forEach((classWeightPower) => {
    [0.015, 0.03, 0.05].forEach((learningRate) => {
      [0.0005, 0.001, 0.003].forEach((reg) => {
        [500, 900].forEach((epochs) => {
          ['base', 'poly2'].forEach((featureMode) => classBiasSets.forEach((classBias) => {
            out.push({
              type: 'softmax',
              featureMode,
              classWeightPower,
              learningRate,
              reg,
              epochs,
              seed: 17,
              classBias,
            });
          }));
        });
      });
    });
  });
  [0.0, 0.05, 0.15, 0.3].forEach((priorStrength) => {
    ['base', 'poly2'].forEach((featureMode) => classBiasSets.forEach((classBias) => {
      out.push({
        type: 'centroid',
        featureMode,
        priorStrength,
        classBias,
      });
    }));
  });
  [0.5, 0.6, 0.7].forEach((normalThreshold) => {
    [0.03, 0.05].forEach((learningRate) => {
      [0.0005, 0.001].forEach((reg) => {
        ['base', 'poly2'].forEach((featureMode) => classBiasSets.forEach((classBias) => {
          out.push({
            type: 'hierarchical',
            featureMode,
            normalThreshold,
            learningRate,
            reg,
            epochs: 700,
            minorityPriorStrength: 0.15,
            classBias,
          });
        }));
      });
    });
  });
  return out;
}

function selectionScore(metrics) {
  const balanced = metrics.balancedAccuracy ?? -Infinity;
  const macroF1 = metrics.macroF1 ?? -Infinity;
  const accuracy = metrics.accuracy ?? -Infinity;
  const normalRecall = metrics.byLabel?.Normal?.total ? (metrics.byLabel.Normal.right / metrics.byLabel.Normal.total) : 0;
  const minorityRecalls = ['Crash', 'Stress', 'Calm']
    .map((label) => metrics.byLabel?.[label]?.total ? (metrics.byLabel[label].right / metrics.byLabel[label].total) : null)
    .filter((value) => value !== null);
  const minorityMean = minorityRecalls.length ? mean(minorityRecalls) : 0;
  return (balanced * 1.8) + (macroF1 * 1.2) + (accuracy * 0.8) + (minorityMean * 1.2) - (normalRecall * 0.1);
}

function summarizeMetrics(metrics) {
  return {
    accuracy: metrics.accuracy,
    balancedAccuracy: metrics.balancedAccuracy,
    macroF1: metrics.macroF1,
    byLabel: metrics.byLabel,
    confusion: metrics.confusion,
  };
}

function run() {
  const artifact = JSON.parse(fs.readFileSync(ARTIFACT_PATH, 'utf8'));
  const windows = buildSequentialWindows(artifact.daily.features || []);
  const labels = labelGrid();
  const models = candidateModels();
  const rng = createRng(99);

  let best = null;
  for (let i = 0; i < SEARCH_ITERATIONS; i += 1) {
    const labelConfig = pick(rng, labels);
    const modelSpec = pick(rng, models);
    const trainRowsRaw = buildDataset(windows.train.rows, labelConfig, modelSpec.featureMode);
    const selectionRowsRaw = buildDataset(windows.selection.rows, labelConfig, modelSpec.featureMode);
    const holdout1RowsRaw = buildDataset(windows.holdout1.rows, labelConfig, modelSpec.featureMode);
    const holdout2RowsRaw = buildDataset(windows.holdout2.rows, labelConfig, modelSpec.featureMode);
    if (!trainRowsRaw.length || !selectionRowsRaw.length || !holdout1RowsRaw.length || !holdout2RowsRaw.length) continue;

    const [trainRows, selectionRows, holdout1Rows, holdout2Rows] = standardizeDatasets(
      trainRowsRaw,
      [selectionRowsRaw, holdout1RowsRaw, holdout2RowsRaw],
    );

    const model = trainModel(modelSpec, trainRows);
    const selectionPred = predictModel(model, selectionRows);
    const holdout1Pred = predictModel(model, holdout1Rows);
    const holdout2Pred = predictModel(model, holdout2Rows);
    const selectionMetrics = evaluateClassification(selectionPred);
    const holdout1Metrics = evaluateClassification(holdout1Pred);
    const holdout2Metrics = evaluateClassification(holdout2Pred);
    const score = selectionScore(selectionMetrics);

    if (
      !best
      || score > best.score
      || (
        score === best.score
        && (holdout1Metrics.balancedAccuracy ?? -Infinity) > (best.holdout1.balancedAccuracy ?? -Infinity)
      )
    ) {
      best = {
        score,
        labelConfig,
        modelSpec,
        selection: selectionMetrics,
        holdout1: holdout1Metrics,
        holdout2: holdout2Metrics,
      };
    }
  }

  if (!best) throw new Error('no_candidate_found');

  const report = {
    generatedAt: new Date().toISOString(),
    sourceArtifact: ARTIFACT_PATH,
    windowDays: WINDOW_DAYS,
    searchIterations: SEARCH_ITERATIONS,
    sourceRange: {
      startDate: windows.sourceStartDate,
      endDate: windows.sourceEndDate,
    },
    windows: {
      train: windows.train.range,
      selection: windows.selection.range,
      holdout1: windows.holdout1.range,
      holdout2: windows.holdout2.range,
    },
    bestModel: {
      score: best.score,
      labelConfig: best.labelConfig,
      modelSpec: best.modelSpec,
      selection: summarizeMetrics(best.selection),
      holdout1: summarizeMetrics(best.holdout1),
      holdout2: summarizeMetrics(best.holdout2),
    },
  };

  fs.mkdirSync(OUTPUT_DIR, { recursive: true });
  const reportPath = path.join(OUTPUT_DIR, 'vixregime-4class-model-search.json');
  fs.writeFileSync(reportPath, `${JSON.stringify(report, null, 2)}\n`, 'utf8');
  console.log(JSON.stringify({
    reportPath,
    windows: report.windows,
    bestModel: report.bestModel,
  }, null, 2));
}

run();
