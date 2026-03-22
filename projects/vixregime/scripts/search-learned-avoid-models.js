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
const SEARCH_ITERATIONS = Math.max(50, Math.trunc(Number(process.env.SEARCH_ITERATIONS || 500)));

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

function emptyConfusion(labels = CLASS_NAMES) {
  const out = {};
  labels.forEach((actual) => {
    out[actual] = {};
    labels.forEach((predicted) => {
      out[actual][predicted] = 0;
    });
  });
  return out;
}

function evaluateClassification(rows = [], includeAvoid = false) {
  const labels = includeAvoid ? CLASS_NAMES.concat(['Avoid']) : CLASS_NAMES;
  const confusion = emptyConfusion(labels);
  let right = 0;
  let total = 0;
  let avoidCount = 0;
  const byLabel = {};
  labels.forEach((label) => {
    byLabel[label] = { total: 0, right: 0 };
  });

  rows.forEach((row) => {
    if (!row.actualLabel || !row.predictedLabel) return;
    total += 1;
    if (row.predictedLabel === 'Avoid') avoidCount += 1;
    byLabel[row.actualLabel].total += 1;
    confusion[row.actualLabel][row.predictedLabel] += 1;
    if (row.actualLabel === row.predictedLabel) {
      right += 1;
      byLabel[row.actualLabel].right += 1;
    }
  });

  const coreRecalls = CLASS_NAMES
    .map((label) => byLabel[label].total > 0 ? (byLabel[label].right / byLabel[label].total) : null)
    .filter((value) => value !== null);

  const macroF1 = CLASS_NAMES
    .map((label) => {
      const tp = confusion[label][label] || 0;
      const predicted = labels.reduce((sum, actual) => sum + (confusion[actual][label] || 0), 0);
      const actual = labels.reduce((sum, predicted) => sum + (confusion[label][predicted] || 0), 0);
      if (!actual || !predicted || !tp) return actual > 0 ? 0 : null;
      const precision = tp / predicted;
      const recall = tp / actual;
      return (2 * precision * recall) / (precision + recall);
    })
    .filter((value) => value !== null);

  return {
    right,
    total,
    avoidCount,
    accuracy: total > 0 ? right / total : null,
    balancedAccuracy: coreRecalls.length ? mean(coreRecalls) : null,
    macroF1: macroF1.length ? mean(macroF1) : null,
    avoidRate: total > 0 ? avoidCount / total : null,
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
        usableBase[7] * usableBase[4],
        usableBase[13] * usableBase[20],
        usableBase[17] * usableBase[29],
        usableBase[38] * usableBase[50],
        usableBase[42] * usableBase[50],
        usableBase[50] * usableBase[51],
      ]);
    }
    rows.push({
      tradeDateUtc: current.tradeDateUtc,
      nextTradeDateUtc: next.tradeDateUtc,
      actualLabel,
      actualClass: classIndex(actualLabel),
      vector,
      eventScore: current.eventScore || 0,
      avoidSuggested: Boolean(current.avoidSuggested),
    });
  }
  return rows;
}

function standardizeRows(trainRows = [], otherRows = []) {
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

function standardizeMeta(trainRows = [], otherRows = []) {
  const dims = trainRows[0]?.metaFeatures?.length || 0;
  const means = [];
  const scales = [];
  for (let i = 0; i < dims; i += 1) {
    const col = trainRows.map((row) => row.metaFeatures[i]);
    means[i] = mean(col);
    scales[i] = stdDev(col);
  }
  const transform = (rows) => rows.map((row) => ({
    ...row,
    metaX: row.metaFeatures.map((value, index) => (value - means[index]) / scales[index]),
  }));
  return [transform(trainRows), ...otherRows.map(transform)];
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

function trainSoftmax(trainRows = [], options = {}) {
  const dims = trainRows[0]?.x?.length || 0;
  const classes = CLASS_NAMES.length;
  const weights = Array.from({ length: classes }, () => Array.from({ length: dims }, () => 0));
  const bias = Array.from({ length: classes }, () => 0);
  const learningRate = options.learningRate ?? 0.03;
  const epochs = options.epochs ?? 600;
  const reg = options.reg ?? 0.001;
  const classWeightPower = options.classWeightPower ?? 1;
  const counts = Array.from({ length: classes }, () => 0);
  trainRows.forEach((row) => { counts[row.actualClass] += 1; });
  const classWeights = counts.map((count) => (count > 0 ? (trainRows.length / (classes * count)) ** classWeightPower : 0));

  for (let epoch = 0; epoch < epochs; epoch += 1) {
    const gradW = Array.from({ length: classes }, () => Array.from({ length: dims }, () => 0));
    const gradB = Array.from({ length: classes }, () => 0);
    trainRows.forEach((row) => {
      const logits = weights.map((weightRow, classIdx) => (
        weightRow.reduce((sum, weight, dimIndex) => sum + (weight * row.x[dimIndex]), bias[classIdx] + (options.classBias?.[classIdx] || 0))
      ));
      const probs = softmax(logits);
      const sampleWeight = classWeights[row.actualClass] || 1;
      for (let classIdx = 0; classIdx < classes; classIdx += 1) {
        const target = classIdx === row.actualClass ? 1 : 0;
        const error = (probs[classIdx] - target) * sampleWeight;
        gradB[classIdx] += error;
        for (let dimIndex = 0; dimIndex < dims; dimIndex += 1) gradW[classIdx][dimIndex] += error * row.x[dimIndex];
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
  return { weights, bias, classBias: options.classBias || Array.from({ length: classes }, () => 0) };
}

function predictSoftmax(model, rows = []) {
  return rows.map((row) => {
    const logits = model.weights.map((weightRow, classIdx) => (
      weightRow.reduce((sum, weight, dimIndex) => sum + (weight * row.x[dimIndex]), model.bias[classIdx] + (model.classBias?.[classIdx] || 0))
    ));
    const probabilities = softmax(logits);
    const predictedClass = argMax(probabilities);
    const sorted = [...probabilities].sort((a, b) => b - a);
    return {
      ...row,
      probabilities,
      predictedClass,
      predictedLabel: CLASS_NAMES[predictedClass],
      confidence: sorted[0],
      margin: sorted[0] - sorted[1],
      entropy: -probabilities.reduce((sum, p) => sum + (p > 0 ? p * Math.log(p) : 0), 0),
    };
  });
}

function buildMetaDataset(predictions = [], wrongPenalty = 1.0) {
  return predictions.map((row) => ({
    ...row,
    metaTarget: row.predictedLabel !== row.actualLabel ? 1 : 0,
    metaFeatures: [
      row.confidence,
      row.margin,
      row.entropy,
      row.eventScore,
      row.avoidSuggested ? 1 : 0,
      row.predictedClass === classIndex('Crash') ? 1 : 0,
      row.predictedClass === classIndex('Stress') ? 1 : 0,
      row.predictedClass === classIndex('Normal') ? 1 : 0,
      row.predictedClass === classIndex('Calm') ? 1 : 0,
      ...row.probabilities,
    ],
    metaWeight: row.predictedLabel !== row.actualLabel ? wrongPenalty : 1,
  }));
}

function trainMetaLogistic(rows = [], options = {}) {
  const dims = rows[0]?.metaX?.length || 0;
  const weights = Array.from({ length: dims }, () => 0);
  let bias = 0;
  const learningRate = options.learningRate ?? 0.05;
  const epochs = options.epochs ?? 500;
  const reg = options.reg ?? 0.001;
  const positives = rows.filter((row) => row.metaTarget === 1).length;
  const negatives = Math.max(1, rows.length - positives);
  const posWeight = (rows.length / (2 * Math.max(1, positives))) * (options.posMultiplier ?? 1.5);
  const negWeight = rows.length / (2 * negatives);

  for (let epoch = 0; epoch < epochs; epoch += 1) {
    const grad = Array.from({ length: dims }, () => 0);
    let gradBias = 0;
    rows.forEach((row) => {
      const logit = row.metaX.reduce((sum, value, index) => sum + (weights[index] * value), bias);
      const prob = sigmoid(logit);
      const sampleWeight = (row.metaTarget === 1 ? posWeight : negWeight) * (row.metaWeight || 1);
      const error = (prob - row.metaTarget) * sampleWeight;
      gradBias += error;
      row.metaX.forEach((value, index) => { grad[index] += error * value; });
    });
    for (let i = 0; i < dims; i += 1) {
      grad[i] = (grad[i] / rows.length) + (reg * weights[i]);
      weights[i] -= learningRate * grad[i];
    }
    bias -= learningRate * (gradBias / rows.length);
  }
  return { weights, bias };
}

function applyMetaAvoid(metaModel, rows = [], threshold) {
  return rows.map((row) => {
    const logit = row.metaX.reduce((sum, value, index) => sum + (metaModel.weights[index] * value), metaModel.bias);
    const avoidProbability = sigmoid(logit);
    return {
      ...row,
      avoidProbability,
      predictedLabel: avoidProbability >= threshold ? 'Avoid' : row.predictedLabel,
    };
  });
}

function candidateBaseModels() {
  const out = [];
  const classBiasSets = [
    [0, 0, 0, 0],
    [0.1, 0.1, 0.2, 0.1],
    [0.2, 0.2, 0, 0.2],
  ];
  [1.0, 1.5].forEach((classWeightPower) => {
    [0.015, 0.03].forEach((learningRate) => {
      [0.0005, 0.001].forEach((reg) => {
        [500, 700].forEach((epochs) => {
          ['base', 'poly2'].forEach((featureMode) => classBiasSets.forEach((classBias) => {
            out.push({ featureMode, classWeightPower, learningRate, reg, epochs, classBias });
          }));
        });
      });
    });
  });
  return out;
}

function candidateMetaConfigs() {
  const out = [];
  [1.25, 1.5, 2.0].forEach((posMultiplier) => {
    [0.03, 0.05].forEach((learningRate) => {
      [0.0005, 0.001].forEach((reg) => {
        [350, 500].forEach((epochs) => {
          [0.45, 0.5, 0.55, 0.6].forEach((threshold) => {
            out.push({ posMultiplier, learningRate, reg, epochs, threshold });
          });
        });
      });
    });
  });
  return out;
}

function scoreMetrics(metrics) {
  const balanced = metrics.balancedAccuracy ?? -Infinity;
  const macroF1 = metrics.macroF1 ?? -Infinity;
  const accuracy = metrics.accuracy ?? -Infinity;
  const avoidRate = metrics.avoidRate ?? 0;
  return (balanced * 2.0) + (macroF1 * 1.35) + (accuracy * 0.75) - (Math.max(0, avoidRate - 0.25) * 0.75);
}

function summarizeMetrics(metrics) {
  return {
    accuracy: metrics.accuracy,
    balancedAccuracy: metrics.balancedAccuracy,
    macroF1: metrics.macroF1,
    avoidRate: metrics.avoidRate,
    byLabel: metrics.byLabel,
    confusion: metrics.confusion,
  };
}

function run() {
  const artifact = JSON.parse(fs.readFileSync(ARTIFACT_PATH, 'utf8'));
  const windows = buildSequentialWindows(artifact.daily.features || []);
  const labels = labelGrid();
  const baseModels = candidateBaseModels();
  const metaConfigs = candidateMetaConfigs();
  const rng = createRng(20260321);
  let best = null;

  for (let i = 0; i < SEARCH_ITERATIONS; i += 1) {
    const labelConfig = pick(rng, labels);
    const baseSpec = pick(rng, baseModels);
    const metaSpec = pick(rng, metaConfigs);

    const trainRowsRaw = buildDataset(windows.train.rows, labelConfig, baseSpec.featureMode);
    const selectionRowsRaw = buildDataset(windows.selection.rows, labelConfig, baseSpec.featureMode);
    const holdout1RowsRaw = buildDataset(windows.holdout1.rows, labelConfig, baseSpec.featureMode);
    const holdout2RowsRaw = buildDataset(windows.holdout2.rows, labelConfig, baseSpec.featureMode);
    if (!trainRowsRaw.length || !selectionRowsRaw.length || !holdout1RowsRaw.length || !holdout2RowsRaw.length) continue;

    const splitIndex = Math.max(20, Math.floor(trainRowsRaw.length * 0.67));
    const baseTrainRaw = trainRowsRaw.slice(0, splitIndex);
    const metaTrainRaw = trainRowsRaw.slice(splitIndex);
    if (metaTrainRaw.length < 10) continue;

    const [baseTrain, metaTrainPreview] = standardizeRows(
      baseTrainRaw,
      [metaTrainRaw],
    );

    const basePreviewModel = trainSoftmax(baseTrain, baseSpec);
    const metaTrainPred = predictSoftmax(basePreviewModel, metaTrainPreview);
    const metaRowsRaw = buildMetaDataset(metaTrainPred, 1.75);
    const [metaTrain] = standardizeMeta(metaRowsRaw, []);
    const metaModel = trainMetaLogistic(metaTrain, metaSpec);

    const [trainRows, selectionRows, holdout1Rows, holdout2Rows] = standardizeRows(
      trainRowsRaw,
      [selectionRowsRaw, holdout1RowsRaw, holdout2RowsRaw],
    );
    const baseModel = trainSoftmax(trainRows, baseSpec);
    const selectionPredBase = predictSoftmax(baseModel, selectionRows);
    const holdout1PredBase = predictSoftmax(baseModel, holdout1Rows);
    const holdout2PredBase = predictSoftmax(baseModel, holdout2Rows);
    const [selectionMeta, holdout1Meta, holdout2Meta] = standardizeMeta(
      metaRowsRaw,
      [
        buildMetaDataset(selectionPredBase, 1.75),
        buildMetaDataset(holdout1PredBase, 1.75),
        buildMetaDataset(holdout2PredBase, 1.75),
      ],
    ).slice(1);

    const selectionPred = applyMetaAvoid(metaModel, selectionMeta, metaSpec.threshold);
    const holdout1Pred = applyMetaAvoid(metaModel, holdout1Meta, metaSpec.threshold);
    const holdout2Pred = applyMetaAvoid(metaModel, holdout2Meta, metaSpec.threshold);
    const selectionMetrics = evaluateClassification(selectionPred, true);
    const holdout1Metrics = evaluateClassification(holdout1Pred, true);
    const holdout2Metrics = evaluateClassification(holdout2Pred, true);
    const score = scoreMetrics(selectionMetrics);

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
        baseSpec,
        metaSpec,
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
    windows: {
      train: windows.train.range,
      selection: windows.selection.range,
      holdout1: windows.holdout1.range,
      holdout2: windows.holdout2.range,
    },
    bestModel: {
      score: best.score,
      labelConfig: best.labelConfig,
      baseSpec: best.baseSpec,
      metaSpec: best.metaSpec,
      selection: summarizeMetrics(best.selection),
      holdout1: summarizeMetrics(best.holdout1),
      holdout2: summarizeMetrics(best.holdout2),
    },
  };

  fs.mkdirSync(OUTPUT_DIR, { recursive: true });
  const reportPath = path.join(OUTPUT_DIR, 'vixregime-learned-avoid-model-search.json');
  fs.writeFileSync(reportPath, `${JSON.stringify(report, null, 2)}\n`, 'utf8');
  console.log(JSON.stringify({
    reportPath,
    windows: report.windows,
    bestModel: report.bestModel,
  }, null, 2));
}

run();
