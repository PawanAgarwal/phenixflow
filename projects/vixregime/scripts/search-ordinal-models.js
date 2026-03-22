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
const SEARCH_ITERATIONS = Math.max(50, Math.trunc(Number(process.env.SEARCH_ITERATIONS || 700)));

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
      current.eventScore,
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
        usableBase[38] * usableBase[46],
      ]);
    }
    rows.push({
      tradeDateUtc: current.tradeDateUtc,
      nextTradeDateUtc: next.tradeDateUtc,
      actualLabel,
      vector,
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
  [0.015, 0.03].forEach((learningRate) => {
    [0.0005, 0.001].forEach((reg) => {
      [450, 650].forEach((epochs) => {
        ['base', 'poly2'].forEach((featureMode) => {
          [1.0, 1.5, 2.0].forEach((upPosMultiplier) => {
            [1.0, 1.5, 2.0].forEach((downPosMultiplier) => {
              [1.0, 1.5, 2.0].forEach((crashPosMultiplier) => {
                [0.45, 0.5, 0.55, 0.6].forEach((upThreshold) => {
                  [0.45, 0.5, 0.55, 0.6].forEach((downThreshold) => {
                    [0.45, 0.5, 0.55, 0.6].forEach((crashThreshold) => {
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

function fitOrdinalModel(trainRows = [], spec) {
  const upRows = trainRows.map((row) => ({ ...row, binaryTarget: row.actualLabel === 'Calm' ? 1 : 0 }));
  const downRows = trainRows.map((row) => ({ ...row, binaryTarget: ['Crash', 'Stress'].includes(row.actualLabel) ? 1 : 0 }));
  const crashRows = trainRows
    .filter((row) => ['Crash', 'Stress'].includes(row.actualLabel))
    .map((row) => ({ ...row, binaryTarget: row.actualLabel === 'Crash' ? 1 : 0 }));

  return {
    upModel: trainBinaryLogistic(upRows, { learningRate: spec.learningRate, reg: spec.reg, epochs: spec.epochs, posMultiplier: spec.upPosMultiplier }),
    downModel: trainBinaryLogistic(downRows, { learningRate: spec.learningRate, reg: spec.reg, epochs: spec.epochs, posMultiplier: spec.downPosMultiplier }),
    crashModel: crashRows.length ? trainBinaryLogistic(crashRows, { learningRate: spec.learningRate, reg: spec.reg, epochs: spec.epochs, posMultiplier: spec.crashPosMultiplier }) : null,
    spec,
  };
}

function predictOrdinal(model, rows = []) {
  const upProbs = predictBinary(model.upModel, rows);
  const downProbs = predictBinary(model.downModel, rows);
  const crashEligible = rows.map((row) => row);
  const crashProbs = model.crashModel ? predictBinary(model.crashModel, crashEligible) : rows.map(() => 0);

  return rows.map((row, index) => {
    const upProb = upProbs[index];
    const downProb = downProbs[index];
    const crashProb = crashProbs[index];
    let predictedLabel = 'Normal';
    if (downProb >= model.spec.downThreshold && downProb >= upProb) {
      predictedLabel = crashProb >= model.spec.crashThreshold ? 'Crash' : 'Stress';
    } else if (upProb >= model.spec.upThreshold && upProb > downProb) {
      predictedLabel = 'Calm';
    }
    return {
      ...row,
      predictedLabel,
      upProb,
      downProb,
      crashProb,
    };
  });
}

function scoreMetrics(metrics) {
  const balanced = metrics.balancedAccuracy ?? -Infinity;
  const macroF1 = metrics.macroF1 ?? -Infinity;
  const accuracy = metrics.accuracy ?? -Infinity;
  return (balanced * 2.0) + (macroF1 * 1.3) + (accuracy * 0.8);
}

function summarize(metrics) {
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
  const specs = candidateSpecs();
  const rng = createRng(20260321);
  let best = null;

  for (let i = 0; i < SEARCH_ITERATIONS; i += 1) {
    const labelConfig = pick(rng, labels);
    const spec = pick(rng, specs);
    const trainRaw = buildDataset(windows.train.rows, labelConfig, spec.featureMode);
    const selectionRaw = buildDataset(windows.selection.rows, labelConfig, spec.featureMode);
    const holdout1Raw = buildDataset(windows.holdout1.rows, labelConfig, spec.featureMode);
    const holdout2Raw = buildDataset(windows.holdout2.rows, labelConfig, spec.featureMode);
    if (!trainRaw.length || !selectionRaw.length || !holdout1Raw.length || !holdout2Raw.length) continue;

    const [trainRows, selectionRows, holdout1Rows, holdout2Rows] = standardize(
      trainRaw,
      [selectionRaw, holdout1Raw, holdout2Raw],
    );
    const model = fitOrdinalModel(trainRows, spec);
    const selectionMetrics = evaluateClassification(predictOrdinal(model, selectionRows));
    const holdout1Metrics = evaluateClassification(predictOrdinal(model, holdout1Rows));
    const holdout2Metrics = evaluateClassification(predictOrdinal(model, holdout2Rows));
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
        spec,
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
      spec: best.spec,
      selection: summarize(best.selection),
      holdout1: summarize(best.holdout1),
      holdout2: summarize(best.holdout2),
    },
  };

  fs.mkdirSync(OUTPUT_DIR, { recursive: true });
  const reportPath = path.join(OUTPUT_DIR, 'vixregime-ordinal-model-search.json');
  fs.writeFileSync(reportPath, `${JSON.stringify(report, null, 2)}\n`, 'utf8');
  console.log(JSON.stringify({
    reportPath,
    windows: report.windows,
    bestModel: report.bestModel,
  }, null, 2));
}

run();
