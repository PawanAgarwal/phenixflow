#!/usr/bin/env node
const fs = require('node:fs');
const path = require('node:path');

const {
  readThresholdConfig,
  applyDailyThresholdConfig,
} = require('../src/vix-regime');

const ARTIFACT_PATH = path.resolve(
  process.env.ARTIFACT_PATH
    || path.join(process.cwd(), 'vixregime', 'artifacts', 'reports', 'vixregime-backtest-2025-01-02-2026-03-21.json'),
);
const OUTPUT_DIR = path.resolve(
  process.env.OUTPUT_DIR
    || path.join(process.cwd(), 'vixregime', 'artifacts', 'reports'),
);
const DEFAULT_CONFIG_PATH = path.resolve(
  process.env.DEFAULT_CONFIG_PATH
    || path.join(process.cwd(), 'vixregime', 'config', 'vix-regime-thresholds.json'),
);
const TRAIN_DAYS = Math.max(60, Math.trunc(Number(process.env.TRAIN_DAYS || 189)));
const TEST_DAYS = Math.max(20, Math.trunc(Number(process.env.TEST_DAYS || 63)));
const SEARCH_ITERATIONS = Math.max(50, Math.trunc(Number(process.env.SEARCH_ITERATIONS || 400)));

const POLICIES = Object.freeze({
  conservative_cash: { Calm: 'SPXL', Normal: 'SPY', Stress: 'CASH', Crash: 'CASH' },
  stress_hedge: { Calm: 'SPXL', Normal: 'SPY', Stress: 'SPXS', Crash: 'CASH' },
  crash_hedge: { Calm: 'SPXL', Normal: 'SPY', Stress: 'CASH', Crash: 'SPXS' },
  full_hedge: { Calm: 'SPXL', Normal: 'SPY', Stress: 'SPXS', Crash: 'SPXS' },
});

function toNumber(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function cloneJson(value) {
  return JSON.parse(JSON.stringify(value));
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

function splitDailyRows(features = []) {
  const rows = features.filter((row) => row && row.tradeDateUtc && row.spyClose > 0 && row.spxlClose > 0 && row.spxsClose > 0);
  const needed = TRAIN_DAYS + TEST_DAYS + 1;
  if (rows.length < needed) throw new Error(`insufficient_daily_rows:${rows.length}:${needed}`);
  const windowRows = rows.slice(-needed);
  return {
    all: windowRows,
    train: windowRows.slice(0, TRAIN_DAYS + 1),
    test: windowRows.slice(TRAIN_DAYS, TRAIN_DAYS + TEST_DAYS + 1),
  };
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

function enrichWithTargets(features = [], labelConfig) {
  const rows = [];
  for (let i = 0; i < features.length - 1; i += 1) {
    const current = features[i];
    const next = features[i + 1];
    const spyReturn = computeReturn(current.spyClose, next.spyClose);
    const spxlReturn = computeReturn(current.spxlClose, next.spxlClose);
    const spxsReturn = computeReturn(current.spxsClose, next.spxsClose);
    rows.push({
      ...current,
      nextTradeDateUtc: next.tradeDateUtc,
      nextSpyReturn: spyReturn,
      nextSpxlReturn: spxlReturn,
      nextSpxsReturn: spxsReturn,
      actualLabel: labelReturn(spyReturn, labelConfig),
    });
  }
  return rows;
}

function emptyConfusion() {
  const labels = ['Calm', 'Normal', 'Stress', 'Crash'];
  const out = {};
  labels.forEach((actual) => {
    out[actual] = {};
    labels.forEach((predicted) => {
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
  rows.forEach((row) => {
    if (!row.actualLabel || !row.regime) return;
    total += 1;
    confusion[row.actualLabel][row.regime] += 1;
    if (!byLabel[row.actualLabel]) byLabel[row.actualLabel] = { total: 0, right: 0 };
    byLabel[row.actualLabel].total += 1;
    if (row.actualLabel === row.regime) {
      right += 1;
      byLabel[row.actualLabel].right += 1;
    }
  });
  return {
    right,
    total,
    accuracy: total > 0 ? right / total : null,
    byLabel,
    confusion,
  };
}

function computePolicyPath(rows = [], policyMap) {
  let equity = 1;
  let benchmark = 1;
  rows.forEach((row) => {
    const action = policyMap[row.regime] || 'CASH';
    let ret = 0;
    if (action === 'SPY') ret = row.nextSpyReturn ?? 0;
    if (action === 'SPXL') ret = row.nextSpxlReturn ?? 0;
    if (action === 'SPXS') ret = row.nextSpxsReturn ?? 0;
    equity *= (1 + ret);
    benchmark *= (1 + (row.nextSpyReturn ?? 0));
  });
  return {
    endingEquity: equity,
    benchmarkEndingEquity: benchmark,
    relativeEquity: (equity / Math.max(1e-9, benchmark)) - 1,
    cumulativeReturn: equity - 1,
    benchmarkReturn: benchmark - 1,
  };
}

function evaluatePolicies(rows = []) {
  return Object.fromEntries(Object.entries(POLICIES).map(([name, policyMap]) => (
    [name, { mapping: policyMap, ...computePolicyPath(rows, policyMap) }]
  )));
}

function evaluateCandidate(baseRows, thresholdConfig, labelConfig) {
  const reclassified = applyDailyThresholdConfig(baseRows, thresholdConfig);
  const withTargets = enrichWithTargets(reclassified, labelConfig);
  const classification = evaluateClassification(withTargets);
  const policies = evaluatePolicies(withTargets);
  const anchorPolicy = policies.conservative_cash;
  const score = (
    (classification.accuracy ?? 0)
    + ((anchorPolicy.relativeEquity ?? 0) * 1.5)
    - Math.max(0, (classification.confusion.Crash?.Normal || 0) * 0.02)
    - Math.max(0, (classification.confusion.Crash?.Calm || 0) * 0.03)
  );
  return {
    score,
    thresholdConfig,
    labelConfig,
    classification,
    policies,
  };
}

function buildCandidate(baseConfig, rng) {
  const candidate = cloneJson(baseConfig);
  candidate.exposures.calm = pick(rng, [1.25, 1.5, 1.75, 2.0]);
  candidate.exposures.normal = pick(rng, [0.85, 0.9, 1.0, 1.05]);
  candidate.exposures.stress = pick(rng, [0.0, 0.1, 0.2, 0.3, 0.4]);
  candidate.exposures.crash = pick(rng, [-0.25, 0.0, 0.1]);
  candidate.day.vixCalmMax = pick(rng, [15, 16, 17, 18, 19, 20]);
  candidate.day.vixNormalMax = pick(rng, [22, 24, 25, 26, 28, 30]);
  candidate.day.vixStressMin = pick(rng, [22, 24, 25, 27, 28, 30, 32]);
  candidate.day.vixCrashMin = pick(rng, [30, 32, 35, 38, 40, 45]);
  candidate.day.vixPctRankCalmMax = pick(rng, [0.2, 0.25, 0.3, 0.35, 0.4]);
  candidate.day.vixPctRankStressMin = pick(rng, [0.7, 0.75, 0.8, 0.85, 0.9]);
  candidate.day.delta5NormalMax = pick(rng, [0, 1, 2, 3, 4]);
  candidate.day.delta5CrashMin = pick(rng, [4, 5, 6, 8]);
  candidate.day.delta10CalmMax = pick(rng, [-2, 0, 1, 2]);
  candidate.day.termSoft9d30 = pick(rng, [1.01, 1.02, 1.03, 1.05, 1.07]);
  candidate.day.termHard9d30 = pick(rng, [1.08, 1.1, 1.12, 1.15]);
  candidate.day.termSoft30d90d = pick(rng, [1.0, 1.01, 1.02, 1.03]);
  candidate.day.termHard30d90d = pick(rng, [1.03, 1.05, 1.07, 1.1]);
  candidate.day.crashBrakeVix1dMultiple = pick(rng, [1.35, 1.45, 1.6, 1.8]);
  candidate.day.crashBrakeVix1d9d = pick(rng, [1.2, 1.25, 1.3, 1.4]);
  if (candidate.day.vixStressMin < candidate.day.vixCalmMax + 3) candidate.day.vixStressMin = candidate.day.vixCalmMax + 3;
  if (candidate.day.vixCrashMin < candidate.day.vixStressMin + 4) candidate.day.vixCrashMin = candidate.day.vixStressMin + 4;
  return candidate;
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

function summarize(result) {
  return {
    score: result.score,
    labelConfig: result.labelConfig,
    classification: result.classification,
    policies: result.policies,
    thresholdConfig: result.thresholdConfig,
  };
}

function run() {
  const artifact = JSON.parse(fs.readFileSync(ARTIFACT_PATH, 'utf8'));
  const defaultConfig = readThresholdConfig(DEFAULT_CONFIG_PATH);
  const split = splitDailyRows(artifact.daily.features || []);
  const rng = createRng();

  const allResults = [];
  let bestOverall = null;

  labelGrid().forEach((labelConfig) => {
    let bestTrain = evaluateCandidate(split.train, defaultConfig, labelConfig);
    for (let i = 0; i < SEARCH_ITERATIONS; i += 1) {
      const candidate = buildCandidate(defaultConfig, rng);
      const trainResult = evaluateCandidate(split.train, candidate, labelConfig);
      if (trainResult.score > bestTrain.score) bestTrain = trainResult;
    }
    const baselineTrain = evaluateCandidate(split.train, defaultConfig, labelConfig);
    const baselineTest = evaluateCandidate(split.test, defaultConfig, labelConfig);
    const tunedTrain = evaluateCandidate(split.train, bestTrain.thresholdConfig, labelConfig);
    const tunedTest = evaluateCandidate(split.test, bestTrain.thresholdConfig, labelConfig);
    const row = {
      labelConfig,
      baseline: { train: summarize(baselineTrain), test: summarize(baselineTest) },
      tuned: { train: summarize(tunedTrain), test: summarize(tunedTest) },
    };
    allResults.push(row);
    if (!bestOverall || (tunedTest.classification.accuracy ?? -1) > (bestOverall.tuned.test.classification.accuracy ?? -1)) {
      bestOverall = row;
    }
  });

  const report = {
    generatedAt: new Date().toISOString(),
    sourceArtifact: ARTIFACT_PATH,
    trainDays: TRAIN_DAYS,
    testDays: TEST_DAYS,
    trainRange: {
      startDate: split.train[0].tradeDateUtc,
      endDate: split.train[split.train.length - 2].tradeDateUtc,
    },
    testRange: {
      startDate: split.test[0].tradeDateUtc,
      endDate: split.test[split.test.length - 2].tradeDateUtc,
    },
    searchIterations: SEARCH_ITERATIONS,
    policies: POLICIES,
    bestOverall,
    allResults,
  };

  fs.mkdirSync(OUTPUT_DIR, { recursive: true });
  const reportPath = path.join(OUTPUT_DIR, 'vixregime-label-policy-search.json');
  fs.writeFileSync(reportPath, `${JSON.stringify(report, null, 2)}\n`, 'utf8');
  console.log(JSON.stringify({
    reportPath,
    bestLabelConfig: bestOverall.labelConfig,
    baselineTestAccuracy: bestOverall.baseline.test.classification.accuracy,
    tunedTestAccuracy: bestOverall.tuned.test.classification.accuracy,
    bestPolicyOnTunedTest: Object.entries(bestOverall.tuned.test.policies).sort((a, b) => b[1].relativeEquity - a[1].relativeEquity)[0],
  }, null, 2));
}

run();
