#!/usr/bin/env node
const fs = require('node:fs');
const path = require('node:path');

const {
  readThresholdConfig,
  applyDailyThresholdConfig,
  computePortfolioPath,
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
const TUNED_CONFIG_PATH = path.resolve(
  process.env.TUNED_CONFIG_PATH
    || path.join(process.cwd(), 'vixregime', 'config', 'vix-regime-thresholds.tuned.json'),
);
const SEARCH_ITERATIONS = Math.max(50, Math.trunc(Number(process.env.SEARCH_ITERATIONS || 600)));
const TRAIN_DAYS = Math.max(60, Math.trunc(Number(process.env.TRAIN_DAYS || 189)));
const TEST_DAYS = Math.max(20, Math.trunc(Number(process.env.TEST_DAYS || 63)));
const TOLERANCE_PCT = Number(process.env.TOLERANCE_PCT || 0.25);

function toNumber(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function cloneJson(value) {
  return JSON.parse(JSON.stringify(value));
}

function createRng(seed = 1337) {
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
  const withSignal = features.filter((row) => row && row.tradeDateUtc && row.spyClose > 0);
  const required = TRAIN_DAYS + TEST_DAYS + 1;
  if (withSignal.length < required) {
    throw new Error(`insufficient_daily_rows:${withSignal.length}:${required}`);
  }
  const windowRows = withSignal.slice(-required);
  return {
    all: windowRows,
    train: windowRows.slice(0, TRAIN_DAYS + 1),
    test: windowRows.slice(TRAIN_DAYS, TRAIN_DAYS + TEST_DAYS + 1),
  };
}

function classifyDayOutcome(regime, benchmarkReturn, tolerancePct) {
  const tol = tolerancePct / 100;
  const bench = toNumber(benchmarkReturn);
  if (bench === null) return 'no_signal';
  if (bench > tol) return (regime === 'Calm' || regime === 'Normal') ? 'right' : 'wrong';
  if (bench < -tol) return (regime === 'Stress' || regime === 'Crash') ? 'right' : 'wrong';
  return 'neutral';
}

function evaluateDirection(observations = [], tolerancePct = TOLERANCE_PCT) {
  const summary = { right: 0, wrong: 0, neutral: 0, no_signal: 0 };
  observations.forEach((row) => {
    summary[classifyDayOutcome(row.regime, row.benchmarkReturn, tolerancePct)] += 1;
  });
  return summary;
}

function evaluateCandidate(features, thresholdConfig) {
  const reclassified = applyDailyThresholdConfig(features, thresholdConfig);
  const backtest = computePortfolioPath(reclassified, { periodsPerYear: 252, transactionCostBps: thresholdConfig.execution.dailyTransactionCostBps });
  const direction = evaluateDirection(backtest.observations, TOLERANCE_PCT);
  const periods = Math.max(1, backtest.summary.periods);
  const rightRate = direction.right / periods;
  const wrongRate = direction.wrong / periods;
  const relativeEquity = (backtest.summary.endingEquity / Math.max(1e-9, backtest.summary.benchmarkEndingEquity)) - 1;
  const turnoverPerPeriod = backtest.summary.turnover / periods;
  const score = (
    (relativeEquity * 4)
    + (rightRate * 1.25)
    - (wrongRate * 1.5)
    - (Math.abs(Math.min(0, backtest.summary.maxDrawdown)) * 0.5)
    - (turnoverPerPeriod * 0.02)
  );
  return {
    score,
    thresholdConfig,
    summary: backtest.summary,
    direction,
    relativeEquity,
  };
}

function buildCandidate(baseConfig, rng) {
  const candidate = cloneJson(baseConfig);
  candidate.exposures.calm = pick(rng, [1.0, 1.15, 1.25, 1.4, 1.5, 1.75, 2.0]);
  candidate.exposures.normal = pick(rng, [0.85, 0.9, 1.0, 1.05]);
  candidate.exposures.stress = pick(rng, [0.0, 0.1, 0.2, 0.3, 0.4, 0.5]);
  candidate.exposures.crash = pick(rng, [-0.5, -0.25, 0.0, 0.1]);

  candidate.day.vixCalmMax = pick(rng, [14, 15, 16, 17, 18, 19]);
  candidate.day.vixNormalMax = pick(rng, [20, 22, 24, 25, 26, 28]);
  candidate.day.vixStressMin = pick(rng, [22, 24, 25, 26, 28, 30]);
  candidate.day.vixCrashMin = pick(rng, [30, 32, 35, 38, 40, 45]);
  candidate.day.vixPctRankCalmMax = pick(rng, [0.2, 0.25, 0.3, 0.35, 0.4]);
  candidate.day.vixPctRankStressMin = pick(rng, [0.7, 0.75, 0.8, 0.85, 0.9]);
  candidate.day.delta5NormalMax = pick(rng, [0, 1, 2, 3, 4]);
  candidate.day.delta5CrashMin = pick(rng, [4, 5, 6, 8, 10]);
  candidate.day.delta10CalmMax = pick(rng, [-4, -2, 0, 1, 2]);
  candidate.day.termSoft9d30 = pick(rng, [1.01, 1.02, 1.03, 1.05, 1.07, 1.1]);
  candidate.day.termHard9d30 = pick(rng, [1.08, 1.1, 1.12, 1.15, 1.2]);
  candidate.day.termSoft30d90d = pick(rng, [1.0, 1.01, 1.02, 1.03, 1.05]);
  candidate.day.termHard30d90d = pick(rng, [1.03, 1.05, 1.07, 1.1]);
  candidate.day.crashBrakeVix1dMultiple = pick(rng, [1.35, 1.45, 1.6, 1.8, 2.0]);
  candidate.day.crashBrakeVix1d9d = pick(rng, [1.2, 1.25, 1.3, 1.4, 1.5]);

  if (candidate.day.vixStressMin < candidate.day.vixCalmMax + 3) {
    candidate.day.vixStressMin = candidate.day.vixCalmMax + 3;
  }
  if (candidate.day.vixCrashMin < candidate.day.vixStressMin + 4) {
    candidate.day.vixCrashMin = candidate.day.vixStressMin + 4;
  }
  if (candidate.day.termHard9d30 < candidate.day.termSoft9d30 + 0.03) {
    candidate.day.termHard9d30 = Number((candidate.day.termSoft9d30 + 0.03).toFixed(2));
  }
  if (candidate.day.termHard30d90d < candidate.day.termSoft30d90d + 0.02) {
    candidate.day.termHard30d90d = Number((candidate.day.termSoft30d90d + 0.02).toFixed(2));
  }
  return candidate;
}

function summarizeResult(result) {
  return {
    score: result.score,
    relativeEquity: result.relativeEquity,
    summary: result.summary,
    direction: result.direction,
    thresholdConfig: result.thresholdConfig,
  };
}

function run() {
  const artifact = JSON.parse(fs.readFileSync(ARTIFACT_PATH, 'utf8'));
  const defaultConfig = readThresholdConfig(DEFAULT_CONFIG_PATH);
  const split = splitDailyRows(artifact.daily.features || []);
  const rng = createRng(20260321);

  let bestTrain = evaluateCandidate(split.train, defaultConfig);
  const leaderboard = [summarizeResult(bestTrain)];

  for (let i = 0; i < SEARCH_ITERATIONS; i += 1) {
    const candidate = buildCandidate(defaultConfig, rng);
    const trainResult = evaluateCandidate(split.train, candidate);
    if (trainResult.score > bestTrain.score) {
      bestTrain = trainResult;
    }
    if (i < 20 || trainResult.score > leaderboard[leaderboard.length - 1].score) {
      leaderboard.push(summarizeResult(trainResult));
      leaderboard.sort((left, right) => right.score - left.score);
      if (leaderboard.length > 15) leaderboard.length = 15;
    }
  }

  const baselineTrain = evaluateCandidate(split.train, defaultConfig);
  const baselineTest = evaluateCandidate(split.test, defaultConfig);
  const tunedTrain = evaluateCandidate(split.train, bestTrain.thresholdConfig);
  const tunedTest = evaluateCandidate(split.test, bestTrain.thresholdConfig);

  const report = {
    generatedAt: new Date().toISOString(),
    sourceArtifact: ARTIFACT_PATH,
    trainDays: TRAIN_DAYS,
    testDays: TEST_DAYS,
    trainRange: {
      startDate: split.train[0].tradeDateUtc,
      endDate: split.train[split.train.length - 2].tradeDateUtc,
      rows: split.train.length,
    },
    testRange: {
      startDate: split.test[0].tradeDateUtc,
      endDate: split.test[split.test.length - 2].tradeDateUtc,
      rows: split.test.length,
    },
    tolerancePct: TOLERANCE_PCT,
    searchIterations: SEARCH_ITERATIONS,
    baseline: {
      train: summarizeResult(baselineTrain),
      test: summarizeResult(baselineTest),
    },
    tuned: {
      train: summarizeResult(tunedTrain),
      test: summarizeResult(tunedTest),
    },
    leaderboard,
  };

  fs.mkdirSync(OUTPUT_DIR, { recursive: true });
  fs.writeFileSync(
    path.join(OUTPUT_DIR, 'vixregime-threshold-search.json'),
    `${JSON.stringify(report, null, 2)}\n`,
    'utf8',
  );
  fs.writeFileSync(TUNED_CONFIG_PATH, `${JSON.stringify(bestTrain.thresholdConfig, null, 2)}\n`, 'utf8');
  console.log(JSON.stringify({
    reportPath: path.join(OUTPUT_DIR, 'vixregime-threshold-search.json'),
    tunedConfigPath: TUNED_CONFIG_PATH,
    baselineTest: report.baseline.test,
    tunedTest: report.tuned.test,
  }, null, 2));
}

run();
