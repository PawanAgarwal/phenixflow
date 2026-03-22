#!/usr/bin/env node
const fs = require('node:fs');
const path = require('node:path');

const BACKTEST_ARTIFACT = path.resolve(
  process.env.BACKTEST_ARTIFACT
    || path.join(process.cwd(), 'vixregime', 'artifacts', 'reports', 'vixregime-backtest-2025-01-02-2026-03-21.json'),
);
const REPORT_9M = path.resolve(
  process.env.REPORT_9M
    || path.join(process.cwd(), 'vixregime', 'artifacts', 'reports', 'vixregime-label-policy-search.json'),
);
const REPORT_6M = path.resolve(
  process.env.REPORT_6M
    || path.join(process.cwd(), 'vixregime', 'artifacts', 'reports', '6m3m', 'vixregime-label-policy-search.json'),
);
const OUTPUT_PATH = path.resolve(
  process.env.OUTPUT_PATH
    || path.join(process.cwd(), 'vixregime', 'artifacts', 'reports', 'vixregime-factor-window-analysis.json'),
);

const FEATURE_SPECS = Object.freeze([
  { key: 'vix', label: 'VIX' },
  { key: 'vix1dOverVix', label: 'VIX1D/VIX' },
  { key: 'ts1d9d', label: 'VIX1D/VIX9D' },
  { key: 'ts9d30', label: 'VIX9D/VIX' },
  { key: 'ts30d90d', label: 'VIX/VIX3M' },
  { key: 'delta5', label: 'dVIX_5d' },
  { key: 'delta10', label: 'dVIX_10d' },
  { key: 'vixPctRank', label: 'VIX_pct_rank_252d' },
  { key: 'spyClose', label: 'SPY_close' },
]);

function toNumber(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function readJson(filePath) {
  return JSON.parse(fs.readFileSync(filePath, 'utf8'));
}

function mean(values = []) {
  if (!values.length) return null;
  return values.reduce((sum, value) => sum + value, 0) / values.length;
}

function stdDev(values = []) {
  if (values.length < 2) return null;
  const avg = mean(values);
  const variance = values.reduce((sum, value) => sum + ((value - avg) ** 2), 0) / (values.length - 1);
  return Math.sqrt(Math.max(variance, 0));
}

function dot(a = [], b = []) {
  let sum = 0;
  for (let i = 0; i < a.length; i += 1) sum += (a[i] || 0) * (b[i] || 0);
  return sum;
}

function multiplyMatrixVector(matrix = [], vector = []) {
  return matrix.map((row) => dot(row, vector));
}

function vectorNorm(vector = []) {
  return Math.sqrt(dot(vector, vector));
}

function normalize(vector = []) {
  const norm = vectorNorm(vector);
  if (!norm) return vector.map(() => 0);
  return vector.map((value) => value / norm);
}

function outerProduct(vector = []) {
  return vector.map((left) => vector.map((right) => left * right));
}

function subtractScaledOuter(matrix = [], vector = [], scalar = 0) {
  const outer = outerProduct(vector);
  return matrix.map((row, rowIndex) => row.map((value, colIndex) => value - (scalar * outer[rowIndex][colIndex])));
}

function powerIteration(matrix = [], iterations = 100) {
  let vector = normalize(matrix.map((_, index) => index + 1));
  for (let i = 0; i < iterations; i += 1) {
    vector = normalize(multiplyMatrixVector(matrix, vector));
  }
  const mv = multiplyMatrixVector(matrix, vector);
  const eigenvalue = dot(vector, mv);
  return { eigenvalue, eigenvector: vector };
}

function covarianceMatrix(rows = []) {
  if (!rows.length) return [];
  const cols = rows[0].length;
  const means = Array.from({ length: cols }, (_, index) => mean(rows.map((row) => row[index])));
  const matrix = Array.from({ length: cols }, () => Array.from({ length: cols }, () => 0));
  for (let i = 0; i < cols; i += 1) {
    for (let j = 0; j < cols; j += 1) {
      let sum = 0;
      rows.forEach((row) => {
        sum += (row[i] - means[i]) * (row[j] - means[j]);
      });
      matrix[i][j] = rows.length > 1 ? sum / (rows.length - 1) : 0;
    }
  }
  return matrix;
}

function pca(rows = [], featureLabels = [], maxComponents = 3) {
  const matrix = covarianceMatrix(rows);
  if (!matrix.length) return { explainedVarianceRatio: [], components: [], featureImportance: [] };

  const totalVariance = matrix.reduce((sum, row, index) => sum + row[index], 0);
  let working = matrix.map((row) => [...row]);
  const components = [];

  for (let i = 0; i < Math.min(maxComponents, matrix.length); i += 1) {
    const { eigenvalue, eigenvector } = powerIteration(working, 200);
    if (!Number.isFinite(eigenvalue) || eigenvalue <= 1e-10) break;
    components.push({
      component: `PC${i + 1}`,
      eigenvalue,
      explainedVarianceRatio: totalVariance > 0 ? eigenvalue / totalVariance : 0,
      loadings: Object.fromEntries(featureLabels.map((label, index) => [label, eigenvector[index]])),
    });
    working = subtractScaledOuter(working, eigenvector, eigenvalue);
  }

  const importance = featureLabels.map((label) => {
    let score = 0;
    components.forEach((component) => {
      score += Math.abs(component.loadings[label] || 0) * component.explainedVarianceRatio;
    });
    return { feature: label, weightedLoading: score };
  }).sort((left, right) => right.weightedLoading - left.weightedLoading);

  return {
    explainedVarianceRatio: components.map((component) => ({
      component: component.component,
      explainedVarianceRatio: component.explainedVarianceRatio,
    })),
    components,
    featureImportance: importance,
  };
}

function getWindowFeatures(backtest, startDate, endDate) {
  return (backtest.daily?.features || []).filter((row) => row.tradeDateUtc >= startDate && row.tradeDateUtc <= endDate);
}

function cleanRows(rows = []) {
  return rows.filter((row) => FEATURE_SPECS.every(({ key }) => toNumber(row[key]) !== null));
}

function zScoreRows(rows = []) {
  const valuesByKey = Object.fromEntries(FEATURE_SPECS.map(({ key }) => [key, rows.map((row) => toNumber(row[key]))]));
  const stats = Object.fromEntries(FEATURE_SPECS.map(({ key }) => {
    const values = valuesByKey[key];
    return [key, { mean: mean(values), stdDev: stdDev(values) || 1 }];
  }));
  const matrix = rows.map((row) => FEATURE_SPECS.map(({ key }) => ((toNumber(row[key]) - stats[key].mean) / stats[key].stdDev)));
  return { matrix, stats };
}

function nextSpyReturn(currentRow, nextRow) {
  const current = toNumber(currentRow?.spyClose);
  const next = toNumber(nextRow?.spyClose);
  if (current === null || next === null || current <= 0) return null;
  return (next / current) - 1;
}

function labelNextReturn(ret, labelConfig) {
  if (ret === null) return null;
  if (ret <= labelConfig.crashMax) return 'Crash';
  if (ret <= labelConfig.stressMax) return 'Stress';
  if (ret < labelConfig.calmMin) return 'Normal';
  return 'Calm';
}

function summarizeByActualLabel(rows = [], labelConfig) {
  const grouped = { Calm: [], Normal: [], Stress: [], Crash: [] };
  for (let i = 0; i < rows.length - 1; i += 1) {
    const current = rows[i];
    const next = rows[i + 1];
    const label = labelNextReturn(nextSpyReturn(current, next), labelConfig);
    if (!label) continue;
    grouped[label].push(current);
  }

  return Object.fromEntries(Object.entries(grouped).map(([label, labelRows]) => {
    const summary = Object.fromEntries(FEATURE_SPECS.map(({ key, label: featureLabel }) => {
      const values = labelRows.map((row) => toNumber(row[key])).filter((value) => value !== null);
      return [featureLabel, {
        mean: mean(values),
        stdDev: stdDev(values),
      }];
    }));
    return [label, { count: labelRows.length, features: summary }];
  }));
}

function buildWindowAnalysis(name, report, backtest) {
  const startDate = report.trainRange.startDate;
  const endDate = report.trainRange.endDate;
  const windowRows = cleanRows(getWindowFeatures(backtest, startDate, endDate));
  const { matrix } = zScoreRows(windowRows);
  const pcaResult = pca(matrix, FEATURE_SPECS.map((feature) => feature.label), 3);
  return {
    name,
    trainRange: report.trainRange,
    testRange: report.testRange,
    labelConfig: report.bestOverall.labelConfig,
    learnedPolicy: {
      mapping: report.bestOverall.tuned.test.policies.stress_hedge.mapping,
      exposures: report.bestOverall.tuned.test.thresholdConfig.exposures,
      dayThresholds: report.bestOverall.tuned.test.thresholdConfig.day,
      testAccuracy: report.bestOverall.tuned.test.classification.accuracy,
      bestPolicyRelativeEquity: report.bestOverall.tuned.test.policies.stress_hedge.relativeEquity,
    },
    rowCount: windowRows.length,
    pca: pcaResult,
    featureByActualLabel: summarizeByActualLabel(windowRows, report.bestOverall.labelConfig),
  };
}

function compareImportance(nineMonth = [], sixMonth = []) {
  const map9 = Object.fromEntries(nineMonth.map((row) => [row.feature, row.weightedLoading]));
  const map6 = Object.fromEntries(sixMonth.map((row) => [row.feature, row.weightedLoading]));
  return FEATURE_SPECS.map(({ label }) => ({
    feature: label,
    nineMonthWeightedLoading: map9[label] ?? null,
    sixMonthWeightedLoading: map6[label] ?? null,
    deltaSixMinusNine: (map6[label] ?? 0) - (map9[label] ?? 0),
  })).sort((left, right) => Math.abs(right.deltaSixMinusNine) - Math.abs(left.deltaSixMinusNine));
}

function run() {
  const backtest = readJson(BACKTEST_ARTIFACT);
  const report9m = readJson(REPORT_9M);
  const report6m = readJson(REPORT_6M);

  const analysis9m = buildWindowAnalysis('9m_train_3m_test', report9m, backtest);
  const analysis6m = buildWindowAnalysis('6m_train_3m_test', report6m, backtest);

  const out = {
    generatedAt: new Date().toISOString(),
    sourceBacktestArtifact: BACKTEST_ARTIFACT,
    reports: {
      nineMonth: REPORT_9M,
      sixMonth: REPORT_6M,
    },
    nineMonth: analysis9m,
    sixMonth: analysis6m,
    importanceComparison: compareImportance(
      analysis9m.pca.featureImportance,
      analysis6m.pca.featureImportance,
    ),
  };

  fs.mkdirSync(path.dirname(OUTPUT_PATH), { recursive: true });
  fs.writeFileSync(OUTPUT_PATH, `${JSON.stringify(out, null, 2)}\n`, 'utf8');
  console.log(JSON.stringify({
    outputPath: OUTPUT_PATH,
    nineMonthTopFactors: analysis9m.pca.featureImportance.slice(0, 5),
    sixMonthTopFactors: analysis6m.pca.featureImportance.slice(0, 5),
  }, null, 2));
}

run();
