#!/usr/bin/env node
const fs = require('node:fs');
const path = require('node:path');

const { PROJECT_ROOT, ensureDir, loadConfig } = require('../src/config');
const { parseArgs } = require('../src/cli');
const { readJsonl, writeJsonl } = require('../src/dataset');
const {
  filterFeatureColumns,
  predictBaseline,
  predictExamples,
  selectNumericFeatureColumns,
  trainExperiment,
} = require('../src/models');
const { compactMetrics, computePredictionMetrics } = require('../src/metrics');
const { splitRowsByConfig } = require('../src/splits');

const FEATURE_SETS = ['price_only', 'cross_asset', 'options_plus_spy', 'all_features'];

function defaultDatasetPath(config) {
  return path.join(
    PROJECT_ROOT,
    'runtime',
    `spy-intraday-dataset-${config.windows.train.startDate}-${config.dataPolicy.historicalCutoffDate}-with-option-features.jsonl`,
  );
}

function predictionRecord({ experiment, horizonName, featureSet, splitName, prediction }) {
  return {
    experiment,
    horizon: horizonName,
    featureSet,
    split: splitName,
    rowId: prediction.row.rowId,
    tradeDate: prediction.row.tradeDate,
    minuteUtc: prediction.row.minuteUtc,
    spyClose: prediction.row.spy_close,
    actualReturn: prediction.actualReturn,
    actualDirection: prediction.actualDirection,
    predictedDirection: prediction.predictedDirection,
    directionProbability: prediction.directionProbability,
    predictedReturn: prediction.predictedReturn,
    confidence: prediction.confidence,
  };
}

function rankResults(results) {
  return results.slice().sort((left, right) => {
    const accuracyDelta = (right.metrics.directionalAccuracy || 0) - (left.metrics.directionalAccuracy || 0);
    if (accuracyDelta !== 0) return accuracyDelta;
    return (right.metrics.balancedAccuracy || 0) - (left.metrics.balancedAccuracy || 0);
  });
}

function main() {
  const args = parseArgs();
  const config = loadConfig(args.config);
  const datasetPath = path.resolve(args.dataset || defaultDatasetPath(config));
  const stamp = new Date().toISOString().replace(/[:.]/g, '-');
  const summaryPath = path.resolve(args.output || path.join(PROJECT_ROOT, 'artifacts', `experiments-${stamp}.json`));
  const predictionsPath = path.resolve(args.predictions || path.join(PROJECT_ROOT, 'runtime', `predictions-${stamp}.jsonl`));

  const rows = readJsonl(datasetPath);
  const splits = splitRowsByConfig(rows, config);
  const allFeatureColumns = selectNumericFeatureColumns(splits.train);
  const allPredictions = [];
  const results = [];

  config.horizons.forEach((horizon) => {
    FEATURE_SETS.forEach((featureSet) => {
      const featureColumns = filterFeatureColumns(allFeatureColumns, featureSet);
      if (!featureColumns.length) return;
      const model = trainExperiment(splits.train, horizon.name, featureColumns);
      splits.tests.forEach(({ window, rows: testRows }) => {
        const predictions = predictExamples(model, testRows);
        const metrics = compactMetrics(computePredictionMetrics(predictions));
        const experiment = `${featureSet}_logistic_ridge`;
        predictions.forEach((prediction) => allPredictions.push(predictionRecord({
          experiment,
          horizonName: horizon.name,
          featureSet,
          splitName: window.name,
          prediction,
        })));
        results.push({
          experiment,
          horizon: horizon.name,
          featureSet,
          split: window.name,
          trainRows: model.trainRowCount,
          testRows: predictions.length,
          featureCount: featureColumns.length,
          metrics,
        });

        const baselinePredictions = predictBaseline(model, testRows);
        const baselineMetrics = compactMetrics(computePredictionMetrics(baselinePredictions));
        baselinePredictions.forEach((prediction) => allPredictions.push(predictionRecord({
          experiment: `${featureSet}_train_prior_baseline`,
          horizonName: horizon.name,
          featureSet,
          splitName: window.name,
          prediction,
        })));
        results.push({
          experiment: `${featureSet}_train_prior_baseline`,
          horizon: horizon.name,
          featureSet,
          split: window.name,
          trainRows: model.trainRowCount,
          testRows: baselinePredictions.length,
          featureCount: featureColumns.length,
          metrics: baselineMetrics,
        });
      });
    });
  });

  const rankedResults = rankResults(results);
  const summary = {
    generatedAt: new Date().toISOString(),
    datasetPath,
    predictionsPath,
    rowCount: rows.length,
    trainWindow: config.windows.train,
    testWindows: config.windows.tests,
    featureSets: FEATURE_SETS,
    featureColumnCount: allFeatureColumns.length,
    accuracyFirstRanking: rankedResults,
    formalReportingNote: `Formal April reporting stops at ${config.dataPolicy.historicalCutoffDate}; provisional intraday rows are excluded unless explicitly scored separately.`,
  };

  ensureDir(path.dirname(summaryPath));
  ensureDir(path.dirname(predictionsPath));
  fs.writeFileSync(summaryPath, `${JSON.stringify(summary, null, 2)}\n`, 'utf8');
  writeJsonl(predictionsPath, allPredictions);
  console.log(JSON.stringify({
    summaryPath,
    predictionsPath,
    resultCount: results.length,
    topResults: rankedResults.slice(0, 10),
  }, null, 2));
}

main();
