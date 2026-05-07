#!/usr/bin/env node
const fs = require('node:fs');
const path = require('node:path');

const { PROJECT_ROOT, ensureDir, loadConfig } = require('../src/config');
const { parseArgs } = require('../src/cli');
const { readJsonlStreaming, writeJsonl } = require('../src/dataset');
const { STRATEGIES, runResearchSuite } = require('../src/research-strategies');

function defaultDatasetPath(config) {
  return path.join(
    PROJECT_ROOT,
    'runtime',
    `spy-intraday-dataset-${config.windows.sensitivityTrain?.startDate || config.windows.train.startDate}-${config.dataPolicy.historicalCutoffDate}-with-option-features.jsonl`,
  );
}

function rankResults(results) {
  return results.slice().sort((left, right) => {
    const leftLift = (left.metrics.balancedAccuracy || 0) - (left.baselineMetrics.balancedAccuracy || 0);
    const rightLift = (right.metrics.balancedAccuracy || 0) - (right.baselineMetrics.balancedAccuracy || 0);
    if (rightLift !== leftLift) return rightLift - leftLift;
    return (right.backtest?.totalReturn || 0) - (left.backtest?.totalReturn || 0);
  });
}

async function main() {
  const args = parseArgs();
  const config = loadConfig(args.config);
  const datasetPath = path.resolve(args.dataset || defaultDatasetPath(config));
  const stamp = new Date().toISOString().replace(/[:.]/g, '-');
  const summaryPath = path.resolve(args.output || path.join(PROJECT_ROOT, 'artifacts', `research-suite-${stamp}.json`));
  const predictionsPath = path.resolve(args.predictions || path.join(PROJECT_ROOT, 'runtime', `research-predictions-${stamp}.jsonl`));
  const requestedStrategies = args.strategy
    ? String(args.strategy).split(',').map((value) => value.trim()).filter(Boolean)
    : STRATEGIES;
  const unknown = requestedStrategies.filter((strategy) => !STRATEGIES.includes(strategy));
  if (unknown.length) throw new Error(`unknown_strategy:${unknown.join(',')}`);

  const rows = await readJsonlStreaming(datasetPath);
  const suite = runResearchSuite(rows, config, requestedStrategies);
  const rankedResults = rankResults(suite.results);
  const summary = {
    generatedAt: new Date().toISOString(),
    datasetPath,
    predictionsPath,
    rowCount: rows.length,
    requestedStrategies,
    trainModes: [
      config.research?.officialTrainName || 'jan_only',
      config.research?.sensitivityTrainName || 'history_plus_jan',
    ],
    rankingBasis: 'balanced accuracy lift over baseline first, SPY policy return second',
    results: rankedResults,
    promising: suite.promising,
    formalReportingNote: `Formal April reporting stops at ${config.dataPolicy.historicalCutoffDate}; provisional rows are excluded.`,
  };

  ensureDir(path.dirname(summaryPath));
  ensureDir(path.dirname(predictionsPath));
  fs.writeFileSync(summaryPath, `${JSON.stringify(summary, null, 2)}\n`, 'utf8');
  writeJsonl(predictionsPath, suite.predictions);
  console.log(JSON.stringify({
    summaryPath,
    predictionsPath,
    resultCount: suite.results.length,
    promisingCount: suite.promising.length,
    topResults: rankedResults.slice(0, 10),
  }, null, 2));
}

if (require.main === module) {
  main().catch((error) => {
    console.error(error.stack || error.message);
    process.exitCode = 1;
  });
}

module.exports = { main };
