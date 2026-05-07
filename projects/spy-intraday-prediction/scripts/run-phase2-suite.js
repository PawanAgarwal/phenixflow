#!/usr/bin/env node
const fs = require('node:fs');
const path = require('node:path');

const { PROJECT_ROOT, ensureDir, loadConfig } = require('../src/config');
const { parseArgs } = require('../src/cli');
const { readJsonlStreaming, writeJsonl } = require('../src/dataset');
const { PHASE2_STRATEGIES, runPhase2Suite } = require('../src/phase2-research');

function defaultDatasetPath(config) {
  return path.join(
    PROJECT_ROOT,
    'runtime',
    `spy-intraday-dataset-${config.windows.sensitivityTrain?.startDate || config.windows.train.startDate}-${config.dataPolicy.historicalCutoffDate}-with-option-features.jsonl`,
  );
}

function rankResults(results) {
  return results.slice().sort((left, right) => {
    const leftPolicyLift = (left.backtest?.totalReturn || 0) - (left.ungatedBacktest?.totalReturn || 0);
    const rightPolicyLift = (right.backtest?.totalReturn || 0) - (right.ungatedBacktest?.totalReturn || 0);
    if (rightPolicyLift !== leftPolicyLift) return rightPolicyLift - leftPolicyLift;
    return (right.backtest?.totalReturn || 0) - (left.backtest?.totalReturn || 0);
  });
}

function compactConsoleResult(result) {
  return {
    trainMode: result.trainMode,
    phase2Strategy: result.phase2Strategy,
    baseStrategy: result.baseStrategy,
    horizon: result.horizon,
    split: result.split,
    selectedThreshold: result.selectedPolicy?.selectedThreshold,
    acceptedShare: result.acceptedShare,
    balancedAccuracy: result.metrics?.balancedAccuracy,
    ungatedBalancedAccuracy: result.ungatedMetrics?.balancedAccuracy,
    totalReturn: result.backtest?.totalReturn,
    ungatedReturn: result.ungatedBacktest?.totalReturn,
    buyAndHoldReturn: result.backtest?.buyAndHoldReturn,
    maxDrawdown: result.backtest?.maxDrawdown,
  };
}

async function main() {
  const args = parseArgs();
  const config = loadConfig(args.config);
  const datasetPath = path.resolve(args.dataset || defaultDatasetPath(config));
  const stamp = new Date().toISOString().replace(/[:.]/g, '-');
  const summaryPath = path.resolve(args.output || path.join(PROJECT_ROOT, 'artifacts', `phase2-suite-${stamp}.json`));
  const predictionsPath = path.resolve(args.predictions || path.join(PROJECT_ROOT, 'runtime', `phase2-predictions-${stamp}.jsonl`));
  const requestedStrategies = args.strategy
    ? String(args.strategy).split(',').map((value) => value.trim()).filter(Boolean)
    : PHASE2_STRATEGIES;
  const unknown = requestedStrategies.filter((strategy) => !PHASE2_STRATEGIES.includes(strategy));
  if (unknown.length) throw new Error(`unknown_phase2_strategy:${unknown.join(',')}`);

  const rows = await readJsonlStreaming(datasetPath);
  const suite = runPhase2Suite(rows, config, requestedStrategies);
  const rankedResults = rankResults(suite.results);
  const summary = {
    generatedAt: new Date().toISOString(),
    datasetPath,
    predictionsPath,
    rowCount: rows.length,
    requestedStrategies,
    rankingBasis: 'policy improvement versus ungated strategy first, then net policy return',
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
    topResults: rankedResults.slice(0, 10).map(compactConsoleResult),
  }, null, 2));
}

if (require.main === module) {
  main().catch((error) => {
    console.error(error.stack || error.message);
    process.exitCode = 1;
  });
}

module.exports = { main };
