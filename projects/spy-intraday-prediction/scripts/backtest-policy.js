#!/usr/bin/env node
const fs = require('node:fs');
const path = require('node:path');

const { PROJECT_ROOT, ensureDir, loadConfig } = require('../src/config');
const { parseArgs, asNumber } = require('../src/cli');
const { computePolicyBacktest } = require('../src/backtest');
const { readJsonl } = require('../src/dataset');

function latestPredictionFile() {
  const runtimeDir = path.join(PROJECT_ROOT, 'runtime');
  if (!fs.existsSync(runtimeDir)) return null;
  const files = fs.readdirSync(runtimeDir)
    .filter((name) => name.startsWith('predictions-') && name.endsWith('.jsonl'))
    .map((name) => path.join(runtimeDir, name))
    .sort((left, right) => fs.statSync(right).mtimeMs - fs.statSync(left).mtimeMs);
  return files[0] || null;
}

function round(value, digits = 6) {
  return typeof value === 'number' && Number.isFinite(value) ? Number(value.toFixed(digits)) : value;
}

function compactBacktest(result) {
  return {
    observations: result.observations,
    inputObservations: result.inputObservations,
    executionPolicy: result.executionPolicy,
    confidenceThreshold: result.confidenceThreshold,
    transactionCostBps: result.transactionCostBps,
    slippageBps: result.slippageBps,
    finalEquity: round(result.finalEquity),
    totalReturn: round(result.totalReturn),
    buyAndHoldReturn: round(result.buyAndHoldReturn),
    excessReturn: round(result.excessReturn),
    maxDrawdown: round(result.maxDrawdown),
    turnover: round(result.turnover),
    averageTurnover: round(result.averageTurnover),
    longShare: round(result.longShare),
    shortShare: round(result.shortShare),
    cashShare: round(result.cashShare),
    notes: result.notes,
  };
}

function main() {
  const args = parseArgs();
  const config = loadConfig(args.config);
  const predictionsPath = path.resolve(args.predictions || latestPredictionFile() || '');
  if (!predictionsPath || !fs.existsSync(predictionsPath)) {
    throw new Error('predictions_required: run scripts/run-experiments.js or pass --predictions <jsonl>');
  }

  const horizon = args.horizon || 'next_1m';
  const experiment = args.experiment || null;
  const split = args.split || null;
  const outputPath = path.resolve(
    args.output || path.join(PROJECT_ROOT, 'artifacts', `policy-backtest-${horizon}-${Date.now()}.json`),
  );
  const rows = readJsonl(predictionsPath).filter((row) => (
    row.horizon === horizon
    && (!experiment || row.experiment === experiment)
    && (!split || row.split === split)
  ));
  const grouped = new Map();
  rows.forEach((row) => {
    const key = `${row.experiment}|${row.horizon}|${row.split}`;
    const list = grouped.get(key) || [];
    list.push(row);
    grouped.set(key, list);
  });

  const backtests = [...grouped.entries()].map(([key, predictions]) => {
    const [rowExperiment, rowHorizon, rowSplit] = key.split('|');
    const result = computePolicyBacktest(predictions, {
      confidenceThreshold: asNumber(args.threshold, config.execution.confidenceThreshold),
      transactionCostBps: asNumber(args['cost-bps'], config.execution.transactionCostBps),
      slippageBps: asNumber(args['slippage-bps'], config.execution.slippageBps),
      horizonName: rowHorizon,
    });
    return {
      experiment: rowExperiment,
      horizon: rowHorizon,
      split: rowSplit,
      ...compactBacktest(result),
    };
  }).sort((left, right) => right.totalReturn - left.totalReturn);

  const report = {
    generatedAt: new Date().toISOString(),
    predictionsPath,
    horizon,
    experiment,
    split,
    rankingBasis: 'secondary policy return after costs; use experiment accuracy ranking first',
    backtests,
  };
  ensureDir(path.dirname(outputPath));
  fs.writeFileSync(outputPath, `${JSON.stringify(report, null, 2)}\n`, 'utf8');
  console.log(JSON.stringify({ outputPath, backtestCount: backtests.length, top: backtests.slice(0, 10) }, null, 2));
}

main();
