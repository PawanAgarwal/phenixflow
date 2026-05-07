#!/usr/bin/env node
const fs = require('node:fs');

const { loadConfig, ensureDir, runtimePath, artifactPath } = require('../src/config');
const { resolveEndDate } = require('../src/calendar');
const { readDailyBarsJsonl, runBacktest } = require('../src/backtest');
const { evaluateSymphony } = require('../src/symphony');

function parseArgs(argv) {
  const out = {};
  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    if (arg === '--start') out.startDate = argv[++index];
    else if (arg === '--build-start') out.buildStartDate = argv[++index];
    else if (arg === '--end') out.endDate = argv[++index];
    else if (arg === '--timing') out.timing = argv[++index];
    else if (arg === '--daily-bars') out.dailyBarsPath = argv[++index];
    else if (arg === '--score') out.scorePath = argv[++index];
    else if (arg === '--label') out.label = argv[++index];
    else if (arg === '--provider') out.provider = argv[++index];
    else if (arg === '--equal-weight-mode') out.equalWeightMode = argv[++index];
    else if (arg === '--rsi-mode') out.rsiMode = argv[++index];
  }
  return out;
}

function defaultScorePath(config) {
  return runtimePath('source', `composer-${config.source.composerSymphonyId}-score.json`);
}

function defaultBarsPath(config, buildStartDate, endDate) {
  return runtimePath(`pym-v5-daily-bars-${buildStartDate}-${endDate}.jsonl`);
}

function pct(value) {
  return value === null ? null : `${(value * 100).toFixed(2)}%`;
}

function main() {
  const args = parseArgs(process.argv.slice(2));
  const config = loadConfig();
  const buildStartDate = args.buildStartDate || config.windows.buildStartDate;
  const startDate = args.startDate || config.windows.backtestStartDate;
  const endDate = resolveEndDate(config, args.endDate || config.windows.endDate);
  const timing = args.timing || config.execution.timing;
  const scorePath = args.scorePath || defaultScorePath(config);
  const barsPath = args.dailyBarsPath || defaultBarsPath(config, buildStartDate, endDate);
  if (!fs.existsSync(scorePath)) throw new Error(`Missing Composer score snapshot: ${scorePath}`);
  if (!fs.existsSync(barsPath)) throw new Error(`Missing daily bars: ${barsPath}. Run npm run pym-v5:build-daily first.`);

  const score = JSON.parse(fs.readFileSync(scorePath, 'utf8'));
  const market = readDailyBarsJsonl(barsPath);
  const result = runBacktest({
    market,
    score,
    evaluateSymphony: (tree, data, signalIndex) => evaluateSymphony(tree, data, signalIndex, {
      equalWeightMode: args.equalWeightMode,
      rsiMode: args.rsiMode,
    }),
    startDate,
    timing,
    transactionCostBps: config.execution.transactionCostBps,
    slippageBps: config.execution.slippageBps,
    initialCapital: config.execution.initialCapital,
  });
  result.source = {
    scorePath,
    barsPath,
    localMarketDataProvider: args.provider || 'Massive',
    composerSymphonyId: config.source.composerSymphonyId,
    equalWeightMode: args.equalWeightMode || 'composer_tree_default_equal',
    rsiMode: args.rsiMode || 'simple',
    timingNote: timing === 'same_close'
      ? 'Diagnostic only: same-close timing uses same-day close-derived signals and is not causal.'
      : 'Causal: prior completed close signal, next close-to-close return.',
  };

  const labelPart = args.label ? `${args.label}-` : '';
  const outPath = artifactPath(`pym-v5-backtest-${labelPart}${timing}-${startDate}-${endDate}.json`);
  ensureDir(artifactPath());
  fs.writeFileSync(outPath, JSON.stringify(result, null, 2));

  console.log({
    timing,
    startDate,
    endDate: result.summary.endDate,
    tradingDays: result.summary.tradingDays,
    finalEquity: Number(result.summary.finalEquity.toFixed(2)),
    totalReturn: pct(result.summary.totalReturn),
    cagr: pct(result.summary.cagr),
    maxDrawdown: pct(result.summary.maxDrawdown),
    sharpe: Number(result.summary.sharpe.toFixed(3)),
    spyBuyHoldReturn: pct(result.summary.spyBuyHoldReturn),
    missingReturnEventCount: result.summary.missingReturnEventCount,
  });
  console.log(`wrote ${outPath}`);
}

main();
