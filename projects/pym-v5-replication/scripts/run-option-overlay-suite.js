#!/usr/bin/env node
const fs = require('node:fs');

const { artifactPath, ensureDir, loadConfig, runtimePath } = require('../src/config');
const { latestDatasetDate, resolveEndDate } = require('../src/calendar');
const { latestOptionBarsDate } = require('../src/option-features');
const { runOptionOverlaySuite, selectStrategies } = require('../src/option-overlay-suite');

function parseArgs(argv) {
  const out = {};
  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    if (arg === '--start') out.startDate = argv[++index];
    else if (arg === '--end') out.endDate = argv[++index];
    else if (arg === '--daily-bars') out.dailyBarsPath = argv[++index];
    else if (arg === '--score') out.scorePath = argv[++index];
    else if (arg === '--option-features') out.optionFeaturesPath = argv[++index];
    else if (arg === '--cost-bps') out.costBps = Number(argv[++index]);
    else if (arg === '--rsi-mode') out.rsiMode = argv[++index];
    else if (arg === '--strategies') out.strategyIds = argv[++index].split(',').map((value) => value.trim()).filter(Boolean);
    else if (arg === '--label') out.label = argv[++index];
    else if (arg === '--output') out.outputPath = argv[++index];
  }
  return out;
}

function defaultOptionFeaturesPath(startDate, endDate) {
  return runtimePath(`pym-v5-option-bar-features-${startDate}-${endDate}.jsonl`);
}

function pct(value) {
  return Number.isFinite(value) ? `${value.toFixed(2)}%` : null;
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const config = loadConfig();
  const startDate = args.startDate || '2025-01-02';
  const requestedEnd = args.endDate || config.windows.endDate;
  const endDate = requestedEnd === 'auto'
    ? [
      resolveEndDate(config, requestedEnd),
      latestOptionBarsDate(config) || latestDatasetDate(config.roots.historical, config.datasets.optionBars || 'option_quotes_1m'),
    ].filter(Boolean).sort().at(0)
    : requestedEnd;
  const optionFeaturesPath = args.optionFeaturesPath || defaultOptionFeaturesPath(startDate, endDate);
  const strategies = selectStrategies(args.strategyIds);
  if (!fs.existsSync(optionFeaturesPath)) throw new Error(`missing_option_features:${optionFeaturesPath}`);

  const result = await runOptionOverlaySuite({
    config,
    startDate,
    endDate,
    costBps: Number.isFinite(args.costBps) ? args.costBps : undefined,
    dailyBarsPath: args.dailyBarsPath,
    scorePath: args.scorePath,
    optionFeaturesPath,
    rsiMode: args.rsiMode || 'wilder',
    strategyIds: strategies.map((strategy) => strategy.id),
  });

  const labelPart = args.label ? `${args.label}-` : '';
  const outputPath = args.outputPath || artifactPath(`pym-v5-option-overlay-suite-${labelPart}${startDate}-${endDate}.json`);
  ensureDir(artifactPath());
  fs.writeFileSync(outputPath, `${JSON.stringify(result, null, 2)}\n`, 'utf8');
  console.log(JSON.stringify({
    outputPath,
    startDate,
    endDate,
    costBps: result.settings.costBps,
    skippedDays: result.skippedDays.length,
    benchmarks: {
      spy: pct((result.benchmarks.spy || 0) * 100),
      qqq: pct((result.benchmarks.qqq || 0) * 100),
      bil: pct((result.benchmarks.bil || 0) * 100),
    },
    top: result.summaries.slice(0, 12).map((summary) => ({
      id: summary.id,
      family: summary.family,
      totalReturn: pct(summary.totalReturnPct),
      maxDrawdown: pct(summary.maxDrawdownPct),
      sharpe: Number(summary.sharpe.toFixed(3)),
      avgTurnover: Number(summary.averageDailyTurnover.toFixed(3)),
    })),
  }, null, 2));
}

main().catch((error) => {
  console.error(error.stack || error.message);
  process.exit(1);
});
