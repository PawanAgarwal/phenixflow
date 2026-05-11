#!/usr/bin/env node
const fs = require('node:fs');

const { resolveEndDate } = require('../src/calendar');
const { artifactPath, ensureDir, loadConfig } = require('../src/config');
const {
  DYNAMIC_INTRADAY_STRATEGIES,
  runDynamicIntradayTiming,
} = require('../src/dynamic-intraday-timing');

function parseArgs(argv) {
  const out = {};
  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    if (arg === '--start') out.startDate = argv[++index];
    else if (arg === '--end') out.endDate = argv[++index];
    else if (arg === '--cost-bps') out.costBps = Number(argv[++index]);
    else if (arg === '--daily-bars') out.dailyBarsPath = argv[++index];
    else if (arg === '--score') out.scorePath = argv[++index];
    else if (arg === '--rsi-mode') out.rsiMode = argv[++index];
    else if (arg === '--strategies') out.strategyIds = argv[++index].split(',').map((value) => value.trim()).filter(Boolean);
    else if (arg === '--option-features') out.optionFeaturesPath = argv[++index];
    else if (arg === '--label') out.label = argv[++index];
  }
  return out;
}

function selectStrategies(strategyIds) {
  if (!strategyIds?.length || strategyIds.includes('all')) return DYNAMIC_INTRADAY_STRATEGIES;
  const selected = DYNAMIC_INTRADAY_STRATEGIES.filter((strategy) => strategyIds.includes(strategy.id));
  const missing = strategyIds.filter((id) => !DYNAMIC_INTRADAY_STRATEGIES.some((strategy) => strategy.id === id));
  if (missing.length) throw new Error(`unknown_dynamic_intraday_strategies:${missing.join(',')}`);
  return selected;
}

function pct(value) {
  return Number.isFinite(value) ? `${value.toFixed(2)}%` : null;
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const config = loadConfig();
  const startDate = args.startDate || '2025-01-02';
  const endDate = resolveEndDate(config, args.endDate || config.windows.endDate);
  const result = await runDynamicIntradayTiming({
    config,
    startDate,
    endDate,
    costBps: Number.isFinite(args.costBps) ? args.costBps : 2,
    dailyBarsPath: args.dailyBarsPath,
    scorePath: args.scorePath,
    rsiMode: args.rsiMode || 'wilder',
    optionFeaturesPath: args.optionFeaturesPath,
    strategies: selectStrategies(args.strategyIds),
    onProgress: ({ day, processedDays }) => {
      if (processedDays % 20 === 0) console.log(`processed ${processedDays} dynamic timing days through ${day.date}`);
    },
  });
  const labelPart = args.label ? `${args.label}-` : '';
  const outPath = artifactPath(`pym-v5-dynamic-intraday-timing-${labelPart}${startDate}-${endDate}.json`);
  ensureDir(artifactPath());
  fs.writeFileSync(outPath, `${JSON.stringify(result, null, 2)}\n`, 'utf8');
  console.log(JSON.stringify({
    startDate,
    endDate,
    costBps: result.settings.costBps,
    skippedDays: result.skippedDays.length,
    optionFeaturesPath: result.settings.optionFeaturesPath,
    benchmarks: {
      window: result.benchmarks.window,
      spyEntryToExit: pct(result.benchmarks.spyEntryToExitReturn * 100),
      qqqEntryToExit: pct(result.benchmarks.qqqEntryToExitReturn * 100),
    },
    top: result.summaries.map((summary) => ({
      id: summary.id,
      totalReturn: pct(summary.totalReturnPct),
      maxDrawdown: pct(summary.maxDrawdownPct),
      sharpe: Number(summary.sharpe.toFixed(3)),
      activeShare: pct(summary.activeShare * 100),
      avgTurnover: Number(summary.averageDailyTurnover.toFixed(3)),
      tradesPerDay: Number(summary.averageTradesPerDay.toFixed(2)),
      optionRiskSkips: summary.skippedByOptionRisk,
    })),
    outputPath: outPath,
  }, null, 2));
}

main().catch((error) => {
  console.error(error.stack || error.message);
  process.exit(1);
});
