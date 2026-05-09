#!/usr/bin/env node
const fs = require('node:fs');

const { loadConfig, artifactPath, ensureDir } = require('../src/config');
const { resolveEndDate } = require('../src/calendar');
const { runIntradaySuite } = require('../src/intraday-suite');
const { STRATEGIES } = require('../src/intraday-strategies');

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
    else if (arg === '--label') out.label = argv[++index];
  }
  return out;
}

function selectStrategies(strategyIds) {
  if (!strategyIds?.length || strategyIds.includes('all')) return STRATEGIES;
  const selected = STRATEGIES.filter((strategy) => strategyIds.includes(strategy.id));
  const missing = strategyIds.filter((id) => !STRATEGIES.some((strategy) => strategy.id === id));
  if (missing.length) throw new Error(`unknown_strategies:${missing.join(',')}`);
  return selected;
}

function fmt(value) {
  return Number.isFinite(value) ? `${value.toFixed(2)}%` : null;
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const config = loadConfig();
  const startDate = args.startDate || '2025-01-02';
  const endDate = resolveEndDate(config, args.endDate || config.windows.endDate);
  const strategies = selectStrategies(args.strategyIds);
  const result = await runIntradaySuite({
    config,
    startDate,
    endDate,
    costBps: Number.isFinite(args.costBps) ? args.costBps : 4,
    dailyBarsPath: args.dailyBarsPath,
    scorePath: args.scorePath,
    rsiMode: args.rsiMode || 'wilder',
    strategies,
    onProgress: ({ day, processedDays }) => {
      if (processedDays % 20 === 0) console.log(`processed ${processedDays} intraday days through ${day.date}`);
    },
  });
  const labelPart = args.label ? `${args.label}-` : '';
  const outPath = artifactPath(`pym-v5-intraday-suite-${labelPart}${startDate}-${endDate}.json`);
  ensureDir(artifactPath());
  fs.writeFileSync(outPath, JSON.stringify(result, null, 2));
  console.log(JSON.stringify({
    startDate,
    endDate,
    costBps: result.settings.costBps,
    skippedDays: result.skippedDays.length,
    top: result.summaries.slice(0, 8).map((summary) => ({
      id: summary.id,
      totalReturn: fmt(summary.totalReturnPct),
      maxDrawdown: fmt(summary.maxDrawdownPct),
      sharpe: Number(summary.sharpe.toFixed(3)),
      tradesPerDay: Number(summary.averageTradesPerDay.toFixed(2)),
      turnoverPerDay: Number(summary.averageDailyTurnover.toFixed(3)),
    })),
    outputPath: outPath,
  }, null, 2));
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
