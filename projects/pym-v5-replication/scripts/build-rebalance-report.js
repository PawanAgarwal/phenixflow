#!/usr/bin/env node
const fs = require('node:fs');

const { loadConfig, ensureDir } = require('../src/config');
const { readDailyBarsJsonl } = require('../src/backtest');
const { loadMassiveEnv } = require('../src/env');
const {
  buildDailyRebalanceReport,
  defaultReportPath,
  defaultScorePath,
  findLatestMassiveEodBarsPath,
} = require('../src/rebalance-report');

function parseArgs(argv) {
  const out = {};
  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    if (arg === '--start') out.startDate = argv[++index];
    else if (arg === '--daily-bars') out.dailyBarsPath = argv[++index];
    else if (arg === '--score') out.scorePath = argv[++index];
    else if (arg === '--out') out.outPath = argv[++index];
    else if (arg === '--rsi-mode') out.rsiMode = argv[++index];
  }
  return out;
}

function main() {
  loadMassiveEnv();
  const args = parseArgs(process.argv.slice(2));
  const config = loadConfig();
  const scorePath = args.scorePath || defaultScorePath(config);
  const barsPath = args.dailyBarsPath || findLatestMassiveEodBarsPath();
  if (!fs.existsSync(scorePath)) throw new Error(`Missing Composer score snapshot: ${scorePath}`);
  if (!barsPath || !fs.existsSync(barsPath)) throw new Error('Missing Massive adjusted EOD bars. Run npm run pym-v5:massive-eod-build first.');

  const score = JSON.parse(fs.readFileSync(scorePath, 'utf8'));
  const market = readDailyBarsJsonl(barsPath);
  const report = buildDailyRebalanceReport({
    market,
    score,
    startDate: args.startDate || '2025-01-01',
    rsiMode: args.rsiMode || 'wilder',
    initialCapital: config.execution.initialCapital,
    transactionCostBps: config.execution.transactionCostBps,
    slippageBps: config.execution.slippageBps,
    source: {
      scorePath,
      barsPath,
      provider: 'Massive adjusted EOD',
      composerSymphonyId: config.source.composerSymphonyId,
    },
  });

  const outPath = args.outPath || defaultReportPath();
  ensureDir(require('node:path').dirname(outPath));
  fs.writeFileSync(outPath, JSON.stringify(report, null, 2));
  console.log(JSON.stringify({
    latestRebalanceDate: report.summary.latestRebalanceDate,
    latestCompletedDate: report.summary.latestCompletedDate,
    snapshots: report.summary.snapshots,
    totalReturnPct: report.summary.totalReturnPct?.toFixed(2),
    outputPath: outPath,
  }, null, 2));
}

main();
