#!/usr/bin/env node
const fs = require('node:fs');
const path = require('node:path');

const { ensureDir, loadConfig, runtimePath, artifactPath } = require('../src/config');
const { defaultScorePath, findLatestMassiveEodBarsPath } = require('../src/rebalance-report');
const { runExtensionStrategiesSuite } = require('../src/extension-strategies-suite');

function parseArgs(argv) {
  const out = {};
  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    if (arg === '--start') out.startDate = argv[++i];
    else if (arg === '--end') out.endDate = argv[++i];
    else if (arg === '--cost-bps') out.costBps = Number(argv[++i]);
    else if (arg === '--label') out.label = argv[++i];
    else if (arg === '--primary-bars') out.primaryDailyBarsPath = argv[++i];
    else if (arg === '--extra-bars') out.extraDailyBarsPath = argv[++i];
    else if (arg === '--score') out.scorePath = argv[++i];
    else if (arg === '--out') out.outPath = argv[++i];
  }
  return out;
}

function findLatestExtraBarsPath() {
  const root = runtimePath();
  if (!fs.existsSync(root)) return null;
  const matches = fs.readdirSync(root)
    .map((name) => {
      const match = name.match(/^pym-v5-extra-eod-daily-bars-(\d{4}-\d{2}-\d{2})-(\d{4}-\d{2}-\d{2})\.jsonl$/);
      return match ? { name, startDate: match[1], endDate: match[2] } : null;
    })
    .filter(Boolean)
    .sort((a, b) => a.endDate.localeCompare(b.endDate) || a.startDate.localeCompare(b.startDate));
  return matches.length ? path.join(root, matches.at(-1).name) : null;
}

function fmt(value, digits = 2) {
  if (!Number.isFinite(value)) return '—';
  return value.toFixed(digits);
}

function compactTable(summaries) {
  return summaries.map((row) => ({
    id: row.id,
    family: row.family,
    returnPct: Number(fmt(row.totalReturnPct)),
    cagrPct: Number(fmt(row.cagrPct)),
    maxDdPct: Number(fmt(row.maxDrawdownPct)),
    sharpe: Number(fmt(row.sharpe, 3)),
    volPct: Number(fmt(row.annualizedVolatilityPct)),
    avgTurnoverPct: Number(fmt(row.averageDailyTurnoverPct)),
    days: row.tradingDays,
    winRatePct: Number(fmt(row.winRatePct, 1)),
  }));
}

function main() {
  const args = parseArgs(process.argv.slice(2));
  const config = loadConfig();
  const primaryDailyBarsPath = args.primaryDailyBarsPath || findLatestMassiveEodBarsPath();
  const extraDailyBarsPath = args.extraDailyBarsPath || findLatestExtraBarsPath();
  const scorePath = args.scorePath || defaultScorePath(config);
  const startDate = args.startDate || '2025-01-02';
  const endDate = args.endDate || null;
  const costBps = Number.isFinite(args.costBps) ? args.costBps : 2;
  const label = args.label || `${startDate}-${endDate || 'auto'}`;
  const report = runExtensionStrategiesSuite({
    primaryDailyBarsPath,
    extraDailyBarsPath,
    scorePath,
    startDate,
    endDate,
    costBps,
  });
  const outPath = args.outPath || artifactPath(`pym-v5-extension-strategies-${label}.json`);
  ensureDir(path.dirname(outPath));
  fs.writeFileSync(outPath, JSON.stringify(report, null, 2));
  console.log(JSON.stringify({
    outputPath: outPath,
    primaryDailyBarsPath,
    extraDailyBarsPath,
    scorePath,
    startDate,
    endDate: report.settings.endDate,
    costBps,
    benchmarks: report.benchmarks,
    rankedBySharpe: compactTable(report.summaries),
    rankedByReturn: compactTable(report.summariesByReturn),
  }, null, 2));
}

main();
