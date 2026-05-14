#!/usr/bin/env node

const fs = require('node:fs');
const path = require('node:path');

const { createDefaultRegistry } = require('../src/default-registry');

const REPO_ROOT = path.resolve(__dirname, '..', '..', '..');
const DEFAULT_OUT_DIR = path.join(REPO_ROOT, 'artifacts', 'strategy-service');

function parseArgs(argv = process.argv.slice(2)) {
  const args = { outDir: DEFAULT_OUT_DIR };
  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    if (arg === '--out-dir') args.outDir = path.resolve(argv[++index]);
    else if (arg.startsWith('--out-dir=')) args.outDir = path.resolve(arg.slice('--out-dir='.length));
    else throw new Error(`unknown_arg:${arg}`);
  }
  return args;
}

function elapsedMs(startedAt) {
  const diff = process.hrtime.bigint() - startedAt;
  return Number(diff) / 1e6;
}

function round(value, digits = 2) {
  return Number.isFinite(value) ? Number(value.toFixed(digits)) : null;
}

function countHoldings(report) {
  return (report.snapshots || []).reduce((sum, snapshot) => sum + (snapshot.holdings || []).length, 0);
}

function profile() {
  const registryStartedAt = process.hrtime.bigint();
  const registry = createDefaultRegistry();
  const registryLoadMs = elapsedMs(registryStartedAt);
  const rows = [];
  for (const metadata of registry.listStrategies()) {
    const strategy = registry.getStrategy(metadata.id);
    const startedAt = process.hrtime.bigint();
    let report;
    let error = null;
    try {
      report = strategy.getReport();
    } catch (err) {
      error = err.message;
    }
    const loadMs = elapsedMs(startedAt);
    rows.push({
      id: metadata.id,
      name: metadata.displayName || metadata.name || metadata.id,
      family: metadata.family || null,
      cadence: metadata.cadence || null,
      loadMs: round(loadMs, 2),
      latestDate: report?.latestDailyResult?.date || report?.summary?.latestCompletedDate || report?.summary?.endDate || null,
      snapshots: report?.snapshots?.length || 0,
      dailyResults: report?.dailyResults?.length || 0,
      holdings: report ? countHoldings(report) : 0,
      trades: report?.normalizedTrades?.length || report?.trades?.length || report?.summary?.trades || report?.summary?.tradeCount || 0,
      artifactGeneratedAt: report?.generatedAt || null,
      error,
    });
  }
  rows.sort((left, right) => right.loadMs - left.loadMs);
  return {
    schemaVersion: 'phenixflow.strategyProfile.v1',
    generatedAt: new Date().toISOString(),
    registryLoadMs: round(registryLoadMs, 2),
    rows,
  };
}

function renderMarkdown(payload) {
  const lines = [
    '# Strategy Profile',
    '',
    `Generated: ${payload.generatedAt}`,
    `Registry load: ${payload.registryLoadMs} ms`,
    '',
    '| Rank | Strategy | Load ms | Latest | Daily rows | Holdings | Trades | Cadence |',
    '|---:|---|---:|---:|---:|---:|---:|---|',
  ];
  payload.rows.forEach((row, index) => {
    lines.push(`| ${index + 1} | ${row.name} | ${row.loadMs?.toFixed(2) || ''} | ${row.latestDate || ''} | ${row.dailyResults} | ${row.holdings} | ${row.trades} | ${row.cadence || ''} |`);
  });
  return `${lines.join('\n')}\n`;
}

function main() {
  const args = parseArgs();
  const payload = profile();
  fs.mkdirSync(args.outDir, { recursive: true });
  const stamp = payload.generatedAt.replace(/[:.]/g, '-');
  const jsonPath = path.join(args.outDir, `strategy-profile-${stamp}.json`);
  const mdPath = path.join(args.outDir, `strategy-profile-${stamp}.md`);
  fs.writeFileSync(jsonPath, `${JSON.stringify(payload, null, 2)}\n`);
  fs.writeFileSync(mdPath, renderMarkdown(payload));
  console.log(JSON.stringify({
    jsonPath,
    mdPath,
    registryLoadMs: payload.registryLoadMs,
    slowest: payload.rows.slice(0, 8).map((row) => ({
      id: row.id,
      loadMs: row.loadMs,
      dailyResults: row.dailyResults,
      trades: row.trades,
    })),
  }, null, 2));
}

try {
  main();
} catch (error) {
  console.error(error.stack || error.message);
  process.exit(1);
}
