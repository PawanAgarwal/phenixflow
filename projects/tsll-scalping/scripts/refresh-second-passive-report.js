#!/usr/bin/env node
const fs = require('node:fs');
const path = require('node:path');
const { spawnSync } = require('node:child_process');

const { PROJECT_ROOT, ensureDir, loadConfig } = require('../src/config');
const { availableDates } = require('../src/calendar');

const REPO_ROOT = path.resolve(PROJECT_ROOT, '..', '..');
const DEFAULT_START = '2025-01-02';

function parseArgs(argv = process.argv.slice(2)) {
  const out = {};
  for (let index = 0; index < argv.length; index += 1) {
    const token = argv[index];
    if (!token.startsWith('--')) continue;
    const key = token.slice(2);
    const next = argv[index + 1];
    if (!next || next.startsWith('--')) {
      out[key] = true;
      continue;
    }
    out[key] = next;
    index += 1;
  }
  return out;
}

function latestDatasetDate(root, datasetId) {
  const datasetRoot = path.join(root, datasetId);
  if (!fs.existsSync(datasetRoot)) return null;
  const dates = fs.readdirSync(datasetRoot)
    .filter((entry) => entry.startsWith('date='))
    .map((entry) => entry.slice('date='.length))
    .sort();
  return dates.at(-1) || null;
}

function resolveEndDate(config, requested) {
  if (requested && requested !== 'auto') return requested;
  const datasetId = config.datasets.stockBars;
  const dates = [config.roots.historical, config.roots.liveParquet]
    .filter(Boolean)
    .map((root) => latestDatasetDate(root, datasetId))
    .filter(Boolean)
    .sort();
  const latest = dates.at(-1);
  if (!latest) throw new Error('No Massive stock bar dates were found for TSLL refresh.');
  return latest;
}

function run(label, args) {
  console.log(JSON.stringify({ step: label, command: process.execPath, args }));
  const startedAt = Date.now();
  const result = spawnSync(process.execPath, args, {
    cwd: REPO_ROOT,
    env: process.env,
    stdio: 'inherit',
  });
  if (result.status !== 0) throw new Error(`step_failed:${label}:exit_${result.status}`);
  console.log(JSON.stringify({ step: label, status: 'ok', elapsedMs: Date.now() - startedAt }));
}

function round(value, digits = 4) {
  return Number.isFinite(value) ? Number(value.toFixed(digits)) : value;
}

function summarize(days, trades) {
  const totals = days.reduce((acc, day) => {
    acc.trades += day.trades || 0;
    acc.netCents += day.netCents || 0;
    acc.pnlPer1000Shares += day.pnlPer1000Shares || 0;
    acc.buyCapitalPer1000Shares += day.buyCapitalPer1000Shares || 0;
    acc.avgEntryNumerator += (day.avgEntry || 0) * (day.trades || 0);
    acc.winningDays += (day.netCents || 0) > 0 ? 1 : 0;
    return acc;
  }, {
    trades: 0,
    netCents: 0,
    pnlPer1000Shares: 0,
    buyCapitalPer1000Shares: 0,
    avgEntryNumerator: 0,
    winningDays: 0,
  });
  const wins = trades.filter((trade) => trade.netCents > 0).length;
  const avgEntry = totals.trades ? totals.avgEntryNumerator / totals.trades : 0;
  const pnlDollarsPerShareUnit = totals.netCents / 100;
  return {
    days: days.length,
    winningDays: totals.winningDays,
    trades: totals.trades,
    winRate: totals.trades ? round(wins / totals.trades, 6) : 0,
    netCents: round(totals.netCents, 4),
    avgNetCents: totals.trades ? round(totals.netCents / totals.trades, 6) : 0,
    pnlPer1000Shares: round(totals.pnlPer1000Shares, 2),
    buyCapitalPer1000Shares: round(totals.buyCapitalPer1000Shares, 2),
    avgEntry: round(avgEntry, 6),
    returnOnBuyTurnover: totals.buyCapitalPer1000Shares ? round(totals.pnlPer1000Shares / totals.buyCapitalPer1000Shares, 8) : 0,
    returnOnRecycledCapital: avgEntry ? round(pnlDollarsPerShareUnit / avgEntry, 8) : 0,
  };
}

function findLatestPriorReport(startDate, endDate) {
  const dir = path.join(REPO_ROOT, 'projects', 'tsll-scalping', 'reports');
  if (!fs.existsSync(dir)) return null;
  return fs.readdirSync(dir)
    .map((name) => {
      const match = name.match(/^tsll-seconds-passive-fixed-(\d{4}-\d{2}-\d{2})-(\d{4}-\d{2}-\d{2})\.json$/);
      if (!match || match[1] !== startDate || match[2] >= endDate) return null;
      return { name, startDate: match[1], endDate: match[2], path: path.join(dir, name) };
    })
    .filter(Boolean)
    .sort((left, right) => right.endDate.localeCompare(left.endDate))
    .at(0) || null;
}

function tradeKey(trade) {
  return [
    trade.tradeDate,
    trade.signalTsUtc,
    trade.entryTsUtc,
    trade.exitTsUtc,
    trade.entryPrice,
    trade.exitPrice,
    trade.netCents,
  ].join('|');
}

function mergeReports({ previousPath, appendPath, outputPath, startDate, endDate }) {
  const previous = JSON.parse(fs.readFileSync(previousPath, 'utf8'));
  const appended = JSON.parse(fs.readFileSync(appendPath, 'utf8'));
  const daysByDate = new Map();
  [...(previous.days || []), ...(appended.days || [])].forEach((day) => daysByDate.set(day.date, day));
  const tradesByKey = new Map();
  [...(previous.trades || []), ...(appended.trades || [])].forEach((trade) => tradesByKey.set(tradeKey(trade), trade));
  const days = [...daysByDate.values()].sort((left, right) => left.date.localeCompare(right.date));
  const trades = [...tradesByKey.values()].sort((left, right) => (
    String(left.tradeDate).localeCompare(String(right.tradeDate))
    || String(left.entryTsUtc || '').localeCompare(String(right.entryTsUtc || ''))
    || String(left.exitTsUtc || '').localeCompare(String(right.exitTsUtc || ''))
  ));
  const merged = {
    ...previous,
    generatedAt: new Date().toISOString(),
    startDate,
    endDate,
    sourceArtifact: [previous.sourceArtifact, appended.sourceArtifact].filter(Boolean).join(','),
    totals: summarize(days, trades),
    days,
    trades,
  };
  ensureDir(path.dirname(outputPath));
  fs.writeFileSync(outputPath, `${JSON.stringify(merged, null, 2)}\n`);
  return merged;
}

function main() {
  const args = parseArgs();
  const config = loadConfig(args.config);
  const startDate = args.start || args['start-date'] || DEFAULT_START;
  const endDate = resolveEndDate(config, args.end || args['end-date'] || 'auto');
  const artifactBase = path.join(
    'projects',
    'tsll-scalping',
    'artifacts',
    `tsll-seconds-passive-mm-fixed-${startDate}-${endDate}-cost0`,
  );
  const source = `${artifactBase}.json`;
  const output = path.join(
    'projects',
    'tsll-scalping',
    'reports',
    `tsll-seconds-passive-fixed-${startDate}-${endDate}.json`,
  );

  if (args.force !== true && fs.existsSync(path.join(REPO_ROOT, output))) {
    console.log(JSON.stringify({
      status: 'current',
      startDate,
      endDate,
      output,
      reason: 'existing_report_covers_resolved_end_date',
    }, null, 2));
    return;
  }

  const prior = args.force === true ? null : findLatestPriorReport(startDate, endDate);
  if (prior) {
    const appendDates = availableDates(config, prior.endDate, endDate, ['stockBars'])
      .filter((date) => date > prior.endDate && date <= endDate);
    if (appendDates.length) {
      const appendStart = appendDates[0];
      const appendEnd = appendDates.at(-1);
      const appendBase = path.join(
        'projects',
        'tsll-scalping',
        'artifacts',
        `tsll-seconds-passive-mm-fixed-${appendStart}-${appendEnd}-cost0`,
      );
      const appendSource = `${appendBase}.json`;
      const appendReport = path.join(
        'projects',
        'tsll-scalping',
        'runtime',
        `tsll-seconds-passive-fixed-${appendStart}-${appendEnd}.json`,
      );
      run('run_second_passive_rest_incremental', [
        'projects/tsll-scalping/scripts/run-second-passive-mm.js',
        '--start-date', appendStart,
        '--end-date', appendEnd,
        '--fixed-candidate',
        '--no-daily-context',
        '--rest-seconds',
        '--cost-cents-per-side', '0',
        '--min-trades', '0',
        '--output', appendBase,
      ]);
      run('export_second_passive_report_incremental', [
        'projects/tsll-scalping/scripts/export-second-passive-report.js',
        '--source', appendSource,
        '--output', appendReport,
      ]);
      const merged = mergeReports({
        previousPath: prior.path,
        appendPath: path.join(REPO_ROOT, appendReport),
        outputPath: path.join(REPO_ROOT, output),
        startDate,
        endDate,
      });
      console.log(JSON.stringify({
        status: 'ok',
        mode: 'incremental',
        startDate,
        endDate,
        prior: path.relative(REPO_ROOT, prior.path),
        appendStart,
        appendEnd,
        output,
        days: merged.totals.days,
        trades: merged.totals.trades,
      }, null, 2));
      return;
    }
  }

  run('run_second_passive_rest', [
    'projects/tsll-scalping/scripts/run-second-passive-mm.js',
    '--start-date', startDate,
    '--end-date', endDate,
    '--fixed-candidate',
    '--no-daily-context',
    '--rest-seconds',
    '--cost-cents-per-side', '0',
    '--min-trades', '0',
    '--output', artifactBase,
  ]);
  run('export_second_passive_report', [
    'projects/tsll-scalping/scripts/export-second-passive-report.js',
    '--source', source,
    '--output', output,
  ]);

  console.log(JSON.stringify({ status: 'ok', startDate, endDate, source, output }, null, 2));
}

try {
  main();
} catch (error) {
  console.error(error.stack || error.message);
  process.exit(1);
}
