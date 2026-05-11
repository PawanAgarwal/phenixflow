#!/usr/bin/env node
const fs = require('node:fs');
const path = require('node:path');
const { spawnSync } = require('node:child_process');

const { PROJECT_ROOT, loadConfig } = require('../src/config');

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
  const result = spawnSync(process.execPath, args, {
    cwd: REPO_ROOT,
    env: process.env,
    stdio: 'inherit',
  });
  if (result.status !== 0) throw new Error(`step_failed:${label}:exit_${result.status}`);
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
