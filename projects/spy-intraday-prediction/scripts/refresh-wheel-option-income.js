#!/usr/bin/env node
const fs = require('node:fs');
const path = require('node:path');
const { spawnSync } = require('node:child_process');

const { PROJECT_ROOT, loadConfig, resolveEndDate } = require('../src/config');

const REPO_ROOT = path.resolve(PROJECT_ROOT, '..', '..');
const DEFAULT_START = '2025-01-02';
const DEFAULT_STRATEGY = 'wheel_weekly_10otm_trend_ivrv_profit50';

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

function run(args) {
  console.log(JSON.stringify({ step: 'backtest_wheel_option_income', command: process.execPath, args }));
  const result = spawnSync(process.execPath, args, {
    cwd: REPO_ROOT,
    env: process.env,
    stdio: 'inherit',
  });
  if (result.status !== 0) throw new Error(`step_failed:backtest_wheel_option_income:exit_${result.status}`);
}

function latestPriorArtifact({ startDate, endDate }) {
  const dir = path.join(REPO_ROOT, 'projects', 'spy-intraday-prediction', 'artifacts');
  if (!fs.existsSync(dir)) return null;
  const prefix = `wheel-expanded-backtest-${startDate}-`;
  return fs.readdirSync(dir)
    .map((name) => {
      if (!name.startsWith(prefix) || !name.endsWith('.json')) return null;
      const reportEnd = name.slice(prefix.length, -'.json'.length);
      if (reportEnd >= endDate) return null;
      return { name, endDate: reportEnd, path: path.join('projects', 'spy-intraday-prediction', 'artifacts', name) };
    })
    .filter(Boolean)
    .sort((left, right) => right.endDate.localeCompare(left.endDate))
    .at(0) || null;
}

function main() {
  const args = parseArgs();
  const config = loadConfig(args.config);
  const startDate = args.start || args['start-date'] || DEFAULT_START;
  const endDate = resolveEndDate(config, args.end || args['end-date'] || 'auto');
  const strategy = args.strategies || args.strategy || DEFAULT_STRATEGY;
  const output = path.join(
    'projects',
    'spy-intraday-prediction',
    'artifacts',
    `wheel-expanded-backtest-${startDate}-${endDate}.json`,
  );

  if (args.force !== true && fs.existsSync(path.join(REPO_ROOT, output))) {
    console.log(JSON.stringify({
      status: 'current',
      startDate,
      endDate,
      output,
      reason: 'existing_artifact_covers_resolved_end_date',
    }, null, 2));
    return;
  }

  const prior = args.force === true ? null : latestPriorArtifact({ startDate, endDate });
  if (prior) {
    const priorReport = JSON.parse(fs.readFileSync(path.join(REPO_ROOT, prior.path), 'utf8'));
    if (priorReport.checkpoint?.schemaVersion === 'wheel-backtest-checkpoint.v1') {
      run([
        'projects/spy-intraday-prediction/scripts/backtest-wheel-strategy.js',
        '--start-date', startDate,
        '--end-date', endDate,
        '--suite', 'expanded',
        '--strategies', strategy,
        '--output', output,
        '--resume-from', prior.path,
      ]);
      console.log(JSON.stringify({
        status: 'ok',
        mode: 'incremental',
        startDate,
        endDate,
        appendFrom: prior.path,
        output,
      }, null, 2));
      return;
    }
  }

  if (args['no-full-rebuild'] === true && args.force !== true) {
    console.log(JSON.stringify({
      status: 'needs_full_rebuild',
      startDate,
      endDate,
      output,
      prior,
      reason: 'wheel_incremental_resume_checkpoint_not_available',
    }, null, 2));
    return;
  }

  run([
    'projects/spy-intraday-prediction/scripts/backtest-wheel-strategy.js',
    '--start-date', startDate,
    '--end-date', endDate,
    '--suite', 'expanded',
    '--strategies', strategy,
    '--output', output,
  ]);
  console.log(JSON.stringify({ status: 'ok', startDate, endDate, output }, null, 2));
}

try {
  main();
} catch (error) {
  console.error(error.stack || error.message);
  process.exit(1);
}
