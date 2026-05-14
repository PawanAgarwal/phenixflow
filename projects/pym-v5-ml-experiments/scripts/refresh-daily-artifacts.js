#!/usr/bin/env node

const fs = require('node:fs');
const path = require('node:path');
const { spawnSync } = require('node:child_process');

const { loadConfig } = require('../../pym-v5-replication/src/config');
const { resolveEndDate } = require('../../pym-v5-replication/src/calendar');

const REPO_ROOT = path.resolve(__dirname, '..', '..', '..');
const PYTHON = fs.existsSync(path.join(REPO_ROOT, 'projects/pym-v5-ml-experiments/.venv/bin/python'))
  ? 'projects/pym-v5-ml-experiments/.venv/bin/python'
  : 'python3';

function parseArgs(argv) {
  const args = {
    start: '2025-01-02',
    trainStart: '2025-01-02',
    predictStart: '2025-02-01',
    end: 'auto',
    eodStart: '2024-01-01',
    optionStrategy: 'grid_pym_option_rank_top8_zm0p5',
    optionLabel: 'grid-top8-zm0p5',
    skipInputRefresh: false,
    force: false,
  };
  for (let index = 0; index < argv.length; index += 1) {
    const key = argv[index];
    const next = argv[index + 1];
    if (key === '--help') {
      args.help = true;
    } else if (key === '--start') {
      args.start = next; index += 1;
    } else if (key === '--train-start') {
      args.trainStart = next; index += 1;
    } else if (key === '--predict-start') {
      args.predictStart = next; index += 1;
    } else if (key === '--end') {
      args.end = next; index += 1;
    } else if (key === '--eod-start') {
      args.eodStart = next; index += 1;
    } else if (key === '--option-strategy') {
      args.optionStrategy = next; index += 1;
    } else if (key === '--option-label') {
      args.optionLabel = next; index += 1;
    } else if (key === '--skip-input-refresh') {
      args.skipInputRefresh = true;
    } else if (key === '--force') {
      args.force = true;
    } else {
      throw new Error(`unknown_arg:${key}`);
    }
  }
  return args;
}

function usage() {
  console.log([
    'Usage: node projects/pym-v5-ml-experiments/scripts/refresh-daily-artifacts.js [options]',
    '',
    'Options:',
    '  --end YYYY-MM-DD|auto        Artifact end date; auto uses latest local Massive stock date.',
    '  --start YYYY-MM-DD           Dataset/option overlay start date. Default: 2025-01-02.',
    '  --predict-start YYYY-MM-DD   Walkforward prediction start. Default: 2025-02-01.',
    '  --train-start YYYY-MM-DD     Walkforward training start. Default: 2025-01-02.',
    '  --skip-input-refresh         Reuse existing daily bars and option features.',
  ].join('\n'));
}

function run(label, command, args) {
  console.log(JSON.stringify({ step: label, command, args }));
  const startedAt = Date.now();
  const result = spawnSync(command, args, {
    cwd: REPO_ROOT,
    stdio: 'inherit',
    shell: false,
  });
  if (result.status !== 0) {
    throw new Error(`step_failed:${label}:exit_${result.status}`);
  }
  console.log(JSON.stringify({ step: label, status: 'ok', elapsedMs: Date.now() - startedAt }));
}

function latestPriorReport({ predictStart, end }) {
  const dir = path.join(REPO_ROOT, 'projects', 'pym-v5-ml-experiments', 'artifacts');
  if (!fs.existsSync(dir)) return null;
  const prefix = `pym-v5-daily-walkforward-micro-features-${predictStart}-`;
  return fs.readdirSync(dir)
    .map((name) => {
      if (!name.startsWith(prefix) || !name.endsWith('.json')) return null;
      const reportEnd = name.slice(prefix.length, -'.json'.length);
      if (reportEnd >= end) return null;
      return { name, end: reportEnd, path: path.join('projects', 'pym-v5-ml-experiments', 'artifacts', name) };
    })
    .filter(Boolean)
    .sort((left, right) => right.end.localeCompare(left.end))
    .at(0)?.path || null;
}

function main() {
  const args = parseArgs(process.argv.slice(2));
  if (args.help) {
    usage();
    return;
  }

  const config = loadConfig();
  const end = resolveEndDate(config, args.end);
  const dailyBars = `projects/pym-v5-replication/runtime/pym-v5-massive-eod-adjusted-daily-bars-${args.eodStart}-${end}.jsonl`;
  const optionFeatures = `projects/pym-v5-replication/runtime/pym-v5-option-bar-features-${args.start}-${end}.jsonl`;
  const dataset = `projects/pym-v5-ml-experiments/artifacts/pym-v5-walkforward-dataset-micro-features-${args.start}-${end}.jsonl`;
  const mlReport = `projects/pym-v5-ml-experiments/artifacts/pym-v5-daily-walkforward-micro-features-${args.predictStart}-${end}.json`;
  const optionOverlay = `projects/pym-v5-replication/artifacts/pym-v5-option-overlay-suite-${args.optionLabel}-${args.start}-${end}.json`;
  const riskOverlay = `projects/pym-v5-ml-experiments/artifacts/pym-v5-two-speed-risk-overlays-${args.predictStart}-${end}.json`;

  if (!args.force
    && fs.existsSync(path.join(REPO_ROOT, dataset))
    && fs.existsSync(path.join(REPO_ROOT, mlReport))
    && fs.existsSync(path.join(REPO_ROOT, optionOverlay))
    && fs.existsSync(path.join(REPO_ROOT, riskOverlay))) {
    console.log(JSON.stringify({
      status: 'current',
      end,
      artifacts: { dataset, mlReport, optionOverlay, riskOverlay },
      reason: 'existing_daily_ml_artifacts_cover_resolved_end_date',
    }, null, 2));
    return;
  }

  if (!args.skipInputRefresh) {
    run('refresh_inputs', 'node', [
      'projects/pym-v5-replication/scripts/refresh-eod-inputs.js',
      '--with-option-features',
      '--end', end,
    ]);
  }

  run('export_walkforward_dataset', 'node', [
    'projects/pym-v5-ml-experiments/scripts/export-walkforward-dataset.js',
    '--daily-bars', dailyBars,
    '--option-features', optionFeatures,
    '--start', args.start,
    '--end', end,
    '--out', dataset,
  ]);

  run('option_overlay_suite', 'node', [
    'projects/pym-v5-replication/scripts/run-option-overlay-suite.js',
    '--start', args.start,
    '--end', end,
    '--daily-bars', dailyBars,
    '--option-features', optionFeatures,
    '--strategies', args.optionStrategy,
    '--label', args.optionLabel,
    '--output', optionOverlay,
  ]);

  const appendFrom = args.force ? null : latestPriorReport({ predictStart: args.predictStart, end });
  const mlArgs = [
    'projects/pym-v5-ml-experiments/python/run_daily_walkforward.py',
    '--dataset', dataset,
    '--out', mlReport,
    '--train-start', args.trainStart,
    '--predict-start', args.predictStart,
    '--progress', '50',
  ];
  if (appendFrom) mlArgs.push('--append-from', appendFrom);
  run('ml_walkforward', PYTHON, mlArgs);

  run('risk_overlays', 'node', [
    'projects/pym-v5-ml-experiments/scripts/apply-risk-overlays.js',
    '--ml-report', mlReport,
    '--dataset', dataset,
    '--option-features', optionFeatures,
    '--option-overlay-report', optionOverlay,
    '--out', riskOverlay,
  ]);

  console.log(JSON.stringify({
    status: 'ok',
    end,
    appendFrom,
    artifacts: {
      dataset,
      mlReport,
      optionOverlay,
      riskOverlay,
    },
  }, null, 2));
}

try {
  main();
} catch (error) {
  console.error(error.stack || error.message);
  process.exit(1);
}
