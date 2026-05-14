#!/usr/bin/env node

const fs = require('node:fs');
const path = require('node:path');
const { spawnSync } = require('node:child_process');

const { loadConfig } = require('../../pym-v5-replication/src/config');
const { resolveEndDate } = require('../../pym-v5-replication/src/calendar');

const REPO_ROOT = path.resolve(__dirname, '..', '..', '..');
const DEFAULT_PYTHON = fs.existsSync(path.join(REPO_ROOT, 'projects/pym-v5-ml-experiments/.venv/bin/python'))
  ? 'projects/pym-v5-ml-experiments/.venv/bin/python'
  : 'python3';

function parseArgs(argv) {
  const args = {
    start: process.env.PYM_V5_OPTION_FEATURES_START || '2025-01-02',
    trainStart: '2025-01-02',
    predictStart: '2025-02-01',
    end: 'auto',
    eodStart: process.env.PYM_V5_EOD_FETCH_START || '2024-01-01',
    strategy: 'lgbm_topk_attention_pym_eq_tinyB',
    skipInputRefresh: false,
    withStressSignal: false,
    force: false,
  };
  for (let index = 0; index < argv.length; index += 1) {
    const key = argv[index];
    const next = argv[index + 1];
    if (key === '--help') args.help = true;
    else if (key === '--start') { args.start = next; index += 1; }
    else if (key === '--train-start') { args.trainStart = next; index += 1; }
    else if (key === '--predict-start') { args.predictStart = next; index += 1; }
    else if (key === '--end') { args.end = next; index += 1; }
    else if (key === '--eod-start') { args.eodStart = next; index += 1; }
    else if (key === '--strategy') { args.strategy = next; index += 1; }
    else if (key === '--skip-input-refresh') args.skipInputRefresh = true;
    else if (key === '--with-stress-signal') args.withStressSignal = true;
    else if (key === '--force') args.force = true;
    else throw new Error(`unknown_arg:${key}`);
  }
  return args;
}

function usage() {
  console.log([
    'Usage: node projects/pym-v5-ml-experiments/scripts/refresh-lgbm-artifact.js [options]',
    '',
    'Options:',
    '  --end YYYY-MM-DD|auto        Artifact end date; auto uses latest local Massive stock date.',
    '  --eod-start YYYY-MM-DD       EOD bar fetch start. Default: env PYM_V5_EOD_FETCH_START or 2024-01-01.',
    '  --start YYYY-MM-DD           Dataset/option-feature start. Default: env PYM_V5_OPTION_FEATURES_START or 2025-01-02.',
    '  --strategy ID                LightGBM strategy id. Default: lgbm_topk_attention_pym_eq_tinyB.',
    '  --with-stress-signal         Also refresh the options-stress artifact if stale.',
    '  --skip-input-refresh         Reuse existing EOD bars and option features.',
    '  --force                      Force rebuild of refresh-eod-inputs outputs.',
  ].join('\n'));
}

function run(label, command, args) {
  console.log(JSON.stringify({ step: label, command, args }));
  const startedAt = Date.now();
  const result = spawnSync(command, args, {
    cwd: REPO_ROOT,
    env: process.env,
    stdio: 'inherit',
    shell: false,
  });
  if (result.status !== 0) throw new Error(`step_failed:${label}:exit_${result.status}`);
  console.log(JSON.stringify({ step: label, status: 'ok', elapsedMs: Date.now() - startedAt }));
}

function latestPriorReport({ predictStart, end }) {
  const dir = path.join(REPO_ROOT, 'projects', 'pym-v5-ml-experiments', 'artifacts');
  if (!fs.existsSync(dir)) return null;
  const prefix = `pym-v5-daily-walkforward-lgbm-tiny-grid-${predictStart}-`;
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
  const dataset = `projects/pym-v5-ml-experiments/artifacts/pym-v5-walkforward-dataset-${args.start}-${end}.jsonl`;
  const lgbmReport = `projects/pym-v5-ml-experiments/artifacts/pym-v5-daily-walkforward-lgbm-tiny-grid-${args.predictStart}-${end}.json`;

  if (!args.force && fs.existsSync(path.join(REPO_ROOT, lgbmReport))) {
    console.log(JSON.stringify({
      status: 'current',
      end,
      artifacts: { dataset, lgbmReport },
      reason: 'existing_lgbm_report_covers_resolved_end_date',
    }, null, 2));
    return;
  }

  if (!args.skipInputRefresh) {
    const refreshArgs = [
      'projects/pym-v5-replication/scripts/refresh-eod-inputs.js',
      '--with-option-features',
      '--end', end,
      '--fetch-start', args.eodStart,
      '--option-start', args.start,
    ];
    if (args.withStressSignal) refreshArgs.push('--with-stress-signal');
    if (args.force) refreshArgs.push('--force');
    run('refresh_inputs', 'node', refreshArgs);
  }

  run('export_walkforward_dataset', 'node', [
    'projects/pym-v5-ml-experiments/scripts/export-walkforward-dataset.js',
    '--daily-bars', dailyBars,
    '--option-features', optionFeatures,
    '--start', args.start,
    '--end', end,
    '--out', dataset,
  ]);

  const appendFrom = args.force ? null : latestPriorReport({ predictStart: args.predictStart, end });
  const walkforwardArgs = [
    'projects/pym-v5-ml-experiments/python/run_daily_walkforward.py',
    '--dataset', dataset,
    '--out', lgbmReport,
    '--train-start', args.trainStart,
    '--predict-start', args.predictStart,
    '--lgbm-only',
    '--strategies', args.strategy,
    '--progress', '50',
  ];
  if (appendFrom) walkforwardArgs.push('--append-from', appendFrom);
  run('lgbm_walkforward', process.env.PYM_V5_ML_PYTHON || DEFAULT_PYTHON, walkforwardArgs);

  console.log(JSON.stringify({
    status: 'ok',
    end,
    appendFrom,
    artifacts: {
      dataset,
      lgbmReport,
    },
  }, null, 2));
}

try {
  main();
} catch (error) {
  console.error(error.stack || error.message);
  process.exit(1);
}
