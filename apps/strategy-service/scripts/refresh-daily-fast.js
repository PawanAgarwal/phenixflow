#!/usr/bin/env node

const { spawnSync } = require('node:child_process');
const path = require('node:path');

const REPO_ROOT = path.resolve(__dirname, '..', '..', '..');

function parseArgs(argv = process.argv.slice(2)) {
  const args = {
    end: 'auto',
    dryRun: false,
    skipMl: false,
    skipWheel: false,
    skipTsll: false,
    skipIntradayBuilders: false,
  };
  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    if (arg === '--end') args.end = argv[++index];
    else if (arg.startsWith('--end=')) args.end = arg.slice('--end='.length);
    else if (arg === '--dry-run') args.dryRun = true;
    else if (arg === '--skip-ml') args.skipMl = true;
    else if (arg === '--skip-wheel') args.skipWheel = true;
    else if (arg === '--skip-tsll') args.skipTsll = true;
    else if (arg === '--skip-intraday-builders') args.skipIntradayBuilders = true;
    else throw new Error(`unknown_arg:${arg}`);
  }
  return args;
}

function step(label, args, options = {}) {
  return {
    label,
    command: process.execPath,
    args,
    optional: Boolean(options.optional),
  };
}

function withEnd(baseArgs, end) {
  return end && end !== 'auto' ? [...baseArgs, '--end', end] : baseArgs;
}

function withEndDate(baseArgs, end) {
  return end && end !== 'auto' ? [...baseArgs, '--end-date', end] : baseArgs;
}

function buildSteps(args) {
  const steps = [
    step('refresh_eod_option_stress_inputs', [
      'projects/pym-v5-replication/scripts/refresh-eod-inputs.js',
      '--with-option-features',
      '--with-stress-signal',
      '--end', args.end,
    ]),
  ];
  if (!args.skipMl) {
    steps.push(
      step('refresh_lgbm_artifact', [
        'projects/pym-v5-ml-experiments/scripts/refresh-lgbm-artifact.js',
        '--skip-input-refresh',
        '--with-stress-signal',
        '--end', args.end,
      ]),
      step('refresh_daily_ml_artifacts', [
        'projects/pym-v5-ml-experiments/scripts/refresh-daily-artifacts.js',
        '--skip-input-refresh',
        '--end', args.end,
      ]),
    );
  }
  if (!args.skipWheel) {
    steps.push(step('refresh_wheel_option_income', withEndDate([
      'projects/spy-intraday-prediction/scripts/refresh-wheel-option-income.js',
    ], args.end)));
  }
  if (!args.skipTsll) {
    steps.push(step('refresh_tsll_second_passive', withEndDate([
      'projects/tsll-scalping/scripts/refresh-second-passive-report.js',
    ], args.end)));
  }
  if (!args.skipIntradayBuilders) {
    steps.push(
      step('build_pym_gated_artifacts', withEnd([
        'projects/spy-intraday-prediction/scripts/build-pym-gated-artifacts.js',
        '--start', '2025-01-02',
      ], args.end)),
      step('build_occ_pc_contrarian_artifacts', withEnd([
        'projects/spy-intraday-prediction/scripts/build-occ-pc-contrarian-artifacts.js',
      ], args.end)),
      step('build_vix_term_artifacts', withEnd([
        'projects/spy-intraday-prediction/scripts/build-vix-term-structure-artifacts.js',
      ], args.end)),
      step('build_vvix_spike_artifacts', withEnd([
        'projects/spy-intraday-prediction/scripts/build-vvix-spike-artifacts.js',
      ], args.end)),
      step('build_gap_down_fade_artifacts', withEnd([
        'projects/spy-intraday-prediction/scripts/build-gap-down-fade-artifacts.js',
      ], args.end)),
    );
  }
  const snapshotArgs = ['apps/strategy-service/scripts/build-refresh-snapshot.js'];
  if (args.end && args.end !== 'auto') snapshotArgs.push('--as-of', args.end);
  steps.push(
    step('persist_strategy_results_and_snapshot', snapshotArgs),
    step('profile_strategy_loads', [
      'apps/strategy-service/scripts/profile-strategies.js',
    ], { optional: true }),
  );
  return steps;
}

function runStep(item) {
  const startedAt = Date.now();
  console.log(JSON.stringify({ step: item.label, command: item.command, args: item.args }));
  const result = spawnSync(item.command, item.args, {
    cwd: REPO_ROOT,
    env: process.env,
    stdio: 'inherit',
  });
  const elapsedMs = Date.now() - startedAt;
  if (result.status !== 0 && !item.optional) {
    throw new Error(`step_failed:${item.label}:exit_${result.status}:elapsedMs_${elapsedMs}`);
  }
  console.log(JSON.stringify({
    step: item.label,
    status: result.status === 0 ? 'ok' : 'failed_optional',
    exitCode: result.status,
    elapsedMs,
  }));
  return { label: item.label, exitCode: result.status, elapsedMs };
}

function main() {
  const args = parseArgs();
  const steps = buildSteps(args);
  if (args.dryRun) {
    console.log(JSON.stringify({ dryRun: true, steps }, null, 2));
    return;
  }
  const startedAt = Date.now();
  const results = steps.map(runStep);
  console.log(JSON.stringify({
    status: 'ok',
    elapsedMs: Date.now() - startedAt,
    steps: results,
  }, null, 2));
}

try {
  main();
} catch (error) {
  console.error(error.stack || error.message);
  process.exit(1);
}
