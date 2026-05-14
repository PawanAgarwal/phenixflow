// Shared helpers for strategy refreshData() implementations.
//
// A "refresh" is an idempotent sequence of steps that fetches fresh data,
// rebuilds derived artifacts, then triggers recompute() on the strategy.
//
// Each step is `{ label, command, args, cwd?, env? }`. Steps run sequentially.
// If any step exits non-zero the sequence stops and onSuccess is NOT called;
// the error is captured in state.refresh.error for the API response.
//
// The handler is non-blocking: refreshData() returns immediately with
// { accepted, status }; the caller can poll the same strategy's metadata
// or refresh field to track progress. This matches the pre-existing
// pym-v5 refreshData pattern.

const { spawn } = require('node:child_process');
const path = require('node:path');

const REPO_ROOT = path.resolve(__dirname, '..', '..', '..', '..');
const LOG_TAIL = 100; // Keep only the last N log lines per refresh.

function createRefreshState() {
  return {
    running: false,
    startedAt: null,
    finishedAt: null,
    exitCode: null,
    log: [],
    error: null,
    currentStep: null,
    completedSteps: [],
    plannedSteps: [],
  };
}

function appendLog(state, chunk) {
  const lines = String(chunk || '').split(/\r?\n/).filter(Boolean);
  state.refresh.log.push(...lines);
  state.refresh.log = state.refresh.log.slice(-LOG_TAIL);
}

function spawnStep(state, step) {
  return new Promise((resolve) => {
    state.refresh.currentStep = step.label;
    const startedAt = Date.now();
    appendLog(state, `\n[step ${step.label}] starting: ${step.command} ${(step.args || []).join(' ')}`);
    const child = spawn(step.command, step.args || [], {
      cwd: step.cwd || REPO_ROOT,
      env: { ...process.env, ...(step.env || {}) },
      stdio: ['ignore', 'pipe', 'pipe'],
    });
    child.stdout.on('data', (chunk) => appendLog(state, chunk));
    child.stderr.on('data', (chunk) => appendLog(state, chunk));
    child.on('error', (error) => {
      state.refresh.error = `${step.label}: ${error.message}`;
      resolve(1);
    });
    child.on('close', (code) => {
      const elapsedMs = Date.now() - startedAt;
      if (code === 0) {
        state.refresh.completedSteps.push(step.label);
        appendLog(state, `[step ${step.label}] OK elapsedMs=${elapsedMs}`);
      } else {
        state.refresh.error = state.refresh.error || `${step.label}: exit ${code}`;
        appendLog(state, `[step ${step.label}] FAILED exit=${code} elapsedMs=${elapsedMs}`);
      }
      resolve(code);
    });
  });
}

function refreshEodInputsStep(options = {}) {
  const args = [
    path.join(REPO_ROOT, 'projects', 'pym-v5-replication', 'scripts', 'refresh-eod-inputs.js'),
    '--fetch-start', process.env.PYM_V5_EOD_FETCH_START || '2024-01-01',
  ];
  if (process.env.PYM_V5_REFRESH_END) args.push('--end', process.env.PYM_V5_REFRESH_END);
  if (process.env.PYM_V5_OPTION_FEATURES_START) args.push('--option-start', process.env.PYM_V5_OPTION_FEATURES_START);
  if (options.withOptionFeatures) args.push('--with-option-features');
  if (options.withStressSignal) args.push('--with-stress-signal');
  if (process.env.PYM_V5_REFRESH_FORCE === '1') args.push('--force');
  return {
    label: options.label || 'refresh-eod-inputs',
    command: process.execPath,
    args,
  };
}

function refreshLgbmArtifactStep(options = {}) {
  const args = [
    path.join(REPO_ROOT, 'projects', 'pym-v5-ml-experiments', 'scripts', 'refresh-lgbm-artifact.js'),
  ];
  if (process.env.PYM_V5_REFRESH_END) args.push('--end', process.env.PYM_V5_REFRESH_END);
  if (options.withStressSignal) args.push('--with-stress-signal');
  if (process.env.PYM_V5_REFRESH_FORCE === '1') args.push('--force');
  return {
    label: options.label || 'refresh-lgbm-artifact',
    command: process.execPath,
    args,
  };
}

// Run a sequence of steps. Each step is a single child process.
// On all-success, calls onSuccess() (typically the strategy's recompute()).
// state must have a `state.refresh` field (created via createRefreshState).
function runRefreshSequence(state, steps, onSuccess) {
  if (state.refresh && state.refresh.running) {
    return { accepted: false, status: state.refresh };
  }
  state.refresh = createRefreshState();
  state.refresh.running = true;
  state.refresh.startedAt = new Date().toISOString();
  state.refresh.plannedSteps = steps.map((s) => s.label);

  (async () => {
    try {
      for (const step of steps) {
        const code = await spawnStep(state, step);
        if (code !== 0) {
          state.refresh.exitCode = code;
          state.refresh.running = false;
          state.refresh.finishedAt = new Date().toISOString();
          return;
        }
      }
      try {
        if (onSuccess) onSuccess();
        state.refresh.exitCode = 0;
      } catch (error) {
        state.refresh.error = `recompute: ${error.message}`;
        state.refresh.exitCode = 99;
      }
    } finally {
      state.refresh.running = false;
      state.refresh.currentStep = null;
      state.refresh.finishedAt = new Date().toISOString();
    }
  })();

  return { accepted: true, status: state.refresh };
}

// Convenience: a no-op refresh that just calls recompute(). Used by
// artifact-backed strategies that don't fetch data dynamically (their
// underlying artifacts are regenerated by separate manual workflows).
function noopRefresh(state, recompute) {
  if (state.refresh && state.refresh.running) {
    return { accepted: false, status: state.refresh };
  }
  state.refresh = createRefreshState();
  state.refresh.startedAt = new Date().toISOString();
  state.refresh.plannedSteps = ['recompute'];
  try {
    if (recompute) recompute();
    state.refresh.completedSteps.push('recompute');
    state.refresh.exitCode = 0;
  } catch (error) {
    state.refresh.error = error.message;
    state.refresh.exitCode = 99;
  }
  state.refresh.finishedAt = new Date().toISOString();
  appendLog(state, `[noop refresh] strategy artifact is static; called recompute() only.`);
  return { accepted: true, status: state.refresh };
}

module.exports = {
  REPO_ROOT,
  appendLog,
  createRefreshState,
  noopRefresh,
  refreshEodInputsStep,
  refreshLgbmArtifactStep,
  runRefreshSequence,
};
