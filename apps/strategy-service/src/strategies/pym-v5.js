const fs = require('node:fs');
const path = require('node:path');
const { spawn } = require('node:child_process');

const { loadConfig } = require('../../../../projects/pym-v5-replication/src/config');
const { readDailyBarsJsonl } = require('../../../../projects/pym-v5-replication/src/backtest');
const { loadMassiveEnv } = require('../../../../projects/pym-v5-replication/src/env');
const {
  buildDailyRebalanceReport,
  defaultScorePath,
  findLatestMassiveEodBarsPath,
} = require('../../../../projects/pym-v5-replication/src/rebalance-report');

function latestBarsMetadata(filePath) {
  if (!filePath || !fs.existsSync(filePath)) return null;
  const basename = path.basename(filePath);
  const match = basename.match(/daily-bars-(\d{4}-\d{2}-\d{2})-(\d{4}-\d{2}-\d{2})\.jsonl$/);
  const stats = fs.statSync(filePath);
  return {
    path: filePath,
    name: basename,
    fetchStartDate: match?.[1] || null,
    endDate: match?.[2] || null,
    updatedAt: stats.mtime.toISOString(),
  };
}

function createPymV5Strategy(options = {}) {
  loadMassiveEnv();
  const config = options.config || loadConfig();
  const state = {
    report: null,
    loadedAt: null,
    refresh: {
      running: false,
      startedAt: null,
      finishedAt: null,
      exitCode: null,
      log: [],
      error: null,
    },
  };

  function getMetadata() {
    return {
      id: 'pym-v5',
      name: 'PYM V5',
      displayName: "Eagle's Park your Money V5",
      family: 'composer',
      cadence: 'daily_eod',
      actionType: 'rebalance',
      dataProvider: 'Massive adjusted EOD',
      strategySource: 'Composer public tree',
      composerSymphonyId: config.source.composerSymphonyId,
      defaultStartDate: options.startDate || process.env.PYM_V5_REBALANCE_START || '2025-01-01',
      supports: ['chart', 'values', 'latest_portfolio', 'portfolio_change', 'refresh_data'],
    };
  }

  function resolvePaths() {
    return {
      scorePath: options.scorePath || process.env.PYM_V5_SCORE_PATH || defaultScorePath(config),
      barsPath: options.barsPath || process.env.PYM_V5_DAILY_BARS_PATH || findLatestMassiveEodBarsPath(),
    };
  }

  function recompute() {
    const { scorePath, barsPath } = resolvePaths();
    if (!scorePath || !fs.existsSync(scorePath)) throw new Error(`missing_score_snapshot:${scorePath}`);
    if (!barsPath || !fs.existsSync(barsPath)) throw new Error('missing_massive_eod_bars: mount runtime data or run pym-v5:massive-eod-build');
    const score = JSON.parse(fs.readFileSync(scorePath, 'utf8'));
    const market = readDailyBarsJsonl(barsPath);
    const report = buildDailyRebalanceReport({
      market,
      score,
      startDate: options.startDate || process.env.PYM_V5_REBALANCE_START || '2025-01-01',
      rsiMode: options.rsiMode || process.env.PYM_V5_RSI_MODE || 'wilder',
      initialCapital: config.execution.initialCapital,
      transactionCostBps: config.execution.transactionCostBps,
      slippageBps: config.execution.slippageBps,
      source: {
        ...getMetadata(),
        scorePath,
        bars: latestBarsMetadata(barsPath),
      },
    });
    state.report = report;
    state.loadedAt = new Date().toISOString();
    return report;
  }

  function getReport() {
    if (!state.report) recompute();
    return state.report;
  }

  function appendRefreshLog(chunk) {
    const lines = String(chunk || '').split(/\r?\n/).filter(Boolean);
    state.refresh.log.push(...lines);
    state.refresh.log = state.refresh.log.slice(-100);
  }

  function refreshData() {
    if (state.refresh.running) return { accepted: false, status: state.refresh };
    state.refresh = {
      running: true,
      startedAt: new Date().toISOString(),
      finishedAt: null,
      exitCode: null,
      log: [],
      error: null,
    };
    const repoRoot = path.resolve(__dirname, '..', '..', '..', '..');
    const scriptPath = path.join(repoRoot, 'projects', 'pym-v5-replication', 'scripts', 'build-massive-eod-daily-bars.js');
    const args = [scriptPath, '--fetch-start', process.env.PYM_V5_EOD_FETCH_START || '2024-01-01'];
    if (process.env.PYM_V5_REFRESH_END) args.push('--end', process.env.PYM_V5_REFRESH_END);
    const child = spawn(process.execPath, args, {
      cwd: repoRoot,
      env: process.env,
      stdio: ['ignore', 'pipe', 'pipe'],
    });
    child.stdout.on('data', appendRefreshLog);
    child.stderr.on('data', appendRefreshLog);
    child.on('error', (error) => {
      state.refresh.error = error.message;
    });
    child.on('close', (code) => {
      state.refresh.running = false;
      state.refresh.finishedAt = new Date().toISOString();
      state.refresh.exitCode = code;
      if (code === 0) {
        try {
          recompute();
        } catch (error) {
          state.refresh.error = error.message;
        }
      } else if (!state.refresh.error) {
        state.refresh.error = `refresh_failed:${code}`;
      }
    });
    return { accepted: true, status: state.refresh };
  }

  return {
    state,
    getMetadata,
    getReport,
    recompute,
    refreshData,
  };
}

module.exports = {
  createPymV5Strategy,
};
