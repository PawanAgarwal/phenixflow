const fs = require('node:fs');
const path = require('node:path');
const { spawn } = require('node:child_process');

const express = require('express');

const { loadConfig } = require('../src/config');
const { readDailyBarsJsonl } = require('../src/backtest');
const { loadMassiveEnv } = require('../src/env');
const {
  buildDailyRebalanceReport,
  defaultScorePath,
  findLatestMassiveEodBarsPath,
  serializeSnapshotForList,
} = require('../src/rebalance-report');

function parseLimit(value, fallback = 500) {
  const parsed = Math.trunc(Number(value));
  if (!Number.isFinite(parsed)) return fallback;
  return Math.max(1, Math.min(5000, parsed));
}

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

function createReportStore(options = {}) {
  loadMassiveEnv();
  const config = options.config || loadConfig();
  const state = {
    report: null,
    error: null,
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

  function resolvePaths() {
    return {
      scorePath: options.scorePath || defaultScorePath(config),
      barsPath: options.barsPath || findLatestMassiveEodBarsPath(),
    };
  }

  function recompute() {
    const { scorePath, barsPath } = resolvePaths();
    if (!scorePath || !fs.existsSync(scorePath)) throw new Error(`missing_score_snapshot:${scorePath}`);
    if (!barsPath || !fs.existsSync(barsPath)) throw new Error('missing_massive_eod_bars: run npm run pym-v5:massive-eod-build first');
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
        scorePath,
        bars: latestBarsMetadata(barsPath),
        composerSymphonyId: config.source.composerSymphonyId,
        provider: 'Massive adjusted EOD',
      },
    });
    state.report = report;
    state.error = null;
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

  function refreshEod() {
    if (state.refresh.running) return { accepted: false, status: state.refresh };
    state.refresh = {
      running: true,
      startedAt: new Date().toISOString(),
      finishedAt: null,
      exitCode: null,
      log: [],
      error: null,
    };
    const scriptPath = path.resolve(__dirname, '..', 'scripts', 'build-massive-eod-daily-bars.js');
    const args = [scriptPath, '--fetch-start', process.env.PYM_V5_EOD_FETCH_START || '2024-01-01'];
    if (process.env.PYM_V5_REFRESH_END) args.push('--end', process.env.PYM_V5_REFRESH_END);
    const child = spawn(process.execPath, args, {
      cwd: path.resolve(__dirname, '..', '..', '..'),
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
    recompute,
    getReport,
    refreshEod,
  };
}

function createApp(options = {}) {
  const app = express();
  const store = options.store || createReportStore(options);
  const publicRoot = path.resolve(__dirname, '..', 'public');

  app.use(express.json());
  app.use(express.static(publicRoot));

  app.get('/api/health', (_req, res) => {
    res.status(200).json({ status: 'ok' });
  });

  app.get('/api/rebalance/summary', (_req, res) => {
    try {
      const report = store.getReport();
      res.status(200).json({
        generatedAt: report.generatedAt,
        loadedAt: store.state.loadedAt,
        source: report.source,
        settings: report.settings,
        summary: report.summary,
        latest: report.latest,
        refresh: store.state.refresh,
      });
    } catch (error) {
      res.status(503).json({ error: { code: 'rebalance_unavailable', message: error.message } });
    }
  });

  app.get('/api/rebalance/days', (req, res) => {
    try {
      const report = store.getReport();
      const start = String(req.query.start || '').trim();
      const end = String(req.query.end || '').trim();
      const limit = parseLimit(req.query.limit, report.snapshots.length);
      const data = report.snapshots
        .filter((snapshot) => (!start || snapshot.date >= start) && (!end || snapshot.date <= end))
        .slice(-limit)
        .map(serializeSnapshotForList);
      res.status(200).json({ data, count: data.length, total: report.snapshots.length });
    } catch (error) {
      res.status(503).json({ error: { code: 'rebalance_unavailable', message: error.message } });
    }
  });

  app.get('/api/rebalance/days/:date', (req, res) => {
    try {
      const report = store.getReport();
      const snapshot = report.snapshots.find((item) => item.date === req.params.date);
      if (!snapshot) {
        res.status(404).json({ error: { code: 'date_not_found', message: `No rebalance snapshot for ${req.params.date}` } });
        return;
      }
      res.status(200).json({ data: snapshot });
    } catch (error) {
      res.status(503).json({ error: { code: 'rebalance_unavailable', message: error.message } });
    }
  });

  app.get('/api/rebalance/latest', (_req, res) => {
    try {
      const report = store.getReport();
      res.status(200).json({ data: report.latest });
    } catch (error) {
      res.status(503).json({ error: { code: 'rebalance_unavailable', message: error.message } });
    }
  });

  app.get('/api/rebalance/equity', (_req, res) => {
    try {
      const report = store.getReport();
      res.status(200).json({ data: report.equitySeries });
    } catch (error) {
      res.status(503).json({ error: { code: 'rebalance_unavailable', message: error.message } });
    }
  });

  app.post('/api/rebalance/recompute', (_req, res) => {
    try {
      const report = store.recompute();
      res.status(200).json({ summary: report.summary, loadedAt: store.state.loadedAt });
    } catch (error) {
      res.status(503).json({ error: { code: 'rebalance_unavailable', message: error.message } });
    }
  });

  app.post('/api/rebalance/refresh-eod', (_req, res) => {
    const result = store.refreshEod();
    res.status(result.accepted ? 202 : 409).json(result.status);
  });

  app.get('*', (_req, res) => {
    res.sendFile(path.join(publicRoot, 'index.html'));
  });

  return app;
}

module.exports = {
  createApp,
  createReportStore,
};
