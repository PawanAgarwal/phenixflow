// Strategy module for the VIX term-structure contrarian variants.

const fs = require('node:fs');
const path = require('node:path');

const { regularSessionExecution } = require('./execution');
const { refreshEodInputsStep, runRefreshSequence } = require('./refresh-helpers');

const REPO_ROOT = path.resolve(__dirname, '..', '..', '..', '..');
const ARTIFACTS_DIR = path.join(REPO_ROOT, 'projects', 'spy-intraday-prediction', 'artifacts');
const BUILD_SCRIPT_PATH = path.join(
  REPO_ROOT, 'projects', 'spy-intraday-prediction', 'scripts', 'build-vix-term-structure-artifacts.js',
);

const VARIANTS = {
  'vix-term-contrarian-intraday-vix3m-1x': {
    name: 'VIX Term Contrarian Intraday (VIX/VIX3M, 1x)',
    displayName: 'VIX Term Contrarian — VIX/VIX3M 1×',
    description: 'Contrarian on VIX/VIX3M term-structure z-score extremes. Inversion (z≥+2) → LONG SPY 9:35→15:55 ET; steep contango (z≤-2) → SHORT.',
    family: 'volatility-contrarian',
    cadence: 'intraday',
    actionType: 'signal_trade',
    execution: regularSessionExecution({ startTime: '09:35', endTime: '15:55' }),
  },
  'vix-term-contrarian-intraday-inv-long-3x-overnight': {
    name: 'VIX Term Contrarian Overnight (Inv-Long 3x)',
    displayName: 'VIX Term Contrarian — Inv-Long 3× Overnight',
    description: 'When VIX1D/VIX3M z-score ≥ +2 (extreme inversion / panic), buy TQQQ at THAT EOD 15:55 ET and exit next session 15:55 ET. 3× leverage capturing the V-shape recovery overnight.',
    family: 'volatility-contrarian',
    cadence: 'intraday_plus_overnight',
    actionType: 'signal_trade',
    execution: regularSessionExecution({ startTime: '15:50', endTime: '15:55' }),
  },
};

function resolveArtifactPath(variantId) {
  return path.join(ARTIFACTS_DIR, `${variantId}-report.json`);
}

function readReport(filePath) {
  if (!filePath || !fs.existsSync(filePath)) {
    throw new Error(`missing_vix_term_artifact:${filePath} — run build-vix-term-structure-artifacts.js`);
  }
  return JSON.parse(fs.readFileSync(filePath, 'utf8'));
}

function createVixTermContrarianStrategy({ variantId, ...options } = {}) {
  if (!VARIANTS[variantId]) throw new Error(`unknown_vix_term_variant:${variantId}`);
  const spec = VARIANTS[variantId];
  const artifactPath = options.artifactPath || resolveArtifactPath(variantId);
  const state = { report: null, loadedAt: null, refresh: null };

  function getMetadata() {
    const report = state.report;
    return {
      id: variantId,
      name: spec.name,
      displayName: spec.displayName,
      family: spec.family,
      cadence: spec.cadence,
      actionType: spec.actionType,
      execution: spec.execution,
      dataProvider: 'Massive indices_1m (VIX1D / VIX / VIX3M) + SPY 1m bars',
      strategySource: 'VIX term-structure z-score contrarian',
      description: spec.description,
      ruleSummary: report?.metadata?.ruleSummary || ['Built from VIX term-structure z-score; see artifact for full rule chain.'],
      sourceLinks: [
        { label: 'CBOE VIX 9-day index', href: 'https://www.cboe.com/tradable_products/vix/vix1d/' },
      ],
      defaultStartDate: report?.summary?.startDate || '2025-01-02',
      artifactPath,
      supports: ['chart', 'values', 'latest_portfolio', 'portfolio_change', 'refresh_data', 'trade_log', 'open_positions', 'sharpe'],
    };
  }

  function recompute() {
    state.report = readReport(artifactPath);
    state.loadedAt = new Date().toISOString();
    return state.report;
  }

  function getReport() {
    if (!state.report) recompute();
    return state.report;
  }

  function refreshData() {
    return runRefreshSequence(state, [
      refreshEodInputsStep(),
      {
        label: 'build-vix-term-structure-artifacts',
        command: process.execPath,
        args: [BUILD_SCRIPT_PATH],
      },
    ], recompute);
  }

  return { state, getMetadata, getReport, recompute, refreshData };
}

module.exports = {
  createVixTermContrarianStrategy,
  VIX_TERM_VARIANTS: Object.keys(VARIANTS),
};
