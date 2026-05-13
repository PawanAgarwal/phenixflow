// Strategy module for the VVIX spike contrarian variant.
// Reads a pre-built artifact at projects/spy-intraday-prediction/artifacts/.

const fs = require('node:fs');
const path = require('node:path');

const { regularSessionExecution } = require('./execution');
const { refreshEodInputsStep, runRefreshSequence } = require('./refresh-helpers');

const REPO_ROOT = path.resolve(__dirname, '..', '..', '..', '..');
const ARTIFACTS_DIR = path.join(REPO_ROOT, 'projects', 'spy-intraday-prediction', 'artifacts');
const BUILD_SCRIPT_PATH = path.join(
  REPO_ROOT, 'projects', 'spy-intraday-prediction', 'scripts', 'build-vvix-spike-artifacts.js',
);

const VARIANT = {
  id: 'vvix-spike-contrarian-overnight-3x',
  name: 'VVIX Spike Contrarian (Overnight, 3×)',
  displayName: 'VVIX Spike Contrarian — Overnight 3× SPXL',
  description: 'When VVIX (vol-of-vol) z-score ≥ +2.0 — extreme "fear about fear" — buy 3× SPXL at that EOD 15:55 ET and exit next session 15:55 ET. Long-only.',
  family: 'volatility-contrarian',
  cadence: 'intraday_plus_overnight',
  actionType: 'signal_trade',
  execution: regularSessionExecution({ startTime: '15:50', endTime: '15:55' }),
};

function resolveArtifactPath(variantId) {
  return path.join(ARTIFACTS_DIR, `${variantId}-report.json`);
}

function readReport(filePath) {
  if (!filePath || !fs.existsSync(filePath)) {
    throw new Error(`missing_vvix_spike_artifact:${filePath} — run build-vvix-spike-artifacts.js`);
  }
  return JSON.parse(fs.readFileSync(filePath, 'utf8'));
}

function createVvixSpikeContrarianStrategy(options = {}) {
  const artifactPath = options.artifactPath || resolveArtifactPath(VARIANT.id);
  const state = { report: null, loadedAt: null, refresh: null };

  function getMetadata() {
    const report = state.report;
    return {
      id: VARIANT.id,
      name: VARIANT.name,
      displayName: VARIANT.displayName,
      family: VARIANT.family,
      cadence: VARIANT.cadence,
      actionType: VARIANT.actionType,
      execution: VARIANT.execution,
      dataProvider: 'Massive indices_1m (VVIX closes) + SPY 1m bars',
      strategySource: 'VVIX z-score spike contrarian (overnight long)',
      description: VARIANT.description,
      ruleSummary: report?.metadata?.ruleSummary || ['Built from VVIX z-score; see artifact for full rule chain.'],
      sourceLinks: [
        { label: 'CBOE VVIX index', href: 'https://www.cboe.com/us/indices/dashboard/vvix/' },
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
      { label: 'build-vvix-spike-artifacts', command: process.execPath, args: [BUILD_SCRIPT_PATH] },
    ], recompute);
  }

  return { state, getMetadata, getReport, recompute, refreshData };
}

module.exports = {
  createVvixSpikeContrarianStrategy,
  VVIX_SPIKE_VARIANT_ID: VARIANT.id,
};
