// Strategy module for the SPY gap-down-fade intraday variant.

const fs = require('node:fs');
const path = require('node:path');

const { regularSessionExecution } = require('./execution');
const { refreshEodInputsStep, runRefreshSequence } = require('./refresh-helpers');

const REPO_ROOT = path.resolve(__dirname, '..', '..', '..', '..');
const ARTIFACTS_DIR = path.join(REPO_ROOT, 'projects', 'spy-intraday-prediction', 'artifacts');
const BUILD_SCRIPT_PATH = path.join(
  REPO_ROOT, 'projects', 'spy-intraday-prediction', 'scripts', 'build-gap-down-fade-artifacts.js',
);

const VARIANT = {
  id: 'gap-down-fade-intraday-3x',
  name: 'SPY Gap-Down Fade (Intraday, 3×)',
  displayName: 'SPY Gap-Down Fade — Intraday 3× SPXL',
  description: 'When SPY opens ≥ 0.5% below the prior close, buy 3× SPXL at 09:35 ET and exit 15:55 ET. Long-only intraday mean-reversion.',
  family: 'mean-reversion',
  cadence: 'intraday',
  actionType: 'signal_trade',
  execution: regularSessionExecution({ startTime: '09:35', endTime: '15:55' }),
};

function resolveArtifactPath(variantId) {
  return path.join(ARTIFACTS_DIR, `${variantId}-report.json`);
}

function readReport(filePath) {
  if (!filePath || !fs.existsSync(filePath)) {
    throw new Error(`missing_gap_down_fade_artifact:${filePath} — run build-gap-down-fade-artifacts.js`);
  }
  return JSON.parse(fs.readFileSync(filePath, 'utf8'));
}

function createGapDownFadeStrategy(options = {}) {
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
      dataProvider: 'Massive EOD adjusted daily bars (SPY open/close) + SPY 1m bars',
      strategySource: 'Pre-market gap fade — long-only intraday mean reversion',
      description: VARIANT.description,
      ruleSummary: report?.metadata?.ruleSummary || ['Built from SPY open/prior_close gap; see artifact for full rule chain.'],
      sourceLinks: [],
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
      { label: 'build-gap-down-fade-artifacts', command: process.execPath, args: [BUILD_SCRIPT_PATH] },
    ], recompute);
  }

  return { state, getMetadata, getReport, recompute, refreshData };
}

module.exports = {
  createGapDownFadeStrategy,
  GAP_DOWN_FADE_VARIANT_ID: VARIANT.id,
};
