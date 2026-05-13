// Strategy module for the OCC put/call contrarian intraday variants.
// Reads a pre-built artifact at
//   projects/spy-intraday-prediction/artifacts/{variantId}-report.json
// produced by scripts/build-occ-pc-contrarian-artifacts.js.
//
// refreshData() runs the upstream EOD inputs refresh (so the OCC P/C data and
// SPY 1m bars are current through today), then rebuilds the OCC P/C artifacts.

const fs = require('node:fs');
const path = require('node:path');

const { refreshEodInputsStep, runRefreshSequence } = require('./refresh-helpers');

const REPO_ROOT = path.resolve(__dirname, '..', '..', '..', '..');
const ARTIFACTS_DIR = path.join(REPO_ROOT, 'projects', 'spy-intraday-prediction', 'artifacts');
const BUILD_SCRIPT_PATH = path.join(
  REPO_ROOT, 'projects', 'spy-intraday-prediction', 'scripts', 'build-occ-pc-contrarian-artifacts.js',
);

const VARIANTS = {
  'occ-pc-contrarian-intraday-1x-long-only': {
    name: 'OCC Put/Call Intraday Contrarian (Long-Only 1x)',
    displayName: 'OCC P/C Intraday Contrarian — Long-Only 1×',
    description: 'When OCC equity put/call ratio z-score ≥ +2.0 (extreme put-buying = fear), go LONG SPY 9:35 → 15:55 ET. Long-only — the greed-side signal does not walk forward.',
    family: 'sentiment-contrarian',
    cadence: 'intraday',
    actionType: 'signal_trade',
  },
  'occ-pc-contrarian-intraday-3x': {
    name: 'OCC Put/Call Intraday Contrarian (3x SPXL/SPXU)',
    displayName: 'OCC P/C Intraday Contrarian — 3× SPXL/SPXU',
    description: 'When |OCC P/C z-score| ≥ 2.5, trade contrarian via 3× leveraged ETFs (SPXL for fear, SPXU for greed). 9:35 → 15:55 ET intraday only.',
    family: 'sentiment-contrarian',
    cadence: 'intraday',
    actionType: 'signal_trade',
  },
};

function resolveArtifactPath(variantId) {
  return path.join(ARTIFACTS_DIR, `${variantId}-report.json`);
}

function readReport(filePath) {
  if (!filePath || !fs.existsSync(filePath)) {
    throw new Error(`missing_occ_pc_contrarian_artifact:${filePath} — run build-occ-pc-contrarian-artifacts.js`);
  }
  return JSON.parse(fs.readFileSync(filePath, 'utf8'));
}

function createOccPcContrarianStrategy({ variantId, ...options } = {}) {
  if (!VARIANTS[variantId]) throw new Error(`unknown_occ_pc_variant:${variantId}`);
  const spec = VARIANTS[variantId];
  const artifactPath = options.artifactPath || resolveArtifactPath(variantId);
  const state = {
    report: null,
    loadedAt: null,
    refresh: null,
  };

  function getMetadata() {
    const report = state.report;
    return {
      id: variantId,
      name: spec.name,
      displayName: spec.displayName,
      family: spec.family,
      cadence: spec.cadence,
      actionType: spec.actionType,
      dataProvider: 'OCC EOD equity put/call ratio + Massive SPY 1m bars',
      strategySource: 'Contrarian z-score on rolling 20-day OCC equity P/C ratio',
      description: spec.description,
      ruleSummary: report?.metadata?.ruleSummary || ['Built from OCC EOD P/C ratio z-score; see artifact for full rule chain.'],
      sourceLinks: [
        { label: 'OCC Daily Statistics', href: 'https://marketdata.theocc.com/daily-open-interest' },
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
        label: 'build-occ-pc-contrarian-artifacts',
        command: process.execPath,
        args: [BUILD_SCRIPT_PATH],
      },
    ], recompute);
  }

  return { state, getMetadata, getReport, recompute, refreshData };
}

module.exports = {
  createOccPcContrarianStrategy,
  OCC_PC_VARIANTS: Object.keys(VARIANTS),
};
