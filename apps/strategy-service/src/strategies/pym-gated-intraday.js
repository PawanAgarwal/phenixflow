// Strategy module for the 4 PYM-gated intraday variants.
// Each strategy reads a pre-built artifact at
//   projects/spy-intraday-prediction/artifacts/{variantId}-report.json
// produced by scripts/build-pym-gated-artifacts.js.
//
// refreshData() runs the upstream PYM EOD refresh, then rebuilds the artifact, then recomputes.

const fs = require('node:fs');
const path = require('node:path');

const { refreshEodInputsStep, runRefreshSequence } = require('./refresh-helpers');

const REPO_ROOT = path.resolve(__dirname, '..', '..', '..', '..');
const ARTIFACTS_DIR = path.join(REPO_ROOT, 'projects', 'spy-intraday-prediction', 'artifacts');
const BUILD_SCRIPT_PATH = path.join(
  REPO_ROOT, 'projects', 'spy-intraday-prediction', 'scripts', 'build-pym-gated-artifacts.js',
);
// Also re-run the PYM v5 backtest so the artifact incorporates the latest EOD bar.
const PYM_BACKTEST_SCRIPT_PATH = path.join(
  REPO_ROOT, 'projects', 'pym-v5-replication', 'scripts', 'run-backtest.js',
);

const VARIANTS = {
  'pym-gated-baseline': {
    name: 'PYM-Gated SPY Intraday Baseline',
    displayName: 'PYM-Gated SPY Intraday (1x Baseline)',
    description: 'PYM v5 daily bias drives SPY direction; enter SPY at 11:30 ET when |bias| >= 0.20; exit at 15:55 ET.',
    family: 'pym-intraday',
    cadence: 'intraday',
    actionType: 'signal_trade',
  },
  'pym-gated-lev3x': {
    name: 'PYM-Gated 3x Intraday (SPXL/SPXU)',
    displayName: 'PYM-Gated 3x Intraday',
    description: 'Same PYM bias gate using 3x leveraged ETFs (SPXL long / SPXU short) for amplified intraday exposure.',
    family: 'pym-intraday',
    cadence: 'intraday',
    actionType: 'signal_trade',
  },
  'pym-gated-overnight-1x': {
    name: 'PYM-Gated SPY 1x + Overnight on Extreme',
    displayName: 'PYM-Gated 1x SPY + Overnight',
    description: 'PYM bias gates SPY trades; when |bias| >= 0.40 enter at prior 15:55 and hold through next 15:55 (captures gap).',
    family: 'pym-intraday',
    cadence: 'intraday_plus_overnight',
    actionType: 'signal_trade',
  },
  'pym-gated-best-combo': {
    name: 'PYM-Gated 3x + Overnight Production',
    displayName: 'PYM-Gated 3x + Overnight (Production)',
    description: '3x leverage with overnight-on-extreme: TQQQ/SQQQ overnight when |bias|>=0.30, SPXL/SPXU intraday for moderate bias. Matches PYM\'s 16-month risk-adjusted return.',
    family: 'pym-intraday',
    cadence: 'intraday_plus_overnight',
    actionType: 'signal_trade',
  },
};

function resolveArtifactPath(variantId) {
  return path.join(ARTIFACTS_DIR, `${variantId}-report.json`);
}

function readReport(filePath) {
  if (!filePath || !fs.existsSync(filePath)) {
    throw new Error(`missing_pym_gated_artifact:${filePath} — run build-pym-gated-artifacts.js`);
  }
  return JSON.parse(fs.readFileSync(filePath, 'utf8'));
}

function createPymGatedIntradayStrategy({ variantId, ...options } = {}) {
  if (!VARIANTS[variantId]) throw new Error(`unknown_pym_gated_variant:${variantId}`);
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
      dataProvider: 'Massive stock_quotes_1m + PYM v5 EOD signal',
      strategySource: 'PYM v5 bias as directional gate; deterministic intraday rule',
      description: spec.description,
      ruleSummary: report?.metadata?.ruleSummary || [
        'Built from PYM v5 daily bias signal. See artifact for full rule chain.',
      ],
      sourceLinks: [
        { label: 'PYM v5 strategy doc', href: 'https://composer.trade' },
        { label: 'Strategy writeup', href: 'projects/spy-intraday-prediction/OPTION_FLOW_STRATEGIES.md' },
      ],
      defaultStartDate: report?.settings ? report.summary.startDate : '2025-01-02',
      artifactPath,
      supports: ['chart', 'values', 'latest_portfolio', 'portfolio_change', 'refresh_data', 'trade_log', 'sharpe'],
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
    // Three-step pipeline:
    //   1. Refresh upstream PYM EOD inputs (stock bars).
    //   2. Re-run PYM v5 backtest so its artifact reflects today's signal.
    //   3. Rebuild all 4 PYM-gated artifacts (we run the builder once; it does all variants).
    // The third step writes the variant artifact we'll read in recompute().
    return runRefreshSequence(state, [
      refreshEodInputsStep(),
      {
        label: 'rerun-pym-v5-backtest',
        command: process.execPath,
        args: [PYM_BACKTEST_SCRIPT_PATH, '--start', '2025-01-02'],
      },
      {
        label: 'build-pym-gated-artifacts',
        command: process.execPath,
        args: [BUILD_SCRIPT_PATH, '--start', '2025-01-02'],
      },
    ], recompute);
  }

  return { state, getMetadata, getReport, recompute, refreshData };
}

module.exports = {
  createPymGatedIntradayStrategy,
  PYM_GATED_VARIANTS: Object.keys(VARIANTS),
};
