// Strategy module for the Cross-Asset Trend strategy with a daily Breadth EMA-50 exposure gate.
//
// Selection (monthly): top-20 assets by 12-month (252d) total-return momentum that are above
// their 200d SMA, inverse-volatility weighted (30% cap), remainder to cash.
// Gate (daily): breadth = % of the asset universe trading above its own EMA-50; total exposure
// ramps linearly from 0% (full cash) when breadth <= 18% to 100% when breadth >= 50%.
//
// Research + verification: research/asset-trend-strategy (verify_breadth_ema50.py reconciles two
// independent engines to ~3e-4 and audits for lookahead; daily 1-day-lagged execution).

const fs = require('node:fs');
const path = require('node:path');

const { dailyEodExecution } = require('./execution');

const REPO_ROOT = path.resolve(__dirname, '..', '..', '..', '..');
const ARTIFACT_PATH = path.join(
  REPO_ROOT, 'projects', 'asset-trend-breadth', 'artifacts', 'breadth-ema50-report.json',
);
const GENERATOR = path.join(
  REPO_ROOT, 'research', 'asset-trend-strategy', 'export_artifact.py',
);

const VARIANT = {
  id: 'asset-trend-breadth-ema50',
  name: 'Cross-Asset Trend — Breadth EMA-50 Gated',
  displayName: 'Cross-Asset Trend — Breadth EMA-50 Gated',
  family: 'cross-asset-momentum',
  cadence: 'daily_eod',
  actionType: 'portfolio_rebalance',
  execution: dailyEodExecution({ time: '16:10' }),
  description:
    'Holds the top-20 asset classes by 12-month momentum (above 200d SMA, inverse-vol, monthly), '
    + 'scaled daily by a cross-asset breadth gate (% of the universe above its EMA-50): exposure '
    + 'ramps from 0% cash below 18% breadth to 100% at 50%. Beats SPY Sharpe (~1.10 vs 0.72 OOS) '
    + 'with much smaller drawdown; verified for lookahead and walk-forward.',
};

function readReport(filePath) {
  if (!filePath || !fs.existsSync(filePath)) {
    throw new Error(`missing_breadth_ema50_artifact:${filePath} — run research/asset-trend-strategy/export_artifact.py`);
  }
  return JSON.parse(fs.readFileSync(filePath, 'utf8'));
}

function createBreadthEma50Strategy(options = {}) {
  const artifactPath = options.artifactPath || ARTIFACT_PATH;
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
      dataProvider: 'Yahoo Finance daily adjusted close — research/asset-universe (~245 cross-asset ETFs)',
      strategySource: '12-month cross-asset momentum + daily breadth (EMA-50) exposure gate',
      description: VARIANT.description,
      ruleSummary: [
        report?.settings?.selection || 'Top-20 by 12-month momentum, >200d SMA, inverse-vol, monthly.',
        report?.settings?.gate || 'Daily breadth EMA-50 exposure ramp 18%->50% (0% cash to fully invested).',
        `Costs: ${report?.settings?.costBps ?? 5} bps one-way; risk-free = cash (BIL).`,
      ],
      defaultStartDate: '2019-07-01',
      artifactPath,
      supports: ['chart', 'values', 'latest_portfolio', 'portfolio_change', 'open_positions', 'sharpe'],
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

  return {
    state, getMetadata, getReport, recompute,
    generatorPath: GENERATOR,
  };
}

module.exports = {
  createBreadthEma50Strategy,
  BREADTH_EMA50_VARIANT_ID: VARIANT.id,
};
