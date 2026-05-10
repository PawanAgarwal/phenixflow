const fs = require('node:fs');
const path = require('node:path');

const { loadConfig, runtimePath } = require('../../../../projects/pym-v5-replication/src/config');
const { loadMassiveEnv } = require('../../../../projects/pym-v5-replication/src/env');
const {
  defaultScorePath,
  findLatestMassiveEodBarsPath,
} = require('../../../../projects/pym-v5-replication/src/rebalance-report');
const {
  buildExtensionRebalanceReport,
  mergeDailyBars,
  precomputeContext,
  strategySleeveMeta,
  strategySleeveMetaCap,
  strategyCreditSpread,
} = require('../../../../projects/pym-v5-replication/src/extension-strategies-suite');

const SLEEVE_META_RULE_SUMMARY = Object.freeze([
  'Evaluate the eight base PYM V5 sub-strategies independently each day.',
  'Score each sleeve by max(0, trailing N-day annualized Sharpe) of its own next-session realized returns.',
  'Reweight sleeves proportional to that score with a per-sleeve floor; merge holdings and renormalize.',
]);

const SLEEVE_META_CAP_RULE_SUMMARY = Object.freeze([
  'Evaluate the eight base PYM V5 sub-strategies independently each day.',
  'Score each sleeve by max(0, trailing N-day annualized Sharpe) of its own next-session realized returns.',
  'Allocate proportional to that score with no floor, but cap any single sleeve at the configured maximum weight; overflow redistributes to the other positive-Sharpe sleeves.',
]);

const CREDIT_OVERLAY_RULE_SUMMARY = Object.freeze([
  'Each EOD compute the HYG/LQD ratio and its 5-day return.',
  'Roll a 21-day mean and standard deviation of the 5-day return; compute a z-score.',
  'When the z-score is below -1, scale base PYM to 50% and add BIL for the rest; otherwise hold base PYM.',
]);

function findLatestExtraBarsPath() {
  const explicit = process.env.PYM_V5_EXTRA_BARS_PATH;
  if (explicit) return path.resolve(explicit);
  const root = runtimePath();
  if (!fs.existsSync(root)) return null;
  const matches = fs.readdirSync(root)
    .map((name) => {
      const match = name.match(/^pym-v5-extra-eod-daily-bars-(\d{4}-\d{2}-\d{2})-(\d{4}-\d{2}-\d{2})\.jsonl$/);
      return match ? { name, startDate: match[1], endDate: match[2] } : null;
    })
    .filter(Boolean)
    .sort((a, b) => a.endDate.localeCompare(b.endDate) || a.startDate.localeCompare(b.startDate));
  return matches.length ? path.join(root, matches.at(-1).name) : null;
}

function fileMetadata(filePath, prefix) {
  if (!filePath || !fs.existsSync(filePath)) return null;
  const basename = path.basename(filePath);
  const match = basename.match(new RegExp(`^${prefix}-(\\d{4}-\\d{2}-\\d{2})-(\\d{4}-\\d{2}-\\d{2})\\.jsonl$`));
  const stats = fs.statSync(filePath);
  return {
    path: filePath,
    name: basename,
    fetchStartDate: match?.[1] || null,
    endDate: match?.[2] || null,
    updatedAt: stats.mtime.toISOString(),
  };
}

function createExtensionStrategy({
  id,
  name,
  displayName,
  family,
  description,
  ruleSummary,
  buildStrategy,
  defaultStartDate,
  baseStrategyId = 'pym-v5',
  needsExtraBars = false,
}) {
  loadMassiveEnv();
  const config = loadConfig();
  const state = { report: null, loadedAt: null };

  function getMetadata() {
    return {
      id,
      name,
      displayName: displayName || name,
      family,
      cadence: 'daily_eod',
      actionType: 'rebalance',
      dataProvider: needsExtraBars ? 'Massive adjusted EOD (PYM universe + credit/sector ETFs)' : 'Massive adjusted EOD',
      strategySource: 'PYM V5 Composer tree plus extension overlay computed locally',
      description,
      ruleSummary,
      composerSymphonyId: config.source.composerSymphonyId,
      baseStrategyId,
      defaultStartDate: defaultStartDate || process.env.PYM_V5_REBALANCE_START || '2025-01-01',
      supports: ['chart', 'values', 'latest_portfolio', 'portfolio_change'],
    };
  }

  function resolvePaths() {
    return {
      scorePath: process.env.PYM_V5_SCORE_PATH || defaultScorePath(config),
      barsPath: process.env.PYM_V5_DAILY_BARS_PATH || findLatestMassiveEodBarsPath(),
      extraBarsPath: needsExtraBars ? (process.env.PYM_V5_EXTRA_BARS_PATH || findLatestExtraBarsPath()) : null,
    };
  }

  function recompute() {
    const { scorePath, barsPath, extraBarsPath } = resolvePaths();
    if (!scorePath || !fs.existsSync(scorePath)) throw new Error(`missing_score_snapshot:${scorePath}`);
    if (!barsPath || !fs.existsSync(barsPath)) throw new Error('missing_massive_eod_bars: mount runtime data or run pym-v5:massive-eod-build');
    if (needsExtraBars && (!extraBarsPath || !fs.existsSync(extraBarsPath))) {
      throw new Error('missing_extra_bars: run pym-v5:build-extra-eod first');
    }
    const score = JSON.parse(fs.readFileSync(scorePath, 'utf8'));
    const market = needsExtraBars ? mergeDailyBars(barsPath, extraBarsPath) : mergeDailyBars(barsPath, null);
    const ctx = precomputeContext(market, score, 'wilder');
    const strategy = buildStrategy();
    const startDate = defaultStartDate || process.env.PYM_V5_REBALANCE_START || '2025-01-01';
    const report = buildExtensionRebalanceReport({
      market,
      score,
      strategy,
      startDate,
      rsiMode: 'wilder',
      initialCapital: config.execution.initialCapital,
      transactionCostBps: config.execution.transactionCostBps,
      slippageBps: config.execution.slippageBps,
      ctx,
      source: {
        ...getMetadata(),
        scorePath,
        bars: fileMetadata(barsPath, 'pym-v5-massive-eod-adjusted-daily-bars'),
        extraBars: fileMetadata(extraBarsPath, 'pym-v5-extra-eod-daily-bars'),
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

  return { state, getMetadata, getReport, recompute };
}

function createPymV5SleeveMetaStrategy(options = {}) {
  const lookback = options.lookback || 21;
  const floor = options.floor ?? 0.05;
  return createExtensionStrategy({
    id: options.id || `pym-v5-sleeve-meta-${lookback}d`,
    name: options.name || `PYM Sleeve Meta ${lookback}d`,
    displayName: options.displayName || `PYM Sleeve Meta-Reweight (${lookback}d Sharpe)`,
    family: options.family || 'pym_sleeve_meta',
    description: options.description
      || `Reweights the eight base PYM V5 sub-strategies daily by their trailing ${lookback}-day realized Sharpe with a ${(floor * 100).toFixed(1)}% per-sleeve floor.`,
    ruleSummary: options.ruleSummary || SLEEVE_META_RULE_SUMMARY,
    defaultStartDate: options.startDate || '2025-01-02',
    buildStrategy: () => strategySleeveMeta({ lookback, floor }),
  });
}

function createPymV5SleeveMetaCapStrategy(options = {}) {
  const lookback = options.lookback || 21;
  const maxWeight = options.maxWeight ?? 0.25;
  const capPct = Math.round(maxWeight * 100);
  return createExtensionStrategy({
    id: options.id || `pym-v5-sleeve-meta-${lookback}d-cap${capPct}`,
    name: options.name || `PYM Sleeve Meta ${lookback}d Cap ${capPct}`,
    displayName: options.displayName || `PYM Sleeve Meta-Reweight (${lookback}d Sharpe, ${capPct}% cap)`,
    family: options.family || 'pym_sleeve_meta',
    description: options.description
      || `Reweights the eight base PYM V5 sub-strategies daily by their trailing ${lookback}-day realized Sharpe with no floor, capping any single sleeve at ${capPct}% so the portfolio always spreads across the strongest sleeves.`,
    ruleSummary: options.ruleSummary || SLEEVE_META_CAP_RULE_SUMMARY,
    defaultStartDate: options.startDate || '2025-01-02',
    buildStrategy: () => strategySleeveMetaCap({ lookback, maxWeight }),
  });
}

function createPymV5CreditOverlayStrategy(options = {}) {
  return createExtensionStrategy({
    id: options.id || 'pym-v5-credit-hyg-lqd-half-bil',
    name: options.name || 'PYM Credit HYG/LQD Half BIL',
    displayName: options.displayName || 'PYM Credit Risk-Off (HYG/LQD)',
    family: options.family || 'pym_credit_overlay',
    description: options.description
      || 'When the HYG/LQD 5-day return drops more than 1 standard deviation below its 21-day mean, scale base PYM to 50% and add BIL for the rest.',
    ruleSummary: options.ruleSummary || CREDIT_OVERLAY_RULE_SUMMARY,
    defaultStartDate: options.startDate || '2025-01-02',
    needsExtraBars: true,
    buildStrategy: () => strategyCreditSpread({ ratioPair: ['HYG', 'LQD'], threshold: -1, mode: 'half_bil' }),
  });
}

module.exports = {
  SLEEVE_META_RULE_SUMMARY,
  SLEEVE_META_CAP_RULE_SUMMARY,
  CREDIT_OVERLAY_RULE_SUMMARY,
  createPymV5SleeveMetaStrategy,
  createPymV5SleeveMetaCapStrategy,
  createPymV5CreditOverlayStrategy,
};
