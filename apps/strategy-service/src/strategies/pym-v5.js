const fs = require('node:fs');
const path = require('node:path');

const { loadConfig } = require('../../../../projects/pym-v5-replication/src/config');
const { readDailyBarsJsonl } = require('../../../../projects/pym-v5-replication/src/backtest');
const { loadMassiveEnv } = require('../../../../projects/pym-v5-replication/src/env');
const { dailyEodExecution } = require('./execution');
const { refreshEodInputsStep, runRefreshSequence } = require('./refresh-helpers');
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

function composerSourceUrl(config) {
  const baseUrl = String(config.source.composerApiBaseUrl || config.source.composerApiBase || '').replace(/\/$/, '');
  if (!baseUrl || !config.source.composerSymphonyId) return null;
  return `${baseUrl}/api/v1/public/symphonies/${config.source.composerSymphonyId}/score`;
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
    const sourceLinks = [
      { label: 'Original Study / Notion', href: config.source.notionUrl },
      { label: 'Composer Factsheet', href: config.source.composerFactsheetUrl },
      { label: 'Composer Source', href: composerSourceUrl(config) },
    ].filter((link) => link.href);
    return {
      id: 'pym-v5',
      name: 'PYM V5',
      displayName: "Eagle's Park your Money V5",
      family: 'composer',
      cadence: 'daily_eod',
      actionType: 'rebalance',
      execution: dailyEodExecution(),
      dataProvider: 'Massive adjusted EOD',
      strategySource: 'Composer public tree',
      description: 'Replicates the Composer PYM V5 daily ETF strategy from the public symphony tree using Massive adjusted EOD data.',
      ruleSummary: [
        'At each market close: evaluate the replicated PYM V5 Composer tree.',
        'Hold the target ETF weights through the next close, with normal rebalance costs applied.',
      ],
      sourceLinks,
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

  function refreshData() {
    return runRefreshSequence(state, [refreshEodInputsStep()], recompute);
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
