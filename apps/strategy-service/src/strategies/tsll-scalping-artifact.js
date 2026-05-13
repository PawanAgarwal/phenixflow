const fs = require('node:fs');
const path = require('node:path');

const { executionSummaryForStrategy } = require('./execution');

const REPO_ROOT = path.resolve(__dirname, '..', '..', '..', '..');
const DEFAULT_REPORT_DIR = 'projects/tsll-scalping/reports';
const DEFAULT_REPORT_PATH = 'projects/tsll-scalping/reports/tsll-seconds-passive-fixed-feb2026.json';
const DEFAULT_REFRESH_START = '2025-01-02';

function resolvePath(filePath) {
  if (!filePath) return null;
  return path.isAbsolute(filePath) ? filePath : path.join(REPO_ROOT, filePath);
}

function finite(value, fallback = 0) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function round(value, digits = 6) {
  return Number.isFinite(value) ? Number(value.toFixed(digits)) : value;
}

function fileMetadata(filePath) {
  if (!filePath || !fs.existsSync(filePath)) return null;
  const stats = fs.statSync(filePath);
  return {
    path: filePath,
    name: path.basename(filePath),
    updatedAt: stats.mtime.toISOString(),
  };
}

function readJson(filePath) {
  if (!filePath || !fs.existsSync(filePath)) throw new Error(`missing_tsll_scalping_report:${filePath}`);
  return JSON.parse(fs.readFileSync(filePath, 'utf8'));
}

function findLatestTsllReportPath() {
  const explicit = process.env.TSLL_SECONDS_PASSIVE_REPORT_PATH;
  if (explicit) return resolvePath(explicit);
  const dir = resolvePath(DEFAULT_REPORT_DIR);
  if (!fs.existsSync(dir)) return resolvePath(DEFAULT_REPORT_PATH);
  const matches = fs.readdirSync(dir)
    .map((name) => {
      const match = name.match(/^tsll-seconds-passive-fixed-(\d{4}-\d{2}-\d{2})-(\d{4}-\d{2}-\d{2})\.json$/);
      return match ? { name, startDate: match[1], endDate: match[2] } : null;
    })
    .filter((item) => item && item.startDate <= DEFAULT_REFRESH_START)
    .sort((left, right) => right.endDate.localeCompare(left.endDate)
      || left.startDate.localeCompare(right.startDate)
      || right.name.localeCompare(left.name));
  return matches.length ? path.join(dir, matches[0].name) : resolvePath(DEFAULT_REPORT_PATH);
}

function maxDrawdown(equitySeries) {
  let peak = equitySeries[0]?.equity || 1;
  let drawdown = 0;
  equitySeries.forEach((point) => {
    peak = Math.max(peak, point.equity);
    if (peak > 0) drawdown = Math.min(drawdown, (point.equity / peak) - 1);
  });
  return drawdown;
}

function holding(equity) {
  return {
    ticker: 'TSLL',
    weight: 1,
    weightPct: 100,
    dollars: equity,
  };
}

function buildReportFromTsllArtifact({ metadata, reportPath, initialCapital }) {
  const artifact = readJson(reportPath);
  const seedCapital = finite(initialCapital, finite(artifact.totals?.avgEntry) * 1000 || 10000);
  let equity = seedCapital;
  const snapshots = [];
  const equitySeries = [];

  (artifact.days || []).forEach((day, index, days) => {
    const startEquity = equity;
    const pnl = finite(day.pnlPer1000Shares);
    const netReturn = startEquity > 0 ? pnl / startEquity : 0;
    equity = startEquity + pnl;
    const nextDate = days[index + 1]?.date || null;
    snapshots.push({
      date: day.date,
      nextDate,
      equityBeforeNextSession: startEquity,
      grossExposure: 1,
      turnover: index === 0 ? 1 : 0,
      turnoverPct: index === 0 ? 100 : 0,
      topHoldings: 'TSLL intraday scalps',
      benchmarkReturns: { spy: 0, qqq: 0 },
      holdings: [holding(startEquity)],
      realized: {
        netReturn,
        netReturnPct: netReturn * 100,
        endEquity: equity,
        pnlDollars: pnl,
        trades: day.trades,
        winRate: day.winRate,
        netCents: day.netCents,
      },
    });
    equitySeries.push({
      date: day.date,
      signalDate: day.date,
      equity,
      dailyReturn: netReturn,
      totalReturn: (equity / seedCapital) - 1,
      spyReturn: 0,
      qqqReturn: 0,
    });
  });

  const totalReturn = equity / seedCapital - 1;
  return {
    generatedAt: new Date().toISOString(),
    source: {
      provider: artifact.provider || 'Massive',
      report: fileMetadata(reportPath),
      sourceArtifact: artifact.sourceArtifact,
      data: artifact.assumptions?.data,
      caveat: artifact.assumptions?.caveat,
    },
    settings: {
      ...artifact.strategy?.settings,
      explicitCostCentsPerSide: artifact.assumptions?.explicitCostCentsPerSide ?? 0,
      initialCapital: seedCapital,
      shareBlock: 1000,
    },
    summary: {
      latestRebalanceDate: snapshots.at(-1)?.date || null,
      snapshots: snapshots.length,
      initialCapital: seedCapital,
      finalEquity: equity,
      totalReturn,
      totalReturnPct: totalReturn * 100,
      spyReturn: 0,
      qqqReturn: 0,
      trades: artifact.totals?.trades || 0,
      positiveDays: artifact.totals?.winningDays || 0,
      tradedDays: artifact.totals?.days || 0,
      winRate: artifact.totals?.winRate || 0,
      netCents: artifact.totals?.netCents || 0,
      avgNetCents: artifact.totals?.avgNetCents || 0,
      pnlPer1000Shares: artifact.totals?.pnlPer1000Shares || 0,
      returnOnBuyTurnover: artifact.totals?.returnOnBuyTurnover || 0,
      returnOnRecycledCapital: artifact.totals?.returnOnRecycledCapital || 0,
      maxDrawdown: round(maxDrawdown(equitySeries), 6),
    },
    latest: snapshots.at(-1) || null,
    snapshots,
    equitySeries,
    skippedDays: [],
    metadata,
  };
}

function createTsllSecondsPassiveScalperStrategy(options = {}) {
  const configuredReportPath = options.reportPath ? resolvePath(options.reportPath) : null;
  const metadata = {
    id: options.id || 'tsll-seconds-passive-scalper',
    name: options.name || 'TSLL Intraday Seconds Passive Scalper',
    displayName: options.displayName || 'TSLL Intraday Seconds Scalp',
    family: 'tsll_scalping',
    cadence: 'intraday_daily_report',
    actionType: 'intraday_scalp',
    execution: executionSummaryForStrategy('tsll-seconds-passive-scalper'),
    dataProvider: 'Massive historical/live bars plus Massive REST 1-second aggregates when stock-trade files are unavailable',
    strategySource: 'TSLL seconds passive limit scalping artifact',
    description: options.description || 'Tracks the TSLL passive limit scalp candidate: buy 3 cents below the prior completed 1-second close, target +3 cents, stop 5 cents, max hold 10 seconds.',
    ruleSummary: [
      'Use only completed 1-second TSLL bars and causal 1-minute SPY/QQQ/TSLA context.',
      'Place buy limit 3c below the prior completed second close when 60s range and market filters pass.',
      'Exit at +3c target, 5c stop, or after 10 seconds; same-second target/stop assumes stop first.',
      'Dashboard report is a seconds-bar proxy; quote/NBBO queue priority still needs validation.',
    ],
    sourceLinks: [],
    defaultStartDate: options.defaultStartDate || DEFAULT_REFRESH_START,
    supports: ['chart', 'values', 'latest_portfolio', 'portfolio_change', 'refresh_data'],
  };
  const state = {
    report: null,
    loadedAt: null,
    refresh: null,
  };

  function getMetadata() {
    return metadata;
  }

  function getReport() {
    if (!state.report) {
      state.report = buildReportFromTsllArtifact({
        metadata,
        reportPath: configuredReportPath || findLatestTsllReportPath(),
        initialCapital: options.initialCapital,
      });
      state.loadedAt = new Date().toISOString();
    }
    return state.report;
  }

  function recompute() {
    state.report = buildReportFromTsllArtifact({
      metadata,
      reportPath: configuredReportPath || findLatestTsllReportPath(),
      initialCapital: options.initialCapital,
    });
    state.loadedAt = new Date().toISOString();
    return state.report;
  }

  function refreshData() {
    const { runRefreshSequence } = require('./refresh-helpers');
    return runRefreshSequence(state, [{
      label: 'refresh-tsll-second-passive-report',
      command: process.execPath,
      args: [
        path.join(REPO_ROOT, 'projects', 'tsll-scalping', 'scripts', 'refresh-second-passive-report.js'),
        '--start-date', process.env.TSLL_SECONDS_PASSIVE_START || DEFAULT_REFRESH_START,
        '--end-date', process.env.TSLL_SECONDS_PASSIVE_END || 'auto',
      ],
    }], recompute);
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
  buildReportFromTsllArtifact,
  createTsllSecondsPassiveScalperStrategy,
};
