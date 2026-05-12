const fs = require('node:fs');
const path = require('node:path');

const REPO_ROOT = path.resolve(__dirname, '..', '..', '..', '..');
const DEFAULT_REPORT_DIR = 'projects/spy-intraday-prediction/artifacts';
const DEFAULT_REPORT_PATH = 'projects/spy-intraday-prediction/artifacts/wheel-expanded-backtest-2026-01-02-2026-04-27.json';
const DEFAULT_ARTIFACT_STRATEGY_ID = 'wheel_weekly_10otm_trend_ivrv_profit50';
const DEFAULT_INITIAL_CAPITAL = 1_000_000;
const DEFAULT_REFRESH_START = '2025-01-02';

const RULE_SUMMARY = Object.freeze([
  'Sell weekly 5-10 DTE puts roughly 10% OTM across the liquid local universe.',
  'Require prior uptrend, positive 20-day return, and derived option IV/RV >= 1.10.',
  'Close short options at 50% profit; keep assigned shares and sell covered calls when eligible.',
]);

function resolvePath(filePath) {
  return path.isAbsolute(filePath) ? filePath : path.join(REPO_ROOT, filePath);
}

function finite(value, fallback = 0) {
  const number = Number(value);
  return Number.isFinite(number) ? number : fallback;
}

function pct(value) {
  return Number.isFinite(value) ? value * 100 : null;
}

function readJson(filePath) {
  if (!filePath || !fs.existsSync(filePath)) throw new Error(`missing_wheel_option_income_artifact:${filePath}`);
  return JSON.parse(fs.readFileSync(filePath, 'utf8'));
}

function findLatestWheelReportPath() {
  const explicit = process.env.WHEEL_OPTION_INCOME_REPORT_PATH;
  if (explicit) return resolvePath(explicit);
  const dir = resolvePath(DEFAULT_REPORT_DIR);
  if (!fs.existsSync(dir)) return resolvePath(DEFAULT_REPORT_PATH);
  const matches = fs.readdirSync(dir)
    .map((name) => {
      const match = name.match(/^wheel-(?:expanded-)?backtest-(\d{4}-\d{2}-\d{2})-(\d{4}-\d{2}-\d{2})\.json$/);
      return match ? { name, startDate: match[1], endDate: match[2] } : null;
    })
    .filter((item) => item && item.startDate <= DEFAULT_REFRESH_START)
    .sort((left, right) => right.endDate.localeCompare(left.endDate)
      || left.startDate.localeCompare(right.startDate)
      || right.name.localeCompare(left.name));
  return matches.length ? path.join(dir, matches[0].name) : resolvePath(DEFAULT_REPORT_PATH);
}

function maxDrawdown(points) {
  let peak = points[0]?.equity || 1;
  let drawdown = 0;
  points.forEach((point) => {
    peak = Math.max(peak, point.equity);
    if (peak > 0) drawdown = Math.min(drawdown, point.equity / peak - 1);
  });
  return drawdown;
}

function benchmarkReturn(sourceReport, symbol, index, count) {
  const series = sourceReport.benchmarkSeries?.[symbol] || sourceReport.benchmarkSeries?.[symbol.toUpperCase()];
  const point = series?.[index];
  if (Number.isFinite(point?.totalReturn)) return point.totalReturn;
  const total = sourceReport.benchmarks?.[symbol]?.totalReturn ?? sourceReport.benchmarks?.[symbol.toUpperCase()]?.totalReturn;
  return Number.isFinite(total) && count > 1 ? total * (index / (count - 1)) : null;
}

function holdingsFromDaily(row) {
  const equity = Math.max(1, finite(row.equity, DEFAULT_INITIAL_CAPITAL));
  const cashWeight = Math.max(0, finite(row.cash) / equity);
  const stockWeight = Math.max(0, finite(row.stockValue) / equity);
  const optionWeight = Math.max(0, finite(row.optionLiability) / equity);
  return [
    {
      ticker: 'CASH',
      weight: cashWeight,
      weightPct: pct(cashWeight),
      dollars: finite(row.cash),
    },
    {
      ticker: 'ASSIGNED_STOCK',
      weight: stockWeight,
      weightPct: pct(stockWeight),
      dollars: finite(row.stockValue),
    },
    {
      ticker: 'SHORT_OPTION_MARK',
      weight: optionWeight,
      weightPct: pct(optionWeight),
      dollars: finite(row.optionLiability),
    },
  ].filter((holding) => holding.weight > 0.000001);
}

function topHoldingsLabel(holdings) {
  return holdings.slice(0, 3).map((holding) => holding.ticker).join(', ');
}

// Pair sell_put / sell_call (opens) with their close events (buy_to_close, expired_worthless,
// put_assignment, call_assignment, put_assignment_liquidation) into trade records keyed by
// contract ticker. Anything still open at the source report's end date becomes an open position.
function buildTradesAndOpenPositions({ sourceReport, artifactStrategyId }) {
  const raw = (sourceReport.tradesByStrategy?.[artifactStrategyId]) || [];
  const opensByTicker = new Map();
  const closedTrades = [];
  for (const ev of raw) {
    if (!ev || !ev.type || !ev.date) continue;
    if (ev.type === 'sell_put' || ev.type === 'sell_call') {
      opensByTicker.set(ev.ticker, ev);
      continue;
    }
    const opener = opensByTicker.get(ev.ticker);
    if (!opener) continue; // close without matching open (shouldn't happen, but skip safely)
    opensByTicker.delete(ev.ticker);
    closedTrades.push({
      ticker: ev.ticker,
      symbol: opener.symbol,
      right: opener.right,
      strike: opener.strike,
      contracts: opener.contracts,
      type: opener.type === 'sell_put' ? 'cash_secured_put' : 'covered_call',
      side: 'SHORT', // selling premium
      entryDate: opener.date,
      exitDate: ev.date,
      exitType: ev.type,
      // Carry-over: trade crosses overnight (entry on a prior calendar day) — true for ~all wheel
      // trades, which is exactly why distinguishing today's activity matters in the UI.
      carryOver: opener.date !== ev.date,
      entryMode: opener.date === ev.date ? 'intraday' : 'overnight',
      bias: null,
      entryPrice: finite(opener.entryPrice),
      exitPrice: finite(ev.exitPrice ?? ev.intrinsic),
      grossReturn: finite(ev.profit) / Math.max(1, Math.abs(finite(opener.grossPremium) || 1)),
      cost: finite(opener.commission) + finite(ev.commission),
      netReturn: null, // wheel P&L tracked at portfolio level; this is informational per-trade
      // Raw event payloads for the UI to render contract details
      open: opener,
      close: ev,
    });
  }
  // Whatever is still in opensByTicker at the end is an open position as of endDate.
  const openPositions = Array.from(opensByTicker.values()).map((opener) => ({
    ticker: opener.ticker,
    symbol: opener.symbol,
    right: opener.right,
    strike: opener.strike,
    expiration: opener.expiration,
    contracts: opener.contracts,
    type: opener.type === 'sell_put' ? 'cash_secured_put' : 'covered_call',
    side: 'SHORT',
    entryDate: opener.date,
    entryPrice: finite(opener.entryPrice),
    grossPremium: finite(opener.grossPremium),
    impliedVol: finite(opener.impliedVol),
    delta: finite(opener.delta),
    dte: opener.dte,
  }));
  return { trades: closedTrades, openPositions };
}

function buildWheelReport({ metadata, sourceReport, artifactStrategyId }) {
  const summary = sourceReport.strategies.find((entry) => entry.id === artifactStrategyId);
  const daily = sourceReport.dailyByStrategy?.[artifactStrategyId] || [];
  if (!summary || !daily.length) throw new Error(`missing_wheel_option_income_strategy:${artifactStrategyId}`);

  const initialCapital = sourceReport.initialCapital || summary.initialCapital || DEFAULT_INITIAL_CAPITAL;
  const equitySeries = daily.map((row, index) => ({
    date: row.date,
    signalDate: row.date,
    equity: finite(row.equity, initialCapital),
    dailyReturn: finite(row.dailyReturn),
    totalReturn: (finite(row.equity, initialCapital) / initialCapital) - 1,
    spyReturn: benchmarkReturn(sourceReport, 'SPY', index, daily.length),
    qqqReturn: benchmarkReturn(sourceReport, 'QQQ', index, daily.length),
  }));

  const snapshots = daily.map((row, index) => {
    const previous = daily[index - 1] || null;
    const holdings = holdingsFromDaily(row);
    return {
      date: row.date,
      rebalanceDate: row.date,
      execution: 'option_1m_entry_window',
      nextDate: daily[index + 1]?.date || null,
      equityBeforeNextSession: finite(row.equity, initialCapital),
      grossExposure: holdings.reduce((sum, holding) => sum + holding.weight, 0),
      turnover: finite(row.entries) + finite(row.closures) + finite(row.expirations),
      turnoverPct: null,
      estimatedRebalanceCost: null,
      estimatedRebalanceCostPct: null,
      holdings,
      topHoldings: topHoldingsLabel(holdings),
      benchmarkReturns: {
        spy: equitySeries[index].spyReturn,
        qqq: equitySeries[index].qqqReturn,
      },
      optionIncomeDiagnostics: {
        openShorts: row.openShorts,
        openPutContracts: row.openPutContracts,
        openCallContracts: row.openCallContracts,
        reservedCollateral: row.reservedCollateral,
        optionLiability: row.optionLiability,
        assignedShareSymbols: row.shareSymbols,
      },
      realized: {
        date: row.date,
        startEquity: finite(previous?.equity, initialCapital),
        endEquity: finite(row.equity, initialCapital),
        grossReturn: finite(row.dailyReturn),
        grossReturnPct: pct(row.dailyReturn),
        netReturn: finite(row.dailyReturn),
        netReturnPct: pct(row.dailyReturn),
        costReturn: null,
        costReturnPct: null,
        missingReturnCount: 0,
      },
    };
  });

  const finalEquity = equitySeries.at(-1)?.equity || initialCapital;
  return {
    generatedAt: new Date().toISOString(),
    source: {
      provider: 'Massive stock_quotes_1m + option_quotes_1m',
      reportPath: sourceReport.sourcePath || null,
      artifactStrategyId,
      generatedAt: sourceReport.generatedAt,
    },
    settings: {
      startDate: sourceReport.startDate,
      endDate: sourceReport.endDate,
      initialCapital,
      execution: sourceReport.execution,
      assumptions: sourceReport.assumptions,
    },
    summary: {
      latestRebalanceDate: snapshots.at(-1)?.date || null,
      latestCompletedDate: snapshots.at(-1)?.date || null,
      snapshots: snapshots.length,
      completedSessions: snapshots.length,
      finalEquity,
      totalReturn: finalEquity / initialCapital - 1,
      totalReturnPct: pct(finalEquity / initialCapital - 1),
      maxDrawdown: maxDrawdown(equitySeries),
      maxDrawdownPct: pct(maxDrawdown(equitySeries)),
      spyReturn: sourceReport.benchmarks?.SPY?.totalReturn ?? null,
      qqqReturn: sourceReport.benchmarks?.QQQ?.totalReturn ?? null,
      sellTradeCount: summary.sellTradeCount,
      buyToCloseCount: summary.buyToCloseCount,
      assignments: summary.putAssignments + summary.putAssignmentLiquidations + summary.callAssignments,
      premiumCollected: summary.premiumCollected,
      buybackCost: summary.buybackCost,
    },
    latest: snapshots.at(-1) || null,
    snapshots,
    equitySeries,
    trades: buildTradesAndOpenPositions({ sourceReport, artifactStrategyId }).trades,
    openPositions: buildTradesAndOpenPositions({ sourceReport, artifactStrategyId }).openPositions,
    skippedDays: [],
    metadata,
  };
}

function createWheelOptionIncomeStrategy(options = {}) {
  const configuredReportPath = options.reportPath ? resolvePath(options.reportPath) : null;
  const artifactStrategyId = options.artifactStrategyId || DEFAULT_ARTIFACT_STRATEGY_ID;
  const metadata = {
    id: options.id || 'option-income-wheel-trend-ivrv',
    name: options.name || 'Intraday Option Income Wheel (IV/RV Trend)',
    displayName: options.displayName || 'Intraday Option Income Wheel',
    family: 'intraday_option_income',
    cadence: 'intraday_plus_overnight',
    actionType: 'short_option_income',
    dataProvider: 'Massive stock and OPRA option 1m aggregates',
    strategySource: 'SPY intraday Massive-only wheel backtest artifact',
    description: 'Sells weekly cash-secured puts at intraday entry windows (5–10 DTE, ~10% OTM) when prior trend and IV/RV filters agree; closes winners at 50% profit; assigned shares roll into covered calls. Positions carry over multiple sessions.',
    ruleSummary: options.ruleSummary || RULE_SUMMARY,
    sourceLinks: [
      { label: 'OIC Cash-Secured Put', href: 'https://www.optionseducation.org/strategies/all-strategies/cash-secured-put' },
      { label: 'Cboe PutWrite VRP', href: 'https://www.cboe.com/insights/posts/white-paper-shows-volatility-risk-premium-facilitated-higher-risk-adjusted-returns-for-put-index/' },
    ],
    artifactStrategyId,
    defaultStartDate: DEFAULT_REFRESH_START,
    supports: ['chart', 'values', 'latest_portfolio', 'portfolio_change', 'refresh_data', 'trade_log', 'open_positions'],
  };
  const state = { report: null, loadedAt: null, refresh: null };

  function loadReport() {
    const reportPath = configuredReportPath || findLatestWheelReportPath();
    const sourceReport = readJson(reportPath);
    sourceReport.sourcePath = reportPath;
    return buildWheelReport({ metadata, sourceReport, artifactStrategyId });
  }

  function getMetadata() {
    return metadata;
  }

  function getReport() {
    if (!state.report) {
      state.report = loadReport();
      state.loadedAt = new Date().toISOString();
    }
    return state.report;
  }

  function recompute() {
    state.report = loadReport();
    state.loadedAt = new Date().toISOString();
    return state.report;
  }

  function refreshData() {
    const { runRefreshSequence } = require('./refresh-helpers');
    return runRefreshSequence(state, [{
      label: 'refresh-wheel-option-income',
      command: process.execPath,
      args: [
        path.join(REPO_ROOT, 'projects', 'spy-intraday-prediction', 'scripts', 'refresh-wheel-option-income.js'),
        '--start-date', process.env.WHEEL_OPTION_INCOME_START || DEFAULT_REFRESH_START,
        '--end-date', process.env.WHEEL_OPTION_INCOME_END || 'auto',
        '--strategies', artifactStrategyId,
      ],
    }], recompute);
  }

  return { state, getMetadata, getReport, recompute, refreshData };
}

module.exports = {
  createWheelOptionIncomeStrategy,
};
