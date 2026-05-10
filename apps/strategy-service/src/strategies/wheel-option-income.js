const fs = require('node:fs');
const path = require('node:path');

const REPO_ROOT = path.resolve(__dirname, '..', '..', '..', '..');
const DEFAULT_REPORT_PATH = 'projects/spy-intraday-prediction/artifacts/wheel-expanded-backtest-2026-01-02-2026-04-27.json';
const DEFAULT_ARTIFACT_STRATEGY_ID = 'wheel_weekly_10otm_trend_ivrv_profit50';
const DEFAULT_INITIAL_CAPITAL = 1_000_000;

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
    skippedDays: [],
    metadata,
  };
}

function createWheelOptionIncomeStrategy(options = {}) {
  const reportPath = resolvePath(options.reportPath || process.env.WHEEL_OPTION_INCOME_REPORT_PATH || DEFAULT_REPORT_PATH);
  const artifactStrategyId = options.artifactStrategyId || DEFAULT_ARTIFACT_STRATEGY_ID;
  const metadata = {
    id: options.id || 'option-income-wheel-trend-ivrv',
    name: options.name || 'Option Income Wheel IV/RV Trend',
    displayName: options.displayName || 'Option Income Wheel',
    family: 'option_income_research',
    cadence: 'daily_eod',
    actionType: 'short_option_income',
    dataProvider: 'Massive stock and OPRA option 1m aggregates',
    strategySource: 'SPY intraday Massive-only wheel backtest artifact',
    description: 'Low-drawdown weekly wheel candidate: sell far OTM puts only when prior trend and IV/RV filters agree, then close winners at 50% profit.',
    ruleSummary: options.ruleSummary || RULE_SUMMARY,
    sourceLinks: [
      { label: 'OIC Cash-Secured Put', href: 'https://www.optionseducation.org/strategies/all-strategies/cash-secured-put' },
      { label: 'Cboe PutWrite VRP', href: 'https://www.cboe.com/insights/posts/white-paper-shows-volatility-risk-premium-facilitated-higher-risk-adjusted-returns-for-put-index/' },
    ],
    artifactStrategyId,
    defaultStartDate: '2026-01-02',
    supports: ['chart', 'values', 'latest_portfolio', 'portfolio_change'],
  };
  const state = { report: null, loadedAt: null, refresh: null };

  function loadReport() {
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

  return { state, getMetadata, getReport, recompute };
}

module.exports = {
  createWheelOptionIncomeStrategy,
};
