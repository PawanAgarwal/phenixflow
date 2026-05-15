#!/usr/bin/env node

const fs = require('node:fs');
const path = require('node:path');

const { createDefaultRegistry } = require('../src/default-registry');
const {
  defaultDbPath,
  openStrategyResultStore,
  persistStrategyReport,
} = require('../src/result-store');

const REPO_ROOT = path.resolve(__dirname, '..', '..', '..');
const DEFAULT_OUT_DIR = path.join(REPO_ROOT, 'artifacts', 'strategy-service');

function parseArgs(argv) {
  const args = { asOf: null, outDir: DEFAULT_OUT_DIR, persistDb: true, dbPath: defaultDbPath() };
  for (let i = 2; i < argv.length; i += 1) {
    const arg = argv[i];
    if (arg === '--as-of') args.asOf = argv[++i];
    else if (arg.startsWith('--as-of=')) args.asOf = arg.slice('--as-of='.length);
    else if (arg === '--out-dir') args.outDir = path.resolve(argv[++i]);
    else if (arg.startsWith('--out-dir=')) args.outDir = path.resolve(arg.slice('--out-dir='.length));
    else if (arg === '--db-path') args.dbPath = path.resolve(argv[++i]);
    else if (arg.startsWith('--db-path=')) args.dbPath = path.resolve(arg.slice('--db-path='.length));
    else if (arg === '--no-db') args.persistDb = false;
    else throw new Error(`unknown_arg:${arg}`);
  }
  return args;
}

function finite(value) {
  return Number.isFinite(value) ? value : null;
}

function round(value, decimals = 2) {
  if (!Number.isFinite(value)) return null;
  const scale = 10 ** decimals;
  return Math.round(value * scale) / scale;
}

function pctFromMaybeFraction(value) {
  if (!Number.isFinite(value)) return null;
  return Math.abs(value) <= 5 ? value * 100 : value;
}

function dollars(value) {
  return round(value, 2);
}

function last(values) {
  return Array.isArray(values) && values.length ? values[values.length - 1] : null;
}

function latestSnapshot(report) {
  return report?.latest || last(report?.snapshots) || null;
}

function initialEquity(report, latest) {
  const summary = report?.summary || {};
  const firstDaily = Array.isArray(report?.dailyResults) ? report.dailyResults[0] : null;
  return finite(summary.initialCapital)
    ?? finite(summary.startEquity)
    ?? finite(firstDaily?.startEquity)
    ?? finite(latest?.realized?.startEquity)
    ?? null;
}

function finalEquity(report, latest) {
  const summary = report?.summary || {};
  const latestDaily = report?.latestDailyResult || null;
  return finite(summary.finalEquity)
    ?? finite(summary.endEquity)
    ?? finite(latestDaily?.endEquity)
    ?? finite(latest?.realized?.endEquity)
    ?? finite(latest?.equityBeforeNextSession)
    ?? null;
}

function totalReturnPct(report, startEquity, endEquity) {
  const summary = report?.summary || {};
  if (Number.isFinite(summary.totalReturnPct)) return round(summary.totalReturnPct, 2);
  if (Number.isFinite(summary.totalReturn)) return round(summary.totalReturn * 100, 2);
  if (Number.isFinite(startEquity) && startEquity > 0 && Number.isFinite(endEquity)) {
    return round(((endEquity / startEquity) - 1) * 100, 2);
  }
  return null;
}

function latestPnl(report) {
  const daily = report?.latestDailyResult || null;
  if (daily) return { pct: round(daily.netReturnPct, 2), dollars: dollars(daily.pnlDollars), source: daily.source || daily.basis };
  return { pct: null, dollars: null, source: 'unavailable' };
}

function tradeDate(trade) {
  return trade?.date || trade?.exitDate || trade?.entryDate || trade?.signalDate || null;
}

function normalizeTrade(trade) {
  return {
    date: tradeDate(trade),
    signalDate: trade?.signalDate || null,
    entryDate: trade?.entryDate || null,
    exitDate: trade?.exitDate || null,
    side: trade?.side || null,
    ticker: trade?.ticker || trade?.symbol || null,
    netReturnPct: round(pctFromMaybeFraction(trade?.netReturnPct ?? trade?.netReturn), 2),
    grossReturnPct: round(pctFromMaybeFraction(trade?.grossReturnPct ?? trade?.grossReturn), 2),
    pnlDollars: dollars(trade?.pnlDollars ?? trade?.pnl),
    entryPrice: finite(trade?.entryPrice),
    exitPrice: finite(trade?.exitPrice),
    carryOver: Boolean(trade?.carryOver),
    reason: trade?.reason || trade?.entryMode || null,
  };
}

function formatContractTrade(trade) {
  return {
    date: tradeDate(trade),
    signalDate: trade?.signalDate || null,
    entryDate: trade?.entryDate || null,
    exitDate: trade?.exitDate || null,
    side: trade?.side || null,
    ticker: trade?.ticker || trade?.symbol || null,
    netReturnPct: round(trade?.netReturnPct, 2),
    grossReturnPct: round(trade?.grossReturnPct, 2),
    pnlDollars: dollars(trade?.pnlDollars ?? trade?.pnl),
    entryPrice: finite(trade?.entryPrice),
    exitPrice: finite(trade?.exitPrice),
    carryOver: Boolean(trade?.carryOver),
    reason: trade?.reason || trade?.entryMode || null,
  };
}

function holdingsFrom(latest, endEquity) {
  const holdings = Array.isArray(latest?.holdings) ? latest.holdings : [];
  return holdings.map((holding) => {
    const weight = Number.isFinite(holding.weight) ? holding.weight : null;
    const dollarsValue = Number.isFinite(holding.dollars)
      ? holding.dollars
      : (Number.isFinite(weight) && Number.isFinite(endEquity) ? weight * endEquity : null);
    return {
      ticker: holding.ticker || holding.symbol || null,
      weight,
      weightPct: Number.isFinite(holding.weightPct) ? round(holding.weightPct, 2) : round((weight || 0) * 100, 2),
      dollars: dollars(dollarsValue),
    };
  });
}

function topHoldingsText(holdings) {
  return holdings.slice(0, 6)
    .map((holding) => `${holding.ticker} ${Number.isFinite(holding.weightPct) ? `${holding.weightPct.toFixed(2)}%` : ''}`.trim())
    .join(', ');
}

function changesFrom(previous, latest) {
  const prev = new Map((previous?.holdings || []).map((holding) => [holding.ticker || holding.symbol, holding]));
  const curr = new Map((latest?.holdings || []).map((holding) => [holding.ticker || holding.symbol, holding]));
  return [...new Set([...prev.keys(), ...curr.keys()])].sort().map((ticker) => {
    const before = prev.get(ticker);
    const after = curr.get(ticker);
    const beforeWeight = finite(before?.weight) ?? 0;
    const afterWeight = finite(after?.weight) ?? 0;
    return {
      ticker,
      previousWeightPct: round(beforeWeight * 100, 2),
      latestWeightPct: round(afterWeight * 100, 2),
      changePct: round((afterWeight - beforeWeight) * 100, 2),
    };
  }).filter((change) => Math.abs(change.changePct || 0) >= 0.01);
}

function isoDate(value) {
  return typeof value === 'string' && /^\d{4}-\d{2}-\d{2}$/.test(value) ? value : null;
}

function dateToMs(value) {
  const iso = isoDate(value);
  return iso ? Date.parse(`${iso}T00:00:00Z`) : NaN;
}

function minusDaysIso(value, days) {
  const ms = dateToMs(value);
  if (!Number.isFinite(ms)) return null;
  const date = new Date(ms);
  date.setUTCDate(date.getUTCDate() - days);
  return date.toISOString().slice(0, 10);
}

function minusMonthsIso(value, months) {
  const ms = dateToMs(value);
  if (!Number.isFinite(ms)) return null;
  const date = new Date(ms);
  date.setUTCMonth(date.getUTCMonth() - months);
  return date.toISOString().slice(0, 10);
}

function snapshotDate(snapshot) {
  return snapshot?.date || snapshot?.rebalanceDate || snapshot?.signalDate || null;
}

function findSnapshotByDate(snapshots, date) {
  if (!date) return null;
  return snapshots.find((snapshot) => snapshotDate(snapshot) === date) || null;
}

function findPreviousSnapshot(snapshots, date) {
  if (!date) return null;
  return [...snapshots]
    .filter((snapshot) => snapshotDate(snapshot) && snapshotDate(snapshot) < date)
    .sort((left, right) => snapshotDate(right).localeCompare(snapshotDate(left)))
    .at(0) || null;
}

function holdingsForSnapshot(snapshot, fallbackEquity) {
  return holdingsFrom(snapshot, finite(snapshot?.equityBeforeNextSession) ?? fallbackEquity);
}

function dailyNetReturn(daily) {
  if (Number.isFinite(daily?.netReturn)) return daily.netReturn;
  if (Number.isFinite(daily?.netReturnPct)) return daily.netReturnPct / 100;
  return null;
}

function dailyResultsThrough(report, asOf) {
  const byDate = new Map();
  (Array.isArray(report?.dailyResults) ? report.dailyResults : []).forEach((daily) => {
    if (!daily?.date) return;
    if (asOf && daily.date > asOf) return;
    byDate.set(daily.date, daily);
  });
  return [...byDate.values()].sort((left, right) => left.date.localeCompare(right.date));
}

function performanceForWindow(dailyResults, startAfterDate = null) {
  const rows = dailyResults.filter((daily) => !startAfterDate || daily.date > startAfterDate);
  const returns = rows.map(dailyNetReturn).filter(Number.isFinite);
  if (!returns.length) return {
    tradingDays: 0,
    returnPct: null,
    sharpe: null,
  };
  const totalReturn = returns.reduce((acc, value) => acc * (1 + value), 1) - 1;
  const mean = returns.reduce((acc, value) => acc + value, 0) / returns.length;
  const variance = returns.length > 1
    ? returns.reduce((acc, value) => acc + ((value - mean) ** 2), 0) / (returns.length - 1)
    : 0;
  const stdev = Math.sqrt(variance);
  return {
    tradingDays: returns.length,
    returnPct: round(totalReturn * 100, 2),
    sharpe: stdev > 0 ? round((mean / stdev) * Math.sqrt(252), 3) : null,
  };
}

function trailingPerformance(dailyResults, asOf) {
  return {
    sinceStart: performanceForWindow(dailyResults),
    oneYear: performanceForWindow(dailyResults, minusMonthsIso(asOf, 12)),
    threeMonth: performanceForWindow(dailyResults, minusMonthsIso(asOf, 3)),
    oneMonth: performanceForWindow(dailyResults, minusMonthsIso(asOf, 1)),
    oneWeek: performanceForWindow(dailyResults, minusDaysIso(asOf, 7)),
  };
}

function performanceText(window) {
  if (!window || !Number.isFinite(window.returnPct)) return '';
  const sharpe = Number.isFinite(window.sharpe) ? ` / ${window.sharpe.toFixed(2)}` : '';
  return `${window.returnPct.toFixed(2)}%${sharpe}`;
}

function changesText(changes, limit = 6) {
  return changes
    .slice()
    .sort((left, right) => Math.abs(right.changePct || 0) - Math.abs(left.changePct || 0))
    .slice(0, limit)
    .map((change) => `${change.ticker} ${change.changePct > 0 ? '+' : ''}${change.changePct.toFixed(2)}%`)
    .join(', ');
}

function tradeSummaryText(trades, limit = 4) {
  return trades.slice(-limit).map((trade) => {
    const bits = [trade.ticker, trade.side].filter(Boolean);
    const pnl = Number.isFinite(trade.netReturnPct) ? `${trade.netReturnPct.toFixed(2)}%` : '';
    return [...bits, pnl].filter(Boolean).join(' ');
  }).join('; ');
}

function pnlStatus({ dailyResult, pnl, latestTradeCount, holdings }) {
  if (!Number.isFinite(pnl.pct)) return 'not_available';
  if (latestTradeCount > 0) return 'latest_trade_realized';
  if (dailyResult?.basis === 'eod_prior_holdings_next_close') return 'eod_mark_to_market';
  if (pnl.pct === 0 && latestTradeCount === 0 && holdings.length === 1 && holdings[0].ticker === 'CASH') {
    return 'flat_cash_no_trade';
  }
  if (pnl.pct === 0 && latestTradeCount === 0) return 'no_latest_day_change';
  return 'reported_by_strategy';
}

function statusLabel(status) {
  return {
    eod_mark_to_market: 'EOD mark',
    flat_cash_no_trade: 'flat/cash',
    latest_trade_realized: 'traded',
    no_latest_day_change: 'no change',
    not_available: 'missing',
    reported_by_strategy: 'reported',
  }[status] || status;
}

function markdownValue(value, suffix = '') {
  return Number.isFinite(value) ? `${value.toFixed(2)}${suffix}` : '';
}

function money(value) {
  return Number.isFinite(value)
    ? value.toLocaleString('en-US', { style: 'currency', currency: 'USD', maximumFractionDigits: 0 })
    : '';
}

function buildRows(registry, { db = null, importedAt = new Date().toISOString(), asOf = null } = {}) {
  const persisted = [];
  const rows = registry.listStrategies().map((listedMetadata) => {
    const strategy = registry.getStrategy(listedMetadata.id);
    const metadata = strategy.getMetadata();
    const report = strategy.getReport();
    if (db) persisted.push(persistStrategyReport(db, { metadata, report, importedAt }));
    const snapshots = Array.isArray(report.snapshots) ? report.snapshots : [];
    const dailyResults = dailyResultsThrough(report, asOf);
    const latestDailyResult = dailyResults.at(-1) || report.latestDailyResult || null;
    const latestDate = latestDailyResult?.date || null;
    const isEodMark = latestDailyResult?.basis === 'eod_prior_holdings_next_close';
    const pnlSourceDate = isEodMark
      ? (latestDailyResult?.targetDate || latestDailyResult?.signalDate || null)
      : (latestDailyResult?.targetDate || latestDate);
    const pnlSourceSnapshot = findSnapshotByDate(snapshots, pnlSourceDate);
    const nextTargetSnapshot = findSnapshotByDate(snapshots, latestDate);
    const previousTargetSnapshot = nextTargetSnapshot ? findPreviousSnapshot(snapshots, snapshotDate(nextTargetSnapshot)) : null;
    const latest = nextTargetSnapshot || pnlSourceSnapshot || latestSnapshot(report);
    const startEquity = initialEquity(report, latest);
    const endEquity = finite(latestDailyResult?.endEquity) ?? finalEquity(report, latest);
    const hasContractTrades = Array.isArray(report.normalizedTrades);
    const rawTrades = hasContractTrades ? report.normalizedTrades : (Array.isArray(report.trades) ? report.trades : []);
    const trades = rawTrades.map((trade) => (hasContractTrades ? formatContractTrade(trade) : normalizeTrade(trade)));
    const totalTrades = trades.length
      || finite(report.summary?.tradeCount)
      || finite(report.summary?.trades)
      || finite(report.summary?.sellTradeCount)
      || null;
    const recentTrades = trades.slice(-20);
    const latestTrades = trades.filter((trade) => tradeDate(trade) === latestDate);
    const pnlSourceHoldings = pnlSourceSnapshot
      ? holdingsForSnapshot(pnlSourceSnapshot, latestDailyResult?.startEquity)
      : [];
    const nextTargetHoldings = nextTargetSnapshot
      ? holdingsForSnapshot(nextTargetSnapshot, endEquity)
      : [];
    const holdings = nextTargetHoldings.length ? nextTargetHoldings : pnlSourceHoldings;
    const pnl = latestDailyResult
      ? {
        pct: round(latestDailyResult.netReturnPct, 2),
        dollars: dollars(latestDailyResult.pnlDollars),
        source: latestDailyResult.source || latestDailyResult.basis,
      }
      : latestPnl(report);
    const status = pnlStatus({ dailyResult: latestDailyResult, pnl, latestTradeCount: latestTrades.length, holdings });
    const holdingChanges = nextTargetSnapshot
      ? changesFrom(previousTargetSnapshot, nextTargetSnapshot)
      : [];
    const targetVsPnlSourceChanges = nextTargetSnapshot && pnlSourceSnapshot
      ? changesFrom(pnlSourceSnapshot, nextTargetSnapshot)
      : holdingChanges;
    const performance = trailingPerformance(dailyResults, latestDate);
    const pnlSourceSummary = isEodMark
      ? `${pnlSourceDate || ''} target held into ${latestDate || ''}`.trim()
      : (latestTrades.length ? tradeSummaryText(latestTrades) : `${latestDate || ''} strategy-reported/flat`.trim());

    return {
      id: metadata.id,
      name: metadata.displayName || metadata.name || metadata.id,
      family: metadata.family || null,
      cadence: metadata.cadence || null,
      generatedAt: report.generatedAt || null,
      loadedAt: strategy.state?.loadedAt || null,
      source: report.source || {
        provider: metadata.dataProvider || null,
        strategySource: metadata.strategySource || null,
      },
      reportDate: latestDate,
      latestTargetDate: nextTargetSnapshot?.date || latest?.date || report.summary?.latestRebalanceDate || latestDate,
      latestRealizedDate: latestDate,
      nextDate: latest?.nextDate || null,
      totalReturnPct: performance.sinceStart.returnPct ?? totalReturnPct(report, startEquity, endEquity),
      latestPnlPct: pnl.pct,
      latestPnlDollars: pnl.dollars,
      latestPnlSource: pnl.source,
      latestPnlStatus: status,
      dailyPnl: {
        date: latestDate,
        basis: latestDailyResult?.basis || null,
        source: pnl.source,
        sourceType: isEodMark ? 'prior_eod_target_holdings' : 'intraday_trades_or_strategy_report',
        sourceDate: pnlSourceDate,
        sourceSummary: pnlSourceSummary,
        startEquity: dollars(latestDailyResult?.startEquity),
        endEquity: dollars(latestDailyResult?.endEquity),
        pnlDollars: pnl.dollars,
        netReturnPct: pnl.pct,
        tradeCount: latestTrades.length,
      },
      pnlSourceHoldingsDate: pnlSourceDate,
      pnlSourceHoldings,
      pnlSourceHoldingsText: topHoldingsText(pnlSourceHoldings),
      nextTarget: {
        status: nextTargetSnapshot ? 'available' : 'not_available',
        targetDate: nextTargetSnapshot?.date || null,
        expectedPnlDate: nextTargetSnapshot?.nextDate || null,
        holdings: nextTargetHoldings,
        holdingsText: topHoldingsText(nextTargetHoldings),
      },
      holdingChanges,
      holdingChangesText: changesText(holdingChanges),
      targetVsPnlSourceChanges,
      targetVsPnlSourceChangesText: changesText(targetVsPnlSourceChanges),
      performance,
      startEquity: dollars(startEquity),
      endEquity: dollars(endEquity),
      finalEquity: dollars(report.summary?.finalEquity),
      maxDrawdownPct: round(report.summary?.maxDrawdownPct, 2),
      sharpe: round(report.summary?.sharpe, 3),
      hitRatePct: round(report.summary?.hitRatePct, 2),
      grossExposurePct: round((latest?.grossExposure ?? 0) * 100, 2),
      turnoverPct: round(latest?.turnoverPct, 2),
      holdingCount: holdings.length,
      holdings,
      topHoldings: topHoldingsText(holdings),
      changesFromPrevious: holdingChanges,
      totalTrades,
      latestTradeCount: latestTrades.length,
      recentTrades,
      latestTrades,
      openPositionCount: Array.isArray(report.openPositions) ? report.openPositions.length : 0,
      openPositions: Array.isArray(report.openPositions) ? report.openPositions : [],
      supports: metadata.supports || [],
    };
  });
  return { rows, persisted };
}

function buildMarkdown(payload) {
  const lines = [
    `# Strategy Service Refresh - ${payload.asOf}`,
    '',
    `Generated: ${payload.generatedAt}`,
    '',
    payload.note,
    '',
    '## Daily Timing Report',
    '',
    'For EOD strategies, the P/L on day **D** comes from the target holdings set after the close on **D-1** and marked to the close on **D**. The **D target for next P/L** column is the holding set after the close on D that should drive the next realized EOD P/L. Intraday strategies use same-day emitted trades or explicit flat-day records as the P/L source.',
    '',
    '| # | Strategy | P/L date D | P/L basis | P/L source holdings/trades | Latest P/L | P/L status | D target for next P/L | Holding changes into D target | Equity | Trades |',
    '|---:|---|---:|---|---|---:|---|---|---|---:|---:|',
  ];
  payload.rows.forEach((row, index) => {
    const sourceText = row.dailyPnl?.sourceType === 'prior_eod_target_holdings'
      ? `${row.pnlSourceHoldingsDate || ''}: ${row.pnlSourceHoldingsText || ''}`.trim()
      : (row.dailyPnl?.sourceSummary || '');
    const nextTargetText = row.nextTarget?.status === 'available'
      ? `${row.nextTarget.targetDate || ''}: ${row.nextTarget.holdingsText || ''}`.trim()
      : 'not available from current artifact';
    lines.push([
      `| ${index + 1}`,
      row.name,
      row.latestRealizedDate || '',
      row.dailyPnl?.basis || '',
      sourceText,
      markdownValue(row.latestPnlPct, '%'),
      statusLabel(row.latestPnlStatus),
      nextTargetText,
      row.targetVsPnlSourceChangesText || row.holdingChangesText || '',
      money(row.endEquity ?? row.finalEquity),
      row.totalTrades ?? '',
    ].join(' | ') + ' |');
  });

  lines.push('', '## Trailing Performance', '');
  lines.push('Each cell is `return / Sharpe` for daily net returns in that window. Blank Sharpe means there were not enough non-zero observations.');
  lines.push('');
  lines.push('| # | Strategy | Since start | 1Y | 3M | 1M | 1W |');
  lines.push('|---:|---|---:|---:|---:|---:|---:|');
  payload.rows.forEach((row, index) => {
    lines.push([
      `| ${index + 1}`,
      row.name,
      performanceText(row.performance?.sinceStart),
      performanceText(row.performance?.oneYear),
      performanceText(row.performance?.threeMonth),
      performanceText(row.performance?.oneMonth),
      performanceText(row.performance?.oneWeek),
    ].join(' | ') + ' |');
  });

  lines.push('', '## P/L Status Legend', '');
  lines.push('- **EOD mark**: daily P/L came from prior EOD target holdings marked to the next EOD close, net of configured costs.');
  lines.push('- **reported**: daily P/L came directly from the strategy adapter contract.');
  lines.push('- **traded**: at least one trade closed on the realized date.');
  lines.push('- **flat/cash**: strategy was in cash with no trade or mark-to-market change on the realized date.');
  lines.push('- **no change**: adapter reported zero latest-day change, but holdings were not pure cash.');
  lines.push('- **missing**: the strategy did not emit a daily realized P/L contract row.');

  lines.push('', '## Recent Trades And Open Positions', '');
  payload.rows.filter((row) => row.totalTrades || row.openPositionCount).forEach((row) => {
    const latest = row.latestTrades.slice(-5).map((trade) => `${trade.date} ${trade.ticker || ''} ${trade.side || ''} ${markdownValue(trade.netReturnPct, '%')}`.trim());
    const recent = row.recentTrades.slice(-5).map((trade) => `${trade.date} ${trade.ticker || ''} ${trade.side || ''} ${markdownValue(trade.netReturnPct, '%')}`.trim());
    const parts = [`total trades ${row.totalTrades || 0}`, `latest-date trades ${row.latestTradeCount || 0}`, `open positions ${row.openPositionCount || 0}`];
    if (latest.length) parts.push(`latest: ${latest.join('; ')}`);
    else if (recent.length) parts.push(`recent: ${recent.join('; ')}`);
    lines.push(`- **${row.name}**: ${parts.join('; ')}`);
  });

  lines.push('', `Detailed JSON: ${payload.paths.json}`);
  return `${lines.join('\n')}\n`;
}

async function main() {
  const args = parseArgs(process.argv);
  const registry = createDefaultRegistry();
  const db = args.persistDb ? openStrategyResultStore(args.dbPath) : null;
  const importedAt = new Date().toISOString();
  const { rows, persisted } = buildRows(registry, { db, importedAt, asOf: args.asOf });
  if (db) db.close();
  const asOf = args.asOf || rows.map((row) => row.latestRealizedDate).filter(Boolean).sort().at(-1);
  const generatedAt = new Date().toISOString();
  const payload = {
    schemaVersion: 'phenixflow.strategyServiceRefresh.v4',
    generatedAt,
    asOf,
    note: 'Rows after 2026-04-27 are provisional per the project runbook. The daily timing report separates P/L source holdings/trades from the target holdings set after the P/L date. EOD rows use prior target holdings marked to the next EOD close; intraday rows use emitted trades or explicit flat-day realized records.',
    registeredStrategyCount: rows.length,
    persisted,
    rows,
    paths: {
      json: path.join(args.outDir, `strategy-service-refresh-${asOf}.json`),
      markdown: path.join(args.outDir, `strategy-service-refresh-${asOf}.md`),
      db: args.persistDb ? args.dbPath : null,
    },
  };
  fs.mkdirSync(args.outDir, { recursive: true });
  fs.writeFileSync(payload.paths.json, `${JSON.stringify(payload, null, 2)}\n`);
  fs.writeFileSync(payload.paths.markdown, buildMarkdown(payload));
  process.stdout.write(JSON.stringify({
    jsonPath: payload.paths.json,
    mdPath: payload.paths.markdown,
    dbPath: payload.paths.db,
    rows: rows.length,
    persisted: persisted.reduce((acc, item) => {
      acc.dailyResults += item.dailyResultCount;
      acc.holdings += item.holdingCount;
      acc.trades += item.tradeCount;
      return acc;
    }, { dailyResults: 0, holdings: 0, trades: 0 }),
    pnlStatuses: rows.reduce((acc, row) => {
      acc[row.latestPnlStatus] = (acc[row.latestPnlStatus] || 0) + 1;
      return acc;
    }, {}),
  }, null, 2));
  process.stdout.write('\n');
}

main().catch((error) => {
  process.stderr.write(`${error.stack || error.message}\n`);
  process.exitCode = 1;
});
