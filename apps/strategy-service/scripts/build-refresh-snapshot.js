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
  }).filter((change) => change.changePct !== 0);
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

function buildRows(registry, { db = null, importedAt = new Date().toISOString() } = {}) {
  const persisted = [];
  const rows = registry.listStrategies().map((listedMetadata) => {
    const strategy = registry.getStrategy(listedMetadata.id);
    const metadata = strategy.getMetadata();
    const report = strategy.getReport();
    if (db) persisted.push(persistStrategyReport(db, { metadata, report, importedAt }));
    const latest = latestSnapshot(report);
    const snapshots = Array.isArray(report.snapshots) ? report.snapshots : [];
    const previousSnapshot = snapshots.length > 1 ? snapshots[snapshots.length - 2] : null;
    const startEquity = initialEquity(report, latest);
    const endEquity = finalEquity(report, latest);
    const hasContractTrades = Array.isArray(report.normalizedTrades);
    const rawTrades = hasContractTrades ? report.normalizedTrades : (Array.isArray(report.trades) ? report.trades : []);
    const trades = rawTrades.map((trade) => (hasContractTrades ? formatContractTrade(trade) : normalizeTrade(trade)));
    const totalTrades = trades.length
      || finite(report.summary?.tradeCount)
      || finite(report.summary?.trades)
      || finite(report.summary?.sellTradeCount)
      || null;
    const recentTrades = trades.slice(-20);
    const latestDailyResult = report.latestDailyResult || null;
    const latestDate = latestDailyResult?.date || null;
    const latestTrades = trades.filter((trade) => tradeDate(trade) === latestDate);
    const holdings = holdingsFrom(latest, endEquity);
    const pnl = latestPnl(report);
    const status = pnlStatus({ dailyResult: latestDailyResult, pnl, latestTradeCount: latestTrades.length, holdings });

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
      latestTargetDate: latest?.date || report.summary?.latestRebalanceDate || latestDate,
      latestRealizedDate: latestDate,
      nextDate: latest?.nextDate || null,
      totalReturnPct: totalReturnPct(report, startEquity, endEquity),
      latestPnlPct: pnl.pct,
      latestPnlDollars: pnl.dollars,
      latestPnlSource: pnl.source,
      latestPnlStatus: status,
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
      changesFromPrevious: changesFrom(previousSnapshot, latest),
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
    '| # | Strategy | Target | Realized | Latest P/L | P/L status | Total return | Max DD | Sharpe | Equity | Trades | Open | Holdings |',
    '|---:|---|---:|---:|---:|---|---:|---:|---:|---:|---:|---:|---|',
  ];
  payload.rows.forEach((row, index) => {
    lines.push([
      `| ${index + 1}`,
      row.name,
      row.latestTargetDate || '',
      row.latestRealizedDate || '',
      markdownValue(row.latestPnlPct, '%'),
      statusLabel(row.latestPnlStatus),
      markdownValue(row.totalReturnPct, '%'),
      markdownValue(row.maxDrawdownPct, '%'),
      Number.isFinite(row.sharpe) ? row.sharpe.toFixed(3) : '',
      money(row.endEquity ?? row.finalEquity),
      row.totalTrades ?? '',
      row.openPositionCount ?? 0,
      row.topHoldings || '',
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
  const { rows, persisted } = buildRows(registry, { db, importedAt });
  if (db) db.close();
  const asOf = args.asOf || rows.map((row) => row.latestRealizedDate).filter(Boolean).sort().at(-1);
  const generatedAt = new Date().toISOString();
  const payload = {
    schemaVersion: 'phenixflow.strategyServiceRefresh.v3',
    generatedAt,
    asOf,
    note: 'Rows after 2026-04-27 are provisional per the project runbook. Latest P/L is read from each strategy result contract; EOD rows use prior target holdings marked to the next EOD close, and intraday rows use emitted trade/flat-day realized records.',
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
