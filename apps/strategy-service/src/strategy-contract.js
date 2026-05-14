function finite(value) {
  return Number.isFinite(value) ? value : null;
}

function pct(value) {
  return Number.isFinite(value) ? value * 100 : null;
}

function isIntraday(metadata = {}) {
  const cadence = String(metadata.cadence || '').toLowerCase();
  const family = String(metadata.family || '').toLowerCase();
  const actionType = String(metadata.actionType || '').toLowerCase();
  return cadence.includes('intraday') || family.includes('intraday') || actionType.includes('intraday')
    || actionType.includes('scalp');
}

function tradeDate(trade = {}) {
  return trade.date || trade.tradeDate || trade.exitDate || trade.entryDate || trade.signalDate || null;
}

function tradeReturnPct(trade = {}, fieldName, fallbackFieldName) {
  if (Number.isFinite(trade[fieldName])) return trade[fieldName];
  if (Number.isFinite(trade[fallbackFieldName])) return pct(trade[fallbackFieldName]);
  if (Number.isFinite(trade.netCents) && Number.isFinite(trade.entryPrice) && trade.entryPrice > 0) {
    return ((trade.netCents / 100) / trade.entryPrice) * 100;
  }
  return null;
}

function tradePnlDollars(trade = {}, shareBlock = 1) {
  if (Number.isFinite(trade.pnlDollars)) return trade.pnlDollars;
  if (Number.isFinite(trade.pnl)) return trade.pnl;
  if (Number.isFinite(trade.netCents)) return (trade.netCents / 100) * shareBlock;
  return null;
}

function normalizeTrade(trade = {}, { strategyId, index = 0, shareBlock = 1 } = {}) {
  const date = tradeDate(trade);
  return {
    strategyId,
    sequence: index,
    date,
    signalDate: trade.signalDate || trade.tradeDate || date,
    entryDate: trade.entryDate || trade.tradeDate || date,
    exitDate: trade.exitDate || trade.tradeDate || date,
    signalTimeUtc: trade.signalTsUtc || trade.signalTimeUtc || null,
    entryTimeUtc: trade.entryTsUtc || trade.entryTimeUtc || null,
    exitTimeUtc: trade.exitTsUtc || trade.exitTimeUtc || null,
    side: trade.side || null,
    ticker: trade.ticker || trade.symbol || null,
    entryPrice: finite(trade.entryPrice),
    exitPrice: finite(trade.exitPrice),
    grossReturn: Number.isFinite(trade.grossReturn) ? trade.grossReturn : null,
    grossReturnPct: tradeReturnPct(trade, 'grossReturnPct', 'grossReturn'),
    netReturn: Number.isFinite(trade.netReturn) ? trade.netReturn : (
      Number.isFinite(trade.netCents) && Number.isFinite(trade.entryPrice) && trade.entryPrice > 0
        ? (trade.netCents / 100) / trade.entryPrice
        : null
    ),
    netReturnPct: tradeReturnPct(trade, 'netReturnPct', 'netReturn'),
    pnlDollars: tradePnlDollars(trade, shareBlock),
    quantity: finite(trade.quantity),
    leverage: finite(trade.leverage),
    carryOver: Boolean(trade.carryOver),
    reason: trade.reason || trade.exitReason || trade.entryMode || null,
    raw: trade,
  };
}

function normalizedTrades(report = {}, metadata = {}) {
  const shareBlock = finite(report.settings?.shareBlock) || 1;
  const trades = Array.isArray(report.trades) ? report.trades : [];
  return trades.map((trade, index) => normalizeTrade(trade, {
    strategyId: metadata.id,
    index,
    shareBlock,
  }));
}

function groupTradesByDate(trades) {
  const byDate = new Map();
  trades.forEach((trade) => {
    if (!trade.date) return;
    const current = byDate.get(trade.date) || [];
    current.push(trade);
    byDate.set(trade.date, current);
  });
  return byDate;
}

function dailyResultFromSnapshot({ snapshot, metadata, tradesByDate }) {
  if (!snapshot?.realized) return null;
  const realized = snapshot.realized;
  const date = realized.date || snapshot.nextDate || snapshot.date;
  const trades = tradesByDate.get(date) || [];
  const startEquity = finite(realized.startEquity) ?? finite(snapshot.equityBeforeNextSession);
  const endEquity = finite(realized.endEquity);
  const pnlDollars = finite(realized.pnlDollars)
    ?? (Number.isFinite(startEquity) && Number.isFinite(endEquity) ? endEquity - startEquity : null);
  const tradeCount = finite(realized.trades) ?? trades.length;
  const netReturn = finite(realized.netReturn)
    ?? (Number.isFinite(startEquity) && startEquity > 0 && Number.isFinite(pnlDollars) ? pnlDollars / startEquity : null);
  const netReturnPct = finite(realized.netReturnPct) ?? pct(netReturn);
  const grossReturn = finite(realized.grossReturn);
  const grossReturnPct = finite(realized.grossReturnPct) ?? pct(grossReturn);

  return {
    schemaVersion: 'phenixflow.strategyDailyResult.v1',
    strategyId: metadata.id,
    date,
    signalDate: snapshot.date,
    targetDate: snapshot.date,
    nextDate: snapshot.nextDate || date,
    cadence: metadata.cadence || null,
    basis: isIntraday(metadata) ? 'intraday_trades' : 'eod_prior_holdings_next_close',
    source: isIntraday(metadata) ? 'strategy_report_realized_trade_pnl' : 'strategy_report_realized_eod_holdings_pnl',
    startEquity,
    endEquity,
    pnlDollars,
    grossReturn,
    grossReturnPct,
    netReturn,
    netReturnPct,
    costReturn: finite(realized.costReturn),
    costReturnPct: finite(realized.costReturnPct),
    missingReturnCount: finite(realized.missingReturnCount) ?? 0,
    tradeCount,
    holdingCount: Array.isArray(snapshot.holdings) ? snapshot.holdings.length : 0,
    topHoldings: snapshot.topHoldings || null,
    tradeIds: trades.map((trade) => trade.sequence),
  };
}

function ensureStrategyResultContract(report, metadata = {}) {
  if (!report || typeof report !== 'object') return report;
  if (report.resultContract?.schemaVersion === 'phenixflow.strategyResultContract.v1') return report;

  const trades = normalizedTrades(report, metadata);
  const tradesByDate = groupTradesByDate(trades);
  const snapshots = Array.isArray(report.snapshots) ? report.snapshots : [];
  const dailyResults = snapshots
    .map((snapshot) => dailyResultFromSnapshot({ snapshot, metadata, tradesByDate }))
    .filter(Boolean);

  report.normalizedTrades = trades;
  report.dailyResults = dailyResults;
  report.latestDailyResult = dailyResults.at(-1) || null;
  report.resultContract = {
    schemaVersion: 'phenixflow.strategyResultContract.v1',
    dailyResultSchemaVersion: 'phenixflow.strategyDailyResult.v1',
    tradeSchemaVersion: 'phenixflow.strategyTrade.v1',
    holdingSchemaVersion: 'phenixflow.strategyHoldingTarget.v1',
    eodPnlBasis: 'prior EOD target holdings marked from signal close to next EOD close, net of configured costs',
    intradayPnlBasis: 'closed strategy trades and flat-day realized records emitted by the strategy artifact',
    generatedAt: new Date().toISOString(),
  };
  return report;
}

function withStrategyResultContract(strategy) {
  function decorate(report) {
    return ensureStrategyResultContract(report, strategy.getMetadata());
  }
  return {
    ...strategy,
    getReport: () => decorate(strategy.getReport()),
    recompute: typeof strategy.recompute === 'function'
      ? (...args) => decorate(strategy.recompute(...args))
      : undefined,
  };
}

module.exports = {
  ensureStrategyResultContract,
  normalizeTrade,
  normalizedTrades,
  withStrategyResultContract,
};
