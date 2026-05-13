const {
  createKernel,
  evaluateBacktestExit,
  onEvent,
  onEventLean,
} = require('../../../packages/strategy-kernels/tsll-seconds-passive-scalper');

function round(value, digits = 4) {
  return typeof value === 'number' && Number.isFinite(value) ? Number(value.toFixed(digits)) : value;
}

function eventTimeForRow(row) {
  return row.tsUtc || (Number.isFinite(row.tsMs) ? new Date(row.tsMs).toISOString() : null);
}

function barEventFromRow(row, sequence, settings, allowEntry) {
  return {
    schemaVersion: 'phenixflow.kernelEvent.v1',
    eventType: 'BAR_1S_CLOSED',
    strategyId: 'tsll-seconds-passive-scalper',
    strategyVersion: '2026.05.13',
    kernelVersion: 'tsll-seconds-passive-scalper.execution.v1',
    symbol: settings.symbol || 'TSLL',
    sequence,
    eventTime: eventTimeForRow(row),
    observedAt: eventTimeForRow(row),
    source: 'phenixflow_backtest_ohlc_1s_proxy',
    quality: {
      delayed: false,
      stale: false,
      complete: true,
    },
    payload: {
      ...row,
      tradeCount: row.trade_count,
      allowEntry,
    },
  };
}

function fillEvent({ row, sequence, settings, side, fillType, fillPrice, payload = {} }) {
  return {
    schemaVersion: 'phenixflow.kernelEvent.v1',
    eventType: 'ORDER_FILLED',
    strategyId: 'tsll-seconds-passive-scalper',
    strategyVersion: '2026.05.13',
    kernelVersion: 'tsll-seconds-passive-scalper.execution.v1',
    symbol: settings.symbol || 'TSLL',
    sequence,
    eventTime: eventTimeForRow(row),
    observedAt: eventTimeForRow(row),
    source: 'phenixflow_backtest_ohlc_1s_proxy',
    quality: {
      delayed: false,
      stale: false,
      complete: true,
    },
    payload: {
      side,
      fillType,
      fillPrice,
      tradeDate: row.tradeDate,
      tsUtc: row.tsUtc,
      ...payload,
    },
  };
}

function cancelEvent({ row, sequence, settings, pendingDecision, reason }) {
  return {
    schemaVersion: 'phenixflow.kernelEvent.v1',
    eventType: 'ORDER_CANCELLED',
    strategyId: 'tsll-seconds-passive-scalper',
    strategyVersion: '2026.05.13',
    kernelVersion: 'tsll-seconds-passive-scalper.execution.v1',
    symbol: settings.symbol || 'TSLL',
    sequence,
    eventTime: eventTimeForRow(row),
    observedAt: eventTimeForRow(row),
    source: 'phenixflow_backtest_ohlc_1s_proxy',
    quality: {
      delayed: false,
      stale: false,
      complete: true,
    },
    payload: {
      reason,
      orderDecisionId: pendingDecision?.decisionId || null,
      signalSequence: pendingDecision?.payload?.signalSequence,
      signalTradeDate: pendingDecision?.payload?.signalTradeDate,
    },
  };
}

function dispatch(state, event, audit) {
  const result = audit ? onEvent(state, event) : onEventLean(state, event);
  if (audit) {
    audit.events.push(event);
    audit.decisions.push(...result.decisions);
    audit.traces.push(...result.traces);
  }
  return result;
}

function createOpenTradeFromPending(pendingDecision, row, sequence) {
  const payload = pendingDecision.payload;
  return {
    tradeDate: payload.signalTradeDate || row.tradeDate,
    signalTsUtc: payload.signalTsUtc,
    entryTsUtc: row.tsUtc,
    signalClose: round(payload.signalClose ?? payload.entryReference?.price, 4),
    entryPrice: round(payload.limitPrice, 4),
    entryPriceRaw: payload.limitPrice,
    buyBelowCloseCents: null,
    targetCents: null,
    stopCents: null,
    grossCents: null,
    netCents: null,
    holdBars: null,
    exitReason: null,
    signalSequence: payload.signalSequence,
    entrySequence: sequence,
  };
}

function completeTrade(openTrade, exit, settings, rowsBySequence) {
  const grossCents = (exit.exitPrice - openTrade.entryPriceRaw) * 100;
  const netCents = grossCents - (settings.costCentsPerSide * 2);
  return {
    tradeDate: openTrade.tradeDate,
    signalTsUtc: openTrade.signalTsUtc,
    entryTsUtc: openTrade.entryTsUtc,
    exitTsUtc: exit.exitTime || rowsBySequence.get(exit.exitSequence)?.tsUtc,
    signalClose: openTrade.signalClose,
    entryPrice: openTrade.entryPrice,
    exitPrice: round(exit.exitPrice, 4),
    buyBelowCloseCents: settings.buyBelowCloseCents,
    targetCents: settings.targetCents,
    stopCents: settings.stopCents,
    grossCents: round(grossCents, 4),
    netCents: round(netCents, 4),
    holdBars: exit.exitSequence - openTrade.signalSequence,
    exitReason: exit.exitReason,
  };
}

function simulateSecondPassiveScalpWithKernel(rows, settings, options = {}) {
  const created = createKernel({
    settings,
    mode: options.mode || 'backtest',
    clock: {
      timezone: 'America/New_York',
      sessionDate: rows[0]?.tradeDate || null,
    },
  });
  let state = created.state;
  const trades = [];
  const rowsBySequence = new Map();
  const audit = options.includeAudit ? { events: [], decisions: [], traces: [] } : null;
  let pendingDecision = null;
  let openTrade = null;

  rows.forEach((row, sequence) => {
    rowsBySequence.set(sequence, row);

    if (pendingDecision) {
      const payload = pendingDecision.payload;
      const entryLimit = payload.limitPrice;
      const through = (settings.throughCents || 0) / 100;
      if (row.tradeDate === payload.signalTradeDate && row.low <= entryLimit - through) {
        const entryEvent = fillEvent({
          row,
          sequence,
          settings,
          side: 'BUY',
          fillType: 'entry',
          fillPrice: entryLimit,
          payload: {
            orderDecisionId: pendingDecision.decisionId,
            signalSequence: payload.signalSequence,
            signalEventId: payload.signalEventId,
            signalTradeDate: payload.signalTradeDate,
            signalTsUtc: payload.signalTsUtc,
            signalClose: payload.signalClose,
            entrySequence: sequence,
            entryTsUtc: row.tsUtc,
          },
        });
        const entryResult = dispatch(state, entryEvent, audit);
        state = entryResult.state;
        openTrade = createOpenTradeFromPending(pendingDecision, row, sequence);
      } else {
        const cancelled = cancelEvent({
          row,
          sequence,
          settings,
          pendingDecision,
          reason: row.tradeDate === payload.signalTradeDate ? 'entry_not_touched_next_bar' : 'session_boundary_before_entry',
        });
        const cancelResult = dispatch(state, cancelled, audit);
        state = cancelResult.state;
      }
      pendingDecision = null;
    }

    const barEvent = barEventFromRow(row, sequence, settings, sequence < rows.length - 2);
    const barResult = dispatch(state, barEvent, audit);
    state = barResult.state;

    if (openTrade && state.position) {
      const exit = evaluateBacktestExit(state, barEvent, settings);
      if (exit) {
        const exitRow = rowsBySequence.get(exit.exitSequence) || row;
        const exitEvent = fillEvent({
          row: exitRow,
          sequence: exit.exitSequence,
          settings,
          side: 'SELL',
          fillType: 'exit',
          fillPrice: exit.exitPrice,
          payload: {
            exitReason: exit.exitReason,
            exitSequence: exit.exitSequence,
            crossedSessionBoundary: exit.crossedSessionBoundary === true,
          },
        });
        const exitResult = dispatch(state, exitEvent, audit);
        state = exitResult.state;
        trades.push(completeTrade(openTrade, exit, settings, rowsBySequence));
        openTrade = null;
      }
    }

    const entryDecision = barResult.decisions.find((decision) => decision.decisionType === 'PLACE_ENTRY_LIMIT');
    if (entryDecision) pendingDecision = entryDecision;
  });

  return {
    trades,
    state,
    audit,
  };
}

module.exports = {
  barEventFromRow,
  simulateSecondPassiveScalpWithKernel,
};
