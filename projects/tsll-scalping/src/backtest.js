const { hasSignal } = require('./strategies');

function round(value, digits = 4) {
  return typeof value === 'number' && Number.isFinite(value) ? Number(value.toFixed(digits)) : value;
}

function maxDrawdown(values) {
  let peak = 0;
  let drawdown = 0;
  let cumulative = 0;
  values.forEach((value) => {
    cumulative += value;
    if (cumulative > peak) peak = cumulative;
    drawdown = Math.min(drawdown, cumulative - peak);
  });
  return drawdown;
}

function summarizeTrades(trades, inputBars) {
  const pnls = trades.map((trade) => trade.netCents);
  const wins = pnls.filter((value) => value > 0);
  const losses = pnls.filter((value) => value < 0);
  const grossWin = wins.reduce((sum, value) => sum + value, 0);
  const grossLoss = losses.reduce((sum, value) => sum + value, 0);
  const total = pnls.reduce((sum, value) => sum + value, 0);
  const totalHoldBars = trades.reduce((sum, trade) => sum + trade.holdBars, 0);
  const byDay = new Map();
  trades.forEach((trade) => {
    const current = byDay.get(trade.tradeDate) || { trades: 0, netCents: 0 };
    current.trades += 1;
    current.netCents += trade.netCents;
    byDay.set(trade.tradeDate, current);
  });
  const dayStats = [...byDay.entries()].map(([date, stats]) => ({
    date,
    trades: stats.trades,
    netCents: round(stats.netCents, 3),
  }));
  return {
    trades: trades.length,
    inputBars,
    netCents: round(total, 3),
    avgNetCents: trades.length ? round(total / trades.length, 4) : 0,
    winRate: trades.length ? round(wins.length / trades.length, 4) : 0,
    profitFactor: grossLoss < 0 ? round(grossWin / Math.abs(grossLoss), 4) : (grossWin > 0 ? null : 0),
    maxDrawdownCents: round(maxDrawdown(pnls), 3),
    avgHoldBars: trades.length ? round(totalHoldBars / trades.length, 2) : 0,
    exposureShare: inputBars ? round(totalHoldBars / inputBars, 4) : 0,
    positiveDays: dayStats.filter((day) => day.netCents > 0).length,
    tradedDays: dayStats.length,
    dayStats,
  };
}

function simulateLongScalp(rows, strategy, settings = {}) {
  const costCentsPerSide = settings.costCentsPerSide ?? 0.5;
  const cooldownBars = settings.cooldownBars ?? 2;
  const target = strategy.execution.targetCents / 100;
  const stop = strategy.execution.stopCents / 100;
  const maxHoldBars = strategy.execution.maxHoldBars;
  const cost = costCentsPerSide / 100;
  const trades = [];
  let index = 0;

  while (index < rows.length - 2) {
    const row = rows[index];
    const next = rows[index + 1];
    if (!hasSignal(row, strategy, settings) || row.tradeDate !== next.tradeDate) {
      index += 1;
      continue;
    }

    const entryIndex = index + 1;
    const entryMid = next.open;
    const entryFill = entryMid + cost;
    const targetMid = entryMid + target;
    const stopMid = entryMid - stop;
    let exitIndex = entryIndex;
    let exitMid = next.close;
    let exitReason = 'time';

    const lastIndex = Math.min(rows.length - 1, entryIndex + maxHoldBars);
    for (let cursor = entryIndex; cursor <= lastIndex; cursor += 1) {
      const bar = rows[cursor];
      if (bar.tradeDate !== row.tradeDate) break;
      exitIndex = cursor;
      exitMid = bar.close;
      if (bar.low <= stopMid && bar.high >= targetMid) {
        exitMid = stopMid;
        exitReason = 'stop_same_bar';
        break;
      }
      if (bar.low <= stopMid) {
        exitMid = stopMid;
        exitReason = 'stop';
        break;
      }
      if (bar.high >= targetMid) {
        exitMid = targetMid;
        exitReason = 'target';
        break;
      }
    }

    const exitFill = exitMid - cost;
    trades.push({
      tradeDate: row.tradeDate,
      entryTsUtc: next.tsUtc,
      exitTsUtc: rows[exitIndex]?.tsUtc,
      entryMid: round(entryMid, 4),
      exitMid: round(exitMid, 4),
      targetCents: strategy.execution.targetCents,
      stopCents: strategy.execution.stopCents,
      netCents: round((exitFill - entryFill) * 100, 4),
      holdBars: exitIndex - entryIndex + 1,
      exitReason,
    });
    index = exitIndex + cooldownBars + 1;
  }

  return {
    strategy: strategy.name,
    params: strategy.params,
    execution: {
      ...strategy.execution,
      costCentsPerSide,
      cooldownBars,
    },
    summary: summarizeTrades(trades, rows.length),
    trades,
  };
}

function rankResults(results, { minTrades = 20 } = {}) {
  return results
    .filter((result) => result.summary.trades >= minTrades)
    .sort((left, right) => {
      const leftScore = left.summary.avgNetCents;
      const rightScore = right.summary.avgNetCents;
      if (rightScore !== leftScore) return rightScore - leftScore;
      return right.summary.netCents - left.summary.netCents;
    });
}

module.exports = {
  simulateLongScalp,
  summarizeTrades,
  rankResults,
};
