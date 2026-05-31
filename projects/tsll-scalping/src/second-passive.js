const PINNED_PROMOTED_SETTINGS = require('../../../packages/strategy-kernels/tsll-seconds-passive-scalper/settings/default.json');
const {
  evaluateFilters,
} = require('../../../packages/strategy-kernels/tsll-seconds-passive-scalper');
const { simulateSecondPassiveScalpWithKernel } = require('./backtest-kernel-adapter');

function round(value, digits = 4) {
  return typeof value === 'number' && Number.isFinite(value) ? Number(value.toFixed(digits)) : value;
}

function maxDrawdown(values) {
  let peak = 0;
  let cumulative = 0;
  let drawdown = 0;
  values.forEach((value) => {
    cumulative += value;
    if (cumulative > peak) peak = cumulative;
    drawdown = Math.min(drawdown, cumulative - peak);
  });
  return drawdown;
}

function passesFilters(row, settings) {
  return evaluateFilters(row, settings).passed;
}

function defaultSettings(input = {}) {
  const merged = {
    ...PINNED_PROMOTED_SETTINGS,
    ...input,
  };
  const strategyOverrideKeys = [
    'buyBelowCloseCents',
    'targetCents',
    'stopCents',
    'maxHoldBars',
    'maxHoldSeconds',
    'minRet60sCents',
    'maxRet60sCents',
  ];
  const customStaticStrategy = input.sessionProfiles === undefined
    && strategyOverrideKeys.some((key) => Object.prototype.hasOwnProperty.call(input, key));
  if (customStaticStrategy) delete merged.sessionProfiles;
  return merged;
}

function summarizeSecondScalps(trades, inputBars) {
  const pnls = trades.map((trade) => trade.netCents);
  const wins = pnls.filter((value) => value > 0);
  const losses = pnls.filter((value) => value < 0);
  const grossWin = wins.reduce((sum, value) => sum + value, 0);
  const grossLoss = losses.reduce((sum, value) => sum + value, 0);
  const total = pnls.reduce((sum, value) => sum + value, 0);
  const holdBars = trades.reduce((sum, trade) => sum + trade.holdBars, 0);
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
    avgHoldBars: trades.length ? round(holdBars / trades.length, 2) : 0,
    positiveDays: dayStats.filter((day) => day.netCents > 0).length,
    tradedDays: dayStats.length,
    dayStats,
  };
}

function simulateSecondPassiveScalp(rows, rawSettings = {}) {
  const settings = defaultSettings(rawSettings);
  const { trades } = simulateSecondPassiveScalpWithKernel(rows, settings);
  return {
    strategy: settings.name,
    settings,
    summary: summarizeSecondScalps(trades, rows.length),
    trades,
  };
}

module.exports = {
  defaultSettings,
  passesFilters,
  simulateSecondPassiveScalp,
  summarizeSecondScalps,
};
