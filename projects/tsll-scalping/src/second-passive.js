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
  if (!row) return false;
  if (row.minutes_from_open < settings.noEntryFirstMinutes) return false;
  if (row.minutes_to_close < settings.noEntryLastMinutes) return false;
  if ((row.trade_count || 0) < settings.minTradeCount) return false;
  if ((row.range_60s_cents || 0) < settings.minRange60sCents) return false;
  if ((row.ret_60s_cents || 0) < settings.minRet60sCents) return false;
  if ((row.ret_1bar_cents || 0) > settings.maxLastBarUpCents) return false;
  if (settings.requireMarketOk && row.market_ok_1m !== 1) return false;
  if ((row.spy_ret_1m || 0) < settings.minSpyRet1m) return false;
  if ((row.qqq_ret_1m || 0) < settings.minQqqRet1m) return false;
  if ((row.tsla_ret_1m || 0) < settings.minTslaRet1m) return false;
  if (settings.requireDailyContext && row.daily_context_ready !== 1) return false;
  if (settings.requireDailyMacroTrend && row.daily_macro_trend_up !== 1) return false;
  if (settings.maxAbsFromPrevCloseAtr !== null && Math.abs(row.daily_tsll_from_prev_close_atr || 0) > settings.maxAbsFromPrevCloseAtr) return false;
  if (settings.maxRangeSoFarAtr !== null && (row.daily_tsll_range_so_far_atr || 0) > settings.maxRangeSoFarAtr) return false;
  return true;
}

function defaultSettings(input = {}) {
  return {
    name: input.name || 'seconds_passive_limit_scalp',
    costCentsPerSide: input.costCentsPerSide ?? 0.5,
    buyBelowCloseCents: input.buyBelowCloseCents ?? 2,
    targetCents: input.targetCents ?? 2,
    stopCents: input.stopCents ?? 5,
    maxHoldBars: input.maxHoldBars ?? 5,
    cooldownBars: input.cooldownBars ?? 2,
    throughCents: input.throughCents ?? 0,
    noEntryFirstMinutes: input.noEntryFirstMinutes ?? 5,
    noEntryLastMinutes: input.noEntryLastMinutes ?? 10,
    minTradeCount: input.minTradeCount ?? 1,
    minRange60sCents: input.minRange60sCents ?? 3,
    minRet60sCents: input.minRet60sCents ?? -20,
    maxLastBarUpCents: input.maxLastBarUpCents ?? 20,
    requireMarketOk: input.requireMarketOk ?? true,
    minSpyRet1m: input.minSpyRet1m ?? -0.001,
    minQqqRet1m: input.minQqqRet1m ?? -0.0012,
    minTslaRet1m: input.minTslaRet1m ?? -0.002,
    requireDailyContext: input.requireDailyContext ?? false,
    requireDailyMacroTrend: input.requireDailyMacroTrend ?? false,
    maxAbsFromPrevCloseAtr: input.maxAbsFromPrevCloseAtr ?? null,
    maxRangeSoFarAtr: input.maxRangeSoFarAtr ?? null,
  };
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
  const trades = [];
  let index = 0;
  while (index < rows.length - 2) {
    const signal = rows[index];
    const entryBar = rows[index + 1];
    if (signal.tradeDate !== entryBar.tradeDate || !passesFilters(signal, settings)) {
      index += 1;
      continue;
    }

    const entryLimit = signal.close - (settings.buyBelowCloseCents / 100);
    if (entryBar.low > entryLimit - (settings.throughCents / 100)) {
      index += 1;
      continue;
    }

    const target = entryLimit + (settings.targetCents / 100);
    const stop = entryLimit - (settings.stopCents / 100);
    const lastIndex = Math.min(rows.length - 1, index + 1 + settings.maxHoldBars);
    let exitIndex = index + 1;
    let exitPrice = entryBar.close;
    let exitReason = 'timeout';

    for (let cursor = index + 1; cursor <= lastIndex; cursor += 1) {
      const row = rows[cursor];
      if (row.tradeDate !== signal.tradeDate) break;
      exitIndex = cursor;
      exitPrice = row.close;
      if (row.low <= stop && row.high >= target + (settings.throughCents / 100)) {
        exitPrice = stop;
        exitReason = 'stop_same_bar';
        break;
      }
      if (row.low <= stop) {
        exitPrice = stop;
        exitReason = 'stop';
        break;
      }
      if (row.high >= target + (settings.throughCents / 100)) {
        exitPrice = target;
        exitReason = 'target';
        break;
      }
    }

    const grossCents = (exitPrice - entryLimit) * 100;
    const netCents = grossCents - (settings.costCentsPerSide * 2);
    trades.push({
      tradeDate: signal.tradeDate,
      signalTsUtc: signal.tsUtc,
      entryTsUtc: entryBar.tsUtc,
      exitTsUtc: rows[exitIndex]?.tsUtc,
      signalClose: round(signal.close, 4),
      entryPrice: round(entryLimit, 4),
      exitPrice: round(exitPrice, 4),
      buyBelowCloseCents: settings.buyBelowCloseCents,
      targetCents: settings.targetCents,
      stopCents: settings.stopCents,
      grossCents: round(grossCents, 4),
      netCents: round(netCents, 4),
      holdBars: exitIndex - index,
      exitReason,
    });
    index = exitIndex + settings.cooldownBars + 1;
  }
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
