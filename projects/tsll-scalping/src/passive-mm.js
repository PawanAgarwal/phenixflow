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

function spreadCents(quote) {
  return (quote.askPrice - quote.bidPrice) * 100;
}

function isEntryQuote(quote, settings) {
  if (!quote) return false;
  const spread = spreadCents(quote);
  const minute = quote.minuteOfDayEt;
  const openMinute = settings.regularOpenMinuteEt ?? 570;
  const closeMinute = settings.regularCloseMinuteEt ?? 960;
  return spread >= settings.minSpreadCents
    && spread <= settings.maxSpreadCents
    && (quote.bidSize || 0) >= settings.minBidSize
    && (quote.askSize || 0) >= settings.minAskSize
    && minute >= openMinute + settings.noEntryFirstMinutes
    && minute < closeMinute - settings.noEntryLastMinutes;
}

function createBuyOrder(quote, settings) {
  return {
    dayIso: quote.dayIso,
    quoteTsMs: quote.tsMs,
    liveAtMs: quote.tsMs + settings.latencyMs,
    price: quote.bidPrice - (settings.buyOffsetCents / 100),
    quoteBid: quote.bidPrice,
    quoteAsk: quote.askPrice,
    spreadCents: spreadCents(quote),
  };
}

function createSellOrder(position, quote, settings) {
  if (!quote || !(quote.askPrice > position.entryPrice)) return null;
  const quoteAsk = quote.askPrice - (settings.sellOffsetCents / 100);
  const minTarget = position.entryPrice + (settings.minProfitCents / 100);
  return {
    dayIso: position.dayIso,
    quoteTsMs: quote.tsMs,
    liveAtMs: quote.tsMs + settings.latencyMs,
    price: Math.max(minTarget, quoteAsk),
    quoteBid: quote.bidPrice,
    quoteAsk: quote.askPrice,
  };
}

function defaultSettings(input = {}) {
  return {
    name: input.name || 'passive_bid_to_ask',
    costCentsPerSide: input.costCentsPerSide ?? 0.5,
    minSpreadCents: input.minSpreadCents ?? 2,
    maxSpreadCents: input.maxSpreadCents ?? 10,
    minProfitCents: input.minProfitCents ?? 1,
    stopCents: input.stopCents ?? 4,
    maxHoldMs: input.maxHoldMs ?? 5000,
    cooldownMs: input.cooldownMs ?? 1000,
    latencyMs: input.latencyMs ?? 0,
    buyOffsetCents: input.buyOffsetCents ?? 0,
    sellOffsetCents: input.sellOffsetCents ?? 0,
    requireBuyThroughCents: input.requireBuyThroughCents ?? 0,
    requireSellThroughCents: input.requireSellThroughCents ?? 0,
    minBidSize: input.minBidSize ?? 1,
    minAskSize: input.minAskSize ?? 1,
    minPrintSize: input.minPrintSize ?? 1,
    noEntryFirstMinutes: input.noEntryFirstMinutes ?? 5,
    noEntryLastMinutes: input.noEntryLastMinutes ?? 5,
    regularOpenMinuteEt: input.regularOpenMinuteEt ?? 570,
    regularCloseMinuteEt: input.regularCloseMinuteEt ?? 960,
    repriceSell: input.repriceSell ?? true,
  };
}

function summarizePassiveTrades(trades, counts) {
  const pnls = trades.map((trade) => trade.netCents);
  const wins = pnls.filter((value) => value > 0);
  const losses = pnls.filter((value) => value < 0);
  const grossWin = wins.reduce((sum, value) => sum + value, 0);
  const grossLoss = losses.reduce((sum, value) => sum + value, 0);
  const total = pnls.reduce((sum, value) => sum + value, 0);
  const holdMs = trades.reduce((sum, trade) => sum + trade.holdMs, 0);
  const byDay = new Map();
  trades.forEach((trade) => {
    const current = byDay.get(trade.dayIso) || { trades: 0, netCents: 0 };
    current.trades += 1;
    current.netCents += trade.netCents;
    byDay.set(trade.dayIso, current);
  });
  const dayStats = [...byDay.entries()].map(([date, stats]) => ({
    date,
    trades: stats.trades,
    netCents: round(stats.netCents, 3),
  }));
  const tradedDays = new Set([
    ...dayStats.map((day) => day.date),
    ...(counts.days || []),
  ]).size;
  const regularSessionMs = 390 * 60 * 1000;
  return {
    trades: trades.length,
    inputQuotes: counts.inputQuotes,
    inputTrades: counts.inputTrades,
    events: counts.events,
    eligibleQuotes: counts.eligibleQuotes,
    buyOrders: counts.buyOrders,
    buyFills: counts.buyFills,
    sellFills: counts.sellFills,
    stopExits: counts.stopExits,
    timeoutExits: counts.timeoutExits,
    endOfDayExits: counts.endOfDayExits,
    netCents: round(total, 3),
    avgNetCents: trades.length ? round(total / trades.length, 4) : 0,
    avgGrossCents: trades.length
      ? round(trades.reduce((sum, trade) => sum + trade.grossCents, 0) / trades.length, 4)
      : 0,
    winRate: trades.length ? round(wins.length / trades.length, 4) : 0,
    profitFactor: grossLoss < 0 ? round(grossWin / Math.abs(grossLoss), 4) : (grossWin > 0 ? null : 0),
    maxDrawdownCents: round(maxDrawdown(pnls), 3),
    avgHoldMs: trades.length ? round(holdMs / trades.length, 2) : 0,
    exposureShare: tradedDays ? round(holdMs / (tradedDays * regularSessionMs), 4) : 0,
    fillRatePerOrder: counts.buyOrders ? round(counts.buyFills / counts.buyOrders, 6) : 0,
    positiveDays: dayStats.filter((day) => day.netCents > 0).length,
    tradedDays: dayStats.length,
    dayStats,
  };
}

function simulatePassiveMarketMaking({ quotes, trades, settings: rawSettings = {} }) {
  const settings = defaultSettings(rawSettings);
  const counts = {
    days: [...new Set([...(quotes || []).map((quote) => quote.dayIso), ...(trades || []).map((trade) => trade.dayIso)].filter(Boolean))],
    inputQuotes: quotes.length,
    inputTrades: trades.length,
    events: 0,
    eligibleQuotes: 0,
    buyOrders: 0,
    buyFills: 0,
    sellFills: 0,
    stopExits: 0,
    timeoutExits: 0,
    endOfDayExits: 0,
  };
  const outputTrades = [];
  let quoteIndex = 0;
  let tradeIndex = 0;
  let currentQuote = null;
  let activeBuy = null;
  let activeSell = null;
  let position = null;
  let cooldownUntilMs = 0;

  function exitPosition(exitPrice, tsMs, reason) {
    const grossCents = (exitPrice - position.entryPrice) * 100;
    const netCents = grossCents - (settings.costCentsPerSide * 2);
    if (reason === 'target') counts.sellFills += 1;
    if (reason === 'stop') counts.stopExits += 1;
    if (reason === 'timeout') counts.timeoutExits += 1;
    if (reason === 'end_of_day') counts.endOfDayExits += 1;
    outputTrades.push({
      dayIso: position.dayIso,
      entryTsUtc: new Date(position.entryTsMs).toISOString(),
      exitTsUtc: new Date(tsMs).toISOString(),
      entryPrice: round(position.entryPrice, 4),
      exitPrice: round(exitPrice, 4),
      entrySpreadCents: round(position.entrySpreadCents, 4),
      grossCents: round(grossCents, 4),
      netCents: round(netCents, 4),
      holdMs: tsMs - position.entryTsMs,
      exitReason: reason,
    });
    position = null;
    activeSell = null;
    activeBuy = null;
    cooldownUntilMs = tsMs + settings.cooldownMs;
  }

  function maybeTimeExit(tsMs, fallbackPrice) {
    if (!position || tsMs - position.entryTsMs < settings.maxHoldMs) return false;
    const exitPrice = currentQuote?.bidPrice || fallbackPrice;
    if (!(exitPrice > 0)) return false;
    exitPosition(exitPrice, tsMs, 'timeout');
    return true;
  }

  function handleQuote(quote) {
    counts.events += 1;
    currentQuote = quote;
    if (position) {
      if (maybeTimeExit(quote.tsMs, quote.bidPrice)) return;
      if (quote.bidPrice <= position.entryPrice - (settings.stopCents / 100)) {
        exitPosition(quote.bidPrice, quote.tsMs, 'stop');
        return;
      }
      if (settings.repriceSell) activeSell = createSellOrder(position, quote, settings);
      return;
    }
    if (quote.tsMs < cooldownUntilMs) {
      activeBuy = null;
      return;
    }
    if (!isEntryQuote(quote, settings)) {
      activeBuy = null;
      return;
    }
    counts.eligibleQuotes += 1;
    counts.buyOrders += 1;
    activeBuy = createBuyOrder(quote, settings);
  }

  function handleTrade(trade) {
    counts.events += 1;
    if (position && maybeTimeExit(trade.tsMs, trade.price)) return;
    if (position) {
      if (trade.price <= position.entryPrice - (settings.stopCents / 100)) {
        const exitPrice = currentQuote?.bidPrice || trade.price;
        exitPosition(exitPrice, trade.tsMs, 'stop');
        return;
      }
      if (
        activeSell
        && trade.size >= settings.minPrintSize
        && trade.tsMs >= activeSell.liveAtMs
        && trade.price >= activeSell.price + (settings.requireSellThroughCents / 100)
      ) {
        exitPosition(activeSell.price, trade.tsMs, 'target');
      }
      return;
    }
    if (
      activeBuy
      && trade.size >= settings.minPrintSize
      && trade.tsMs >= activeBuy.liveAtMs
      && trade.price <= activeBuy.price - (settings.requireBuyThroughCents / 100)
    ) {
      counts.buyFills += 1;
      position = {
        dayIso: trade.dayIso || activeBuy.dayIso,
        entryTsMs: trade.tsMs,
        entryPrice: activeBuy.price,
        entrySpreadCents: activeBuy.spreadCents,
      };
      activeBuy = null;
      activeSell = createSellOrder(position, currentQuote, settings);
    }
  }

  while (quoteIndex < quotes.length || tradeIndex < trades.length) {
    const nextQuote = quotes[quoteIndex];
    const nextTrade = trades[tradeIndex];
    if (nextQuote && (!nextTrade || nextQuote.tsMs <= nextTrade.tsMs)) {
      quoteIndex += 1;
      handleQuote(nextQuote);
    } else {
      tradeIndex += 1;
      handleTrade(nextTrade);
    }
  }

  if (position) {
    const lastTs = currentQuote?.tsMs || position.entryTsMs;
    const exitPrice = currentQuote?.bidPrice || position.entryPrice;
    exitPosition(exitPrice, lastTs, 'end_of_day');
  }

  return {
    strategy: settings.name,
    settings,
    summary: summarizePassiveTrades(outputTrades, counts),
    trades: outputTrades,
  };
}

module.exports = {
  defaultSettings,
  simulatePassiveMarketMaking,
  summarizePassiveTrades,
};
