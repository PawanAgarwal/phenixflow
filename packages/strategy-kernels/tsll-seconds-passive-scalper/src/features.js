function finite(value, fallback = 0) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function nullableFinite(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function centsToPrice(cents) {
  return finite(cents) / 100;
}

function round(value, digits = 4) {
  return typeof value === 'number' && Number.isFinite(value) ? Number(value.toFixed(digits)) : value;
}

function safeReturn(current, previous) {
  if (!Number.isFinite(current) || !Number.isFinite(previous) || previous <= 0) return 0;
  return (current / previous) - 1;
}

function marketContextFromState(state, symbol) {
  const item = state?.contextBySymbol?.[String(symbol || '').toUpperCase()];
  if (!item) return null;
  return item;
}

function payloadValue(payload, features, names, fallback = 0) {
  for (const name of names) {
    if (features && features[name] !== undefined) return features[name];
    if (payload && payload[name] !== undefined) return payload[name];
  }
  return fallback;
}

function deriveRollingFeatures(state, payload, settings) {
  const history = Array.isArray(state?.primaryBars) ? state.primaryBars : [];
  const barSeconds = Math.max(1, Math.trunc(finite(settings?.barSeconds, 1)));
  const lookback60 = Math.max(1, Math.round(60 / barSeconds));
  const lookback180 = Math.max(1, Math.round(180 / barSeconds));
  const currentClose = finite(payload.close, null);
  const prev1 = history.at(-1);
  const prev3 = history.at(-3);
  const prev60 = history.at(-lookback60);
  const prev180 = history.at(-lookback180);
  const prior12 = history.slice(Math.max(0, history.length - 12));
  const prior36 = history.slice(Math.max(0, history.length - 36));
  const high12 = prior12.length ? Math.max(...prior12.map((item) => item.high)) : finite(payload.high, 0);
  const low12 = prior12.length ? Math.min(...prior12.map((item) => item.low)) : finite(payload.low, 0);
  const high36 = prior36.length ? Math.max(...prior36.map((item) => item.high)) : finite(payload.high, 0);
  const low36 = prior36.length ? Math.min(...prior36.map((item) => item.low)) : finite(payload.low, 0);
  return {
    ret_1bar_cents: Number.isFinite(prev1?.close) && Number.isFinite(currentClose) ? (currentClose - prev1.close) * 100 : 0,
    ret_3bar_cents: Number.isFinite(prev3?.close) && Number.isFinite(currentClose) ? (currentClose - prev3.close) * 100 : 0,
    ret_60s_cents: Number.isFinite(prev60?.close) && Number.isFinite(currentClose) ? (currentClose - prev60.close) * 100 : 0,
    ret_180s_cents: Number.isFinite(prev180?.close) && Number.isFinite(currentClose) ? (currentClose - prev180.close) * 100 : 0,
    prev_high_60s: high12,
    prev_low_60s: low12,
    prev_high_180s: high36,
    prev_low_180s: low36,
    range_60s_cents: high12 && low12 ? (high12 - low12) * 100 : 0,
  };
}

function featureRowFromBarEvent(state, event, settings) {
  const payload = event?.payload || {};
  const features = payload.features || {};
  const rolling = deriveRollingFeatures(state, payload, settings);
  const spyContext = marketContextFromState(state, 'SPY');
  const qqqContext = marketContextFromState(state, 'QQQ');
  const tslaContext = marketContextFromState(state, 'TSLA');
  const spyRet1m = finite(payloadValue(payload, features, ['spy_ret_1m', 'spyRet1m'], spyContext?.ret_1m ?? 0));
  const qqqRet1m = finite(payloadValue(payload, features, ['qqq_ret_1m', 'qqqRet1m'], qqqContext?.ret_1m ?? 0));
  const tslaRet1m = finite(payloadValue(payload, features, ['tsla_ret_1m', 'tslaRet1m'], tslaContext?.ret_1m ?? 0));
  const marketOkDefault = spyRet1m > -0.0005 && qqqRet1m > -0.0007 ? 1 : 0;
  return {
    tradeDate: payload.tradeDate || payload.dayIso || event.tradeDate || String(event.eventTime || '').slice(0, 10),
    tsUtc: payload.tsUtc || event.eventTime || null,
    tsMs: nullableFinite(payload.tsMs),
    symbol: event.symbol || payload.symbol || settings?.symbol || 'TSLL',
    sequence: finite(event.sequence, 0),
    open: finite(payload.open, 0),
    high: finite(payload.high, 0),
    low: finite(payload.low, 0),
    close: finite(payload.close, 0),
    volume: finite(payload.volume, 0),
    trade_count: finite(payloadValue(payload, features, ['trade_count', 'tradeCount'], 0)),
    vwap: finite(payload.vwap, finite(payload.close, 0)),
    minutes_from_open: finite(payloadValue(payload, features, ['minutes_from_open', 'minutesFromOpen'], 0)),
    minutes_to_close: finite(payloadValue(payload, features, ['minutes_to_close', 'minutesToClose'], 0)),
    ret_1bar_cents: finite(payloadValue(payload, features, ['ret_1bar_cents', 'ret1barCents'], rolling.ret_1bar_cents)),
    ret_3bar_cents: finite(payloadValue(payload, features, ['ret_3bar_cents', 'ret3barCents'], rolling.ret_3bar_cents)),
    ret_60s_cents: finite(payloadValue(payload, features, ['ret_60s_cents', 'ret60sCents'], rolling.ret_60s_cents)),
    ret_180s_cents: finite(payloadValue(payload, features, ['ret_180s_cents', 'ret180sCents'], rolling.ret_180s_cents)),
    range_60s_cents: finite(payloadValue(payload, features, ['range_60s_cents', 'range60sCents'], rolling.range_60s_cents)),
    spy_ret_1m: spyRet1m,
    qqq_ret_1m: qqqRet1m,
    tsla_ret_1m: tslaRet1m,
    spy_ret_5m: finite(payloadValue(payload, features, ['spy_ret_5m', 'spyRet5m'], spyContext?.ret_5m ?? 0)),
    qqq_ret_5m: finite(payloadValue(payload, features, ['qqq_ret_5m', 'qqqRet5m'], qqqContext?.ret_5m ?? 0)),
    tsla_ret_5m: finite(payloadValue(payload, features, ['tsla_ret_5m', 'tslaRet5m'], tslaContext?.ret_5m ?? 0)),
    market_ok_1m: finite(payloadValue(payload, features, ['market_ok_1m', 'marketOk1m'], marketOkDefault)),
    daily_context_ready: finite(payloadValue(payload, features, ['daily_context_ready', 'dailyContextReady'], 0)),
    daily_macro_trend_up: finite(payloadValue(payload, features, ['daily_macro_trend_up', 'dailyMacroTrendUp'], 0)),
    daily_tsll_from_prev_close_atr: finite(payloadValue(payload, features, ['daily_tsll_from_prev_close_atr', 'dailyTsllFromPrevCloseAtr'], 0)),
    daily_tsll_range_so_far_atr: finite(payloadValue(payload, features, ['daily_tsll_range_so_far_atr', 'dailyTsllRangeSoFarAtr'], 0)),
    allowEntry: payload.allowEntry !== false,
  };
}

function filterResult(name, actual, threshold, passed) {
  return {
    name,
    actual: round(actual, 8),
    threshold,
    passed: Boolean(passed),
  };
}

function evaluateFilters(row, settings) {
  const filters = [];
  const add = (name, actual, threshold, passed) => {
    const item = filterResult(name, actual, threshold, passed);
    filters.push(item);
    return item.passed;
  };
  let passed = true;
  passed = add('minutes_from_open', row?.minutes_from_open ?? null, `>=${settings.noEntryFirstMinutes}`, row && row.minutes_from_open >= settings.noEntryFirstMinutes) && passed;
  passed = add('minutes_to_close', row?.minutes_to_close ?? null, `>=${settings.noEntryLastMinutes}`, row && row.minutes_to_close >= settings.noEntryLastMinutes) && passed;
  passed = add('minTradeCount', row?.trade_count ?? 0, settings.minTradeCount, (row?.trade_count || 0) >= settings.minTradeCount) && passed;
  passed = add('minRange60sCents', row?.range_60s_cents ?? 0, settings.minRange60sCents, (row?.range_60s_cents || 0) >= settings.minRange60sCents) && passed;
  passed = add('minRet60sCents', row?.ret_60s_cents ?? 0, settings.minRet60sCents, (row?.ret_60s_cents || 0) >= settings.minRet60sCents) && passed;
  passed = add('maxLastBarUpCents', row?.ret_1bar_cents ?? 0, settings.maxLastBarUpCents, (row?.ret_1bar_cents || 0) <= settings.maxLastBarUpCents) && passed;
  if (settings.requireMarketOk) {
    passed = add('market_ok_1m', row?.market_ok_1m ?? 0, 1, row?.market_ok_1m === 1) && passed;
  }
  passed = add('minSpyRet1m', row?.spy_ret_1m ?? 0, settings.minSpyRet1m, (row?.spy_ret_1m || 0) >= settings.minSpyRet1m) && passed;
  passed = add('minQqqRet1m', row?.qqq_ret_1m ?? 0, settings.minQqqRet1m, (row?.qqq_ret_1m || 0) >= settings.minQqqRet1m) && passed;
  passed = add('minTslaRet1m', row?.tsla_ret_1m ?? 0, settings.minTslaRet1m, (row?.tsla_ret_1m || 0) >= settings.minTslaRet1m) && passed;
  if (settings.requireDailyContext) {
    passed = add('daily_context_ready', row?.daily_context_ready ?? 0, 1, row?.daily_context_ready === 1) && passed;
  }
  if (settings.requireDailyMacroTrend) {
    passed = add('daily_macro_trend_up', row?.daily_macro_trend_up ?? 0, 1, row?.daily_macro_trend_up === 1) && passed;
  }
  if (settings.maxAbsFromPrevCloseAtr !== null && settings.maxAbsFromPrevCloseAtr !== undefined) {
    passed = add('maxAbsFromPrevCloseAtr', Math.abs(row?.daily_tsll_from_prev_close_atr || 0), settings.maxAbsFromPrevCloseAtr, Math.abs(row?.daily_tsll_from_prev_close_atr || 0) <= settings.maxAbsFromPrevCloseAtr) && passed;
  }
  if (settings.maxRangeSoFarAtr !== null && settings.maxRangeSoFarAtr !== undefined) {
    passed = add('maxRangeSoFarAtr', row?.daily_tsll_range_so_far_atr || 0, settings.maxRangeSoFarAtr, (row?.daily_tsll_range_so_far_atr || 0) <= settings.maxRangeSoFarAtr) && passed;
  }
  return { passed: Boolean(row && passed), filters };
}

function compactFeatureTrace(row) {
  if (!row) return {};
  return {
    close: round(row.close, 4),
    ret1barCents: round(row.ret_1bar_cents, 4),
    ret60sCents: round(row.ret_60s_cents, 4),
    range60sCents: round(row.range_60s_cents, 4),
    tradeCount: row.trade_count,
    marketOk1m: row.market_ok_1m === 1,
    spyRet1m: round(row.spy_ret_1m, 8),
    qqqRet1m: round(row.qqq_ret_1m, 8),
    tslaRet1m: round(row.tsla_ret_1m, 8),
    minutesFromOpen: row.minutes_from_open,
    minutesToClose: row.minutes_to_close,
  };
}

function evaluateBacktestExit(state, barEvent, settings) {
  const position = state?.position;
  if (!position || !barEvent || barEvent.eventType !== 'BAR_1S_CLOSED') return null;
  const payload = barEvent.payload || {};
  const tradeDate = payload.tradeDate || payload.dayIso || String(barEvent.eventTime || '').slice(0, 10);
  if (position.tradeDate && tradeDate && tradeDate !== position.tradeDate) {
    const lastBar = state.lastPrimaryBar || {};
    return {
      exitReason: 'timeout',
      exitPrice: lastBar.close ?? position.entryFillPrice,
      exitSequence: lastBar.sequence ?? position.entrySequence,
      exitTime: lastBar.tsUtc || lastBar.eventTime || barEvent.eventTime,
      crossedSessionBoundary: true,
    };
  }
  const high = finite(payload.high, 0);
  const low = finite(payload.low, 0);
  const close = finite(payload.close, position.entryFillPrice);
  const sequence = finite(barEvent.sequence, position.entrySequence);
  const targetPrice = finite(position.targetPrice, position.entryFillPrice + centsToPrice(settings.targetCents));
  const stopPrice = finite(position.stopPrice, position.entryFillPrice - centsToPrice(settings.stopCents));
  const throughPrice = centsToPrice(settings.throughCents);
  const targetTouched = high >= targetPrice + throughPrice;
  const stopTouched = low <= stopPrice;
  if (targetTouched && stopTouched) {
    if ((settings.sameBarTargetStopPriority || 'stop_first') === 'target_first') {
      return { exitReason: 'target_same_bar', exitPrice: targetPrice, exitSequence: sequence, exitTime: barEvent.eventTime };
    }
    return { exitReason: 'stop_same_bar', exitPrice: stopPrice, exitSequence: sequence, exitTime: barEvent.eventTime };
  }
  if (stopTouched) return { exitReason: 'stop', exitPrice: stopPrice, exitSequence: sequence, exitTime: barEvent.eventTime };
  if (targetTouched) return { exitReason: 'target', exitPrice: targetPrice, exitSequence: sequence, exitTime: barEvent.eventTime };
  const maxHoldBars = Math.max(0, Math.trunc(finite(settings.maxHoldBars, 0)));
  if (sequence >= position.entrySequence + maxHoldBars) {
    return { exitReason: 'timeout', exitPrice: close, exitSequence: sequence, exitTime: barEvent.eventTime };
  }
  return null;
}

module.exports = {
  centsToPrice,
  compactFeatureTrace,
  evaluateBacktestExit,
  evaluateFilters,
  featureRowFromBarEvent,
  finite,
  round,
  safeReturn,
};
