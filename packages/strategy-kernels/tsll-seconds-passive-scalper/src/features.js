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

function profileEntries(settings) {
  const profiles = settings?.sessionProfiles;
  if (!profiles) return [];
  const entries = Array.isArray(profiles)
    ? profiles.map((profile, index) => [`profile_${index}`, profile])
    : Object.entries(profiles);
  return entries
    .filter(([, profile]) => profile && typeof profile === 'object' && profile.disabled !== true)
    .sort((left, right) => finite(right[1].priority, 0) - finite(left[1].priority, 0));
}

function rowMinuteOfDayEt(row) {
  const value = row?.minuteOfDayEt ?? row?.minute_of_day_et;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function rowSessionName(row) {
  return String(row?.sessionName || row?.session_name || '').trim().toLowerCase();
}

function profileMinute(profile, primaryName, fallbackName, fallback = null) {
  const value = profile?.[primaryName] ?? profile?.[fallbackName];
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function inMinuteWindow(minute, openMinute, closeMinute) {
  if (!Number.isFinite(minute) || !Number.isFinite(openMinute) || !Number.isFinite(closeMinute)) return true;
  if (closeMinute >= openMinute) return minute >= openMinute && minute < closeMinute;
  return minute >= openMinute || minute < closeMinute;
}

function profileMatchesRow(profile, row, settings) {
  const minute = rowMinuteOfDayEt(row);
  const sessionName = rowSessionName(row);
  if (minute !== null) {
    const openMinute = profileMinute(profile, 'openMinuteEt', 'sessionOpenMinuteEt');
    const closeMinute = profileMinute(profile, 'closeMinuteEt', 'sessionCloseMinuteEt');
    if (!inMinuteWindow(minute, openMinute, closeMinute)) return false;
    if (profile.excludeRegularSession === true) {
      const regularOpen = finite(profile.regularOpenMinuteEt, finite(settings?.regularOpenMinuteEt, 570));
      const regularClose = finite(profile.regularCloseMinuteEt, finite(settings?.regularCloseMinuteEt, 960));
      if (inMinuteWindow(minute, regularOpen, regularClose)) return false;
    }
    if (profile.matchSessionName === true && profile.sessionName && sessionName !== String(profile.sessionName).toLowerCase()) return false;
    return true;
  }
  if (!sessionName) return false;
  const profileNames = [
    profile.profileId,
    profile.sessionName,
    profile.name,
  ].filter(Boolean).map((value) => String(value).toLowerCase());
  return profileNames.includes(sessionName);
}

function resolveSettingsForRow(settings, row) {
  const entries = profileEntries(settings);
  if (!entries.length) return { ...settings, _profileMatched: true };
  const matched = entries.find(([, profile]) => profileMatchesRow(profile, row, settings));
  if (!matched) {
    const hasMinute = rowMinuteOfDayEt(row) !== null;
    return {
      ...settings,
      _profileId: null,
      _profileName: null,
      _profileMatched: !hasMinute,
    };
  }
  const [key, profile] = matched;
  return {
    ...settings,
    ...profile,
    sessionProfiles: settings.sessionProfiles,
    _profileKey: key,
    _profileId: profile.profileId || profile.sessionName || key,
    _profileName: profile.profileName || profile.name || profile.profileId || key,
    _profileMatched: true,
  };
}

function effectiveMinutesFromOpen(row, settings) {
  const minute = rowMinuteOfDayEt(row);
  const openMinute = profileMinute(settings, 'openMinuteEt', 'sessionOpenMinuteEt');
  if (minute !== null && Number.isFinite(openMinute)) return minute - openMinute;
  return row?.minutes_from_open;
}

function effectiveMinutesToClose(row, settings) {
  const minute = rowMinuteOfDayEt(row);
  const closeMinute = profileMinute(settings, 'closeMinuteEt', 'sessionCloseMinuteEt');
  if (minute !== null && Number.isFinite(closeMinute)) return closeMinute - minute - 1;
  return row?.minutes_to_close;
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
    minuteOfDayEt: nullableFinite(payloadValue(payload, features, ['minuteOfDayEt', 'minute_of_day_et'], undefined)),
    secondOfDayEt: nullableFinite(payloadValue(payload, features, ['secondOfDayEt', 'second_of_day_et'], undefined)),
    sessionName: String(payloadValue(payload, features, ['sessionName', 'session_name'], '') || ''),
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
  const activeSettings = resolveSettingsForRow(settings, row);
  const minutesFromOpen = effectiveMinutesFromOpen(row, activeSettings);
  const minutesToClose = effectiveMinutesToClose(row, activeSettings);
  const filters = [];
  const add = (name, actual, threshold, passed) => {
    const item = filterResult(name, actual, threshold, passed);
    filters.push(item);
    return item.passed;
  };
  let passed = true;
  if (profileEntries(settings).length) {
    passed = add('session_profile', activeSettings._profileId || 'none', 'configured_profile', activeSettings._profileMatched === true) && passed;
  }
  passed = add('minutes_from_open', minutesFromOpen ?? null, `>=${activeSettings.noEntryFirstMinutes}`, row && minutesFromOpen >= activeSettings.noEntryFirstMinutes) && passed;
  passed = add('minutes_to_close', minutesToClose ?? null, `>=${activeSettings.noEntryLastMinutes}`, row && minutesToClose >= activeSettings.noEntryLastMinutes) && passed;
  passed = add('minTradeCount', row?.trade_count ?? 0, activeSettings.minTradeCount, (row?.trade_count || 0) >= activeSettings.minTradeCount) && passed;
  passed = add('minRange60sCents', row?.range_60s_cents ?? 0, activeSettings.minRange60sCents, (row?.range_60s_cents || 0) >= activeSettings.minRange60sCents) && passed;
  passed = add('minRet60sCents', row?.ret_60s_cents ?? 0, activeSettings.minRet60sCents, (row?.ret_60s_cents || 0) >= activeSettings.minRet60sCents) && passed;
  if (activeSettings.maxRet60sCents !== null && activeSettings.maxRet60sCents !== undefined) {
    passed = add('maxRet60sCents', row?.ret_60s_cents ?? 0, activeSettings.maxRet60sCents, (row?.ret_60s_cents || 0) <= activeSettings.maxRet60sCents) && passed;
  }
  passed = add('maxLastBarUpCents', row?.ret_1bar_cents ?? 0, activeSettings.maxLastBarUpCents, (row?.ret_1bar_cents || 0) <= activeSettings.maxLastBarUpCents) && passed;
  if (activeSettings.requireMarketOk) {
    passed = add('market_ok_1m', row?.market_ok_1m ?? 0, 1, row?.market_ok_1m === 1) && passed;
  }
  passed = add('minSpyRet1m', row?.spy_ret_1m ?? 0, activeSettings.minSpyRet1m, (row?.spy_ret_1m || 0) >= activeSettings.minSpyRet1m) && passed;
  passed = add('minQqqRet1m', row?.qqq_ret_1m ?? 0, activeSettings.minQqqRet1m, (row?.qqq_ret_1m || 0) >= activeSettings.minQqqRet1m) && passed;
  passed = add('minTslaRet1m', row?.tsla_ret_1m ?? 0, activeSettings.minTslaRet1m, (row?.tsla_ret_1m || 0) >= activeSettings.minTslaRet1m) && passed;
  if (activeSettings.requireDailyContext) {
    passed = add('daily_context_ready', row?.daily_context_ready ?? 0, 1, row?.daily_context_ready === 1) && passed;
  }
  if (activeSettings.requireDailyMacroTrend) {
    passed = add('daily_macro_trend_up', row?.daily_macro_trend_up ?? 0, 1, row?.daily_macro_trend_up === 1) && passed;
  }
  if (activeSettings.maxAbsFromPrevCloseAtr !== null && activeSettings.maxAbsFromPrevCloseAtr !== undefined) {
    passed = add('maxAbsFromPrevCloseAtr', Math.abs(row?.daily_tsll_from_prev_close_atr || 0), activeSettings.maxAbsFromPrevCloseAtr, Math.abs(row?.daily_tsll_from_prev_close_atr || 0) <= activeSettings.maxAbsFromPrevCloseAtr) && passed;
  }
  if (activeSettings.maxRangeSoFarAtr !== null && activeSettings.maxRangeSoFarAtr !== undefined) {
    passed = add('maxRangeSoFarAtr', row?.daily_tsll_range_so_far_atr || 0, activeSettings.maxRangeSoFarAtr, (row?.daily_tsll_range_so_far_atr || 0) <= activeSettings.maxRangeSoFarAtr) && passed;
  }
  return { passed: Boolean(row && passed), filters, settings: activeSettings };
}

function compactFeatureTrace(row, settings = null) {
  if (!row) return {};
  const activeSettings = settings ? resolveSettingsForRow(settings, row) : {};
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
    minuteOfDayEt: row.minuteOfDayEt,
    sessionName: row.sessionName || null,
    profileId: activeSettings._profileId || null,
    minutesFromOpen: settings ? effectiveMinutesFromOpen(row, activeSettings) : row.minutes_from_open,
    minutesToClose: settings ? effectiveMinutesToClose(row, activeSettings) : row.minutes_to_close,
  };
}

function evaluateEarlyExit({ row, position, settings, sequence }) {
  const rule = settings?.earlyExit;
  if (!rule || !position || !row) return null;
  const type = typeof rule === 'string' ? rule : rule.type;
  const holdBars = sequence - finite(position.entrySequence, sequence);
  const entryPrice = finite(position.entryFillPrice, finite(position.entryPrice, row.close));
  if (type === 'stale_loss_exit') {
    const minHoldBars = Math.max(0, Math.trunc(finite(rule.minHoldBars, 8)));
    const maxQqqRet1m = finite(rule.maxQqqRet1m, 0);
    if (holdBars >= minHoldBars && row.close < entryPrice && row.qqq_ret_1m < maxQqqRet1m) {
      return {
        exitReason: rule.reason || 'stale_loss_exit',
        exitPrice: row.close,
      };
    }
  }
  if (type === 'market_weak_exit') {
    const maxQqqRet1m = finite(rule.maxQqqRet1m, -0.0012);
    const maxTslaRet1m = finite(rule.maxTslaRet1m, -0.0025);
    if (row.qqq_ret_1m < maxQqqRet1m || row.tsla_ret_1m < maxTslaRet1m) {
      return {
        exitReason: rule.reason || 'market_weak_exit',
        exitPrice: row.close,
      };
    }
  }
  if (type === 'profit_lock') {
    const minHoldBars = Math.max(0, Math.trunc(finite(rule.minHoldBars, 6)));
    const minGrossCents = finite(rule.minGrossCents, 2);
    if (holdBars >= minHoldBars && (row.close - entryPrice) * 100 >= minGrossCents) {
      return {
        exitReason: rule.reason || 'profit_lock',
        exitPrice: row.close,
      };
    }
  }
  return null;
}

function evaluateBacktestExit(state, barEvent, settings) {
  const position = state?.position;
  if (!position || !barEvent || barEvent.eventType !== 'BAR_1S_CLOSED') return null;
  const payload = barEvent.payload || {};
  const row = featureRowFromBarEvent(state, barEvent, settings);
  const tradeSettings = position.tradeSettings || resolveSettingsForRow(settings, row);
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
  const targetPrice = finite(position.targetPrice, position.entryFillPrice + centsToPrice(tradeSettings.targetCents));
  const stopPrice = finite(position.stopPrice, position.entryFillPrice - centsToPrice(tradeSettings.stopCents));
  const throughPrice = centsToPrice(tradeSettings.throughCents);
  const targetTouched = high >= targetPrice + throughPrice;
  const stopTouched = low <= stopPrice;
  if (targetTouched && stopTouched) {
    if ((tradeSettings.sameBarTargetStopPriority || 'stop_first') === 'target_first') {
      return { exitReason: 'target_same_bar', exitPrice: targetPrice, exitSequence: sequence, exitTime: barEvent.eventTime };
    }
    return { exitReason: 'stop_same_bar', exitPrice: stopPrice, exitSequence: sequence, exitTime: barEvent.eventTime };
  }
  if (stopTouched) return { exitReason: 'stop', exitPrice: stopPrice, exitSequence: sequence, exitTime: barEvent.eventTime };
  if (targetTouched) return { exitReason: 'target', exitPrice: targetPrice, exitSequence: sequence, exitTime: barEvent.eventTime };
  const early = evaluateEarlyExit({ row, position, settings: tradeSettings, sequence });
  if (early) return { ...early, exitSequence: sequence, exitTime: barEvent.eventTime };
  const maxHoldBars = Math.max(0, Math.trunc(finite(tradeSettings.maxHoldBars, 0)));
  if (sequence >= position.entrySequence + maxHoldBars) {
    return { exitReason: 'timeout', exitPrice: close, exitSequence: sequence, exitTime: barEvent.eventTime };
  }
  return null;
}

module.exports = {
  centsToPrice,
  compactFeatureTrace,
  effectiveMinutesFromOpen,
  effectiveMinutesToClose,
  evaluateEarlyExit,
  evaluateBacktestExit,
  evaluateFilters,
  featureRowFromBarEvent,
  finite,
  resolveSettingsForRow,
  round,
  safeReturn,
};
