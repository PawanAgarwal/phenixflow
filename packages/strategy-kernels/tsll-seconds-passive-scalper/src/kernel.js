const {
  clone,
  sha256Canonical,
  sha256Jsonl,
  withContentHashId,
} = require('./canonical');
const {
  centsToPrice,
  compactFeatureTrace,
  evaluateEarlyExit,
  evaluateBacktestExit,
  evaluateFilters,
  featureRowFromBarEvent,
  finite,
  resolveSettingsForRow,
  round,
  safeReturn,
} = require('./features');

const SCHEMA_VERSION = 'phenixflow.strategyKernel.v1';
const STRATEGY_ID = 'tsll-seconds-passive-scalper';
const STRATEGY_VERSION = '2026.05.31';
const KERNEL_ID = 'tsll-seconds-passive-scalper.execution.v1';
const KERNEL_API = 'phenixflow.kernel.module.v1';
const TIMEZONE = 'America/New_York';
const DEFAULT_SYMBOL = 'TSLL';
const HISTORY_LIMIT = 240;

function describe() {
  return {
    schemaVersion: SCHEMA_VERSION,
    strategyId: STRATEGY_ID,
    strategyVersion: STRATEGY_VERSION,
    kernelId: KERNEL_ID,
    kernelApi: KERNEL_API,
    timingClass: 'SCALP',
    timezone: TIMEZONE,
    deterministic: true,
  };
}

function normalizeSettings(settings) {
  if (!settings || typeof settings !== 'object' || Array.isArray(settings)) {
    throw new Error('invalid_kernel_settings:settings_required');
  }
  const barSeconds = Math.max(1, Math.trunc(finite(settings.barSeconds, 1)));
  const maxHoldBars = Math.max(0, Math.trunc(finite(settings.maxHoldBars, finite(settings.maxHoldSeconds, 10) / barSeconds)));
  const cooldownBars = Math.max(0, Math.trunc(finite(settings.cooldownBars, finite(settings.cooldownSeconds, 2) / barSeconds)));
  const normalized = {
    ...settings,
    symbol: settings.symbol || DEFAULT_SYMBOL,
    barSeconds,
    maxHoldBars,
    maxHoldSeconds: finite(settings.maxHoldSeconds, maxHoldBars * barSeconds),
    cooldownBars,
    cooldownSeconds: finite(settings.cooldownSeconds, cooldownBars * barSeconds),
    entryLifetimeSeconds: finite(settings.entryLifetimeSeconds, barSeconds),
    sameBarTargetStopPriority: settings.sameBarTargetStopPriority || 'stop_first',
    contextSymbols: Array.isArray(settings.contextSymbols) ? settings.contextSymbols : ['SPY', 'QQQ', 'TSLA'],
    missingContextPolicy: settings.missingContextPolicy || {
      backtest: 'neutral',
      paper: 'fail_closed',
      live: 'fail_closed',
    },
  };
  if (settings.sessionProfiles && typeof settings.sessionProfiles === 'object') {
    const normalizeProfile = (profile) => {
      const profileMaxHoldBars = Math.max(0, Math.trunc(finite(profile.maxHoldBars, finite(profile.maxHoldSeconds, normalized.maxHoldSeconds) / barSeconds)));
      const profileCooldownBars = Math.max(0, Math.trunc(finite(profile.cooldownBars, finite(profile.cooldownSeconds, normalized.cooldownSeconds) / barSeconds)));
      return {
        ...profile,
        barSeconds,
        maxHoldBars: profileMaxHoldBars,
        maxHoldSeconds: finite(profile.maxHoldSeconds, profileMaxHoldBars * barSeconds),
        cooldownBars: profileCooldownBars,
        cooldownSeconds: finite(profile.cooldownSeconds, profileCooldownBars * barSeconds),
        entryLifetimeSeconds: finite(profile.entryLifetimeSeconds, normalized.entryLifetimeSeconds),
        sameBarTargetStopPriority: profile.sameBarTargetStopPriority || normalized.sameBarTargetStopPriority,
      };
    };
    normalized.sessionProfiles = Array.isArray(settings.sessionProfiles)
      ? settings.sessionProfiles.map(normalizeProfile)
      : Object.fromEntries(Object.entries(settings.sessionProfiles).map(([key, profile]) => [key, normalizeProfile(profile)]));
  }
  return normalized;
}

function addSeconds(timestamp, seconds) {
  const ms = Date.parse(timestamp || '');
  if (!Number.isFinite(ms)) return null;
  return new Date(ms + (seconds * 1000)).toISOString();
}

function sessionDateFromClock(clock) {
  if (clock?.sessionDate) return clock.sessionDate;
  if (clock?.eventTime) return String(clock.eventTime).slice(0, 10);
  return null;
}

function initialKernelState({ settings, initialState = null, mode = 'paper', clock = {} }) {
  const normalizedSettings = normalizeSettings(settings);
  if (initialState) return clone(initialState);
  return {
    schemaVersion: 'phenixflow.kernelState.v1',
    strategyId: STRATEGY_ID,
    strategyVersion: STRATEGY_VERSION,
    kernelVersion: KERNEL_ID,
    mode,
    clock: {
      timezone: clock.timezone || TIMEZONE,
      sessionDate: sessionDateFromClock(clock),
    },
    settings: normalizedSettings,
    settingsSha256: sha256Canonical(normalizedSettings),
    session: {
      active: false,
      tradeDate: sessionDateFromClock(clock),
    },
    contextBySymbol: {},
    primaryBars: [],
    lastPrimaryBar: null,
    pendingEntry: null,
    position: null,
    cooldownUntilSequence: null,
    eventCount: 0,
  };
}

function createKernel(input = {}) {
  const state = initialKernelState(input);
  return {
    state,
    traces: [{
      schemaVersion: 'phenixflow.kernelTrace.v1',
      traceId: sha256Canonical({
        reason: 'kernel_created',
        stateAfterSha256: sha256Canonical(state),
      }),
      strategyId: STRATEGY_ID,
      strategyVersion: STRATEGY_VERSION,
      kernelVersion: KERNEL_ID,
      eventId: null,
      eventSha256: null,
      stateBeforeSha256: null,
      stateAfterSha256: sha256Canonical(state),
      decisionSha256: sha256Jsonl([]),
      filters: [],
      features: {},
      reason: 'kernel_created',
    }],
  };
}

function ensureEventEnvelope(event) {
  if (!event || typeof event !== 'object' || Array.isArray(event)) {
    throw new Error('invalid_kernel_event:event_must_be_object');
  }
  if (!event.eventType) throw new Error('invalid_kernel_event:eventType_required');
  const normalized = clone(event);
  normalized.schemaVersion = normalized.schemaVersion || 'phenixflow.kernelEvent.v1';
  normalized.strategyId = normalized.strategyId || STRATEGY_ID;
  normalized.strategyVersion = normalized.strategyVersion || STRATEGY_VERSION;
  normalized.kernelVersion = normalized.kernelVersion || KERNEL_ID;
  normalized.symbol = normalized.symbol || normalized.payload?.symbol || DEFAULT_SYMBOL;
  normalized.sequence = finite(normalized.sequence, 0);
  normalized.eventTime = normalized.eventTime || normalized.payload?.tsUtc || null;
  normalized.eventId = normalized.eventId || sha256Canonical({
    schemaVersion: normalized.schemaVersion,
    eventType: normalized.eventType,
    strategyId: normalized.strategyId,
    strategyVersion: normalized.strategyVersion,
    kernelVersion: normalized.kernelVersion,
    symbol: normalized.symbol,
    sequence: normalized.sequence,
    eventTime: normalized.eventTime,
    payload: normalized.payload || {},
  });
  return normalized;
}

function decisionEnvelope(event, decisionType, payload, traceId = null) {
  const base = {
    schemaVersion: 'phenixflow.kernelDecision.v1',
    decisionType,
    strategyId: STRATEGY_ID,
    strategyVersion: STRATEGY_VERSION,
    kernelVersion: KERNEL_ID,
    symbol: event.symbol || payload?.symbol || DEFAULT_SYMBOL,
    eventId: event.eventId,
    decisionTime: event.eventTime,
    expiresAt: payload?.entryLifetimeSeconds ? addSeconds(event.eventTime, payload.entryLifetimeSeconds) : null,
    idempotencyKey: `${STRATEGY_ID}:${STRATEGY_VERSION}:${event.sequence}:${decisionType}`,
    payload,
    traceId,
  };
  if (!base.expiresAt) delete base.expiresAt;
  return withContentHashId(base, 'decisionId');
}

function noopDecision(event, reason) {
  return decisionEnvelope(event, 'NOOP', { reason });
}

function appendPrimaryBar(state, row, event) {
  const next = {
    sequence: row.sequence,
    eventId: event.eventId,
    eventTime: event.eventTime,
    tsUtc: row.tsUtc || event.eventTime,
    tradeDate: row.tradeDate,
    open: round(row.open, 6),
    high: round(row.high, 6),
    low: round(row.low, 6),
    close: round(row.close, 6),
    volume: round(row.volume, 4),
  };
  state.primaryBars = [...(state.primaryBars || []), next].slice(-HISTORY_LIMIT);
  state.lastPrimaryBar = next;
}

function updateContextBar(state, event) {
  const symbol = String(event.symbol || event.payload?.symbol || '').toUpperCase();
  if (!symbol) return;
  const payload = event.payload || {};
  const previous = state.contextBySymbol[symbol];
  const close = finite(payload.close, previous?.close ?? 0);
  state.contextBySymbol[symbol] = {
    symbol,
    sequence: event.sequence,
    eventTime: event.eventTime,
    close,
    volume: finite(payload.volume, 0),
    ret_1m: finite(payload.ret_1m ?? payload.ret1 ?? payload.ret1m, safeReturn(close, previous?.close)),
    ret_5m: finite(payload.ret_5m ?? payload.ret5 ?? payload.ret5m, 0),
    ret_15m: finite(payload.ret_15m ?? payload.ret15 ?? payload.ret15m, 0),
    stale: event.quality?.stale === true,
  };
}

function contextPolicyForMode(settings, mode) {
  const policy = settings.missingContextPolicy || {};
  return policy[mode] || policy.default || (mode === 'backtest' ? 'neutral' : 'fail_closed');
}

function contextStatus(state, event, row, settings) {
  const policy = contextPolicyForMode(settings, state.mode);
  if (policy !== 'fail_closed') {
    return {
      ok: true,
      policy,
      reason: 'context_neutral_defaults',
      missing: [],
      stale: [],
    };
  }
  const missing = [];
  const stale = [];
  const payload = event.payload || {};
  (settings.contextSymbols || []).forEach((symbol) => {
    const key = symbol.toLowerCase();
    const hasPayloadRet = payload[`${key}_ret_1m`] !== undefined
      || payload.features?.[`${key}_ret_1m`] !== undefined
      || payload.features?.[`${key}Ret1m`] !== undefined;
    const context = state.contextBySymbol?.[symbol];
    if (!hasPayloadRet && !context) missing.push(symbol);
    if (context?.stale) stale.push(symbol);
  });
  if (event.quality?.stale === true) stale.push(row.symbol || event.symbol);
  return {
    ok: missing.length === 0 && stale.length === 0,
    policy,
    reason: missing.length ? 'missing_context_fail_closed' : (stale.length ? 'stale_context_fail_closed' : 'context_ready'),
    missing,
    stale,
  };
}

function inCooldown(state, row) {
  return state.cooldownUntilSequence !== null
    && state.cooldownUntilSequence !== undefined
    && row.sequence <= state.cooldownUntilSequence;
}

function compactTradeSettings(settings) {
  return {
    profileId: settings.profileId || settings._profileId || null,
    profileName: settings.profileName || settings._profileName || null,
    buyBelowCloseCents: settings.buyBelowCloseCents,
    targetCents: settings.targetCents,
    stopCents: settings.stopCents,
    maxHoldBars: settings.maxHoldBars,
    maxHoldSeconds: settings.maxHoldSeconds,
    cooldownBars: settings.cooldownBars,
    cooldownSeconds: settings.cooldownSeconds,
    throughCents: settings.throughCents,
    costCentsPerSide: settings.costCentsPerSide,
    sameBarTargetStopPriority: settings.sameBarTargetStopPriority,
    earlyExit: settings.earlyExit || null,
  };
}

function buildEntryDecision(state, event, row, tradeSettings) {
  const settings = tradeSettings || state.settings;
  const limitPrice = row.close - centsToPrice(settings.buyBelowCloseCents);
  const decision = decisionEnvelope(event, 'PLACE_ENTRY_LIMIT', {
    side: 'BUY',
    quantityPolicy: {
      type: 'budget_or_max_shares',
      maxShares: settings.maxPositionShares || 1000,
    },
    limitPrice,
    entryReference: {
      type: 'prior_completed_1s_close',
      price: round(row.close, 4),
    },
    entryLifetimeSeconds: settings.entryLifetimeSeconds,
    signalSequence: row.sequence,
    signalEventId: event.eventId,
    signalTradeDate: row.tradeDate,
    signalTsUtc: row.tsUtc || event.eventTime,
    signalClose: round(row.close, 4),
    profileId: settings._profileId || null,
    profileName: settings._profileName || null,
    sessionName: row.sessionName || settings.sessionName || null,
    tradeSettings: compactTradeSettings(settings),
  });
  state.pendingEntry = {
    orderDecisionId: decision.decisionId,
    signalSequence: row.sequence,
    signalEventId: event.eventId,
    signalTradeDate: row.tradeDate,
    signalTsUtc: row.tsUtc || event.eventTime,
    signalClose: round(row.close, 4),
    limitPrice,
    expiresSequence: row.sequence + 1,
    expiresAt: decision.expiresAt || addSeconds(event.eventTime, settings.entryLifetimeSeconds),
    profileId: settings._profileId || null,
    profileName: settings._profileName || null,
    sessionName: row.sessionName || settings.sessionName || null,
    tradeSettings: compactTradeSettings(settings),
  };
  return decision;
}

function positionRulesDecision(state, event, fillPrice, tradeSettings = state.settings) {
  const settings = tradeSettings || state.settings;
  return decisionEnvelope(event, 'REGISTER_POSITION_RULES', {
    entryFillPrice: fillPrice,
    targetPrice: fillPrice + centsToPrice(settings.targetCents),
    stopPrice: fillPrice - centsToPrice(settings.stopCents),
    maxHoldBars: settings.maxHoldBars,
    maxHoldSeconds: settings.maxHoldSeconds,
    sameBarTargetStopPriority: settings.sameBarTargetStopPriority,
    cooldownBars: settings.cooldownBars,
    cooldownSeconds: settings.cooldownSeconds,
    profileId: settings.profileId || settings._profileId || null,
    profileName: settings.profileName || settings._profileName || null,
    tradeSettings: compactTradeSettings(settings),
  });
}

function exitTimeoutDecision(state, event, tradeSettings = state.settings) {
  const settings = tradeSettings || state.settings;
  return decisionEnvelope(event, 'EXIT_TIMEOUT', {
    side: 'SELL',
    reason: 'max_hold_elapsed',
    positionEntrySequence: state.position?.entrySequence,
    maxHoldBars: settings.maxHoldBars,
    profileId: state.position?.profileId || settings._profileId || null,
  });
}

function flattenDecision(event, reason, extra = {}) {
  return decisionEnvelope(event, 'FLATTEN_POSITION', {
    side: 'SELL',
    reason,
    ...extra,
  });
}

function handleBarClosed(state, event) {
  const decisions = [];
  let reason = 'bar_processed';
  let filters = [];
  const row = featureRowFromBarEvent(state, event, state.settings);
  const activeSettings = resolveSettingsForRow(state.settings, row);
  const context = contextStatus(state, event, row, state.settings);

  if (state.pendingEntry && event.sequence > state.pendingEntry.expiresSequence) {
    decisions.push(decisionEnvelope(event, 'CANCEL_ENTRY', {
      reason: 'entry_lifetime_expired',
      orderDecisionId: state.pendingEntry.orderDecisionId,
    }));
    state.pendingEntry = null;
  }

  if (state.position) {
    const positionSettings = state.position.tradeSettings || activeSettings;
    reason = 'position_open';
    const earlyExit = evaluateEarlyExit({
      row,
      position: state.position,
      settings: positionSettings,
      sequence: event.sequence,
    });
    const timeoutAt = state.position.entrySequence + positionSettings.maxHoldBars;
    if (earlyExit) {
      decisions.push(flattenDecision(event, earlyExit.exitReason, {
        positionEntrySequence: state.position.entrySequence,
        exitPrice: earlyExit.exitPrice,
        profileId: state.position.profileId || positionSettings.profileId || null,
      }));
      reason = earlyExit.exitReason;
    } else if (event.sequence >= timeoutAt) {
      decisions.push(exitTimeoutDecision(state, event, positionSettings));
      reason = 'timeout_rule_due';
    }
  } else if (state.pendingEntry) {
    reason = 'entry_pending';
  } else if (!row.allowEntry) {
    reason = 'backtest_signal_window_closed';
  } else if (inCooldown(state, row)) {
    reason = 'cooldown_active';
  } else if (!context.ok) {
    reason = context.reason;
    filters.push({
      name: 'context_ready',
      actual: { missing: context.missing, stale: context.stale, policy: context.policy },
      threshold: 'all_required_context_fresh',
      passed: false,
    });
  } else {
    const result = evaluateFilters(row, state.settings);
    filters = result.filters;
    if (result.passed) {
      decisions.push(buildEntryDecision(state, event, row, result.settings));
      reason = 'entry_filters_passed';
    } else {
      reason = 'entry_filters_failed';
    }
  }

  appendPrimaryBar(state, row, event);
  return {
    decisions,
    trace: {
      filters,
      features: {
        ...compactFeatureTrace(row, state.settings),
        contextPolicy: context.policy,
        missingContext: context.missing,
        staleContext: context.stale,
      },
      reason,
    },
  };
}

function handleOrderFilled(state, event) {
  const payload = event.payload || {};
  const side = String(payload.side || '').toUpperCase();
  const fillType = payload.fillType || (side === 'BUY' ? 'entry' : 'exit');
  const decisions = [];
  let reason = 'order_filled';
  if (fillType === 'entry' || side === 'BUY') {
    const pending = state.pendingEntry || {};
    const tradeSettings = pending.tradeSettings || state.settings;
    const fillPrice = finite(payload.fillPrice ?? payload.price, pending.limitPrice);
    const signalSequence = finite(payload.signalSequence, pending.signalSequence ?? event.sequence - 1);
    const tradeDate = payload.tradeDate || pending.signalTradeDate || String(event.eventTime || '').slice(0, 10);
    state.pendingEntry = null;
    state.position = {
      symbol: event.symbol || state.settings.symbol,
      side: 'LONG',
      status: 'OPEN',
      entryFillPrice: fillPrice,
      entrySequence: finite(payload.entrySequence, event.sequence),
      entryEventId: event.eventId,
      entryTsUtc: payload.entryTsUtc || event.eventTime,
      signalSequence,
      signalEventId: payload.signalEventId || pending.signalEventId || null,
      signalTsUtc: payload.signalTsUtc || pending.signalTsUtc || null,
      signalClose: round(finite(payload.signalClose, pending.signalClose), 4),
      tradeDate,
      targetPrice: fillPrice + centsToPrice(tradeSettings.targetCents),
      stopPrice: fillPrice - centsToPrice(tradeSettings.stopCents),
      profileId: pending.profileId || tradeSettings.profileId || null,
      profileName: pending.profileName || tradeSettings.profileName || null,
      sessionName: pending.sessionName || tradeSettings.sessionName || null,
      tradeSettings,
    };
    decisions.push(positionRulesDecision(state, event, fillPrice, tradeSettings));
    reason = 'entry_filled';
  } else {
    const position = state.position;
    state.position = null;
    if (position) {
      const cooldownBars = Math.max(0, Math.trunc(finite(position.tradeSettings?.cooldownBars, state.settings.cooldownBars)));
      state.cooldownUntilSequence = finite(payload.exitSequence, event.sequence) + cooldownBars;
      reason = payload.exitReason || 'exit_filled';
    } else {
      reason = 'exit_fill_without_open_position';
    }
  }
  return {
    decisions,
    trace: {
      filters: [],
      features: {},
      reason,
    },
  };
}

function handleOrderClosed(state, event) {
  const payload = event.payload || {};
  if (!state.pendingEntry || !payload.orderDecisionId || payload.orderDecisionId === state.pendingEntry.orderDecisionId) {
    state.pendingEntry = null;
  }
  return {
    decisions: [],
    trace: {
      filters: [],
      features: {},
      reason: event.eventType === 'ORDER_REJECTED' ? 'entry_rejected' : 'entry_cancelled',
    },
  };
}

function handleSessionEnded(state, event) {
  const decisions = [];
  state.pendingEntry = null;
  if (state.position) {
    decisions.push(flattenDecision(event, 'session_ended', {
      positionEntrySequence: state.position.entrySequence,
      profileId: state.position.profileId || null,
    }));
  }
  state.session = {
    ...state.session,
    active: false,
    endedAt: event.eventTime,
  };
  return {
    decisions,
    trace: {
      filters: [],
      features: {},
      reason: state.position ? 'session_end_flatten_required' : 'session_ended',
    },
  };
}

function handleTimer(state, event) {
  const decisions = [];
  let reason = 'timer_processed';
  const positionSettings = state.position?.tradeSettings || state.settings;
  if (state.position && event.sequence >= state.position.entrySequence + positionSettings.maxHoldBars) {
    decisions.push(exitTimeoutDecision(state, event, positionSettings));
    reason = 'timeout_rule_due';
  }
  return {
    decisions,
    trace: { filters: [], features: {}, reason },
  };
}

function reduceEvent(state, event) {
  switch (event.eventType) {
    case 'SESSION_STARTED':
      state.session = {
        active: true,
        tradeDate: event.payload?.tradeDate || String(event.eventTime || '').slice(0, 10),
        startedAt: event.eventTime,
      };
      state.pendingEntry = null;
      state.position = null;
      state.cooldownUntilSequence = null;
      return { decisions: [], trace: { filters: [], features: {}, reason: 'session_started' } };
    case 'SESSION_ENDED':
      return handleSessionEnded(state, event);
    case 'BAR_1M_CLOSED':
      updateContextBar(state, event);
      return { decisions: [], trace: { filters: [], features: {}, reason: 'context_bar_processed' } };
    case 'BAR_1S_CLOSED':
      return handleBarClosed(state, event);
    case 'ORDER_FILLED':
      return handleOrderFilled(state, event);
    case 'ORDER_CANCELLED':
    case 'ORDER_REJECTED':
      return handleOrderClosed(state, event);
    case 'TIMER':
      return handleTimer(state, event);
    default:
      return { decisions: [noopDecision(event, 'unsupported_event_type')], trace: { filters: [], features: {}, reason: 'unsupported_event_type' } };
  }
}

function traceEnvelope({ event, stateBefore, stateAfter, decisions, trace }) {
  const base = {
    schemaVersion: 'phenixflow.kernelTrace.v1',
    traceId: sha256Canonical({
      eventId: event.eventId,
      stateBeforeSha256: sha256Canonical(stateBefore),
      stateAfterSha256: sha256Canonical(stateAfter),
      reason: trace.reason || 'event_processed',
    }),
    strategyId: STRATEGY_ID,
    strategyVersion: STRATEGY_VERSION,
    kernelVersion: KERNEL_ID,
    kernelArtifactSha256: null,
    settingsSha256: stateAfter.settingsSha256,
    eventId: event.eventId,
    eventSha256: sha256Canonical(event),
    stateBeforeSha256: sha256Canonical(stateBefore),
    stateAfterSha256: sha256Canonical(stateAfter),
    decisionSha256: sha256Jsonl(decisions),
    filters: trace.filters || [],
    features: trace.features || {},
    reason: trace.reason || 'event_processed',
  };
  return base;
}

function onEvent(inputState, inputEvent) {
  const stateBefore = clone(inputState);
  const state = clone(inputState);
  if (!state || typeof state !== 'object') throw new Error('invalid_kernel_state:state_required');
  state.eventCount = finite(state.eventCount, 0) + 1;
  const event = ensureEventEnvelope(inputEvent);
  const { decisions, trace } = reduceEvent(state, event);
  const traces = [traceEnvelope({
    event,
    stateBefore,
    stateAfter: state,
    decisions,
    trace,
  })];
  const decisionsWithTrace = decisions.map((decision) => {
    const next = {
      ...decision,
      traceId: traces[0].traceId,
    };
    return withContentHashId(next, 'decisionId');
  });
  traces[0].decisionSha256 = sha256Jsonl(decisionsWithTrace);
  return {
    state,
    decisions: decisionsWithTrace,
    traces,
  };
}

function onEventLean(inputState, inputEvent) {
  const state = clone(inputState);
  if (!state || typeof state !== 'object') throw new Error('invalid_kernel_state:state_required');
  state.eventCount = finite(state.eventCount, 0) + 1;
  const event = ensureEventEnvelope(inputEvent);
  const { decisions } = reduceEvent(state, event);
  return {
    state,
    decisions,
    traces: [],
  };
}

function replay({ settings, events, expectedDecisionSha256, expectedTraceSha256, initialState = null, mode = 'backtest', clock = {} }) {
  const created = createKernel({ settings, initialState, mode, clock });
  let state = created.state;
  const allDecisions = [];
  const allTraces = [];
  (events || []).forEach((event) => {
    const result = onEvent(state, event);
    state = result.state;
    allDecisions.push(...result.decisions);
    allTraces.push(...result.traces);
  });
  const actualDecisionSha256 = sha256Jsonl(allDecisions);
  const actualTraceSha256 = sha256Jsonl(allTraces);
  const passed = (!expectedDecisionSha256 || expectedDecisionSha256 === actualDecisionSha256)
    && (!expectedTraceSha256 || expectedTraceSha256 === actualTraceSha256);
  return {
    passed,
    actualDecisionSha256,
    actualTraceSha256,
    expectedDecisionSha256: expectedDecisionSha256 || null,
    expectedTraceSha256: expectedTraceSha256 || null,
    decisions: allDecisions,
    traces: allTraces,
    state,
  };
}

module.exports = {
  KERNEL_ID,
  STRATEGY_ID,
  STRATEGY_VERSION,
  TIMEZONE,
  createKernel,
  describe,
  evaluateBacktestExit,
  evaluateFilters,
  featureRowFromBarEvent,
  normalizeSettings,
  onEvent,
  onEventLean,
  replay,
  resolveSettingsForRow,
};
