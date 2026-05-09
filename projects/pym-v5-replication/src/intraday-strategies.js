const SAFE_TICKERS = new Set(['AGG', 'BIL', 'BND', 'BSV', 'IEF', 'IEI', 'IGIB', 'MUB', 'SHV', 'SHY', 'TIP']);
const INVERSE_TICKER_MAP = new Map([
  ['CURE', 'XLV'],
  ['EDC', 'EDZ'],
  ['EDV', 'TMV'],
  ['EDZ', 'EDC'],
  ['EEM', 'EDZ'],
  ['GLL', 'UGL'],
  ['GLD', 'GLL'],
  ['IAU', 'GLL'],
  ['IEF', 'TMV'],
  ['IWM', 'TWM'],
  ['PSQ', 'QQQ'],
  ['QID', 'QLD'],
  ['QLD', 'QID'],
  ['QQQ', 'PSQ'],
  ['ROM', 'QID'],
  ['SH', 'SSO'],
  ['SOXL', 'SOXS'],
  ['SOXS', 'SOXL'],
  ['SOXX', 'SOXS'],
  ['SPXL', 'SPXU'],
  ['SPXU', 'SPXL'],
  ['SPY', 'SH'],
  ['SQQQ', 'TQQQ'],
  ['SSO', 'SH'],
  ['SVIX', 'VIXY'],
  ['SVXY', 'VIXY'],
  ['TECL', 'TECS'],
  ['TECS', 'TECL'],
  ['TLT', 'TMV'],
  ['TMF', 'TMV'],
  ['TMV', 'TMF'],
  ['TQQQ', 'SQQQ'],
  ['TWM', 'UWM'],
  ['UDN', 'UUP'],
  ['UGL', 'GLL'],
  ['UPRO', 'SPXU'],
  ['UUP', 'UDN'],
  ['UVXY', 'SVIX'],
  ['UWM', 'TWM'],
  ['VIXM', 'SVIX'],
  ['VIXY', 'SVIX'],
  ['XLK', 'TECS'],
  ['XLV', 'CURE'],
]);

function normalizeWeights(weights, maxExposure = 1) {
  let total = 0;
  weights.forEach((weight) => {
    if (Number.isFinite(weight) && weight > 0) total += weight;
  });
  if (total <= 0) return new Map();
  const out = new Map();
  weights.forEach((weight, ticker) => {
    if (Number.isFinite(weight) && weight > 0) out.set(ticker, (weight / total) * maxExposure);
  });
  return out;
}

function rawTargetWeights(snapshot) {
  return new Map((snapshot?.holdings || []).map((holding) => [holding.ticker, holding.weight]));
}

function minuteClose(ctx, ticker, minuteEt = ctx.minuteEt) {
  return ctx.dayBars.barsByTicker.get(ticker)?.get(minuteEt)?.close ?? null;
}

function firstClose(ctx, ticker) {
  const bars = ctx.dayBars.barsByTicker.get(ticker);
  if (!bars) return null;
  for (const minute of ctx.minutes) {
    const close = bars.get(minute)?.close;
    if (Number.isFinite(close) && close > 0) return close;
  }
  return null;
}

function returnSinceOpen(ctx, ticker) {
  const start = firstClose(ctx, ticker);
  const close = minuteClose(ctx, ticker);
  if (!Number.isFinite(start) || !Number.isFinite(close) || start <= 0) return null;
  return (close / start) - 1;
}

function returnLookback(ctx, ticker, lookbackMinutes) {
  const current = minuteClose(ctx, ticker);
  const previous = minuteClose(ctx, ticker, ctx.minuteEt - lookbackMinutes);
  if (!Number.isFinite(current) || !Number.isFinite(previous) || previous <= 0) return null;
  return (current / previous) - 1;
}

function intradayVwap(ctx, ticker) {
  const bars = ctx.dayBars.barsByTicker.get(ticker);
  if (!bars) return null;
  let volume = 0;
  let dollars = 0;
  for (const minute of ctx.minutes) {
    if (minute > ctx.minuteEt) break;
    const bar = bars.get(minute);
    if (!bar || !Number.isFinite(bar.close) || !Number.isFinite(bar.volume) || bar.volume <= 0) continue;
    volume += bar.volume;
    dollars += bar.close * bar.volume;
  }
  return volume > 0 ? dollars / volume : null;
}

function aboveVwap(ctx, ticker, threshold = 0) {
  const close = minuteClose(ctx, ticker);
  const vwap = intradayVwap(ctx, ticker);
  if (!Number.isFinite(close) || !Number.isFinite(vwap) || vwap <= 0) return false;
  return (close / vwap) - 1 >= threshold;
}

function targetTickers(ctx) {
  return [...rawTargetWeights(ctx.targetSnapshot).keys()];
}

function fullTarget(ctx) {
  return normalizeWeights(rawTargetWeights(ctx.targetSnapshot), 1);
}

function topTargetWeights(ctx, count) {
  const selected = [...rawTargetWeights(ctx.targetSnapshot).entries()]
    .sort((left, right) => right[1] - left[1] || left[0].localeCompare(right[0]))
    .slice(0, count);
  return normalizeWeights(new Map(selected), 1);
}

function inverseTargetWeights(snapshot) {
  const inverse = new Map();
  rawTargetWeights(snapshot).forEach((weight, ticker) => {
    const inverseTicker = INVERSE_TICKER_MAP.get(ticker);
    if (!inverseTicker) return;
    inverse.set(inverseTicker, (inverse.get(inverseTicker) || 0) + weight);
  });
  return inverse;
}

function inverseTarget(ctx) {
  return normalizeWeights(inverseTargetWeights(ctx.targetSnapshot), 1);
}

function inverseTopMomentum(ctx, { count = 5, lookbackMinutes = 60 } = {}) {
  const inverse = inverseTargetWeights(ctx.targetSnapshot);
  const scored = [...inverse.entries()]
    .map(([ticker, targetWeight]) => ({
      ticker,
      targetWeight,
      score: returnLookback(ctx, ticker, lookbackMinutes),
      openReturn: returnSinceOpen(ctx, ticker),
      vwapOk: aboveVwap(ctx, ticker),
    }))
    .filter((item) => (
      Number.isFinite(item.score)
      && item.score > 0
      && Number.isFinite(item.openReturn)
      && item.openReturn >= -0.002
      && item.vwapOk
    ))
    .sort((left, right) => right.score - left.score)
    .slice(0, count);
  return normalizeWeights(new Map(scored.map((item) => [item.ticker, item.targetWeight])), 1);
}

function inverseRequiredTickers(snapshot) {
  return [...new Set([...inverseTargetWeights(snapshot).keys()])];
}

function vwapGate(ctx, { minOpenReturn = 0, safeFallback = true } = {}) {
  const target = rawTargetWeights(ctx.targetSnapshot);
  const active = new Map();
  target.forEach((weight, ticker) => {
    const openReturn = returnSinceOpen(ctx, ticker);
    if (SAFE_TICKERS.has(ticker) && safeFallback) {
      active.set(ticker, weight);
    } else if (Number.isFinite(openReturn) && openReturn >= minOpenReturn && aboveVwap(ctx, ticker)) {
      active.set(ticker, weight);
    }
  });
  return normalizeWeights(active, 1);
}

function topMomentum(ctx, { count = 3, lookbackMinutes = 15, requireVwap = true } = {}) {
  const target = rawTargetWeights(ctx.targetSnapshot);
  const scored = targetTickers(ctx)
    .map((ticker) => ({
      ticker,
      targetWeight: target.get(ticker) || 0,
      score: returnLookback(ctx, ticker, lookbackMinutes),
      openReturn: returnSinceOpen(ctx, ticker),
      vwapOk: !requireVwap || aboveVwap(ctx, ticker),
    }))
    .filter((item) => (
      Number.isFinite(item.score)
      && item.score > 0
      && Number.isFinite(item.openReturn)
      && item.openReturn >= -0.002
      && item.vwapOk
    ))
    .sort((left, right) => right.score - left.score)
    .slice(0, count);
  const active = new Map(scored.map((item) => [item.ticker, Math.max(0.0001, item.targetWeight)]));
  return normalizeWeights(active, 1);
}

function pullbackRecovery(ctx, { minOpenReturn = -0.006, maxOpenReturn = 0.004 } = {}) {
  const target = rawTargetWeights(ctx.targetSnapshot);
  const active = new Map();
  target.forEach((weight, ticker) => {
    const openReturn = returnSinceOpen(ctx, ticker);
    if (SAFE_TICKERS.has(ticker)) active.set(ticker, weight);
    else if (
      Number.isFinite(openReturn)
      && openReturn >= minOpenReturn
      && openReturn <= maxOpenReturn
      && aboveVwap(ctx, ticker, -0.001)
    ) {
      active.set(ticker, weight);
    }
  });
  return normalizeWeights(active, 1);
}

const STRATEGIES = [
  {
    id: 'pym_full_day_935_1555',
    name: 'PYM target, 9:35-15:55',
    description: 'Uses previous EOD PYM target weights intraday, flat overnight.',
    startMinute: 575,
    endMinute: 955,
    intervalMinutes: 380,
    decide: fullTarget,
  },
  {
    id: 'pym_fade_full_day_935_1555',
    name: 'Fade PYM target, 9:35-15:55',
    description: 'Uses inverse ETF counterparts to fade the previous EOD PYM target intraday, flat overnight.',
    startMinute: 575,
    endMinute: 955,
    intervalMinutes: 380,
    requiredTickers: inverseRequiredTickers,
    decide: inverseTarget,
  },
  {
    id: 'pym_top3_weight_full_day_935_1555',
    name: 'PYM top-3 target weights, 9:35-15:55',
    description: 'Trades only the three largest previous-EOD target weights intraday, flat overnight.',
    startMinute: 575,
    endMinute: 955,
    intervalMinutes: 380,
    decide: (ctx) => topTargetWeights(ctx, 3),
  },
  {
    id: 'pym_top5_weight_full_day_935_1555',
    name: 'PYM top-5 target weights, 9:35-15:55',
    description: 'Trades only the five largest previous-EOD target weights intraday, flat overnight.',
    startMinute: 575,
    endMinute: 955,
    intervalMinutes: 380,
    decide: (ctx) => topTargetWeights(ctx, 5),
  },
  {
    id: 'pym_top3_weight_morning_935_1030',
    name: 'PYM top-3 target weights, morning',
    description: 'Trades the three largest previous-EOD target weights from 9:35 to 10:30.',
    startMinute: 575,
    endMinute: 630,
    intervalMinutes: 55,
    decide: (ctx) => topTargetWeights(ctx, 3),
  },
  {
    id: 'pym_top5_weight_morning_935_1030',
    name: 'PYM top-5 target weights, morning',
    description: 'Trades the five largest previous-EOD target weights from 9:35 to 10:30.',
    startMinute: 575,
    endMinute: 630,
    intervalMinutes: 55,
    decide: (ctx) => topTargetWeights(ctx, 5),
  },
  {
    id: 'pym_top3_weight_afternoon_1430_1555',
    name: 'PYM top-3 target weights, afternoon',
    description: 'Trades the three largest previous-EOD target weights from 14:30 to 15:55.',
    startMinute: 870,
    endMinute: 955,
    intervalMinutes: 85,
    decide: (ctx) => topTargetWeights(ctx, 3),
  },
  {
    id: 'pym_top5_weight_afternoon_1430_1555',
    name: 'PYM top-5 target weights, afternoon',
    description: 'Trades the five largest previous-EOD target weights from 14:30 to 15:55.',
    startMinute: 870,
    endMinute: 955,
    intervalMinutes: 85,
    decide: (ctx) => topTargetWeights(ctx, 5),
  },
  {
    id: 'pym_top3_weight_935_1430',
    name: 'PYM top-3 target weights, 9:35-14:30',
    description: 'Trades the three largest previous-EOD target weights from 9:35 to 14:30, avoiding the weak late-session bucket.',
    startMinute: 575,
    endMinute: 870,
    intervalMinutes: 295,
    decide: (ctx) => topTargetWeights(ctx, 3),
  },
  {
    id: 'pym_top5_weight_935_1430',
    name: 'PYM top-5 target weights, 9:35-14:30',
    description: 'Trades the five largest previous-EOD target weights from 9:35 to 14:30, avoiding the weak late-session bucket.',
    startMinute: 575,
    endMinute: 870,
    intervalMinutes: 295,
    decide: (ctx) => topTargetWeights(ctx, 5),
  },
  {
    id: 'pym_top8_weight_935_1430',
    name: 'PYM top-8 target weights, 9:35-14:30',
    description: 'Trades the eight largest previous-EOD target weights from 9:35 to 14:30, avoiding the weak late-session bucket.',
    startMinute: 575,
    endMinute: 870,
    intervalMinutes: 295,
    decide: (ctx) => topTargetWeights(ctx, 8),
  },
  {
    id: 'pym_top8_weight_full_day_935_1555',
    name: 'PYM top-8 target weights, 9:35-15:55',
    description: 'Trades only the eight largest previous-EOD target weights intraday, flat overnight.',
    startMinute: 575,
    endMinute: 955,
    intervalMinutes: 380,
    decide: (ctx) => topTargetWeights(ctx, 8),
  },
  {
    id: 'pym_vwap_gate_15m',
    name: 'PYM VWAP gate, 15m',
    description: 'Every 15 minutes, holds target tickers above VWAP and positive from open; bond/cash sleeves stay eligible.',
    startMinute: 585,
    endMinute: 955,
    intervalMinutes: 15,
    decide: (ctx) => vwapGate(ctx, { minOpenReturn: 0, safeFallback: true }),
  },
  {
    id: 'pym_morning_full_935_1030',
    name: 'PYM target, morning window',
    description: 'Uses previous EOD PYM target weights from 9:35 to 10:30, then exits.',
    startMinute: 575,
    endMinute: 630,
    intervalMinutes: 55,
    decide: fullTarget,
  },
  {
    id: 'pym_afternoon_full_1430_1555',
    name: 'PYM target, afternoon window',
    description: 'Uses previous EOD PYM target weights from 14:30 to 15:55, then exits.',
    startMinute: 870,
    endMinute: 955,
    intervalMinutes: 85,
    decide: fullTarget,
  },
  {
    id: 'pym_late_momentum_60m',
    name: 'PYM late momentum, 60m',
    description: 'At 14:30, holds target tickers with positive 60-minute momentum into 15:55.',
    startMinute: 870,
    endMinute: 955,
    intervalMinutes: 85,
    decide: (ctx) => topMomentum(ctx, { count: 5, lookbackMinutes: 60, requireVwap: true }),
  },
  {
    id: 'pym_fade_late_momentum_60m',
    name: 'Fade PYM late momentum, 60m',
    description: 'At 14:30, trades inverse counterparts with positive 60-minute momentum into 15:55.',
    startMinute: 870,
    endMinute: 955,
    intervalMinutes: 85,
    requiredTickers: inverseRequiredTickers,
    decide: (ctx) => inverseTopMomentum(ctx, { count: 5, lookbackMinutes: 60 }),
  },
  {
    id: 'pym_vwap_gate_30m',
    name: 'PYM VWAP gate, 30m',
    description: 'Same VWAP gate with lower churn.',
    startMinute: 600,
    endMinute: 955,
    intervalMinutes: 30,
    decide: (ctx) => vwapGate(ctx, { minOpenReturn: 0, safeFallback: true }),
  },
  {
    id: 'pym_top3_momentum_15m',
    name: 'PYM top-3 momentum, 15m',
    description: 'Every 15 minutes, concentrates in the three target tickers with strongest positive 15-minute momentum.',
    startMinute: 600,
    endMinute: 955,
    intervalMinutes: 15,
    decide: (ctx) => topMomentum(ctx, { count: 3, lookbackMinutes: 15, requireVwap: true }),
  },
  {
    id: 'pym_top5_momentum_30m',
    name: 'PYM top-5 momentum, 30m',
    description: 'Every 30 minutes, concentrates in five positive momentum target tickers.',
    startMinute: 600,
    endMinute: 955,
    intervalMinutes: 30,
    decide: (ctx) => topMomentum(ctx, { count: 5, lookbackMinutes: 30, requireVwap: true }),
  },
  {
    id: 'pym_pullback_recovery_15m',
    name: 'PYM pullback recovery, 15m',
    description: 'Every 15 minutes, enters target tickers that recovered near VWAP after mild intraday weakness.',
    startMinute: 600,
    endMinute: 955,
    intervalMinutes: 15,
    decide: (ctx) => pullbackRecovery(ctx),
  },
];

module.exports = {
  SAFE_TICKERS,
  INVERSE_TICKER_MAP,
  STRATEGIES,
  normalizeWeights,
  rawTargetWeights,
  inverseTargetWeights,
  returnSinceOpen,
  returnLookback,
  intradayVwap,
};
