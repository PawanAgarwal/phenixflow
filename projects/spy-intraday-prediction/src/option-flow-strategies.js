// Seven BullFlow/CheddarFlow-style strategies operating on the per-minute feature dataset.
//
// Each *intraday* strategy is `(rowsForDay, ctx) -> intents[]`, where intents have:
//   { minute_ms, side: 'LONG'|'SHORT', exit_minute_ms? OR hold_minutes? }
// Each *swing* strategy is `(rowsForDay, ctx) -> { side: 'LONG'|'SHORT'|'FLAT', notes? } | null`.
//
// All strategies are deliberately rule-based and tunable via the `params` ctx argument so we can
// later sweep parameters in walk-forward fashion. Defaults match the original plan in chat.

const REGULAR_CLOSE_ET = 960;

function safeStd(values) {
  if (values.length < 2) return 0;
  const m = values.reduce((a, b) => a + b, 0) / values.length;
  const v = values.reduce((a, b) => a + ((b - m) ** 2), 0) / values.length;
  return Math.sqrt(v);
}
function safeMean(values) {
  if (values.length === 0) return 0;
  return values.reduce((a, b) => a + b, 0) / values.length;
}

// Trailing-window z-score helper used by several strategies.
function zScoreSeries(values, idx, lookback) {
  const start = Math.max(0, idx - lookback + 1);
  const window = values.slice(start, idx + 1);
  if (window.length < Math.min(5, lookback)) return 0;
  const m = safeMean(window);
  const s = safeStd(window);
  if (s === 0) return 0;
  return (values[idx] - m) / s;
}

// ============================================================================
// S1 — Aggressive Call/Put Sweep Momentum
// Net aggressive sweep premium = (call_sweep_buy + put_sweep_sell) - (call_sweep_sell + put_sweep_buy)
// Z-scored over trailing N-minute window. Cross threshold → LONG/SHORT for hold_minutes.
// ============================================================================
function strategyS1(rows, ctx = {}) {
  const params = {
    lookback: 30,
    enterZ: 1.5,
    exitZ: 0.5,
    holdMinutes: 30,
    cooldownMinutes: 15,
    minSignalPremium: 50_000, // require at least $50K aggressive sweep premium in the minute
    ...(ctx.params || {}),
  };
  // Net aggressive sweep premium per minute = (call_sweep_buy + put_sweep_sell) − (call_sweep_sell + put_sweep_buy)
  // Positive = aggressive bullish; negative = aggressive bearish.
  const aggregate = rows.map((r) => {
    const cb = r.flow_sweep_call_buy_premium || 0;
    const cs = r.flow_sweep_call_sell_premium || 0;
    const pb = r.flow_sweep_put_buy_premium || 0;
    const ps = r.flow_sweep_put_sell_premium || 0;
    return (cb + ps) - (cs + pb);
  });
  const intents = [];
  let cooldownUntilIdx = -1;
  for (let i = 0; i < rows.length; i += 1) {
    if (i < cooldownUntilIdx) continue;
    if (Math.abs(aggregate[i]) < params.minSignalPremium) continue;
    const z = zScoreSeries(aggregate, i, params.lookback);
    if (z > params.enterZ) {
      intents.push({
        minute_ms: rows[i].minute_ms,
        side: 'LONG',
        hold_minutes: params.holdMinutes,
        notes: { z, aggregate: aggregate[i] },
      });
      cooldownUntilIdx = i + params.holdMinutes + params.cooldownMinutes;
    } else if (z < -params.enterZ) {
      intents.push({
        minute_ms: rows[i].minute_ms,
        side: 'SHORT',
        hold_minutes: params.holdMinutes,
        notes: { z, aggregate: aggregate[i] },
      });
      cooldownUntilIdx = i + params.holdMinutes + params.cooldownMinutes;
    }
  }
  return intents;
}

// ============================================================================
// S2 — Block-Print Follow
// Look for minutes with net block-buy premium > threshold, dominated by calls or puts.
// Call-dominant net block-buy → LONG. Put-dominant net block-buy → SHORT (puts as hedges/bears).
// ============================================================================
function strategyS2(rows, ctx = {}) {
  const params = {
    minNetBlockPremium: 1_000_000, // $1M
    minDominance: 0.7, // fraction of net block flow that must be one side (call vs put)
    holdMinutes: 60,
    cooldownMinutes: 30,
    ...(ctx.params || {}),
  };
  const intents = [];
  let cooldownUntilIdx = -1;
  for (let i = 0; i < rows.length; i += 1) {
    if (i < cooldownUntilIdx) continue;
    const r = rows[i];
    // Net block premium: blocks are mostly "absolute size" so we use call_block_buy and put_block_buy alone (these are buys initiated by customers)
    const callBlockBuy = r.flow_block_call_buy_premium ?? 0;
    const putBlockBuy = r.flow_block_put_buy_premium ?? 0;
    const totalBuy = callBlockBuy + putBlockBuy;
    if (totalBuy < params.minNetBlockPremium) continue;
    const callShare = totalBuy === 0 ? 0 : callBlockBuy / totalBuy;
    if (callShare >= params.minDominance) {
      intents.push({
        minute_ms: r.minute_ms,
        side: 'LONG',
        hold_minutes: params.holdMinutes,
        notes: { callBlockBuy, putBlockBuy, callShare },
      });
      cooldownUntilIdx = i + params.holdMinutes + params.cooldownMinutes;
    } else if (1 - callShare >= params.minDominance) {
      intents.push({
        minute_ms: r.minute_ms,
        side: 'SHORT',
        hold_minutes: params.holdMinutes,
        notes: { callBlockBuy, putBlockBuy, putShare: 1 - callShare },
      });
      cooldownUntilIdx = i + params.holdMinutes + params.cooldownMinutes;
    }
  }
  return intents;
}

// ============================================================================
// S3 — vGEX-Regime Trend/MR
// At decisionMinute ET (default 10:30 = 630), measure cum_dealer_gamma since open.
// If dealer_gamma >= 0 (mean-revert regime): fade overnight gap.
// If dealer_gamma < 0 (trend regime): ride overnight gap.
// One trade per day, held to 15:30 ET.
// ============================================================================
function strategyS3(rows, ctx = {}) {
  const params = {
    decisionMinuteEt: 630, // 10:30
    exitMinuteEt: 930, // 15:30
    minOvernightGapBps: 10, // require at least 0.10% overnight move
    ...(ctx.params || {}),
  };
  const decisionRow = rows.find((r) => r.minute_of_day_et === params.decisionMinuteEt);
  if (!decisionRow) return [];
  const overnight = decisionRow.overnight_return;
  if (!Number.isFinite(overnight)) return [];
  if (Math.abs(overnight) * 10_000 < params.minOvernightGapBps) return [];
  const dealerGamma = decisionRow.cum_dealer_gamma;
  if (!Number.isFinite(dealerGamma)) return [];
  const exitRow = rows.find((r) => r.minute_of_day_et === params.exitMinuteEt) || rows[rows.length - 1];
  const isMeanRevert = dealerGamma >= 0;
  let side;
  if (isMeanRevert) {
    side = overnight > 0 ? 'SHORT' : 'LONG';
  } else {
    side = overnight > 0 ? 'LONG' : 'SHORT';
  }
  return [{
    minute_ms: decisionRow.minute_ms,
    side,
    exit_minute_ms: exitRow.minute_ms,
    notes: { regime: isMeanRevert ? 'MR' : 'TREND', dealer_gamma: dealerGamma, overnight },
  }];
}

// ============================================================================
// S4 — 0DTE Gamma Squeeze
// Detect spikes in 0DTE call-buy premium z-scored over trailing N minutes,
// confirmed by SPY moving in the call-bought direction in the same minute.
// LONG signal on call-buy spike with positive ret_5m; SHORT on put-buy spike with negative ret_5m.
// Hold short window (15m) because 0DTE moves are fast.
// ============================================================================
function strategyS4(rows, ctx = {}) {
  const params = {
    lookback: 30,
    enterZ: 2.0,
    holdMinutes: 15,
    cooldownMinutes: 10,
    minPremium: 200_000,
    minRet5m: 0.0005, // 5bps
    ...(ctx.params || {}),
  };
  const callBuy = rows.map((r) => r.flow_premium_0dte_call_buy || 0);
  const putBuy = rows.map((r) => r.flow_premium_0dte_put_buy || 0);
  const intents = [];
  let cooldownUntilIdx = -1;
  for (let i = 0; i < rows.length; i += 1) {
    if (i < cooldownUntilIdx) continue;
    const r = rows[i];
    if ((callBuy[i] < params.minPremium) && (putBuy[i] < params.minPremium)) continue;
    const zCall = zScoreSeries(callBuy, i, params.lookback);
    const zPut = zScoreSeries(putBuy, i, params.lookback);
    const ret5m = r.ret_5m || 0;
    if (zCall > params.enterZ && ret5m > params.minRet5m && callBuy[i] >= params.minPremium) {
      intents.push({
        minute_ms: r.minute_ms,
        side: 'LONG',
        hold_minutes: params.holdMinutes,
        notes: { zCall, ret5m, callBuy: callBuy[i] },
      });
      cooldownUntilIdx = i + params.holdMinutes + params.cooldownMinutes;
    } else if (zPut > params.enterZ && ret5m < -params.minRet5m && putBuy[i] >= params.minPremium) {
      intents.push({
        minute_ms: r.minute_ms,
        side: 'SHORT',
        hold_minutes: params.holdMinutes,
        notes: { zPut, ret5m, putBuy: putBuy[i] },
      });
      cooldownUntilIdx = i + params.holdMinutes + params.cooldownMinutes;
    }
  }
  return intents;
}

// ============================================================================
// S5 — Charm Pin (Friday PM)
// On Fridays after 13:00 ET, if dealer charm flow is positive (dealers earn time decay),
// fade the intraday move from session_open. Larger intraday move → larger fade.
// ============================================================================
function strategyS5(rows, ctx = {}) {
  const params = {
    decisionMinuteEt: 780, // 13:00
    exitMinuteEt: 930,
    minIntradayPctMove: 0.003, // 30bps
    minPositiveCharm: 0,
    ...(ctx.params || {}),
  };
  const dayIso = ctx.dayIso || rows[0]?.date_et;
  const date = new Date(`${dayIso}T00:00:00.000Z`);
  if (date.getUTCDay() !== 5) return []; // Friday only
  const decisionRow = rows.find((r) => r.minute_of_day_et === params.decisionMinuteEt);
  if (!decisionRow) return [];
  const intraday = decisionRow.intraday_return;
  if (!Number.isFinite(intraday)) return [];
  if (Math.abs(intraday) < params.minIntradayPctMove) return [];
  const cumCharm = decisionRow.cum_dealer_charm || 0;
  if (cumCharm <= params.minPositiveCharm) return [];
  const exitRow = rows.find((r) => r.minute_of_day_et === params.exitMinuteEt) || rows[rows.length - 1];
  const side = intraday > 0 ? 'SHORT' : 'LONG';
  return [{
    minute_ms: decisionRow.minute_ms,
    side,
    exit_minute_ms: exitRow.minute_ms,
    notes: { intraday, cum_dealer_charm: cumCharm },
  }];
}

// ============================================================================
// S6 — Vanna-Led Trend (Swing, next-day open-to-close)
// End-of-day cum_dealer_vanna + VIX 2-day change. Strong positive dealer vanna + falling VIX
// → bullish carry (dealers buy as VIX drops). Negative + rising VIX → bearish.
// ============================================================================
function strategyS6(rows, ctx = {}) {
  const params = {
    minDealerVannaAbs: 100_000,
    vixLookbackBars: 60, // ~60 trailing minutes (~1h)
    minVixChangePct: 0.01,
    ...(ctx.params || {}),
  };
  const lastRow = rows[rows.length - 1];
  if (!lastRow) return null;
  const cumVanna = lastRow.cum_dealer_vanna;
  if (!Number.isFinite(cumVanna)) return null;
  if (Math.abs(cumVanna) < params.minDealerVannaAbs) return null;
  // VIX change today: first VIX vs last VIX
  const firstVix = rows.find((r) => Number.isFinite(r.vix_close))?.vix_close;
  const lastVix = lastRow.vix_close;
  if (!Number.isFinite(firstVix) || !Number.isFinite(lastVix) || firstVix === 0) return null;
  const vixChange = lastVix / firstVix - 1;
  if (Math.abs(vixChange) < params.minVixChangePct) return null;
  let side = 'FLAT';
  if (cumVanna > 0 && vixChange < 0) side = 'LONG'; // dealers long-vanna unwind as vol drops
  else if (cumVanna < 0 && vixChange > 0) side = 'SHORT';
  if (side === 'FLAT') return null;
  return { side, notes: { cum_dealer_vanna: cumVanna, vixChange } };
}

// ============================================================================
// S7 — Smart-Money Premium Flow Composite (Swing, next-day open-to-close)
// EOD net aggressive premium (call_buy + put_sell − call_sell − put_buy), z-scored
// over trailing 20 trading days. Trade in direction of the signal.
// ============================================================================
function strategyS7(rows, ctx = {}) {
  const params = {
    enterZ: 1.0,
    historyDays: 20,
    ...(ctx.params || {}),
  };
  const lastRow = rows[rows.length - 1];
  if (!lastRow) return null;
  const netToday = (lastRow.cum_call_buy_premium || 0)
    + (lastRow.cum_put_sell_premium || 0)
    - (lastRow.cum_call_sell_premium || 0)
    - (lastRow.cum_put_buy_premium || 0);
  // History accumulated via ctx.prior
  const history = (ctx.prior || []).slice(-params.historyDays).map((d) => {
    const r = d.rows[d.rows.length - 1];
    if (!r) return 0;
    return (r.cum_call_buy_premium || 0)
      + (r.cum_put_sell_premium || 0)
      - (r.cum_call_sell_premium || 0)
      - (r.cum_put_buy_premium || 0);
  });
  if (history.length < Math.min(5, params.historyDays)) return null;
  const m = safeMean(history);
  const s = safeStd(history);
  if (s === 0) return null;
  const z = (netToday - m) / s;
  let side = 'FLAT';
  if (z > params.enterZ) side = 'LONG';
  else if (z < -params.enterZ) side = 'SHORT';
  if (side === 'FLAT') return null;
  return { side, notes: { netToday, z } };
}

// ============================================================================
// Contrarian variants — reverse the direction of S1, S2, S4 because the
// initial backtest showed flow-following loses money consistently.
// ============================================================================
function reverseSide(intent) {
  if (!intent) return intent;
  return { ...intent, side: intent.side === 'LONG' ? 'SHORT' : 'LONG' };
}
function strategyS1Contrarian(rows, ctx) { return strategyS1(rows, ctx).map(reverseSide); }
function strategyS2Contrarian(rows, ctx) { return strategyS2(rows, ctx).map(reverseSide); }
function strategyS4Contrarian(rows, ctx) { return strategyS4(rows, ctx).map(reverseSide); }

// ============================================================================
// Benchmark: buy-and-hold SPY each day from open to close (no overnight risk)
// ============================================================================
function strategyBuyHoldDay(rows) {
  if (rows.length < 2) return [];
  return [{
    minute_ms: rows[0].minute_ms,
    side: 'LONG',
    exit_minute_ms: rows[rows.length - 1].minute_ms,
    hold_minutes: rows.length,
    notes: { kind: 'buy_hold_intraday' },
  }];
}

module.exports = {
  strategyS1,
  strategyS2,
  strategyS3,
  strategyS4,
  strategyS5,
  strategyS6,
  strategyS7,
  strategyS1Contrarian,
  strategyS2Contrarian,
  strategyS4Contrarian,
  strategyBuyHoldDay,
};
