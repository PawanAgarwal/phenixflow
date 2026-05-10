function inTradingWindow(row, settings) {
  const noEntryFirstMinutes = settings.noEntryFirstMinutes ?? 5;
  const noEntryLastMinutes = settings.noEntryLastMinutes ?? 5;
  return row.minutes_from_open >= noEntryFirstMinutes && row.minutes_to_close >= noEntryLastMinutes;
}

function signalDipReversal(row, params, settings) {
  if (!inTradingWindow(row, settings)) return false;
  return row.ret_60s_cents <= -params.dipCents
    && row.ret_3bar_cents >= params.bounceCents
    && row.close >= row.prev_low_60s + (params.offLowCents / 100)
    && row.market_ok_1m === 1
    && row.tsla_ret_1m >= params.minTslaRet1m
    && row.range_60s_cents >= params.minRangeCents;
}

function signalVwapSnapback(row, params, settings) {
  if (!inTradingWindow(row, settings)) return false;
  return row.vwap_dist_cents <= -params.vwapBelowCents
    && row.ret_3bar_cents >= params.bounceCents
    && row.tsla_ret_5m >= params.minTslaRet5m
    && row.qqq_ret_5m >= params.minQqqRet5m
    && row.range_60s_cents >= params.minRangeCents;
}

function signalMicroBreakout(row, params, settings) {
  if (!inTradingWindow(row, settings)) return false;
  return row.close >= row.prev_high_60s + (params.breakoutCents / 100)
    && row.ret_3bar_cents >= params.minPushCents
    && row.tsla_ret_1m >= params.minTslaRet1m
    && row.qqq_ret_1m >= params.minQqqRet1m
    && row.volume_30s >= params.minVolume30s;
}

function signalTslaLeadLag(row, params, settings) {
  if (!inTradingWindow(row, settings)) return false;
  return row.tsla_ret_1m >= params.minTslaRet1m
    && row.qqq_ret_1m >= params.minQqqRet1m
    && row.ret_60s_cents <= params.maxTsllLagCents
    && row.ret_3bar_cents >= params.minTurnCents
    && row.market_ok_1m === 1;
}

function signalRangeFade(row, params, settings) {
  if (!inTradingWindow(row, settings)) return false;
  return row.close <= row.prev_low_180s + (params.nearLowCents / 100)
    && row.ret_3bar_cents >= params.bounceCents
    && row.range_60s_cents >= params.minRangeCents
    && row.tsla_ret_5m >= params.minTslaRet5m
    && row.spy_ret_1m > -0.001
    && row.qqq_ret_1m > -0.001;
}

function signalOptionFlowDip(row, params, settings) {
  if (!inTradingWindow(row, settings)) return false;
  const tsllFlow = Math.max(row.opt_tsll_trade_imbalance_5m || 0, row.opt_tsll_quote_imbalance_5m || 0);
  const tslaFlow = Math.max(row.opt_tsla_trade_imbalance_5m || 0, row.opt_tsla_quote_imbalance_5m || 0);
  return row.ret_60s_cents <= -params.dipCents
    && row.ret_3bar_cents >= params.bounceCents
    && row.market_ok_1m === 1
    && row.tsla_ret_1m >= params.minTslaRet1m
    && (tsllFlow >= params.minOptionImbalance || tslaFlow >= params.minOptionImbalance);
}

const SIGNALS = {
  dip_reversal_macro: signalDipReversal,
  vwap_snapback: signalVwapSnapback,
  micro_breakout: signalMicroBreakout,
  tsla_lead_lag: signalTslaLeadLag,
  range_fade: signalRangeFade,
  option_flow_dip: signalOptionFlowDip,
};

function cartesian(items) {
  return items.reduce((acc, [key, values]) => (
    acc.flatMap((entry) => values.map((value) => ({ ...entry, [key]: value })))
  ), [{}]);
}

function buildStrategyGrid(settings = {}) {
  const executionGrid = cartesian([
    ['targetCents', settings.targetCents || [2, 3, 5]],
    ['stopCents', settings.stopCents || [2, 4, 6]],
    ['maxHoldBars', settings.maxHoldBars || [12, 24, 48]],
  ]);
  const specs = [
    {
      name: 'dip_reversal_macro',
      params: cartesian([
        ['dipCents', [3, 5, 8, 12]],
        ['bounceCents', [0.5, 1.0]],
        ['offLowCents', [0.5, 2.0]],
        ['minTslaRet1m', [-0.0004, 0, 0.0004]],
        ['minRangeCents', [4, 8]],
      ]),
    },
    {
      name: 'vwap_snapback',
      params: cartesian([
        ['vwapBelowCents', [3, 5, 8, 12]],
        ['bounceCents', [0.5, 1.0]],
        ['minTslaRet5m', [-0.001, 0]],
        ['minQqqRet5m', [-0.0008, 0]],
        ['minRangeCents', [4, 8]],
      ]),
    },
    {
      name: 'micro_breakout',
      params: cartesian([
        ['breakoutCents', [0, 0.5, 1.0]],
        ['minPushCents', [0.5, 1.5]],
        ['minTslaRet1m', [0, 0.0008]],
        ['minQqqRet1m', [-0.0002, 0.0002]],
        ['minVolume30s', [0, 1500]],
      ]),
    },
    {
      name: 'tsla_lead_lag',
      params: cartesian([
        ['minTslaRet1m', [0.0005, 0.001, 0.0015]],
        ['minQqqRet1m', [-0.0002, 0.0002]],
        ['maxTsllLagCents', [-4, 0, 2]],
        ['minTurnCents', [0, 1.0]],
      ]),
    },
    {
      name: 'range_fade',
      params: cartesian([
        ['nearLowCents', [1, 3]],
        ['bounceCents', [0.5, 1.0]],
        ['minRangeCents', [4, 8, 12]],
        ['minTslaRet5m', [-0.0015, 0]],
      ]),
    },
    {
      name: 'option_flow_dip',
      params: cartesian([
        ['dipCents', [3, 6, 10]],
        ['bounceCents', [0.5, 1.0]],
        ['minTslaRet1m', [-0.0004, 0, 0.0004]],
        ['minOptionImbalance', [0.1, 0.25, 0.4]],
      ]),
    },
  ];

  return specs.flatMap((spec) => spec.params.flatMap((params) => (
    executionGrid.map((execution) => ({
      name: spec.name,
      params,
      execution,
    }))
  )));
}

function hasSignal(row, strategy, settings) {
  return Boolean(SIGNALS[strategy.name]?.(row, strategy.params, settings));
}

module.exports = {
  SIGNALS,
  buildStrategyGrid,
  hasSignal,
};
