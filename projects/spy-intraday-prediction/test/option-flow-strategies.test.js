const s = require('../src/option-flow-strategies');

// Helper to fabricate a minimal day of feature rows.
function makeRows({ minutes = 390, mutate } = {}) {
  const rows = [];
  const start = Date.UTC(2026, 0, 5, 14, 30); // Mon Jan 5 2026 14:30 UTC = 9:30 ET
  for (let i = 0; i < minutes; i += 1) {
    const minute_ms = start + i * 60_000;
    rows.push({
      minute_ms,
      date_et: '2026-01-05',
      minute_of_day_et: 570 + i,
      spy_open: 500 + i * 0.001,
      spy_close: 500 + i * 0.001,
      spy_high: 500.1 + i * 0.001,
      spy_low: 499.9 + i * 0.001,
      spy_volume: 1000,
      vix_close: 15,
      ret_5m: 0,
      ret_60m: 0,
      intraday_return: i * 0.0001,
      overnight_return: 0.002,
      flow_sweep_call_buy_premium: 0,
      flow_sweep_call_sell_premium: 0,
      flow_sweep_put_buy_premium: 0,
      flow_sweep_put_sell_premium: 0,
      flow_block_call_buy_premium: 0,
      flow_block_put_buy_premium: 0,
      flow_premium_0dte_call_buy: 0,
      flow_premium_0dte_put_buy: 0,
      cum_dealer_gamma: 0,
      cum_dealer_vanna: 0,
      cum_dealer_charm: 0,
      cum_call_buy_premium: 0,
      cum_call_sell_premium: 0,
      cum_put_buy_premium: 0,
      cum_put_sell_premium: 0,
    });
  }
  if (mutate) mutate(rows);
  return rows;
}

describe('S1 sweep momentum', () => {
  it('fires LONG when call_buy sweep premium spikes', () => {
    const rows = makeRows({ mutate: (rs) => {
      // build a steady-state baseline then a spike at minute 100
      rs[100].flow_sweep_call_buy_premium = 5_000_000;
    }});
    const intents = s.strategyS1(rows, {});
    expect(intents.length).toBeGreaterThan(0);
    expect(intents[0].side).toBe('LONG');
  });
  it('fires SHORT when put_buy sweep premium spikes', () => {
    const rows = makeRows({ mutate: (rs) => { rs[120].flow_sweep_put_buy_premium = 5_000_000; }});
    const intents = s.strategyS1(rows, {});
    expect(intents.length).toBeGreaterThan(0);
    expect(intents[0].side).toBe('SHORT');
  });
});

describe('S2 block follow', () => {
  it('fires LONG on dominant call-block buying', () => {
    const rows = makeRows({ mutate: (rs) => {
      rs[50].flow_block_call_buy_premium = 2_000_000;
      rs[50].flow_block_put_buy_premium = 100_000;
    }});
    const intents = s.strategyS2(rows, {});
    expect(intents.length).toBe(1);
    expect(intents[0].side).toBe('LONG');
  });
  it('fires SHORT on dominant put-block buying', () => {
    const rows = makeRows({ mutate: (rs) => {
      rs[50].flow_block_put_buy_premium = 2_000_000;
      rs[50].flow_block_call_buy_premium = 100_000;
    }});
    const intents = s.strategyS2(rows, {});
    expect(intents.length).toBe(1);
    expect(intents[0].side).toBe('SHORT');
  });
});

describe('S3 vGEX regime', () => {
  it('rides gap when dealer gamma is negative (trend regime)', () => {
    const rows = makeRows({ mutate: (rs) => {
      rs.forEach((r) => { r.overnight_return = 0.005; r.cum_dealer_gamma = -2_000_000; });
    }});
    const intents = s.strategyS3(rows, { dayIso: '2026-01-05' });
    expect(intents.length).toBe(1);
    expect(intents[0].side).toBe('LONG');
  });
  it('fades gap when dealer gamma is positive (mean-revert regime)', () => {
    const rows = makeRows({ mutate: (rs) => {
      rs.forEach((r) => { r.overnight_return = 0.005; r.cum_dealer_gamma = 2_000_000; });
    }});
    const intents = s.strategyS3(rows, { dayIso: '2026-01-05' });
    expect(intents.length).toBe(1);
    expect(intents[0].side).toBe('SHORT');
  });
});

describe('S5 charm pin (Friday-only)', () => {
  it('skips non-Friday', () => {
    const rows = makeRows({ mutate: (rs) => {
      rs.forEach((r) => { r.intraday_return = 0.01; r.cum_dealer_charm = 1; });
    }});
    const intents = s.strategyS5(rows, { dayIso: '2026-01-05' }); // Monday
    expect(intents).toEqual([]);
  });
  it('fades large up move on Friday with positive charm', () => {
    const rows = makeRows({ mutate: (rs) => {
      rs.forEach((r) => { r.intraday_return = 0.01; r.cum_dealer_charm = 1; });
    }});
    const intents = s.strategyS5(rows, { dayIso: '2026-01-09' }); // Friday
    expect(intents.length).toBe(1);
    expect(intents[0].side).toBe('SHORT');
  });
});

describe('S6 vanna trend (swing)', () => {
  it('goes LONG when dealer vanna is positive and VIX dropped', () => {
    const rows = makeRows({ mutate: (rs) => {
      rs.forEach((r) => { r.cum_dealer_vanna = 5_000_000; });
      rs[0].vix_close = 18;
      rs[rs.length - 1].vix_close = 16; // ~-11% change
    }});
    const sig = s.strategyS6(rows, { dayIso: '2026-01-05' });
    expect(sig).toBeTruthy();
    expect(sig.side).toBe('LONG');
  });
});

describe('S7 premium flow composite (swing)', () => {
  it('returns null without sufficient history', () => {
    const rows = makeRows();
    const sig = s.strategyS7(rows, { dayIso: '2026-01-05', prior: [] });
    expect(sig).toBeNull();
  });
});
