const {
  DYNAMIC_INTRADAY_STRATEGIES,
  breakoutEntry,
  reclaimEntry,
  spyPutPressureFromOptionRow,
} = require('../src/dynamic-intraday-timing');

describe('dynamic intraday timing helpers', () => {
  it('registers base strategies and prior-day option filter variants', () => {
    const ids = DYNAMIC_INTRADAY_STRATEGIES.map((strategy) => strategy.id);
    expect(ids).toContain('dynamic_reclaim_fixed_exit');
    expect(ids).toContain('dynamic_reclaim_fixed_exit_prev_option_filter');
    expect(ids).toContain('first_confirm_after_1115_fixed_exit');
    expect(ids).toContain('first_confirm_stock_volume_after_1115_fixed_exit');
    expect(ids).toContain('first_confirm_stock_option_veto_after_1115_fixed_exit');
    expect(ids).toContain('first_reclaim_stock_option_volume_after_1115_fixed_exit');
    expect(ids).toContain('first_reclaim_stock_volume_veto_after_1115_fixed_exit');
    expect(ids).toContain('fixed_1200_confirm_fixed_exit');
    const baseCount = ids.filter((id) => !id.endsWith('_prev_option_filter')).length;
    expect(ids.filter((id) => id.endsWith('_prev_option_filter'))).toHaveLength(baseCount);
  });

  it('uses SPY put pressure as the larger bearish option-flow z-score', () => {
    const pressure = spyPutPressureFromOptionRow({
      roots: {
        SPY: {
          rolling: {
            putCallPremiumRatioZ20: 1.4,
            premiumImbalanceZ20: -2.8,
          },
        },
      },
    });
    expect(pressure).toBeCloseTo(2.8);
  });

  it('requires a 1m RSI or VWAP reclaim inside a positive 5m trend', () => {
    const feature = {
      rsi1m20: 54,
      rsi5m20: 56,
      ret5m20: 0.01,
      aboveVwap: true,
    };
    expect(reclaimEntry(feature, { rsi1m20: 48, aboveVwap: true })).toBe(true);
    expect(reclaimEntry(feature, { rsi1m20: 55, aboveVwap: true })).toBe(false);
  });

  it('requires a confirmed break above the opening range', () => {
    const dayState = {
      openingRangeHigh: new Map([['SPY', 100]]),
    };
    expect(breakoutEntry({
      ticker: 'SPY',
      close: 100.2,
      rsi5m20: 54,
      ret5m20: 0.003,
      aboveVwap: true,
    }, dayState)).toBe(true);
    expect(breakoutEntry({
      ticker: 'SPY',
      close: 100.05,
      rsi5m20: 54,
      ret5m20: 0.003,
      aboveVwap: true,
    }, dayState)).toBe(false);
  });
});
