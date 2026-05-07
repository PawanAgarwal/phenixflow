const { parseOpraTicker, daysBetween } = require('../src/opra');
const { nsToMinuteMs, getEtParts } = require('../src/time');
const { dteBucket, optionMoneynessBucket } = require('../src/dataset');

describe('OPRA and time parsing', () => {
  it('parses OPRA option roots and strikes', () => {
    expect(parseOpraTicker('O:SPY260116C00120000')).toEqual({
      ticker: 'O:SPY260116C00120000',
      root: 'SPY',
      expiration: '2026-01-16',
      right: 'CALL',
      strike: 120,
    });
    expect(parseOpraTicker('O:SPXW260102P06000000').root).toBe('SPXW');
    expect(parseOpraTicker('SPY260116C00120000')).toBeNull();
  });

  it('computes expiration distance and minute buckets', () => {
    expect(daysBetween('2026-01-02', '2026-01-16')).toBe(14);
    const minuteMs = nsToMinuteMs('1767364200000000000');
    expect(new Date(minuteMs).toISOString()).toBe('2026-01-02T14:30:00.000Z');
    expect(getEtParts(minuteMs).minuteOfDayEt).toBe(570);
    expect(nsToMinuteMs('bad-value')).toBeNull();
  });

  it('buckets option DTE and moneyness proxies', () => {
    expect(dteBucket(0)).toBe('0dte');
    expect(dteBucket(1)).toBe('1dte');
    expect(dteBucket(5)).toBe('2_7dte');
    expect(dteBucket(14)).toBe('8_30dte');
    expect(dteBucket(45)).toBeNull();

    const call = parseOpraTicker('O:SPY260116C00120000');
    const put = parseOpraTicker('O:SPY260116P00120000');
    expect(optionMoneynessBucket(call, 119.8)).toBe('atm');
    expect(optionMoneynessBucket(call, 110)).toBe('otm_call');
    expect(optionMoneynessBucket(put, 130)).toBe('otm_put');
  });
});
