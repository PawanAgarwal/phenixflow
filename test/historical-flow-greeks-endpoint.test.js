const { __private } = require('../src/historical-flow');

describe('historical flow greeks endpoint resolution', () => {
  it('applies optional rate/dividend/version query params from env', () => {
    const endpoint = __private.resolveThetaGreeksEndpoint(
      'AAPL',
      '2025-12-19',
      '2025-11-03',
      {
        THETADATA_BASE_URL: 'http://127.0.0.1:25503',
        THETADATA_GREEKS_RATE_TYPE: 'bond',
        THETADATA_GREEKS_RATE_VALUE: '0.043',
        THETADATA_GREEKS_ANNUAL_DIVIDEND: '1.04',
        THETADATA_GREEKS_VERSION: '2',
      },
      { format: 'ndjson' },
    );

    const parsed = new URL(endpoint);
    expect(parsed.searchParams.get('rate_type')).toBe('bond');
    expect(parsed.searchParams.get('rate_value')).toBe('0.043');
    expect(parsed.searchParams.get('annual_dividend')).toBe('1.04');
    expect(parsed.searchParams.get('version')).toBe('2');
    expect(parsed.searchParams.get('format')).toBe('ndjson');
  });

  it('uses symbol-specific dividend override before global dividend', () => {
    const endpoint = __private.resolveThetaGreeksEndpoint(
      'AAPL',
      '2025-12-19',
      '2025-11-03',
      {
        THETADATA_BASE_URL: 'http://127.0.0.1:25503',
        THETADATA_GREEKS_ANNUAL_DIVIDEND: '0.5',
        THETADATA_GREEKS_DIVIDEND_OVERRIDES: 'AAPL=1.11,MSFT=0.8',
      },
      { format: 'json' },
    );

    const parsed = new URL(endpoint);
    expect(parsed.searchParams.get('annual_dividend')).toBe('1.11');
  });

  it('ignores invalid optional greek model params', () => {
    const endpoint = __private.resolveThetaGreeksEndpoint(
      'AAPL',
      '2025-12-19',
      '2025-11-03',
      {
        THETADATA_BASE_URL: 'http://127.0.0.1:25503',
        THETADATA_GREEKS_RATE_TYPE: 'bad_type',
        THETADATA_GREEKS_RATE_VALUE: 'not_a_number',
        THETADATA_GREEKS_ANNUAL_DIVIDEND: '-1',
        THETADATA_GREEKS_VERSION: '0',
      },
      { format: 'json' },
    );

    const parsed = new URL(endpoint);
    expect(parsed.searchParams.get('rate_type')).toBeNull();
    expect(parsed.searchParams.get('rate_value')).toBeNull();
    expect(parsed.searchParams.get('annual_dividend')).toBeNull();
    expect(parsed.searchParams.get('version')).toBeNull();
  });
});
