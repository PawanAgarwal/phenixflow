const { __private } = require('../src/historical-flow');

describe('historical flow parser', () => {
  it('converts Theta columnar response object into row objects', () => {
    const rows = __private.parseJsonRows(JSON.stringify({
      symbol: ['AAPL', 'AAPL'],
      right: ['CALL', 'PUT'],
      price: [1.2, 1.5],
      size: [10, 20],
    }));

    expect(rows).toEqual([
      { symbol: 'AAPL', right: 'CALL', price: 1.2, size: 10 },
      { symbol: 'AAPL', right: 'PUT', price: 1.5, size: 20 },
    ]);
  });

  it('builds endpoint with expected query params', () => {
    const endpoint = __private.resolveThetaEndpoint('AAPL', '20260213', {
      THETADATA_BASE_URL: 'http://127.0.0.1:25503',
      THETADATA_HISTORICAL_OPTION_PATH: '/v3/option/history/trade_quote',
    });

    expect(endpoint).toContain('symbol=AAPL');
    expect(endpoint).toContain('date=20260213');
    expect(endpoint).toContain('format=json');
  });

  it('builds OI endpoint with default history path and contract params', () => {
    const endpoint = __private.resolveThetaOiEndpoint({
      symbol: 'AAPL',
      expiration: '2026-02-20',
      strike: 200,
      right: 'CALL',
    }, '2026-02-13', {
      THETADATA_BASE_URL: 'http://127.0.0.1:25503',
    });

    expect(endpoint).toContain('/v3/option/history/open_interest');
    expect(endpoint).toContain('symbol=AAPL');
    expect(endpoint).toContain('expiration=20260220');
    expect(endpoint).toContain('strike=200');
    expect(endpoint).toContain('right=CALL');
    expect(endpoint).toContain('date=20260213');
  });

  it('builds bulk OI endpoint for symbol/day', () => {
    const endpoint = __private.resolveThetaOiBulkEndpoint('AAPL', '2026-02-13', {
      THETADATA_BASE_URL: 'http://127.0.0.1:25503',
    });

    expect(endpoint).toContain('/v3/option/history/open_interest');
    expect(endpoint).toContain('symbol=AAPL');
    expect(endpoint).toContain('expiration=*');
    expect(endpoint).toContain('date=20260213');
  });
});
