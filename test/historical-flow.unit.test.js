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
    expect(endpoint).toContain('format=ndjson');
  });

  it('includes start_time and end_time when provided for trade history endpoint', () => {
    const endpoint = __private.resolveThetaEndpoint(
      'SPY',
      '20260213',
      {
        THETADATA_BASE_URL: 'http://127.0.0.1:25503',
      },
      {
        startTime: '13:00:00',
        endTime: '13:59:59',
      },
    );

    expect(endpoint).toContain('start_time=13%3A00%3A00');
    expect(endpoint).toContain('end_time=13%3A59%3A59');
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

  it('builds 1m option quote endpoint for symbol/day', () => {
    const endpoint = __private.resolveThetaOptionQuoteEndpoint('AAPL', '2026-02-13', {
      THETADATA_BASE_URL: 'http://127.0.0.1:25503',
    });

    expect(endpoint).toContain('/v3/option/history/quote');
    expect(endpoint).toContain('symbol=AAPL');
    expect(endpoint).toContain('date=20260213');
    expect(endpoint).toContain('interval=1m');
  });

  it('includes start_time and end_time when provided for quote endpoint', () => {
    const endpoint = __private.resolveThetaOptionQuoteEndpoint(
      'SPY',
      '2026-02-13',
      {
        THETADATA_BASE_URL: 'http://127.0.0.1:25503',
      },
      {
        startTime: '13:00:00',
        endTime: '13:59:59',
      },
    );

    expect(endpoint).toContain('start_time=13%3A00%3A00');
    expect(endpoint).toContain('end_time=13%3A59%3A59');
  });

  it('splits configured large symbols into hourly windows', () => {
    const windows = __private.resolveThetaTimeWindowsForSymbol('SPY', {
      startTime: '13:15:00',
      env: {
        THETADATA_LARGE_SYMBOLS: 'SPY,QQQ',
        THETADATA_LARGE_SYMBOL_WINDOW_MINUTES: '60',
      },
    });

    expect(windows[0]).toEqual({ startTime: '13:15:00', endTime: '14:14:59' });
    expect(windows[1]).toEqual({ startTime: '14:15:00', endTime: '15:14:59' });
    expect(windows[windows.length - 1]).toEqual({ startTime: '23:15:00', endTime: '23:59:59' });
  });

  it('keeps non-large symbols as a single request window', () => {
    const windows = __private.resolveThetaTimeWindowsForSymbol('AAPL', {
      startTime: '13:15:00',
      env: {
        THETADATA_LARGE_SYMBOLS: 'SPY,QQQ',
        THETADATA_LARGE_SYMBOL_WINDOW_MINUTES: '60',
      },
    });

    expect(windows).toEqual([{ startTime: '13:15:00', endTime: null }]);
  });

  it('bounds large-symbol windows to market session when provided', () => {
    const windows = __private.resolveThetaTimeWindowsForSymbol('SPY', {
      startTime: null,
      sessionStartTime: '09:30:00',
      sessionEndTime: '16:15:00',
      env: {
        THETADATA_LARGE_SYMBOLS: 'SPY,QQQ',
        THETADATA_LARGE_SYMBOL_WINDOW_MINUTES: '60',
      },
    });

    expect(windows[0]).toEqual({ startTime: '09:30:00', endTime: '10:29:59' });
    expect(windows[windows.length - 1]).toEqual({ startTime: '15:30:00', endTime: '16:15:00' });
  });

  it('bounds non-large symbol single window to market session when provided', () => {
    const windows = __private.resolveThetaTimeWindowsForSymbol('AAPL', {
      startTime: null,
      sessionStartTime: '09:30:00',
      sessionEndTime: '16:15:00',
      env: {
        THETADATA_LARGE_SYMBOLS: 'SPY,QQQ',
        THETADATA_LARGE_SYMBOL_WINDOW_MINUTES: '60',
      },
    });

    expect(windows).toEqual([{ startTime: '09:30:00', endTime: '16:15:00' }]);
  });
});
