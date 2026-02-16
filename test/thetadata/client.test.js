const { ThetaDataClient, parseThetaRows } = require('../../src/thetadata/client');

describe('thetadata client parser', () => {
  it('parses object rows and watermark', () => {
    const parsed = parseThetaRows(JSON.stringify({
      rows: [{ symbol: 'AAPL', price: 1.2 }],
      watermark: 'w2',
    }));

    expect(parsed).toEqual({
      rows: [{ symbol: 'AAPL', price: 1.2 }],
      watermark: 'w2',
    });
  });

  it('parses columnar responses into row objects', () => {
    const parsed = parseThetaRows(JSON.stringify({
      symbol: ['AAPL', 'AAPL'],
      right: ['CALL', 'PUT'],
      price: [1.2, 1.3],
      size: [10, 12],
    }));

    expect(parsed.rows).toEqual([
      { symbol: 'AAPL', right: 'CALL', price: 1.2, size: 10 },
      { symbol: 'AAPL', right: 'PUT', price: 1.3, size: 12 },
    ]);
  });
});

describe('ThetaDataClient', () => {
  it('adds watermark and symbol params to ingest URL', () => {
    const client = new ThetaDataClient({
      baseUrl: 'http://127.0.0.1:25503',
      ingestPath: '/v3/option/stream/trade_quote',
      fetchImpl: async () => ({
        ok: true,
        status: 200,
        text: async () => JSON.stringify({ rows: [] }),
      }),
    });

    const endpoint = client.buildIngestUrl({ symbol: 'AAPL', watermark: 'w10', limit: 50 });
    expect(endpoint).toContain('symbol=AAPL');
    expect(endpoint).toContain('watermark=w10');
    expect(endpoint).toContain('limit=50');
    expect(endpoint).toContain('format=json');
  });
});
