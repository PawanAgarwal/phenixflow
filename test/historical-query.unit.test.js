const {
  parseHistoricalFilters,
  getRequiredMetricsForQuery,
  applyHistoricalFilters,
} = require('../src/historical-query');

describe('historical query parsing and filtering', () => {
  it('parses query params into normalized historical filters', () => {
    const filters = parseHistoricalFilters({
      chips: 'Calls,100k+,Large Size',
      type: 'c',
      expiration: '2026-02-20T14:30:00.000Z',
      side: 'ask',
      sentiment: 'Bullish',
      minValue: '100000',
      maxVolOi: '3.5',
      maxSigScore: '0.95',
    });

    expect(filters).toMatchObject({
      chips: ['calls', '100k+', 'large-size'],
      right: 'CALL',
      expiration: '2026-02-20',
      side: 'ASK',
      sentiment: 'bullish',
      minValue: 100000,
      maxVolOi: 3.5,
      maxSigScore: 0.95,
    });
  });

  it('derives required metrics for chips + scalar filters', () => {
    const metrics = getRequiredMetricsForQuery(parseHistoricalFilters({
      chips: 'OTM,Vol>OI',
      side: 'AA',
      minRepeat3m: '20',
      minSigScore: '0.9',
    }));

    expect(metrics.sort()).toEqual(['execution', 'otmPct', 'repeat3m', 'sigScore', 'volOiRatio']);
  });

  it('applies chip and scalar filters with AND semantics', () => {
    const rows = [
      {
        id: 'a',
        right: 'CALL',
        expiration: '2026-02-20',
        executionSide: 'ASK',
        sentiment: 'bullish',
        value: 200000,
        size: 1200,
        dte: 30,
        otmPct: 8,
        volOiRatio: 2.2,
        repeat3m: 22,
        sigScore: 0.91,
        chips: ['calls', '100k+', 'large-size', 'repeat-flow', 'unusual'],
      },
      {
        id: 'b',
        right: 'CALL',
        expiration: '2026-02-27',
        executionSide: 'BID',
        sentiment: 'bullish',
        value: 120000,
        size: 500,
        dte: 40,
        otmPct: 3,
        volOiRatio: 1.2,
        repeat3m: 10,
        sigScore: 0.75,
        chips: ['calls', '100k+'],
      },
    ];

    const filters = parseHistoricalFilters({
      chips: 'calls,100k+,repeat flow',
      expiration: '2026-02-20',
      side: 'ASK',
      minVolOi: '2.0',
      minSigScore: '0.9',
      minRepeat3m: '20',
    });

    const result = applyHistoricalFilters(rows, filters);
    expect(result.map((row) => row.id)).toEqual(['a']);
  });
});
