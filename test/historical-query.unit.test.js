const {
  parseHistoricalFilters,
  getRequiredMetricsForQuery,
  applyHistoricalFilters,
} = require('../src/historical-query');

describe('historical query parsing and filtering', () => {
  it('parses query params into normalized historical filters', () => {
    const filters = parseHistoricalFilters({
      chips: 'Calls,100k+,Large Size',
      right: 'c',
      sentiment: 'Bullish',
      minValue: '100000',
      maxSigScore: '0.95',
    });

    expect(filters).toMatchObject({
      chips: ['calls', '100k+', 'large-size'],
      right: 'CALL',
      sentiment: 'bullish',
      minValue: 100000,
      maxSigScore: 0.95,
    });
  });

  it('derives required metrics for chips + scalar filters', () => {
    const metrics = getRequiredMetricsForQuery(parseHistoricalFilters({
      chips: 'OTM,Vol>OI',
      minRepeat3m: '20',
      minSigScore: '0.9',
    }));

    expect(metrics.sort()).toEqual(['otmPct', 'repeat3m', 'sigScore', 'volOiRatio']);
  });

  it('applies chip and scalar filters with AND semantics', () => {
    const rows = [
      {
        id: 'a',
        right: 'CALL',
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
      minVolOi: '2.0',
      minSigScore: '0.9',
      minRepeat3m: '20',
    });

    const result = applyHistoricalFilters(rows, filters);
    expect(result.map((row) => row.id)).toEqual(['a']);
  });
});
