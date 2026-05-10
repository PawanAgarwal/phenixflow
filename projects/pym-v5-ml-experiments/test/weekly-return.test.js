const {
  buildWeeklyAnchors,
  buildWeeklySamples,
  nextCalendarFriday,
  sampleTargetReturn,
} = require('../src/weekly-return');

function syntheticMarket() {
  const dates = [
    '2024-01-01', '2024-01-02', '2024-01-03', '2024-01-04', '2024-01-05',
    '2024-01-08', '2024-01-09', '2024-01-10', '2024-01-11', '2024-01-12',
    '2024-01-16', '2024-01-17', '2024-01-18', '2024-01-19',
  ];
  const rows = [];
  dates.forEach((date, index) => {
    const spy = 100 + index;
    const bil = 100 + index * 0.01;
    rows.push({ date, ticker: 'SPY', open: spy - 0.2, high: spy + 0.5, low: spy - 0.5, close: spy, volume: 1000 });
    rows.push({ date, ticker: 'BIL', open: bil, high: bil, low: bil, close: bil, volume: 1000 });
    rows.push({ date, ticker: 'QQQ', open: spy + 10, high: spy + 11, low: spy + 9, close: spy + 10, volume: 1000 });
  });
  const tickers = ['BIL', 'QQQ', 'SPY'];
  const byDate = new Map(dates.map((date) => [date, new Map()]));
  rows.forEach((row) => byDate.get(row.date).set(row.ticker, row));
  const closes = new Map(tickers.map((ticker) => [
    ticker,
    dates.map((date) => byDate.get(date).get(ticker).close),
  ]));
  return { rows, dates, tickers, byDate, closes };
}

function longSyntheticMarket() {
  const dates = [];
  const rows = [];
  const start = new Date('2023-01-02T00:00:00Z');
  for (let day = 0; day < 430; day += 1) {
    const date = new Date(start);
    date.setUTCDate(date.getUTCDate() + day);
    const dow = date.getUTCDay();
    if (dow === 0 || dow === 6) continue;
    const label = date.toISOString().slice(0, 10);
    dates.push(label);
  }
  dates.forEach((date, index) => {
    const trend = 100 + index * 0.1;
    const spy = trend + Math.sin(index / 7);
    const qqq = 120 + index * 0.13;
    const bil = 100 + index * 0.01;
    [['SPY', spy], ['QQQ', qqq], ['BIL', bil], ['IWM', spy * 0.8], ['TLT', 90 - index * 0.01], ['VIXY', 20 + Math.sin(index / 3)]].forEach(([ticker, close]) => {
      rows.push({ date, ticker, open: close * 0.999, high: close * 1.002, low: close * 0.998, close, volume: 1000 + index });
    });
  });
  const tickers = [...new Set(rows.map((row) => row.ticker))].sort();
  const byDate = new Map(dates.map((date) => [date, new Map()]));
  rows.forEach((row) => byDate.get(row.date).set(row.ticker, row));
  const closes = new Map(tickers.map((ticker) => [
    ticker,
    dates.map((date) => byDate.get(date).get(ticker)?.close ?? null),
  ]));
  return { rows, dates, tickers, byDate, closes };
}

describe('weekly return experiment helpers', () => {
  it('uses the last trading day in each week as the weekly anchor', () => {
    const anchors = buildWeeklyAnchors(syntheticMarket());
    expect(anchors.map((anchor) => anchor.date)).toEqual(['2024-01-05', '2024-01-12', '2024-01-19']);
    expect(nextCalendarFriday('2024-01-05')).toBe('2024-01-12');
  });

  it('builds causal weekly samples with close-to-close and next-open targets', () => {
    const market = longSyntheticMarket();
    const { samples, latestPredictionSample, metadata } = buildWeeklySamples({
      market,
      targetTicker: 'SPY',
      safeTicker: 'BIL',
      minLookbackDays: 60,
    });
    expect(samples.length).toBeGreaterThan(0);
    expect(samples[0].featureGroups.target).toHaveLength(metadata.featureNames.target.length);
    expect(Number.isFinite(sampleTargetReturn(samples[0], 'close_to_close'))).toBe(true);
    expect(Number.isFinite(sampleTargetReturn(samples[0], 'next_open_to_week_close'))).toBe(true);
    expect(latestPredictionSample.date).toBe(market.dates.at(-1));
  });
});
