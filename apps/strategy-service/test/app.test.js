const request = require('supertest');

const { createApp } = require('../src/app');

function fakeStrategy() {
  const metadata = {
    id: 'fake-strategy',
    name: 'Fake Strategy',
    cadence: 'daily_eod',
    supports: ['chart', 'latest_portfolio'],
  };
  const report = {
    generatedAt: '2026-05-08T00:00:00.000Z',
    source: { provider: 'test' },
    settings: { startDate: '2026-05-06' },
    summary: {
      latestRebalanceDate: '2026-05-07',
      snapshots: 2,
      totalReturn: 0.1,
      totalReturnPct: 10,
    },
    latest: null,
    equitySeries: [
      {
        date: '2026-05-06',
        signalDate: '2026-05-05',
        equity: 100,
        totalReturn: 0,
        spyReturn: 0,
        qqqReturn: 0,
      },
      {
        date: '2026-05-07',
        signalDate: '2026-05-06',
        equity: 110,
        totalReturn: 0.1,
        spyReturn: 0.03,
        qqqReturn: 0.04,
      },
    ],
    snapshots: [
      {
        date: '2026-05-06',
        nextDate: '2026-05-07',
        equityBeforeNextSession: 100,
        grossExposure: 1,
        turnover: 1,
        turnoverPct: 100,
        topHoldings: 'AAA',
        benchmarkReturns: { spy: 0, qqq: 0 },
        holdings: [{ ticker: 'AAA', weight: 1, weightPct: 100, dollars: 100 }],
        realized: { netReturn: 0.1, netReturnPct: 10, endEquity: 110 },
      },
      {
        date: '2026-05-07',
        nextDate: null,
        equityBeforeNextSession: 110,
        grossExposure: 1,
        turnover: 1,
        turnoverPct: 100,
        topHoldings: 'BBB',
        benchmarkReturns: { spy: 0.03, qqq: 0.04 },
        holdings: [{ ticker: 'BBB', weight: 1, weightPct: 100, dollars: 110 }],
        realized: null,
      },
    ],
  };
  report.latest = report.snapshots[1];
  return {
    state: { loadedAt: '2026-05-08T00:01:00.000Z', refresh: { running: false } },
    getMetadata: () => metadata,
    getReport: () => report,
    recompute: () => report,
  };
}

function fakeRegistry() {
  const strategy = fakeStrategy();
  return {
    listStrategies: () => [strategy.getMetadata()],
    getStrategy: (id) => {
      if (id !== 'fake-strategy') {
        const error = new Error(`unknown_strategy:${id}`);
        error.statusCode = 404;
        error.code = 'unknown_strategy';
        throw error;
      }
      return strategy;
    },
  };
}

describe('strategy-service API', () => {
  it('lists strategies and serves chart ranges', async () => {
    const app = createApp({ registry: fakeRegistry() });
    const strategies = await request(app).get('/api/strategies').expect(200);
    expect(strategies.body.data).toEqual([expect.objectContaining({ id: 'fake-strategy' })]);

    const chart = await request(app)
      .get('/api/strategies/fake-strategy/chart?start=2026-05-07&end=2026-05-07')
      .expect(200);
    expect(chart.body.range.points).toBe(1);
    expect(chart.body.data[0]).toEqual(expect.objectContaining({ date: '2026-05-07', equity: 110 }));
  });

  it('returns latest portfolio change from the previous rebalance', async () => {
    const app = createApp({ registry: fakeRegistry() });
    const response = await request(app).get('/api/strategies/fake-strategy/portfolio/latest').expect(200);
    expect(response.body.data.snapshot.date).toBe('2026-05-07');
    expect(response.body.data.changeFromPrevious.added[0]).toEqual(expect.objectContaining({ ticker: 'BBB' }));
    expect(response.body.data.changeFromPrevious.removed[0]).toEqual(expect.objectContaining({ ticker: 'AAA' }));
  });
});
