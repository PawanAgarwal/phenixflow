const request = require('supertest');

const { createApp } = require('../src/app');
const { createDefaultRegistry } = require('../src/default-registry');

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
  it('registers the PYM studies for dashboard discovery', () => {
    const registry = createDefaultRegistry();
    const strategies = registry.listStrategies();
    expect(strategies.map((strategy) => strategy.id)).toEqual([
      'pym-v5',
      'pym-v5-option-rank-top8',
      'pym-v5-ml-two-speed-attention',
      'pym-v5-ml-calm-trend-router',
      'pym-v5-ml-option-top8-50-50',
      'pym-v5-two-speed-option-meta21',
      'pym-v5-spy-put-pressure-bil',
      'pym-v5-sleeve-meta-21d-cap25',
      'pym-v5-cap25-lgbm-blend40',
      'pym-v5-cap25-lgbm-blend40-stress',
      'option-income-wheel-trend-ivrv',
      'tsll-seconds-passive-scalper',
      'pym-gated-intraday-baseline',
      'pym-gated-intraday-lev3x',
      'pym-gated-intraday-overnight-1x',
      'pym-gated-intraday-best-combo',
      'pym-gated-intraday-deadzone-biasprop-1500exit-3x',
      'occ-pc-contrarian-intraday-1x-long-only',
      'occ-pc-contrarian-intraday-3x',
      'vix-term-contrarian-intraday-vix3m-1x',
      'vix-term-contrarian-intraday-inv-long-3x-overnight',
    ]);
    const byId = Object.fromEntries(strategies.map((strategy) => [strategy.id, strategy]));
    expect(byId['pym-v5'].sourceLinks.map((link) => link.label)).toEqual([
      'Original Study / Notion',
      'Composer Factsheet',
      'Composer Source',
    ]);
    expect(byId['pym-v5-spy-put-pressure-bil'].ruleSummary).toContain(
      'if SPY option put-pressure z-score >= 2.5: hold 100% BIL',
    );
    expect(byId['pym-v5-ml-calm-trend-router'].ruleSummary.join(' ')).toContain('Calm-trend conditions');
    expect(byId['pym-v5-ml-option-top8-50-50'].ruleSummary.join(' ')).toContain('50% option top-8');
    expect(byId['pym-v5-two-speed-option-meta21'].ruleSummary.join(' ')).toContain('prior 21-day');
    expect(byId['pym-v5-sleeve-meta-21d-cap25'].ruleSummary.join(' ')).toContain('cap any single sleeve');
    expect(byId['pym-v5-sleeve-meta-21d-cap25'].displayName).toContain('25% cap');
    expect(byId['pym-v5-cap25-lgbm-blend40'].ruleSummary.join(' ')).toContain('60% cap25 + 40% LGBM');
    expect(byId['pym-v5-cap25-lgbm-blend40'].displayName).toContain('LightGBM');
    expect(byId['pym-v5-cap25-lgbm-blend40-stress'].ruleSummary.join(' ')).toContain('options-stress signal');
    expect(byId['pym-v5-cap25-lgbm-blend40-stress'].displayName).toContain('Options Stress');
    expect(byId['option-income-wheel-trend-ivrv'].ruleSummary.join(' ')).toContain('IV/RV >= 1.10');
    expect(byId['tsll-seconds-passive-scalper'].ruleSummary.join(' ')).toContain('buy limit 3c');
    expect(byId['pym-gated-intraday-baseline'].supports).toContain('trade_log');
  });

  it('lists strategies and serves chart ranges', async () => {
    const app = createApp({ registry: fakeRegistry() });
    const strategies = await request(app).get('/api/strategies').expect(200);
    expect(strategies.body.data).toEqual([expect.objectContaining({ id: 'fake-strategy' })]);

    const chart = await request(app)
      .get('/api/strategies/fake-strategy/chart?start=2026-05-07&end=2026-05-07')
      .expect(200);
    expect(chart.body.range.points).toBe(1);
    expect(chart.body.data[0]).toEqual(expect.objectContaining({ date: '2026-05-07', equity: 110 }));

    const trades = await request(app)
      .get('/api/strategies/fake-strategy/trades?limit=3')
      .expect(200);
    expect(trades.body.range.count).toBe(0);
    expect(trades.body.data).toEqual([]);
  });

  it('returns latest portfolio change from the previous rebalance', async () => {
    const app = createApp({ registry: fakeRegistry() });
    const response = await request(app).get('/api/strategies/fake-strategy/portfolio/latest').expect(200);
    expect(response.body.data.snapshot.date).toBe('2026-05-07');
    expect(response.body.data.changeFromPrevious.added[0]).toEqual(expect.objectContaining({ ticker: 'BBB' }));
    expect(response.body.data.changeFromPrevious.removed[0]).toEqual(expect.objectContaining({ ticker: 'AAA' }));
  });
});
