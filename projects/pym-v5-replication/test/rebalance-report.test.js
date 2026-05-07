const { buildDailyRebalanceReport } = require('../src/rebalance-report');

describe('buildDailyRebalanceReport', () => {
  it('includes daily rebalance snapshots and leaves the latest open when there is no next close', () => {
    const market = {
      dates: ['2025-01-02', '2025-01-03', '2025-01-06'],
      closes: new Map([
        ['SPY', [100, 102, 101]],
        ['QQQ', [200, 198, 204]],
      ]),
      splitAdjustments: [],
    };
    const score = { step: 'asset', ticker: 'SPY' };
    const report = buildDailyRebalanceReport({
      market,
      score,
      startDate: '2025-01-02',
      initialCapital: 10000,
      transactionCostBps: 0,
      slippageBps: 0,
    });

    expect(report.snapshots).toHaveLength(3);
    expect(report.snapshots[0].holdings).toEqual(expect.arrayContaining([
      expect.objectContaining({ ticker: 'SPY', weight: 1 }),
    ]));
    expect(report.snapshots[0].realized.netReturn).toBeCloseTo(0.02, 8);
    expect(report.latest.date).toBe('2025-01-06');
    expect(report.latest.realized).toBeNull();
    expect(report.summary.completedSessions).toBe(2);
    expect(report.summary.finalEquity).toBeCloseTo(10100, 8);
  });
});
