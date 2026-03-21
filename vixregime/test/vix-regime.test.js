const {
  percentileRank,
  computeCoverageReport,
  buildDailyCloses,
  buildMinuteAlignment,
  buildDailyFeatures,
  buildMinuteFeatures,
  computePortfolioPath,
} = require('../src/vix-regime');

const thresholdConfig = {
  exposures: {
    calm: 2,
    normal: 1,
    stress: 0.3,
    crash: -1,
  },
  day: {
    vixCalmMax: 16,
    vixNormalMax: 25,
    vixStressMin: 25,
    vixCrashMin: 35,
    vixPctRankCalmMax: 0.3,
    vixPctRankStressMin: 0.7,
    delta5NormalMax: 2,
    delta5CrashMin: 5,
    delta10CalmMax: 0,
    termSoft9d30: 1.03,
    termHard9d30: 1.1,
    termSoft30d90d: 1,
    termHard30d90d: 1.05,
    crashBrakeVix1dMultiple: 1.35,
    crashBrakeVix1d9d: 1.2,
  },
  minute: {
    vixCalmMax: 16,
    vixNormalMax: 25,
    vixStressMin: 25,
    vixCrashMin: 35,
    vixPctRankCalmMax: 0.3,
    vixPctRankStressMin: 0.7,
    delta30NormalMax: 1.5,
    delta60CrashMin: 3.5,
    termSoft9d30: 1.03,
    termHard9d30: 1.1,
    termSoft30d90d: 1,
    termHard30d90d: 1.05,
    crashBrakeVix1dMultiple: 1.35,
    crashBrakeVix1d9d: 1.2,
  },
};

function makeRow(symbol, tradeDateUtc, minuteUtc, close) {
  return {
    symbol,
    tradeDateUtc,
    minuteUtc,
    close,
  };
}

describe('vix regime helpers', () => {
  it('computes percentile rank on a finite window', () => {
    expect(percentileRank([10, 12, 15, 18], 15)).toBe(0.75);
  });

  it('reports missing symbol coverage relative to SPY', () => {
    const rows = [
      makeRow('SPY', '2025-01-02', '2025-01-02T14:30:00.000Z', 100),
      makeRow('SPY', '2025-01-03', '2025-01-03T14:30:00.000Z', 101),
      makeRow('SPX', '2025-01-02', '2025-01-02T14:30:00.000Z', 5000),
    ];

    const report = computeCoverageReport(rows, { requiredSymbols: ['SPY', 'SPX'] });
    const spx = report.symbols.find((row) => row.symbol === 'SPX');

    expect(report.datasetReady).toBe(false);
    expect(spx.missingDaysRelativeToSpy).toBe(1);
    expect(spx.missingDayList).toEqual(['2025-01-03']);
  });

  it('builds day and minute features and classifies without lookahead', () => {
    const rows = [
      makeRow('SPX', '2025-01-02', '2025-01-02T20:59:00.000Z', 5000),
      makeRow('SPY', '2025-01-02', '2025-01-02T20:59:00.000Z', 500),
      makeRow('SPXL', '2025-01-02', '2025-01-02T20:59:00.000Z', 100),
      makeRow('SPXS', '2025-01-02', '2025-01-02T20:59:00.000Z', 50),
      makeRow('VIX', '2025-01-02', '2025-01-02T20:59:00.000Z', 14),
      makeRow('VIX9D', '2025-01-02', '2025-01-02T20:59:00.000Z', 13),
      makeRow('VIX1D', '2025-01-02', '2025-01-02T20:59:00.000Z', 12),
      makeRow('VIX3M', '2025-01-02', '2025-01-02T20:59:00.000Z', 16),
      makeRow('SPX', '2025-01-03', '2025-01-03T20:58:00.000Z', 4900),
      makeRow('SPY', '2025-01-03', '2025-01-03T20:58:00.000Z', 490),
      makeRow('SPXL', '2025-01-03', '2025-01-03T20:58:00.000Z', 90),
      makeRow('SPXS', '2025-01-03', '2025-01-03T20:58:00.000Z', 60),
      makeRow('VIX', '2025-01-03', '2025-01-03T20:58:00.000Z', 40),
      makeRow('VIX9D', '2025-01-03', '2025-01-03T20:58:00.000Z', 44),
      makeRow('VIX1D', '2025-01-03', '2025-01-03T20:58:00.000Z', 60),
      makeRow('VIX3M', '2025-01-03', '2025-01-03T20:58:00.000Z', 35),
      makeRow('SPX', '2025-01-03', '2025-01-03T14:31:00.000Z', 4950),
      makeRow('SPY', '2025-01-03', '2025-01-03T14:31:00.000Z', 495),
      makeRow('SPXL', '2025-01-03', '2025-01-03T14:31:00.000Z', 95),
      makeRow('SPXS', '2025-01-03', '2025-01-03T14:31:00.000Z', 55),
      makeRow('VIX', '2025-01-03', '2025-01-03T14:31:00.000Z', 32),
      makeRow('VIX9D', '2025-01-03', '2025-01-03T14:31:00.000Z', 34),
      makeRow('VIX1D', '2025-01-03', '2025-01-03T14:31:00.000Z', 43),
      makeRow('VIX3M', '2025-01-03', '2025-01-03T14:31:00.000Z', 30),
    ];

    const dailyRows = buildDailyCloses(rows);
    const minuteRows = buildMinuteAlignment(rows);
    const dailyFeatures = buildDailyFeatures(dailyRows, thresholdConfig);
    const minuteFeatures = buildMinuteFeatures(minuteRows, thresholdConfig, dailyFeatures);

    expect(dailyFeatures).toHaveLength(2);
    expect(dailyFeatures[0].regime).toBe('Calm');
    expect(dailyFeatures[1].regime).toBe('Crash');
    expect(minuteFeatures.find((row) => row.timestamp === '2025-01-03T14:31:00.000Z').regime).toBe('Crash');
  });

  it('computes next-bar backtest stats and cash-aware weights', () => {
    const rows = [
      {
        timestamp: '2025-01-02T20:59:00.000Z',
        tradeDateUtc: '2025-01-02',
        regime: 'Calm',
        exposure: 2,
        reasons: ['vix_calm'],
        spyClose: 100,
        spxlClose: 100,
        spxsClose: 100,
      },
      {
        timestamp: '2025-01-03T20:59:00.000Z',
        tradeDateUtc: '2025-01-03',
        regime: 'Stress',
        exposure: 0.3,
        reasons: ['vix_stress'],
        spyClose: 101,
        spxlClose: 103,
        spxsClose: 97,
      },
      {
        timestamp: '2025-01-06T20:59:00.000Z',
        tradeDateUtc: '2025-01-06',
        regime: 'Crash',
        exposure: -1,
        reasons: ['vix_crash'],
        spyClose: 99,
        spxlClose: 95,
        spxsClose: 104,
      },
    ];

    const result = computePortfolioPath(rows, { periodsPerYear: 252 });
    expect(result.observations).toHaveLength(2);
    expect(result.summary.turnover).toBeGreaterThan(0);
    expect(result.summary.regimeOccupancy.Calm).toBe(1);
    expect(result.summary.regimeOccupancy.Stress).toBe(1);
  });
});
