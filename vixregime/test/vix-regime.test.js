const {
  percentileRank,
  computeCoverageReport,
  buildDailyCloses,
  buildMinuteAlignment,
  buildDailyFeatures,
  buildMinuteFeatures,
  computePortfolioPath,
  buildDailyFirstMinuteCloses,
  computeDailyNextOpenToClosePath,
} = require('../src/vix-regime');
const {
  thirdFriday,
  annotateDailyEventFeatures,
} = require('../src/event-days');

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

function makeRow(symbol, tradeDateUtc, minuteUtc, close, extras = {}) {
  return {
    symbol,
    tradeDateUtc,
    minuteUtc,
    close,
    ...extras,
  };
}

describe('vix regime helpers', () => {
  it('computes percentile rank on a finite window', () => {
    expect(percentileRank([10, 12, 15, 18], 15)).toBe(0.75);
  });

  it('flags deterministic event days like opex and fomc', () => {
    expect(thirdFriday(2025, 9)).toBe('2025-09-19');
    const annotated = annotateDailyEventFeatures(
      [{ tradeDateUtc: '2025-09-17' }, { tradeDateUtc: '2025-09-19' }],
      {
        fomcDates: new Set(['2025-09-17']),
        earningsDates: new Set(),
        monthlyOpex: new Set(['2025-09-19']),
        quarterlyOpex: new Set(['2025-09-19']),
        macroByType: new Map([['CPI', new Set(['2025-09-17'])]]),
      },
    );
    expect(annotated[0].isFomcDay).toBe(true);
    expect(annotated[0].isCpiDay).toBe(true);
    expect(annotated[0].eventScore).toBeGreaterThan(0);
    expect(annotated[1].isMonthlyOpex).toBe(true);
    expect(annotated[1].isQuarterlyOpex).toBe(true);
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

  it('derives richer daily SPY and VIX features from daily bars', () => {
    const rows = [];
    for (let i = 0; i < 25; i += 1) {
      const day = String(i + 2).padStart(2, '0');
      const tradeDateUtc = `2025-01-${day}`;
      rows.push(
        makeRow('SPY', tradeDateUtc, `${tradeDateUtc}T14:30:00.000Z`, 100 + i, { open: 99 + i, high: 101 + i, low: 98 + i }),
        makeRow('SPY', tradeDateUtc, `${tradeDateUtc}T20:59:00.000Z`, 100.5 + i, { open: 100 + i, high: 102 + i, low: 99 + i }),
        makeRow('SPXL', tradeDateUtc, `${tradeDateUtc}T20:59:00.000Z`, 200 + (i * 2)),
        makeRow('SPXS', tradeDateUtc, `${tradeDateUtc}T20:59:00.000Z`, 50 - (i * 0.2)),
        makeRow('SPX', tradeDateUtc, `${tradeDateUtc}T20:59:00.000Z`, 5000 + (i * 10)),
        makeRow('VIX', tradeDateUtc, `${tradeDateUtc}T20:59:00.000Z`, 20 - (i * 0.1)),
        makeRow('VIX9D', tradeDateUtc, `${tradeDateUtc}T20:59:00.000Z`, 19 - (i * 0.1)),
        makeRow('VIX1D', tradeDateUtc, `${tradeDateUtc}T20:59:00.000Z`, 18 - (i * 0.1)),
        makeRow('VIX3M', tradeDateUtc, `${tradeDateUtc}T20:59:00.000Z`, 21 - (i * 0.1)),
      );
    }

    const dailyRows = buildDailyCloses(rows);
    const dailyFeatures = buildDailyFeatures(dailyRows, thresholdConfig);
    const last = dailyFeatures[dailyFeatures.length - 1];

    expect(last.spyOpen).toBeCloseTo(123, 10);
    expect(last.spyClose).toBeCloseTo(124.5, 10);
    expect(last.spyIntradayReturn).toBeCloseTo((124.5 / 123) - 1, 10);
    expect(last.spyRangePct).toBeGreaterThan(0);
    expect(last.spyReturn1d).not.toBeNull();
    expect(last.spyReturn5d).not.toBeNull();
    expect(last.spyMa20).not.toBeNull();
    expect(last.spyMaGap20).not.toBeNull();
    expect(last.spyRealizedVol10).not.toBeNull();
    expect(last.vixChange1d).not.toBeNull();
    expect(last.vix9dChange1d).not.toBeNull();
    expect(last.ts9d30Delta1).not.toBeNull();
    expect(last.vixRiskPremium10).not.toBeNull();
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
    expect(result.summary.executionConvention.returnWindow).toBe('current_bar_close_to_next_bar_close');
    expect(result.observations[0].signalTradeDateUtc).toBe('2025-01-02');
    expect(result.observations[0].holdingPeriodEndTradeDateUtc).toBe('2025-01-03');
    expect(result.observations[1].signalTradeDateUtc).toBe('2025-01-03');
    expect(result.observations[1].holdingPeriodEndTradeDateUtc).toBe('2025-01-06');
    expect(result.summary.turnover).toBeGreaterThan(0);
    expect(result.summary.regimeOccupancy.Calm).toBe(1);
    expect(result.summary.regimeOccupancy.Stress).toBe(1);
  });

  it('computes daily next-open-to-close backtest from prior close signal', () => {
    const rawRows = [
      makeRow('SPY', '2025-01-02', '2025-01-02T20:59:00.000Z', 100),
      makeRow('SPXL', '2025-01-02', '2025-01-02T20:59:00.000Z', 100),
      makeRow('SPXS', '2025-01-02', '2025-01-02T20:59:00.000Z', 100),
      makeRow('VIX', '2025-01-02', '2025-01-02T20:59:00.000Z', 14),
      makeRow('VIX9D', '2025-01-02', '2025-01-02T20:59:00.000Z', 13),
      makeRow('VIX1D', '2025-01-02', '2025-01-02T20:59:00.000Z', 12),
      makeRow('VIX3M', '2025-01-02', '2025-01-02T20:59:00.000Z', 16),
      makeRow('SPX', '2025-01-02', '2025-01-02T20:59:00.000Z', 5000),

      makeRow('SPY', '2025-01-03', '2025-01-03T14:30:00.000Z', 101),
      makeRow('SPXL', '2025-01-03', '2025-01-03T14:30:00.000Z', 102),
      makeRow('SPXS', '2025-01-03', '2025-01-03T14:30:00.000Z', 98),
      makeRow('SPY', '2025-01-03', '2025-01-03T20:59:00.000Z', 103),
      makeRow('SPXL', '2025-01-03', '2025-01-03T20:59:00.000Z', 106),
      makeRow('SPXS', '2025-01-03', '2025-01-03T20:59:00.000Z', 95),
      makeRow('VIX', '2025-01-03', '2025-01-03T20:59:00.000Z', 15),
      makeRow('VIX9D', '2025-01-03', '2025-01-03T20:59:00.000Z', 14),
      makeRow('VIX1D', '2025-01-03', '2025-01-03T20:59:00.000Z', 13),
      makeRow('VIX3M', '2025-01-03', '2025-01-03T20:59:00.000Z', 17),
      makeRow('SPX', '2025-01-03', '2025-01-03T20:59:00.000Z', 5050),
    ];

    const dailyRows = buildDailyCloses(rawRows);
    const firstMinuteRows = buildDailyFirstMinuteCloses(rawRows);
    const dailyFeatures = buildDailyFeatures(dailyRows, thresholdConfig);
    const result = computeDailyNextOpenToClosePath(dailyFeatures, firstMinuteRows, { periodsPerYear: 252 });

    expect(result.observations).toHaveLength(1);
    expect(result.summary.executionConvention.returnWindow).toBe('next_day_first_minute_to_same_day_close');
    expect(result.observations[0].signalTradeDateUtc).toBe('2025-01-02');
    expect(result.observations[0].holdingPeriodStartTradeDateUtc).toBe('2025-01-03');
    expect(result.observations[0].holdingPeriodEndTradeDateUtc).toBe('2025-01-03');
    expect(result.observations[0].benchmarkReturn).toBeCloseTo((103 / 101) - 1, 10);
  });
});
