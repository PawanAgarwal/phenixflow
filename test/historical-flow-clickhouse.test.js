const { __private } = require('../src/historical-flow');

function createQueryRowsStub(overrides = {}) {
  return (query) => {
    if (query.includes('FROM options.option_trade_day_cache')) {
      return overrides.dayCache || [{
        symbol: 'AAPL',
        tradeDateUtc: '2026-03-04',
        cacheStatus: 'full',
        rowCount: 2,
        lastSyncAtUtc: '2026-03-07T08:00:00.000Z',
        lastError: null,
        sourceEndpoint: null,
      }];
    }

    if (query.includes('FROM options.option_trade_metric_day_cache')) {
      return overrides.metricCache || [{
        metricName: 'sigScore',
        cacheStatus: 'full',
        rowCount: 2,
        lastError: null,
        lastSyncAtUtc: '2026-03-07T08:00:00.000Z',
      }];
    }

    if (query.includes('FROM options.option_trade_enriched')) {
      return overrides.rows || [{
        id: 'trade-1',
        tradeTsUtc: '2026-03-04T14:30:00.000Z',
        symbol: 'AAPL',
        expiration: '2026-03-20',
        strike: 200,
        right: 'CALL',
        price: 1.5,
        size: 10,
        bid: 1.4,
        ask: 1.6,
        conditionCode: '18',
        exchange: 'OPRA',
        value: 1500,
        dte: 16,
        spot: 198.5,
        otmPct: 0.75,
        dayVolume: 100,
        oi: 250,
        volOiRatio: 0.4,
        repeat3m: 0,
        sigScore: 0.82,
        sentiment: 'bullish',
        executionSide: 'ASK',
        symbolVol1m: 1000,
        symbolVolBaseline15m: 400,
        openWindowBaseline: 300,
        bullishRatio15m: 0.8,
        isSweep: 0,
        isMultileg: 0,
        minuteOfDayEt: 570,
        delta: 0.45,
        impliedVol: 0.32,
        timeNorm: 0.5,
        deltaNorm: 0.6,
        ivSkewNorm: 0.7,
        valueShockNorm: 0.3,
        dteSwingNorm: 0.4,
        flowImbalanceNorm: 0.2,
        deltaPressureNorm: 0.1,
        cpOiPressureNorm: 0.15,
        ivSkewSurfaceNorm: 0.25,
        ivTermSlopeNorm: 0.35,
        underlyingTrendConfirmNorm: 0.45,
        liquidityQualityNorm: 0.55,
        multilegPenaltyNorm: 0,
        ruleVersion: 'v4_expanded_default',
        scoreQuality: 'complete',
        missingMetricsJson: '[]',
        sigScoreComponentsJson: '{"time":0.5}',
        chipsJson: '["calls","high-sig"]',
      }];
    }

    return [];
  };
}

describe('historical flow ClickHouse helpers', () => {
  it('loads a fully cached historical day from ClickHouse', () => {
    const result = __private.loadClickHouseHistoricalDay({
      symbol: 'AAPL',
      dayIso: '2026-03-04',
      from: '2026-03-04T00:00:00.000Z',
      to: '2026-03-04T23:59:59.999Z',
      requiredMetrics: ['sigScore'],
      env: {
        CLICKHOUSE_HOST: '127.0.0.1',
        CLICKHOUSE_PORT: '9000',
        CLICKHOUSE_DATABASE: 'options',
      },
      queryRows: createQueryRowsStub(),
    });

    expect(result).not.toBeNull();
    expect(result.observability.source).toBe('clickhouse');
    expect(result.observability.artifactPath).toBe('clickhouse://127.0.0.1:9000/options');
    expect(result.dayCache).toMatchObject({ cacheStatus: 'full', rowCount: 2 });
    expect(result.enrichment).toMatchObject({
      synced: false,
      reason: 'metric_cache_full',
      rowCount: 1,
      ruleVersion: 'v4_expanded_default',
    });
    expect(result.rows).toHaveLength(1);
    expect(result.rows[0]).toMatchObject({
      id: 'trade-1',
      chips: ['calls', 'high-sig'],
      missingMetrics: [],
      sigScoreComponents: { time: 0.5 },
    });
  });

  it('falls back when a required metric cache is not full in ClickHouse', () => {
    const result = __private.loadClickHouseHistoricalDay({
      symbol: 'AAPL',
      dayIso: '2026-03-04',
      from: '2026-03-04T00:00:00.000Z',
      to: '2026-03-04T23:59:59.999Z',
      requiredMetrics: ['sigScore'],
      env: {},
      queryRows: createQueryRowsStub({
        metricCache: [{
          metricName: 'sigScore',
          cacheStatus: 'partial',
          rowCount: 1,
          lastError: 'still_hydrating',
          lastSyncAtUtc: '2026-03-07T08:00:00.000Z',
        }],
      }),
    });

    expect(result).toBeNull();
  });

  it('uses latest grouped cache rows for metric cache lookups', () => {
    let capturedQuery = '';
    const metricCacheMap = __private.getClickHouseMetricCacheMap({
      symbol: 'AAPL',
      dayIso: '2026-03-04',
      env: {},
      queryRows: (query) => {
        capturedQuery = query;
        return [{
          metricName: 'sigScore',
          cacheStatus: 'full',
          rowCount: 2,
          lastError: null,
          lastSyncAtUtc: '2026-03-07T08:00:00.000Z',
        }];
      },
    });

    expect(capturedQuery).toContain('argMax(cache_status, last_sync_at_utc)');
    expect(capturedQuery).toContain('GROUP BY metric_name');
    expect(metricCacheMap.sigScore).toMatchObject({
      cacheStatus: 'full',
      rowCount: 2,
    });
  });

  it('lists cached days from the latest grouped day cache rows', () => {
    let capturedQuery = '';
    const rows = __private.listClickHouseCachedDays({}, (query) => {
      capturedQuery = query;
      return [{ symbol: 'AAPL', dayIso: '2026-03-04', rowCount: 2 }];
    });

    expect(capturedQuery).toContain('argMax(row_count, last_sync_at_utc)');
    expect(capturedQuery).toContain("HAVING argMax(cache_status, last_sync_at_utc) = 'full'");
    expect(rows).toEqual([{ symbol: 'AAPL', dayIso: '2026-03-04', rowCount: 2 }]);
  });

  it('skips delete mutations for versioned cache/support tables', () => {
    expect(__private.requiresClickHouseDeleteBeforeInsert('option_trade_day_cache')).toBe(false);
    expect(__private.requiresClickHouseDeleteBeforeInsert('option_trade_metric_day_cache')).toBe(false);
    expect(__private.requiresClickHouseDeleteBeforeInsert('supplemental_metric_cache')).toBe(false);
    expect(__private.requiresClickHouseDeleteBeforeInsert('feature_baseline_intraday')).toBe(false);
    expect(__private.requiresClickHouseDeleteBeforeInsert('option_trade_enriched')).toBe(true);
  });
});
