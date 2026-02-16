const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const request = require('supertest');
const { createApp } = require('../src/app');
const { queryFlow, __private } = require('../src/flow');
const { getThresholdFilterSettings } = require('../src/flow-filter-definitions');

describe('MON-41 execution filters', () => {
  describe('unit: execution filter parsing + classification', () => {
    it('parses execution filters from chips/execution list and boolean params', () => {
      const filters = __private.parseExecutionFilterSet({
        chips: 'Calls,Ask',
        execution: 'aa,sweeps,unknown',
        bid: 'true',
      });

      expect(Array.from(filters).sort()).toEqual(['aa', 'ask', 'bid', 'calls', 'sweeps']);
    });

    it('classifies AA/Ask/Bid boundaries and sweeps', () => {
      const aaTrade = __private.buildExecutionFlags({ right: 'C', price: 2.12, bid: 2.0, ask: 2.1, isSweep: true });
      expect(aaTrade).toMatchObject({ calls: true, puts: false, bid: false, ask: false, aa: true, sweeps: true });

      const askTrade = __private.buildExecutionFlags({ right: 'P', price: 2.1, bid: 2.0, ask: 2.1 });
      expect(askTrade).toMatchObject({ calls: false, puts: true, bid: false, ask: true, aa: false, sweeps: false });

      const bidTrade = __private.buildExecutionFlags({ right: 'P', price: 2.0, bid: 2.0, ask: 2.1 });
      expect(bidTrade).toMatchObject({ bid: true, ask: false, aa: false });
    });
  });

  describe('integration: /api/flow execution filters over production query pipeline', () => {
    let app;
    let artifactPath;

    beforeAll(() => {
      app = createApp();
      const artifactDir = fs.mkdtempSync(path.join(os.tmpdir(), 'mon-41-exec-filters-'));
      artifactPath = path.join(artifactDir, 'flow-read.json');
      fs.writeFileSync(
        artifactPath,
        JSON.stringify({
          rows: [
            {
              id: 'trade_call_bid',
              symbol: 'AAPL',
              strategy: 'breakout',
              status: 'open',
              timeframe: '1m',
              pnl: 5,
              volume: 10,
              createdAt: '2026-02-15T16:00:00.000Z',
              updatedAt: '2026-02-15T16:00:01.000Z',
              right: 'CALL',
              price: 1.0,
              bid: 1.0,
              ask: 1.1,
              isSweep: false,
            },
            {
              id: 'trade_put_ask',
              symbol: 'AAPL',
              strategy: 'breakout',
              status: 'open',
              timeframe: '1m',
              pnl: 7,
              volume: 10,
              createdAt: '2026-02-15T16:00:02.000Z',
              updatedAt: '2026-02-15T16:00:03.000Z',
              right: 'PUT',
              price: 1.1,
              bid: 1.0,
              ask: 1.1,
              isSweep: false,
            },
            {
              id: 'trade_call_aa_sweep',
              symbol: 'AAPL',
              strategy: 'breakout',
              status: 'open',
              timeframe: '1m',
              pnl: 9,
              volume: 10,
              createdAt: '2026-02-15T16:00:04.000Z',
              updatedAt: '2026-02-15T16:00:05.000Z',
              right: 'CALL',
              price: 1.12,
              bid: 1.0,
              ask: 1.1,
              isSweep: true,
            },
          ],
        }),
        'utf8',
      );
    });

    const baseQuery = {
      source: 'real-ingest',
      artifactPath: undefined,
      limit: 20,
      sortBy: 'createdAt',
      sortOrder: 'asc',
    };

    it.each([
      ['calls', { calls: 'true' }, ['trade_call_bid', 'trade_call_aa_sweep']],
      ['puts', { puts: 'true' }, ['trade_put_ask']],
      ['bid', { bid: 'true' }, ['trade_call_bid']],
      ['ask', { ask: 'true' }, ['trade_put_ask']],
      ['aa', { aa: 'true' }, ['trade_call_aa_sweep']],
      ['sweeps', { sweeps: 'true' }, ['trade_call_aa_sweep']],
      ['chips alias', { chips: 'calls,aa' }, ['trade_call_aa_sweep']],
      ['execution alias', { execution: 'puts,ask' }, ['trade_put_ask']],
    ])('filters by %s', async (_name, filterQuery, expectedIds) => {
      const response = await request(app)
        .get('/api/flow')
        .query({ ...baseQuery, artifactPath, ...filterQuery });

      expect(response.statusCode).toBe(200);
      expect(response.body.data.map((row) => row.id)).toEqual(expectedIds);
      expect(response.body.meta.observability).toMatchObject({
        source: 'real-ingest',
        artifactPath: path.resolve(artifactPath),
      });
    });

    it('applies execution filters inside flow query/filter pipeline directly', () => {
      const result = queryFlow(
        { source: 'real-ingest', artifactPath, execution: 'calls,aa' },
        { filterVersion: 'legacy' },
      );

      expect(result.data.map((row) => row.id)).toEqual(['trade_call_aa_sweep']);
    });
  });

  describe('MON-42 size/value threshold filters', () => {
    const withThresholdEnv = (overrides, callback) => {
      const previous = {
        FLOW_FILTER_PREMIUM_100K_MIN: process.env.FLOW_FILTER_PREMIUM_100K_MIN,
        FLOW_FILTER_PREMIUM_SIZABLE_MIN: process.env.FLOW_FILTER_PREMIUM_SIZABLE_MIN,
        FLOW_FILTER_PREMIUM_WHALES_MIN: process.env.FLOW_FILTER_PREMIUM_WHALES_MIN,
        FLOW_FILTER_SIZE_LARGE_MIN: process.env.FLOW_FILTER_SIZE_LARGE_MIN,
      };

      Object.assign(process.env, overrides);

      try {
        return callback();
      } finally {
        Object.entries(previous).forEach(([key, value]) => {
          if (value === undefined) {
            delete process.env[key];
          } else {
            process.env[key] = value;
          }
        });
      }
    };

    it('parses threshold chips and explicit booleans', () => {
      const filters = __private.parseThresholdFilterSet({
        chips: 'Sizable,Large Size,unknown',
        sizeValue: '100k+,whales',
        largeSize: 'true',
      });

      expect(Array.from(filters).sort()).toEqual(['100k', 'largeSize', 'sizable', 'whales']);
    });

    it('uses canonical premium and size fields at exact threshold boundaries', () => {
      withThresholdEnv(
        {
          FLOW_FILTER_PREMIUM_100K_MIN: '100000',
          FLOW_FILTER_PREMIUM_SIZABLE_MIN: '25000',
          FLOW_FILTER_PREMIUM_WHALES_MIN: '500000',
          FLOW_FILTER_SIZE_LARGE_MIN: '1000',
        },
        () => {
          const thresholds = getThresholdFilterSettings(process.env);

          expect(__private.buildThresholdFlags({ premium: thresholds.sizable - 1, size: 999 }, thresholds)).toMatchObject({
            sizable: false,
            largeSize: false,
          });

          expect(__private.buildThresholdFlags({ premium: thresholds.sizable, size: thresholds.largeSize }, thresholds)).toMatchObject({
            sizable: true,
            largeSize: true,
          });

          expect(__private.buildThresholdFlags({ premium: thresholds['100k'] - 1 }, thresholds)['100k']).toBe(false);
          expect(__private.buildThresholdFlags({ premium: thresholds['100k'] }, thresholds)['100k']).toBe(true);

          expect(__private.buildThresholdFlags({ premium: thresholds.whales - 1 }, thresholds).whales).toBe(false);
          expect(__private.buildThresholdFlags({ premium: thresholds.whales }, thresholds).whales).toBe(true);
        },
      );
    });

    it('filters /api/flow real-ingest rows by threshold filters including fallback premium calc', () => {
      const artifactDir = fs.mkdtempSync(path.join(os.tmpdir(), 'mon-42-threshold-filters-'));
      const artifactPath = path.join(artifactDir, 'flow-read.json');

      fs.writeFileSync(
        artifactPath,
        JSON.stringify({
          rows: [
            {
              id: 'below_100k',
              symbol: 'AAPL',
              strategy: 'breakout',
              status: 'open',
              timeframe: '1m',
              pnl: 1,
              volume: 10,
              createdAt: '2026-02-15T16:00:00.000Z',
              updatedAt: '2026-02-15T16:00:01.000Z',
              premium: 99999,
              size: 999,
            },
            {
              id: 'at_100k_large',
              symbol: 'AAPL',
              strategy: 'breakout',
              status: 'open',
              timeframe: '1m',
              pnl: 1,
              volume: 10,
              createdAt: '2026-02-15T16:00:02.000Z',
              updatedAt: '2026-02-15T16:00:03.000Z',
              premium: 100000,
              size: 1000,
            },
            {
              id: 'at_whales_via_price_size',
              symbol: 'AAPL',
              strategy: 'breakout',
              status: 'open',
              timeframe: '1m',
              pnl: 1,
              volume: 10,
              createdAt: '2026-02-15T16:00:04.000Z',
              updatedAt: '2026-02-15T16:00:05.000Z',
              price: 5,
              size: 1000,
            },
          ],
        }),
        'utf8',
      );

      withThresholdEnv(
        {
          FLOW_FILTER_PREMIUM_100K_MIN: '100000',
          FLOW_FILTER_PREMIUM_SIZABLE_MIN: '25000',
          FLOW_FILTER_PREMIUM_WHALES_MIN: '500000',
          FLOW_FILTER_SIZE_LARGE_MIN: '1000',
        },
        () => {
          const result100k = queryFlow({ source: 'real-ingest', artifactPath, sizeValue: '100k+' });
          expect(result100k.data.map((row) => row.id).sort()).toEqual(['at_100k_large', 'at_whales_via_price_size']);

          const resultWhales = queryFlow({ source: 'real-ingest', artifactPath, whales: 'true' });
          expect(resultWhales.data.map((row) => row.id)).toEqual(['at_whales_via_price_size']);

          const resultLarge = queryFlow({ source: 'real-ingest', artifactPath, chips: 'Large Size' });
          expect(resultLarge.data.map((row) => row.id).sort()).toEqual(['at_100k_large', 'at_whales_via_price_size']);
        },
      );
    });
  });
});
