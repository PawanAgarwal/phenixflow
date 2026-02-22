const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const request = require('supertest');
const Database = require('better-sqlite3');
const { createApp } = require('../src/app');

const FRIDAY_FROM = '2026-02-13T00:00:00.000Z';
const FRIDAY_TO = '2026-02-13T23:59:59.999Z';

function makeTempDbPath() {
  const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'flow-historical-api-'));
  return { tempDir, dbPath: path.join(tempDir, 'historical.sqlite') };
}

describe('historical flow API', () => {
  let app;
  let tempDir;
  let dbPath;
  let previousDbPath;
  let previousThetaBaseUrl;
  let previousThetaSpotPath;
  let previousThetaOiPath;
  let previousFetch;
  let fetchCalls;

  beforeAll(() => {
    previousDbPath = process.env.PHENIX_DB_PATH;
    previousThetaBaseUrl = process.env.THETADATA_BASE_URL;
    previousThetaSpotPath = process.env.THETADATA_SPOT_PATH;
    previousThetaOiPath = process.env.THETADATA_OI_PATH;
    previousFetch = global.fetch;

    ({ tempDir, dbPath } = makeTempDbPath());
    process.env.PHENIX_DB_PATH = dbPath;
    process.env.THETADATA_BASE_URL = 'http://thetadata.local:25503';

    fetchCalls = [];

    global.fetch = async (url) => {
      fetchCalls.push(String(url));
      return {
        ok: true,
        status: 200,
        text: async () => JSON.stringify({
          symbol: ['AAPL', 'AAPL'],
          trade_timestamp: ['2026-02-13T14:35:00.000Z', '2026-02-13T15:10:00.000Z'],
          expiration: ['2026-02-20', '2026-02-27'],
          strike: [212.5, 215],
          right: ['CALL', 'CALL'],
          price: [1.87, 2.11],
          size: [200, 340],
          bid: [1.84, 2.07],
          ask: [1.88, 2.12],
          condition: [18, 18],
          exchange: ['OPRA', 'OPRA'],
        }),
      };
    };

    app = createApp();
  });

  afterAll(() => {
    if (previousDbPath === undefined) delete process.env.PHENIX_DB_PATH;
    else process.env.PHENIX_DB_PATH = previousDbPath;

    if (previousThetaBaseUrl === undefined) delete process.env.THETADATA_BASE_URL;
    else process.env.THETADATA_BASE_URL = previousThetaBaseUrl;

    if (previousThetaSpotPath === undefined) delete process.env.THETADATA_SPOT_PATH;
    else process.env.THETADATA_SPOT_PATH = previousThetaSpotPath;

    if (previousThetaOiPath === undefined) delete process.env.THETADATA_OI_PATH;
    else process.env.THETADATA_OI_PATH = previousThetaOiPath;

    global.fetch = previousFetch;

    if (tempDir && fs.existsSync(tempDir)) {
      fs.rmSync(tempDir, { recursive: true, force: true });
    }
  });

  it('downloads from Theta on cache miss, persists into sqlite, and filters by symbol', async () => {
    const response = await request(app)
      .get('/api/flow/historical')
      .query({ from: FRIDAY_FROM, to: FRIDAY_TO, symbol: 'aapl' });

    expect(response.statusCode).toBe(200);
    expect(response.body.data).toHaveLength(2);
    expect(response.body.meta.sync).toMatchObject({
      synced: true,
      fetchedRows: 2,
      upsertedRows: 2,
      cachedRows: 0,
    });

    const db = new Database(dbPath, { readonly: true });
    const count = db.prepare('SELECT COUNT(*) AS c FROM option_trades WHERE symbol = ?').get('AAPL').c;
    const symbolMinuteCount = db.prepare(`
      SELECT COUNT(*) AS c
      FROM option_symbol_minute_derived
      WHERE symbol = ? AND trade_date_utc = ?
    `).get('AAPL', '2026-02-13').c;
    const contractMinuteCount = db.prepare(`
      SELECT COUNT(*) AS c
      FROM option_contract_minute_derived
      WHERE symbol = ? AND trade_date_utc = ?
    `).get('AAPL', '2026-02-13').c;
    const dayCache = db
      .prepare('SELECT cache_status AS cacheStatus, row_count AS rowCount FROM option_trade_day_cache WHERE symbol = ? AND trade_date_utc = ?')
      .get('AAPL', '2026-02-13');
    db.close();

    expect(count).toBe(2);
    expect(symbolMinuteCount).toBeGreaterThan(0);
    expect(contractMinuteCount).toBeGreaterThan(0);
    expect(dayCache).toMatchObject({ cacheStatus: 'full', rowCount: 2 });
    expect(fetchCalls.length).toBe(1);
  });

  it('skips Theta call on cache hit for same day/symbol', async () => {
    const response = await request(app)
      .get('/api/flow/historical')
      .query({ from: FRIDAY_FROM, to: FRIDAY_TO, symbol: 'AAPL' });

    expect(response.statusCode).toBe(200);
    expect(response.body.meta.sync).toMatchObject({
      synced: false,
      reason: 'day_cache_full',
      fetchedRows: 0,
      upsertedRows: 0,
      cachedRows: 2,
    });

    expect(fetchCalls.length).toBe(1);
  });

  it('reports full filtered total even when page limit truncates rows', async () => {
    const response = await request(app)
      .get('/api/flow/historical')
      .query({ from: FRIDAY_FROM, to: FRIDAY_TO, symbol: 'IBM', limit: 1 });

    expect(response.statusCode).toBe(200);
    expect(response.body.data).toHaveLength(1);
    expect(response.body.meta.total).toBe(2);
  });

  it('marks day cache partial when Theta sync fails and retries next call', async () => {
    let failNextMsftFetch = true;
    global.fetch = async (url) => {
      fetchCalls.push(String(url));
      if (String(url).includes('symbol=MSFT') && failNextMsftFetch) {
        failNextMsftFetch = false;
        return {
          ok: false,
          status: 500,
          text: async () => JSON.stringify({ error: 'boom' }),
        };
      }
      return {
        ok: true,
        status: 200,
        text: async () => JSON.stringify({
          symbol: ['MSFT'],
          trade_timestamp: ['2026-02-13T14:35:00.000Z'],
          expiration: ['2026-02-20'],
          strike: [420],
          right: ['CALL'],
          price: [1.87],
          size: [200],
          bid: [1.84],
          ask: [1.88],
          condition: [18],
          exchange: ['OPRA'],
        }),
      };
    };

    const first = await request(app)
      .get('/api/flow/historical')
      .query({ from: FRIDAY_FROM, to: FRIDAY_TO, symbol: 'MSFT' });

    expect(first.statusCode).toBe(502);
    expect(first.body.error.code).toBe('thetadata_sync_failed');

    const db = new Database(dbPath, { readonly: true });
    const dayCacheAfterFailure = db
      .prepare('SELECT cache_status AS cacheStatus FROM option_trade_day_cache WHERE symbol = ? AND trade_date_utc = ?')
      .get('MSFT', '2026-02-13');
    db.close();

    expect(dayCacheAfterFailure).toMatchObject({ cacheStatus: 'partial' });

    const second = await request(app)
      .get('/api/flow/historical')
      .query({ from: FRIDAY_FROM, to: FRIDAY_TO, symbol: 'MSFT' });

    expect(second.statusCode).toBe(200);
    expect(second.body.meta.sync).toMatchObject({
      synced: true,
      fetchedRows: 1,
      upsertedRows: 1,
    });
  });

  it('returns validation error for missing required params', async () => {
    const response = await request(app).get('/api/flow/historical').query({ symbol: 'AAPL' });
    expect(response.statusCode).toBe(400);
    expect(response.body.error.code).toBe('invalid_query');
  });

  it('filters by historical chips and right query params', async () => {
    global.fetch = async (url) => {
      fetchCalls.push(String(url));
      return {
        ok: true,
        status: 200,
        text: async () => JSON.stringify({
          symbol: ['NFLX', 'NFLX', 'NFLX'],
          trade_timestamp: ['2026-02-13T14:35:00.000Z', '2026-02-13T14:36:00.000Z', '2026-02-13T14:37:00.000Z'],
          expiration: ['2026-02-20', '2026-02-20', '2026-02-20'],
          strike: [1000, 1000, 1000],
          right: ['CALL', 'PUT', 'CALL'],
          price: [10, 9.5, 10.2],
          size: [100, 100, 1500],
          bid: [10, 9.5, 10],
          ask: [10.1, 9.6, 10.1],
          condition: [18, 18, 18],
          exchange: ['OPRA', 'OPRA', 'OPRA'],
        }),
      };
    };

    const callsOnly = await request(app)
      .get('/api/flow/historical')
      .query({ from: FRIDAY_FROM, to: FRIDAY_TO, symbol: 'NFLX', chips: 'calls' });

    expect(callsOnly.statusCode).toBe(200);
    expect(callsOnly.body.data.every((row) => row.right === 'CALL')).toBe(true);

    const bidOnly = await request(app)
      .get('/api/flow/historical')
      .query({ from: FRIDAY_FROM, to: FRIDAY_TO, symbol: 'NFLX', chips: 'bid' });

    expect(bidOnly.statusCode).toBe(200);
    expect(bidOnly.body.data).toHaveLength(2);
    expect(bidOnly.body.data.every((row) => Array.isArray(row.chips) && row.chips.includes('bid'))).toBe(true);

    const putAskOnly = await request(app)
      .get('/api/flow/historical')
      .query({
        from: FRIDAY_FROM,
        to: FRIDAY_TO,
        symbol: 'NFLX',
        type: 'put',
        side: 'BID',
        expiration: '2026-02-20',
      });

    expect(putAskOnly.statusCode).toBe(200);
    expect(putAskOnly.body.data).toHaveLength(1);
    expect(putAskOnly.body.data[0]).toMatchObject({
      right: 'PUT',
      expiration: '2026-02-20',
    });
  });

  it('returns metric_unavailable for filters that require unavailable metrics', async () => {
    const priorFetch = global.fetch;
    const priorSpotPath = process.env.THETADATA_SPOT_PATH;
    process.env.THETADATA_SPOT_PATH = '/v3/stock/history/ohlc';

    try {
      const db = new Database(dbPath);
      db.prepare('DELETE FROM option_trade_metric_day_cache WHERE symbol = ? AND trade_date_utc = ?')
        .run('AAPL', '2026-02-13');
      db.close();

      global.fetch = async (url) => {
        const endpoint = String(url);
        if (endpoint.includes('/v3/option/history/trade_quote')) {
          return {
            ok: true,
            status: 200,
            text: async () => JSON.stringify({
              symbol: ['AAPL'],
              trade_timestamp: ['2026-02-13T14:35:00.000Z'],
              expiration: ['2026-02-20'],
              strike: [212.5],
              right: ['CALL'],
              price: [1.87],
              size: [200],
              bid: [1.84],
              ask: [1.88],
              condition: [18],
              exchange: ['OPRA'],
            }),
          };
        }

        return {
          ok: false,
          status: 404,
          text: async () => JSON.stringify({ error: 'not_found' }),
        };
      };

      const response = await request(app)
        .get('/api/flow/historical')
        .query({ from: FRIDAY_FROM, to: FRIDAY_TO, symbol: 'AAPL', chips: 'otm' });

      expect(response.statusCode).toBe(422);
      expect(response.body.error.code).toBe('metric_unavailable');
    } finally {
      global.fetch = priorFetch;
      if (priorSpotPath === undefined) delete process.env.THETADATA_SPOT_PATH;
      else process.env.THETADATA_SPOT_PATH = priorSpotPath;
    }
  });

  it('uses gov OI reference rows as fallback so vol>oi filter can run without Theta OI endpoint', async () => {
    const db = new Database(dbPath);
    db.exec(`
      CREATE TABLE IF NOT EXISTS option_open_interest_reference (
        source TEXT NOT NULL,
        source_url TEXT,
        as_of_date TEXT NOT NULL,
        symbol TEXT NOT NULL,
        expiration TEXT NOT NULL,
        strike REAL NOT NULL,
        option_right TEXT NOT NULL CHECK (option_right IN ('CALL', 'PUT')),
        oi INTEGER NOT NULL CHECK (oi >= 0),
        raw_payload_json TEXT,
        ingested_at_utc TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
        PRIMARY KEY (source, as_of_date, symbol, expiration, strike, option_right)
      );
    `);
    db.prepare(`
      INSERT INTO option_open_interest_reference (
        source,
        source_url,
        as_of_date,
        symbol,
        expiration,
        strike,
        option_right,
        oi,
        raw_payload_json
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(source, as_of_date, symbol, expiration, strike, option_right) DO UPDATE SET
        oi = excluded.oi,
        raw_payload_json = excluded.raw_payload_json,
        ingested_at_utc = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
    `).run('FINRA', 'https://example.gov/oi.csv', '2026-02-13', 'AAPL', '2026-02-20', 212.5, 'CALL', 10, '{}');
    db.prepare(`
      INSERT INTO option_open_interest_reference (
        source,
        source_url,
        as_of_date,
        symbol,
        expiration,
        strike,
        option_right,
        oi,
        raw_payload_json
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(source, as_of_date, symbol, expiration, strike, option_right) DO UPDATE SET
        oi = excluded.oi,
        raw_payload_json = excluded.raw_payload_json,
        ingested_at_utc = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
    `).run('FINRA', 'https://example.gov/oi.csv', '2026-02-13', 'AAPL', '2026-02-27', 215, 'CALL', 10, '{}');
    db.close();

    const response = await request(app)
      .get('/api/flow/historical')
      .query({ from: FRIDAY_FROM, to: FRIDAY_TO, symbol: 'AAPL', chips: 'vol>oi' });

    expect(response.statusCode).toBe(200);
    expect(response.body.data.length).toBeGreaterThan(0);
    expect(response.body.data.every((row) => Array.isArray(row.chips) && row.chips.includes('vol>oi'))).toBe(true);
  });

  it('hydrates spot and oi from Theta endpoints when configured', async () => {
    const priorSpotPath = process.env.THETADATA_SPOT_PATH;
    const priorOiPath = process.env.THETADATA_OI_PATH;

    process.env.THETADATA_SPOT_PATH = '/v3/stock/snapshot/quote';
    process.env.THETADATA_OI_PATH = '/v3/option/history/open_interest';

    try {
      global.fetch = async (url) => {
        const endpoint = String(url);
        fetchCalls.push(endpoint);

        if (endpoint.includes('/v3/option/history/trade_quote')) {
          return {
            ok: true,
            status: 200,
            text: async () => JSON.stringify({
              symbol: ['QQQ'],
              trade_timestamp: ['2026-02-13T14:35:00.000Z'],
              expiration: ['2026-02-20'],
              strike: [500],
              right: ['CALL'],
              price: [2.5],
              size: [100],
              bid: [2.45],
              ask: [2.55],
              condition: [18],
              exchange: ['OPRA'],
            }),
          };
        }

        if (endpoint.includes('/v3/stock/snapshot/quote')) {
          return {
            ok: true,
            status: 200,
            text: async () => JSON.stringify({ symbol: 'QQQ', last: 490 }),
          };
        }

        if (endpoint.includes('/v3/option/history/open_interest')) {
          return {
            ok: true,
            status: 200,
            text: async () => JSON.stringify({ oi: 20 }),
          };
        }

        return {
          ok: false,
          status: 404,
          text: async () => JSON.stringify({ error: 'not_found' }),
        };
      };

      const response = await request(app)
        .get('/api/flow/historical')
        .query({ from: FRIDAY_FROM, to: FRIDAY_TO, symbol: 'QQQ', chips: 'otm,vol>oi' });

      expect(response.statusCode).toBe(200);
      expect(response.body.data).toHaveLength(1);
      expect(response.body.data[0]).toMatchObject({
        symbol: 'QQQ',
        spot: 490,
        oi: 20,
      });
      expect(response.body.data[0].chips).toEqual(expect.arrayContaining(['otm', 'vol>oi']));
      expect(fetchCalls.some((url) => url.includes('/v3/stock/snapshot/quote'))).toBe(true);
      expect(fetchCalls.some((url) => url.includes('/v3/option/history/open_interest'))).toBe(true);
    } finally {
      if (priorSpotPath === undefined) delete process.env.THETADATA_SPOT_PATH;
      else process.env.THETADATA_SPOT_PATH = priorSpotPath;

      if (priorOiPath === undefined) delete process.env.THETADATA_OI_PATH;
      else process.env.THETADATA_OI_PATH = priorOiPath;
    }
  });

  it('treats missing contracts from successful Theta bulk OI response as zero and marks volOi cache full', async () => {
    const priorOiPath = process.env.THETADATA_OI_PATH;
    process.env.THETADATA_OI_PATH = '/v3/option/history/open_interest';

    try {
      global.fetch = async (url) => {
        const endpoint = String(url);
        fetchCalls.push(endpoint);

        if (endpoint.includes('/v3/option/history/trade_quote')) {
          return {
            ok: true,
            status: 200,
            text: async () => JSON.stringify({
              symbol: ['NFLX', 'NFLX'],
              trade_timestamp: ['2026-02-13T14:35:00.000Z', '2026-02-13T14:36:00.000Z'],
              expiration: ['2026-02-20', '2026-02-27'],
              strike: [500, 505],
              right: ['CALL', 'CALL'],
              price: [2, 2],
              size: [100, 100],
              bid: [1.9, 1.9],
              ask: [2.1, 2.1],
              condition: [18, 18],
              exchange: ['OPRA', 'OPRA'],
            }),
          };
        }

        if (endpoint.includes('/v3/option/history/open_interest') && endpoint.includes('expiration=*')) {
          return {
            ok: true,
            status: 200,
            text: async () => JSON.stringify({
              symbol: ['NFLX'],
              expiration: ['2026-02-20'],
              strike: [500],
              right: ['CALL'],
              open_interest: [10],
              timestamp: ['2026-02-13T06:30:00'],
            }),
          };
        }

        return {
          ok: false,
          status: 404,
          text: async () => JSON.stringify({ error: 'not_found' }),
        };
      };

      const response = await request(app)
        .get('/api/flow/historical')
        .query({ from: FRIDAY_FROM, to: FRIDAY_TO, symbol: 'NFLX', chips: 'vol>oi' });

      expect(response.statusCode).toBe(200);
      expect(response.body.data.length).toBeGreaterThanOrEqual(2);

      const db = new Database(dbPath, { readonly: true });
      const volOiCache = db.prepare(`
        SELECT cache_status AS cacheStatus
        FROM option_trade_metric_day_cache
        WHERE symbol = ? AND trade_date_utc = ? AND metric_name = ?
      `).get('NFLX', '2026-02-13', 'volOiRatio');
      db.close();

      expect(volOiCache).toMatchObject({ cacheStatus: 'full' });
    } finally {
      if (priorOiPath === undefined) delete process.env.THETADATA_OI_PATH;
      else process.env.THETADATA_OI_PATH = priorOiPath;
    }
  });

  it('marks cache partial for explicit limit request, then upgrades to full on no-limit sync', async () => {
    global.fetch = async (url) => {
      fetchCalls.push(String(url));
      return {
        ok: true,
        status: 200,
        text: async () => JSON.stringify({
          symbol: ['TSLA'],
          trade_timestamp: ['2026-02-13T14:35:00.000Z'],
          expiration: ['2026-02-20'],
          strike: [360],
          right: ['CALL'],
          price: [1.11],
          size: [10],
          bid: [1.1],
          ask: [1.12],
          condition: [18],
          exchange: ['OPRA'],
        }),
      };
    };

    const limited = await request(app)
      .get('/api/flow/historical')
      .query({ from: FRIDAY_FROM, to: FRIDAY_TO, symbol: 'TSLA', limit: 1 });

    expect(limited.statusCode).toBe(200);
    expect(limited.body.meta.sync).toMatchObject({
      synced: true,
      cacheStatus: 'partial',
    });

    let db = new Database(dbPath, { readonly: true });
    const partialCache = db
      .prepare('SELECT cache_status AS cacheStatus FROM option_trade_day_cache WHERE symbol = ? AND trade_date_utc = ?')
      .get('TSLA', '2026-02-13');
    db.close();
    expect(partialCache).toMatchObject({ cacheStatus: 'partial' });

    const noLimit = await request(app)
      .get('/api/flow/historical')
      .query({ from: FRIDAY_FROM, to: FRIDAY_TO, symbol: 'TSLA' });

    expect(noLimit.statusCode).toBe(200);
    expect(noLimit.body.meta.sync).toMatchObject({
      synced: true,
      cacheStatus: 'full',
    });

    db = new Database(dbPath, { readonly: true });
    const fullCache = db
      .prepare('SELECT cache_status AS cacheStatus FROM option_trade_day_cache WHERE symbol = ? AND trade_date_utc = ?')
      .get('TSLA', '2026-02-13');
    db.close();
    expect(fullCache).toMatchObject({ cacheStatus: 'full' });
  });
});
