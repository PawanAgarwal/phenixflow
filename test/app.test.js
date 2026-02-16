const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const Database = require('better-sqlite3');
const request = require('supertest');
const { createApp } = require('../src/app');

function expectFlowRecordContract(record) {
  expect(record).toEqual({
    id: expect.any(String),
    symbol: expect.any(String),
    strategy: expect.any(String),
    status: expect.any(String),
    timeframe: expect.any(String),
    pnl: expect.any(Number),
    volume: expect.any(Number),
    createdAt: expect.any(String),
    updatedAt: expect.any(String),
  });
}

async function withEnv(overrides, callback) {
  const previous = {};
  Object.keys(overrides).forEach((key) => {
    previous[key] = process.env[key];
    const nextValue = overrides[key];
    if (nextValue === undefined || nextValue === null) {
      delete process.env[key];
    } else {
      process.env[key] = String(nextValue);
    }
  });

  try {
    return await callback();
  } finally {
    Object.entries(previous).forEach(([key, value]) => {
      if (value === undefined) delete process.env[key];
      else process.env[key] = value;
    });
  }
}

function createReadyTestDb({ backlogCount = 0 } = {}) {
  const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'ready-db-'));
  const dbPath = path.join(tempDir, 'ready.sqlite');
  const db = new Database(dbPath);

  db.exec(`
    CREATE TABLE option_trades (
      trade_id TEXT PRIMARY KEY
    );
    CREATE TABLE option_trade_enriched (
      trade_id TEXT PRIMARY KEY
    );
  `);

  const insertTrade = db.prepare('INSERT INTO option_trades (trade_id) VALUES (?)');
  const insertEnriched = db.prepare('INSERT INTO option_trade_enriched (trade_id) VALUES (?)');

  insertTrade.run('trade_covered');
  insertEnriched.run('trade_covered');

  for (let index = 0; index < backlogCount; index += 1) {
    insertTrade.run(`trade_backlog_${index}`);
  }

  db.close();

  return {
    dbPath,
    cleanup: () => fs.rmSync(tempDir, { recursive: true, force: true }),
  };
}

describe('API contracts', () => {
  it('GET /health returns the expected status contract', async () => {
    const app = createApp();

    const response = await request(app).get('/health');

    expect(response.statusCode).toBe(200);
    expect(response.headers['content-type']).toContain('application/json');
    expect(response.body).toEqual({ status: 'ok' });
  });

  it('GET /ready returns not_ready when ThetaData is not configured', async () => {
    const app = createApp();
    const readyDb = createReadyTestDb();

    try {
      await withEnv({
        PHENIX_DB_PATH: readyDb.dbPath,
        THETADATA_BASE_URL: undefined,
      }, async () => {
        const response = await request(app).get('/ready');

        expect(response.statusCode).toBe(503);
        expect(response.body).toEqual({
          status: 'not_ready',
          checks: {
            db: 'ok',
            thetadata: 'fail',
            enrichmentBacklog: 'ok',
          },
          reason: 'thetadata_not_configured',
        });
      });
    } finally {
      readyDb.cleanup();
    }
  });

  it('GET /ready returns ready when db/thetadata/backlog checks pass', async () => {
    const app = createApp();
    const readyDb = createReadyTestDb();
    const originalFetch = global.fetch;

    global.fetch = vi.fn(async () => ({ ok: true, status: 200 }));

    try {
      await withEnv({
        PHENIX_DB_PATH: readyDb.dbPath,
        THETADATA_BASE_URL: 'http://127.0.0.1:25503',
        THETADATA_HEALTH_PATH: '/',
        PHENIX_ENRICHMENT_BACKLOG_MAX: '0',
      }, async () => {
        const response = await request(app).get('/ready');

        expect(response.statusCode).toBe(200);
        expect(response.body).toEqual({
          status: 'ready',
          checks: {
            db: 'ok',
            thetadata: 'ok',
            enrichmentBacklog: 'ok',
          },
          version: '0.1.0',
        });
      });
    } finally {
      global.fetch = originalFetch;
      readyDb.cleanup();
    }
  });

  it('GET /api/flow returns pagination contract and data schema', async () => {
    const app = createApp();

    const response = await request(app).get('/api/flow?limit=3');

    expect(response.statusCode).toBe(200);
    expect(response.headers['content-type']).toContain('application/json');
    expect(response.body.page).toEqual({
      limit: 3,
      hasMore: true,
      nextCursor: expect.any(String),
      sortBy: 'createdAt',
      sortOrder: 'desc',
      total: 10,
    });
    expect(response.body.meta).toMatchObject({ filterVersion: 'legacy' });
    expect(response.body.data).toHaveLength(3);
    response.body.data.forEach(expectFlowRecordContract);
  });

  it('GET /api/flow/facets returns facets schema contract', async () => {
    const app = createApp();

    const response = await request(app).get('/api/flow/facets').query({ status: 'open' });

    expect(response.statusCode).toBe(200);
    expect(response.headers['content-type']).toContain('application/json');
    expect(response.body).toEqual({
      facets: {
        symbol: {
          AAPL: 1,
          NVDA: 2,
          AMZN: 1,
          META: 1,
        },
        status: {
          open: 5,
        },
      },
      total: 5,
      meta: { filterVersion: 'legacy' },
    });
  });

  it('GET /api/flow/summary returns aggregate totals, ratios, and top symbols', async () => {
    const app = createApp();
    const artifactDir = fs.mkdtempSync(path.join(os.tmpdir(), 'flow-summary-'));
    const artifactPath = path.join(artifactDir, 'flow-read.json');

    fs.writeFileSync(artifactPath, JSON.stringify({
      rows: [
        {
          id: 'sum_1',
          symbol: 'AAPL',
          strategy: 'breakout',
          status: 'open',
          timeframe: '1m',
          pnl: 1,
          volume: 10,
          createdAt: '2026-02-15T16:00:00.000Z',
          updatedAt: '2026-02-15T16:00:01.000Z',
          right: 'CALL',
          price: 5,
          size: 300,
          bid: 4.9,
          ask: 5,
          dayVolume: 300,
          oi: 100,
          sigScore: 0.95,
        },
        {
          id: 'sum_2',
          symbol: 'TSLA',
          strategy: 'breakout',
          status: 'open',
          timeframe: '1m',
          pnl: 2,
          volume: 20,
          createdAt: '2026-02-15T16:00:02.000Z',
          updatedAt: '2026-02-15T16:00:03.000Z',
          right: 'PUT',
          price: 5,
          size: 100,
          bid: 4.9,
          ask: 5,
          dayVolume: 100,
          oi: 100,
          sigScore: 0.5,
        },
        {
          id: 'sum_3',
          symbol: 'AAPL',
          strategy: 'mean-reversion',
          status: 'open',
          timeframe: '1m',
          pnl: 3,
          volume: 20,
          createdAt: '2026-02-15T16:00:04.000Z',
          updatedAt: '2026-02-15T16:00:05.000Z',
          price: 2,
          size: 100,
          bid: 1.9,
          ask: 2.1,
          dayVolume: 20,
          oi: 100,
          sigScore: 0.1,
        },
      ],
    }), 'utf8');

    try {
      const response = await request(app).get('/api/flow/summary').query({
        source: 'real-ingest',
        artifactPath,
        topSymbolsLimit: 1,
      });

      expect(response.statusCode).toBe(200);
      expect(response.headers['content-type']).toContain('application/json');
      expect(response.body).toEqual({
        data: {
          totals: {
            rows: 3,
            contracts: 500,
            premium: 220000,
            bullish: 1,
            bearish: 1,
            neutral: 1,
          },
          ratios: {
            bullishRatio: 1 / 3,
            highSigRatio: 1 / 3,
            unusualRatio: 1 / 3,
          },
          topSymbols: [
            { symbol: 'AAPL', rows: 2, premium: 170000 },
          ],
        },
        meta: {
          filterVersion: 'legacy',
          ruleVersion: 'historical-v1',
          observability: {
            source: 'real-ingest',
            artifactPath: path.resolve(artifactPath),
            rowCount: 3,
            fallbackReason: null,
          },
        },
      });
    } finally {
      fs.rmSync(artifactDir, { recursive: true, force: true });
    }
  });

  it('GET /api/flow/filters/catalog returns chip dictionary and thresholds', async () => {
    const app = createApp();

    const response = await request(app).get('/api/flow/filters/catalog');

    expect(response.statusCode).toBe(200);
    expect(response.headers['content-type']).toContain('application/json');
    expect(response.body.meta).toEqual({ filterVersion: 'legacy' });
    expect(response.body.data.ruleVersion).toBe('historical-v1');
    expect(response.body.data.thresholds).toMatchObject({
      premium100kMin: 100000,
      premiumSizableMin: 250000,
      premiumWhalesMin: 500000,
      sizeLargeMin: 1000,
      repeatFlowMin: 20,
      highSigMin: 0.9,
    });

    const chipIds = response.body.data.chips.map((chip) => chip.id);
    expect(chipIds).toContain('calls');
    expect(chipIds).toContain('sweeps');
    expect(chipIds).toContain('high-sig');
    expect(response.body.data.enums).toEqual({
      right: ['CALL', 'PUT'],
      sentiment: ['bullish', 'bearish', 'neutral'],
      side: ['BID', 'ASK', 'AA', 'OTHER'],
    });
  });

  it('GET /api/flow/stream returns stream event contract with pagination', async () => {
    const app = createApp();

    const response = await request(app).get('/api/flow/stream?limit=2&sortBy=pnl&sortOrder=desc');

    expect(response.statusCode).toBe(200);
    expect(response.headers['content-type']).toContain('application/json');
    expect(response.body.page).toMatchObject({
      limit: 2,
      hasMore: true,
      nextCursor: expect.any(String),
      sortBy: 'pnl',
      sortOrder: 'desc',
      total: 10,
    });
    expect(response.body.meta).toMatchObject({ filterVersion: 'legacy' });
    expect(response.body.data).toHaveLength(2);
    response.body.data.forEach((event) => {
      expect(event).toEqual({
        sequence: expect.any(Number),
        eventType: 'flow.updated',
        flow: expect.any(Object),
      });
      expectFlowRecordContract(event.flow);
    });
  });

  it('GET /api/flow/:id returns detail contract and handles not found', async () => {
    const app = createApp();

    const detail = await request(app).get('/api/flow/flow_003');
    expect(detail.statusCode).toBe(200);
    expect(detail.body).toEqual({
      data: {
        id: 'flow_003',
        symbol: 'NVDA',
        strategy: 'trend-following',
        status: 'open',
        timeframe: '1d',
        pnl: 300,
        volume: 2100,
        createdAt: '2026-01-03T10:30:00.000Z',
        updatedAt: '2026-01-06T11:20:00.000Z',
      },
    });

    const missing = await request(app).get('/api/flow/flow_999');
    expect(missing.statusCode).toBe(404);
    expect(missing.body).toEqual({ error: { code: 'not_found', message: 'Flow not found' } });
  });

  it('backward-compat v1 paths return the same contracts as current paths', async () => {
    const app = createApp();

    const [v2List, v1List] = await Promise.all([
      request(app).get('/api/flow').query({ symbol: 'AAPL', limit: 2 }),
      request(app).get('/api/v1/flow').query({ symbol: 'AAPL', limit: 2 }),
    ]);

    expect(v1List.statusCode).toBe(200);
    expect(v1List.body).toEqual(v2List.body);

    const [v2Facets, v1Facets] = await Promise.all([
      request(app).get('/api/flow/facets').query({ status: 'closed' }),
      request(app).get('/api/v1/flow/facets').query({ status: 'closed' }),
    ]);

    expect(v1Facets.statusCode).toBe(200);
    expect(v1Facets.body).toEqual(v2Facets.body);

    const [v2Summary, v1Summary] = await Promise.all([
      request(app).get('/api/flow/summary').query({ status: 'closed' }),
      request(app).get('/api/v1/flow/summary').query({ status: 'closed' }),
    ]);

    expect(v1Summary.statusCode).toBe(200);
    expect(v1Summary.body).toEqual(v2Summary.body);

    const [v2Catalog, v1Catalog] = await Promise.all([
      request(app).get('/api/flow/filters/catalog'),
      request(app).get('/api/v1/flow/filters/catalog'),
    ]);

    expect(v1Catalog.statusCode).toBe(200);
    expect(v1Catalog.body).toEqual(v2Catalog.body);

    const [v2Stream, v1Stream] = await Promise.all([
      request(app).get('/api/flow/stream').query({ limit: 2 }),
      request(app).get('/api/v1/flow/stream').query({ limit: 2 }),
    ]);

    expect(v1Stream.statusCode).toBe(200);
    expect(v1Stream.body).toEqual(v2Stream.body);

    const [v2Detail, v1Detail] = await Promise.all([
      request(app).get('/api/flow/flow_001'),
      request(app).get('/api/v1/flow/flow_001'),
    ]);

    expect(v1Detail.statusCode).toBe(200);
    expect(v1Detail.body).toEqual(v2Detail.body);
  });

  it('creates new presets/alerts with query DSL v2 payloads', async () => {
    const app = createApp();

    const presetResponse = await request(app)
      .post('/api/flow/presets')
      .send({
        name: 'Open AAPL',
        payload: {
          symbol: 'AAPL',
          status: 'open',
        },
      });

    expect(presetResponse.statusCode).toBe(201);
    expect(presetResponse.body.data.payloadVersion).toBe('v2');
    expect(presetResponse.body.data.payload).toEqual({
      version: 2,
      combinator: 'and',
      clauses: [
        { field: 'symbol', op: 'eq', value: 'AAPL' },
        { field: 'status', op: 'eq', value: 'open' },
      ],
    });

    const alertResponse = await request(app)
      .post('/api/flow/alerts')
      .send({
        name: 'High volume',
        payload: {
          version: 2,
          combinator: 'and',
          clauses: [{ field: 'volume', op: 'gte', value: 1000 }],
        },
      });

    expect(alertResponse.statusCode).toBe(201);
    expect(alertResponse.body.data.payloadVersion).toBe('v2');
    expect(alertResponse.body.data.payload).toEqual({
      version: 2,
      combinator: 'and',
      clauses: [{ field: 'volume', op: 'gte', value: 1000 }],
    });
  });

  it('loads saved presets in legacy format through compatibility mode', async () => {
    const app = createApp();

    const createResponse = await request(app)
      .post('/api/flow/presets')
      .send({ name: 'Legacy compat', payload: { symbol: 'TSLA', minPnl: 5 } });

    const id = createResponse.body.data.id;

    const v2Read = await request(app).get(`/api/flow/presets/${id}`);
    expect(v2Read.statusCode).toBe(200);
    expect(v2Read.body.data.payloadVersion).toBe('v2');
    expect(v2Read.body.data.payload).toEqual({
      version: 2,
      combinator: 'and',
      clauses: [
        { field: 'symbol', op: 'eq', value: 'TSLA' },
        { field: 'pnl', op: 'gte', value: 5 },
      ],
    });

    const v1Read = await request(app).get(`/api/v1/flow/presets/${id}`);
    expect(v1Read.statusCode).toBe(200);
    expect(v1Read.body.data.payloadVersion).toBe('legacy');
    expect(v1Read.body.data.payload).toEqual({ symbol: 'TSLA', minPnl: 5 });
  });

  it('POST /api/flow is rejected because endpoint contract is GET-only', async () => {
    const app = createApp();

    const response = await request(app).post('/api/flow').send({ symbol: 'AAPL' });

    expect(response.statusCode).toBe(404);
  });
});
