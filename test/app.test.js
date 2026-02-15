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

describe('API contracts', () => {
  it('GET /health returns the expected status contract', async () => {
    const app = createApp();

    const response = await request(app).get('/health');

    expect(response.statusCode).toBe(200);
    expect(response.headers['content-type']).toContain('application/json');
    expect(response.body).toEqual({ status: 'ok' });
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
    expect(response.body.meta).toEqual({ filterVersion: 'legacy' });
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
    expect(response.body.meta).toEqual({ filterVersion: 'legacy' });
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

  it('POST /api/flow is rejected because endpoint contract is GET-only', async () => {
    const app = createApp();

    const response = await request(app).post('/api/flow').send({ symbol: 'AAPL' });

    expect(response.statusCode).toBe(404);
  });
});
