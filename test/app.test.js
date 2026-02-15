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

  it('GET /api/flow returns request/response contract defaults', async () => {
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

    expect(response.body.data).toHaveLength(3);
    expect(response.body.data.map((item) => item.id)).toEqual(['flow_001', 'flow_002', 'flow_003']);
    response.body.data.forEach(expectFlowRecordContract);
  });

  it('GET /api/flow supports query filter contracts and value typing', async () => {
    const app = createApp();

    const response = await request(app)
      .get('/api/flow')
      .query({
        symbol: 'AAPL',
        strategy: 'breakout',
        status: 'open',
        timeframe: '1h',
        minPnl: '100',
        maxPnl: '130',
        minVolume: '1000',
        maxVolume: '1300',
        from: '2026-01-01T00:00:00.000Z',
        to: '2026-01-10T00:00:00.000Z',
        search: 'aapl',
      });

    expect(response.statusCode).toBe(200);
    expect(response.body.page.total).toBe(1);
    expect(response.body.data).toHaveLength(1);
    expect(response.body.data[0].id).toBe('flow_001');
    expectFlowRecordContract(response.body.data[0]);
  });

  it('GET /api/flow paginates with cursor and preserves sort regression behavior', async () => {
    const app = createApp();

    const firstPage = await request(app).get('/api/flow').query({ sortBy: 'pnl', sortOrder: 'desc', limit: '2' });
    expect(firstPage.statusCode).toBe(200);
    expect(firstPage.body.data.map((item) => item.id)).toEqual(['flow_010', 'flow_003']);

    const secondPage = await request(app)
      .get('/api/flow')
      .query({ sortBy: 'pnl', sortOrder: 'desc', limit: '2', cursor: firstPage.body.page.nextCursor });

    expect(secondPage.statusCode).toBe(200);
    expect(secondPage.body.data.map((item) => item.id)).toEqual(['flow_007', 'flow_001']);
    expect(secondPage.body.page.hasMore).toBe(true);
  });

  it('GET /api/flow keeps stable success status for invalid query inputs', async () => {
    const app = createApp();

    const response = await request(app)
      .get('/api/flow')
      .query({ sortBy: 'invalid', sortOrder: 'invalid', limit: '0', cursor: '%%%not-base64%%%' });

    expect(response.statusCode).toBe(200);
    expect(response.body.page).toMatchObject({
      sortBy: 'createdAt',
      sortOrder: 'desc',
      limit: 25,
      total: 10,
    });
  });

  it('GET /api/flow caps limit to the max contract value (regression guard)', async () => {
    const app = createApp();

    const response = await request(app).get('/api/flow?limit=999');

    expect(response.statusCode).toBe(200);
    expect(response.body.page.limit).toBe(100);
    expect(response.body.data).toHaveLength(10);
  });

  it('POST /api/flow is rejected because endpoint contract is GET-only', async () => {
    const app = createApp();

    const response = await request(app).post('/api/flow').send({ symbol: 'AAPL' });

    expect(response.statusCode).toBe(404);
  });
});
