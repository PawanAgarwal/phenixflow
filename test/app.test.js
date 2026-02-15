const request = require('supertest');
const { createApp } = require('../src/app');

describe('API contracts', () => {
  it('GET /health returns service health', async () => {
    const response = await request(createApp()).get('/health');
    expect(response.statusCode).toBe(200);
    expect(response.body).toEqual({ status: 'ok' });
  });

  it('GET /api/flow returns defaults with pagination metadata', async () => {
    const response = await request(createApp()).get('/api/flow?limit=3');
    expect(response.statusCode).toBe(200);
    expect(response.body.page).toEqual({
      limit: 3,
      hasMore: true,
      nextCursor: expect.any(String),
      sortBy: 'createdAt',
      sortOrder: 'desc',
      total: 10,
    });
    expect(response.body.data.map((d) => d.id)).toEqual(['flow_001', 'flow_002', 'flow_003']);
  });

  it('GET /api/flow supports full filters', async () => {
    const response = await request(createApp()).get('/api/flow').query({
      symbol: 'AAPL',
      strategy: 'breakout',
      status: 'open',
      timeframe: '1h',
      minPnl: '100',
      maxPnl: '130',
      minVolume: '1000',
      maxVolume: '1300',
      createdFrom: '2026-01-01T00:00:00.000Z',
      createdTo: '2026-01-10T00:00:00.000Z',
      updatedFrom: '2026-01-06T00:00:00.000Z',
      updatedTo: '2026-01-06T10:00:00.000Z',
      search: 'aapl',
    });

    expect(response.statusCode).toBe(200);
    expect(response.body.page.total).toBe(1);
    expect(response.body.data[0].id).toBe('flow_001');
  });

  it('GET /api/flow paginates by cursor and validates cursor query consistency', async () => {
    const first = await request(createApp()).get('/api/flow').query({ sortBy: 'pnl', sortOrder: 'desc', limit: '2' });
    expect(first.body.data.map((d) => d.id)).toEqual(['flow_010', 'flow_003']);

    const second = await request(createApp())
      .get('/api/flow')
      .query({ sortBy: 'pnl', sortOrder: 'desc', limit: '2', cursor: first.body.page.nextCursor });

    expect(second.statusCode).toBe(200);
    expect(second.body.data.map((d) => d.id)).toEqual(['flow_007', 'flow_001']);

    const mismatch = await request(createApp()).get('/api/flow').query({ sortBy: 'createdAt', cursor: first.body.page.nextCursor });
    expect(mismatch.statusCode).toBe(400);
    expect(mismatch.body).toEqual({ error: 'Cursor does not match current query.' });
  });

  it('GET /api/flow returns 400 for invalid query values', async () => {
    const response = await request(createApp()).get('/api/flow').query({ sortBy: 'invalid' });
    expect(response.statusCode).toBe(400);
  });

  it('GET /api/flow caps limit to 100', async () => {
    const response = await request(createApp()).get('/api/flow?limit=999');
    expect(response.statusCode).toBe(200);
    expect(response.body.page.limit).toBe(100);
    expect(response.body.data).toHaveLength(10);
  });
});
