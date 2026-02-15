const request = require('supertest');
const { createApp } = require('../src/app');

describe('GET /health', () => {
  it('returns service health', async () => {
    const app = createApp();

    const response = await request(app).get('/health');

    expect(response.statusCode).toBe(200);
    expect(response.body).toEqual({ status: 'ok' });
  });
});

describe('GET /api/flow', () => {
  it('returns first page with default ordering and cursor metadata', async () => {
    const app = createApp();

    const response = await request(app).get('/api/flow?limit=3');

    expect(response.statusCode).toBe(200);
    expect(response.body.page).toMatchObject({ limit: 3, hasMore: true, sortBy: 'createdAt', sortOrder: 'desc', total: 10 });
    expect(response.body.page.nextCursor).toBeTruthy();
    expect(response.body.data.map((item) => item.id)).toEqual(['flow_001', 'flow_002', 'flow_003']);
  });

  it('supports full filter set and returns only matching records', async () => {
    const app = createApp();

    const response = await request(app)
      .get('/api/flow')
      .query({
        symbol: 'AAPL', strategy: 'breakout', status: 'open', timeframe: '1h',
        minPnl: '100', maxPnl: '130', minVolume: '1000', maxVolume: '1300',
        from: '2026-01-01T00:00:00.000Z', to: '2026-01-10T00:00:00.000Z', search: 'aapl',
      });

    expect(response.statusCode).toBe(200);
    expect(response.body.page.total).toBe(1);
    expect(response.body.data).toHaveLength(1);
    expect(response.body.data[0].id).toBe('flow_001');
  });

  it('paginates correctly with cursor and preserves ordering semantics', async () => {
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

  it('uses safe fallbacks for invalid query inputs', async () => {
    const app = createApp();

    const response = await request(app)
      .get('/api/flow')
      .query({ sortBy: 'invalid', sortOrder: 'invalid', limit: '0', cursor: '%%%not-base64%%%' });

    expect(response.statusCode).toBe(200);
    expect(response.body.page).toMatchObject({ sortBy: 'createdAt', sortOrder: 'desc', limit: 25, total: 10 });
  });
});
