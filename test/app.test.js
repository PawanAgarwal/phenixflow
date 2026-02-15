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
    expect(response.body.data[0]).toMatchObject({
      id: 'flow_001',
      isProfitable: true,
      pnlDirection: 'up',
      createdAtEpochMs: expect.any(Number),
      updatedAtEpochMs: expect.any(Number),
      ageHours: expect.any(Number),
    });
    expect(response.body.meta).toMatchObject({
      appliedQuickFilters: [],
      returnedCount: 3,
      cursorRequested: false,
      referenceNow: '2026-01-07T00:00:00.000Z',
    });
  });

  it('GET /api/flow supports canonical filters with aliases and quickFilters', async () => {
    const response = await request(createApp()).get('/api/flow').query({
      symbol: 'NVDA',
      minPnl: '100',
      volumeMax: '2100',
      from: '2025-12-20T00:00:00.000Z',
      createdTo: '2026-01-10T00:00:00.000Z',
      updatedFrom: '2026-01-06T00:00:00.000Z',
      quickFilters: 'openOnly,winners,recentlyUpdated',
      search: 'nvda',
      sortBy: 'updatedAt',
      sortOrder: 'desc',
    });

    expect(response.statusCode).toBe(200);
    expect(response.body.page.total).toBe(2);
    expect(response.body.data.map((row) => row.id)).toEqual(['flow_010', 'flow_003']);
    expect(response.body.meta.appliedQuickFilters).toEqual(['openOnly', 'winners', 'recentlyUpdated']);
    expect(response.body.meta.appliedFilters).toMatchObject({
      symbol: 'NVDA',
      minPnl: 100,
      maxVolume: 2100,
      createdFrom: '2025-12-20T00:00:00.000Z',
      createdTo: '2026-01-10T00:00:00.000Z',
    });
  });

  it('GET /api/flow paginates by cursor and validates cursor query consistency', async () => {
    const first = await request(createApp()).get('/api/flow').query({
      sortBy: 'pnl',
      sortOrder: 'desc',
      limit: '2',
      quickFilters: 'openOnly',
    });
    expect(first.body.data.map((d) => d.id)).toEqual(['flow_010', 'flow_003']);

    const second = await request(createApp())
      .get('/api/flow')
      .query({ sortBy: 'pnl', sortOrder: 'desc', limit: '2', quickFilters: 'openOnly', cursor: first.body.page.nextCursor });

    expect(second.statusCode).toBe(200);
    expect(second.body.data.map((d) => d.id)).toEqual(['flow_007', 'flow_001']);

    const mismatch = await request(createApp())
      .get('/api/flow')
      .query({ sortBy: 'pnl', sortOrder: 'desc', quickFilters: 'winners', cursor: first.body.page.nextCursor });
    expect(mismatch.statusCode).toBe(400);
    expect(mismatch.body).toEqual({ error: 'Cursor does not match current query.' });
  });

  it('GET /api/flow returns 400 for invalid query values', async () => {
    const badSort = await request(createApp()).get('/api/flow').query({ sortBy: 'invalid' });
    expect(badSort.statusCode).toBe(400);

    const badQuickFilter = await request(createApp()).get('/api/flow').query({ quickFilters: 'openOnly,unknownFilter' });
    expect(badQuickFilter.statusCode).toBe(400);

    const conflictingQuickFilters = await request(createApp()).get('/api/flow').query({ quickFilters: 'openOnly,closedOnly' });
    expect(conflictingQuickFilters.statusCode).toBe(400);
  });

  it('GET /api/flow caps limit to 100', async () => {
    const response = await request(createApp()).get('/api/flow?limit=999');
    expect(response.statusCode).toBe(200);
    expect(response.body.page.limit).toBe(100);
    expect(response.body.data).toHaveLength(10);
  });
});
