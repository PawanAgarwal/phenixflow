const request = require('supertest');

const { baseUrlFrom, createApp } = require('../src/app');

function jsonResponse(body, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { 'content-type': 'application/json' },
  });
}

describe('studies-dashboard API', () => {
  it('normalizes the strategy service base URL', () => {
    expect(baseUrlFrom('http://strategy-service:3120///')).toBe('http://strategy-service:3120');
  });

  it('proxies strategy API requests to the configured service', async () => {
    const calls = [];
    const app = createApp({
      strategyApiUrl: 'http://strategy-service:3120/',
      fetchImpl: async (url, init) => {
        calls.push({ url, init });
        return jsonResponse({ data: [{ id: 'pym-v5' }] });
      },
    });

    const response = await request(app).get('/api/strategies?limit=10').expect(200);
    expect(response.body.data).toEqual([{ id: 'pym-v5' }]);
    expect(calls[0].url).toBe('http://strategy-service:3120/api/strategies?limit=10');
    expect(calls[0].init.method).toBe('GET');
  });

  it('reports upstream health through the dashboard health endpoint', async () => {
    const app = createApp({
      strategyApiUrl: 'http://strategy-service:3120',
      fetchImpl: async () => jsonResponse({ status: 'ok' }),
    });

    const response = await request(app).get('/api/health').expect(200);
    expect(response.body.service).toBe('phenixflow-studies-dashboard');
    expect(response.body.upstream).toEqual({ ok: true, status: 200 });
  });
});
