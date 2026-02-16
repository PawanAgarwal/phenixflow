const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const request = require('supertest');
const { createApp } = require('../../../src/app');

(async () => {
  const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'qa-hist-repro-'));
  process.env.PHENIX_DB_PATH = path.join(tempDir, 'historical.sqlite');
  process.env.THETADATA_BASE_URL = 'http://thetadata.local:25503';

  global.fetch = async () => ({
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
  });

  const app = createApp();
  const res = await request(app)
    .get('/api/flow/historical')
    .query({ from: '2026-02-13T00:00:00.000Z', to: '2026-02-13T23:59:59.999Z', symbol: 'AAPL' });

  console.log(JSON.stringify({ status: res.statusCode, body: res.body }, null, 2));
})();
