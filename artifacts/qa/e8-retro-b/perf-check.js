const request = require('supertest');
const { createApp } = require('../../../src/app');

(async () => {
  const app = createApp();
  const runs = 120;
  const warmup = 20;
  const samples = [];
  for (let i = 0; i < runs; i += 1) {
    const t0 = process.hrtime.bigint();
    const res = await request(app)
      .get('/api/flow')
      .query({ limit: 50, symbol: 'AAPL', status: 'open', timeframe: '1d' });
    const ms = Number(process.hrtime.bigint() - t0) / 1e6;
    if (res.statusCode !== 200) {
      console.error(`non-200 response on iter ${i}: ${res.statusCode}`);
      process.exit(2);
    }
    if (i >= warmup) samples.push(ms);
  }

  samples.sort((a, b) => a - b);
  const pct = (p) => samples[Math.min(samples.length - 1, Math.ceil((p / 100) * samples.length) - 1)];
  const summary = {
    runs,
    warmup,
    measured: samples.length,
    p50_ms: Number(pct(50).toFixed(2)),
    p95_ms: Number(pct(95).toFixed(2)),
    p99_ms: Number(pct(99).toFixed(2)),
    max_ms: Number(samples[samples.length - 1].toFixed(2)),
    target_p95_ms: 350,
    pass: pct(95) <= 350,
  };

  console.log(JSON.stringify(summary, null, 2));
})();
