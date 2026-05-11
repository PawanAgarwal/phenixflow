// Build per-minute ATM straddle mid prices for a given root.
// For each minute, pick the nearest-to-spot strike among 0/1 DTE contracts (separate buckets),
// take call + put close prices as the "straddle" price.
// Output: per-day JSONL.gz with [minute_ms, spot, dte_bucket, strike, call_price, put_price, straddle_price].

const fs = require('node:fs');
const path = require('node:path');
const zlib = require('node:zlib');
const readline = require('node:readline');

const { defaultOutputPath: defaultGreeksPath } = require('./build-greeks-1m');

async function loadGreeksDay(projectRoot, root, dayIso) {
  const p = defaultGreeksPath(projectRoot, root, dayIso);
  if (!fs.existsSync(p)) return [];
  const rows = [];
  const stream = fs.createReadStream(p).pipe(zlib.createGunzip());
  const rl = readline.createInterface({ input: stream, crlfDelay: Infinity });
  for await (const line of rl) {
    if (!line) continue;
    rows.push(JSON.parse(line));
  }
  return rows;
}

function buildAtmStraddleSeries(rows, dteBucket /* '0dte' | '1dte' */) {
  // Group rows by minute_ms then find the best ATM pairing within each minute.
  const byMinute = new Map();
  for (const r of rows) {
    if (r.dte == null) continue;
    const inBucket = dteBucket === '0dte' ? r.dte === 0 : r.dte === 1;
    if (!inBucket) continue;
    if (!Number.isFinite(r.price) || r.price <= 0) continue;
    const m = byMinute.get(r.minute_ms) || { calls: new Map(), puts: new Map(), spot: r.spot };
    const map = r.right === 'CALL' ? m.calls : m.puts;
    // Keep the latest record per strike
    map.set(r.strike, r);
    m.spot = r.spot;
    byMinute.set(r.minute_ms, m);
  }
  // For each minute, find the strike with both call+put present, closest to spot.
  const out = [];
  for (const [minuteMs, { calls, puts, spot }] of byMinute.entries()) {
    if (!Number.isFinite(spot)) continue;
    let bestStrike = null;
    let bestDist = Infinity;
    for (const strike of calls.keys()) {
      if (!puts.has(strike)) continue;
      const d = Math.abs(strike - spot);
      if (d < bestDist) { bestDist = d; bestStrike = strike; }
    }
    if (bestStrike === null) continue;
    const c = calls.get(bestStrike);
    const p = puts.get(bestStrike);
    out.push({
      minute_ms: minuteMs,
      date_et: c.date_et,
      minute_of_day_et: c.minute_of_day_et,
      dte_bucket: dteBucket,
      strike: bestStrike,
      spot,
      call_price: c.price,
      put_price: p.price,
      straddle_price: c.price + p.price,
      call_iv: c.iv,
      put_iv: p.iv,
      call_delta: c.delta,
      put_delta: p.delta,
      call_gamma: c.gamma,
      put_gamma: p.gamma,
      call_vega_per_1pct: c.vega_per_1pct,
      put_vega_per_1pct: p.vega_per_1pct,
      call_theta_per_day: c.theta_per_day,
      put_theta_per_day: p.theta_per_day,
    });
  }
  out.sort((a, b) => a.minute_ms - b.minute_ms);
  return out;
}

async function buildAtmStraddleForDay({ projectRoot, root, dayIso, outputPath }) {
  const rows = await loadGreeksDay(projectRoot, root, dayIso);
  if (rows.length === 0) throw new Error(`No greeks for ${root} ${dayIso}`);
  const series0 = buildAtmStraddleSeries(rows, '0dte');
  const series1 = buildAtmStraddleSeries(rows, '1dte');
  fs.mkdirSync(path.dirname(outputPath), { recursive: true });
  const gzip = zlib.createGzip();
  const file = fs.createWriteStream(outputPath);
  gzip.pipe(file);
  const done = new Promise((resolve, reject) => {
    file.on('finish', resolve);
    file.on('error', reject);
    gzip.on('error', reject);
  });
  for (const r of [...series0, ...series1]) {
    gzip.write(`${JSON.stringify(r)}\n`);
  }
  gzip.end();
  await done;
  return { rows0: series0.length, rows1: series1.length };
}

function defaultStraddlePath(projectRoot, root, dayIso) {
  return path.join(projectRoot, 'runtime', 'atm-straddle-1m', root, `date=${dayIso}`, `${dayIso}.jsonl.gz`);
}

async function loadStraddleDay(projectRoot, root, dayIso) {
  const p = defaultStraddlePath(projectRoot, root, dayIso);
  if (!fs.existsSync(p)) return new Map();
  const byMinute = new Map();
  const stream = fs.createReadStream(p).pipe(zlib.createGunzip());
  const rl = readline.createInterface({ input: stream, crlfDelay: Infinity });
  for await (const line of rl) {
    if (!line) continue;
    const r = JSON.parse(line);
    if (!byMinute.has(r.minute_ms)) byMinute.set(r.minute_ms, {});
    byMinute.get(r.minute_ms)[r.dte_bucket] = r;
  }
  return byMinute;
}

module.exports = {
  buildAtmStraddleForDay,
  defaultStraddlePath,
  loadStraddleDay,
};
