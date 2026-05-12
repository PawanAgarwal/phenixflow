#!/usr/bin/env node
// Sweep exit times on the top variants. Tests whether the 15:55 ET default exit
// is optimal or whether late-day mean reversion is eating returns.

const fs = require('node:fs');
const path = require('node:path');
const zlib = require('node:zlib');
const readline = require('node:readline');

const { PROJECT_ROOT } = require('../src/config');
const { pymBias } = require('../src/pym-bias-strategy');

const PYM_ROOT = path.resolve(__dirname, '..', '..', 'pym-v5-replication');
const pymConfig = require(path.join(PYM_ROOT, 'src', 'config')).loadConfig();
const { readDailyBarsJsonl } = require(path.join(PYM_ROOT, 'src', 'backtest'));
const { loadMassiveEnv } = require(path.join(PYM_ROOT, 'src', 'env'));
const { buildDailyRebalanceReport, defaultScorePath, findLatestMassiveEodBarsPath } = require(path.join(PYM_ROOT, 'src', 'rebalance-report'));

function defaultFeaturesPath(root, day) {
  return path.join(PROJECT_ROOT, 'runtime', 'features-1m', root, `date=${day}`, `${day}.jsonl.gz`);
}

async function loadFeatures(day) {
  const p = defaultFeaturesPath('SPY', day);
  if (!fs.existsSync(p)) return null;
  const stream = fs.createReadStream(p).pipe(zlib.createGunzip());
  const rl = readline.createInterface({ input: stream, crlfDelay: Infinity });
  const out = [];
  for await (const line of rl) { if (!line) continue; out.push(JSON.parse(line)); }
  return out;
}

function recomputePym() {
  loadMassiveEnv();
  const barsPath = findLatestMassiveEodBarsPath();
  const scorePath = defaultScorePath(pymConfig);
  const score = JSON.parse(fs.readFileSync(scorePath, 'utf8'));
  const market = readDailyBarsJsonl(barsPath);
  const report = buildDailyRebalanceReport({
    market, score, startDate: '2025-01-01', rsiMode: 'wilder',
    initialCapital: 10_000, transactionCostBps: 1, slippageBps: 1,
    source: { provider: 'in-memory', barsPath, scorePath },
  });
  const byDate = new Map();
  for (const snap of report.snapshots || []) {
    if (!snap.nextDate) continue;
    const h = {};
    for (const item of snap.holdings || []) {
      if (item?.ticker && Number.isFinite(item.weight)) h[item.ticker] = item.weight;
    }
    byDate.set(snap.nextDate, { bias: pymBias(h) });
  }
  return byDate;
}

const TR_S='2025-01-02', TR_E='2025-12-31', TE_S='2026-01-02', TE_E='2026-04-27';

function stats(trades) {
  if (!trades.length) return { n:0, net:0, sharpe:0, dd:0, hit:0 };
  const n = trades.length;
  const sum = trades.reduce((a,t)=>a+t.net,0);
  const wins = trades.filter(t=>t.net>0).length;
  const m = sum/n;
  const sd = Math.sqrt(trades.reduce((a,t)=>a+((t.net-m)**2),0)/n);
  let eq=1,peak=1,dd=0;
  for (const t of trades) { eq*=1+t.net; if(eq>peak)peak=eq; if(peak>0)dd=Math.min(dd,eq/peak-1); }
  return { n, net:+(sum*100).toFixed(2), sharpe:+(sd>0?(m/sd)*Math.sqrt(252):0).toFixed(2), dd:+(dd*100).toFixed(2), hit:+(wins/n*100).toFixed(1) };
}

function isWeekend(d) { const x = new Date(d+'T00:00:00.000Z').getUTCDay(); return x===0||x===6; }

async function sweep({ pymByDate, days, entryMinute, exitMinutes, biasGate, biasSkip }) {
  // Pre-load features
  const cache = new Map();
  for (const day of days) {
    const pe = pymByDate.get(day);
    if (!pe || !Number.isFinite(pe.bias)) continue;
    const ab = Math.abs(pe.bias);
    if (ab < biasGate) continue;
    if (biasSkip && ab >= biasSkip[0] && ab < biasSkip[1]) continue;
    const rows = await loadFeatures(day);
    if (rows) cache.set(day, rows);
  }
  const labels = { 870:'14:30', 900:'15:00', 915:'15:15', 930:'15:30', 945:'15:45', 955:'15:55' };
  const results = [];
  for (const exitMin of exitMinutes) {
    const trades = [];
    for (const [day, feats] of cache.entries()) {
      const pe = pymByDate.get(day);
      const entry = feats.find(r => r.minute_of_day_et === entryMinute);
      const exit = feats.find(r => r.minute_of_day_et === exitMin) || feats[feats.length-1];
      if (!entry || !Number.isFinite(entry.spy_open) || !Number.isFinite(exit.spy_close)) continue;
      const side = pe.bias > 0 ? 'LONG' : 'SHORT';
      const size = Math.min(Math.abs(pe.bias) / 0.30, 1.5);
      const sign = side === 'LONG' ? 1 : -1;
      const gross = sign * size * (exit.spy_close / entry.spy_open - 1);
      const cost = (2 / 10_000) * size;
      trades.push({ date: day, net: gross - cost });
    }
    const tr = stats(trades.filter(t => t.date >= TR_S && t.date <= TR_E));
    const te = stats(trades.filter(t => t.date >= TE_S && t.date <= TE_E));
    results.push({ exit: exitMin, label: labels[exitMin] || String(exitMin), train: tr, test: te });
  }
  return results;
}

async function main() {
  const pymByDate = recomputePym();
  const days = [];
  const cur = new Date('2025-01-02T00:00:00.000Z');
  const stop = new Date('2026-04-27T00:00:00.000Z');
  while (cur <= stop) {
    const d = cur.toISOString().slice(0,10);
    if (!isWeekend(d) && pymByDate.has(d)) days.push(d);
    cur.setUTCDate(cur.getUTCDate()+1);
  }
  const exits = [870, 900, 915, 930, 945, 955];
  const labels = { 870:'14:30', 900:'15:00', 915:'15:15', 930:'15:30', 945:'15:45', 955:'15:55' };

  process.stdout.write('\n== dead-zone-biasprop (entry 11:30) ==\n');
  process.stdout.write(`  ${'exit'.padEnd(6)} ${'train N'.padStart(7)} ${'train net'.padStart(11)} ${'train Sh'.padStart(9)}  ${'test N'.padStart(7)} ${'test net'.padStart(11)} ${'test Sh'.padStart(9)} ${'test DD'.padStart(9)}\n`);
  const r1 = await sweep({ pymByDate, days, entryMinute: 690, exitMinutes: exits, biasGate: 0.20, biasSkip: [0.30, 0.40] });
  for (const r of r1) {
    process.stdout.write(`  ${r.label.padEnd(6)} ${String(r.train.n).padStart(7)} ${(r.train.net+'%').padStart(11)} ${String(r.train.sharpe).padStart(9)}  ${String(r.test.n).padStart(7)} ${(r.test.net+'%').padStart(11)} ${String(r.test.sharpe).padStart(9)} ${(r.test.dd+'%').padStart(9)}\n`);
  }

  process.stdout.write('\n== high-only (|bias| >= 0.40, entry 11:30) ==\n');
  process.stdout.write(`  ${'exit'.padEnd(6)} ${'train N'.padStart(7)} ${'train net'.padStart(11)} ${'train Sh'.padStart(9)}  ${'test N'.padStart(7)} ${'test net'.padStart(11)} ${'test Sh'.padStart(9)} ${'test DD'.padStart(9)}\n`);
  const r2 = await sweep({ pymByDate, days, entryMinute: 690, exitMinutes: exits, biasGate: 0.40, biasSkip: null });
  for (const r of r2) {
    process.stdout.write(`  ${r.label.padEnd(6)} ${String(r.train.n).padStart(7)} ${(r.train.net+'%').padStart(11)} ${String(r.train.sharpe).padStart(9)}  ${String(r.test.n).padStart(7)} ${(r.test.net+'%').padStart(11)} ${String(r.test.sharpe).padStart(9)} ${(r.test.dd+'%').padStart(9)}\n`);
  }

  process.stdout.write('\n== baseline (1x flat, |bias| >= 0.20, entry 11:30) ==\n');
  process.stdout.write(`  ${'exit'.padEnd(6)} ${'train N'.padStart(7)} ${'train net'.padStart(11)} ${'train Sh'.padStart(9)}  ${'test N'.padStart(7)} ${'test net'.padStart(11)} ${'test Sh'.padStart(9)} ${'test DD'.padStart(9)}\n`);
  // baseline 1x: no biasprop sizing → just override size to 1
  const cache = new Map();
  for (const day of days) {
    const pe = pymByDate.get(day);
    if (!pe || !Number.isFinite(pe.bias) || Math.abs(pe.bias) < 0.20) continue;
    const rows = await loadFeatures(day);
    if (rows) cache.set(day, rows);
  }
  for (const exitMin of exits) {
    const trades = [];
    for (const [day, feats] of cache.entries()) {
      const pe = pymByDate.get(day);
      const entry = feats.find(r => r.minute_of_day_et === 690);
      const exit = feats.find(r => r.minute_of_day_et === exitMin) || feats[feats.length-1];
      if (!entry || !Number.isFinite(entry.spy_open) || !Number.isFinite(exit.spy_close)) continue;
      const sign = pe.bias > 0 ? 1 : -1;
      const gross = sign * (exit.spy_close / entry.spy_open - 1);
      const cost = 2 / 10_000;
      trades.push({ date: day, net: gross - cost });
    }
    const tr = stats(trades.filter(t => t.date >= TR_S && t.date <= TR_E));
    const te = stats(trades.filter(t => t.date >= TE_S && t.date <= TE_E));
    const label = labels[exitMin] || String(exitMin);
    process.stdout.write(`  ${label.padEnd(6)} ${String(tr.n).padStart(7)} ${(tr.net+'%').padStart(11)} ${String(tr.sharpe).padStart(9)}  ${String(te.n).padStart(7)} ${(te.net+'%').padStart(11)} ${String(te.sharpe).padStart(9)} ${(te.dd+'%').padStart(9)}\n`);
  }

  const out = path.join(PROJECT_ROOT, 'artifacts', 'exit-time-sweep.json');
  fs.writeFileSync(out, JSON.stringify({ generatedAt: new Date().toISOString(), deadzone_biasprop: r1, high_only: r2 }, null, 2));
  process.stdout.write(`\nWritten ${out}\n`);
}

main().catch(err => { process.stderr.write(`${err.stack || err.message}\n`); process.exit(1); });
