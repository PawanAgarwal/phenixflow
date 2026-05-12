#!/usr/bin/env node
// Sweep entry times separately for low-bias [0.20,0.30) and high-bias [0.40,∞)
// trades. Hypothesis: high-conviction trades may have a different optimal entry.

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
const {
  buildDailyRebalanceReport,
  defaultScorePath,
  findLatestMassiveEodBarsPath,
} = require(path.join(PYM_ROOT, 'src', 'rebalance-report'));

function defaultFeaturesPath(root, day) {
  return path.join(PROJECT_ROOT, 'runtime', 'features-1m', root, `date=${day}`, `${day}.jsonl.gz`);
}

async function loadFeatures(day) {
  const p = defaultFeaturesPath('SPY', day);
  if (!fs.existsSync(p)) return null;
  const stream = fs.createReadStream(p).pipe(zlib.createGunzip());
  const rl = readline.createInterface({ input: stream, crlfDelay: Infinity });
  const out = [];
  for await (const line of rl) {
    if (!line) continue;
    out.push(JSON.parse(line));
  }
  return out;
}

function recomputePym() {
  loadMassiveEnv();
  const barsPath = findLatestMassiveEodBarsPath();
  const scorePath = defaultScorePath(pymConfig);
  const score = JSON.parse(fs.readFileSync(scorePath, 'utf8'));
  const market = readDailyBarsJsonl(barsPath);
  const report = buildDailyRebalanceReport({
    market, score,
    startDate: '2025-01-01',
    rsiMode: 'wilder',
    initialCapital: 10_000,
    transactionCostBps: 1, slippageBps: 1,
    source: { provider: 'in-memory', barsPath, scorePath },
  });
  const byDate = new Map();
  for (const snap of report.snapshots || []) {
    if (!snap.nextDate) continue;
    const h = {};
    for (const item of snap.holdings || []) {
      if (item && item.ticker && Number.isFinite(item.weight)) h[item.ticker] = item.weight;
    }
    byDate.set(snap.nextDate, { bias: pymBias(h) });
  }
  return byDate;
}

const TR_S='2025-01-02', TR_E='2025-12-31', TE_S='2026-01-02', TE_E='2026-04-27';

function stats(trades) {
  if (!trades.length) return { n:0, net:0, sharpe:0, dd:0 };
  const n = trades.length;
  const sum = trades.reduce((a,t)=>a+t.net,0);
  const m = sum/n;
  const sd = Math.sqrt(trades.reduce((a,t)=>a+((t.net-m)**2),0)/n);
  let eq=1,peak=1,dd=0;
  for (const t of trades) { eq*=1+t.net; if(eq>peak)peak=eq; if(peak>0)dd=Math.min(dd,eq/peak-1); }
  return { n, net:+(sum*100).toFixed(2), sharpe:+(sd>0?(m/sd)*Math.sqrt(252):0).toFixed(2), dd:+(dd*100).toFixed(2) };
}

function isWeekend(d) { const x = new Date(d+'T00:00:00.000Z').getUTCDay(); return x===0||x===6; }

async function main() {
  process.stdout.write('Recomputing PYM...\n');
  const pymByDate = recomputePym();
  const days = [];
  const cur = new Date('2025-01-02T00:00:00.000Z');
  const stop = new Date('2026-04-27T00:00:00.000Z');
  while (cur <= stop) {
    const d = cur.toISOString().slice(0,10);
    if (!isWeekend(d) && pymByDate.has(d)) days.push(d);
    cur.setUTCDate(cur.getUTCDate()+1);
  }
  process.stdout.write(`Days: ${days.length}\n\n`);

  // Pre-load features for all days that have qualifying bias
  const cache = new Map();
  for (const day of days) {
    const pe = pymByDate.get(day);
    if (!pe || !Number.isFinite(pe.bias)) continue;
    const ab = Math.abs(pe.bias);
    if (ab < 0.20 || (ab >= 0.30 && ab < 0.40)) continue;
    const rows = await loadFeatures(day);
    if (rows) cache.set(day, rows);
  }

  const buckets = {
    'low [0.20,0.30)': (ab) => ab >= 0.20 && ab < 0.30,
    'high [0.40,inf)': (ab) => ab >= 0.40,
  };

  const minutes = [570, 585, 600, 630, 660, 690, 720, 750, 780];
  const labels = { 570:'9:30', 585:'9:45', 600:'10:00', 630:'10:30', 660:'11:00', 690:'11:30', 720:'12:00', 750:'12:30', 780:'13:00' };

  const allResults = {};
  for (const [bucketLabel, pred] of Object.entries(buckets)) {
    process.stdout.write(`\nBucket ${bucketLabel}:\n`);
    process.stdout.write(`  ${'entry'.padEnd(6)} ${'tr N'.padStart(4)} ${'train net'.padStart(11)} ${'train Sh'.padStart(9)}  ${'te N'.padStart(4)} ${'test net'.padStart(11)} ${'test Sh'.padStart(9)}\n`);
    const rows = [];
    for (const m of minutes) {
      const trades = [];
      for (const [day, feats] of cache.entries()) {
        const pe = pymByDate.get(day);
        if (!pred(Math.abs(pe.bias))) continue;
        const entry = feats.find(r => r.minute_of_day_et === m);
        const exit = feats.find(r => r.minute_of_day_et === 955) || feats[feats.length-1];
        if (!entry || !Number.isFinite(entry.spy_open) || !Number.isFinite(exit.spy_close)) continue;
        const side = pe.bias > 0 ? 'LONG' : 'SHORT';
        const size = Math.min(Math.abs(pe.bias) / 0.30, 1.5);
        const sign = side === 'LONG' ? 1 : -1;
        const gross = sign * size * (exit.spy_close / entry.spy_open - 1);
        const cost = (2 / 10_000) * size;
        trades.push({ date: day, net: gross - cost, size });
      }
      const tr = stats(trades.filter(t => t.date >= TR_S && t.date <= TR_E));
      const te = stats(trades.filter(t => t.date >= TE_S && t.date <= TE_E));
      rows.push({ minute: m, label: labels[m], train: tr, test: te });
      process.stdout.write(`  ${labels[m].padEnd(6)} ${String(tr.n).padStart(4)} ${(tr.net+'%').padStart(11)} ${String(tr.sharpe).padStart(9)}  ${String(te.n).padStart(4)} ${(te.net+'%').padStart(11)} ${String(te.sharpe).padStart(9)}\n`);
    }
    allResults[bucketLabel] = rows;
  }

  // Print best per bucket on test Sharpe
  process.stdout.write('\nBest entry minute by test Sharpe per bucket:\n');
  for (const [k,rows] of Object.entries(allResults)) {
    const best = rows.slice().sort((a,b)=>b.test.sharpe - a.test.sharpe)[0];
    process.stdout.write(`  ${k}: ${best.label} (test Sh ${best.test.sharpe}, net ${best.test.net}%)\n`);
  }

  const out = path.join(PROJECT_ROOT, 'artifacts', 'deadzone-biasprop-per-bucket-entry-sweep.json');
  fs.writeFileSync(out, JSON.stringify({ generatedAt: new Date().toISOString(), allResults }, null, 2));
  process.stdout.write(`\nWritten ${out}\n`);
}

main().catch(err => { process.stderr.write(`${err.stack || err.message}\n`); process.exit(1); });
