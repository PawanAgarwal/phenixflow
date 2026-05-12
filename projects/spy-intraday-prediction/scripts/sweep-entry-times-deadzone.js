#!/usr/bin/env node
// Sweep entry times on the dead-zone-biasprop variant to see if a different
// minute lifts train/test performance vs the baseline 11:30 ET entry.

const fs = require('node:fs');
const path = require('node:path');

const { PROJECT_ROOT } = require('../src/config');
const { loadPymHoldings, pymBias } = require('../src/pym-bias-strategy');
const zlib = require('node:zlib');
const readline = require('node:readline');

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
    transactionCostBps: 1,
    slippageBps: 1,
    source: { provider: 'in-memory', barsPath, scorePath },
  });
  const byDate = new Map();
  for (const snap of report.snapshots || []) {
    if (!snap.nextDate) continue;
    const holdingsMap = {};
    for (const h of snap.holdings || []) {
      if (h && h.ticker && Number.isFinite(h.weight)) holdingsMap[h.ticker] = h.weight;
    }
    byDate.set(snap.nextDate, { bias: pymBias(holdingsMap) });
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

async function runVariant({ pymByDate, days, entryMinuteEt, exitMinuteEt, biasGate, biasSkip, biasDenom, biasCap, costBps, label, includeOpenLong=false }) {
  const featuresCache = new Map();
  async function getFeatures(day) {
    if (!featuresCache.has(day)) featuresCache.set(day, await loadFeatures(day));
    return featuresCache.get(day);
  }
  const trades = [];
  for (const day of days) {
    const pe = pymByDate.get(day);
    if (!pe || !Number.isFinite(pe.bias)) continue;
    const ab = Math.abs(pe.bias);
    if (ab < biasGate) continue;
    if (biasSkip && ab >= biasSkip[0] && ab < biasSkip[1]) continue;
    const side = pe.bias > 0 ? 'LONG' : 'SHORT';
    const rows = await getFeatures(day);
    if (!rows || !rows.length) continue;
    const entry = rows.find(r => r.minute_of_day_et === entryMinuteEt);
    const exit = rows.find(r => r.minute_of_day_et === exitMinuteEt) || rows[rows.length-1];
    if (!entry || !Number.isFinite(entry.spy_open) || !Number.isFinite(exit.spy_close)) continue;
    const entryPrice = entry.spy_open;
    const exitPrice = exit.spy_close;
    const size = Math.min(ab / biasDenom, biasCap);
    const sign = side === 'LONG' ? 1 : -1;
    const gross = sign * size * (exitPrice / entryPrice - 1);
    const cost = (costBps / 10_000) * size;
    trades.push({ date: day, side, size, bias: pe.bias, gross, cost, net: gross - cost });
  }
  return { label, trades };
}

function isWeekend(d) {
  const x = new Date(d + 'T00:00:00.000Z').getUTCDay();
  return x === 0 || x === 6;
}

async function main() {
  process.stdout.write('Recomputing PYM in-memory...\n');
  const pymByDate = recomputePym();
  process.stdout.write(`PYM days: ${pymByDate.size}\n`);
  const days = [];
  const cur = new Date('2025-01-02T00:00:00.000Z');
  const stop = new Date('2026-04-27T00:00:00.000Z');
  while (cur <= stop) {
    const d = cur.toISOString().slice(0,10);
    if (!isWeekend(d) && pymByDate.has(d)) days.push(d);
    cur.setUTCDate(cur.getUTCDate()+1);
  }
  process.stdout.write(`Days in window: ${days.length}\n\n`);

  // Sweep entry minutes for dead-zone-biasprop config
  const config = { biasGate: 0.20, biasSkip: [0.30, 0.40], biasDenom: 0.30, biasCap: 1.5, exitMinuteEt: 955, costBps: 2 };
  const minutes = [570, 585, 600, 630, 660, 690, 720, 750, 780, 810, 840];
  const labels = { 570:'9:30', 585:'9:45', 600:'10:00', 630:'10:30', 660:'11:00', 690:'11:30', 720:'12:00', 750:'12:30', 780:'13:00', 810:'13:30', 840:'14:00' };

  process.stdout.write('Entry-time sweep on dead-zone-biasprop (skip 0.30-0.40, size = min(|bias|/0.30, 1.5)):\n');
  process.stdout.write(`  ${'entry'.padEnd(6)} ${'N'.padStart(4)}  ${'train net'.padStart(11)}  ${'train Sh'.padStart(9)} ${'train DD'.padStart(9)}  ${'test net'.padStart(11)}  ${'test Sh'.padStart(9)} ${'test DD'.padStart(9)} ${'test hit'.padStart(9)}\n`);
  const results = [];
  for (const m of minutes) {
    const { trades } = await runVariant({ pymByDate, days, entryMinuteEt: m, ...config });
    const tr = stats(trades.filter(t => t.date >= TR_S && t.date <= TR_E));
    const te = stats(trades.filter(t => t.date >= TE_S && t.date <= TE_E));
    results.push({ minute: m, label: labels[m], train: tr, test: te });
    process.stdout.write(`  ${labels[m].padEnd(6)} ${String(tr.n+te.n).padStart(4)}  ${(tr.net+'%').padStart(11)}  ${String(tr.sharpe).padStart(9)} ${(tr.dd+'%').padStart(9)}  ${(te.net+'%').padStart(11)}  ${String(te.sharpe).padStart(9)} ${(te.dd+'%').padStart(9)} ${(te.hit+'%').padStart(9)}\n`);
  }

  const out = path.join(PROJECT_ROOT, 'artifacts', 'deadzone-biasprop-entry-time-sweep.json');
  fs.writeFileSync(out, JSON.stringify({ generatedAt: new Date().toISOString(), config, results }, null, 2));
  process.stdout.write(`\nWritten ${out}\n`);
}

main().catch(err => { process.stderr.write(`${err.stack || err.message}\n`); process.exit(1); });
