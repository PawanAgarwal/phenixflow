#!/usr/bin/env node
// Build a daily options-stress signal artifact from:
//   1. VIXY 5d log-return  (always available, derived from local daily bars)
//   2. ^VIX daily close     (Massive REST, available 2023-02-14 onward)
//   3. OCC equity put/call ratio  (local OCC files, available 2021-01-04 onward)
//
// Output JSONL rows: { date, vixyLogRet5, vixClose, equityPCRatio,
//                      vixyZ60, vixZ60, occZ60, stress }
// Where `stress` is the mean of available z-scores per day. Only daily VIX
// data is fetched fresh on each run; everything else is read from local
// runtime data so re-runs are fast.
//
// Usage:
//   node projects/pym-v5-ml-experiments/scripts/build-options-stress-signal.js \
//     [--start 2016-01-01] [--end 2026-05-08] [--out path/to/file.jsonl] \
//     [--bars path/to/daily-bars.jsonl]

const fs = require('node:fs');
const path = require('node:path');

const { ensureDir, runtimePath } = require('../../pym-v5-replication/src/config');
const { loadMassiveEnv } = require('../../pym-v5-replication/src/env');

const OCC_ROOT = '/Volumes/SEC4TB/massive-data/occ/option_open_interest_eod';
const DEFAULT_START = '2016-01-01';
const DEFAULT_END = new Date().toISOString().slice(0, 10);
const DEFAULT_OUT_DIR = path.resolve(__dirname, '..', 'artifacts');

function parseArgs(argv) {
  const out = { start: DEFAULT_START, end: DEFAULT_END };
  for (let i = 0; i < argv.length; i += 1) {
    const a = argv[i];
    if (a === '--start') out.start = argv[++i];
    else if (a === '--end') out.end = argv[++i];
    else if (a === '--out') out.outPath = argv[++i];
    else if (a === '--bars') out.barsPath = argv[++i];
  }
  return out;
}

function findLatestDailyBars() {
  const root = runtimePath();
  if (!fs.existsSync(root)) return null;
  // Prefer the longest-history bars file for the longest stress signal.
  const matches = fs.readdirSync(root)
    .map((name) => {
      const m = name.match(/^pym-v5-massive-eod-adjusted-daily-bars-(\d{4}-\d{2}-\d{2})-(\d{4}-\d{2}-\d{2})\.jsonl$/);
      return m ? { name, startDate: m[1], endDate: m[2] } : null;
    })
    .filter(Boolean)
    .sort((a, b) => a.endDate.localeCompare(b.endDate) || a.startDate.localeCompare(b.startDate));
  if (!matches.length) return null;
  // Take the file with latest endDate; if multiple share endDate, prefer EARLIEST startDate (longest history).
  const latestEnd = matches.at(-1).endDate;
  const sameEnd = matches.filter((m) => m.endDate === latestEnd).sort((a, b) => a.startDate.localeCompare(b.startDate));
  return path.join(root, sameEnd[0].name);
}

async function fetchVix(startDate, endDate, apiKey) {
  const url = `https://api.massive.com/v2/aggs/ticker/I:VIX/range/1/day/${startDate}/${endDate}?adjusted=true&sort=asc&limit=50000&apiKey=${apiKey}`;
  const r = await fetch(url);
  if (!r.ok) throw new Error('VIX fetch failed: ' + r.status);
  const j = await r.json();
  const map = new Map();
  (j.results || []).forEach((agg) => {
    const date = new Date(agg.t).toISOString().slice(0, 10);
    map.set(date, agg.c);
  });
  return map;
}

function parseOccDir(startDate, endDate) {
  const map = new Map();
  if (!fs.existsSync(OCC_ROOT)) return map;
  fs.readdirSync(OCC_ROOT).filter((n) => n.startsWith('date=')).forEach((d) => {
    const date = d.slice('date='.length);
    if (date < startDate || date > endDate) return;
    const file = path.join(OCC_ROOT, d, `${date}.csv`);
    if (!fs.existsSync(file)) return;
    const content = fs.readFileSync(file, 'utf8').trim().split('\n');
    if (content.length < 2) return;
    const header = content[0].split(',');
    const values = content[1].split(',');
    const row = {};
    header.forEach((h, i) => { row[h] = values[i]; });
    const eqCalls = Number(row.equity_calls) || 0;
    const eqPuts = Number(row.equity_puts) || 0;
    if (eqCalls > 0) map.set(date, eqPuts / eqCalls);
  });
  return map;
}

function loadVixyCloses(barsPath, startDate, endDate) {
  const map = new Map();
  if (!fs.existsSync(barsPath)) throw new Error('missing_bars:' + barsPath);
  const content = fs.readFileSync(barsPath, 'utf8');
  content.split('\n').filter(Boolean).forEach((line) => {
    const r = JSON.parse(line);
    if (r.ticker !== 'VIXY') return;
    if (!Number.isFinite(r.close) || r.close <= 0) return;
    if (r.date < startDate || r.date > endDate) return;
    map.set(r.date, r.close);
  });
  return map;
}

function trailingZ(values, lookback = 60) {
  if (values.length < Math.max(20, Math.floor(lookback / 2))) return 0;
  const n = Math.min(lookback, values.length - 1);
  const slice = values.slice(-n - 1, -1);
  const m = slice.reduce((a, b) => a + b, 0) / slice.length;
  const sd = Math.sqrt(slice.reduce((a, b) => a + (b - m) ** 2, 0) / slice.length);
  if (sd === 0) return 0;
  const last = values.at(-1);
  return Number.isFinite(last) ? (last - m) / sd : 0;
}

async function main() {
  loadMassiveEnv();
  const args = parseArgs(process.argv.slice(2));
  const apiKey = process.env.MASSIVE_API_KEY || process.env.POLYGON_API_KEY;
  if (!apiKey) throw new Error('Missing MASSIVE_API_KEY');
  const barsPath = args.barsPath || findLatestDailyBars();
  if (!barsPath) throw new Error('no_daily_bars_file_found');
  console.log('Inputs:');
  console.log('  start:', args.start, '  end:', args.end);
  console.log('  bars:', barsPath);
  console.log('  occ:', OCC_ROOT);

  console.log('Fetching ^VIX daily ...');
  const vixByDate = await fetchVix(args.start, args.end, apiKey);
  console.log('  VIX days fetched:', vixByDate.size);

  console.log('Parsing OCC OI files ...');
  const occByDate = parseOccDir(args.start, args.end);
  console.log('  OCC days parsed:', occByDate.size);

  console.log('Loading VIXY closes from daily bars ...');
  const vixyByDate = loadVixyCloses(barsPath, args.start, args.end);
  console.log('  VIXY days:', vixyByDate.size);

  // Compute VIXY 5d log-return
  const vixyLogRet5 = new Map();
  const vixySorted = [...vixyByDate.entries()].sort((a, b) => a[0].localeCompare(b[0]));
  vixySorted.forEach(([date, close], i) => {
    if (i < 5) return;
    const prev = vixySorted[i - 5][1];
    if (close > 0 && prev > 0) vixyLogRet5.set(date, Math.log(close / prev));
  });

  // Build sorted series for trailing-z computation
  const allDates = [...new Set([...vixyByDate.keys(), ...vixByDate.keys(), ...occByDate.keys()])].sort();
  const vixyLog5Sorted = [...vixyLogRet5.entries()].sort((a, b) => a[0].localeCompare(b[0]));
  const vixSorted = [...vixByDate.entries()].sort((a, b) => a[0].localeCompare(b[0]));
  const occSorted = [...occByDate.entries()].sort((a, b) => a[0].localeCompare(b[0]));

  const outPath = args.outPath || path.join(DEFAULT_OUT_DIR, `options-stress-signal-${args.start}-${args.end}.jsonl`);
  ensureDir(path.dirname(outPath));
  const stream = fs.createWriteStream(outPath);

  let written = 0;
  let signalDays = 0;
  allDates.forEach((date) => {
    if (date < args.start || date > args.end) return;
    const vixyV = vixyLogRet5.get(date) ?? null;
    const vixV = vixByDate.get(date) ?? null;
    const occV = occByDate.get(date) ?? null;

    const signals = [];
    if (vixyV != null) {
      const series = vixyLog5Sorted.filter(([d]) => d <= date).map(([, v]) => v);
      if (series.length >= 30) signals.push(trailingZ(series, 60));
    }
    if (vixV != null) {
      const series = vixSorted.filter(([d]) => d <= date).map(([, v]) => v);
      if (series.length >= 30) signals.push(trailingZ(series, 60));
    }
    if (occV != null) {
      const series = occSorted.filter(([d]) => d <= date).map(([, v]) => v);
      if (series.length >= 30) signals.push(trailingZ(series, 60));
    }
    const stress = signals.length ? signals.reduce((a, b) => a + b, 0) / signals.length : null;
    if (stress != null) signalDays += 1;

    stream.write(JSON.stringify({
      date,
      vixyLogRet5: vixyV,
      vixClose: vixV,
      equityPCRatio: occV,
      vixyZ60: signals[0] ?? null,
      vixZ60: vixV != null && signals.length > 1 ? signals[signals.length - (occV != null ? 2 : 1)] : null,
      occZ60: occV != null ? signals[signals.length - 1] : null,
      stress,
    }) + '\n');
    written += 1;
  });

  await new Promise((resolve) => stream.end(resolve));
  console.log(`wrote ${outPath}`);
  console.log(`  rows: ${written}`);
  console.log(`  rows with non-null stress: ${signalDays}`);
}

main().catch((e) => { console.error(e); process.exit(1); });
