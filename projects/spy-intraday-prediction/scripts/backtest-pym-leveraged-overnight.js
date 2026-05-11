#!/usr/bin/env node
// Scale up the production strategy via three orthogonal upgrades:
//   1. Leverage (3× sizing — emulates TQQQ/SQQQ; QQQ has more vol so the effective beta is realistic)
//   2. Overnight hold on extreme bias (|bias| >= 0.40 → enter at prior-day 15:55, exit next-day 15:55)
//   3. Granular sleeve features (vol sleeve weight, inverse weight as separate signals)
//
// Goal: see whether removing the artificial constraints (1× SPY, intraday-only, coarse bias)
// gives us the rest of PYM's return.

const path = require('node:path');
const fs = require('node:fs');
const zlib = require('node:zlib');
const readline = require('node:readline');

const { PROJECT_ROOT } = require('../src/config');
const { writeJson } = require('../src/strategy-runner');
const { loadPymHoldings, pymBias } = require('../src/pym-bias-strategy');
const { defaultFeaturesPath } = require('../src/build-features-1m');

const PYM_ARTIFACT = '/Users/pawanagarwal/github/phenixflow/projects/pym-v5-replication/artifacts/pym-v5-backtest-massive-eod-rsi-wilder-next_close-2025-01-02-2026-05-06.json';

// Volatility-sleeve weight (long-vol holdings = bear signal)
const VOL_LONG = new Set(['VIXY', 'UVXY']);
const VOL_SHORT = new Set(['SVXY', 'SVIX']);
const INVERSE = new Set(['SQQQ','SPXU','SOXS','EDZ','PSQ','TECS','TZA','SH','SDS','TMV','QID','FAZ']);
const LEVERAGED_BULL = new Set(['TQQQ','UPRO','SOXL','TECL','EDC','SPXL','FAS','TNA','QLD']);

function sleeveDecomp(holdings) {
  let volLong=0, volShort=0, inverse=0, levBull=0;
  for (const [t, w] of Object.entries(holdings)) {
    if (!Number.isFinite(w)) continue;
    if (VOL_LONG.has(t)) volLong += w;
    if (VOL_SHORT.has(t)) volShort += w;
    if (INVERSE.has(t)) inverse += w;
    if (LEVERAGED_BULL.has(t)) levBull += w;
  }
  return { volLong, volShort, inverse, levBull };
}

async function loadFeaturesForDay(projectRoot, root, day) {
  const p = defaultFeaturesPath(projectRoot, root, day);
  if (!fs.existsSync(p)) return [];
  const stream = fs.createReadStream(p).pipe(zlib.createGunzip());
  const rl = readline.createInterface({ input: stream, crlfDelay: Infinity });
  const out = [];
  for await (const line of rl) {
    if (!line) continue;
    out.push(JSON.parse(line));
  }
  return out;
}

function isWeekend(d) {
  const x = new Date(`${d}T00:00:00.000Z`).getUTCDay();
  return x === 0 || x === 6;
}

function listDates(start, end) {
  const out = [];
  const cur = new Date(`${start}T00:00:00.000Z`);
  const stop = new Date(`${end}T00:00:00.000Z`);
  while (cur <= stop) {
    const d = cur.toISOString().slice(0, 10);
    if (!isWeekend(d)) out.push(d);
    cur.setUTCDate(cur.getUTCDate() + 1);
  }
  return out;
}

function summarize(trades) {
  if (trades.length === 0) return { n: 0, gross: 0, net: 0, sharpe: 0, dd: 0, hit: 0 };
  const n = trades.length;
  const sumNet = trades.reduce((a, t) => a + t.net_return, 0);
  const sumGross = trades.reduce((a, t) => a + t.gross_return, 0);
  const m = sumNet / n;
  const sd = Math.sqrt(trades.reduce((a, t) => a + ((t.net_return - m) ** 2), 0) / n);
  let cum = 0; let peak = 0; let dd = 0;
  for (const t of trades) { cum += t.net_return; if (cum > peak) peak = cum; if (peak - cum > dd) dd = peak - cum; }
  const wins = trades.filter((t) => t.net_return > 0).length;
  return {
    n,
    gross_pct: sumGross * 100,
    net_pct: sumNet * 100,
    avg_net_bps: m * 10000,
    sharpe: sd > 0 ? (m / sd) * Math.sqrt(252) : 0,
    dd_pct: dd * 100,
    hit_rate: wins / n,
  };
}

async function runStrategy({ pymByDate, startDate, endDate, params }) {
  const p = {
    biasLong: 0.20,
    biasShort: -0.20,
    extremeBias: 0.40, // |bias| >= this and overnight=true → multi-day hold
    leverage: 1.0,
    overnight: false, // if true: enter at prior-day 15:55 (after PYM close), exit on signal day 15:55
    entryMinuteEt: 690, // 11:30 ET intraday entry
    exitMinuteEt: 955, // 15:55 ET intraday exit
    costBpsRoundTrip: 2,
    useSleeveSignal: false, // use VolLong-VolShort + (-Inverse) + LevBull as alternate signal
    ...params,
  };
  const days = listDates(startDate, endDate).filter((d) => pymByDate.has(d));
  const trades = [];
  for (let i = 0; i < days.length; i += 1) {
    const day = days[i];
    const pe = pymByDate.get(day);
    const sleeve = sleeveDecomp(pe.holdings);
    // Compute the directional signal
    let signal = pe.bias;
    if (p.useSleeveSignal) {
      // alt signal: leveraged bull - inverse - (volLong-volShort)
      signal = sleeve.levBull - sleeve.inverse - (sleeve.volLong - sleeve.volShort);
    }
    let side = null;
    if (signal >= p.biasLong) side = 'LONG';
    else if (signal <= p.biasShort) side = 'SHORT';
    if (!side) continue;

    const extremeMode = p.overnight && Math.abs(signal) >= p.extremeBias;
    let entryPrice, exitPrice, entryDate;
    // eslint-disable-next-line no-await-in-loop
    const rowsToday = await loadFeaturesForDay(PROJECT_ROOT, 'SPY', day);
    if (rowsToday.length === 0) continue;
    const exitRow = rowsToday.find((r) => r.minute_of_day_et === p.exitMinuteEt) || rowsToday[rowsToday.length - 1];
    if (!Number.isFinite(exitRow.spy_close)) continue;

    if (extremeMode) {
      // Enter at PRIOR day close (15:55), exit at today's close
      const prevDay = days[i - 1];
      if (!prevDay) continue;
      // eslint-disable-next-line no-await-in-loop
      const rowsPrev = await loadFeaturesForDay(PROJECT_ROOT, 'SPY', prevDay);
      if (rowsPrev.length === 0) continue;
      const prevExitRow = rowsPrev.find((r) => r.minute_of_day_et === p.exitMinuteEt) || rowsPrev[rowsPrev.length - 1];
      if (!Number.isFinite(prevExitRow.spy_close)) continue;
      entryPrice = prevExitRow.spy_close;
      entryDate = prevDay;
    } else {
      const entryRow = rowsToday.find((r) => r.minute_of_day_et === p.entryMinuteEt);
      if (!entryRow || !Number.isFinite(entryRow.spy_open)) continue;
      entryPrice = entryRow.spy_open;
      entryDate = day;
    }
    exitPrice = exitRow.spy_close;
    const sign = side === 'LONG' ? +1 : -1;
    const gross = sign * p.leverage * (exitPrice / entryPrice - 1);
    // Cost is absolute (not scaled by leverage). For leveraged-ETF trades (TQQQ/SQQQ) spreads
    // are 2-3 bps RT regardless of the 3× underlying exposure. For margin-buy of 3× SPY notional,
    // pass costBpsRoundTrip = 6 bps explicitly to account for proportional cost.
    const cost = p.costBpsRoundTrip / 10000;
    const net = gross - cost;
    trades.push({
      date: day, entry_date: entryDate, side, signal, leverage: p.leverage, extremeMode,
      entry_price: entryPrice, exit_price: exitPrice,
      gross_return: gross, cost, net_return: net,
      sleeve_volLong: sleeve.volLong, sleeve_inverse: sleeve.inverse, sleeve_levBull: sleeve.levBull,
    });
  }
  return { params: p, stats: summarize(trades), trades };
}

const WINDOWS = [
  { name: 'train_2025', startDate: '2025-01-02', endDate: '2025-12-31' },
  { name: 'test_2026', startDate: '2026-01-02', endDate: '2026-04-27' },
  { name: 'full_16mo', startDate: '2025-01-02', endDate: '2026-04-27' },
];

const VARIANTS = [
  { name: 'baseline_V', params: { biasLong: 0.20, biasShort: -0.20, leverage: 1.0 } },
  // Realistic leveraged-ETF cost: TQQQ/UPRO RT spreads are ~2-3 bps, not 6
  { name: 'lev3x_realistic_cost', params: { biasLong: 0.20, biasShort: -0.20, leverage: 3.0, costBpsRoundTrip: 3 } },
  { name: 'lev3x_optimistic_cost', params: { biasLong: 0.20, biasShort: -0.20, leverage: 3.0, costBpsRoundTrip: 2 } },
  { name: 'overnight_only_extreme', params: { biasLong: 0.20, biasShort: -0.20, overnight: true, extremeBias: 0.40 } },
  { name: 'overnight_extreme_lev3x_real', params: { biasLong: 0.20, biasShort: -0.20, leverage: 3.0, overnight: true, extremeBias: 0.40, costBpsRoundTrip: 3 } },
  { name: 'overnight_extreme_lev3x_opt', params: { biasLong: 0.20, biasShort: -0.20, leverage: 3.0, overnight: true, extremeBias: 0.40, costBpsRoundTrip: 2 } },
  { name: 'sleeve_signal_3x_real', params: { useSleeveSignal: true, biasLong: 0.10, biasShort: -0.10, leverage: 3.0, costBpsRoundTrip: 3 } },
  { name: 'all_three_real', params: { useSleeveSignal: true, biasLong: 0.10, biasShort: -0.10, leverage: 3.0, overnight: true, extremeBias: 0.20, costBpsRoundTrip: 3 } },
  // Cleanest combo: bias 0.20 + overnight on extreme + lev3x at realistic cost
  { name: 'best_combo', params: { biasLong: 0.20, biasShort: -0.20, leverage: 3.0, overnight: true, extremeBias: 0.30, costBpsRoundTrip: 3 } },
];

async function main() {
  const pymByDate = loadPymHoldings(PYM_ARTIFACT);
  const allResults = {};
  for (const v of VARIANTS) {
    process.stdout.write(`\n=== ${v.name} ===\n`);
    process.stdout.write(`  params: ${JSON.stringify(v.params)}\n`);
    allResults[v.name] = {};
    for (const w of WINDOWS) {
      // eslint-disable-next-line no-await-in-loop
      const r = await runStrategy({ pymByDate, startDate: w.startDate, endDate: w.endDate, params: v.params });
      const s = r.stats;
      process.stdout.write(`  ${w.name.padEnd(12)} N=${String(s.n).padStart(4)} gross=${s.gross_pct.toFixed(2).padStart(7)}% net=${s.net_pct.toFixed(2).padStart(7)}% sharpe=${s.sharpe.toFixed(2).padStart(6)} dd=${s.dd_pct.toFixed(2)}% hit=${(s.hit_rate*100).toFixed(1)}%\n`);
      allResults[v.name][w.name] = s;
    }
  }
  const outPath = path.join(PROJECT_ROOT, 'artifacts', 'pym-leveraged-overnight-summary.json');
  writeJson(outPath, { generated_at: new Date().toISOString(), variants: VARIANTS, results: allResults });
  process.stdout.write(`\nWritten ${outPath}\n`);
}

main().catch((err) => { process.stderr.write(`${err.stack || err.message}\n`); process.exit(1); });
