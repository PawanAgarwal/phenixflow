#!/usr/bin/env node
// Orthogonality + portfolio-combine analysis for the 3 promising research
// items (A3 VVIX, B1 gap-down, S2 fear-portfolio) plus the 4 existing
// registered fear-extreme variants.
//
// For each strategy we compute a Map<date, dailyReturn> over the full
// 16-month sensitivity window.  Then:
//   1. Pairwise daily-return correlation matrix
//   2. Pairwise fire-day overlap
//   3. Equal-weight portfolios for every subset
//   4. Compare best portfolio vs each individual
//
// Reads the 4 existing registered strategies from their pre-built
// artifacts; re-derives A3 and B1 inline (their research scripts don't
// dump trades to disk).

const path = require('node:path');
const fs = require('node:fs');
const {
  loadVixTermZSeriesWide, loadDailyBars, stockTradingDaysInRange,
  executeTrade, PROJECT_ROOT, SENSITIVITY_WINDOWS,
} = require('../../src/research-utils');

const ARTIFACT_DIR = path.join(PROJECT_ROOT, 'artifacts');
const REGISTERED = [
  'occ-pc-contrarian-intraday-1x-long-only',
  'occ-pc-contrarian-intraday-3x',
  'vix-term-contrarian-intraday-vix3m-1x',
  'vix-term-contrarian-intraday-inv-long-3x-overnight',
];

const WINDOW = SENSITIVITY_WINDOWS.full; // 2025-01-02 → 2026-05-12

function readReportDailyReturns(id) {
  const fp = path.join(ARTIFACT_DIR, `${id}-report.json`);
  const j = JSON.parse(fs.readFileSync(fp, 'utf8'));
  const m = new Map();
  for (const r of j.equitySeries || []) {
    if (r.date && Number.isFinite(r.dailyReturn)) m.set(r.date, r.dailyReturn);
  }
  // Trade-fire set (calendar dates trade landed on)
  const fires = new Set((j.trades || []).map((t) => t.date).filter(Boolean));
  return { dailyReturns: m, fires };
}

// Re-derive A3 (VVIX z>=+2 overnight 3× SPXL long) daily returns.
async function deriveA3(vixByDay) {
  const days = stockTradingDaysInRange('2024-11-01', WINDOW.endDate);
  const m = new Map();
  const fires = new Set();
  for (let i = 0; i < days.length - 1; i += 1) {
    const sig = days[i];
    if (sig < WINDOW.startDate || sig > WINDOW.endDate) continue;
    const v = vixByDay.get(sig);
    if (!v || !Number.isFinite(v.z_vvix) || v.z_vvix < 2.0) continue;
    // eslint-disable-next-line no-await-in-loop
    const tr = await executeTrade({
      side: 'LONG', leverage: 3.0,
      signalDay: sig, entryDay: sig, exitDay: days[i + 1],
      entryMinuteEt: 955, exitMinuteEt: 955,
      costBpsRoundTrip: 3,
    });
    if (!tr) continue;
    // Attribute P&L to exit day (the day equity changes).
    m.set(tr.exitDay, (m.get(tr.exitDay) || 0) + tr.netReturn);
    fires.add(tr.exitDay);
  }
  return { dailyReturns: m, fires };
}

// Re-derive B1 (gap-down-long-3x intraday).
async function deriveB1(spyBars) {
  const days = stockTradingDaysInRange('2024-11-01', WINDOW.endDate);
  const m = new Map();
  const fires = new Set();
  for (let i = 1; i < days.length; i += 1) {
    const d = days[i];
    if (d < WINDOW.startDate || d > WINDOW.endDate) continue;
    const a = spyBars.get(days[i - 1]); const b = spyBars.get(d);
    if (!a || !b) continue;
    const gap = b.open / a.close - 1;
    if (!Number.isFinite(gap) || gap > -0.005) continue;
    // eslint-disable-next-line no-await-in-loop
    const tr = await executeTrade({
      side: 'LONG', leverage: 3.0,
      signalDay: d, entryDay: d, exitDay: d,
      entryMinuteEt: 575, exitMinuteEt: 955,
      costBpsRoundTrip: 3,
    });
    if (!tr) continue;
    m.set(d, (m.get(d) || 0) + tr.netReturn);
    fires.add(d);
  }
  return { dailyReturns: m, fires };
}

function unionDates(...maps) {
  const s = new Set();
  for (const m of maps) for (const d of m.keys()) s.add(d);
  return [...s].sort();
}

function buildSeries(dailyReturns, dates) {
  return dates.map((d) => dailyReturns.get(d) ?? 0);
}

function correlation(a, b) {
  if (a.length !== b.length || a.length < 3) return 0;
  const ma = a.reduce((s, v) => s + v, 0) / a.length;
  const mb = b.reduce((s, v) => s + v, 0) / b.length;
  let num = 0; let da = 0; let db = 0;
  for (let i = 0; i < a.length; i += 1) {
    const xa = a[i] - ma; const xb = b[i] - mb;
    num += xa * xb; da += xa * xa; db += xb * xb;
  }
  if (da === 0 || db === 0) return 0;
  return num / Math.sqrt(da * db);
}

function summarize(returns) {
  const n = returns.length;
  if (n === 0) return { trade_days: 0, total_net_pct: 0, sharpe: 0, max_dd_pct: 0, hit_rate: 0 };
  const active = returns.filter((v) => v !== 0);
  const m = returns.reduce((s, v) => s + v, 0) / n;
  const sd = Math.sqrt(returns.reduce((s, v) => s + ((v - m) ** 2), 0) / n);
  const sharpe = sd > 0 ? (m / sd) * Math.sqrt(252) : 0;
  let cum = 0; let peak = 0; let dd = 0;
  for (const v of returns) { cum += v; if (cum > peak) peak = cum; if (peak - cum > dd) dd = peak - cum; }
  const wins = active.filter((v) => v > 0).length;
  return {
    trade_days: active.length,
    total_net_pct: cum * 100,
    sharpe,
    max_dd_pct: dd * 100,
    hit_rate: active.length ? wins / active.length : 0,
  };
}

function combinations(arr, k) {
  const out = [];
  function rec(start, chosen) {
    if (chosen.length === k) { out.push([...chosen]); return; }
    for (let i = start; i <= arr.length - (k - chosen.length); i += 1) {
      chosen.push(arr[i]); rec(i + 1, chosen); chosen.pop();
    }
  }
  rec(0, []);
  return out;
}

function shortName(id) {
  return {
    'A3-VVIX': 'A3-VVIX',
    'B1-gapdown': 'B1-gapdown',
    'S2-fearport': 'S2-fearport',
    'occ-pc-contrarian-intraday-1x-long-only': 'occ-1x',
    'occ-pc-contrarian-intraday-3x': 'occ-3x',
    'vix-term-contrarian-intraday-vix3m-1x': 'vix-1x',
    'vix-term-contrarian-intraday-inv-long-3x-overnight': 'vix-3x-ON',
  }[id] || id;
}

async function main() {
  process.stdout.write('Loading shared data (VIX + SPY daily bars)...\n');
  const t0 = Date.now();
  const [vixByDay, spyBars] = await Promise.all([
    loadVixTermZSeriesWide(20),
    loadDailyBars('SPY', '2024-11-01', WINDOW.endDate),
  ]);
  process.stdout.write(`Loaded in ${((Date.now()-t0)/1000).toFixed(1)}s\n`);

  // Build daily-return Maps for all candidate strategies.
  process.stdout.write('Deriving A3 and B1 daily returns...\n');
  const a3 = await deriveA3(vixByDay);
  const b1 = await deriveB1(spyBars);

  // S2 = equal-weight of the 4 registered fear variants.
  const regs = {};
  for (const id of REGISTERED) regs[id] = readReportDailyReturns(id);
  const s2DailyReturns = new Map();
  for (const id of REGISTERED) {
    for (const [d, r] of regs[id].dailyReturns.entries()) {
      s2DailyReturns.set(d, (s2DailyReturns.get(d) || 0) + 0.25 * r);
    }
  }
  const s2 = { dailyReturns: s2DailyReturns, fires: new Set([...regs[REGISTERED[0]].fires, ...regs[REGISTERED[1]].fires, ...regs[REGISTERED[2]].fires, ...regs[REGISTERED[3]].fires]) };

  const strategies = {
    'A3-VVIX': a3, 'B1-gapdown': b1, 'S2-fearport': s2,
    ...regs,
  };
  const allDates = unionDates(...Object.values(strategies).map((s) => s.dailyReturns));
  // Restrict to window
  const dates = allDates.filter((d) => d >= WINDOW.startDate && d <= WINDOW.endDate);

  // Per-strategy stats
  process.stdout.write('\nPer-strategy 16-mo stats (Jan 2025 → May 2026):\n');
  process.stdout.write('  strategy            days  net %      Sharpe  maxDD %  hit %\n');
  const ids = Object.keys(strategies);
  const seriesById = {};
  for (const id of ids) seriesById[id] = buildSeries(strategies[id].dailyReturns, dates);
  for (const id of ids) {
    const s = summarize(seriesById[id]);
    process.stdout.write(
      `  ${shortName(id).padEnd(20)}${String(s.trade_days).padStart(4)}  ${s.total_net_pct.toFixed(2).padStart(8)}  ${s.sharpe.toFixed(2).padStart(6)}  ${s.max_dd_pct.toFixed(2).padStart(7)}  ${(s.hit_rate * 100).toFixed(1).padStart(5)}\n`,
    );
  }

  // -------- Pairwise correlation matrix --------
  process.stdout.write('\nPairwise daily-return correlation matrix (full window):\n');
  process.stdout.write('  ' + ' '.padEnd(22) + ids.map((id) => shortName(id).padStart(12)).join('') + '\n');
  for (const a of ids) {
    const row = [shortName(a).padEnd(22)];
    for (const b of ids) {
      row.push(correlation(seriesById[a], seriesById[b]).toFixed(2).padStart(12));
    }
    process.stdout.write('  ' + row.join('') + '\n');
  }

  // -------- Fire-day overlap --------
  process.stdout.write('\nFire-day overlap (Jaccard = |A ∩ B| / |A ∪ B|):\n');
  process.stdout.write('  ' + ' '.padEnd(22) + ids.map((id) => shortName(id).padStart(12)).join('') + '\n');
  for (const a of ids) {
    const row = [shortName(a).padEnd(22)];
    for (const b of ids) {
      const sa = strategies[a].fires; const sb = strategies[b].fires;
      let inter = 0; for (const d of sa) if (sb.has(d)) inter += 1;
      const uni = sa.size + sb.size - inter;
      row.push((uni === 0 ? 0 : inter / uni).toFixed(2).padStart(12));
    }
    process.stdout.write('  ' + row.join('') + '\n');
  }

  // -------- Subset portfolios --------
  process.stdout.write('\nAll equal-weight portfolios (k=2..N) ranked by Sharpe:\n');
  const PORT_CANDIDATES = ids; // include all
  const portfolios = [];
  for (let k = 2; k <= PORT_CANDIDATES.length; k += 1) {
    for (const subset of combinations(PORT_CANDIDATES, k)) {
      const series = subset.map((id) => seriesById[id]);
      const w = 1 / k;
      const combined = dates.map((_, i) => series.reduce((s, ser) => s + w * ser[i], 0));
      const stats = summarize(combined);
      portfolios.push({ subset, stats });
    }
  }
  portfolios.sort((a, b) => b.stats.sharpe - a.stats.sharpe);
  process.stdout.write('  rank  k  subset                                                            net %   Sharpe  maxDD %  hit %\n');
  for (let i = 0; i < Math.min(15, portfolios.length); i += 1) {
    const p = portfolios[i];
    const subsetStr = p.subset.map(shortName).join('+');
    process.stdout.write(
      `  ${String(i + 1).padStart(4)}  ${p.subset.length}  ${subsetStr.padEnd(65)} ${p.stats.total_net_pct.toFixed(2).padStart(7)}  ${p.stats.sharpe.toFixed(2).padStart(6)}  ${p.stats.max_dd_pct.toFixed(2).padStart(7)}  ${(p.stats.hit_rate * 100).toFixed(1).padStart(5)}\n`,
    );
  }

  // -------- Best diversifier for each top single strategy --------
  process.stdout.write('\nBest 2-way diversifier for each top single strategy (by Sharpe gain):\n');
  for (const id of ['A3-VVIX', 'B1-gapdown', 'S2-fearport']) {
    const single = summarize(seriesById[id]);
    let bestPart = null; let bestSharpe = -Infinity;
    for (const other of ids) {
      if (other === id) continue;
      const series = [seriesById[id], seriesById[other]];
      const combined = dates.map((_, i) => 0.5 * series[0][i] + 0.5 * series[1][i]);
      const stats = summarize(combined);
      if (stats.sharpe > bestSharpe) { bestSharpe = stats.sharpe; bestPart = { other, stats }; }
    }
    process.stdout.write(`  ${shortName(id)} + ${shortName(bestPart.other)}: Sharpe ${bestPart.stats.sharpe.toFixed(2)} (vs ${single.sharpe.toFixed(2)} solo), maxDD ${bestPart.stats.max_dd_pct.toFixed(2)}% (vs ${single.max_dd_pct.toFixed(2)}%)\n`);
  }

  const outPath = path.join(PROJECT_ROOT, 'artifacts', 'research', 'combined-promising-strategies.json');
  fs.mkdirSync(path.dirname(outPath), { recursive: true });
  fs.writeFileSync(outPath, JSON.stringify({
    generated_at: new Date().toISOString(),
    window: WINDOW,
    per_strategy: Object.fromEntries(ids.map((id) => [id, summarize(seriesById[id])])),
    correlation_matrix: Object.fromEntries(ids.map((a) => [a, Object.fromEntries(ids.map((b) => [b, correlation(seriesById[a], seriesById[b])]))])),
    top_portfolios: portfolios.slice(0, 20).map((p) => ({ subset: p.subset, ...p.stats })),
  }, null, 2));
  process.stdout.write(`\nWritten ${outPath}\n`);
}

main().catch((err) => { process.stderr.write(`${err.stack || err.message}\n`); process.exit(1); });
