#!/usr/bin/env node
// S2 — Portfolio combiner of the 4 existing fear-extreme variants
//
// Read pre-built artifacts for:
//   - occ-pc-contrarian-intraday-1x-long-only
//   - occ-pc-contrarian-intraday-3x
//   - vix-term-contrarian-intraday-vix3m-1x
//   - vix-term-contrarian-intraday-inv-long-3x-overnight
// Align dailyReturn streams, compute pairwise correlation matrix, then build an
// equal-weight portfolio of dailyReturn and report Sharpe / maxDD / vs SPY.
//
// No new signal — just a meta-portfolio over existing variants.  Does NOT
// register a strategy.  Surfaces a comparison table for the user to judge.

const fs = require('node:fs');
const path = require('node:path');

const { PROJECT_ROOT, OFFICIAL_WINDOWS, SENSITIVITY_WINDOWS } = require('../../src/research-utils');

const ARTIFACT_DIR = path.join(PROJECT_ROOT, 'artifacts');
const VARIANTS = [
  'occ-pc-contrarian-intraday-1x-long-only',
  'occ-pc-contrarian-intraday-3x',
  'vix-term-contrarian-intraday-vix3m-1x',
  'vix-term-contrarian-intraday-inv-long-3x-overnight',
];

function readReport(id) {
  const fp = path.join(ARTIFACT_DIR, `${id}-report.json`);
  if (!fs.existsSync(fp)) throw new Error(`missing_artifact:${fp}`);
  return JSON.parse(fs.readFileSync(fp, 'utf8'));
}

function dailyReturnsByDate(report) {
  const out = new Map();
  for (const r of report.equitySeries || []) {
    if (Number.isFinite(r.dailyReturn)) out.set(r.date, r.dailyReturn);
  }
  return out;
}

function tradeDatesByVariant(report) {
  return new Set((report.trades || []).map((t) => t.date).filter(Boolean));
}

function unionDates(maps) {
  const out = new Set();
  for (const m of maps) for (const d of m.keys()) out.add(d);
  return [...out].sort();
}

function filterDates(dates, startDate, endDate) {
  return dates.filter((d) => d >= startDate && d <= endDate);
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

function summarize(dailyReturns) {
  const n = dailyReturns.length;
  if (n === 0) return { trade_count: 0, days_active: 0, hit_rate: 0, total_net_pct: 0, sharpe_per_trade: 0, max_drawdown_pct: 0 };
  const active = dailyReturns.filter((v) => v !== 0);
  const m = dailyReturns.reduce((s, v) => s + v, 0) / n;
  const sd = Math.sqrt(dailyReturns.reduce((s, v) => s + ((v - m) ** 2), 0) / n);
  const sharpe = sd > 0 ? (m / sd) * Math.sqrt(252) : 0;
  let cum = 0; let peak = 0; let dd = 0;
  for (const v of dailyReturns) {
    cum += v; if (cum > peak) peak = cum; if (peak - cum > dd) dd = peak - cum;
  }
  const wins = active.filter((v) => v > 0).length;
  return {
    trade_count: active.length,
    days_active: active.length,
    hit_rate: active.length ? wins / active.length : 0,
    total_net_pct: cum * 100,
    sharpe_per_trade: sharpe,
    max_drawdown_pct: dd * 100,
  };
}

function buildSeries(variantData, dates) {
  return dates.map((d) => variantData.get(d) ?? 0);
}

function portfolio(variantSeries, weights) {
  const n = variantSeries[0].length;
  const out = new Array(n).fill(0);
  for (let i = 0; i < n; i += 1) {
    for (let j = 0; j < variantSeries.length; j += 1) out[i] += weights[j] * variantSeries[j][i];
  }
  return out;
}

function main() {
  const reports = {};
  const dailyByVariant = {};
  const tradeDays = {};
  for (const id of VARIANTS) {
    reports[id] = readReport(id);
    dailyByVariant[id] = dailyReturnsByDate(reports[id]);
    tradeDays[id] = tradeDatesByVariant(reports[id]);
  }

  const allDates = unionDates(VARIANTS.map((v) => dailyByVariant[v]));
  const results = {};
  const portfolioStats = {};
  const correlationMatrices = {};

  function evalWindow(name, startDate, endDate) {
    const dates = filterDates(allDates, startDate, endDate);
    const seriesByVariant = {};
    for (const id of VARIANTS) seriesByVariant[id] = buildSeries(dailyByVariant[id], dates);

    // Per-variant stats
    results[name] = {};
    for (const id of VARIANTS) results[name][id] = summarize(seriesByVariant[id]);

    // Equal-weight portfolio (25% each)
    const ew = portfolio(VARIANTS.map((id) => seriesByVariant[id]), [0.25, 0.25, 0.25, 0.25]);
    portfolioStats[name] = {
      'equal-weight 4-variant': summarize(ew),
    };
    // Risk-weighted: scale 3x variants down to compare like-for-like (we just present EW for now;
    // user feedback rules say do not pre-optimize weights — stay equal-weight to keep results
    // honest and out-of-sample.)

    // Correlation matrix of daily returns within the window
    const m = {};
    for (const a of VARIANTS) {
      m[a] = {};
      for (const b of VARIANTS) m[a][b] = correlation(seriesByVariant[a], seriesByVariant[b]);
    }
    correlationMatrices[name] = m;

    // Days where N variants fire together
    const fireCount = { 1: 0, 2: 0, 3: 0, 4: 0 };
    for (const d of dates) {
      let k = 0;
      for (const id of VARIANTS) if (tradeDays[id].has(d)) k += 1;
      if (k >= 1) fireCount[Math.min(k, 4)] += 1;
    }
    portfolioStats[name].fireOverlap = fireCount;
  }

  const officialWindows = [OFFICIAL_WINDOWS.train, ...OFFICIAL_WINDOWS.tests];
  for (const w of officialWindows) evalWindow(w.name, w.startDate, w.endDate);
  for (const w of [SENSITIVITY_WINDOWS.train, SENSITIVITY_WINDOWS.test, SENSITIVITY_WINDOWS.full]) {
    evalWindow(w.name, w.startDate, w.endDate);
  }

  // -------------------- Print tables --------------------
  process.stdout.write('\nS2 — Equal-weight 4-variant portfolio of existing fear-extreme strategies\n');
  process.stdout.write('  window                                trades  net %    hit %  Sharpe  maxDD %\n');
  for (const [winName, p] of Object.entries(portfolioStats)) {
    const s = p['equal-weight 4-variant'];
    const row = [
      winName.padEnd(36),
      String(s.trade_count).padStart(6),
      s.total_net_pct.toFixed(2).padStart(7),
      (s.hit_rate * 100).toFixed(1).padStart(7),
      s.sharpe_per_trade.toFixed(2).padStart(7),
      s.max_drawdown_pct.toFixed(2).padStart(8),
    ];
    process.stdout.write('  ' + row.join('  ') + '\n');
  }
  process.stdout.write('\nFire-overlap counts (per window, # of variants firing on same day):\n');
  for (const [winName, p] of Object.entries(portfolioStats)) {
    process.stdout.write(`  ${winName}: ${JSON.stringify(p.fireOverlap)}\n`);
  }
  process.stdout.write('\nCorrelation matrix (full 16-mo sensitivity window):\n');
  const m = correlationMatrices['full_16mo_sensitivity'];
  const labels = VARIANTS.map((v) => v.replace('-intraday', '').replace('contrarian-', ''));
  process.stdout.write('  ' + ' '.padEnd(28) + labels.map((l) => l.padStart(14)).join('') + '\n');
  for (let i = 0; i < VARIANTS.length; i += 1) {
    const row = [labels[i].padEnd(28)];
    for (let j = 0; j < VARIANTS.length; j += 1) {
      row.push(m[VARIANTS[i]][VARIANTS[j]].toFixed(2).padStart(14));
    }
    process.stdout.write('  ' + row.join('') + '\n');
  }

  // -------------------- Verdict --------------------
  const fullEw = portfolioStats['full_16mo_sensitivity']['equal-weight 4-variant'];
  const testEw = portfolioStats['test_2026_sensitivity']['equal-weight 4-variant'];
  let verdict;
  if (fullEw.trade_count < 10) verdict = 'reject (too few combined trade-days)';
  else if (fullEw.total_net_pct > 0 && testEw.total_net_pct > 0 && fullEw.sharpe_per_trade > 1.5) verdict = 'strong';
  else if (fullEw.total_net_pct > 0 && testEw.total_net_pct > 0) verdict = 'marginal';
  else verdict = 'reject (failed walk-forward)';

  process.stdout.write(`\nVerdict: ${verdict}\n`);
  process.stdout.write(`  Reasoning: full-window net ${fullEw.total_net_pct.toFixed(2)}%, Sharpe ${fullEw.sharpe_per_trade.toFixed(2)}, ` +
    `2026 test net ${testEw.total_net_pct.toFixed(2)}%.\n`);

  const out = {
    generated_at: new Date().toISOString(),
    variants: VARIANTS,
    weights: [0.25, 0.25, 0.25, 0.25],
    per_variant_stats: results,
    portfolio_stats: portfolioStats,
    correlation_matrices: correlationMatrices,
    verdict,
  };
  const outPath = path.join(PROJECT_ROOT, 'artifacts', 'research', 's2-fear-portfolio-combiner.json');
  fs.mkdirSync(path.dirname(outPath), { recursive: true });
  fs.writeFileSync(outPath, JSON.stringify(out, null, 2));
  process.stdout.write(`\nWritten ${outPath}\n`);
}

main();
