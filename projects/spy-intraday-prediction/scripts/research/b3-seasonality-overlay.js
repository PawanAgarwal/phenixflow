#!/usr/bin/env node
// B3 — Day-of-week + turn-of-month + OPEX seasonality overlay
//
// Position-size scalar (not a standalone strategy).  For each calendar
// feature compute the mean / Sharpe of SPY daily close-to-close return.  The
// output is a table of multipliers a strategy can apply to position size on
// each kind of day.
//
// Calendar features:
//   - day-of-week: Mon/Tue/Wed/Thu/Fri (compared to baseline = all days)
//   - turn-of-month: days within +/- 2 trading sessions of month-end
//   - OPEX week: 3rd Friday of each month and the week leading up to it
//
// Walk-forward: training 2025 estimate, test 2026 confirm.  Reject features
// that flip sign between train and test windows.

const path = require('node:path');
const fs = require('node:fs');
const {
  loadDailyBars, stockTradingDaysInRange, OFFICIAL_WINDOWS, SENSITIVITY_WINDOWS,
  PROJECT_ROOT,
} = require('../../src/research-utils');

function dayOfWeek(yyyymmdd) {
  const dt = new Date(`${yyyymmdd}T00:00:00Z`);
  return dt.getUTCDay(); // 0=Sun .. 6=Sat
}

function isThirdFriday(yyyymmdd) {
  const dt = new Date(`${yyyymmdd}T00:00:00Z`);
  if (dt.getUTCDay() !== 5) return false;
  const day = dt.getUTCDate();
  return day >= 15 && day <= 21;
}

function isOpexWeek(yyyymmdd) {
  // Same calendar week (Mon-Fri) as the 3rd Friday of the month.
  const dt = new Date(`${yyyymmdd}T00:00:00Z`);
  // Find the 3rd Friday of this month.
  const y = dt.getUTCFullYear(); const m = dt.getUTCMonth();
  let count = 0;
  for (let d = 1; d <= 31; d += 1) {
    const x = new Date(Date.UTC(y, m, d));
    if (x.getUTCMonth() !== m) break;
    if (x.getUTCDay() === 5) {
      count += 1;
      if (count === 3) {
        // Check if `dt` is within the Mon-Fri of this week.
        const fridayDate = x.getUTCDate();
        const day = dt.getUTCDate();
        return day >= fridayDate - 4 && day <= fridayDate;
      }
    }
  }
  return false;
}

function isTurnOfMonth(yyyymmdd, allDates) {
  // Within +/- 2 trading sessions of month-end.
  const i = allDates.indexOf(yyyymmdd);
  if (i < 0) return false;
  // Find next month-end in the date list.
  for (let k = Math.max(0, i - 2); k <= Math.min(allDates.length - 1, i + 2); k += 1) {
    if (k + 1 >= allDates.length) continue;
    const a = allDates[k]; const b = allDates[k + 1];
    if (a.slice(0, 7) !== b.slice(0, 7)) {
      // a is last trading day of its month
      // window is [k-2, k+2]
      if (i >= k - 2 && i <= k + 2) return true;
    }
  }
  return false;
}

function statsOf(returns) {
  if (returns.length === 0) return { n: 0, mean: 0, sharpe: 0, hit: 0 };
  const m = returns.reduce((a, b) => a + b, 0) / returns.length;
  const sd = Math.sqrt(returns.reduce((a, b) => a + ((b - m) ** 2), 0) / returns.length);
  const wins = returns.filter((v) => v > 0).length;
  return { n: returns.length, mean: m, sharpe: sd > 0 ? (m / sd) * Math.sqrt(252) : 0, hit: wins / returns.length };
}

function evalWindow(daily, allDates, dateFilter) {
  const out = { all: [], dow: { 1: [], 2: [], 3: [], 4: [], 5: [] }, tom: [], opex: [], thirdFri: [] };
  for (let i = 1; i < allDates.length; i += 1) {
    const d = allDates[i];
    if (dateFilter && !dateFilter(d)) continue;
    const prev = allDates[i - 1];
    const a = daily.get(prev); const b = daily.get(d);
    if (!a || !b || !Number.isFinite(a.close) || !Number.isFinite(b.close)) continue;
    const r = b.close / a.close - 1;
    out.all.push(r);
    out.dow[dayOfWeek(d)]?.push(r);
    if (isTurnOfMonth(d, allDates)) out.tom.push(r);
    if (isOpexWeek(d)) out.opex.push(r);
    if (isThirdFriday(d)) out.thirdFri.push(r);
  }
  const result = { all: statsOf(out.all) };
  for (const dow of [1, 2, 3, 4, 5]) result[`dow_${dow}`] = statsOf(out.dow[dow] || []);
  result.tom = statsOf(out.tom);
  result.opex = statsOf(out.opex);
  result.third_friday = statsOf(out.thirdFri);
  return result;
}

async function main() {
  process.stdout.write('Loading SPY daily bars...\n');
  const t0 = Date.now();
  const spyBars = await loadDailyBars('SPY', '2024-11-01', '2026-05-12');
  const allDates = [...spyBars.keys()].sort();
  process.stdout.write(`Loaded ${spyBars.size} SPY days in ${((Date.now()-t0)/1000).toFixed(1)}s\n`);

  const windows = {
    train_2025: { start: '2025-01-02', end: '2025-12-31' },
    test_2026: { start: '2026-01-02', end: '2026-05-12' },
    full_16mo: { start: '2025-01-02', end: '2026-05-12' },
  };
  const results = {};
  for (const [name, w] of Object.entries(windows)) {
    results[name] = evalWindow(spyBars, allDates, (d) => d >= w.start && d <= w.end);
  }

  // Print table
  const features = ['all', 'dow_1', 'dow_2', 'dow_3', 'dow_4', 'dow_5', 'tom', 'opex', 'third_friday'];
  const labels = { all: 'all-days', dow_1: 'Mon', dow_2: 'Tue', dow_3: 'Wed', dow_4: 'Thu', dow_5: 'Fri', tom: 'turn-of-month', opex: 'OPEX week', third_friday: '3rd Friday' };
  process.stdout.write('\nB3 — daily SPY return by calendar feature\n');
  process.stdout.write('  feature           train_2025                test_2026                 full_16mo\n');
  process.stdout.write('                    n   mean(bps) Sharpe hit%  n   mean(bps) Sharpe hit%  n   mean(bps) Sharpe hit%\n');
  for (const f of features) {
    const r25 = results.train_2025[f];
    const r26 = results.test_2026[f];
    const rf = results.full_16mo[f];
    process.stdout.write(
      '  ' + labels[f].padEnd(17) +
      ' ' + String(r25.n).padStart(3) + ' ' + (r25.mean * 10000).toFixed(1).padStart(8) +
      ' ' + r25.sharpe.toFixed(2).padStart(6) + ' ' + (r25.hit * 100).toFixed(1).padStart(5) +
      '  ' + String(r26.n).padStart(3) + ' ' + (r26.mean * 10000).toFixed(1).padStart(8) +
      ' ' + r26.sharpe.toFixed(2).padStart(6) + ' ' + (r26.hit * 100).toFixed(1).padStart(5) +
      '  ' + String(rf.n).padStart(3) + ' ' + (rf.mean * 10000).toFixed(1).padStart(8) +
      ' ' + rf.sharpe.toFixed(2).padStart(6) + ' ' + (rf.hit * 100).toFixed(1).padStart(5) + '\n',
    );
  }

  // -------- Multiplier table --------
  // For each calendar feature, compute a multiplier = (mean_full / mean_all_full),
  // but flag features that flip sign between train and test (= unreliable).
  process.stdout.write('\nSize-multiplier proposals (multiplier = full-window mean / all-days mean):\n');
  const multipliers = {};
  const baselineMean = results.full_16mo.all.mean || 1e-9;
  for (const f of features) {
    if (f === 'all') continue;
    const mult = results.full_16mo[f].mean / baselineMean;
    const signTrain = Math.sign(results.train_2025[f].mean || 0);
    const signTest = Math.sign(results.test_2026[f].mean || 0);
    const stable = signTrain !== 0 && signTrain === signTest;
    multipliers[f] = { mult, stable, train_mean: results.train_2025[f].mean, test_mean: results.test_2026[f].mean };
    process.stdout.write(`  ${labels[f].padEnd(17)} mult=${mult.toFixed(2).padStart(6)}  stable=${stable ? 'YES' : 'NO '}\n`);
  }

  // -------- Verdict --------
  // Overlay is "usable" if at least one calendar feature has |mult| > 1.3 AND stable across train/test.
  const usable = Object.entries(multipliers).filter(([, v]) => Math.abs(v.mult) > 1.3 && v.stable);
  const verdict = usable.length === 0
    ? 'reject (no calendar feature is both meaningful and stable)'
    : `marginal — usable overlays: ${usable.map(([k]) => labels[k]).join(', ')}`;
  process.stdout.write(`\nVerdict: ${verdict}\n`);

  const out = { generated_at: new Date().toISOString(), results, multipliers, verdict };
  const outPath = path.join(PROJECT_ROOT, 'artifacts', 'research', 'b3-seasonality-overlay.json');
  fs.mkdirSync(path.dirname(outPath), { recursive: true });
  fs.writeFileSync(outPath, JSON.stringify(out, null, 2));
  process.stdout.write(`\nWritten ${outPath}\n`);
}

main().catch((err) => { process.stderr.write(`${err.stack || err.message}\n`); process.exit(1); });
