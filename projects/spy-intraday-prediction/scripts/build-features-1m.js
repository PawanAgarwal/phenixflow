#!/usr/bin/env node
// Combine Phase 1 (greeks) + Phase 2 (trade-flow) + SPY/VIX 1m bars + OCC EOD overlay
// into a per-minute feature dataset ready for backtests.
//
// Output: runtime/features-1m/{ROOT}/date={YYYY-MM-DD}/{date}.jsonl.gz

const path = require('node:path');
const fs = require('node:fs');

const { loadConfig, PROJECT_ROOT } = require('../src/config');
const { listCalendarDates } = require('../src/time');
const {
  buildFeaturesForDay, defaultFeaturesPath, buildOccOverlay, loadStockBars, loadIndexBars,
} = require('../src/build-features-1m');
const { defaultFlowPaths } = require('../src/build-trade-flow-1m');

const DEFAULT_OCC_ROOT = '/Volumes/SEC4TB/massive-data/occ/option_open_interest_eod';

function parseArgs(argv) {
  const out = { root: 'SPY', force: false };
  for (let i = 0; i < argv.length; i += 1) {
    const a = argv[i];
    if (a === '--start') out.start = argv[++i];
    else if (a === '--end') out.end = argv[++i];
    else if (a === '--date') out.date = argv[++i];
    else if (a === '--root') out.root = argv[++i].toUpperCase();
    else if (a === '--underlying') out.underlying = argv[++i];
    else if (a === '--occ-root') out.occRoot = argv[++i];
    else if (a === '--force') out.force = true;
  }
  if (out.date) { out.start = out.date; out.end = out.date; }
  return out;
}

function isWeekend(dayIso) {
  const d = new Date(`${dayIso}T00:00:00.000Z`);
  const dow = d.getUTCDay();
  return dow === 0 || dow === 6;
}

async function previousClose(config, dayIso, symbol) {
  // Walk back up to 7 calendar days for previous close, routing I:* to indices.
  for (let back = 1; back <= 7; back += 1) {
    const d = new Date(`${dayIso}T00:00:00.000Z`);
    d.setUTCDate(d.getUTCDate() - back);
    const prev = d.toISOString().slice(0, 10);
    if (isWeekend(prev)) continue;
    // eslint-disable-next-line no-await-in-loop
    const bars = symbol.startsWith('I:')
      ? await loadIndexBars(config, prev, symbol)
      : await loadStockBars(config, prev, symbol);
    if (bars.size > 0) {
      const sorted = Array.from(bars.entries()).sort((a, b) => b[0] - a[0]);
      return sorted[0][1].close;
    }
  }
  return null;
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (!args.start || !args.end) {
    process.stderr.write('Usage: build-features-1m.js --start YYYY-MM-DD --end YYYY-MM-DD [--root SPY]\n');
    process.exit(1);
  }
  const config = loadConfig();
  const occRoot = args.occRoot || DEFAULT_OCC_ROOT;
  const allDates = listCalendarDates(args.start, args.end).filter((d) => !isWeekend(d));
  const overlay = await buildOccOverlay({ occRoot, dates: allDates });
  for (const dayIso of allDates) {
    const { flow: flowPath } = defaultFlowPaths(PROJECT_ROOT, args.root, dayIso);
    if (!fs.existsSync(flowPath)) {
      process.stderr.write(`[skip] ${args.root} ${dayIso} (no flow)\n`);
      continue;
    }
    const outputPath = defaultFeaturesPath(PROJECT_ROOT, args.root, dayIso);
    if (!args.force && fs.existsSync(outputPath) && fs.statSync(outputPath).size > 100) {
      process.stdout.write(`[skip] ${args.root} ${dayIso} (features exist)\n`);
      continue;
    }
    const t0 = Date.now();
    try {
      // eslint-disable-next-line no-await-in-loop
      // eslint-disable-next-line no-await-in-loop
      const underlying = args.underlying || args.root;
      const prevClose = await previousClose(config, dayIso, underlying);
      // eslint-disable-next-line no-await-in-loop
      const result = await buildFeaturesForDay({
        config,
        dayIso,
        occOverlayDay: overlay.get(dayIso) || null,
        flowPath,
        outputPath,
        prevDayClose: prevClose,
        underlyingSymbol: underlying,
      });
      const ms = Date.now() - t0;
      process.stdout.write(`[ok]   ${args.root} ${dayIso} rows=${result.rows} ${ms}ms\n`);
    } catch (err) {
      process.stderr.write(`[fail] ${args.root} ${dayIso} ${err.message}\n`);
    }
  }
}

main().catch((err) => {
  process.stderr.write(`Fatal: ${err.stack || err.message}\n`);
  process.exit(1);
});
