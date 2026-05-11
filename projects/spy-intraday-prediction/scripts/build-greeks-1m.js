#!/usr/bin/env node
// Build per-contract per-minute greeks for the specified date range and option roots.
// Reads option_quotes_1m + underlying 1m bars from Massive, solves IV, writes JSONL.gz per day.
//
// Usage:
//   node scripts/build-greeks-1m.js --start 2026-01-02 --end 2026-01-30 --roots SPY
//   node scripts/build-greeks-1m.js --date 2026-01-02 --roots SPY,SPXW
//   node scripts/build-greeks-1m.js --start 2026-01-02 --end 2026-04-27 --roots SPY --concurrency 1

const path = require('node:path');
const fs = require('node:fs');

const { loadConfig, PROJECT_ROOT } = require('../src/config');
const { listCalendarDates } = require('../src/time');
const { buildGreeksForDay, defaultOutputPath } = require('../src/build-greeks-1m');

function parseArgs(argv) {
  const out = { roots: ['SPY'], force: false };
  for (let i = 0; i < argv.length; i += 1) {
    const a = argv[i];
    if (a === '--start') out.start = argv[++i];
    else if (a === '--end') out.end = argv[++i];
    else if (a === '--date') out.date = argv[++i];
    else if (a === '--roots') out.roots = argv[++i].split(',').map((s) => s.trim().toUpperCase());
    else if (a === '--risk-free') out.riskFree = Number(argv[++i]);
    else if (a === '--div-yield') out.divYield = Number(argv[++i]);
    else if (a === '--force') out.force = true;
    else if (a === '--help' || a === '-h') out.help = true;
  }
  if (out.date) {
    out.start = out.date;
    out.end = out.date;
  }
  return out;
}

function isHoliday(dayIso) {
  const d = new Date(`${dayIso}T00:00:00.000Z`);
  const dow = d.getUTCDay();
  return dow === 0 || dow === 6;
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (args.help || !args.start || !args.end) {
    process.stderr.write('Usage: build-greeks-1m.js --start YYYY-MM-DD --end YYYY-MM-DD [--roots SPY,SPXW] [--force]\n');
    process.exit(args.help ? 0 : 1);
  }
  const config = loadConfig();
  const dates = listCalendarDates(args.start, args.end);
  const results = [];
  for (const dayIso of dates) {
    if (isHoliday(dayIso)) continue;
    for (const root of args.roots) {
      const outputPath = defaultOutputPath(PROJECT_ROOT, root, dayIso);
      if (!args.force && fs.existsSync(outputPath) && fs.statSync(outputPath).size > 100) {
        process.stdout.write(`[skip] ${root} ${dayIso} (exists)\n`);
        continue;
      }
      const t0 = Date.now();
      try {
        // eslint-disable-next-line no-await-in-loop
        const stats = await buildGreeksForDay({
          config,
          dayIso,
          roots: [root],
          riskFree: Number.isFinite(args.riskFree) ? args.riskFree : undefined,
          divYield: Number.isFinite(args.divYield) ? args.divYield : undefined,
          outputPath,
        });
        const ms = Date.now() - t0;
        process.stdout.write(
          `[ok]   ${root} ${dayIso} seen=${stats.rowsSeen} kept=${stats.rowsKept} solved=${stats.rowsSolved} no_iv=${stats.rowsNoIv} no_spot=${stats.rowsNoSpot} low_liq=${stats.rowsLowLiquidity} ${ms}ms\n`,
        );
        results.push({ root, dayIso, stats, ms });
      } catch (err) {
        process.stderr.write(`[fail] ${root} ${dayIso} ${err.message}\n`);
      }
    }
  }
  process.stdout.write(`Done. ${results.length} day-root combos written.\n`);
}

main().catch((err) => {
  process.stderr.write(`Fatal: ${err.stack || err.message}\n`);
  process.exit(1);
});
