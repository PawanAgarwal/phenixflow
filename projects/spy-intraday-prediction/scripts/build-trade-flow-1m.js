#!/usr/bin/env node
// Build per-minute aggressor-classified option flow + notable trade feed.
// Inputs:
//   - option_trades_all (raw OPRA prints)
//   - option_quotes_1m (1m bar high/low for bar-position rule)
//   - greeks-1m output (Phase 1) for delta/gamma/vanna at trade time
//
// Outputs (per day):
//   - runtime/trade-flow-1m/{ROOT}/date={YYYY-MM-DD}/{date}.flow.jsonl.gz  → 1m aggregated flow
//   - runtime/trade-flow-1m/{ROOT}/date={YYYY-MM-DD}/{date}.notable.jsonl.gz → per-trade enriched (sweeps/blocks/large)

const path = require('node:path');
const fs = require('node:fs');

const { loadConfig, PROJECT_ROOT } = require('../src/config');
const { listCalendarDates } = require('../src/time');
const { buildTradeFlowForDay, defaultFlowPaths } = require('../src/build-trade-flow-1m');
const { defaultOutputPath: defaultGreeksPath } = require('../src/build-greeks-1m');

function parseArgs(argv) {
  const out = { root: 'SPY', force: false };
  for (let i = 0; i < argv.length; i += 1) {
    const a = argv[i];
    if (a === '--start') out.start = argv[++i];
    else if (a === '--end') out.end = argv[++i];
    else if (a === '--date') out.date = argv[++i];
    else if (a === '--root') out.root = argv[++i].toUpperCase();
    else if (a === '--force') out.force = true;
    else if (a === '--help' || a === '-h') out.help = true;
  }
  if (out.date) {
    out.start = out.date;
    out.end = out.date;
  }
  return out;
}

function isWeekend(dayIso) {
  const d = new Date(`${dayIso}T00:00:00.000Z`);
  const dow = d.getUTCDay();
  return dow === 0 || dow === 6;
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (args.help || !args.start || !args.end) {
    process.stderr.write('Usage: build-trade-flow-1m.js --start YYYY-MM-DD --end YYYY-MM-DD [--root SPY] [--force]\n');
    process.exit(args.help ? 0 : 1);
  }
  const config = loadConfig();
  const dates = listCalendarDates(args.start, args.end);
  for (const dayIso of dates) {
    if (isWeekend(dayIso)) continue;
    const greeksPath = defaultGreeksPath(PROJECT_ROOT, args.root, dayIso);
    if (!fs.existsSync(greeksPath)) {
      process.stderr.write(`[skip] ${args.root} ${dayIso} (no greeks at ${greeksPath})\n`);
      continue;
    }
    const { flow: outputFlowPath, notable: outputNotablePath } = defaultFlowPaths(PROJECT_ROOT, args.root, dayIso);
    if (!args.force && fs.existsSync(outputFlowPath) && fs.statSync(outputFlowPath).size > 100) {
      process.stdout.write(`[skip] ${args.root} ${dayIso} (flow exists)\n`);
      continue;
    }
    const t0 = Date.now();
    try {
      // eslint-disable-next-line no-await-in-loop
      const stats = await buildTradeFlowForDay({
        config,
        dayIso,
        root: args.root,
        greeksPath,
        outputFlowPath,
        outputNotablePath,
      });
      const ms = Date.now() - t0;
      process.stdout.write(
        `[ok]   ${args.root} ${dayIso} seen=${stats.rowsSeen} kept=${stats.rowsKept} bar=${stats.sideBar} tick=${stats.sideTick} unk=${stats.sideUnk} sweeps=${stats.sweepClusters}(${stats.sweepTrades}) blocks=${stats.blocks} notable=${stats.notableEmitted} ${ms}ms\n`,
      );
    } catch (err) {
      process.stderr.write(`[fail] ${args.root} ${dayIso} ${err.message}\n`);
    }
  }
}

main().catch((err) => {
  process.stderr.write(`Fatal: ${err.stack || err.message}\n`);
  process.exit(1);
});
