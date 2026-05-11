#!/usr/bin/env node
const path = require('node:path');
const fs = require('node:fs');

const { PROJECT_ROOT } = require('../src/config');
const { listCalendarDates } = require('../src/time');
const { buildAtmStraddleForDay, defaultStraddlePath } = require('../src/build-atm-straddle-1m');

function parseArgs(argv) {
  const o = { root: 'SPY', force: false };
  for (let i = 0; i < argv.length; i += 1) {
    const a = argv[i];
    if (a === '--start') o.start = argv[++i];
    else if (a === '--end') o.end = argv[++i];
    else if (a === '--date') o.date = argv[++i];
    else if (a === '--root') o.root = argv[++i].toUpperCase();
    else if (a === '--force') o.force = true;
  }
  if (o.date) { o.start = o.date; o.end = o.date; }
  return o;
}

function isWeekend(d) {
  const x = new Date(`${d}T00:00:00.000Z`).getUTCDay();
  return x === 0 || x === 6;
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const dates = listCalendarDates(args.start, args.end).filter((d) => !isWeekend(d));
  for (const day of dates) {
    const out = defaultStraddlePath(PROJECT_ROOT, args.root, day);
    if (!args.force && fs.existsSync(out) && fs.statSync(out).size > 100) {
      process.stdout.write(`[skip] ${args.root} ${day} (exists)\n`);
      continue;
    }
    const t0 = Date.now();
    try {
      // eslint-disable-next-line no-await-in-loop
      const r = await buildAtmStraddleForDay({ projectRoot: PROJECT_ROOT, root: args.root, dayIso: day, outputPath: out });
      process.stdout.write(`[ok]   ${args.root} ${day} rows_0dte=${r.rows0} rows_1dte=${r.rows1} ${Date.now()-t0}ms\n`);
    } catch (err) {
      process.stderr.write(`[fail] ${args.root} ${day} ${err.message}\n`);
    }
  }
}

main().catch((err) => { process.stderr.write(`${err.stack || err.message}\n`); process.exit(1); });
