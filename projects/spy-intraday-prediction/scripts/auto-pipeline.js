#!/usr/bin/env node
// Auto-run trade-flow + features for a list of roots, once greeks exist for each day.
// Skips days where downstream output already exists (idempotent).

const { spawnSync } = require('node:child_process');
const fs = require('node:fs');
const path = require('node:path');

const { PROJECT_ROOT } = require('../src/config');
const { defaultOutputPath: defaultGreeksPath } = require('../src/build-greeks-1m');
const { defaultFlowPaths } = require('../src/build-trade-flow-1m');
const { defaultFeaturesPath } = require('../src/build-features-1m');

function parseArgs(argv) {
  const o = { stages: 'flow,features' };
  for (let i = 0; i < argv.length; i += 1) {
    const a = argv[i];
    if (a === '--roots') o.roots = argv[++i].split(',');
    else if (a === '--start') o.start = argv[++i];
    else if (a === '--end') o.end = argv[++i];
    else if (a === '--stages') o.stages = argv[++i];
  }
  if (!o.roots || !o.start || !o.end) {
    process.stderr.write('Usage: auto-pipeline.js --roots SPY,QQQ,NVDA --start 2025-01-02 --end 2026-04-27 [--stages flow,features]\n');
    process.exit(1);
  }
  return o;
}

function runChild(cmd, args) {
  const r = spawnSync(cmd, args, { stdio: 'inherit', maxBuffer: 1 << 28 });
  return r.status === 0;
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

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const stages = new Set(args.stages.split(','));
  const dates = listDates(args.start, args.end);
  for (const root of args.roots) {
    process.stdout.write(`\n=== root ${root} ===\n`);
    if (stages.has('flow')) {
      // Identify days where greeks exist but flow doesn't
      const flowMissing = dates.filter((d) => {
        const greeksFile = defaultGreeksPath(PROJECT_ROOT, root, d);
        const { flow } = defaultFlowPaths(PROJECT_ROOT, root, d);
        return fs.existsSync(greeksFile) && (!fs.existsSync(flow) || fs.statSync(flow).size < 100);
      });
      if (flowMissing.length === 0) {
        process.stdout.write(`  flow: nothing to do\n`);
      } else {
        process.stdout.write(`  flow: ${flowMissing.length} days to process\n`);
        const startDate = flowMissing[0];
        const endDate = flowMissing[flowMissing.length - 1];
        runChild('node', ['--max-old-space-size=6144',
          path.join(PROJECT_ROOT, 'scripts', 'build-trade-flow-1m.js'),
          '--start', startDate, '--end', endDate, '--root', root]);
      }
    }
    if (stages.has('features')) {
      const featuresMissing = dates.filter((d) => {
        const flowFile = defaultFlowPaths(PROJECT_ROOT, root, d).flow;
        const feat = defaultFeaturesPath(PROJECT_ROOT, root, d);
        return fs.existsSync(flowFile) && (!fs.existsSync(feat) || fs.statSync(feat).size < 100);
      });
      if (featuresMissing.length === 0) {
        process.stdout.write(`  features: nothing to do\n`);
      } else {
        process.stdout.write(`  features: ${featuresMissing.length} days to process\n`);
        const startDate = featuresMissing[0];
        const endDate = featuresMissing[featuresMissing.length - 1];
        runChild('node', ['--max-old-space-size=4096',
          path.join(PROJECT_ROOT, 'scripts', 'build-features-1m.js'),
          '--start', startDate, '--end', endDate, '--root', root]);
      }
    }
  }
}

main().catch((err) => { process.stderr.write(`${err.stack || err.message}\n`); process.exit(1); });
