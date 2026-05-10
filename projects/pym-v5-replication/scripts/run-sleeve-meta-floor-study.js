#!/usr/bin/env node
const fs = require('node:fs');
const path = require('node:path');

const { artifactPath, ensureDir, loadConfig } = require('../src/config');
const { defaultScorePath, findLatestMassiveEodBarsPath } = require('../src/rebalance-report');
const {
  runExtensionStrategiesSuite,
} = require('../src/extension-strategies-suite');
const {
  // re-export references to the strategy factories
} = require('../src/extension-strategies-suite');

const {
  // these aren't exported by name but defaultStrategies pulls them; build inline list
} = require('../src/extension-strategies-suite');

const suite = require('../src/extension-strategies-suite');

function buildFloorStudyStrategies(lookback) {
  const list = [];
  list.push({
    id: 'base_pym',
    family: 'baseline',
    name: 'Base PYM V5',
    fn: (ctx) => ctx.baseWeights,
  });
  // Floor sweep
  [0, 0.0125, 0.025, 0.0375, 0.05, 0.0625, 0.075, 0.1, 0.125].forEach((floor) => {
    list.push(suite.strategySleeveMeta({ lookback, floor }));
  });
  // Single-sleeve cap variants (no floor)
  [0.25, 0.30, 0.35, 0.40, 0.50].forEach((maxWeight) => {
    list.push(suite.strategySleeveMetaCap({ lookback, maxWeight }));
  });
  // Dispersion-aware floor
  [{ minFloor: 0, maxFloor: 0.125 }, { minFloor: 0, maxFloor: 0.075 }, { minFloor: 0.025, maxFloor: 0.075 }].forEach((opts) => {
    list.push(suite.strategySleeveMetaDispersion({ lookback, ...opts }));
  });
  // Auto-floor walk-forward selector
  [21, 42, 63, 126].forEach((autoLookback) => {
    list.push(suite.strategySleeveMetaAutoFloor({ lookback, autoLookback }));
  });
  return list;
}

function parseArgs(argv) {
  const out = {};
  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    if (arg === '--start') out.startDate = argv[++i];
    else if (arg === '--end') out.endDate = argv[++i];
    else if (arg === '--lookback') out.lookback = Number(argv[++i]);
    else if (arg === '--cost-bps') out.costBps = Number(argv[++i]);
    else if (arg === '--label') out.label = argv[++i];
    else if (arg === '--out') out.outPath = argv[++i];
  }
  return out;
}

function main() {
  const args = parseArgs(process.argv.slice(2));
  const config = loadConfig();
  const lookback = Number.isFinite(args.lookback) ? args.lookback : 21;
  const startDate = args.startDate || '2025-01-02';
  const endDate = args.endDate || null;
  const costBps = Number.isFinite(args.costBps) ? args.costBps : 2;
  const label = args.label || `lookback${lookback}-${startDate}-${endDate || 'auto'}`;
  const strategies = buildFloorStudyStrategies(lookback);
  const report = runExtensionStrategiesSuite({
    primaryDailyBarsPath: findLatestMassiveEodBarsPath(),
    extraDailyBarsPath: null,
    scorePath: defaultScorePath(config),
    startDate,
    endDate,
    costBps,
    strategies,
  });
  const outPath = args.outPath || artifactPath(`pym-v5-sleeve-meta-floor-study-${label}.json`);
  ensureDir(path.dirname(outPath));
  fs.writeFileSync(outPath, JSON.stringify(report, null, 2));
  console.log('wrote', outPath);
}

main();
