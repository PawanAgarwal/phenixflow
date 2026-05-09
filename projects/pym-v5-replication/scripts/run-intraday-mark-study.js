#!/usr/bin/env node
const fs = require('node:fs');

const { loadConfig, artifactPath, ensureDir } = require('../src/config');
const { resolveEndDate } = require('../src/calendar');
const {
  DEFAULT_SEGMENTS,
  DEFAULT_UNIVERSES,
  runIntradayMarkStudy,
} = require('../src/intraday-mark-study');

function parseArgs(argv) {
  const out = {};
  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    if (arg === '--start') out.startDate = argv[++index];
    else if (arg === '--end') out.endDate = argv[++index];
    else if (arg === '--cost-bps') out.costBps = Number(argv[++index]);
    else if (arg === '--daily-bars') out.dailyBarsPath = argv[++index];
    else if (arg === '--score') out.scorePath = argv[++index];
    else if (arg === '--rsi-mode') out.rsiMode = argv[++index];
    else if (arg === '--segments') out.segmentIds = argv[++index].split(',').map((value) => value.trim()).filter(Boolean);
    else if (arg === '--universes') out.universeIds = argv[++index].split(',').map((value) => value.trim()).filter(Boolean);
    else if (arg === '--label') out.label = argv[++index];
  }
  return out;
}

function selectById(all, selectedIds, label) {
  if (!selectedIds?.length || selectedIds.includes('all')) return all;
  const selected = all.filter((item) => selectedIds.includes(item.id));
  const missing = selectedIds.filter((id) => !all.some((item) => item.id === id));
  if (missing.length) throw new Error(`unknown_${label}:${missing.join(',')}`);
  return selected;
}

function pct(value) {
  return Number.isFinite(value) ? `${value.toFixed(2)}%` : null;
}

function bps(value) {
  return Number.isFinite(value) ? Number(value.toFixed(2)) : null;
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const config = loadConfig();
  const startDate = args.startDate || '2025-01-02';
  const endDate = resolveEndDate(config, args.endDate || config.windows.endDate);
  const result = await runIntradayMarkStudy({
    config,
    startDate,
    endDate,
    costBps: Number.isFinite(args.costBps) ? args.costBps : 4,
    dailyBarsPath: args.dailyBarsPath,
    scorePath: args.scorePath,
    rsiMode: args.rsiMode || 'wilder',
    segments: selectById(DEFAULT_SEGMENTS, args.segmentIds, 'segments'),
    universes: selectById(DEFAULT_UNIVERSES, args.universeIds, 'universes'),
    onProgress: ({ day, processedDays }) => {
      if (processedDays % 20 === 0) console.log(`processed ${processedDays} mark-study days through ${day.date}`);
    },
  });
  const labelPart = args.label ? `${args.label}-` : '';
  const outPath = artifactPath(`pym-v5-intraday-mark-study-${labelPart}${startDate}-${endDate}.json`);
  ensureDir(artifactPath());
  fs.writeFileSync(outPath, JSON.stringify(result, null, 2));

  const interestingIds = new Set([
    'close_to_0935',
    'close_to_1030',
    'close_to_1130',
    'close_to_1230',
    'close_to_1330',
    'close_to_1430',
    '0935_to_1030',
    '1030_to_1130',
    '1130_to_1230',
    '1230_to_1330',
    '1330_to_1430',
    '1430_to_1555',
    '0935_to_1430',
    '0935_to_1555',
    'close_to_1555',
  ]);
  console.log(JSON.stringify({
    startDate,
    endDate,
    costBps: result.settings.costBps,
    skippedDays: result.skippedDays.length,
    highlights: result.summaries
      .filter((summary) => interestingIds.has(summary.segmentId))
      .map((summary) => ({
        universe: summary.universeId,
        segment: summary.segmentId,
        observations: summary.observations,
        grossReturn: pct(summary.grossTotalReturnPct),
        netReturn: pct(summary.netTotalReturnPct),
        meanGrossBps: bps(summary.meanGrossBps),
        meanNetBps: bps(summary.meanNetBps),
        netSharpe: bps(summary.netSharpe),
        netMaxDrawdown: pct(summary.netMaxDrawdownPct),
        netWinRate: pct(summary.netWinRate * 100),
      })),
    outputPath: outPath,
  }, null, 2));
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
