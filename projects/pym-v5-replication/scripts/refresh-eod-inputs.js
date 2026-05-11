#!/usr/bin/env node
const fs = require('node:fs');
const path = require('node:path');
const { spawnSync } = require('node:child_process');

const { readDailyBarsJsonl } = require('../src/backtest');
const { openCalendarDays, resolveEndDate } = require('../src/calendar');
const { ensureDir, loadConfig, runtimePath } = require('../src/config');
const { loadMassiveEnv } = require('../src/env');
const { buildOptionFeatureFile, defaultOptionRoots } = require('../src/option-features');
const { defaultScorePath, findLatestMassiveEodBarsPath } = require('../src/rebalance-report');

const REPO_ROOT = path.resolve(__dirname, '..', '..', '..');
const DEFAULT_EOD_FETCH_START = '2024-01-01';
const DEFAULT_OPTION_START = '2025-01-02';
const DEFAULT_STRESS_START = '2016-01-01';

function parseArgs(argv) {
  const out = {};
  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    if (arg === '--end') out.endDate = argv[++index];
    else if (arg === '--fetch-start') out.fetchStartDate = argv[++index];
    else if (arg === '--option-start') out.optionStartDate = argv[++index];
    else if (arg === '--score') out.scorePath = argv[++index];
    else if (arg === '--daily-bars') out.dailyBarsPath = argv[++index];
    else if (arg === '--with-option-features') out.withOptionFeatures = true;
    else if (arg === '--with-stress-signal') out.withStressSignal = true;
    else if (arg === '--force') out.force = true;
    else if (arg === '--concurrency') out.concurrency = argv[++index];
  }
  return out;
}

function parseRange(name, prefix, extension) {
  const escaped = prefix.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  const match = String(name || '').match(new RegExp(`^${escaped}-(\\d{4}-\\d{2}-\\d{2})-(\\d{4}-\\d{2}-\\d{2})\\.${extension}$`));
  return match ? { startDate: match[1], endDate: match[2] } : null;
}

function sortedRuntimeMatches(prefix, extension) {
  const root = runtimePath();
  if (!fs.existsSync(root)) return [];
  return fs.readdirSync(root)
    .map((name) => {
      const range = parseRange(name, prefix, extension);
      return range ? { ...range, name, path: path.join(root, name) } : null;
    })
    .filter(Boolean)
    .sort((left, right) => (
      left.endDate.localeCompare(right.endDate)
      || left.startDate.localeCompare(right.startDate)
      || left.name.localeCompare(right.name)
    ));
}

function findBestEodBars(fetchStartDate, endDate) {
  const matches = sortedRuntimeMatches('pym-v5-massive-eod-adjusted-daily-bars', 'jsonl')
    .filter((item) => item.startDate === fetchStartDate && item.endDate >= endDate);
  return matches.at(-1) || null;
}

function findLatestEodBars(fetchStartDate) {
  const matches = sortedRuntimeMatches('pym-v5-massive-eod-adjusted-daily-bars', 'jsonl')
    .filter((item) => !fetchStartDate || item.startDate === fetchStartDate);
  return matches.at(-1) || null;
}

function resolveRefreshEndDate(config, args) {
  const requested = args.endDate || config.windows.endDate;
  if (requested && requested !== 'auto') return resolveEndDate(config, requested);
  try {
    return resolveEndDate(config, requested);
  } catch (error) {
    const fetchStartDate = args.fetchStartDate || process.env.PYM_V5_EOD_FETCH_START || DEFAULT_EOD_FETCH_START;
    const latestBars = findLatestEodBars(fetchStartDate) || findLatestEodBars();
    if (latestBars?.endDate) {
      console.error(`[refresh-eod-inputs] ${error.message}; falling back to latest existing EOD bars ${latestBars.endDate}`);
      return latestBars.endDate;
    }
    throw error;
  }
}

function findBestOptionFeatures(startDate, endDate) {
  const matches = sortedRuntimeMatches('pym-v5-option-bar-features', 'jsonl')
    .filter((item) => item.startDate === startDate && item.endDate >= endDate);
  return matches.at(-1) || null;
}

function findLatestOptionFeaturesForStart(startDate) {
  const matches = sortedRuntimeMatches('pym-v5-option-bar-features', 'jsonl')
    .filter((item) => item.startDate === startDate);
  return matches.at(-1) || null;
}

function findLatestStressSignal(endDate) {
  const root = path.join(REPO_ROOT, 'projects', 'pym-v5-ml-experiments', 'artifacts');
  if (!fs.existsSync(root)) return null;
  const matches = fs.readdirSync(root)
    .map((name) => {
      const range = parseRange(name, 'options-stress-signal', 'jsonl');
      return range ? { ...range, name, path: path.join(root, name) } : null;
    })
    .filter(Boolean)
    .sort((left, right) => (
      left.endDate.localeCompare(right.endDate)
      || left.startDate.localeCompare(right.startDate)
      || left.name.localeCompare(right.name)
    ));
  return matches.filter((item) => item.endDate >= endDate).at(-1) || null;
}

function runNodeStep(label, args) {
  console.log(`[refresh-eod-inputs] ${label}: node ${args.join(' ')}`);
  const result = spawnSync(process.execPath, args, {
    cwd: REPO_ROOT,
    env: process.env,
    stdio: 'inherit',
  });
  if (result.status !== 0) {
    throw new Error(`${label}: exit ${result.status}`);
  }
}

function readJsonIfExists(filePath) {
  if (!filePath || !fs.existsSync(filePath)) return null;
  return JSON.parse(fs.readFileSync(filePath, 'utf8'));
}

function readFeatureRows(filePath) {
  const rows = new Map();
  fs.readFileSync(filePath, 'utf8').split(/\r?\n/).forEach((line) => {
    if (!line.trim()) return;
    const row = JSON.parse(line);
    if (row.date) rows.set(row.date, row);
  });
  return rows;
}

function writeFeatureRows(filePath, rowsByDate) {
  const lines = [...rowsByDate.values()]
    .sort((left, right) => left.date.localeCompare(right.date))
    .map((row) => JSON.stringify(row));
  fs.writeFileSync(filePath, `${lines.join('\n')}\n`, 'utf8');
}

function mergeCoverage(left = [], right = []) {
  const byDate = new Map();
  [...left, ...right].forEach((item) => {
    if (item?.date) byDate.set(item.date, item);
  });
  return [...byDate.values()].sort((a, b) => a.date.localeCompare(b.date));
}

function mergeOptionFeatureFiles({ base, delta, startDate, endDate, outputPath }) {
  const rows = readFeatureRows(base.path);
  readFeatureRows(delta.outputPath).forEach((row, date) => rows.set(date, row));
  writeFeatureRows(outputPath, rows);

  const baseManifest = readJsonIfExists(base.path.replace(/\.jsonl$/, '.manifest.json')) || {};
  const deltaManifest = delta.manifest || {};
  const coverage = mergeCoverage(baseManifest.coverage, deltaManifest.coverage);
  const manifest = {
    generatedAt: new Date().toISOString(),
    source: deltaManifest.source || baseManifest.source || null,
    startDate,
    endDate,
    outputPath,
    selectedRoots: deltaManifest.selectedRoots || baseManifest.selectedRoots || [],
    processedDays: coverage.length || rows.size,
    totalRowsRead: (baseManifest.totalRowsRead || 0) + (deltaManifest.totalRowsRead || 0),
    totalRowsUsed: (baseManifest.totalRowsUsed || 0) + (deltaManifest.totalRowsUsed || 0),
    missingFileDays: coverage.filter((item) => item.missingFile).map((item) => item.date),
    coverage,
    incrementalFrom: {
      basePath: base.path,
      deltaPath: delta.outputPath,
    },
  };
  fs.writeFileSync(outputPath.replace(/\.jsonl$/, '.manifest.json'), `${JSON.stringify(manifest, null, 2)}\n`, 'utf8');
  return { outputPath, manifestPath: outputPath.replace(/\.jsonl$/, '.manifest.json'), manifest };
}

async function ensureEodBars({ args, config, endDate, steps }) {
  const fetchStartDate = args.fetchStartDate || process.env.PYM_V5_EOD_FETCH_START || DEFAULT_EOD_FETCH_START;
  if (!args.force) {
    const existing = args.dailyBarsPath
      ? { path: path.resolve(args.dailyBarsPath), startDate: null, endDate }
      : findBestEodBars(fetchStartDate, endDate);
    if (existing && fs.existsSync(existing.path)) {
      steps.push({ step: 'eod-bars', action: 'skip', reason: 'coverage_current', path: existing.path });
      return existing.path;
    }
  }

  const scriptPath = path.join(REPO_ROOT, 'projects', 'pym-v5-replication', 'scripts', 'build-massive-eod-daily-bars.js');
  const stepArgs = [scriptPath, '--fetch-start', fetchStartDate, '--end', endDate];
  if (args.concurrency) stepArgs.push('--concurrency', args.concurrency);
  runNodeStep('build-massive-eod-bars', stepArgs);
  const builtPath = runtimePath(`pym-v5-massive-eod-adjusted-daily-bars-${fetchStartDate}-${endDate}.jsonl`);
  if (!fs.existsSync(builtPath)) throw new Error(`expected_eod_bars_missing:${builtPath}`);
  steps.push({ step: 'eod-bars', action: 'build', path: builtPath });
  return builtPath;
}

async function ensureOptionFeatures({ args, config, scorePath, barsPath, endDate, steps }) {
  if (!args.withOptionFeatures) return null;
  const startDate = args.optionStartDate || process.env.PYM_V5_OPTION_FEATURES_START || DEFAULT_OPTION_START;
  if (!args.force) {
    const current = findBestOptionFeatures(startDate, endDate);
    if (current) {
      steps.push({ step: 'option-features', action: 'skip', reason: 'coverage_current', path: current.path });
      return current.path;
    }
  }

  const market = readDailyBarsJsonl(barsPath || findLatestMassiveEodBarsPath());
  const score = JSON.parse(fs.readFileSync(scorePath, 'utf8'));
  const roots = defaultOptionRoots(score, market);
  const outputPath = runtimePath(`pym-v5-option-bar-features-${startDate}-${endDate}.jsonl`);
  const base = args.force ? null : findLatestOptionFeaturesForStart(startDate);
  const firstMissingDate = base ? base.endDate : startDate;
  const days = openCalendarDays(config.roots.calendar, startDate, endDate)
    .filter((day) => market.dates.includes(day.date))
    .filter((day) => !base || day.date > firstMissingDate);

  if (base && days.length) {
    const deltaStart = days[0].date;
    const deltaPath = runtimePath(`pym-v5-option-bar-features-delta-${deltaStart}-${endDate}-${process.pid}.jsonl`);
    const delta = await buildOptionFeatureFile({
      config,
      market,
      score,
      days,
      startDate: deltaStart,
      endDate,
      roots,
      outputPath: deltaPath,
      onProgress: ({ day, processedDays, result }) => {
        console.error(JSON.stringify({
          mode: 'incremental_option_features',
          processedDays,
          date: day.date,
          rowsRead: result.rowsRead,
          rowsUsed: result.rowsUsed,
          activeRoots: Object.keys(result.roots).length,
          missingFile: result.missingFile,
        }));
      },
    });
    const merged = mergeOptionFeatureFiles({ base, delta, startDate, endDate, outputPath });
    [delta.outputPath, delta.manifestPath].forEach((filePath) => {
      if (fs.existsSync(filePath)) fs.unlinkSync(filePath);
    });
    steps.push({
      step: 'option-features',
      action: 'incremental_build',
      basePath: base.path,
      addedDays: days.length,
      path: merged.outputPath,
    });
    return merged.outputPath;
  }

  const fullDays = openCalendarDays(config.roots.calendar, startDate, endDate)
    .filter((day) => market.dates.includes(day.date));
  const result = await buildOptionFeatureFile({
    config,
    market,
    score,
    days: fullDays,
    startDate,
    endDate,
    roots,
    outputPath,
    onProgress: ({ day, processedDays, result: dayResult }) => {
      if (processedDays === 1 || processedDays % 20 === 0 || day.date === endDate) {
        console.error(JSON.stringify({
          mode: 'full_option_features',
          processedDays,
          date: day.date,
          rowsRead: dayResult.rowsRead,
          rowsUsed: dayResult.rowsUsed,
          activeRoots: Object.keys(dayResult.roots).length,
          missingFile: dayResult.missingFile,
        }));
      }
    },
  });
  steps.push({ step: 'option-features', action: 'full_build', days: fullDays.length, path: result.outputPath });
  return result.outputPath;
}

function ensureStressSignal({ args, barsPath, endDate, steps }) {
  if (!args.withStressSignal) return null;
  if (!args.force) {
    const existing = findLatestStressSignal(endDate);
    if (existing) {
      steps.push({ step: 'stress-signal', action: 'skip', reason: 'coverage_current', path: existing.path });
      return existing.path;
    }
  }
  const scriptPath = path.join(REPO_ROOT, 'projects', 'pym-v5-ml-experiments', 'scripts', 'build-options-stress-signal.js');
  runNodeStep('build-stress-signal', [scriptPath, '--start', DEFAULT_STRESS_START, '--end', endDate, '--bars', barsPath]);
  const outPath = path.join(
    REPO_ROOT,
    'projects',
    'pym-v5-ml-experiments',
    'artifacts',
    `options-stress-signal-${DEFAULT_STRESS_START}-${endDate}.jsonl`,
  );
  steps.push({ step: 'stress-signal', action: 'build', path: outPath });
  return outPath;
}

async function main() {
  loadMassiveEnv();
  const args = parseArgs(process.argv.slice(2));
  const config = loadConfig();
  const endDate = resolveRefreshEndDate(config, args);
  const scorePath = args.scorePath || defaultScorePath(config);
  if (!fs.existsSync(scorePath)) throw new Error(`missing_score_snapshot:${scorePath}`);
  ensureDir(runtimePath());

  const steps = [];
  const barsPath = await ensureEodBars({ args, config, endDate, steps });
  const optionFeaturesPath = await ensureOptionFeatures({ args, config, scorePath, barsPath, endDate, steps });
  const stressSignalPath = ensureStressSignal({ args, barsPath, endDate, steps });

  console.log(JSON.stringify({
    endDate,
    barsPath,
    optionFeaturesPath,
    stressSignalPath,
    steps,
  }, null, 2));
}

main().catch((error) => {
  console.error(error.stack || error.message);
  process.exit(1);
});
