#!/usr/bin/env node
const fs = require('node:fs');
const path = require('node:path');
const readline = require('node:readline');

const { PROJECT_ROOT, ensureDir, loadConfig } = require('../src/config');
const { parseArgs } = require('../src/cli');

function defaultInputPath(config) {
  return path.join(
    PROJECT_ROOT,
    'runtime',
    `spy-intraday-dataset-${config.windows.sensitivityTrain?.startDate || config.windows.train.startDate}-${config.dataPolicy.historicalCutoffDate}-with-option-features.jsonl`,
  );
}

function defaultOutputPath(config) {
  return path.join(
    PROJECT_ROOT,
    'runtime',
    `spy-intraday-python-compact-${config.windows.sensitivityTrain?.startDate || config.windows.train.startDate}-${config.dataPolicy.historicalCutoffDate}.jsonl`,
  );
}

function keepColumn(column) {
  if ([
    'rowId',
    'tradeDate',
    'minuteUtc',
    'minuteMs',
    'minuteOfDayEt',
    'minutes_from_open',
    'minutes_to_close',
    'spy_close',
  ].includes(column)) return true;
  if (column.startsWith('label_next_5m_')) return true;
  if (column.startsWith('label_next_60m_')) return true;
  if (column.startsWith('label_eod_close_')) return true;
  if (column.startsWith('label_last_30m_')) return true;
  if (column.startsWith('label_abs_return_')) return true;
  if (column.includes('_ret_')) return true;
  if (column.includes('_breadth_')) return true;
  if (column.includes('_rel_spy_')) return true;
  if (column.includes('_volume_log')) return true;
  if (column.startsWith('spy_rv_')) return true;
  if (column.startsWith('vix')) return true;
  if (column.startsWith('gamma_proxy_')) return true;
  if (column.startsWith('opening_')) return true;
  if (column.startsWith('opt_spy_')) return true;
  if (column.startsWith('opt_spx_')) return true;
  return false;
}

function compactRow(row) {
  const out = {};
  Object.entries(row).forEach(([key, value]) => {
    if (keepColumn(key)) out[key] = value;
  });
  return out;
}

async function exportCompact(inputPath, outputPath) {
  ensureDir(path.dirname(outputPath));
  const reader = readline.createInterface({
    input: fs.createReadStream(inputPath, { encoding: 'utf8' }),
    crlfDelay: Infinity,
  });
  const writer = fs.createWriteStream(outputPath, { encoding: 'utf8' });
  let rowCount = 0;
  let keptColumnCount = null;
  for await (const line of reader) {
    if (!line.trim()) continue;
    const compact = compactRow(JSON.parse(line));
    if (keptColumnCount === null) keptColumnCount = Object.keys(compact).length;
    writer.write(`${JSON.stringify(compact)}\n`);
    rowCount += 1;
    if (rowCount % 25_000 === 0) {
      process.stderr.write(`[spy-intraday-python-export] rows=${rowCount}\n`);
    }
  }
  await new Promise((resolve, reject) => {
    writer.end(resolve);
    writer.on('error', reject);
  });
  return { inputPath, outputPath, rowCount, keptColumnCount };
}

async function main() {
  const args = parseArgs();
  const config = loadConfig(args.config);
  const inputPath = path.resolve(args.input || args.dataset || defaultInputPath(config));
  const outputPath = path.resolve(args.output || defaultOutputPath(config));
  const result = await exportCompact(inputPath, outputPath);
  console.log(JSON.stringify(result, null, 2));
}

if (require.main === module) {
  main().catch((error) => {
    console.error(error.stack || error.message);
    process.exitCode = 1;
  });
}

module.exports = {
  compactRow,
  exportCompact,
  keepColumn,
};
