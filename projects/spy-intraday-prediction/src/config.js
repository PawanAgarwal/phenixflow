const fs = require('node:fs');
const path = require('node:path');

const PROJECT_ROOT = path.resolve(__dirname, '..');
const DEFAULT_CONFIG_PATH = path.join(PROJECT_ROOT, 'config', 'universe.json');

function loadConfig(configPath = DEFAULT_CONFIG_PATH) {
  return JSON.parse(fs.readFileSync(configPath, 'utf8'));
}

function datasetDateDir(config, datasetKey, dayIso) {
  const datasetId = config.datasets[datasetKey] || datasetKey;
  return path.join(config.roots.historical, datasetId, `date=${dayIso}`);
}

function datasetCsvPath(config, datasetKey, dayIso) {
  return path.join(datasetDateDir(config, datasetKey, dayIso), `${dayIso}.csv.gz`);
}

function datasetParquetPath(config, datasetKey, dayIso) {
  if (!config.roots.liveParquet) return null;
  const datasetId = config.datasets[datasetKey] || datasetKey;
  return path.join(config.roots.liveParquet, datasetId, `date=${dayIso}`, `${dayIso}.live.parquet`);
}

function resolveDatasetSource(config, datasetKey, dayIso) {
  const csvPath = datasetCsvPath(config, datasetKey, dayIso);
  if (fs.existsSync(csvPath)) {
    return { format: 'csv.gz', filePath: csvPath, preferredFilePath: csvPath };
  }
  const parquetPath = datasetParquetPath(config, datasetKey, dayIso);
  if (parquetPath && fs.existsSync(parquetPath)) {
    return { format: 'parquet', filePath: parquetPath, preferredFilePath: parquetPath };
  }
  return {
    format: 'missing',
    filePath: parquetPath || csvPath,
    preferredFilePath: parquetPath || csvPath,
    fallbackFilePath: csvPath,
  };
}

function latestDatasetDate(root, datasetId) {
  const datasetRoot = path.join(root, datasetId);
  if (!fs.existsSync(datasetRoot)) return null;
  const dates = fs.readdirSync(datasetRoot)
    .filter((entry) => entry.startsWith('date='))
    .map((entry) => entry.slice('date='.length))
    .sort();
  return dates.at(-1) || null;
}

function latestDatasetDateAcrossRoots(roots, datasetId) {
  const dates = roots
    .filter(Boolean)
    .map((root) => latestDatasetDate(root, datasetId))
    .filter(Boolean)
    .sort();
  return dates.at(-1) || null;
}

function resolveEndDate(config, requestedEndDate) {
  if (requestedEndDate && requestedEndDate !== 'auto') return requestedEndDate;
  const stockLatest = latestDatasetDateAcrossRoots(
    [config.roots.historical, config.roots.liveParquet],
    config.datasets.stockBars,
  );
  const optionLatest = latestDatasetDateAcrossRoots(
    [config.roots.historical, config.roots.liveParquet],
    config.datasets.optionBars,
  );
  const latest = [stockLatest, optionLatest].filter(Boolean).sort()[0];
  if (!latest) throw new Error('No local Massive stock/option dates were found.');
  return latest;
}

function datasetSuccessPath(config, datasetKey, dayIso) {
  return path.join(datasetDateDir(config, datasetKey, dayIso), '_SUCCESS.json');
}

function ensureDir(dirPath) {
  fs.mkdirSync(dirPath, { recursive: true });
}

module.exports = {
  PROJECT_ROOT,
  DEFAULT_CONFIG_PATH,
  loadConfig,
  datasetDateDir,
  datasetCsvPath,
  datasetParquetPath,
  resolveDatasetSource,
  latestDatasetDate,
  latestDatasetDateAcrossRoots,
  resolveEndDate,
  datasetSuccessPath,
  ensureDir,
};
