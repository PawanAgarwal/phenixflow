const fs = require('node:fs');
const path = require('node:path');

const PROJECT_ROOT = path.resolve(__dirname, '..');
const DEFAULT_CONFIG_PATH = path.join(PROJECT_ROOT, 'config', 'default.json');

function loadConfig(configPath = DEFAULT_CONFIG_PATH) {
  return JSON.parse(fs.readFileSync(configPath, 'utf8'));
}

function ensureDir(dirPath) {
  fs.mkdirSync(dirPath, { recursive: true });
}

function runtimePath(...parts) {
  return path.join(PROJECT_ROOT, 'runtime', ...parts);
}

function artifactPath(...parts) {
  return path.join(PROJECT_ROOT, 'artifacts', ...parts);
}

function stockCsvPath(config, dayIso) {
  return path.join(
    config.roots.historical,
    config.datasets.stockBars,
    `date=${dayIso}`,
    `${dayIso}.csv.gz`,
  );
}

function stockParquetPath(config, dayIso) {
  if (!config.roots.liveParquet) return null;
  return path.join(
    config.roots.liveParquet,
    config.datasets.stockBars,
    `date=${dayIso}`,
    `${dayIso}.live.parquet`,
  );
}

function stockSuccessPath(config, dayIso) {
  return path.join(
    config.roots.historical,
    config.datasets.stockBars,
    `date=${dayIso}`,
    '_SUCCESS.json',
  );
}

module.exports = {
  PROJECT_ROOT,
  DEFAULT_CONFIG_PATH,
  loadConfig,
  ensureDir,
  runtimePath,
  artifactPath,
  stockCsvPath,
  stockParquetPath,
  stockSuccessPath,
};
