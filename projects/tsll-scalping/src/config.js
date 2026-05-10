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

function datasetDateDir(config, datasetKey, dayIso) {
  const datasetId = config.datasets[datasetKey] || datasetKey;
  return path.join(config.roots.historical, datasetId, `date=${dayIso}`);
}

function datasetCsvPath(config, datasetKey, dayIso) {
  return path.join(datasetDateDir(config, datasetKey, dayIso), `${dayIso}.csv.gz`);
}

function datasetSuccessPath(config, datasetKey, dayIso) {
  return path.join(datasetDateDir(config, datasetKey, dayIso), '_SUCCESS.json');
}

function runtimePath(...segments) {
  return path.join(PROJECT_ROOT, 'runtime', ...segments);
}

function artifactPath(...segments) {
  return path.join(PROJECT_ROOT, 'artifacts', ...segments);
}

module.exports = {
  PROJECT_ROOT,
  DEFAULT_CONFIG_PATH,
  loadConfig,
  ensureDir,
  datasetDateDir,
  datasetCsvPath,
  datasetSuccessPath,
  runtimePath,
  artifactPath,
};
