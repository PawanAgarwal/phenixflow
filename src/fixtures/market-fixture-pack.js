const fs = require('node:fs');
const path = require('node:path');

const PACKS_DIR = path.resolve(__dirname, '../../fixtures/packs');
const REGISTRY_PATH = path.join(PACKS_DIR, 'market-fixtures.registry.json');

function clone(value) {
  return JSON.parse(JSON.stringify(value));
}

function readJson(filePath) {
  return JSON.parse(fs.readFileSync(filePath, 'utf8'));
}

function getFixturePackPath(packId, fixtureVersion) {
  const registry = readJson(REGISTRY_PATH);
  const packVersions = registry?.packs?.[packId];

  if (!packVersions) {
    throw new Error(`fixture_pack_not_found:${packId}`);
  }

  const fileName = packVersions[fixtureVersion];
  if (!fileName) {
    throw new Error(`fixture_pack_version_not_found:${packId}@${fixtureVersion}`);
  }

  return path.join(PACKS_DIR, fileName);
}

function loadFixturePack(options = {}) {
  const packId = options.packId || 'mon63-market-core';
  const fixtureVersion = options.fixtureVersion || 'v1';
  const filePath = getFixturePackPath(packId, fixtureVersion);
  const pack = readJson(filePath);

  if (pack.packId !== packId || pack.fixtureVersion !== fixtureVersion) {
    throw new Error(`fixture_pack_identity_mismatch:${packId}@${fixtureVersion}`);
  }

  return clone(pack);
}

function listFixturePackVersions(packId = 'mon63-market-core') {
  const registry = readJson(REGISTRY_PATH);
  const packVersions = registry?.packs?.[packId];

  if (!packVersions) {
    return [];
  }

  return Object.keys(packVersions).sort();
}

function buildScenarioSlice(pack, scenarioId) {
  const scenario = (pack.scenarios || []).find((candidate) => candidate.id === scenarioId);
  if (!scenario) {
    throw new Error(`fixture_scenario_not_found:${scenarioId}`);
  }

  const byId = (rows = []) => Object.fromEntries(rows.map((row) => [row.id, row]));
  const tradesById = byId(pack.datasets?.trades);
  const quotesById = byId(pack.datasets?.quotes);
  const oiById = byId(pack.datasets?.oi);
  const spotById = byId(pack.datasets?.spot);

  return {
    scenario: clone(scenario),
    datasets: {
      trades: (scenario.tradeIds || []).map((id) => tradesById[id]).filter(Boolean),
      quotes: (scenario.quoteIds || []).map((id) => quotesById[id]).filter(Boolean),
      oi: (scenario.oiIds || []).map((id) => oiById[id]).filter(Boolean),
      spot: (scenario.spotIds || []).map((id) => spotById[id]).filter(Boolean),
    },
  };
}

module.exports = {
  loadFixturePack,
  listFixturePackVersions,
  buildScenarioSlice,
};
