const fs = require('node:fs');
const path = require('node:path');

const PACK_FILE_BY_NAME = Object.freeze({
  trades: 'trades.json',
  quotes: 'quotes.json',
  openInterest: 'open-interest.json',
  spot: 'spot.json',
});

function fixturePacksDir() {
  return path.resolve(__dirname, '../../fixtures/packs');
}

function fixturePackPath(name) {
  const file = PACK_FILE_BY_NAME[name];
  if (!file) {
    throw new Error(`Unknown fixture pack "${name}"`);
  }

  return path.join(fixturePacksDir(), file);
}

function readFixturePack(name) {
  const filePath = fixturePackPath(name);
  const raw = fs.readFileSync(filePath, 'utf8');
  return JSON.parse(raw);
}

function loadFixturePack(name) {
  return structuredClone(readFixturePack(name));
}

function loadFixtureRows(name) {
  return loadFixturePack(name).rows;
}

function listFixturePacks() {
  return Object.keys(PACK_FILE_BY_NAME);
}

module.exports = {
  fixturePackPath,
  loadFixturePack,
  loadFixtureRows,
  listFixturePacks,
};
