#!/usr/bin/env node
const fs = require('node:fs');
const path = require('node:path');

const settings = require('../settings/default.json');
const { replay } = require('../src/kernel');
const { sha256Jsonl } = require('../src/canonical');

const ROOT = path.resolve(__dirname, '..');

function readJsonl(relativePath) {
  const text = fs.readFileSync(path.join(ROOT, relativePath), 'utf8');
  return text.split(/\r?\n/).filter(Boolean).map((line) => JSON.parse(line));
}

function main() {
  const events = readJsonl('fixtures/replay-input.jsonl');
  const expectedDecisions = readJsonl('fixtures/expected-decisions.jsonl');
  const expectedTraces = readJsonl('fixtures/expected-traces.jsonl');
  const result = replay({
    settings,
    events,
    mode: 'backtest',
    clock: { timezone: 'America/New_York', sessionDate: '2026-05-13' },
    expectedDecisionSha256: sha256Jsonl(expectedDecisions),
    expectedTraceSha256: sha256Jsonl(expectedTraces),
  });
  console.log(JSON.stringify({
    passed: result.passed,
    actualDecisionSha256: result.actualDecisionSha256,
    actualTraceSha256: result.actualTraceSha256,
    expectedDecisionSha256: result.expectedDecisionSha256,
    expectedTraceSha256: result.expectedTraceSha256,
  }, null, 2));
  if (!result.passed) process.exitCode = 1;
}

main();
