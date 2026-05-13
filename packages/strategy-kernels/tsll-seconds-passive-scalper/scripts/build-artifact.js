#!/usr/bin/env node
const fs = require('node:fs');
const path = require('node:path');

const settings = require('../settings/default.json');
const { describe } = require('../src/kernel');
const {
  canonicalStringify,
  sha256Canonical,
  sha256Jsonl,
  sha256Text,
} = require('../src/canonical');

const ROOT = path.resolve(__dirname, '..');
const REPO_ROOT = path.resolve(ROOT, '..', '..', '..');
const DIST = path.join(ROOT, 'dist');

function readJson(filePath) {
  return JSON.parse(fs.readFileSync(filePath, 'utf8'));
}

function writeJson(relativePath, value) {
  const filePath = path.join(ROOT, relativePath);
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  fs.writeFileSync(filePath, `${JSON.stringify(value, null, 2)}\n`, 'utf8');
}

function readJsonl(relativePath) {
  const filePath = path.join(ROOT, relativePath);
  if (!fs.existsSync(filePath)) return [];
  return fs.readFileSync(filePath, 'utf8').split(/\r?\n/).filter(Boolean).map((line) => JSON.parse(line));
}

function shaFile(filePath) {
  return sha256Text(fs.readFileSync(filePath));
}

function listFiles(dir, prefix = '') {
  return fs.readdirSync(dir, { withFileTypes: true }).flatMap((entry) => {
    const relative = path.join(prefix, entry.name);
    if (entry.isDirectory()) return listFiles(path.join(dir, entry.name), relative);
    return [relative.replace(/\\/g, '/')];
  });
}

function writeDistWrappers() {
  fs.mkdirSync(DIST, { recursive: true });
  fs.writeFileSync(path.join(DIST, 'kernel.mjs'), [
    "import kernel from '../src/kernel.js';",
    'export const describe = kernel.describe;',
    'export const createKernel = kernel.createKernel;',
    'export const onEvent = kernel.onEvent;',
    'export const onEventLean = kernel.onEventLean;',
    'export const replay = kernel.replay;',
    'export const evaluateBacktestExit = kernel.evaluateBacktestExit;',
    'export default { describe, createKernel, onEvent, onEventLean, replay, evaluateBacktestExit };',
    '',
  ].join('\n'), 'utf8');
  fs.writeFileSync(path.join(DIST, 'features.mjs'), [
    "import features from '../src/features.js';",
    'export const featureRowFromBarEvent = features.featureRowFromBarEvent;',
    'export const evaluateFilters = features.evaluateFilters;',
    'export const evaluateBacktestExit = features.evaluateBacktestExit;',
    'export default { featureRowFromBarEvent, evaluateFilters, evaluateBacktestExit };',
    '',
  ].join('\n'), 'utf8');
}

function buildManifest() {
  const reportPath = path.join(REPO_ROOT, 'projects/tsll-scalping/reports/tsll-seconds-passive-fixed-2025-01-02-2026-05-12.json');
  const fallbackReportPath = path.join(REPO_ROOT, 'projects/tsll-scalping/reports/tsll-seconds-passive-fixed-feb2026.json');
  const resolvedReportPath = fs.existsSync(reportPath) ? reportPath : fallbackReportPath;
  const report = readJson(resolvedReportPath);
  const sourceArtifactPath = path.join(REPO_ROOT, report.sourceArtifact.replace(/^projects\//, 'projects/'));
  const sourceArtifactExists = fs.existsSync(sourceArtifactPath);
  const fixtureDecisions = readJsonl('fixtures/expected-decisions.jsonl');
  const fixtureTraces = readJsonl('fixtures/expected-traces.jsonl');
  const adapterTrades = readJsonl('fixtures/backtest-adapter/expected-trades.jsonl');
  const sourceArtifactSha256 = sourceArtifactExists ? shaFile(sourceArtifactPath) : null;
  const summaryPayload = {
    strategy: report.strategy,
    assumptions: report.assumptions,
    totals: report.totals,
    days: report.days,
  };
  return {
    schemaVersion: 'phenixflow.strategyKernel.v1',
    strategy: {
      id: 'tsll-seconds-passive-scalper',
      name: 'TSLL Seconds Passive Scalper',
      version: '2026.05.13',
      timingClass: 'SCALP',
      timezone: 'America/New_York',
    },
    artifact: {
      id: 'tsll-seconds-passive-scalper.execution.v1',
      createdAt: '2026-05-13T00:00:00.000Z',
    },
    runtime: {
      type: 'node',
      entrypoint: 'dist/kernel.mjs',
      moduleApi: 'phenixflow.kernel.module.v1',
      sidecarApi: 'phenixflow.kernel.sidecar.v1',
      nodeRange: '>=20',
    },
    module: describe(),
    settings: {
      path: 'settings/default.json',
      sha256: sha256Canonical(settings),
      frozen: true,
    },
    inputs: {
      requiredEventTypes: [
        'SESSION_STARTED',
        'BAR_1S_CLOSED',
        'BAR_1M_CLOSED',
        'QUOTE_UPDATED',
        'ORDER_ACKED',
        'ORDER_FILLED',
        'ORDER_CANCELLED',
        'TIMER',
        'SESSION_ENDED',
      ],
      symbols: ['TSLL'],
      contextSymbols: ['SPY', 'QQQ', 'TSLA'],
      barRequirements: {
        primary: '1s',
        context: '1m',
        regularSessionOnly: true,
      },
      quoteRequirements: {
        maxQuoteAgeMs: settings.quoteMaxAgeMs,
      },
    },
    activation: {
      type: 'regular_session_window',
      startTime: '09:35',
      endTime: '15:50',
    },
    execution: {
      orderType: 'limit',
      entryReference: 'prior_completed_1s_close',
      entryLifetimeSeconds: settings.entryLifetimeSeconds,
      barSeconds: settings.barSeconds,
      maxConcurrentPositions: settings.maxConcurrentPositions,
      maxPositionShares: settings.maxPositionShares,
      paperFillModel: 'quote_conservative.v1',
      backtestFillModel: 'ohlc_1s_proxy.v1',
    },
    fixtures: {
      input: 'fixtures/replay-input.jsonl',
      expectedDecisions: 'fixtures/expected-decisions.jsonl',
      expectedTraces: 'fixtures/expected-traces.jsonl',
      decisionSha256: sha256Jsonl(fixtureDecisions),
      traceSha256: sha256Jsonl(fixtureTraces),
      suiteSha256: sha256Canonical({
        decisions: sha256Jsonl(fixtureDecisions),
        traces: sha256Jsonl(fixtureTraces),
      }),
    },
    adapterFixtures: {
      backtestFillModel: 'ohlc_1s_proxy.v1',
      expectedTrades: 'fixtures/backtest-adapter/expected-trades.jsonl',
      suiteSha256: sha256Jsonl(adapterTrades),
    },
    provenance: {
      phenixFlowGitSha: 'c573cb91a87edde7d1be68e2756d79ab3033876c',
      researchReportPath: path.relative(REPO_ROOT, resolvedReportPath),
      researchReportSha256: shaFile(resolvedReportPath),
      researchArtifactId: report.sourceArtifact,
      researchDatasetId: `tsll-1s-${report.startDate}-${report.endDate}-${report.assumptions?.data?.includes('REST') ? 'massive-rest-1s' : 'massive-trades-1s'}-barSeconds1-nodaily`,
      researchSourceArtifactSha256: sourceArtifactSha256,
      researchDateRange: {
        startDate: report.startDate,
        endDate: report.endDate,
      },
      costSetting: {
        costCentsPerSide: report.assumptions?.explicitCostCentsPerSide ?? settings.costCentsPerSide,
      },
      promotedSettingsSha256: sha256Canonical(settings),
      expectedSummarySha256: sha256Canonical(summaryPayload),
      theoreticalPerformanceSha256: sha256Canonical(report.totals),
    },
  };
}

function writeChecksums() {
  const excluded = new Set(['checksums.sha256.json']);
  const files = listFiles(ROOT)
    .filter((relative) => !relative.startsWith('scripts/'))
    .filter((relative) => !relative.startsWith('node_modules/'))
    .filter((relative) => !excluded.has(relative))
    .sort();
  const checksums = {
    schemaVersion: 'phenixflow.artifactChecksums.v1',
    algorithm: 'sha256',
    excludes: [...excluded].sort(),
    files: Object.fromEntries(files.map((relative) => [
      relative,
      shaFile(path.join(ROOT, relative)),
    ])),
  };
  fs.writeFileSync(path.join(ROOT, 'checksums.sha256.json'), `${JSON.stringify(checksums, null, 2)}\n`, 'utf8');
}

function main() {
  writeDistWrappers();
  writeJson('kernel.manifest.json', buildManifest());
  writeChecksums();
  console.log(canonicalStringify({
    manifest: 'kernel.manifest.json',
    settingsSha256: sha256Canonical(settings),
    checksums: 'checksums.sha256.json',
  }));
}

main();
