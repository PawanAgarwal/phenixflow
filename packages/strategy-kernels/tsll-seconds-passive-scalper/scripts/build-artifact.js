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

function optionalShaFile(filePath) {
  return fs.existsSync(filePath) ? shaFile(filePath) : null;
}

function findResult(report, id) {
  const results = Array.isArray(report?.results) ? report.results : [];
  return results.find((item) => item.id === id || item.strategy === id) || null;
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
    'export const resolveSettingsForRow = kernel.resolveSettingsForRow;',
    'export default { describe, createKernel, onEvent, onEventLean, replay, evaluateBacktestExit, resolveSettingsForRow };',
    '',
  ].join('\n'), 'utf8');
  fs.writeFileSync(path.join(DIST, 'features.mjs'), [
    "import features from '../src/features.js';",
    'export const featureRowFromBarEvent = features.featureRowFromBarEvent;',
    'export const evaluateFilters = features.evaluateFilters;',
    'export const evaluateBacktestExit = features.evaluateBacktestExit;',
    'export const resolveSettingsForRow = features.resolveSettingsForRow;',
    'export default { featureRowFromBarEvent, evaluateFilters, evaluateBacktestExit, resolveSettingsForRow };',
    '',
  ].join('\n'), 'utf8');
}

function buildManifest() {
  const rthResearchPath = path.join(REPO_ROOT, 'projects/tsll-scalping/artifacts/tsll-scalp-improvement-analysis-market-filters-2025-01-02-2026-05-29.json');
  const extendedResearchPath = path.join(REPO_ROOT, 'projects/tsll-scalping/artifacts/tsll-scalp-improvement-analysis-market-filters-extended-2025-01-02-2026-05-29.json');
  const rthResearch = readJson(rthResearchPath);
  const extendedResearch = readJson(extendedResearchPath);
  const selectedRth = findResult(rthResearch, 'dyn_b4_stale_loss_exit__base');
  const selectedExtended = findResult(extendedResearch, 'b5_t5_s8_h20__not_overextended_60s');
  const fixtureDecisions = readJsonl('fixtures/expected-decisions.jsonl');
  const fixtureTraces = readJsonl('fixtures/expected-traces.jsonl');
  const adapterTrades = readJsonl('fixtures/backtest-adapter/expected-trades.jsonl');
  const summaryPayload = {
    settings,
    selectedProfiles: {
      regular: selectedRth,
      extended: selectedExtended,
    },
    sourceReports: [
      path.relative(REPO_ROOT, rthResearchPath),
      path.relative(REPO_ROOT, extendedResearchPath),
    ],
  };
  return {
    schemaVersion: 'phenixflow.strategyKernel.v1',
    strategy: {
      id: 'tsll-seconds-passive-scalper',
      name: 'TSLL Hybrid RTH and Extended-Hours Seconds Passive Scalper',
      version: '2026.05.31',
      timingClass: 'SCALP',
      timezone: 'America/New_York',
    },
    artifact: {
      id: 'tsll-seconds-passive-scalper.execution.v1',
      createdAt: '2026-05-31T00:00:00.000Z',
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
        regularSessionOnly: false,
        sessions: ['regular', 'extended'],
      },
      quoteRequirements: {
        maxQuoteAgeMs: settings.quoteMaxAgeMs,
      },
    },
    activation: {
      type: 'regular_session_window',
      startTime: '04:05',
      endTime: '19:50',
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
      researchReportPath: path.relative(REPO_ROOT, rthResearchPath),
      researchReportSha256: shaFile(rthResearchPath),
      researchReportPaths: [
        path.relative(REPO_ROOT, rthResearchPath),
        path.relative(REPO_ROOT, extendedResearchPath),
      ],
      researchReportSha256ByPath: {
        [path.relative(REPO_ROOT, rthResearchPath)]: optionalShaFile(rthResearchPath),
        [path.relative(REPO_ROOT, extendedResearchPath)]: optionalShaFile(extendedResearchPath),
      },
      researchArtifactId: path.relative(REPO_ROOT, extendedResearchPath),
      researchArtifactIds: [
        path.relative(REPO_ROOT, rthResearchPath),
        path.relative(REPO_ROOT, extendedResearchPath),
      ],
      researchDatasetId: `tsll-1s-${rthResearch.startDate}-${rthResearch.endDate}-massive-rest-1s-extended-hybrid-barSeconds1-nodaily`,
      researchSourceArtifactSha256: sha256Canonical({
        rth: optionalShaFile(rthResearchPath),
        extended: optionalShaFile(extendedResearchPath),
      }),
      researchDateRange: {
        startDate: rthResearch.startDate,
        endDate: rthResearch.endDate,
      },
      costSetting: {
        costCentsPerSide: settings.costCentsPerSide,
      },
      selectedProfiles: {
        regular: {
          id: selectedRth?.id || null,
          label: selectedRth?.label || null,
          summary: selectedRth?.overall || null,
          test2026Ytd: selectedRth?.test2026Ytd || null,
        },
        extended: {
          id: selectedExtended?.id || null,
          label: selectedExtended?.label || null,
          summary: selectedExtended?.overall || null,
          test2026Ytd: selectedExtended?.test2026Ytd || null,
        },
      },
      promotedSettingsSha256: sha256Canonical(settings),
      expectedSummarySha256: sha256Canonical(summaryPayload),
      theoreticalPerformanceSha256: sha256Canonical({
        regular: selectedRth?.overall || null,
        extended: selectedExtended?.overall || null,
      }),
    },
  };
}

function writeChecksums() {
  const excluded = new Set(['checksums.sha256.json']);
  const files = listFiles(ROOT)
    .filter((relative) => relative === 'scripts/replay-fixtures.js' || !relative.startsWith('scripts/'))
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
