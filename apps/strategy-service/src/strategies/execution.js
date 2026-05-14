const crypto = require('node:crypto');
const {
  getTsllKernelArtifactMetadata,
} = require('../kernel-artifact');

const MANIFEST_VERSION = 'execution-manifest.v1';
const MARKET_TIMEZONE = 'America/New_York';
const REGULAR_SESSION = 'REGULAR';
const TSLL_KERNEL_MANIFEST = require('../../../../packages/strategy-kernels/tsll-seconds-passive-scalper/kernel.manifest.json');
const TSLL_KERNEL_CHECKSUMS = require('../../../../packages/strategy-kernels/tsll-seconds-passive-scalper/checksums.sha256.json');
const TSLL_KERNEL_ARTIFACT = getTsllKernelArtifactMetadata();

const STATUSES = Object.freeze(['research_only', 'paper_enabled', 'live_enabled']);
const ACTIONABLE_STATUSES = Object.freeze(['paper_enabled', 'live_enabled']);
const TIMING_CLASSES = Object.freeze(['EOD', 'INTRADAY', 'SCALP']);
const ACTIVATION_TYPES = Object.freeze(['after_market_close', 'regular_session_window']);
const PROMOTED_AUTHORIZATION = Object.freeze({
  authorized: true,
  domain: 'production_candidate',
  authorizedStatuses: ACTIONABLE_STATUSES,
});
const RESEARCH_AUTHORIZATION = Object.freeze({
  authorized: false,
  domain: 'research',
  authorizedStatuses: Object.freeze(['research_only']),
});

const DAILY_IDEMPOTENCY_FIELDS = Object.freeze(['strategyId', 'signalDate']);
const INTRADAY_IDEMPOTENCY_FIELDS = Object.freeze(['strategyId', 'signalDate', 'signalTimestamp']);
const PROMOTED_DAILY_IDEMPOTENCY_FIELDS = Object.freeze(['strategyId', 'strategyVersion', 'signalDate']);
const PROMOTED_SCALP_IDEMPOTENCY_FIELDS = Object.freeze(['strategyId', 'strategyVersion', 'signalTimestamp']);

function canonicalize(value) {
  if (value === undefined) return undefined;
  if (value === null || typeof value !== 'object') return value;
  if (Array.isArray(value)) return value.map(canonicalize).filter((item) => item !== undefined);
  return Object.keys(value).sort().reduce((out, key) => {
    const next = canonicalize(value[key]);
    if (next !== undefined) out[key] = next;
    return out;
  }, {});
}

function sha256Canonical(value) {
  return crypto.createHash('sha256').update(JSON.stringify(canonicalize(value))).digest('hex');
}

const TSLL_KERNEL_CHECKSUMS_SHA256 = sha256Canonical(TSLL_KERNEL_CHECKSUMS);

const PYM_V5_SYMBOLS = Object.freeze([
  'AGG', 'BIL', 'BND', 'BSV', 'CURE', 'EDC', 'EDV', 'EDZ', 'EEM', 'GLD',
  'GLL', 'IAU', 'IEF', 'IEI', 'IGIB', 'IOO', 'IVE', 'IWB', 'IWM', 'KMLM',
  'KRE', 'MGC', 'MUB', 'PSQ', 'QID', 'QLD', 'QQQ', 'ROM', 'SH', 'SHV',
  'SHY', 'SOXL', 'SOXS', 'SOXX', 'SPXL', 'SPXU', 'SPY', 'SPYV', 'SQQQ',
  'SSO', 'SVIX', 'SVXY', 'TECL', 'TECS', 'TIP', 'TLT', 'TMF', 'TMV',
  'TQQQ', 'TWM', 'UDN', 'UGE', 'UGL', 'UPRO', 'USO', 'UTSL', 'UUP',
  'UVXY', 'UWM', 'VBF', 'VIXM', 'VIXY', 'VOX', 'VT', 'VTI', 'VTV', 'VV',
  'XLF', 'XLK', 'XLP', 'XLU', 'XLV', 'ZROZ',
]);

const MANIFEST_DEFINITIONS = Object.freeze({
  'pym-v5': Object.freeze({
    promoted: true,
    manifestVersion: MANIFEST_VERSION,
    strategyId: 'pym-v5',
    strategyVersion: 'pym-v5.execution.v1',
    status: 'paper_enabled',
    promotion: PROMOTED_AUTHORIZATION,
    timingClass: 'EOD',
    timezone: MARKET_TIMEZONE,
    session: REGULAR_SESSION,
    activation: Object.freeze({ type: 'after_market_close', time: '16:05' }),
    signalCadence: 'daily_eod',
    symbols: PYM_V5_SYMBOLS,
    signalEndpoint: '/api/portfolio-targets/pym-v5/latest',
    idempotencyKeyFields: PROMOTED_DAILY_IDEMPOTENCY_FIELDS,
    executionDefaults: Object.freeze({
      orderType: 'market',
      targetType: 'portfolio_weights',
      rebalanceMode: 'replace_target_weights',
      maxQuoteAgeSeconds: null,
      signalCutoff: 'latest_after_activation',
      brokerLogic: 'external_runtime',
    }),
    riskDefaults: Object.freeze({
      allowedSymbols: PYM_V5_SYMBOLS,
      maxOrderValue: null,
      maxPositionWeight: 1,
      cashSymbol: 'BIL',
    }),
    theoreticalPerformance: Object.freeze({
      summaryStats: Object.freeze({
        source: '/api/strategies/pym-v5',
        totalReturnPct: null,
        maxDrawdownPct: null,
        sharpe: null,
        hitRatePct: null,
      }),
      latestExpectedSignalDate: null,
      latestExpectedTargetDate: null,
    }),
    provenance: Object.freeze({
      sourceArtifactPaths: Object.freeze([
        'projects/pym-v5-replication/artifacts/pym-v5-rebalance-report.json',
        'projects/pym-v5-replication/runtime/source/composer-XPGix2infTwwWMORgqmV-score.json',
        'projects/pym-v5-replication/runtime/pym-v5-massive-eod-adjusted-daily-bars-*.jsonl',
      ]),
      generatedAt: null,
      backtestWindow: Object.freeze({ startDate: '2025-01-01', endDate: 'latest_available_massive_eod' }),
      commit: null,
    }),
  }),
  'tsll-seconds-passive-scalper': Object.freeze({
    promoted: true,
    manifestVersion: MANIFEST_VERSION,
    strategyId: 'tsll-seconds-passive-scalper',
    strategyVersion: 'tsll-seconds-passive-scalper.execution.v1',
    status: 'paper_enabled',
    promotion: PROMOTED_AUTHORIZATION,
    timingClass: 'SCALP',
    timezone: MARKET_TIMEZONE,
    session: REGULAR_SESSION,
    activation: Object.freeze({ type: 'regular_session_window', startTime: '09:35', endTime: '15:50' }),
    signalCadence: 'continuous_intraday',
    symbols: Object.freeze(['TSLL']),
    signalEndpoint: '/api/kernels/tsll-seconds-passive-scalper.execution.v1/manifest',
    idempotencyKeyFields: PROMOTED_SCALP_IDEMPOTENCY_FIELDS,
    executionDefaults: Object.freeze({
      orderType: 'limit',
      entryReference: 'prior_completed_1s_close',
      buyBelowCloseCents: 3,
      targetCents: 3,
      stopCents: 5,
      maxHoldSeconds: 10,
      maxHoldBars: 10,
      barSeconds: 1,
      cooldownSeconds: 2,
      maxQuoteAgeSeconds: 2,
      sameSecondTargetStopPriority: 'stop_first',
      sameBarTargetStopPriority: 'stop_first',
      brokerLogic: 'external_runtime',
    }),
    kernel: Object.freeze({
      schemaVersion: 'phenixflow.strategyKernel.v1',
      runtime: Object.freeze({
        type: 'node',
        entrypoint: 'dist/kernel.mjs',
        moduleApi: 'phenixflow.kernel.module.v1',
      }),
      sidecarApi: 'phenixflow.kernel.sidecar.v1',
      artifactUri: '/api/kernels/tsll-seconds-passive-scalper.execution.v1/manifest',
      downloadUri: TSLL_KERNEL_ARTIFACT.downloadUri,
      artifactSha256: TSLL_KERNEL_ARTIFACT.artifactSha256,
      downloadSha256: TSLL_KERNEL_ARTIFACT.downloadSha256,
      checksumsSha256: TSLL_KERNEL_CHECKSUMS_SHA256,
      settingsSha256: TSLL_KERNEL_MANIFEST.settings.sha256,
      fixtureSuiteSha256: TSLL_KERNEL_MANIFEST.fixtures.suiteSha256,
    }),
    riskDefaults: Object.freeze({
      allowedSymbols: Object.freeze(['TSLL']),
      maxOrderValue: null,
      maxPositionShares: 1000,
      maxConcurrentPositions: 1,
    }),
    theoreticalPerformance: Object.freeze({
      summaryStats: Object.freeze({
        source: '/api/strategies/tsll-seconds-passive-scalper',
        totalReturnPct: null,
        maxDrawdownPct: null,
        winRate: null,
        trades: null,
        pnlPer1000Shares: null,
      }),
      latestExpectedSignalDate: null,
      latestExpectedTargetDate: null,
    }),
    provenance: Object.freeze({
      sourceArtifactPaths: Object.freeze([
        TSLL_KERNEL_MANIFEST.provenance.researchReportPath,
        TSLL_KERNEL_MANIFEST.provenance.researchArtifactId,
      ]),
      generatedAt: null,
      backtestWindow: Object.freeze(TSLL_KERNEL_MANIFEST.provenance.researchDateRange),
      commit: TSLL_KERNEL_MANIFEST.provenance.phenixFlowGitSha,
      baseline: Object.freeze({
        reportPath: TSLL_KERNEL_MANIFEST.provenance.researchReportPath,
        reportSha256: TSLL_KERNEL_MANIFEST.provenance.researchReportSha256,
        artifactId: TSLL_KERNEL_MANIFEST.provenance.researchArtifactId,
        sourceArtifactSha256: TSLL_KERNEL_MANIFEST.provenance.researchSourceArtifactSha256,
        datasetId: TSLL_KERNEL_MANIFEST.provenance.researchDatasetId,
        expectedSummarySha256: TSLL_KERNEL_MANIFEST.provenance.expectedSummarySha256,
        theoreticalPerformanceSha256: TSLL_KERNEL_MANIFEST.provenance.theoreticalPerformanceSha256,
        costSetting: Object.freeze(TSLL_KERNEL_MANIFEST.provenance.costSetting),
      }),
      kernel: Object.freeze({
        kernelId: TSLL_KERNEL_MANIFEST.artifact.id,
        strategyVersion: TSLL_KERNEL_MANIFEST.strategy.version,
        settingsSha256: TSLL_KERNEL_MANIFEST.settings.sha256,
        fixtureSuiteSha256: TSLL_KERNEL_MANIFEST.fixtures.suiteSha256,
        artifactSha256: TSLL_KERNEL_ARTIFACT.artifactSha256,
        downloadSha256: TSLL_KERNEL_ARTIFACT.downloadSha256,
        checksumsSha256: TSLL_KERNEL_CHECKSUMS_SHA256,
      }),
    }),
  }),
});

function clone(value) {
  return value === undefined ? undefined : JSON.parse(JSON.stringify(value));
}

function assertPlainObject(value, name) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    throw new Error(`invalid_execution_manifest:${name}_must_be_object`);
  }
}

function assertNonEmptyString(value, name) {
  if (typeof value !== 'string' || value.trim() === '') {
    throw new Error(`invalid_execution_manifest:${name}_must_be_non_empty_string`);
  }
}

function assertStringArray(value, name) {
  if (!Array.isArray(value) || value.length === 0 || value.some((item) => typeof item !== 'string' || item.trim() === '')) {
    throw new Error(`invalid_execution_manifest:${name}_must_be_non_empty_string_array`);
  }
}

function validateActivation(manifest) {
  assertPlainObject(manifest.activation, 'activation');
  if (!ACTIVATION_TYPES.includes(manifest.activation.type)) {
    throw new Error('invalid_execution_manifest:activation_type_unknown');
  }
  if (manifest.activation.type === 'after_market_close') {
    if (manifest.timingClass !== 'EOD') {
      throw new Error('invalid_execution_manifest:after_market_close_requires_eod_timing');
    }
    assertNonEmptyString(manifest.activation.time, 'activation.time');
  }
  if (manifest.activation.type === 'regular_session_window') {
    if (manifest.timingClass === 'EOD') {
      throw new Error('invalid_execution_manifest:regular_session_window_requires_intraday_or_scalp_timing');
    }
    assertNonEmptyString(manifest.activation.startTime, 'activation.startTime');
    assertNonEmptyString(manifest.activation.endTime, 'activation.endTime');
  }
}

function validateExecutionManifestDefinition(definition) {
  assertPlainObject(definition, 'definition');
  assertNonEmptyString(definition.manifestVersion, 'manifestVersion');
  assertNonEmptyString(definition.strategyId, 'strategyId');
  assertNonEmptyString(definition.strategyVersion, 'strategyVersion');
  if (!STATUSES.includes(definition.status)) throw new Error('invalid_execution_manifest:status_unknown');
  assertPlainObject(definition.promotion, 'promotion');
  if (typeof definition.promotion.authorized !== 'boolean') {
    throw new Error('invalid_execution_manifest:promotion.authorized_must_be_boolean');
  }
  assertNonEmptyString(definition.promotion.domain, 'promotion.domain');
  assertStringArray(definition.promotion.authorizedStatuses, 'promotion.authorizedStatuses');
  if (!TIMING_CLASSES.includes(definition.timingClass)) throw new Error('invalid_execution_manifest:timing_class_unknown');
  assertNonEmptyString(definition.timezone, 'timezone');
  assertNonEmptyString(definition.session, 'session');
  validateActivation(definition);
  assertStringArray(definition.symbols, 'symbols');
  assertNonEmptyString(definition.signalEndpoint, 'signalEndpoint');
  assertStringArray(definition.idempotencyKeyFields, 'idempotencyKeyFields');
  assertPlainObject(definition.executionDefaults, 'executionDefaults');
  assertNonEmptyString(definition.executionDefaults.orderType, 'executionDefaults.orderType');
  assertPlainObject(definition.riskDefaults, 'riskDefaults');
  assertStringArray(definition.riskDefaults.allowedSymbols, 'riskDefaults.allowedSymbols');
  assertPlainObject(definition.theoreticalPerformance, 'theoreticalPerformance');
  assertPlainObject(definition.theoreticalPerformance.summaryStats, 'theoreticalPerformance.summaryStats');
  assertPlainObject(definition.provenance, 'provenance');
  assertStringArray(definition.provenance.sourceArtifactPaths, 'provenance.sourceArtifactPaths');
  assertPlainObject(definition.provenance.backtestWindow, 'provenance.backtestWindow');

  if (ACTIONABLE_STATUSES.includes(definition.status) && definition.promoted !== true) {
    throw new Error('invalid_execution_manifest:actionable_status_requires_promoted_strategy');
  }
  if (ACTIONABLE_STATUSES.includes(definition.status) && definition.promotion.authorized !== true) {
    throw new Error('invalid_execution_manifest:actionable_status_requires_promotion_authorization');
  }
  if (!ACTIONABLE_STATUSES.includes(definition.status) && definition.promotion.authorized === true) {
    throw new Error('invalid_execution_manifest:promotion_authorization_requires_actionable_status');
  }
  return true;
}

function publicManifest(definition) {
  validateExecutionManifestDefinition(definition);
  const manifest = clone(definition);
  delete manifest.promoted;
  return manifest;
}

const EXECUTION_MANIFESTS = Object.freeze(Object.fromEntries(
  Object.entries(MANIFEST_DEFINITIONS).map(([strategyId, definition]) => [
    strategyId,
    Object.freeze(publicManifest(definition)),
  ]),
));

function executionSummaryFromManifest(manifest) {
  return {
    manifestVersion: manifest.manifestVersion,
    strategyVersion: manifest.strategyVersion,
    status: manifest.status,
    promotion: clone(manifest.promotion),
    timingClass: manifest.timingClass,
    timezone: manifest.timezone,
    session: manifest.session,
    activation: clone(manifest.activation),
    signalCadence: manifest.signalCadence,
    idempotencyKeyFields: [...manifest.idempotencyKeyFields],
  };
}

function getExecutionManifest(strategyId) {
  const manifest = EXECUTION_MANIFESTS[String(strategyId || '').trim()];
  if (!manifest) return null;
  return clone(manifest);
}

function getExecutionManifestDefinition(strategyId) {
  const definition = MANIFEST_DEFINITIONS[String(strategyId || '').trim()];
  if (!definition) return null;
  return clone(definition);
}

function listExecutionManifests({ strategyIds } = {}) {
  const allowed = strategyIds ? new Set(strategyIds.map((id) => String(id))) : null;
  return Object.values(EXECUTION_MANIFESTS)
    .filter((manifest) => !allowed || allowed.has(manifest.strategyId))
    .map(clone);
}

function executionSummaryForStrategy(strategyId) {
  const manifest = getExecutionManifest(strategyId);
  if (!manifest) throw new Error(`missing_execution_manifest:${strategyId}`);
  return executionSummaryFromManifest(manifest);
}

function dailyEodExecution({ time = '16:05' } = {}) {
  return executionSummaryFromManifest({
    manifestVersion: MANIFEST_VERSION,
    strategyVersion: 'research.execution.v1',
    status: 'research_only',
    promotion: clone(RESEARCH_AUTHORIZATION),
    timingClass: 'EOD',
    timezone: MARKET_TIMEZONE,
    session: REGULAR_SESSION,
    activation: { type: 'after_market_close', time },
    signalCadence: 'daily_eod',
    idempotencyKeyFields: [...DAILY_IDEMPOTENCY_FIELDS],
  });
}

function regularSessionExecution({
  timingClass = 'INTRADAY',
  startTime = '09:35',
  endTime = '15:55',
} = {}) {
  return executionSummaryFromManifest({
    manifestVersion: MANIFEST_VERSION,
    strategyVersion: 'research.execution.v1',
    status: 'research_only',
    promotion: clone(RESEARCH_AUTHORIZATION),
    timingClass,
    timezone: MARKET_TIMEZONE,
    session: REGULAR_SESSION,
    activation: { type: 'regular_session_window', startTime, endTime },
    signalCadence: 'continuous_intraday',
    idempotencyKeyFields: [...INTRADAY_IDEMPOTENCY_FIELDS],
  });
}

module.exports = {
  MANIFEST_VERSION,
  ACTIONABLE_STATUSES,
  getExecutionManifest,
  getExecutionManifestDefinition,
  listExecutionManifests,
  executionSummaryForStrategy,
  executionSummaryFromManifest,
  validateExecutionManifestDefinition,
  dailyEodExecution,
  regularSessionExecution,
};
