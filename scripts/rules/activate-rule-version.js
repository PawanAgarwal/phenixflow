#!/usr/bin/env node
const crypto = require('node:crypto');
const fs = require('node:fs');
const Database = require('better-sqlite3');
const { resolveDbPath } = require('../../src/config/env');
const { __private: { ensureSchema } } = require('../../src/historical-flow');

function usage() {
  console.error('Usage: node scripts/rules/activate-rule-version.js <versionId> [--checksum=<sha256>] [--skip-calibration-gate] [--calibration-report=<path>] [--min-calibration-rows=<n>] [--min-precision-proxy=<0..1>]');
}

function parseArgs(argv) {
  const [, , versionId, ...rest] = argv;
  if (!versionId) return { error: 'version_required' };
  const checksumArg = rest.find((token) => token.startsWith('--checksum='));
  const calibrationReportArg = rest.find((token) => token.startsWith('--calibration-report='));
  const minRowsArg = rest.find((token) => token.startsWith('--min-calibration-rows='));
  const minPrecisionArg = rest.find((token) => token.startsWith('--min-precision-proxy='));
  const skipCalibrationGate = rest.includes('--skip-calibration-gate');
  return {
    versionId,
    checksum: checksumArg ? checksumArg.slice('--checksum='.length).trim() : null,
    skipCalibrationGate,
    calibrationReportPath: calibrationReportArg ? calibrationReportArg.slice('--calibration-report='.length).trim() : null,
    minCalibrationRows: minRowsArg ? Number(minRowsArg.slice('--min-calibration-rows='.length).trim()) : null,
    minPrecisionProxy: minPrecisionArg ? Number(minPrecisionArg.slice('--min-precision-proxy='.length).trim()) : null,
  };
}

function computeChecksum(configJson) {
  return crypto.createHash('sha256').update(String(configJson || ''), 'utf8').digest('hex');
}

function toFiniteNumber(value, fallback) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function runCalibrationGate(parsed) {
  if (parsed.skipCalibrationGate) {
    return {
      skipped: true,
      reason: 'skip_flag',
    };
  }

  const reportPath = parsed.calibrationReportPath
    || process.env.SIGSCORE_CALIBRATION_OUTPUT
    || 'artifacts/reports/sigscore-calibration.json';
  const minRows = toFiniteNumber(parsed.minCalibrationRows, toFiniteNumber(process.env.RULE_ACTIVATE_MIN_CALIBRATION_ROWS, 1000));
  const minPrecisionProxy = toFiniteNumber(parsed.minPrecisionProxy, toFiniteNumber(process.env.RULE_ACTIVATE_MIN_PRECISION_PROXY, 0.15));

  let report;
  try {
    report = JSON.parse(fs.readFileSync(reportPath, 'utf8'));
  } catch (error) {
    throw new Error(`calibration_gate_failed:report_unreadable:${error.message}`);
  }

  const totalRows = toFiniteNumber(report.totalRows, 0);
  if (totalRows < minRows) {
    throw new Error(`calibration_gate_failed:insufficient_rows:${totalRows}<${minRows}`);
  }

  const precisionProxy = toFiniteNumber(report.precisionProxy, null);
  if (precisionProxy === null) {
    throw new Error('calibration_gate_failed:precision_proxy_missing');
  }

  if (precisionProxy < minPrecisionProxy) {
    throw new Error(`calibration_gate_failed:precision_proxy_below_threshold:${precisionProxy}<${minPrecisionProxy}`);
  }

  return {
    skipped: false,
    reportPath,
    totalRows,
    precisionProxy,
    minRows,
    minPrecisionProxy,
  };
}

function run() {
  const parsed = parseArgs(process.argv);
  if (parsed.error) {
    usage();
    process.exitCode = 1;
    return;
  }

  const dbPath = resolveDbPath(process.env);
  const db = new Database(dbPath);
  ensureSchema(db);

  try {
    const calibrationGate = runCalibrationGate(parsed);

    const row = db.prepare(`
      SELECT version_id AS versionId, config_json AS configJson, checksum
      FROM filter_rule_versions
      WHERE version_id = @versionId
      LIMIT 1
    `).get({ versionId: parsed.versionId });

    if (!row) {
      throw new Error(`rule_version_not_found:${parsed.versionId}`);
    }

    const computedChecksum = computeChecksum(row.configJson);
    if (parsed.checksum && parsed.checksum !== computedChecksum && parsed.checksum !== row.checksum) {
      throw new Error('checksum_mismatch');
    }

    const txn = db.transaction(() => {
      db.prepare(`
        UPDATE filter_rule_versions
        SET is_active = 0,
            activated_at_utc = NULL
        WHERE is_active = 1
      `).run();

      db.prepare(`
        UPDATE filter_rule_versions
        SET is_active = 1,
            activated_at_utc = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
        WHERE version_id = @versionId
      `).run({ versionId: parsed.versionId });
    });

    txn();

    console.log(JSON.stringify({
      status: 'ok',
      versionId: parsed.versionId,
      checksum: computedChecksum,
      calibrationGate,
      dbPath,
    }, null, 2));
  } finally {
    db.close();
  }
}

run();
