#!/usr/bin/env node
const crypto = require('node:crypto');
const Database = require('better-sqlite3');
const { resolveDbPath } = require('../../src/config/env');
const { __private: { ensureSchema } } = require('../../src/historical-flow');

function usage() {
  console.error('Usage: node scripts/rules/activate-rule-version.js <versionId> [--checksum=<sha256>]');
}

function parseArgs(argv) {
  const [, , versionId, ...rest] = argv;
  if (!versionId) return { error: 'version_required' };
  const checksumArg = rest.find((token) => token.startsWith('--checksum='));
  return {
    versionId,
    checksum: checksumArg ? checksumArg.slice('--checksum='.length).trim() : null,
  };
}

function computeChecksum(configJson) {
  return crypto.createHash('sha256').update(String(configJson || ''), 'utf8').digest('hex');
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
      dbPath,
    }, null, 2));
  } finally {
    db.close();
  }
}

run();
