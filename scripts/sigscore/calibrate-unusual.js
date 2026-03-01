#!/usr/bin/env node
const fs = require('node:fs');
const path = require('node:path');
const Database = require('better-sqlite3');
const { resolveDbPath } = require('../../src/config/env');

function toFinite(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function percentile(sorted, p) {
  if (!sorted.length) return null;
  const idx = Math.min(sorted.length - 1, Math.max(0, Math.floor((p / 100) * sorted.length)));
  return sorted[idx];
}

function run() {
  const dbPath = resolveDbPath(process.env);
  const outputPath = process.env.SIGSCORE_CALIBRATION_OUTPUT
    || path.resolve(process.cwd(), 'artifacts', 'reports', 'sigscore-calibration.json');

  if (!fs.existsSync(dbPath)) {
    throw new Error(`db_missing:${dbPath}`);
  }

  const db = new Database(dbPath, { readonly: true });
  try {
    const rows = db.prepare(`
      SELECT
        trade_ts_utc AS tradeTsUtc,
        symbol,
        sig_score AS sigScore,
        vol_oi_ratio AS volOiRatio,
        repeat3m,
        chips_json AS chipsJson,
        score_quality AS scoreQuality,
        rule_version AS ruleVersion
      FROM option_trade_enriched
      WHERE sig_score IS NOT NULL
      ORDER BY trade_ts_utc ASC
    `).all();

    const scores = rows
      .map((row) => toFinite(row.sigScore))
      .filter((score) => score !== null)
      .sort((a, b) => a - b);

    const qualityCounts = rows.reduce((acc, row) => {
      const key = row.scoreQuality || 'unknown';
      acc[key] = (acc[key] || 0) + 1;
      return acc;
    }, {});

    const ruleCounts = rows.reduce((acc, row) => {
      const key = row.ruleVersion || 'unknown';
      acc[key] = (acc[key] || 0) + 1;
      return acc;
    }, {});

    const unusualProxy = rows.reduce((acc, row) => {
      const sigScore = toFinite(row.sigScore) || 0;
      const volOiRatio = toFinite(row.volOiRatio) || 0;
      const repeat3m = toFinite(row.repeat3m) || 0;
      if (sigScore >= 0.9) acc.highSig += 1;
      if (volOiRatio >= 2.0) acc.volOiHigh += 1;
      if (repeat3m >= 20) acc.repeatHigh += 1;
      if (sigScore >= 0.9 && (volOiRatio >= 2.0 || repeat3m >= 20)) acc.highSigConfirmed += 1;
      return acc;
    }, {
      highSig: 0,
      volOiHigh: 0,
      repeatHigh: 0,
      highSigConfirmed: 0,
    });

    const report = {
      generatedAt: new Date().toISOString(),
      dbPath,
      totalRows: rows.length,
      scoreDistribution: {
        p50: percentile(scores, 50),
        p75: percentile(scores, 75),
        p90: percentile(scores, 90),
        p95: percentile(scores, 95),
        p99: percentile(scores, 99),
      },
      qualityCounts,
      ruleCounts,
      unusualProxy,
      precisionProxy: unusualProxy.highSig
        ? unusualProxy.highSigConfirmed / unusualProxy.highSig
        : 0,
    };

    fs.mkdirSync(path.dirname(outputPath), { recursive: true });
    fs.writeFileSync(outputPath, `${JSON.stringify(report, null, 2)}\n`, 'utf8');
    console.log(JSON.stringify({ status: 'ok', outputPath, totalRows: rows.length }, null, 2));
  } finally {
    db.close();
  }
}

run();
