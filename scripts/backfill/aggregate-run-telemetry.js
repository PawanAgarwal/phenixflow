#!/usr/bin/env node

const fs = require('node:fs');
const path = require('node:path');

function parseArgs(argv) {
  const out = {};
  for (let i = 2; i < argv.length; i += 1) {
    const token = argv[i];
    if (!token.startsWith('--')) continue;
    const key = token.slice(2);
    const value = argv[i + 1];
    if (!value || value.startsWith('--')) {
      out[key] = '1';
      continue;
    }
    out[key] = value;
    i += 1;
  }
  return out;
}

function asNumber(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
}

function ensureMapAgg(map, key, seed) {
  if (!map.has(key)) map.set(key, { ...seed });
  return map.get(key);
}

function listWorkerLogs(runDir) {
  const stages = ['download', 'enrich'];
  const logs = [];
  stages.forEach((stage) => {
    const stageDir = path.join(runDir, stage);
    if (!fs.existsSync(stageDir)) return;
    const files = fs.readdirSync(stageDir)
      .filter((entry) => entry.endsWith('.log'))
      .sort()
      .map((entry) => path.join(stageDir, entry));
    logs.push(...files);
  });
  return logs;
}

function mapToSortedArray(map, comparator = null) {
  const rows = Array.from(map.entries()).map(([key, value]) => ({ key, ...value }));
  if (typeof comparator === 'function') rows.sort(comparator);
  return rows;
}

function safeJsonParse(raw) {
  try {
    return JSON.parse(raw);
  } catch {
    return null;
  }
}

function main() {
  const args = parseArgs(process.argv);
  const runDir = path.resolve(String(args['run-dir'] || '').trim());
  if (!runDir || !fs.existsSync(runDir)) {
    throw new Error('usage: --run-dir <artifacts/reports/clickhouse-historical-pipeline-...> [--out-dir dir]');
  }

  const outDir = path.resolve(args['out-dir'] || runDir);
  fs.mkdirSync(outDir, { recursive: true });

  const logs = listWorkerLogs(runDir);
  if (logs.length === 0) {
    throw new Error(`no_log_files_found:${runDir}`);
  }

  let parsedLines = 0;
  let parsedEvents = 0;
  const thetaByApi = new Map();
  const deleteByTable = new Map();

  const totals = {
    thetaDownloadDurationMs: 0,
    thetaDownloadRows: 0,
    thetaDownloadBytes: 0,
    thetaDownloadRequests: 0,
    tradeSyncJobs: 0,
    tradeParsedRows: 0,
    tradeFetchedRows: 0,
    tradeUpsertedRows: 0,
    tradeStreamDownloadDurationMs: 0,
    tradeInsertDurationMs: 0,
    tradeWallDurationMs: 0,
    quoteSyncJobs: 0,
    quoteParsedRows: 0,
    quoteInsertedRows: 0,
    quoteStreamDownloadDurationMs: 0,
    quoteInsertDurationMs: 0,
    quoteWallDurationMs: 0,
    stockSyncJobs: 0,
    stockFetchedRows: 0,
    stockInsertedRows: 0,
    stockFetchDurationMs: 0,
    stockInsertDurationMs: 0,
    stockReloadDurationMs: 0,
    deleteAuditOps: 0,
    deleteAuditDurationMs: 0,
    enrichProgressBatches: 0,
    enrichProgressRows: 0,
  };

  logs.forEach((logPath) => {
    const lines = fs.readFileSync(logPath, 'utf8').split(/\r?\n/);
    lines.forEach((line) => {
      parsedLines += 1;
      const markerMatch = line.match(/^\[([A-Z0-9_]+)\]\s+(.*)$/);
      if (!markerMatch) return;
      const marker = markerMatch[1];
      const payload = safeJsonParse(markerMatch[2]);
      if (!payload || typeof payload !== 'object') return;
      parsedEvents += 1;

      if (marker === 'THETA_DOWNLOAD') {
        const api = String(payload.api || 'unknown');
        const agg = ensureMapAgg(thetaByApi, api, {
          requests: 0,
          okRequests: 0,
          errorRequests: 0,
          rows: 0,
          bytes: 0,
          durationMs: 0,
        });
        agg.requests += 1;
        agg.okRequests += payload.ok === true ? 1 : 0;
        agg.errorRequests += payload.ok === true ? 0 : 1;
        agg.rows += asNumber(payload.rows);
        agg.bytes += asNumber(payload.bytesDownloaded);
        agg.durationMs += asNumber(payload.durationMs);

        totals.thetaDownloadRequests += 1;
        totals.thetaDownloadRows += asNumber(payload.rows);
        totals.thetaDownloadBytes += asNumber(payload.bytesDownloaded);
        totals.thetaDownloadDurationMs += asNumber(payload.durationMs);
        return;
      }

      if (marker === 'TRADE_SYNC_STATS') {
        totals.tradeSyncJobs += 1;
        totals.tradeParsedRows += asNumber(payload.parsedRows);
        totals.tradeFetchedRows += asNumber(payload.fetchedRows);
        totals.tradeUpsertedRows += asNumber(payload.upsertedRows);
        totals.tradeStreamDownloadDurationMs += asNumber(payload.streamDownloadDurationMs);
        totals.tradeInsertDurationMs += asNumber(payload.insertDurationMs);
        totals.tradeWallDurationMs += asNumber(payload.wallDurationMs);
        return;
      }

      if (marker === 'QUOTE_SYNC_STATS') {
        totals.quoteSyncJobs += 1;
        totals.quoteParsedRows += asNumber(payload.parsedRows);
        totals.quoteInsertedRows += asNumber(payload.insertedRows);
        totals.quoteStreamDownloadDurationMs += asNumber(payload.streamDownloadDurationMs);
        totals.quoteInsertDurationMs += asNumber(payload.insertDurationMs);
        totals.quoteWallDurationMs += asNumber(payload.wallDurationMs);
        return;
      }

      if (marker === 'STOCK_SYNC_STATS') {
        totals.stockSyncJobs += 1;
        totals.stockFetchedRows += asNumber(payload.fetchedRows);
        totals.stockInsertedRows += asNumber(payload.insertedRows);
        totals.stockFetchDurationMs += asNumber(payload.fetchDurationMs);
        totals.stockInsertDurationMs += asNumber(payload.insertDurationMs);
        totals.stockReloadDurationMs += asNumber(payload.reloadDurationMs);
        return;
      }

      if (marker === 'CLICKHOUSE_DELETE_AUDIT') {
        const tableName = String(payload.tableName || 'unknown');
        const agg = ensureMapAgg(deleteByTable, tableName, {
          operations: 0,
          successful: 0,
          failed: 0,
          durationMs: 0,
          rowsBeforeDelete: 0,
          rowsAfterDeleteImmediate: 0,
        });
        agg.operations += 1;
        agg.successful += payload.success === true ? 1 : 0;
        agg.failed += payload.success === true ? 0 : 1;
        agg.durationMs += asNumber(payload.durationMs);
        agg.rowsBeforeDelete += asNumber(payload.rowsBeforeDelete);
        agg.rowsAfterDeleteImmediate += asNumber(payload.rowsAfterDeleteImmediate);

        totals.deleteAuditOps += 1;
        totals.deleteAuditDurationMs += asNumber(payload.durationMs);
        return;
      }

      if (marker === 'ENRICH_BATCH_PROGRESS') {
        totals.enrichProgressBatches += 1;
        totals.enrichProgressRows += asNumber(payload.rows);
      }
    });
  });

  const thetaByApiRows = mapToSortedArray(thetaByApi, (a, b) => b.durationMs - a.durationMs)
    .map((row) => ({
      ...row,
      rowsPerSec: row.durationMs > 0
        ? Number((row.rows / (row.durationMs / 1000)).toFixed(2))
        : 0,
      bytesPerSec: row.durationMs > 0
        ? Number((row.bytes / (row.durationMs / 1000)).toFixed(2))
        : 0,
      mibPerSec: row.durationMs > 0
        ? Number(((row.bytes / (1024 * 1024)) / (row.durationMs / 1000)).toFixed(3))
        : 0,
    }));

  const summary = {
    runDir,
    parsedAt: new Date().toISOString(),
    logFiles: logs.length,
    parsedLines,
    parsedEvents,
    totals,
    derived: {
      avgThetaRowsPerRequest: totals.thetaDownloadRequests > 0
        ? Number((totals.thetaDownloadRows / totals.thetaDownloadRequests).toFixed(2))
        : 0,
      avgThetaBytesPerRequest: totals.thetaDownloadRequests > 0
        ? Number((totals.thetaDownloadBytes / totals.thetaDownloadRequests).toFixed(2))
        : 0,
      avgThetaMsPerRequest: totals.thetaDownloadRequests > 0
        ? Number((totals.thetaDownloadDurationMs / totals.thetaDownloadRequests).toFixed(2))
        : 0,
      thetaRowsPerSec: totals.thetaDownloadDurationMs > 0
        ? Number((totals.thetaDownloadRows / (totals.thetaDownloadDurationMs / 1000)).toFixed(2))
        : 0,
      thetaBytesPerSec: totals.thetaDownloadDurationMs > 0
        ? Number((totals.thetaDownloadBytes / (totals.thetaDownloadDurationMs / 1000)).toFixed(2))
        : 0,
      thetaMiBPerSec: totals.thetaDownloadDurationMs > 0
        ? Number((((totals.thetaDownloadBytes / (1024 * 1024)) / (totals.thetaDownloadDurationMs / 1000)).toFixed(3)))
        : 0,
      quoteRowsPerSecInsertPhase: totals.quoteInsertDurationMs > 0
        ? Number((totals.quoteInsertedRows / (totals.quoteInsertDurationMs / 1000)).toFixed(2))
        : 0,
      tradeRowsPerSecInsertPhase: totals.tradeInsertDurationMs > 0
        ? Number((totals.tradeUpsertedRows / (totals.tradeInsertDurationMs / 1000)).toFixed(2))
        : 0,
      stockRowsPerSecInsertPhase: totals.stockInsertDurationMs > 0
        ? Number((totals.stockInsertedRows / (totals.stockInsertDurationMs / 1000)).toFixed(2))
        : 0,
      deleteOpsPerSec: totals.deleteAuditDurationMs > 0
        ? Number((totals.deleteAuditOps / (totals.deleteAuditDurationMs / 1000)).toFixed(2))
        : 0,
    },
    thetaByApi: thetaByApiRows,
    deleteByTable: mapToSortedArray(deleteByTable, (a, b) => b.durationMs - a.durationMs),
  };

  const runLabel = path.basename(runDir).replace(/^clickhouse-historical-pipeline-/, '');
  const outPath = path.join(outDir, `telemetry-summary-${runLabel}.json`);
  fs.writeFileSync(outPath, `${JSON.stringify(summary, null, 2)}\n`, 'utf8');

  console.log(`Telemetry summary: ${outPath}`);
  console.log(JSON.stringify({
    runDir: summary.runDir,
    logFiles: summary.logFiles,
    parsedLines: summary.parsedLines,
    parsedEvents: summary.parsedEvents,
    thetaRowsPerSec: summary.derived.thetaRowsPerSec,
    thetaMiBPerSec: summary.derived.thetaMiBPerSec,
    tradeInsertRowsPerSec: summary.derived.tradeRowsPerSecInsertPhase,
    quoteInsertRowsPerSec: summary.derived.quoteRowsPerSecInsertPhase,
    stockInsertRowsPerSec: summary.derived.stockRowsPerSecInsertPhase,
    totalThetaDownloadDurationMs: summary.totals.thetaDownloadDurationMs,
    totalDeleteDurationMs: summary.totals.deleteAuditDurationMs,
  }, null, 2));
}

main();
