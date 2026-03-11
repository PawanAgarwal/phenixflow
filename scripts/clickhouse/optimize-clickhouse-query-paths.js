#!/usr/bin/env node

const { execQuerySync, queryRowsSync } = require('../../src/storage/clickhouse');

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

function parseBool(value, defaultValue = false) {
  if (value === undefined) return defaultValue;
  const raw = String(value).trim().toLowerCase();
  if (raw === '1' || raw === 'true' || raw === 'yes' || raw === 'on') return true;
  if (raw === '0' || raw === 'false' || raw === 'no' || raw === 'off') return false;
  return defaultValue;
}

function parsePartitions(value) {
  if (!value) return [];
  return String(value)
    .split(',')
    .map((token) => token.trim())
    .filter((token) => /^\d{6}$/.test(token));
}

function runQuery(sql, label) {
  execQuerySync(sql);
  if (label) {
    console.log(label);
  }
}

function ensureProjectionSettings() {
  runQuery(
    "ALTER TABLE options.option_quote_minute_raw MODIFY SETTING deduplicate_merge_projection_mode = 'rebuild'",
    'Configured projection dedup setting: option_quote_minute_raw',
  );
  runQuery(
    "ALTER TABLE options.stock_ohlc_minute_raw MODIFY SETTING deduplicate_merge_projection_mode = 'rebuild'",
    'Configured projection dedup setting: stock_ohlc_minute_raw',
  );
}

function ensureCoverageProjections() {
  runQuery(
    `ALTER TABLE options.option_quote_minute_raw
      ADD PROJECTION IF NOT EXISTS p_cov_day_symbol_minute
      (
        SELECT
          trade_date_utc,
          symbol,
          minute_bucket_utc,
          count() AS row_count
        GROUP BY
          trade_date_utc,
          symbol,
          minute_bucket_utc
      )`,
    'Ensured projection: option_quote_minute_raw.p_cov_day_symbol_minute',
  );
  runQuery(
    `ALTER TABLE options.stock_ohlc_minute_raw
      ADD PROJECTION IF NOT EXISTS p_cov_day_symbol_minute
      (
        SELECT
          trade_date_utc,
          symbol,
          minute_bucket_utc,
          count() AS row_count
        GROUP BY
          trade_date_utc,
          symbol,
          minute_bucket_utc
      )`,
    'Ensured projection: stock_ohlc_minute_raw.p_cov_day_symbol_minute',
  );
  runQuery(
    `ALTER TABLE options.option_trades
      ADD PROJECTION IF NOT EXISTS p_cov_day_symbol_minute
      (
        SELECT
          trade_date,
          symbol,
          toStartOfMinute(trade_ts_utc) AS minute_bucket_utc,
          count() AS row_count
        GROUP BY
          trade_date,
          symbol,
          minute_bucket_utc
      )`,
    'Ensured projection: option_trades.p_cov_day_symbol_minute',
  );
  runQuery(
    `ALTER TABLE options.option_trade_enriched
      ADD PROJECTION IF NOT EXISTS p_cov_day_symbol_minute
      (
        SELECT
          trade_date,
          symbol,
          toStartOfMinute(trade_ts_utc) AS minute_bucket_utc,
          count() AS row_count
        GROUP BY
          trade_date,
          symbol,
          minute_bucket_utc
      )`,
    'Ensured projection: option_trade_enriched.p_cov_day_symbol_minute',
  );
}

function materializeCoverageProjections(partitions = [], waitForMutation = false) {
  if (!Array.isArray(partitions) || partitions.length === 0) {
    return;
  }

  const tables = [
    'option_quote_minute_raw',
    'stock_ohlc_minute_raw',
    'option_trades',
    'option_trade_enriched',
  ];
  const syncClause = waitForMutation ? ' SETTINGS mutations_sync = 1' : '';

  tables.forEach((tableName) => {
    partitions.forEach((partition) => {
      const startedAt = Date.now();
      runQuery(
        `ALTER TABLE options.${tableName}
          MATERIALIZE PROJECTION p_cov_day_symbol_minute
          IN PARTITION ${partition}${syncClause}`,
      );
      const elapsedSec = ((Date.now() - startedAt) / 1000).toFixed(1);
      console.log(`Queued materialization: ${tableName} partition ${partition} (${elapsedSec}s)`);
    });
  });
}

function printMaterializationStatus() {
  const rows = queryRowsSync(`
    SELECT
      table,
      count() AS mutation_count,
      sum(parts_to_do) AS parts_to_do,
      min(is_done) AS any_open
    FROM system.mutations
    WHERE database = 'options'
      AND command ILIKE '%MATERIALIZE PROJECTION p_cov_day_symbol_minute%'
    GROUP BY table
    ORDER BY table
  `);

  if (rows.length === 0) {
    console.log('No projection materialization mutations found.');
    return;
  }

  console.log('Projection materialization status:');
  rows.forEach((row) => {
    const open = Number(row.any_open || 0) === 0 ? 'open' : 'done';
    console.log(
      `- ${row.table}: mutations=${row.mutation_count} parts_to_do=${row.parts_to_do} state=${open}`,
    );
  });
}

function main() {
  const args = parseArgs(process.argv);
  const partitions = parsePartitions(args.partitions || '');
  const materialize = parseBool(args.materialize, partitions.length > 0);
  const wait = parseBool(args.wait, false);
  const statusOnly = parseBool(args['status-only'], false);

  if (statusOnly) {
    printMaterializationStatus();
    return;
  }

  ensureProjectionSettings();
  ensureCoverageProjections();
  if (materialize) {
    materializeCoverageProjections(partitions, wait);
  }
  printMaterializationStatus();
}

main();

