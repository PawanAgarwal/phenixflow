#!/usr/bin/env node

const fs = require('node:fs');
const path = require('node:path');
const {
  queryRowsSync,
  execQuerySync,
  insertJsonRowsSync,
} = require('../../../packages/clickhouse-core');

function nowIso() {
  return new Date().toISOString();
}

function parseIntEnv(name, fallback) {
  const raw = Number(process.env[name]);
  if (!Number.isFinite(raw)) return fallback;
  return Math.trunc(raw);
}

function parseFloatEnv(name, fallback) {
  const raw = Number(process.env[name]);
  if (!Number.isFinite(raw)) return fallback;
  return raw;
}

function parseCsvEnv(name) {
  const raw = String(process.env[name] || '').trim();
  if (!raw) return [];
  return raw
    .split(',')
    .map((value) => String(value || '').trim().toUpperCase())
    .filter(Boolean);
}

function toSafeNumber(value, fallback = null) {
  const numeric = Number(value);
  return Number.isFinite(numeric) ? numeric : fallback;
}

function dayToInt(dayIso) {
  return Number(String(dayIso || '').replaceAll('-', ''));
}

function buildChunkKey({ symbol, dayIso, chunkStartMs, chunkEndMs }) {
  return `${symbol}|${dayIso}|${chunkStartMs}|${chunkEndMs}`;
}

function toUtcIso(value) {
  if (value === null || value === undefined) return null;
  const date = value instanceof Date ? value : new Date(Number(value));
  if (Number.isNaN(date.getTime())) return null;
  return date.toISOString();
}

function parseSymbolDayList(filePath) {
  const raw = fs.readFileSync(filePath, 'utf8');
  const lines = raw.split(/\r?\n/).map((line) => line.trim()).filter(Boolean);
  const jobs = [];
  for (const line of lines) {
    if (line.startsWith('#')) continue;
    if (/^symbol[\t,]/i.test(line)) continue;
    const parts = line.includes('\t') ? line.split('\t') : line.split(',');
    if (parts.length < 2) continue;
    const symbol = String(parts[0] || '').trim().toUpperCase();
    const dayIso = String(parts[1] || '').trim();
    if (!symbol || !/^\d{4}-\d{2}-\d{2}$/.test(dayIso)) continue;
    jobs.push({ symbol, dayIso });
  }
  return jobs;
}

function buildRunId() {
  const ts = new Date().toISOString().replace(/[-:]/g, '').replace(/\..+/, 'Z');
  return process.env.CALC_GREEKS_RUN_ID || `calc-greeks-${ts}`;
}

function resolveRange() {
  const range = queryRowsSync(`
    SELECT
      toString(min(trade_date_utc)) AS min_day,
      toString(max(trade_date_utc)) AS max_day
    FROM options.option_trade_day_cache
  `)[0] || {};
  const minDay = String(range.min_day || '').trim() || '2025-10-01';
  const maxDay = String(range.max_day || '').trim() || minDay;
  const startDay = String(process.env.CALC_GREEKS_START_DATE || minDay).trim();
  const endDay = String(process.env.CALC_GREEKS_END_DATE || maxDay).trim();
  return { startDay, endDay };
}

function ensureSchema() {
  execQuerySync(`
    CREATE TABLE IF NOT EXISTS options.option_calculated_greeks_minute
    (
      symbol LowCardinality(String),
      trade_date_utc Date,
      expiration Date,
      strike Float64,
      option_right Enum8('CALL' = 1, 'PUT' = -1),
      minute_bucket_utc DateTime64(3, 'UTC'),
      bid Nullable(Float64),
      ask Nullable(Float64),
      last Nullable(Float64),
      mid_price Nullable(Float64),
      underlying_price Nullable(Float64),
      risk_free_rate Nullable(Float64),
      dividend_yield Nullable(Float64),
      time_to_expiry_years Nullable(Float64),
      implied_vol Nullable(Float64),
      delta Nullable(Float64),
      gamma Nullable(Float64),
      theta_annual Nullable(Float64),
      theta_per_day Nullable(Float64),
      vega_annual Nullable(Float64),
      vega_per_1pct Nullable(Float64),
      rho_annual Nullable(Float64),
      rho_per_1pct Nullable(Float64),
      model_price Nullable(Float64),
      price_error_abs Nullable(Float64),
      iv_low Nullable(Float64),
      iv_high Nullable(Float64),
      calc_status LowCardinality(String),
      calc_run_id String,
      calc_version LowCardinality(String),
      ingested_at_utc DateTime64(3, 'UTC')
    )
    ENGINE = ReplacingMergeTree(ingested_at_utc)
    PARTITION BY toYYYYMM(trade_date_utc)
    ORDER BY (symbol, expiration, strike, option_right, trade_date_utc, minute_bucket_utc)
    SETTINGS deduplicate_merge_projection_mode = 'rebuild'
  `);

  execQuerySync(`
    CREATE TABLE IF NOT EXISTS options.option_calculated_greeks_chunk_status
    (
      symbol LowCardinality(String),
      trade_date_utc Date,
      chunk_start_utc DateTime64(3, 'UTC'),
      chunk_end_utc DateTime64(3, 'UTC'),
      chunk_index UInt16,
      chunk_total UInt16,
      chunk_minutes UInt16,
      source_mode LowCardinality(String),
      calc_run_id String,
      calc_version LowCardinality(String),
      status LowCardinality(String),
      source_rate Nullable(Float64),
      source_rate_date Nullable(Date),
      started_at_utc DateTime64(3, 'UTC'),
      completed_at_utc Nullable(DateTime64(3, 'UTC')),
      elapsed_ms UInt64,
      inserted_rows UInt64,
      solved_rows UInt64,
      missing_underlying_rows UInt64,
      missing_price_rows UInt64,
      invalid_input_rows UInt64,
      expired_rows UInt64,
      unsolved_rows UInt64,
      avg_price_error Nullable(Float64),
      p95_price_error Nullable(Float64),
      error_message Nullable(String),
      updated_at_utc DateTime64(3, 'UTC')
    )
    ENGINE = ReplacingMergeTree(updated_at_utc)
    PARTITION BY toYYYYMM(trade_date_utc)
    ORDER BY (symbol, trade_date_utc, chunk_start_utc, chunk_end_utc, chunk_minutes, source_mode, calc_version)
  `);

  execQuerySync(`
    CREATE TABLE IF NOT EXISTS options.option_calculated_greeks_day_status
    (
      symbol LowCardinality(String),
      trade_date_utc Date,
      calc_run_id String,
      calc_version LowCardinality(String),
      status LowCardinality(String),
      source_rate Nullable(Float64),
      source_rate_date Nullable(Date),
      started_at_utc DateTime64(3, 'UTC'),
      completed_at_utc Nullable(DateTime64(3, 'UTC')),
      elapsed_ms UInt64,
      inserted_rows UInt64,
      solved_rows UInt64,
      missing_underlying_rows UInt64,
      missing_price_rows UInt64,
      invalid_input_rows UInt64,
      expired_rows UInt64,
      unsolved_rows UInt64,
      avg_price_error Nullable(Float64),
      p95_price_error Nullable(Float64),
      error_message Nullable(String),
      updated_at_utc DateTime64(3, 'UTC')
    )
    ENGINE = ReplacingMergeTree(updated_at_utc)
    PARTITION BY toYYYYMM(trade_date_utc)
    ORDER BY (symbol, trade_date_utc, calc_run_id)
  `);
}

function ensureFunctions() {
  execQuerySync(`
    CREATE FUNCTION IF NOT EXISTS bs_norm_cdf
    AS (x) -> (0.5 * (1 + erf(x / sqrt(2))))
  `);
  execQuerySync(`
    CREATE FUNCTION IF NOT EXISTS bs_norm_pdf
    AS (x) -> (exp(-0.5 * x * x) / sqrt(2 * pi()))
  `);
  execQuerySync(`
    CREATE FUNCTION IF NOT EXISTS bs_d1
    AS (sigma, s, k, r, q, t) -> ((log(s / k) + (r - q + 0.5 * sigma * sigma) * t) / (sigma * sqrt(t)))
  `);
  execQuerySync(`
    CREATE FUNCTION IF NOT EXISTS bs_d2
    AS (sigma, s, k, r, q, t) -> (bs_d1(sigma, s, k, r, q, t) - sigma * sqrt(t))
  `);
  execQuerySync(`
    CREATE FUNCTION IF NOT EXISTS bs_price
    AS (sigma, s, k, r, q, t, is_call) -> if(
      is_call = 1,
      (s * exp(-q * t) * bs_norm_cdf(bs_d1(sigma, s, k, r, q, t)))
        - (k * exp(-r * t) * bs_norm_cdf(bs_d2(sigma, s, k, r, q, t))),
      (k * exp(-r * t) * bs_norm_cdf(-bs_d2(sigma, s, k, r, q, t)))
        - (s * exp(-q * t) * bs_norm_cdf(-bs_d1(sigma, s, k, r, q, t)))
    )
  `);
  execQuerySync(`
    CREATE FUNCTION IF NOT EXISTS bs_delta
    AS (sigma, s, k, r, q, t, is_call) -> if(
      is_call = 1,
      exp(-q * t) * bs_norm_cdf(bs_d1(sigma, s, k, r, q, t)),
      exp(-q * t) * (bs_norm_cdf(bs_d1(sigma, s, k, r, q, t)) - 1)
    )
  `);
  execQuerySync(`
    CREATE FUNCTION IF NOT EXISTS bs_gamma
    AS (sigma, s, k, r, q, t) -> (exp(-q * t) * bs_norm_pdf(bs_d1(sigma, s, k, r, q, t)) / (s * sigma * sqrt(t)))
  `);
  execQuerySync(`
    CREATE FUNCTION IF NOT EXISTS bs_vega
    AS (sigma, s, k, r, q, t) -> (s * exp(-q * t) * bs_norm_pdf(bs_d1(sigma, s, k, r, q, t)) * sqrt(t))
  `);
  execQuerySync(`
    CREATE FUNCTION IF NOT EXISTS bs_theta_annual
    AS (sigma, s, k, r, q, t, is_call) -> if(
      is_call = 1,
      (-(s * exp(-q * t) * bs_norm_pdf(bs_d1(sigma, s, k, r, q, t)) * sigma) / (2 * sqrt(t)))
        - (r * k * exp(-r * t) * bs_norm_cdf(bs_d2(sigma, s, k, r, q, t)))
        + (q * s * exp(-q * t) * bs_norm_cdf(bs_d1(sigma, s, k, r, q, t))),
      (-(s * exp(-q * t) * bs_norm_pdf(bs_d1(sigma, s, k, r, q, t)) * sigma) / (2 * sqrt(t)))
        + (r * k * exp(-r * t) * bs_norm_cdf(-bs_d2(sigma, s, k, r, q, t)))
        - (q * s * exp(-q * t) * bs_norm_cdf(-bs_d1(sigma, s, k, r, q, t)))
    )
  `);
  execQuerySync(`
    CREATE FUNCTION IF NOT EXISTS bs_rho_annual
    AS (sigma, s, k, r, q, t, is_call) -> if(
      is_call = 1,
      (k * t * exp(-r * t) * bs_norm_cdf(bs_d2(sigma, s, k, r, q, t))),
      (-k * t * exp(-r * t) * bs_norm_cdf(-bs_d2(sigma, s, k, r, q, t)))
    )
  `);
  execQuerySync(`
    CREATE FUNCTION IF NOT EXISTS bs_iv_bounds_cfg
    AS (target, s, k, r, q, t, is_call, iv_low, iv_high, iterations) -> arrayFold(
      (acc, step) -> if(
        bs_price(((tupleElement(acc, 1) + tupleElement(acc, 2)) / 2), s, k, r, q, t, is_call) > target,
        tuple(tupleElement(acc, 1), ((tupleElement(acc, 1) + tupleElement(acc, 2)) / 2)),
        tuple(((tupleElement(acc, 1) + tupleElement(acc, 2)) / 2), tupleElement(acc, 2))
      ),
      range(greatest(toUInt32(1), iterations)),
      tuple(iv_low, iv_high)
    )
  `);
}

function writeStatusRow(row) {
  insertJsonRowsSync(`
    INSERT INTO options.option_calculated_greeks_day_status (
      symbol,
      trade_date_utc,
      calc_run_id,
      calc_version,
      status,
      source_rate,
      source_rate_date,
      started_at_utc,
      completed_at_utc,
      elapsed_ms,
      inserted_rows,
      solved_rows,
      missing_underlying_rows,
      missing_price_rows,
      invalid_input_rows,
      expired_rows,
      unsolved_rows,
      avg_price_error,
      p95_price_error,
      error_message,
      updated_at_utc
    )
  `, [row]);
}

function writeChunkStatusRow(row) {
  insertJsonRowsSync(`
    INSERT INTO options.option_calculated_greeks_chunk_status (
      symbol,
      trade_date_utc,
      chunk_start_utc,
      chunk_end_utc,
      chunk_index,
      chunk_total,
      chunk_minutes,
      source_mode,
      calc_run_id,
      calc_version,
      status,
      source_rate,
      source_rate_date,
      started_at_utc,
      completed_at_utc,
      elapsed_ms,
      inserted_rows,
      solved_rows,
      missing_underlying_rows,
      missing_price_rows,
      invalid_input_rows,
      expired_rows,
      unsolved_rows,
      avg_price_error,
      p95_price_error,
      error_message,
      updated_at_utc
    )
  `, [row]);
}

function loadJobs({ startDay, endDay, symbolDayListPath }) {
  if (symbolDayListPath) {
    const rawJobs = parseSymbolDayList(symbolDayListPath);
    const startInt = dayToInt(startDay);
    const endInt = dayToInt(endDay);
    return rawJobs
      .filter((job) => {
        const dayInt = dayToInt(job.dayIso);
        return dayInt >= startInt && dayInt <= endInt;
      })
      .sort((a, b) => (a.dayIso === b.dayIso ? a.symbol.localeCompare(b.symbol) : a.dayIso.localeCompare(b.dayIso)));
  }
  return queryRowsSync(`
    SELECT
      symbol,
      toString(trade_date_utc) AS dayIso
    FROM options.option_trade_day_cache
    WHERE trade_date_utc >= toDate({startDay:String})
      AND trade_date_utc <= toDate({endDay:String})
    GROUP BY symbol, trade_date_utc
    ORDER BY trade_date_utc ASC, symbol ASC
  `, { startDay, endDay }).map((row) => ({
    symbol: String(row.symbol || '').trim().toUpperCase(),
    dayIso: String(row.dayIso || '').trim(),
  })).filter((row) => row.symbol && /^\d{4}-\d{2}-\d{2}$/.test(row.dayIso));
}

function loadCompletedSet({ startDay, endDay }) {
  const rows = queryRowsSync(`
    SELECT
      symbol,
      toString(trade_date_utc) AS dayIso
    FROM options.option_calculated_greeks_day_status
    WHERE trade_date_utc >= toDate({startDay:String})
      AND trade_date_utc <= toDate({endDay:String})
      AND status = 'complete'
    GROUP BY symbol, trade_date_utc
  `, { startDay, endDay });
  return new Set(rows.map((row) => `${row.symbol}|${row.dayIso}`));
}

function loadChunkCompletedDaySet({ startDay, endDay, chunkMinutes, sourceMode, calcVersion }) {
  const rows = queryRowsSync(`
    SELECT
      symbol,
      toString(trade_date_utc) AS dayIso
    FROM options.option_calculated_greeks_chunk_status FINAL
    WHERE trade_date_utc >= toDate({startDay:String})
      AND trade_date_utc <= toDate({endDay:String})
      AND chunk_minutes = {chunkMinutes:UInt16}
      AND source_mode = {sourceMode:String}
      AND calc_version = {calcVersion:String}
    GROUP BY symbol, trade_date_utc
    HAVING count() > 0
      AND count() = max(chunk_total)
      AND countIf(status = 'complete') = max(chunk_total)
  `, {
    startDay,
    endDay,
    chunkMinutes,
    sourceMode,
    calcVersion,
  });
  return new Set(rows.map((row) => `${row.symbol}|${row.dayIso}`));
}

function loadCompletedChunkSet({ startDay, endDay, chunkMinutes, sourceMode, calcVersion }) {
  const rows = queryRowsSync(`
    SELECT
      symbol,
      toString(trade_date_utc) AS dayIso,
      toInt64(toUnixTimestamp64Milli(chunk_start_utc)) AS chunkStartMs,
      toInt64(toUnixTimestamp64Milli(chunk_end_utc)) AS chunkEndMs
    FROM options.option_calculated_greeks_chunk_status FINAL
    WHERE trade_date_utc >= toDate({startDay:String})
      AND trade_date_utc <= toDate({endDay:String})
      AND chunk_minutes = {chunkMinutes:UInt16}
      AND source_mode = {sourceMode:String}
      AND calc_version = {calcVersion:String}
      AND status = 'complete'
  `, {
    startDay,
    endDay,
    chunkMinutes,
    sourceMode,
    calcVersion,
  });
  return new Set(rows.map((row) => buildChunkKey({
    symbol: String(row.symbol || '').trim().toUpperCase(),
    dayIso: String(row.dayIso || '').trim(),
    chunkStartMs: toSafeNumber(row.chunkStartMs, 0) || 0,
    chunkEndMs: toSafeNumber(row.chunkEndMs, 0) || 0,
  })));
}

function loadChunkWindowsForDay({ symbol, dayIso, sourceMode, chunkMinutes }) {
  const safeChunkMinutes = Math.max(1, Math.trunc(chunkMinutes));
  const rangeRow = sourceMode === 'trade_minute'
    ? (queryRowsSync(`
        SELECT
          min(toUnixTimestamp64Milli(toStartOfMinute(trade_ts_utc))) AS minMs,
          max(toUnixTimestamp64Milli(toStartOfMinute(trade_ts_utc))) AS maxMs
        FROM options.option_trades
        WHERE symbol = {symbol:String}
          AND trade_date = toDate({dayIso:String})
      `, { symbol, dayIso })[0] || {})
    : (queryRowsSync(`
        SELECT
          min(toUnixTimestamp64Milli(minute_bucket_utc)) AS minMs,
          max(toUnixTimestamp64Milli(minute_bucket_utc)) AS maxMs
        FROM options.option_quote_minute_raw
        WHERE symbol = {symbol:String}
          AND trade_date_utc = toDate({dayIso:String})
      `, { symbol, dayIso })[0] || {});

  const minMs = toSafeNumber(rangeRow.minMs, null);
  const maxMs = toSafeNumber(rangeRow.maxMs, null);
  if (minMs === null || maxMs === null) return [];

  const chunkDurationMs = safeChunkMinutes * 60 * 1000;
  const endExclusiveMs = maxMs + (60 * 1000);
  const windows = [];
  for (let chunkStartMs = minMs; chunkStartMs < endExclusiveMs; chunkStartMs += chunkDurationMs) {
    const chunkEndMs = Math.min(chunkStartMs + chunkDurationMs, endExclusiveMs);
    windows.push({
      chunkStartMs,
      chunkEndMs,
      chunkMinutes: safeChunkMinutes,
    });
  }
  return windows.map((window, index) => ({
    ...window,
    chunkIndex: index + 1,
    chunkTotal: windows.length,
  }));
}

function expandJobsForChunking(jobs, { sourceMode, chunkMinutes, chunkSymbols }) {
  const useChunking = chunkMinutes > 0;
  if (!useChunking) return jobs;
  const chunkSymbolSet = new Set((chunkSymbols || []).map((value) => String(value || '').trim().toUpperCase()).filter(Boolean));
  const shouldFilterSymbols = chunkSymbolSet.size > 0;

  return jobs.flatMap((job) => {
    if (shouldFilterSymbols && !chunkSymbolSet.has(job.symbol)) {
      return [job];
    }

    const windows = loadChunkWindowsForDay({
      symbol: job.symbol,
      dayIso: job.dayIso,
      sourceMode,
      chunkMinutes,
    });
    if (windows.length === 0) {
      return [job];
    }

    return windows.map((window) => ({
      ...job,
      chunkStartMs: window.chunkStartMs,
      chunkEndMs: window.chunkEndMs,
      chunkIndex: window.chunkIndex,
      chunkTotal: window.chunkTotal,
      chunkMinutes: window.chunkMinutes,
    }));
  });
}

function resolveRateForDay(dayIso, fallbackRate) {
  const rows = queryRowsSync(`
    SELECT
      rate_decimal AS rate,
      toString(effective_date) AS rate_day
    FROM options.reference_sofr_daily
    WHERE effective_date <= toDate({dayIso:String})
    ORDER BY effective_date DESC
    LIMIT 1
  `, { dayIso });
  if (!rows.length) {
    return { rate: fallbackRate, rateDay: null, source: 'fallback' };
  }
  return {
    rate: toSafeNumber(rows[0].rate, fallbackRate),
    rateDay: rows[0].rate_day || null,
    source: 'reference_sofr_daily',
  };
}

function insertCalculatedGreeksForJob({
  symbol,
  dayIso,
  chunkStartMs = null,
  chunkEndMs = null,
  runId,
  calcVersion,
  sourceMode,
  rate,
  dividendYield,
  ivLow,
  ivHigh,
  ivIterations,
  maxThreads,
  maxMemoryBytes,
}) {
  const safeMaxThreads = Math.max(1, Math.trunc(maxThreads));
  const safeMaxMemoryBytes = Math.max(512 * 1024 * 1024, Math.trunc(maxMemoryBytes));
  const hasChunkBounds = Number.isFinite(chunkStartMs) && Number.isFinite(chunkEndMs) && chunkEndMs > chunkStartMs;
  const tradeChunkWhereSql = hasChunkBounds
    ? `
                AND trade_ts_utc >= toDateTime64({chunkStartMs:Int64} / 1000.0, 3, 'UTC')
                AND trade_ts_utc < toDateTime64({chunkEndMs:Int64} / 1000.0, 3, 'UTC')
      `
    : '';
  const minuteChunkWhereSql = hasChunkBounds
    ? `
                AND minute_bucket_utc >= toDateTime64({chunkStartMs:Int64} / 1000.0, 3, 'UTC')
                AND minute_bucket_utc < toDateTime64({chunkEndMs:Int64} / 1000.0, 3, 'UTC')
      `
    : '';
  const sourceSubquery = sourceMode === 'trade_minute'
    ? `
              SELECT
                symbol,
                toDate(trade_ts_utc) AS trade_date_utc,
                expiration,
                strike,
                option_right,
                toStartOfMinute(trade_ts_utc) AS minute_bucket_utc,
                argMax(bid, trade_ts_utc) AS bid,
                argMax(ask, trade_ts_utc) AS ask,
                argMax(price, trade_ts_utc) AS last
              FROM options.option_trades
              WHERE symbol = {symbol:String}
                AND trade_date = toDate({dayIso:String})
${tradeChunkWhereSql}
              GROUP BY
                symbol,
                trade_date_utc,
                expiration,
                strike,
                option_right,
                minute_bucket_utc
      `
    : `
              SELECT
                symbol,
                trade_date_utc,
                expiration,
                strike,
                option_right,
                minute_bucket_utc,
                argMax(bid, ingested_at_utc) AS bid,
                argMax(ask, ingested_at_utc) AS ask,
                argMax(last, ingested_at_utc) AS last
              FROM options.option_quote_minute_raw
              WHERE symbol = {symbol:String}
                AND trade_date_utc = toDate({dayIso:String})
${minuteChunkWhereSql}
              GROUP BY
                symbol,
                trade_date_utc,
                expiration,
                strike,
                option_right,
                minute_bucket_utc
      `;
  execQuerySync(`
    INSERT INTO options.option_calculated_greeks_minute (
      symbol,
      trade_date_utc,
      expiration,
      strike,
      option_right,
      minute_bucket_utc,
      bid,
      ask,
      last,
      mid_price,
      underlying_price,
      risk_free_rate,
      dividend_yield,
      time_to_expiry_years,
      implied_vol,
      delta,
      gamma,
      theta_annual,
      theta_per_day,
      vega_annual,
      vega_per_1pct,
      rho_annual,
      rho_per_1pct,
      model_price,
      price_error_abs,
      iv_low,
      iv_high,
      calc_status,
      calc_run_id,
      calc_version,
      ingested_at_utc
    )
    WITH
      toFloat64({riskFree:Float64}) AS risk_free_rate_const,
      toFloat64({dividendYield:Float64}) AS dividend_yield_const,
      toFloat64({ivLow:Float64}) AS iv_low_const,
      toFloat64({ivHigh:Float64}) AS iv_high_const,
      toUInt32({ivIterations:UInt32}) AS iv_iterations_const,
      toFloat64(1e-9) AS eps
    SELECT
      l3.symbol,
      l3.trade_date_utc,
      l3.expiration,
      l3.strike,
      l3.option_right,
      l3.minute_bucket_utc,
      l3.bid,
      l3.ask,
      l3.last,
      l3.mid_price,
      l3.underlying_price,
      risk_free_rate_const AS risk_free_rate,
      dividend_yield_const AS dividend_yield,
      l3.t_years AS time_to_expiry_years,
      l3.implied_vol,
      if(l3.calc_status = 'ok', bs_delta(l3.implied_vol, l3.underlying_price, l3.strike, risk_free_rate_const, dividend_yield_const, l3.t_years, l3.is_call), NULL) AS delta,
      if(l3.calc_status = 'ok', bs_gamma(l3.implied_vol, l3.underlying_price, l3.strike, risk_free_rate_const, dividend_yield_const, l3.t_years), NULL) AS gamma,
      if(l3.calc_status = 'ok', bs_theta_annual(l3.implied_vol, l3.underlying_price, l3.strike, risk_free_rate_const, dividend_yield_const, l3.t_years, l3.is_call), NULL) AS theta_annual,
      if(l3.calc_status = 'ok', (bs_theta_annual(l3.implied_vol, l3.underlying_price, l3.strike, risk_free_rate_const, dividend_yield_const, l3.t_years, l3.is_call) / 365.0), NULL) AS theta_per_day,
      if(l3.calc_status = 'ok', bs_vega(l3.implied_vol, l3.underlying_price, l3.strike, risk_free_rate_const, dividend_yield_const, l3.t_years), NULL) AS vega_annual,
      if(l3.calc_status = 'ok', (bs_vega(l3.implied_vol, l3.underlying_price, l3.strike, risk_free_rate_const, dividend_yield_const, l3.t_years) / 100.0), NULL) AS vega_per_1pct,
      if(l3.calc_status = 'ok', bs_rho_annual(l3.implied_vol, l3.underlying_price, l3.strike, risk_free_rate_const, dividend_yield_const, l3.t_years, l3.is_call), NULL) AS rho_annual,
      if(l3.calc_status = 'ok', (bs_rho_annual(l3.implied_vol, l3.underlying_price, l3.strike, risk_free_rate_const, dividend_yield_const, l3.t_years, l3.is_call) / 100.0), NULL) AS rho_per_1pct,
      l3.model_price,
      if(l3.model_price IS NULL OR l3.mid_price IS NULL, NULL, abs(l3.model_price - l3.mid_price)) AS price_error_abs,
      if(l3.price_in_range, tupleElement(l3.iv_bounds, 1), NULL) AS iv_low,
      if(l3.price_in_range, tupleElement(l3.iv_bounds, 2), NULL) AS iv_high,
      l3.calc_status,
      {runId:String} AS calc_run_id,
      {calcVersion:String} AS calc_version,
      toDateTime64(now64(3), 3, 'UTC') AS ingested_at_utc
    FROM (
      SELECT
        l2.*,
        if(l2.price_in_range, ((tupleElement(l2.iv_bounds, 1) + tupleElement(l2.iv_bounds, 2)) / 2), NULL) AS implied_vol,
        if(
          l2.price_in_range,
          bs_price(
            ((tupleElement(l2.iv_bounds, 1) + tupleElement(l2.iv_bounds, 2)) / 2),
            l2.underlying_price,
            l2.strike,
            risk_free_rate_const,
            dividend_yield_const,
            l2.t_years,
            l2.is_call
          ),
          NULL
        ) AS model_price,
        multiIf(
          l2.mid_price IS NULL, 'missing_price',
          l2.underlying_price IS NULL OR l2.underlying_price <= 0 OR l2.strike <= 0, 'missing_underlying',
          l2.t_years <= 0, 'expired',
          NOT l2.price_in_range, 'invalid_input',
          'ok'
        ) AS calc_status
      FROM (
        SELECT
          base.*,
          if(
            base.price_in_range,
            bs_iv_bounds_cfg(
              base.mid_price,
              base.underlying_price,
              base.strike,
              risk_free_rate_const,
              dividend_yield_const,
              base.t_years,
              base.is_call,
              iv_low_const,
              iv_high_const,
              iv_iterations_const
            ),
            tuple(toFloat64(0), toFloat64(0))
          ) AS iv_bounds
        FROM (
          SELECT
            priced.*,
            (
              priced.mid_price IS NOT NULL
              AND priced.underlying_price > 0
              AND priced.strike > 0
              AND priced.t_years > 0
              AND priced.intrinsic_price IS NOT NULL
              AND priced.upper_bound_price IS NOT NULL
              AND priced.mid_price > (priced.intrinsic_price + eps)
              AND priced.mid_price < (priced.upper_bound_price - eps)
            ) AS price_in_range
          FROM (
            SELECT
              base.*,
              if(
                base.underlying_price > 0 AND base.strike > 0 AND base.t_years > 0,
                if(
                  base.option_right = 'CALL',
                  greatest(
                    (base.underlying_price * exp(-dividend_yield_const * base.t_years))
                      - (base.strike * exp(-risk_free_rate_const * base.t_years)),
                    0.0
                  ),
                  greatest(
                    (base.strike * exp(-risk_free_rate_const * base.t_years))
                      - (base.underlying_price * exp(-dividend_yield_const * base.t_years)),
                    0.0
                  )
                ),
                NULL
              ) AS intrinsic_price,
              if(
                base.underlying_price > 0 AND base.strike > 0 AND base.t_years > 0,
                if(
                  base.option_right = 'CALL',
                  (base.underlying_price * exp(-dividend_yield_const * base.t_years)),
                  (base.strike * exp(-risk_free_rate_const * base.t_years))
                ),
                NULL
              ) AS upper_bound_price
            FROM (
              SELECT
                q.symbol,
                q.trade_date_utc,
                q.expiration,
                q.strike,
                q.option_right,
                q.minute_bucket_utc,
                q.bid,
                q.ask,
                q.last,
                if((q.bid > 0) AND (q.ask > 0), (q.bid + q.ask) / 2, if(q.last > 0, q.last, NULL)) AS mid_price,
                if(isNull(s.close), NULL, toFloat64(s.close)) AS underlying_price,
                greatest(
                  0.0,
                  dateDiff(
                    'second',
                    q.minute_bucket_utc,
                    toDateTime64(
                      toTimeZone(toDateTime(q.expiration, 'America/New_York') + toIntervalHour(16), 'UTC'),
                      3,
                      'UTC'
                    )
                  ) / 31557600.0
                ) AS t_years,
                if(q.option_right = 'CALL', 1, 0) AS is_call
              FROM (
${sourceSubquery}
              ) AS q
              LEFT JOIN (
                SELECT
                  symbol,
                  trade_date_utc,
                  minute_bucket_utc,
                  argMax(close, ingested_at_utc) AS close
                FROM options.stock_ohlc_minute_raw
                WHERE symbol = {symbol:String}
                  AND trade_date_utc = toDate({dayIso:String})
${minuteChunkWhereSql}
                GROUP BY
                  symbol,
                  trade_date_utc,
                  minute_bucket_utc
              ) AS s
                ON q.symbol = s.symbol
               AND q.trade_date_utc = s.trade_date_utc
               AND q.minute_bucket_utc = s.minute_bucket_utc
            ) AS base
          ) AS priced
        ) AS base
      ) AS l2
    ) AS l3
    SETTINGS
      short_circuit_function_evaluation = 'enable',
      max_threads = ${safeMaxThreads},
      max_memory_usage = ${safeMaxMemoryBytes}
  `, {
    symbol,
    dayIso,
    chunkStartMs,
    chunkEndMs,
    runId,
    calcVersion,
    riskFree: rate,
    dividendYield,
    ivLow,
    ivHigh,
    ivIterations,
  });
}

function summarizeJobRows({ symbol, dayIso, runId }) {
  const row = queryRowsSync(`
    SELECT
      count() AS insertedRows,
      countIf(calc_status = 'ok') AS solvedRows,
      countIf(calc_status = 'missing_underlying') AS missingUnderlyingRows,
      countIf(calc_status = 'missing_price') AS missingPriceRows,
      countIf(calc_status = 'invalid_input') AS invalidInputRows,
      countIf(calc_status = 'expired') AS expiredRows,
      countIf(calc_status = 'unsolved') AS unsolvedRows,
      avgOrNull(price_error_abs) AS avgPriceError,
      quantileTDigest(0.95)(price_error_abs) AS p95PriceError
    FROM options.option_calculated_greeks_minute
    WHERE calc_run_id = {runId:String}
      AND symbol = {symbol:String}
      AND trade_date_utc = toDate({dayIso:String})
  `, { runId, symbol, dayIso })[0] || {};
  return {
    insertedRows: toSafeNumber(row.insertedRows, 0) || 0,
    solvedRows: toSafeNumber(row.solvedRows, 0) || 0,
    missingUnderlyingRows: toSafeNumber(row.missingUnderlyingRows, 0) || 0,
    missingPriceRows: toSafeNumber(row.missingPriceRows, 0) || 0,
    invalidInputRows: toSafeNumber(row.invalidInputRows, 0) || 0,
    expiredRows: toSafeNumber(row.expiredRows, 0) || 0,
    unsolvedRows: toSafeNumber(row.unsolvedRows, 0) || 0,
    avgPriceError: toSafeNumber(row.avgPriceError, null),
    p95PriceError: toSafeNumber(row.p95PriceError, null),
  };
}

function summarizeChunkRows({ symbol, dayIso, runId, chunkStartMs, chunkEndMs }) {
  const row = queryRowsSync(`
    SELECT
      count() AS insertedRows,
      countIf(calc_status = 'ok') AS solvedRows,
      countIf(calc_status = 'missing_underlying') AS missingUnderlyingRows,
      countIf(calc_status = 'missing_price') AS missingPriceRows,
      countIf(calc_status = 'invalid_input') AS invalidInputRows,
      countIf(calc_status = 'expired') AS expiredRows,
      countIf(calc_status = 'unsolved') AS unsolvedRows,
      avgOrNull(price_error_abs) AS avgPriceError,
      quantileTDigest(0.95)(price_error_abs) AS p95PriceError
    FROM options.option_calculated_greeks_minute
    WHERE calc_run_id = {runId:String}
      AND symbol = {symbol:String}
      AND trade_date_utc = toDate({dayIso:String})
      AND minute_bucket_utc >= toDateTime64({chunkStartMs:Int64} / 1000.0, 3, 'UTC')
      AND minute_bucket_utc < toDateTime64({chunkEndMs:Int64} / 1000.0, 3, 'UTC')
  `, {
    runId,
    symbol,
    dayIso,
    chunkStartMs,
    chunkEndMs,
  })[0] || {};

  return {
    insertedRows: toSafeNumber(row.insertedRows, 0) || 0,
    solvedRows: toSafeNumber(row.solvedRows, 0) || 0,
    missingUnderlyingRows: toSafeNumber(row.missingUnderlyingRows, 0) || 0,
    missingPriceRows: toSafeNumber(row.missingPriceRows, 0) || 0,
    invalidInputRows: toSafeNumber(row.invalidInputRows, 0) || 0,
    expiredRows: toSafeNumber(row.expiredRows, 0) || 0,
    unsolvedRows: toSafeNumber(row.unsolvedRows, 0) || 0,
    avgPriceError: toSafeNumber(row.avgPriceError, null),
    p95PriceError: toSafeNumber(row.p95PriceError, null),
  };
}

function reconcileDayStatusFromChunks({
  symbol,
  dayIso,
  runId,
  calcVersion,
  sourceMode,
  chunkMinutes,
  sourceRate,
  sourceRateDate,
}) {
  const row = queryRowsSync(`
    SELECT
      count() AS chunkRows,
      max(chunk_total) AS expectedChunks,
      countIf(status = 'complete') AS completeChunks,
      countIf(status = 'running') AS runningChunks,
      countIf(status = 'failed') AS failedChunks,
      min(started_at_utc) AS startedAtUtc,
      max(completed_at_utc) AS completedAtUtc,
      sumIf(elapsed_ms, status = 'complete') AS elapsedMs,
      sumIf(inserted_rows, status = 'complete') AS insertedRows,
      sumIf(solved_rows, status = 'complete') AS solvedRows,
      sumIf(missing_underlying_rows, status = 'complete') AS missingUnderlyingRows,
      sumIf(missing_price_rows, status = 'complete') AS missingPriceRows,
      sumIf(invalid_input_rows, status = 'complete') AS invalidInputRows,
      sumIf(expired_rows, status = 'complete') AS expiredRows,
      sumIf(unsolved_rows, status = 'complete') AS unsolvedRows,
      avgIf(avg_price_error, status = 'complete') AS avgPriceError,
      maxIf(p95_price_error, status = 'complete') AS p95PriceError,
      argMaxIf(error_message, updated_at_utc, status = 'failed') AS errorMessage,
      max(updated_at_utc) AS updatedAtUtc
    FROM options.option_calculated_greeks_chunk_status FINAL
    WHERE symbol = {symbol:String}
      AND trade_date_utc = toDate({dayIso:String})
      AND chunk_minutes = {chunkMinutes:UInt16}
      AND source_mode = {sourceMode:String}
      AND calc_version = {calcVersion:String}
  `, {
    symbol,
    dayIso,
    chunkMinutes,
    sourceMode,
    calcVersion,
  })[0] || {};

  const expectedChunks = toSafeNumber(row.expectedChunks, 0) || 0;
  const completeChunks = toSafeNumber(row.completeChunks, 0) || 0;
  const runningChunks = toSafeNumber(row.runningChunks, 0) || 0;
  const failedChunks = toSafeNumber(row.failedChunks, 0) || 0;
  let status = 'running';
  if (expectedChunks > 0 && completeChunks >= expectedChunks) {
    status = 'complete';
  } else if (runningChunks > 0) {
    status = 'running';
  } else if (failedChunks > 0) {
    status = 'failed';
  }

  writeStatusRow({
    symbol,
    trade_date_utc: dayIso,
    calc_run_id: runId,
    calc_version: calcVersion,
    status,
    source_rate: sourceRate,
    source_rate_date: sourceRateDate,
    started_at_utc: row.startedAtUtc || nowIso(),
    completed_at_utc: status === 'complete' ? (row.completedAtUtc || nowIso()) : null,
    elapsed_ms: toSafeNumber(row.elapsedMs, 0) || 0,
    inserted_rows: toSafeNumber(row.insertedRows, 0) || 0,
    solved_rows: toSafeNumber(row.solvedRows, 0) || 0,
    missing_underlying_rows: toSafeNumber(row.missingUnderlyingRows, 0) || 0,
    missing_price_rows: toSafeNumber(row.missingPriceRows, 0) || 0,
    invalid_input_rows: toSafeNumber(row.invalidInputRows, 0) || 0,
    expired_rows: toSafeNumber(row.expiredRows, 0) || 0,
    unsolved_rows: toSafeNumber(row.unsolvedRows, 0) || 0,
    avg_price_error: toSafeNumber(row.avgPriceError, null),
    p95_price_error: toSafeNumber(row.p95PriceError, null),
    error_message: status === 'failed' ? (row.errorMessage || 'chunk_failure') : null,
    updated_at_utc: row.updatedAtUtc || nowIso(),
  });

  return {
    status,
    expectedChunks,
    completeChunks,
    runningChunks,
    failedChunks,
    insertedRows: toSafeNumber(row.insertedRows, 0) || 0,
    solvedRows: toSafeNumber(row.solvedRows, 0) || 0,
  };
}

function summarizeRun(runId) {
  const totals = queryRowsSync(`
    SELECT
      count() AS rows,
      countIf(calc_status = 'ok') AS solved_rows,
      countIf(calc_status = 'missing_underlying') AS missing_underlying_rows,
      countIf(calc_status = 'missing_price') AS missing_price_rows,
      countIf(calc_status = 'invalid_input') AS invalid_input_rows,
      countIf(calc_status = 'expired') AS expired_rows,
      countIf(calc_status = 'unsolved') AS unsolved_rows
    FROM options.option_calculated_greeks_minute
    WHERE calc_run_id = {runId:String}
  `, { runId })[0] || {};

  const monthly = queryRowsSync(`
    SELECT
      toStartOfMonth(trade_date_utc) AS month,
      count() AS rows,
      countIf(calc_status = 'ok') AS solved_rows
    FROM options.option_calculated_greeks_minute
    WHERE calc_run_id = {runId:String}
    GROUP BY month
    ORDER BY month
  `, { runId });

  return {
    rows: toSafeNumber(totals.rows, 0) || 0,
    solvedRows: toSafeNumber(totals.solved_rows, 0) || 0,
    missingUnderlyingRows: toSafeNumber(totals.missing_underlying_rows, 0) || 0,
    missingPriceRows: toSafeNumber(totals.missing_price_rows, 0) || 0,
    invalidInputRows: toSafeNumber(totals.invalid_input_rows, 0) || 0,
    expiredRows: toSafeNumber(totals.expired_rows, 0) || 0,
    unsolvedRows: toSafeNumber(totals.unsolved_rows, 0) || 0,
    monthly,
  };
}

function writeReport(reportPath, payload) {
  const resolved = path.resolve(reportPath);
  fs.mkdirSync(path.dirname(resolved), { recursive: true });
  fs.writeFileSync(resolved, `${JSON.stringify(payload, null, 2)}\n`, 'utf8');
  return resolved;
}

function main() {
  const runId = buildRunId();
  const calcVersion = String(process.env.CALC_GREEKS_VERSION || 'bs_v1').trim() || 'bs_v1';
  const workerTotal = Math.max(1, parseIntEnv('CALC_GREEKS_WORKER_TOTAL', parseIntEnv('BACKFILL_WORKER_TOTAL', 1)));
  const workerIndex = Math.max(0, parseIntEnv('CALC_GREEKS_WORKER_INDEX', parseIntEnv('BACKFILL_WORKER_INDEX', 0)));
  if (workerIndex >= workerTotal) {
    throw new Error(`invalid_worker_index:${workerIndex}/${workerTotal}`);
  }
  const reportPath = process.env.CALC_GREEKS_REPORT_PATH
    || path.resolve(process.cwd(), 'artifacts', 'reports', `calculated-greeks-${runId}-worker${workerIndex}.json`);
  const symbolDayListPath = String(process.env.CALC_GREEKS_SYMBOL_DAY_LIST_PATH || process.env.BACKFILL_SYMBOL_DAY_LIST_PATH || '').trim();
  const skipCompleted = String(process.env.CALC_GREEKS_SKIP_COMPLETED || '1') === '1';
  const fallbackRate = parseFloatEnv('CALC_GREEKS_FALLBACK_RATE', 0.0);
  const dividendYield = parseFloatEnv('CALC_GREEKS_DIVIDEND_YIELD', 0.0);
  const sourceMode = String(process.env.CALC_GREEKS_SOURCE || 'quote_minute').trim().toLowerCase();
  if (sourceMode !== 'quote_minute' && sourceMode !== 'trade_minute') {
    throw new Error(`invalid_calc_greeks_source:${sourceMode}`);
  }
  const ivLow = parseFloatEnv('CALC_GREEKS_IV_LOW', 0.0005);
  const ivHigh = parseFloatEnv('CALC_GREEKS_IV_HIGH', 5.0);
  const ivIterations = Math.max(1, parseIntEnv('CALC_GREEKS_IV_ITERATIONS', 30));
  const chunkMinutes = Math.max(0, parseIntEnv('CALC_GREEKS_CHUNK_MINUTES', 0));
  const chunkSymbols = parseCsvEnv('CALC_GREEKS_CHUNK_SYMBOLS');
  const maxThreads = Math.max(1, parseIntEnv('CALC_GREEKS_QUERY_MAX_THREADS', 4));
  const maxMemoryBytes = Math.max(512 * 1024 * 1024, parseIntEnv('CALC_GREEKS_QUERY_MAX_MEMORY_BYTES', 3 * 1024 * 1024 * 1024));
  const heartbeatEvery = Math.max(1, parseIntEnv('CALC_GREEKS_HEARTBEAT_EVERY', 10));

  const runStartedAt = nowIso();
  const timing = {
    schemaMs: 0,
    loadJobsMs: 0,
    processingMs: 0,
  };

  const schemaStarted = Date.now();
  ensureSchema();
  ensureFunctions();
  timing.schemaMs = Date.now() - schemaStarted;

  const { startDay, endDay } = resolveRange();
  const loadJobsStarted = Date.now();
  let jobs = loadJobs({ startDay, endDay, symbolDayListPath: symbolDayListPath || null });
  if (skipCompleted) {
    const completedSet = loadCompletedSet({ startDay, endDay });
    if (chunkMinutes > 0) {
      const chunkCompletedDays = loadChunkCompletedDaySet({
        startDay,
        endDay,
        chunkMinutes,
        sourceMode,
        calcVersion,
      });
      chunkCompletedDays.forEach((key) => completedSet.add(key));
    }
    jobs = jobs.filter((job) => !completedSet.has(`${job.symbol}|${job.dayIso}`));
  }
  jobs = expandJobsForChunking(jobs, {
    sourceMode,
    chunkMinutes,
    chunkSymbols,
  });
  if (skipCompleted && chunkMinutes > 0) {
    const completedChunkSet = loadCompletedChunkSet({
      startDay,
      endDay,
      chunkMinutes,
      sourceMode,
      calcVersion,
    });
    jobs = jobs.filter((job) => {
      if (!Number.isFinite(job.chunkStartMs) || !Number.isFinite(job.chunkEndMs)) {
        return true;
      }
      return !completedChunkSet.has(buildChunkKey(job));
    });
  }
  const assignedJobs = jobs.filter((job, idx) => (idx % workerTotal) === workerIndex);
  timing.loadJobsMs = Date.now() - loadJobsStarted;

  console.log('[CALC_GREEKS_WORKER_START]', JSON.stringify({
    runId,
    calcVersion,
    workerIndex,
    workerTotal,
    startDay,
    endDay,
    symbolDayListPath: symbolDayListPath || null,
    skipCompleted,
    totalJobs: jobs.length,
    assignedJobs: assignedJobs.length,
    fallbackRate,
    dividendYield,
    sourceMode,
    ivLow,
    ivHigh,
    ivIterations,
    chunkMinutes,
    chunkSymbols,
    maxThreads,
    maxMemoryBytes,
  }));

  const jobsOut = [];
  let completed = 0;
  let failed = 0;
  let skipped = 0;
  let insertedRowsTotal = 0;
  let solvedRowsTotal = 0;
  const processingStarted = Date.now();

  assignedJobs.forEach((job, idx) => {
    const startedAtIso = nowIso();
    const jobStart = Date.now();
    const isChunkJob = Number.isFinite(job.chunkStartMs) && Number.isFinite(job.chunkEndMs);
    const chunkStartIso = isChunkJob ? toUtcIso(job.chunkStartMs) : null;
    const chunkEndIso = isChunkJob ? toUtcIso(job.chunkEndMs) : null;
    const statusBase = {
      symbol: job.symbol,
      trade_date_utc: job.dayIso,
      calc_run_id: runId,
      calc_version: calcVersion,
      source_rate: null,
      source_rate_date: null,
      started_at_utc: startedAtIso,
      completed_at_utc: null,
      elapsed_ms: 0,
      inserted_rows: 0,
      solved_rows: 0,
      missing_underlying_rows: 0,
      missing_price_rows: 0,
      invalid_input_rows: 0,
      expired_rows: 0,
      unsolved_rows: 0,
      avg_price_error: null,
      p95_price_error: null,
      error_message: null,
      updated_at_utc: startedAtIso,
    };
    const chunkStatusBase = isChunkJob ? {
      symbol: job.symbol,
      trade_date_utc: job.dayIso,
      chunk_start_utc: chunkStartIso,
      chunk_end_utc: chunkEndIso,
      chunk_index: job.chunkIndex,
      chunk_total: job.chunkTotal,
      chunk_minutes: job.chunkMinutes,
      source_mode: sourceMode,
      calc_run_id: runId,
      calc_version: calcVersion,
      source_rate: null,
      source_rate_date: null,
      started_at_utc: startedAtIso,
      completed_at_utc: null,
      elapsed_ms: 0,
      inserted_rows: 0,
      solved_rows: 0,
      missing_underlying_rows: 0,
      missing_price_rows: 0,
      invalid_input_rows: 0,
      expired_rows: 0,
      unsolved_rows: 0,
      avg_price_error: null,
      p95_price_error: null,
      error_message: null,
      updated_at_utc: startedAtIso,
    } : null;
    let rate = null;
    let rateDay = null;
    let rateSource = null;

    try {
      const rateResult = resolveRateForDay(job.dayIso, fallbackRate);
      rate = toSafeNumber(rateResult.rate, fallbackRate);
      rateDay = rateResult.rateDay || null;
      rateSource = rateResult.source;

      if (isChunkJob) {
        writeChunkStatusRow({
          ...chunkStatusBase,
          status: 'running',
          source_rate: rate,
          source_rate_date: rateDay,
        });
        reconcileDayStatusFromChunks({
          symbol: job.symbol,
          dayIso: job.dayIso,
          runId,
          calcVersion,
          sourceMode,
          chunkMinutes: job.chunkMinutes,
          sourceRate: rate,
          sourceRateDate: rateDay,
        });
      } else {
        writeStatusRow({
          ...statusBase,
          status: 'running',
        });
      }

      const insertStarted = Date.now();
      insertCalculatedGreeksForJob({
        symbol: job.symbol,
        dayIso: job.dayIso,
        chunkStartMs: isChunkJob ? job.chunkStartMs : null,
        chunkEndMs: isChunkJob ? job.chunkEndMs : null,
        runId,
        calcVersion,
        sourceMode,
        rate,
        dividendYield,
        ivLow,
        ivHigh,
        ivIterations,
        maxThreads,
        maxMemoryBytes,
      });
      const insertMs = Date.now() - insertStarted;

      const metrics = isChunkJob
        ? summarizeChunkRows({
          symbol: job.symbol,
          dayIso: job.dayIso,
          runId,
          chunkStartMs: job.chunkStartMs,
          chunkEndMs: job.chunkEndMs,
        })
        : summarizeJobRows({
          symbol: job.symbol,
          dayIso: job.dayIso,
          runId,
        });
      const elapsedMs = Date.now() - jobStart;

      insertedRowsTotal += metrics.insertedRows;
      solvedRowsTotal += metrics.solvedRows;
      completed += 1;

      let dayAggregate = null;
      if (isChunkJob) {
        writeChunkStatusRow({
          ...chunkStatusBase,
          status: 'complete',
          source_rate: rate,
          source_rate_date: rateDay,
          completed_at_utc: nowIso(),
          elapsed_ms: elapsedMs,
          inserted_rows: metrics.insertedRows,
          solved_rows: metrics.solvedRows,
          missing_underlying_rows: metrics.missingUnderlyingRows,
          missing_price_rows: metrics.missingPriceRows,
          invalid_input_rows: metrics.invalidInputRows,
          expired_rows: metrics.expiredRows,
          unsolved_rows: metrics.unsolvedRows,
          avg_price_error: metrics.avgPriceError,
          p95_price_error: metrics.p95PriceError,
          updated_at_utc: nowIso(),
        });
        dayAggregate = reconcileDayStatusFromChunks({
          symbol: job.symbol,
          dayIso: job.dayIso,
          runId,
          calcVersion,
          sourceMode,
          chunkMinutes: job.chunkMinutes,
          sourceRate: rate,
          sourceRateDate: rateDay,
        });
      } else {
        writeStatusRow({
          ...statusBase,
          status: 'complete',
          source_rate: rate,
          source_rate_date: rateDay,
          completed_at_utc: nowIso(),
          elapsed_ms: elapsedMs,
          inserted_rows: metrics.insertedRows,
          solved_rows: metrics.solvedRows,
          missing_underlying_rows: metrics.missingUnderlyingRows,
          missing_price_rows: metrics.missingPriceRows,
          invalid_input_rows: metrics.invalidInputRows,
          expired_rows: metrics.expiredRows,
          unsolved_rows: metrics.unsolvedRows,
          avg_price_error: metrics.avgPriceError,
          p95_price_error: metrics.p95PriceError,
          updated_at_utc: nowIso(),
        });
      }

      const jobReport = {
        symbol: job.symbol,
        dayIso: job.dayIso,
        status: 'complete',
        rate,
        rateDay,
        rateSource,
        sourceMode,
        elapsedMs,
        insertMs,
        rowsPerSecond: metrics.insertedRows > 0 ? (metrics.insertedRows / Math.max(1, insertMs / 1000)) : 0,
        chunkStartIso,
        chunkEndIso,
        chunkIndex: isChunkJob ? job.chunkIndex : null,
        chunkTotal: isChunkJob ? job.chunkTotal : null,
        chunkMinutes: isChunkJob ? job.chunkMinutes : null,
        dayStatus: dayAggregate?.status || 'complete',
        ...metrics,
      };
      jobsOut.push(jobReport);
      if ((completed + failed + skipped) % heartbeatEvery === 0 || (idx + 1) === assignedJobs.length) {
        console.log('[CALC_GREEKS_HEARTBEAT]', JSON.stringify({
          runId,
          workerIndex,
          workerTotal,
          processed: completed + failed + skipped,
          assigned: assignedJobs.length,
          completed,
          failed,
          skipped,
          insertedRowsTotal,
          solvedRowsTotal,
          lastJob: jobReport,
        }));
      }
    } catch (error) {
      failed += 1;
      const elapsedMs = Date.now() - jobStart;
      const message = error?.message || String(error);
      if (isChunkJob) {
        writeChunkStatusRow({
          ...chunkStatusBase,
          status: 'failed',
          source_rate: rate,
          source_rate_date: rateDay,
          completed_at_utc: nowIso(),
          elapsed_ms: elapsedMs,
          error_message: message,
          updated_at_utc: nowIso(),
        });
        reconcileDayStatusFromChunks({
          symbol: job.symbol,
          dayIso: job.dayIso,
          runId,
          calcVersion,
          sourceMode,
          chunkMinutes: job.chunkMinutes,
          sourceRate: rate,
          sourceRateDate: rateDay,
        });
      } else {
        writeStatusRow({
          ...statusBase,
          status: 'failed',
          completed_at_utc: nowIso(),
          elapsed_ms: elapsedMs,
          error_message: message,
          updated_at_utc: nowIso(),
        });
      }
      const jobReport = {
        symbol: job.symbol,
        dayIso: job.dayIso,
        status: 'failed',
        elapsedMs,
        chunkStartIso,
        chunkEndIso,
        chunkIndex: isChunkJob ? job.chunkIndex : null,
        chunkTotal: isChunkJob ? job.chunkTotal : null,
        chunkMinutes: isChunkJob ? job.chunkMinutes : null,
        error: message,
      };
      jobsOut.push(jobReport);
      console.error('[CALC_GREEKS_JOB_FAILED]', JSON.stringify({
        runId,
        workerIndex,
        workerTotal,
        ...jobReport,
      }));
    }
  });

  timing.processingMs = Date.now() - processingStarted;
  const runFinishedAt = nowIso();
  const aggregate = summarizeRun(runId);
  const report = {
    runId,
    calcVersion,
    workerIndex,
    workerTotal,
    runStartedAt,
    runFinishedAt,
    range: { startDay, endDay },
    config: {
      symbolDayListPath: symbolDayListPath || null,
      skipCompleted,
      fallbackRate,
      dividendYield,
      sourceMode,
      ivLow,
      ivHigh,
      ivIterations,
      chunkMinutes,
      chunkSymbols,
      maxThreads,
      maxMemoryBytes,
      heartbeatEvery,
    },
    timingsMs: timing,
    totals: {
      assignedJobs: assignedJobs.length,
      completedJobs: completed,
      failedJobs: failed,
      skippedJobs: skipped,
      insertedRowsTotal,
      solvedRowsTotal,
    },
    aggregate,
    jobs: jobsOut,
  };
  const outPath = writeReport(reportPath, report);
  console.log('[CALC_GREEKS_WORKER_DONE]', JSON.stringify({
    runId,
    workerIndex,
    workerTotal,
    assignedJobs: assignedJobs.length,
    completedJobs: completed,
    failedJobs: failed,
    skippedJobs: skipped,
    insertedRowsTotal,
    solvedRowsTotal,
    reportPath: outPath,
  }));

  if (failed > 0) {
    process.exitCode = 1;
  }
}

try {
  main();
} catch (error) {
  console.error('[CALC_GREEKS_FATAL]', error?.stack || error?.message || String(error));
  process.exitCode = 1;
}
