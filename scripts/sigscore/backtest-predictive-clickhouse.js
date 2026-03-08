#!/usr/bin/env node
const fs = require('node:fs');
const path = require('node:path');

const {
  resolveFlowReadBackend,
  buildArtifactPath,
  queryRowsSync,
} = require('../../src/storage/clickhouse');

const VALID_SIGNAL_COLUMNS = new Set(['avg_sig_score', 'max_sig_score']);

const BACKTEST_DAYS = Math.max(7, Math.trunc(Number(process.env.SIGSCORE_BACKTEST_DAYS || 30)));
const SIGNAL_COLUMN = String(process.env.SIGSCORE_SIGNAL_COLUMN || 'max_sig_score').trim();
const OUTPUT_PATH = path.resolve(
  process.env.SIGSCORE_BACKTEST_REPORT_PATH
    || path.join(process.cwd(), 'artifacts', 'reports', 'sigscore-predictive-backtest-last30d.json'),
);

const STOCK_LOOKBACK_DAYS = BACKTEST_DAYS + 35;

function nowIso() {
  return new Date().toISOString();
}

function toPct(value) {
  if (!Number.isFinite(Number(value))) return null;
  return Number(value) * 100;
}

function toBps(value) {
  if (!Number.isFinite(Number(value))) return null;
  return Number(value) * 10000;
}

function normalizeNumber(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function buildDatasetCte(signalColumn) {
  return `
WITH
signal_rows AS (
  SELECT
    symbol,
    minute_bucket_utc AS minute_utc,
    ${signalColumn} AS score
  FROM options.option_symbol_minute_derived
  WHERE trade_date_utc >= toDate(now('UTC') - INTERVAL {signalDays:UInt32} DAY)
    AND ${signalColumn} IS NOT NULL
),
stock_rows AS (
  SELECT
    symbol,
    minute_bucket_utc AS minute_utc,
    close,
    row_number() OVER (PARTITION BY symbol ORDER BY minute_bucket_utc ASC) AS rn
  FROM options.stock_ohlc_minute_raw
  WHERE trade_date_utc >= toDate(now('UTC') - INTERVAL {stockDays:UInt32} DAY)
    AND close > 0
),
base AS (
  SELECT
    s.symbol,
    s.minute_utc,
    s.score,
    st.close AS px0,
    st.rn AS rn
  FROM signal_rows AS s
  INNER JOIN stock_rows AS st
    ON st.symbol = s.symbol
   AND st.minute_utc = s.minute_utc
),
dataset AS (
  SELECT
    b.symbol AS symbol,
    b.minute_utc AS minute_utc,
    b.score AS score,
    b.px0 AS px0,
    if(h1.close > 0, (h1.close / b.px0) - 1, NULL) AS ret_1h,
    if(hd.close > 0, (hd.close / b.px0) - 1, NULL) AS ret_1d,
    if(hw.close > 0, (hw.close / b.px0) - 1, NULL) AS ret_1w,
    if(h2w.close > 0, (h2w.close / b.px0) - 1, NULL) AS ret_2w,
    if(h3w.close > 0, (h3w.close / b.px0) - 1, NULL) AS ret_3w
  FROM base AS b
  LEFT JOIN stock_rows AS h1
    ON h1.symbol = b.symbol
   AND h1.rn = b.rn + 60
  LEFT JOIN stock_rows AS hd
    ON hd.symbol = b.symbol
   AND hd.rn = b.rn + 390
  LEFT JOIN stock_rows AS hw
    ON hw.symbol = b.symbol
   AND hw.rn = b.rn + 1950
  LEFT JOIN stock_rows AS h2w
    ON h2w.symbol = b.symbol
   AND h2w.rn = b.rn + 3900
  LEFT JOIN stock_rows AS h3w
    ON h3w.symbol = b.symbol
   AND h3w.rn = b.rn + 5850
)
`;
}

function buildSummaryQuery(signalColumn) {
  const cte = buildDatasetCte(signalColumn);
  return `
${cte}
,
thresholds_1h AS (
  SELECT
    quantileTDigestIf(0.2)(score, isNotNull(ret_1h)) AS q20,
    quantileTDigestIf(0.8)(score, isNotNull(ret_1h)) AS q80
  FROM dataset
),
thresholds_1d AS (
  SELECT
    quantileTDigestIf(0.2)(score, isNotNull(ret_1d)) AS q20,
    quantileTDigestIf(0.8)(score, isNotNull(ret_1d)) AS q80
  FROM dataset
),
thresholds_1w AS (
  SELECT
    quantileTDigestIf(0.2)(score, isNotNull(ret_1w)) AS q20,
    quantileTDigestIf(0.8)(score, isNotNull(ret_1w)) AS q80
  FROM dataset
),
thresholds_2w AS (
  SELECT
    quantileTDigestIf(0.2)(score, isNotNull(ret_2w)) AS q20,
    quantileTDigestIf(0.8)(score, isNotNull(ret_2w)) AS q80
  FROM dataset
),
thresholds_3w AS (
  SELECT
    quantileTDigestIf(0.2)(score, isNotNull(ret_3w)) AS q20,
    quantileTDigestIf(0.8)(score, isNotNull(ret_3w)) AS q80
  FROM dataset
)
SELECT
  '1h' AS horizon,
  countIf(isNotNull(ret_1h)) AS samples,
  corr(score, ret_1h) AS pearson_corr,
  any(t.q20) AS score_q20,
  any(t.q80) AS score_q80,
  avgIf(ret_1h, isNotNull(ret_1h)) AS avg_return_all,
  avgIf(ret_1h, score >= t.q80 AND isNotNull(ret_1h)) AS avg_return_high_sig,
  avgIf(ret_1h, score <= t.q20 AND isNotNull(ret_1h)) AS avg_return_low_sig,
  countIf(score >= t.q80 AND isNotNull(ret_1h)) AS high_sig_samples,
  countIf(score <= t.q20 AND isNotNull(ret_1h)) AS low_sig_samples,
  avgIf(ret_1h > 0, score >= t.q80 AND isNotNull(ret_1h)) AS high_sig_up_hit_rate,
  avgIf(ret_1h < 0, score <= t.q20 AND isNotNull(ret_1h)) AS low_sig_down_hit_rate
FROM dataset
CROSS JOIN thresholds_1h AS t
UNION ALL
SELECT
  '1d' AS horizon,
  countIf(isNotNull(ret_1d)) AS samples,
  corr(score, ret_1d) AS pearson_corr,
  any(t.q20) AS score_q20,
  any(t.q80) AS score_q80,
  avgIf(ret_1d, isNotNull(ret_1d)) AS avg_return_all,
  avgIf(ret_1d, score >= t.q80 AND isNotNull(ret_1d)) AS avg_return_high_sig,
  avgIf(ret_1d, score <= t.q20 AND isNotNull(ret_1d)) AS avg_return_low_sig,
  countIf(score >= t.q80 AND isNotNull(ret_1d)) AS high_sig_samples,
  countIf(score <= t.q20 AND isNotNull(ret_1d)) AS low_sig_samples,
  avgIf(ret_1d > 0, score >= t.q80 AND isNotNull(ret_1d)) AS high_sig_up_hit_rate,
  avgIf(ret_1d < 0, score <= t.q20 AND isNotNull(ret_1d)) AS low_sig_down_hit_rate
FROM dataset
CROSS JOIN thresholds_1d AS t
UNION ALL
SELECT
  '1w' AS horizon,
  countIf(isNotNull(ret_1w)) AS samples,
  corr(score, ret_1w) AS pearson_corr,
  any(t.q20) AS score_q20,
  any(t.q80) AS score_q80,
  avgIf(ret_1w, isNotNull(ret_1w)) AS avg_return_all,
  avgIf(ret_1w, score >= t.q80 AND isNotNull(ret_1w)) AS avg_return_high_sig,
  avgIf(ret_1w, score <= t.q20 AND isNotNull(ret_1w)) AS avg_return_low_sig,
  countIf(score >= t.q80 AND isNotNull(ret_1w)) AS high_sig_samples,
  countIf(score <= t.q20 AND isNotNull(ret_1w)) AS low_sig_samples,
  avgIf(ret_1w > 0, score >= t.q80 AND isNotNull(ret_1w)) AS high_sig_up_hit_rate,
  avgIf(ret_1w < 0, score <= t.q20 AND isNotNull(ret_1w)) AS low_sig_down_hit_rate
FROM dataset
CROSS JOIN thresholds_1w AS t
UNION ALL
SELECT
  '2w' AS horizon,
  countIf(isNotNull(ret_2w)) AS samples,
  corr(score, ret_2w) AS pearson_corr,
  any(t.q20) AS score_q20,
  any(t.q80) AS score_q80,
  avgIf(ret_2w, isNotNull(ret_2w)) AS avg_return_all,
  avgIf(ret_2w, score >= t.q80 AND isNotNull(ret_2w)) AS avg_return_high_sig,
  avgIf(ret_2w, score <= t.q20 AND isNotNull(ret_2w)) AS avg_return_low_sig,
  countIf(score >= t.q80 AND isNotNull(ret_2w)) AS high_sig_samples,
  countIf(score <= t.q20 AND isNotNull(ret_2w)) AS low_sig_samples,
  avgIf(ret_2w > 0, score >= t.q80 AND isNotNull(ret_2w)) AS high_sig_up_hit_rate,
  avgIf(ret_2w < 0, score <= t.q20 AND isNotNull(ret_2w)) AS low_sig_down_hit_rate
FROM dataset
CROSS JOIN thresholds_2w AS t
UNION ALL
SELECT
  '3w' AS horizon,
  countIf(isNotNull(ret_3w)) AS samples,
  corr(score, ret_3w) AS pearson_corr,
  any(t.q20) AS score_q20,
  any(t.q80) AS score_q80,
  avgIf(ret_3w, isNotNull(ret_3w)) AS avg_return_all,
  avgIf(ret_3w, score >= t.q80 AND isNotNull(ret_3w)) AS avg_return_high_sig,
  avgIf(ret_3w, score <= t.q20 AND isNotNull(ret_3w)) AS avg_return_low_sig,
  countIf(score >= t.q80 AND isNotNull(ret_3w)) AS high_sig_samples,
  countIf(score <= t.q20 AND isNotNull(ret_3w)) AS low_sig_samples,
  avgIf(ret_3w > 0, score >= t.q80 AND isNotNull(ret_3w)) AS high_sig_up_hit_rate,
  avgIf(ret_3w < 0, score <= t.q20 AND isNotNull(ret_3w)) AS low_sig_down_hit_rate
FROM dataset
CROSS JOIN thresholds_3w AS t
ORDER BY horizon ASC
`;
}

function buildBucketQuery(signalColumn) {
  const cte = buildDatasetCte(signalColumn);
  return `
${cte}
SELECT
  horizon,
  bucket_idx,
  count() AS samples,
  avg(ret) AS avg_return,
  avg(ret > 0) AS up_hit_rate
FROM (
  SELECT
    least(4, greatest(0, toInt32(floor(score * 5)))) AS bucket_idx,
    ['1h', '1d', '1w', '2w', '3w'] AS horizons,
    [ret_1h, ret_1d, ret_1w, ret_2w, ret_3w] AS returns
  FROM dataset
)
ARRAY JOIN horizons AS horizon, returns AS ret
WHERE isNotNull(ret)
GROUP BY horizon, bucket_idx
ORDER BY horizon ASC, bucket_idx ASC
`;
}

function run() {
  if (!VALID_SIGNAL_COLUMNS.has(SIGNAL_COLUMN)) {
    throw new Error(`invalid_signal_column:${SIGNAL_COLUMN}`);
  }

  const backend = resolveFlowReadBackend(process.env);
  if (backend !== 'clickhouse') {
    throw new Error(`clickhouse_backend_required:${backend}`);
  }

  const params = {
    signalDays: BACKTEST_DAYS,
    stockDays: STOCK_LOOKBACK_DAYS,
  };

  const startedAt = nowIso();
  const summaryRows = queryRowsSync(buildSummaryQuery(SIGNAL_COLUMN), params, process.env);
  const bucketRows = queryRowsSync(buildBucketQuery(SIGNAL_COLUMN), params, process.env);

  const summary = summaryRows.map((row) => {
    const high = normalizeNumber(row.avg_return_high_sig);
    const low = normalizeNumber(row.avg_return_low_sig);
    const spread = (high !== null && low !== null) ? (high - low) : null;
    return {
      horizon: String(row.horizon),
      samples: Math.trunc(Number(row.samples || 0)),
      pearsonCorr: normalizeNumber(row.pearson_corr),
      scoreQ20: normalizeNumber(row.score_q20),
      scoreQ80: normalizeNumber(row.score_q80),
      avgReturnAllPct: toPct(row.avg_return_all),
      avgReturnHighSigPct: toPct(high),
      avgReturnLowSigPct: toPct(low),
      highMinusLowSpreadBps: toBps(spread),
      highSigSamples: Math.trunc(Number(row.high_sig_samples || 0)),
      lowSigSamples: Math.trunc(Number(row.low_sig_samples || 0)),
      highSigUpHitRate: normalizeNumber(row.high_sig_up_hit_rate),
      lowSigDownHitRate: normalizeNumber(row.low_sig_down_hit_rate),
    };
  });

  const buckets = bucketRows.map((row) => ({
    horizon: String(row.horizon),
    bucketIndex: Math.trunc(Number(row.bucket_idx || 0)),
    bucketRange: [
      Math.trunc(Number(row.bucket_idx || 0)) * 0.2,
      (Math.trunc(Number(row.bucket_idx || 0)) + 1) * 0.2,
    ],
    samples: Math.trunc(Number(row.samples || 0)),
    avgReturnPct: toPct(row.avg_return),
    upHitRate: normalizeNumber(row.up_hit_rate),
  }));

  const report = {
    generatedAt: nowIso(),
    startedAt,
    readBackend: backend,
    artifactPath: buildArtifactPath(process.env),
    config: {
      backtestDays: BACKTEST_DAYS,
      stockLookbackDays: STOCK_LOOKBACK_DAYS,
      signalColumn: SIGNAL_COLUMN,
      horizons: {
        '1h': 60,
        '1d': 390,
        '1w': 1950,
        '2w': 3900,
        '3w': 5850,
      },
    },
    summary,
    buckets,
  };

  fs.mkdirSync(path.dirname(OUTPUT_PATH), { recursive: true });
  fs.writeFileSync(OUTPUT_PATH, `${JSON.stringify(report, null, 2)}\n`, 'utf8');

  const latestPath = path.join(path.dirname(OUTPUT_PATH), 'sigscore-predictive-backtest-latest.json');
  fs.writeFileSync(latestPath, `${JSON.stringify(report, null, 2)}\n`, 'utf8');

  console.log(JSON.stringify({
    outputPath: OUTPUT_PATH,
    latestPath,
    rows: summary.length,
    summary,
  }, null, 2));
}

try {
  run();
} catch (error) {
  console.error(error.message || String(error));
  process.exitCode = 1;
}
