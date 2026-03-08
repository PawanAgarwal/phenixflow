-- One-time rebuild of chunk status tables from existing historical data.
-- Canonical expected grid is derived from option_trade_quote chunks per symbol-day.

ALTER TABLE options.option_download_chunk_status DELETE WHERE 1 SETTINGS mutations_sync = 1;
ALTER TABLE options.option_enrich_chunk_status DELETE WHERE 1 SETTINGS mutations_sync = 1;

INSERT INTO options.option_download_chunk_status
(
  symbol,
  trade_date_utc,
  stream_name,
  chunk_start_utc,
  chunk_end_utc,
  chunk_minutes,
  row_count,
  minute_count,
  status,
  source_endpoint,
  last_error,
  updated_at_utc
)
WITH
  toUInt16(10) AS chunk_minutes,
  trade_chunks AS
  (
    SELECT
      symbol,
      trade_date AS trade_date_utc,
      toStartOfInterval(toStartOfMinute(trade_ts_utc), INTERVAL 10 MINUTE) AS chunk_start_utc,
      count() AS expected_row_count,
      uniqExact(toStartOfMinute(trade_ts_utc)) AS expected_minute_count
    FROM options.option_trades
    GROUP BY symbol, trade_date, chunk_start_utc
  ),
  quote_chunks AS
  (
    SELECT
      symbol,
      trade_date_utc,
      toStartOfInterval(minute_bucket_utc, INTERVAL 10 MINUTE) AS chunk_start_utc,
      count() AS row_count,
      uniqExact(minute_bucket_utc) AS minute_count,
      argMax(source_endpoint, ingested_at_utc) AS source_endpoint
    FROM options.option_quote_minute_raw
    GROUP BY symbol, trade_date_utc, chunk_start_utc
  ),
  stock_chunks AS
  (
    SELECT
      symbol,
      trade_date_utc,
      toStartOfInterval(minute_bucket_utc, INTERVAL 10 MINUTE) AS chunk_start_utc,
      count() AS row_count,
      uniqExact(minute_bucket_utc) AS minute_count,
      argMax(source_endpoint, ingested_at_utc) AS source_endpoint
    FROM options.stock_ohlc_minute_raw
    GROUP BY symbol, trade_date_utc, chunk_start_utc
  )
SELECT
  symbol,
  trade_date_utc,
  stream_name,
  chunk_start_utc,
  chunk_start_utc + toIntervalMinute(chunk_minutes) AS chunk_end_utc,
  chunk_minutes,
  row_count,
  minute_count,
  status,
  source_endpoint,
  CAST(NULL AS Nullable(String)) AS last_error,
  now64(3) AS updated_at_utc
FROM
(
  SELECT
    t.symbol AS symbol,
    t.trade_date_utc AS trade_date_utc,
    'option_trade_quote_1m' AS stream_name,
    t.chunk_start_utc AS chunk_start_utc,
    toUInt64(t.expected_row_count) AS row_count,
    toUInt16(t.expected_minute_count) AS minute_count,
    'complete' AS status,
    CAST(NULL AS Nullable(String)) AS source_endpoint
  FROM trade_chunks AS t

  UNION ALL

  SELECT
    t.symbol AS symbol,
    t.trade_date_utc AS trade_date_utc,
    'option_quote_1m' AS stream_name,
    t.chunk_start_utc AS chunk_start_utc,
    toUInt64(ifNull(q.row_count, 0)) AS row_count,
    toUInt16(ifNull(q.minute_count, 0)) AS minute_count,
    multiIf(
      ifNull(q.row_count, 0) = 0, 'missing',
      ifNull(q.minute_count, 0) < t.expected_minute_count, 'partial',
      ifNull(q.minute_count, 0) > t.expected_minute_count, 'extra',
      'complete'
    ) AS status,
    q.source_endpoint AS source_endpoint
  FROM trade_chunks AS t
  LEFT JOIN quote_chunks AS q
    ON t.symbol = q.symbol
   AND t.trade_date_utc = q.trade_date_utc
   AND t.chunk_start_utc = q.chunk_start_utc

  UNION ALL

  SELECT
    t.symbol AS symbol,
    t.trade_date_utc AS trade_date_utc,
    'stock_price_1m' AS stream_name,
    t.chunk_start_utc AS chunk_start_utc,
    toUInt64(ifNull(s.row_count, 0)) AS row_count,
    toUInt16(ifNull(s.minute_count, 0)) AS minute_count,
    multiIf(
      ifNull(s.row_count, 0) = 0, 'missing',
      ifNull(s.minute_count, 0) < t.expected_minute_count, 'partial',
      ifNull(s.minute_count, 0) > t.expected_minute_count, 'extra',
      'complete'
    ) AS status,
    s.source_endpoint AS source_endpoint
  FROM trade_chunks AS t
  LEFT JOIN stock_chunks AS s
    ON t.symbol = s.symbol
   AND t.trade_date_utc = s.trade_date_utc
   AND t.chunk_start_utc = s.chunk_start_utc
);

INSERT INTO options.option_enrich_chunk_status
(
  symbol,
  trade_date_utc,
  stream_name,
  chunk_start_utc,
  chunk_end_utc,
  chunk_minutes,
  input_row_count,
  output_row_count,
  input_minute_count,
  output_minute_count,
  status,
  rule_version,
  last_error,
  updated_at_utc
)
WITH
  toUInt16(10) AS chunk_minutes,
  input_chunks AS
  (
    SELECT
      symbol,
      trade_date AS trade_date_utc,
      toStartOfInterval(toStartOfMinute(trade_ts_utc), INTERVAL 10 MINUTE) AS chunk_start_utc,
      count() AS input_row_count,
      uniqExact(toStartOfMinute(trade_ts_utc)) AS input_minute_count
    FROM options.option_trades
    GROUP BY symbol, trade_date, chunk_start_utc
  ),
  output_chunks AS
  (
    SELECT
      symbol,
      trade_date AS trade_date_utc,
      toStartOfInterval(toStartOfMinute(trade_ts_utc), INTERVAL 10 MINUTE) AS chunk_start_utc,
      count() AS output_row_count,
      uniqExact(toStartOfMinute(trade_ts_utc)) AS output_minute_count,
      argMax(rule_version, enriched_at_utc) AS rule_version
    FROM options.option_trade_enriched
    GROUP BY symbol, trade_date, chunk_start_utc
  )
SELECT
  i.symbol AS symbol,
  i.trade_date_utc AS trade_date_utc,
  'option_trade_enriched_1m' AS stream_name,
  i.chunk_start_utc AS chunk_start_utc,
  i.chunk_start_utc + toIntervalMinute(chunk_minutes) AS chunk_end_utc,
  chunk_minutes,
  toUInt64(i.input_row_count) AS input_row_count,
  toUInt64(ifNull(o.output_row_count, 0)) AS output_row_count,
  toUInt16(i.input_minute_count) AS input_minute_count,
  toUInt16(ifNull(o.output_minute_count, 0)) AS output_minute_count,
  multiIf(
    ifNull(o.output_row_count, 0) = 0, 'missing',
    ifNull(o.output_row_count, 0) < i.input_row_count, 'partial',
    ifNull(o.output_row_count, 0) > i.input_row_count, 'extra',
    'complete'
  ) AS status,
  o.rule_version AS rule_version,
  CAST(NULL AS Nullable(String)) AS last_error,
  now64(3) AS updated_at_utc
FROM input_chunks AS i
LEFT JOIN output_chunks AS o
  ON i.symbol = o.symbol
 AND i.trade_date_utc = o.trade_date_utc
 AND i.chunk_start_utc = o.chunk_start_utc

UNION ALL

SELECT
  o.symbol AS symbol,
  o.trade_date_utc AS trade_date_utc,
  'option_trade_enriched_1m' AS stream_name,
  o.chunk_start_utc AS chunk_start_utc,
  o.chunk_start_utc + toIntervalMinute(chunk_minutes) AS chunk_end_utc,
  chunk_minutes,
  toUInt64(0) AS input_row_count,
  toUInt64(o.output_row_count) AS output_row_count,
  toUInt16(0) AS input_minute_count,
  toUInt16(o.output_minute_count) AS output_minute_count,
  'extra' AS status,
  o.rule_version AS rule_version,
  CAST(NULL AS Nullable(String)) AS last_error,
  now64(3) AS updated_at_utc
FROM output_chunks AS o
LEFT ANTI JOIN input_chunks AS i
  ON i.symbol = o.symbol
 AND i.trade_date_utc = o.trade_date_utc
 AND i.chunk_start_utc = o.chunk_start_utc;
