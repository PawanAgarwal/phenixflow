-- One-time rebuild of chunk status tables from existing historical data.
-- Uses fixed 10-minute chunking to match default CLICKHOUSE_CHUNK_STATUS_MINUTES.

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
WITH toUInt16(10) AS chunk_minutes
SELECT
  symbol,
  trade_date_utc,
  stream_name,
  chunk_start_utc,
  chunk_start_utc + toIntervalMinute(chunk_minutes) AS chunk_end_utc,
  chunk_minutes,
  row_count,
  minute_count,
  'available' AS status,
  source_endpoint,
  CAST(NULL AS Nullable(String)) AS last_error,
  now64(3) AS updated_at_utc
FROM
(
  SELECT
    symbol,
    trade_date AS trade_date_utc,
    'option_trade_quote_1m' AS stream_name,
    toStartOfInterval(toStartOfMinute(trade_ts_utc), INTERVAL 10 MINUTE) AS chunk_start_utc,
    count() AS row_count,
    uniqExact(toStartOfMinute(trade_ts_utc)) AS minute_count,
    CAST(NULL AS Nullable(String)) AS source_endpoint
  FROM options.option_trades
  GROUP BY symbol, trade_date, chunk_start_utc

  UNION ALL

  SELECT
    symbol,
    trade_date_utc,
    'option_quote_1m' AS stream_name,
    toStartOfInterval(minute_bucket_utc, INTERVAL 10 MINUTE) AS chunk_start_utc,
    count() AS row_count,
    uniqExact(minute_bucket_utc) AS minute_count,
    argMax(source_endpoint, ingested_at_utc) AS source_endpoint
  FROM options.option_quote_minute_raw
  GROUP BY symbol, trade_date_utc, chunk_start_utc

  UNION ALL

  SELECT
    symbol,
    trade_date_utc,
    'stock_price_1m' AS stream_name,
    toStartOfInterval(minute_bucket_utc, INTERVAL 10 MINUTE) AS chunk_start_utc,
    count() AS row_count,
    uniqExact(minute_bucket_utc) AS minute_count,
    argMax(source_endpoint, ingested_at_utc) AS source_endpoint
  FROM options.stock_ohlc_minute_raw
  GROUP BY symbol, trade_date_utc, chunk_start_utc
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
  coalesce(i.symbol, o.symbol) AS symbol,
  coalesce(i.trade_date_utc, o.trade_date_utc) AS trade_date_utc,
  'option_trade_enriched_1m' AS stream_name,
  coalesce(i.chunk_start_utc, o.chunk_start_utc) AS chunk_start_utc,
  coalesce(i.chunk_start_utc, o.chunk_start_utc) + toIntervalMinute(chunk_minutes) AS chunk_end_utc,
  chunk_minutes,
  toUInt64(ifNull(i.input_row_count, 0)) AS input_row_count,
  toUInt64(ifNull(o.output_row_count, 0)) AS output_row_count,
  toUInt16(ifNull(i.input_minute_count, 0)) AS input_minute_count,
  toUInt16(ifNull(o.output_minute_count, 0)) AS output_minute_count,
  multiIf(
    ifNull(i.input_row_count, 0) = 0 AND ifNull(o.output_row_count, 0) > 0, 'extra',
    ifNull(i.input_row_count, 0) = ifNull(o.output_row_count, 0) AND ifNull(i.input_row_count, 0) > 0, 'complete',
    ifNull(i.input_row_count, 0) > 0 AND ifNull(o.output_row_count, 0) = 0, 'missing',
    ifNull(o.output_row_count, 0) < ifNull(i.input_row_count, 0), 'partial',
    ifNull(o.output_row_count, 0) > ifNull(i.input_row_count, 0), 'extra',
    'available'
  ) AS status,
  o.rule_version AS rule_version,
  CAST(NULL AS Nullable(String)) AS last_error,
  now64(3) AS updated_at_utc
FROM input_chunks AS i
FULL OUTER JOIN output_chunks AS o
  ON i.symbol = o.symbol
 AND i.trade_date_utc = o.trade_date_utc
 AND i.chunk_start_utc = o.chunk_start_utc;
