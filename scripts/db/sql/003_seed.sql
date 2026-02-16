PRAGMA foreign_keys = ON;

BEGIN TRANSACTION;

UPDATE filter_rule_versions
SET is_active = 0,
    activated_at_utc = NULL
WHERE version_id <> 'core_quant_v1_default' AND is_active = 1;

INSERT INTO filter_rule_versions (
  version_id,
  config_json,
  checksum,
  is_active,
  activated_at_utc
)
VALUES (
  'core_quant_v1_default',
  '{"version":"core_quant_v1_default","chips":{"calls":{"enabled":true},"puts":{"enabled":true},"bid":{"enabled":true},"ask":{"enabled":true},"aa":{"enabled":true},"100k":{"threshold":100000},"sizable":{"threshold":250000},"whales":{"threshold":500000},"largeSize":{"threshold":1000}},"sigScoreWeights":{"valuePctile":0.35,"volOiNorm":0.25,"repeatNorm":0.20,"otmNorm":0.10,"sideConfidence":0.10}}',
  'core_quant_v1_default_checksum',
  1,
  strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
)
ON CONFLICT(version_id) DO UPDATE SET
  config_json = excluded.config_json,
  checksum = excluded.checksum,
  is_active = 1,
  activated_at_utc = COALESCE(filter_rule_versions.activated_at_utc, excluded.activated_at_utc);

INSERT INTO ingest_checkpoints (stream_name, watermark)
VALUES ('theta_options_stream', NULL)
ON CONFLICT(stream_name) DO NOTHING;

INSERT INTO saved_queries (
  id,
  kind,
  name,
  payload_version,
  query_dsl_v2_json,
  created_at_utc,
  updated_at_utc
)
VALUES (
  'preset_core_quant_default',
  'preset',
  'Core Quant Default',
  'v2',
  '{"version":2,"combinator":"and","clauses":[]}',
  strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
  strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
)
ON CONFLICT(id) DO NOTHING;

COMMIT;
