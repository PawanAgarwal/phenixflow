PRAGMA foreign_keys = ON;

BEGIN TRANSACTION;

UPDATE filter_rule_versions
SET is_active = 0,
    activated_at_utc = NULL
WHERE version_id <> 'v4_expanded_default' AND is_active = 1;

INSERT INTO filter_rule_versions (
  version_id,
  config_json,
  checksum,
  is_active,
  activated_at_utc
)
VALUES (
  'v1_baseline_default',
  '{"version":"v1_baseline_default","sigScoreModel":"v1_baseline","chips":{"100k":{"threshold":100000},"sizable":{"threshold":250000},"whales":{"threshold":500000},"largeSize":{"threshold":1000},"repeatFlow":{"threshold":20},"volOi":{"threshold":1.0},"unusualVolOi":{"threshold":2.0},"urgentVolOi":{"threshold":2.5},"highSig":{"threshold":0.9},"bullflowRatio":{"threshold":0.65}},"sigScoreWeights":{"valuePctile":0.35,"volOiNorm":0.25,"repeatNorm":0.20,"otmNorm":0.10,"sideConfidence":0.10}}',
  'v1_baseline_default_checksum',
  0,
  NULL
)
ON CONFLICT(version_id) DO UPDATE SET
  config_json = excluded.config_json,
  checksum = excluded.checksum,
  is_active = 0,
  activated_at_utc = NULL;

INSERT INTO filter_rule_versions (
  version_id,
  config_json,
  checksum,
  is_active,
  activated_at_utc
)
VALUES (
  'v4_expanded_default',
  '{"version":"v4_expanded_default","sigScoreModel":"v4_expanded","chips":{"100k":{"threshold":100000},"sizable":{"threshold":250000},"whales":{"threshold":500000},"largeSize":{"threshold":1000},"repeatFlow":{"threshold":20},"volOi":{"threshold":1.0},"unusualVolOi":{"threshold":2.0},"urgentVolOi":{"threshold":2.5},"highSig":{"threshold":0.9},"bullflowRatio":{"threshold":0.65}},"sigScoreWeights":{"valuePctile":0.18,"volOiNorm":0.15,"repeatNorm":0.08,"otmNorm":0.08,"sideConfidence":0.06,"dteNorm":0.04,"spreadNorm":0.04,"sweepNorm":0.12,"multilegNorm":-0.12,"timeNorm":0.07,"deltaNorm":0.08,"ivSkewNorm":0.06}}',
  'v4_expanded_default_checksum',
  1,
  strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
)
ON CONFLICT(version_id) DO UPDATE SET
  config_json = excluded.config_json,
  checksum = excluded.checksum,
  is_active = 1,
  activated_at_utc = COALESCE(filter_rule_versions.activated_at_utc, excluded.activated_at_utc);

INSERT INTO filter_rule_versions (
  version_id,
  config_json,
  checksum,
  is_active,
  activated_at_utc
)
VALUES (
  'v5_swing_default',
  '{"version":"v5_swing_default","sigScoreModel":"v5_swing","targetSpec":{"horizon":"swing_1_5d","label":"directional_plus_magnitude","calibrationWindowDays":120},"weightBlend":{"prior":0.7,"calibrated":0.3},"chips":{"100k":{"threshold":100000},"sizable":{"threshold":250000},"whales":{"threshold":500000},"largeSize":{"threshold":1000},"repeatFlow":{"threshold":20},"volOi":{"threshold":1.0},"unusualVolOi":{"threshold":2.0},"urgentVolOi":{"threshold":2.5},"highSig":{"threshold":0.9},"bullflowRatio":{"threshold":0.65}},"sigScoreWeights":{"valueShockNorm":0.10,"volOiNorm":0.10,"repeatNorm":0.06,"otmNorm":0.05,"dteSwingNorm":0.06,"flowImbalanceNorm":0.12,"deltaPressureNorm":0.12,"cpOiPressureNorm":0.08,"ivSkewSurfaceNorm":0.08,"ivTermSlopeNorm":0.06,"underlyingTrendConfirmNorm":0.10,"liquidityQualityNorm":0.07,"sweepNorm":0.06,"multilegPenaltyNorm":-0.08}}',
  'v5_swing_default_checksum',
  0,
  NULL
)
ON CONFLICT(version_id) DO UPDATE SET
  config_json = excluded.config_json,
  checksum = excluded.checksum,
  is_active = 0,
  activated_at_utc = NULL;

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
