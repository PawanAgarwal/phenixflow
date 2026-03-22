const DEFAULT_BASELINE_WEIGHTS = Object.freeze({
  valuePctile: 0.35,
  volOiNorm: 0.25,
  repeatNorm: 0.20,
  otmNorm: 0.10,
  sideConfidence: 0.10,
});

const DEFAULT_EXPANDED_WEIGHTS = Object.freeze({
  valuePctile: 0.18,
  volOiNorm: 0.15,
  repeatNorm: 0.08,
  otmNorm: 0.08,
  sideConfidence: 0.06,
  dteNorm: 0.04,
  spreadNorm: 0.04,
  sweepNorm: 0.12,
  multilegNorm: -0.12,
  timeNorm: 0.07,
  deltaNorm: 0.08,
  ivSkewNorm: 0.06,
});

const DEFAULT_SWING_WEIGHTS = Object.freeze({
  valueShockNorm: 0.10,
  volOiNorm: 0.10,
  repeatNorm: 0.06,
  otmNorm: 0.05,
  dteSwingNorm: 0.06,
  flowImbalanceNorm: 0.12,
  deltaPressureNorm: 0.12,
  cpOiPressureNorm: 0.08,
  ivSkewSurfaceNorm: 0.08,
  ivTermSlopeNorm: 0.06,
  underlyingTrendConfirmNorm: 0.10,
  liquidityQualityNorm: 0.07,
  sweepNorm: 0.06,
  multilegPenaltyNorm: -0.08,
});

function parseRuleJson(rawJson) {
  if (typeof rawJson !== 'string' || !rawJson.trim()) return null;
  try {
    const parsed = JSON.parse(rawJson);
    return parsed && typeof parsed === 'object' ? parsed : null;
  } catch {
    return null;
  }
}

function toFiniteNumber(value, fallback) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function normalizeModel(rawModel) {
  if (typeof rawModel !== 'string') return 'v4_expanded';
  const normalized = rawModel.trim().toLowerCase();
  if (normalized === 'v1_baseline' || normalized === 'baseline' || normalized === 'v1') return 'v1_baseline';
  if (normalized === 'v4_expanded' || normalized === 'expanded' || normalized === 'v4') return 'v4_expanded';
  if (normalized === 'v5_swing' || normalized === 'swing' || normalized === 'v5') return 'v5_swing';
  return 'v4_expanded';
}

function pickScoringModel(ruleJson = {}, env = process.env) {
  if (env.FLOW_SIGSCORE_MODEL) return normalizeModel(env.FLOW_SIGSCORE_MODEL);
  return normalizeModel(ruleJson?.sigScoreModel || ruleJson?.scoring?.model || 'v4_expanded');
}

function mergeWeights(model, rawWeights = {}) {
  const defaults = model === 'v1_baseline'
    ? DEFAULT_BASELINE_WEIGHTS
    : (model === 'v5_swing' ? DEFAULT_SWING_WEIGHTS : DEFAULT_EXPANDED_WEIGHTS);
  const merged = { ...defaults };

  Object.keys(defaults).forEach((key) => {
    if (Object.prototype.hasOwnProperty.call(rawWeights, key)) {
      merged[key] = toFiniteNumber(rawWeights[key], defaults[key]);
    }
  });

  return merged;
}

function mapThresholdsFromRule(ruleJson = {}, currentThresholds = {}) {
  const chips = ruleJson?.chips && typeof ruleJson.chips === 'object' ? ruleJson.chips : {};
  return {
    ...currentThresholds,
    premium100kMin: toFiniteNumber(chips['100k']?.threshold, currentThresholds.premium100kMin),
    premiumSizableMin: toFiniteNumber(chips.sizable?.threshold, currentThresholds.premiumSizableMin),
    premiumWhalesMin: toFiniteNumber(chips.whales?.threshold, currentThresholds.premiumWhalesMin),
    sizeLargeMin: toFiniteNumber(chips.largeSize?.threshold, currentThresholds.sizeLargeMin),
    repeatFlowMin: toFiniteNumber(chips.repeatFlow?.threshold, currentThresholds.repeatFlowMin),
    volOiMin: toFiniteNumber(chips.volOi?.threshold, currentThresholds.volOiMin),
    unusualVolOiMin: toFiniteNumber(chips.unusualVolOi?.threshold, currentThresholds.unusualVolOiMin),
    urgentVolOiMin: toFiniteNumber(chips.urgentVolOi?.threshold, currentThresholds.urgentVolOiMin),
    highSigMin: toFiniteNumber(chips.highSig?.threshold, currentThresholds.highSigMin),
    bullflowRatioMin: toFiniteNumber(chips.bullflowRatio?.threshold, currentThresholds.bullflowRatioMin),
  };
}

function resolveActiveRuleConfig(db, thresholds, env = process.env) {
  let row = null;
  try {
    row = db.prepare(`
      SELECT version_id AS versionId, config_json AS configJson, checksum
      FROM filter_rule_versions
      WHERE is_active = 1
      ORDER BY COALESCE(activated_at_utc, created_at_utc) DESC
      LIMIT 1
    `).get();
  } catch {
    row = null;
  }

  const ruleJson = parseRuleJson(row?.configJson) || {};
  const scoringModel = pickScoringModel(ruleJson, env);
  const ruleModel = normalizeModel(ruleJson?.sigScoreModel || ruleJson?.scoring?.model || '');
  const hasEnvModelOverride = typeof env.FLOW_SIGSCORE_MODEL === 'string' && env.FLOW_SIGSCORE_MODEL.trim().length > 0;
  const canUseRuleWeights = !hasEnvModelOverride || ruleModel === scoringModel;
  const rawWeights = canUseRuleWeights
    ? (ruleJson?.sigScoreWeights && typeof ruleJson.sigScoreWeights === 'object'
      ? ruleJson.sigScoreWeights
      : (ruleJson?.scoring?.weights || {}))
    : {};

  const weights = mergeWeights(scoringModel, rawWeights);
  const resolvedThresholds = mapThresholdsFromRule(ruleJson, thresholds);

  return {
    versionId: row?.versionId || (
      scoringModel === 'v1_baseline'
        ? 'v1_baseline_default'
        : (scoringModel === 'v5_swing' ? 'v5_swing_default' : 'v4_expanded_default')
    ),
    checksum: row?.checksum || null,
    scoringModel,
    weights,
    thresholds: resolvedThresholds,
    targetSpec: ruleJson?.targetSpec && typeof ruleJson.targetSpec === 'object' ? ruleJson.targetSpec : null,
    calibrationWindowDays: toFiniteNumber(
      ruleJson?.calibrationWindowDays,
      toFiniteNumber(ruleJson?.targetSpec?.calibrationWindowDays, null),
    ),
    weightBlend: ruleJson?.weightBlend && typeof ruleJson.weightBlend === 'object'
      ? {
        prior: toFiniteNumber(ruleJson.weightBlend.prior, null),
        calibrated: toFiniteNumber(ruleJson.weightBlend.calibrated, null),
      }
      : null,
  };
}

module.exports = {
  DEFAULT_BASELINE_WEIGHTS,
  DEFAULT_EXPANDED_WEIGHTS,
  DEFAULT_SWING_WEIGHTS,
  resolveActiveRuleConfig,
};
