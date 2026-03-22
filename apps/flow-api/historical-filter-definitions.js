const DEFAULT_THRESHOLDS = Object.freeze({
  premium100kMin: 100000,
  premiumSizableMin: 250000,
  premiumWhalesMin: 500000,
  sizeLargeMin: 1000,
  repeatFlowMin: 20,
  volOiMin: 1.0,
  unusualVolOiMin: 2.0,
  urgentVolOiMin: 2.5,
  highSigMin: 0.9,
  bullflowRatioMin: 0.65,
});

const CHIP_DEFINITIONS = Object.freeze([
  {
    id: 'calls',
    label: 'Calls',
    aliases: ['calls', 'call', 'c'],
    requiredMetrics: ['execution'],
  },
  {
    id: 'puts',
    label: 'Puts',
    aliases: ['puts', 'put', 'p'],
    requiredMetrics: ['execution'],
  },
  {
    id: 'bid',
    label: 'Bid',
    aliases: ['bid'],
    requiredMetrics: ['execution'],
  },
  {
    id: 'ask',
    label: 'Ask',
    aliases: ['ask'],
    requiredMetrics: ['execution'],
  },
  {
    id: 'aa',
    label: 'AA',
    aliases: ['aa'],
    requiredMetrics: ['execution'],
  },
  {
    id: '100k+',
    label: '100k+',
    aliases: ['100k+', '100k'],
    requiredMetrics: ['value'],
  },
  {
    id: 'sizable',
    label: 'Sizable',
    aliases: ['sizable'],
    requiredMetrics: ['value'],
  },
  {
    id: 'whales',
    label: 'Whales',
    aliases: ['whales'],
    requiredMetrics: ['value'],
  },
  {
    id: 'large-size',
    label: 'Large Size',
    aliases: ['large size', 'large-size', 'large_size', 'largesize'],
    requiredMetrics: ['size'],
  },
  {
    id: 'leaps',
    label: 'LEAPS',
    aliases: ['leaps'],
    requiredMetrics: ['dte'],
  },
  {
    id: 'weeklies',
    label: 'Weeklies',
    aliases: ['weeklies', 'weekly'],
    requiredMetrics: ['expiration'],
  },
  {
    id: 'repeat-flow',
    label: 'Repeat Flow',
    aliases: ['repeat flow', 'repeat-flow', 'repeat_flow', 'repeat'],
    requiredMetrics: ['repeat3m'],
  },
  {
    id: 'otm',
    label: 'OTM',
    aliases: ['otm'],
    requiredMetrics: ['otmPct'],
  },
  {
    id: 'vol>oi',
    label: 'Vol>OI',
    aliases: ['vol>oi', 'voloi', 'vol/oi', 'vol_oi'],
    requiredMetrics: ['volOiRatio'],
  },
  {
    id: 'rising-vol',
    label: 'Rising Vol',
    aliases: ['rising vol', 'rising-vol', 'rising_vol'],
    requiredMetrics: ['symbolVolStats'],
  },
  {
    id: 'am-spike',
    label: 'AM Spike',
    aliases: ['am spike', 'am-spike', 'am_spike'],
    requiredMetrics: ['symbolVolStats'],
  },
  {
    id: 'bullflow',
    label: 'Bullflow',
    aliases: ['bullflow'],
    requiredMetrics: ['bullishRatio15m', 'sentiment'],
  },
  {
    id: 'high-sig',
    label: 'High Sig',
    aliases: ['high sig', 'high-sig', 'high_sig'],
    requiredMetrics: ['sigScore'],
  },
  {
    id: 'unusual',
    label: 'Unusual',
    aliases: ['unusual'],
    requiredMetrics: ['value', 'volOiRatio'],
  },
  {
    id: 'urgent',
    label: 'Urgent',
    aliases: ['urgent'],
    requiredMetrics: ['repeat3m', 'value', 'dte', 'volOiRatio'],
  },
  {
    id: 'position-builders',
    label: 'Position Builders',
    aliases: ['position builders', 'position-builders', 'position_builders'],
    requiredMetrics: ['dte', 'otmPct', 'size', 'execution'],
  },
  {
    id: 'grenade',
    label: 'Grenade',
    aliases: ['grenade'],
    requiredMetrics: ['dte', 'otmPct', 'value'],
  },
]);

const DEFINITION_BY_ID = CHIP_DEFINITIONS.reduce((acc, definition) => {
  acc[definition.id] = definition;
  return acc;
}, {});

const DEFINITION_BY_ALIAS = CHIP_DEFINITIONS.reduce((acc, definition) => {
  definition.aliases.forEach((alias) => {
    acc[alias.toLowerCase()] = definition;
  });
  return acc;
}, {});

function toNumber(value, fallback) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function getThresholds(env = process.env) {
  return {
    premium100kMin: toNumber(env.FLOW_FILTER_PREMIUM_100K_MIN, DEFAULT_THRESHOLDS.premium100kMin),
    premiumSizableMin: toNumber(env.FLOW_FILTER_PREMIUM_SIZABLE_MIN, DEFAULT_THRESHOLDS.premiumSizableMin),
    premiumWhalesMin: toNumber(env.FLOW_FILTER_PREMIUM_WHALES_MIN, DEFAULT_THRESHOLDS.premiumWhalesMin),
    sizeLargeMin: toNumber(env.FLOW_FILTER_SIZE_LARGE_MIN, DEFAULT_THRESHOLDS.sizeLargeMin),
    repeatFlowMin: toNumber(env.FLOW_FILTER_REPEAT3M_MIN, DEFAULT_THRESHOLDS.repeatFlowMin),
    volOiMin: toNumber(env.FLOW_FILTER_VOL_OI_MIN, DEFAULT_THRESHOLDS.volOiMin),
    unusualVolOiMin: toNumber(env.FLOW_FILTER_VOL_OI_UNUSUAL_MIN, DEFAULT_THRESHOLDS.unusualVolOiMin),
    urgentVolOiMin: toNumber(env.FLOW_FILTER_VOL_OI_URGENT_MIN, DEFAULT_THRESHOLDS.urgentVolOiMin),
    highSigMin: toNumber(env.FLOW_FILTER_HIGH_SIG_MIN, DEFAULT_THRESHOLDS.highSigMin),
    bullflowRatioMin: toNumber(env.FLOW_FILTER_BULLFLOW_RATIO_MIN, DEFAULT_THRESHOLDS.bullflowRatioMin),
  };
}

function normalizeChipToken(rawToken) {
  if (typeof rawToken !== 'string') return null;
  const normalized = rawToken.trim().toLowerCase();
  return DEFINITION_BY_ALIAS[normalized] || null;
}

function parseChipList(rawValue) {
  if (typeof rawValue !== 'string') return [];
  const selected = [];
  const seen = new Set();

  rawValue.split(',').forEach((token) => {
    const definition = normalizeChipToken(token);
    if (!definition || seen.has(definition.id)) return;
    seen.add(definition.id);
    selected.push(definition.id);
  });

  return selected;
}

function getChipDefinition(chipId) {
  return DEFINITION_BY_ID[chipId] || null;
}

function getRequiredMetricsForChips(chips = []) {
  const required = new Set();

  chips.forEach((chipId) => {
    const definition = getChipDefinition(chipId);
    if (!definition) return;
    definition.requiredMetrics.forEach((metric) => required.add(metric));
  });

  return Array.from(required);
}

module.exports = {
  DEFAULT_THRESHOLDS,
  CHIP_DEFINITIONS,
  getThresholds,
  parseChipList,
  getChipDefinition,
  getRequiredMetricsForChips,
};
