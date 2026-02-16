const THRESHOLD_FILTER_DEFINITIONS = Object.freeze([
  {
    key: '100k',
    aliases: ['100k+', '100k'],
    metric: 'premium',
    envVar: 'FLOW_FILTER_PREMIUM_100K_MIN',
    defaultThreshold: 100000,
    clauseField: 'canonical.premium',
    label: '100k+',
  },
  {
    key: 'sizable',
    aliases: ['sizable'],
    metric: 'premium',
    envVar: 'FLOW_FILTER_PREMIUM_SIZABLE_MIN',
    defaultThreshold: 25000,
    clauseField: 'canonical.premium',
    label: 'Sizable',
  },
  {
    key: 'whales',
    aliases: ['whales'],
    metric: 'premium',
    envVar: 'FLOW_FILTER_PREMIUM_WHALES_MIN',
    defaultThreshold: 500000,
    clauseField: 'canonical.premium',
    label: 'Whales',
  },
  {
    key: 'largeSize',
    aliases: ['large size', 'large-size', 'large_size', 'largesize'],
    metric: 'size',
    envVar: 'FLOW_FILTER_SIZE_LARGE_MIN',
    defaultThreshold: 1000,
    clauseField: 'canonical.size',
    label: 'Large Size',
  },
]);

const DEFINITION_BY_KEY = THRESHOLD_FILTER_DEFINITIONS.reduce((acc, definition) => {
  acc[definition.key] = definition;
  return acc;
}, {});

const DEFINITION_BY_ALIAS = THRESHOLD_FILTER_DEFINITIONS.reduce((acc, definition) => {
  definition.aliases.forEach((alias) => {
    acc[alias.toLowerCase()] = definition;
  });
  return acc;
}, {});

function parseThreshold(value, fallback) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function getThresholdFilterSettings(env = process.env) {
  return THRESHOLD_FILTER_DEFINITIONS.reduce((acc, definition) => {
    acc[definition.key] = parseThreshold(env[definition.envVar], definition.defaultThreshold);
    return acc;
  }, {});
}

function findThresholdDefinition(rawToken) {
  if (typeof rawToken !== 'string') return null;
  const normalized = rawToken.trim().toLowerCase();
  return DEFINITION_BY_ALIAS[normalized] || null;
}

function getThresholdDefinitionByKey(key) {
  return DEFINITION_BY_KEY[key] || null;
}

module.exports = {
  THRESHOLD_FILTER_DEFINITIONS,
  getThresholdFilterSettings,
  findThresholdDefinition,
  getThresholdDefinitionByKey,
};
