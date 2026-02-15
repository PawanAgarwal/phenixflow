const ACTIVITY_FILTERS = {
  REPEAT_FLOW: 'Repeat Flow',
  URGENT: 'Urgent',
  POSITION_BUILDERS: 'Position Builders',
};

function toFiniteNumber(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
}

function normalizeMetrics(metrics = {}) {
  return {
    repeatPace: toFiniteNumber(metrics.repeatPace),
    sweepPace: toFiniteNumber(metrics.sweepPace),
    sweepAcceleration: toFiniteNumber(metrics.sweepAcceleration),
  };
}

function isRepeatFlow(metrics) {
  const normalized = normalizeMetrics(metrics);
  return normalized.repeatPace >= 0.7 && normalized.repeatPace - normalized.sweepPace >= 0.15;
}

function isUrgent(metrics) {
  const normalized = normalizeMetrics(metrics);
  return normalized.sweepPace >= 0.8 && normalized.sweepAcceleration >= 0.2;
}

function isPositionBuilder(metrics) {
  const normalized = normalizeMetrics(metrics);
  return (
    normalized.sweepPace >= 0.55
    && normalized.sweepPace < 0.8
    && normalized.sweepAcceleration > -0.2
    && normalized.sweepAcceleration < 0.2
  );
}

function computeActivityFilters(metrics) {
  const chips = [];

  if (isRepeatFlow(metrics)) {
    chips.push(ACTIVITY_FILTERS.REPEAT_FLOW);
  }

  if (isUrgent(metrics)) {
    chips.push(ACTIVITY_FILTERS.URGENT);
  } else if (isPositionBuilder(metrics)) {
    chips.push(ACTIVITY_FILTERS.POSITION_BUILDERS);
  }

  return chips;
}

module.exports = {
  ACTIVITY_FILTERS,
  computeActivityFilters,
  isRepeatFlow,
  isUrgent,
  isPositionBuilder,
};
