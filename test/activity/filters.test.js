const fs = require('node:fs');
const path = require('node:path');
const {
  ACTIVITY_FILTERS,
  computeActivityFilters,
  isUrgent,
  isPositionBuilder,
} = require('../../src/activity/filters');

const fixturePath = path.resolve(__dirname, '../../fixtures/replays/mon44-activity-filters.json');
const replayFixtures = JSON.parse(fs.readFileSync(fixturePath, 'utf8'));

describe('MON-44 activity filters', () => {
  it('computes filter chips from repeat/sweep pacing metrics', () => {
    for (const replay of replayFixtures) {
      const chips = computeActivityFilters(replay);
      expect(chips).toEqual(replay.expectedChips);
    }
  });

  it('keeps Urgent and Position Builders separable by rule logic', () => {
    const urgentOnly = { repeatPace: 0.4, sweepPace: 0.88, sweepAcceleration: 0.3 };
    const positionOnly = { repeatPace: 0.4, sweepPace: 0.66, sweepAcceleration: 0.06 };

    expect(isUrgent(urgentOnly)).toBe(true);
    expect(isPositionBuilder(urgentOnly)).toBe(false);
    expect(computeActivityFilters(urgentOnly)).toContain(ACTIVITY_FILTERS.URGENT);

    expect(isPositionBuilder(positionOnly)).toBe(true);
    expect(isUrgent(positionOnly)).toBe(false);
    expect(computeActivityFilters(positionOnly)).toContain(ACTIVITY_FILTERS.POSITION_BUILDERS);
  });

  it('prioritizes Urgent over Position Builders when both thresholds could overlap', () => {
    const overlapMetrics = { repeatPace: 0.5, sweepPace: 0.84, sweepAcceleration: 0.21 };

    expect(computeActivityFilters(overlapMetrics)).toEqual([ACTIVITY_FILTERS.URGENT]);
  });
});
