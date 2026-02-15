const fs = require('node:fs');
const path = require('node:path');
const { runShadowRollout, validateSessions, diffArrays } = require('../../src/shadow/rollout');

const fixturePath = path.resolve(__dirname, '../../fixtures/packs/mon77-sessions.json');
const sessions = JSON.parse(fs.readFileSync(fixturePath, 'utf8'));

describe('MON-77 shadow rollout', () => {
  it('requires at least 3 sessions', () => {
    expect(() => validateSessions(sessions.slice(0, 2))).toThrow(/at least 3/);
    expect(() => validateSessions(sessions)).not.toThrow();
  });

  it('computes deterministic diff arrays', () => {
    const diff = diffArrays(['A', 'B', 'C'], ['B', 'D']);
    expect(diff).toEqual({ removedByNew: ['A', 'C'], addedByNew: ['D'] });
  });

  it('produces a report with per-session and summary data', () => {
    const report = runShadowRollout(sessions);

    expect(report.mode).toBe('shadow');
    expect(report.summary.totalSessions).toBeGreaterThanOrEqual(3);
    expect(report.sessions).toHaveLength(3);

    for (const session of report.sessions) {
      expect(Array.isArray(session.oldOutput)).toBe(true);
      expect(Array.isArray(session.newOutput)).toBe(true);
      expect(session.diff).toHaveProperty('removedByNew');
      expect(session.diff).toHaveProperty('addedByNew');
    }
  });
});
