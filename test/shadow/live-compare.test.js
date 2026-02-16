const { queryFlow } = require('../../src/flow');
const { buildShadowComparison } = require('../../src/shadow/live-compare');

describe('shadow live compare helper', () => {
  it('computes added and removed IDs between active and shadow sets', () => {
    const comparison = buildShadowComparison({
      activeVersion: 'legacy',
      shadowVersion: 'candidate',
      activeRows: [{ id: 'a' }, { id: 'b' }],
      shadowRows: [{ id: 'b' }, { id: 'c' }],
    });

    expect(comparison).toEqual({
      activeVersion: 'legacy',
      shadowVersion: 'candidate',
      activeCount: 2,
      shadowCount: 2,
      addedByShadow: ['c'],
      removedByShadow: ['a'],
      truncated: {
        addedByShadow: 1,
        removedByShadow: 1,
      },
    });
  });
});

describe('queryFlow shadow comparison', () => {
  it('emits legacy-vs-candidate shadow metadata when requested', () => {
    const response = queryFlow(
      {
        source: 'fixtures',
        symbol: 'aapl',
        shadow: 'true',
      },
      { filterVersion: 'legacy' },
    );

    expect(response.page.total).toBe(0);
    expect(response.meta.shadow).toMatchObject({
      activeVersion: 'legacy',
      shadowVersion: 'candidate',
      activeCount: 0,
      shadowCount: 2,
    });
    expect(response.meta.shadow.addedByShadow).toEqual(expect.arrayContaining(['flow_001', 'flow_005']));
  });
});
