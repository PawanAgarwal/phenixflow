const {
  loadFixturePack,
  listFixturePackVersions,
  buildScenarioSlice,
} = require('../../src/fixtures/market-fixture-pack');

describe('MON-63 market fixture packs', () => {
  it('loads a versioned fixture pack with representative and edge-case scenarios', () => {
    const pack = loadFixturePack({ packId: 'mon63-market-core', fixtureVersion: 'v1' });

    expect(pack.packId).toBe('mon63-market-core');
    expect(pack.fixtureVersion).toBe('v1');
    expect(pack.datasets.trades.length).toBeGreaterThan(0);
    expect(pack.datasets.quotes.length).toBeGreaterThan(0);
    expect(pack.datasets.oi.length).toBeGreaterThan(0);
    expect(pack.datasets.spot.length).toBeGreaterThan(0);

    const types = new Set(pack.scenarios.map((scenario) => scenario.type));
    expect(types.has('representative')).toBe(true);
    expect(types.has('edge-case')).toBe(true);
  });

  it('lists available versions for reuse across suites', () => {
    expect(listFixturePackVersions('mon63-market-core')).toEqual(['v1']);
  });

  it('returns deterministic scenario slices for focused tests', () => {
    const pack = loadFixturePack({ packId: 'mon63-market-core', fixtureVersion: 'v1' });
    const slice = buildScenarioSlice(pack, 'edge-zero-oi');

    expect(slice.scenario.id).toBe('edge-zero-oi');
    expect(slice.datasets.trades).toHaveLength(1);
    expect(slice.datasets.oi).toHaveLength(1);
    expect(slice.datasets.oi[0].value).toBe(0);
  });
});
