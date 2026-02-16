# Deterministic Fixture Packs (MON-63)

This document defines the deterministic market fixture packs used by tests that require aligned **trades**, **quotes**, **OI**, and **spot** records.

## Pack location and versioning

- Registry: `fixtures/packs/market-fixtures.registry.json`
- Initial pack: `fixtures/packs/mon63-market-core.v1.json`
- Version lookup key: `packId@fixtureVersion` (example: `mon63-market-core@v1`)

The registry allows additional versions (`v2`, `v3`, ...) without breaking existing suites.

## Scenario coverage

`mon63-market-core@v1` includes both representative and edge-case scenarios:

- representative-liquid-flow
- edge-zero-oi
- edge-crossed-market
- edge-stale-spot
- edge-halted-no-quote

This satisfies deterministic coverage for standard market flow and failure-mode enrichment inputs.

## Data loading utilities

Utility module: `src/fixtures/market-fixture-pack.js`

### API

- `loadFixturePack({ packId, fixtureVersion })`
  - Loads and validates a versioned pack from the registry.
- `listFixturePackVersions(packId)`
  - Returns available versions for reuse by test suites.
- `buildScenarioSlice(pack, scenarioId)`
  - Materializes a scenario-specific subset by joining IDs into concrete dataset rows.

### Example

```js
const {
  loadFixturePack,
  buildScenarioSlice,
} = require('../src/fixtures/market-fixture-pack');

const pack = loadFixturePack({ packId: 'mon63-market-core', fixtureVersion: 'v1' });
const zeroOiCase = buildScenarioSlice(pack, 'edge-zero-oi');
```

## Determinism guarantees

- Fixture timestamps and values are static constants.
- Scenario slices preserve explicit ID order from scenario definitions.
- Loading returns deep clones so tests cannot mutate canonical fixture files.
