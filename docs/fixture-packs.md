# Deterministic fixture packs

MON-63 adds checked-in, deterministic market-data fixture packs for test scenarios.

## Available packs

Stored under `fixtures/packs`:

- `trades.json`
- `quotes.json`
- `open-interest.json`
- `spot.json`

Each pack includes:

- `packType`
- `version`
- `seed` (deterministic identifier)
- `generatedAt` (fixed timestamp)
- `rows` (test payload records)

## Loader helpers

Use `src/fixtures` helpers in tests:

```js
const { loadFixturePack, loadFixtureRows, listFixturePacks } = require('../src/fixtures');

const tradesPack = loadFixturePack('trades');
const quoteRows = loadFixtureRows('quotes');
const available = listFixturePacks();
```

Helper behavior:

- `loadFixturePack(name)` returns a deep-cloned object so tests can safely mutate data.
- `loadFixtureRows(name)` returns the `rows` array from a deep-cloned pack.
- `fixturePackPath(name)` resolves the on-disk file path.
- `listFixturePacks()` returns the canonical keys: `trades`, `quotes`, `openInterest`, `spot`.

## Example test usage

```js
const { loadFixtureRows } = require('../src/fixtures');

it('computes spread from quote fixtures', () => {
  const quotes = loadFixtureRows('quotes');
  const btc = quotes.find((row) => row.symbol === 'BTC-USD');

  expect(btc.ask - btc.bid).toBe(1);
});
```
