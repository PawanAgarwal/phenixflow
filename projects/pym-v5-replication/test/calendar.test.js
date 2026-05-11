const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');

const { latestDatasetDateAcrossRoots, resolveEndDate } = require('../src/calendar');

describe('calendar data-root resolution', () => {
  it('resolves auto end date from live parquet when historical cache is behind', () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), 'pym-calendar-'));
    const historical = path.join(root, 'historical');
    const liveParquet = path.join(root, 'live-parquet');
    fs.mkdirSync(path.join(historical, 'stock_quotes_1m', 'date=2026-05-08'), { recursive: true });
    fs.mkdirSync(path.join(liveParquet, 'stock_quotes_1m', 'date=2026-05-11'), { recursive: true });

    const config = {
      roots: { historical, liveParquet },
      datasets: { stockBars: 'stock_quotes_1m' },
      windows: { endDate: 'auto' },
    };

    expect(latestDatasetDateAcrossRoots([historical, liveParquet], 'stock_quotes_1m')).toBe('2026-05-11');
    expect(resolveEndDate(config, 'auto')).toBe('2026-05-11');
  });
});
