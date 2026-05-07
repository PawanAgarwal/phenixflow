const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');

const {
  coverageForDataset,
  listParquetFiles,
  liveParquetCoverage,
  openCalendarDays,
} = require('../src/coverage');

describe('Massive filesystem coverage helpers', () => {
  it('checks only date directories and manifest files', () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), 'spy-intraday-coverage-'));
    const calendarPath = path.join(root, 'calendar.json');
    fs.writeFileSync(calendarPath, JSON.stringify({
      days: [
        { date: '2026-01-02', isOpen: true },
        { date: '2026-01-03', isOpen: false },
        { date: '2026-01-05', isOpen: true },
      ],
    }));
    fs.mkdirSync(path.join(root, 'stock_quotes_1m', 'date=2026-01-02'), { recursive: true });
    fs.writeFileSync(path.join(root, 'stock_quotes_1m', 'date=2026-01-02', '_SUCCESS.json'), '{}');

    const calendarDays = openCalendarDays(calendarPath, '2026-01-02', '2026-01-05');
    const report = coverageForDataset({ root, datasetId: 'stock_quotes_1m', calendarDays });
    expect(calendarDays).toEqual(['2026-01-02', '2026-01-05']);
    expect(report.ready).toBe(false);
    expect(report.missingOpenDays).toEqual(['2026-01-05']);
  });

  it('reports provisional parquet files separately', () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), 'spy-intraday-parquet-'));
    const dayRoot = path.join(root, 'indices_1m', 'date=2026-04-28');
    fs.mkdirSync(dayRoot, { recursive: true });
    fs.writeFileSync(path.join(dayRoot, '2026-04-28.live.parquet'), '');

    expect(listParquetFiles(root, 'indices_1m', '2026-04-28')).toHaveLength(1);
    const report = liveParquetCoverage({ root, datasetIds: ['indices_1m', 'option_trades_all'], dayIso: '2026-04-28' });
    expect(report.indices_1m.available).toBe(true);
    expect(report.option_trades_all.available).toBe(false);
  });
});
