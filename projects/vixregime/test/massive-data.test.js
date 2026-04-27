const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const zlib = require('node:zlib');

const { loadMassiveMinuteRows, __private } = require('../src/massive-data');

function toNs(isoString) {
  return String(BigInt(Date.parse(isoString)) * 1000000n);
}

function writeGzip(targetPath, body) {
  fs.mkdirSync(path.dirname(targetPath), { recursive: true });
  fs.writeFileSync(targetPath, zlib.gzipSync(body));
}

describe('massive direct loader', () => {
  it('resolves NY cash-session bounds across DST', () => {
    const jan = __private.resolveRegularSessionBoundsUtcMs('2025-01-08');
    const apr = __private.resolveRegularSessionBoundsUtcMs('2025-04-02');

    expect(new Date(jan.startMs).toISOString()).toBe('2025-01-08T14:30:00.000Z');
    expect(new Date(jan.endMs).toISOString()).toBe('2025-01-08T21:00:00.000Z');
    expect(new Date(apr.startMs).toISOString()).toBe('2025-04-02T13:30:00.000Z');
    expect(new Date(apr.endMs).toISOString()).toBe('2025-04-02T20:00:00.000Z');
  });

  it('loads Massive stock and index minute rows directly from CSV files', async () => {
    const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'vixregime-massive-'));

    try {
      writeGzip(
        path.join(tempRoot, 'massive', 'stock_quotes_1m', 'date=2025-01-08', '2025-01-08.csv.gz'),
        [
          'ticker,volume,open,close,high,low,window_start,transactions',
          `SPY,1000,500,501,502,499,${toNs('2025-01-08T08:00:00.000Z')},10`,
          `SPY,2000,510,511,512,509,${toNs('2025-01-08T14:30:00.000Z')},20`,
          `SPXL,3000,100,101,101,99,${toNs('2025-01-08T14:31:00.000Z')},30`,
        ].join('\n'),
      );

      fs.mkdirSync(path.join(tempRoot, 'massive', 'indices_1m', 'date=2025-01-08'), { recursive: true });
      fs.writeFileSync(
        path.join(tempRoot, 'massive', 'indices_1m', 'date=2025-01-08', '2025-01-08.csv'),
        [
          `I:VIX,17.9,18,18.1,17.8,${toNs('2025-01-08T08:15:00.000Z')}`,
          `I:VIX,18,18.1,18.2,17.9,${toNs('2025-01-08T14:30:00.000Z')}`,
          `I:SPX,5900,5901,5902,5899,${toNs('2025-01-08T14:32:00.000Z')}`,
        ].join('\n'),
      );

      const rows = await loadMassiveMinuteRows({
        startDate: '2025-01-08',
        endDate: '2025-01-08',
        requiredSymbols: ['SPY', 'SPXL', 'SPX', 'VIX'],
        env: { MASSIVE_DATA_ROOT: tempRoot },
      });

      expect(rows).toEqual([
        {
          symbol: 'SPX',
          tradeDateUtc: '2025-01-08',
          minuteUtc: '2025-01-08T14:32:00.000Z',
          open: 5900,
          high: 5902,
          low: 5899,
          close: 5901,
          volume: null,
        },
        {
          symbol: 'SPXL',
          tradeDateUtc: '2025-01-08',
          minuteUtc: '2025-01-08T14:31:00.000Z',
          open: 100,
          high: 101,
          low: 99,
          close: 101,
          volume: 3000,
        },
        {
          symbol: 'SPY',
          tradeDateUtc: '2025-01-08',
          minuteUtc: '2025-01-08T14:30:00.000Z',
          open: 510,
          high: 512,
          low: 509,
          close: 511,
          volume: 2000,
        },
        {
          symbol: 'VIX',
          tradeDateUtc: '2025-01-08',
          minuteUtc: '2025-01-08T14:30:00.000Z',
          open: 18,
          high: 18.2,
          low: 17.9,
          close: 18.1,
          volume: null,
        },
      ]);
    } finally {
      fs.rmSync(tempRoot, { recursive: true, force: true });
    }
  });
});
