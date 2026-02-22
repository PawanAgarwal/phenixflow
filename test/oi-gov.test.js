const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const Database = require('better-sqlite3');
const {
  parseGovOiPayload,
  syncOptionOiFromGov,
  queryOptionOi,
  listOptionOiSources,
  loadReferenceOiMap,
  ensureOiGovSchema,
} = require('../src/oi-gov');

describe('gov oi module', () => {
  it('parses CSV payload with common aliases', () => {
    const payload = [
      'symbol,expiration,strike,right,oi,as_of_date',
      'AAPL,2026-02-20,200,C,1234,2026-02-13',
      'AAPL,2026-02-20,200,P,2345,2026-02-13',
    ].join('\n');

    const parsed = parseGovOiPayload(payload, { source: 'finra' });
    expect(parsed.source).toBe('FINRA');
    expect(parsed.accepted).toHaveLength(2);
    expect(parsed.accepted[0]).toMatchObject({
      symbol: 'AAPL',
      expiration: '2026-02-20',
      strike: 200,
      optionRight: 'CALL',
      oi: 1234,
      asOfDate: '2026-02-13',
    });
    expect(parsed.rejected).toHaveLength(0);
  });

  it('syncs rows from gov source and exposes query/list/map views', async () => {
    const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'oi-gov-'));
    const dbPath = path.join(tempDir, 'oi.sqlite');

    const fakeFetch = async () => ({
      ok: true,
      status: 200,
      text: async () => [
        'symbol,expiration,strike,right,oi,date',
        'NVDA,2026-02-20,140,C,4000,2026-02-13',
        'NVDA,2026-02-20,140,P,3500,2026-02-13',
      ].join('\n'),
    });

    try {
      const sync = await syncOptionOiFromGov({
        source: 'FINRA',
        sourceUrl: 'https://example.gov/oi.csv',
        env: { PHENIX_DB_PATH: dbPath },
        fetchImpl: fakeFetch,
      });

      expect(sync).toMatchObject({
        source: 'FINRA',
        fetchedRows: 2,
        acceptedRows: 2,
        rejectedRows: 0,
      });

      const query = queryOptionOi({ symbol: 'NVDA', asOfDate: '2026-02-13' }, { PHENIX_DB_PATH: dbPath });
      expect(query.data).toHaveLength(2);

      const sources = listOptionOiSources({ PHENIX_DB_PATH: dbPath });
      expect(sources.data).toHaveLength(1);
      expect(sources.data[0]).toMatchObject({
        source: 'FINRA',
        asOfDate: '2026-02-13',
      });

      const db = new Database(dbPath);
      ensureOiGovSchema(db);
      const map = loadReferenceOiMap(db, { symbol: 'NVDA', asOfDate: '2026-02-13' });
      db.close();

      expect(map.get('NVDA|2026-02-20|140|CALL')).toBe(4000);
      expect(map.get('NVDA|2026-02-20|140|PUT')).toBe(3500);
    } finally {
      fs.rmSync(tempDir, { recursive: true, force: true });
    }
  });

  it('supports CME source URL resolution and sends CME fetch headers', async () => {
    const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'oi-gov-cme-'));
    const dbPath = path.join(tempDir, 'oi.sqlite');
    const calls = [];

    const fakeFetch = async (url, options) => {
      calls.push({ url, options });
      return {
        ok: true,
        status: 200,
        text: async () => [
          'symbol,expiration,strike,right,oi,date',
          'AAPL,2026-02-20,200,C,1111,2026-02-13',
        ].join('\n'),
      };
    };

    try {
      const result = await syncOptionOiFromGov({
        source: 'CMEGROUP',
        env: {
          PHENIX_DB_PATH: dbPath,
          GOV_OI_CME_URL: 'https://www.cmegroup.com/CmeWS/mvc/VoiTotals/V2/Download?tradeDate=20260213',
        },
        fetchImpl: fakeFetch,
      });

      expect(result).toMatchObject({
        source: 'CME',
        acceptedRows: 1,
      });

      expect(calls).toHaveLength(1);
      expect(calls[0].url).toContain('cmegroup.com');
      expect(calls[0].options.headers.Referer).toContain('cmegroup.com/market-data');
      expect(calls[0].options.headers['User-Agent']).toContain('PhenixFlowOI');
    } finally {
      fs.rmSync(tempDir, { recursive: true, force: true });
    }
  });

  it('fails sync when source returns blocked/anti-scraping response', async () => {
    await expect(syncOptionOiFromGov({
      source: 'CME',
      sourceUrl: 'https://www.cmegroup.com/CmeWS/mvc/VoiTotals/V2/Download?tradeDate=20260213',
      fetchImpl: async () => ({
        ok: false,
        status: 403,
        text: async () => '{"message":"This IP address is blocked due to suspected web scraping activity."}',
      }),
    })).rejects.toThrow('source_fetch_failed:403:blocked');
  });

  it('fails sync when payload has no parseable rows', async () => {
    await expect(syncOptionOiFromGov({
      source: 'FINRA',
      sourceUrl: 'https://example.gov/oi.csv',
      fetchImpl: async () => ({
        ok: true,
        status: 200,
        text: async () => '{}',
      }),
    })).rejects.toThrow('source_payload_empty');
  });

  it('fails sync when payload contains rows but none are usable options OI rows', async () => {
    await expect(syncOptionOiFromGov({
      source: 'CME',
      sourceUrl: 'https://example.com/cme.csv',
      fetchImpl: async () => ({
        ok: true,
        status: 200,
        text: async () => [
          'exchange,tradeDate,totalVolume',
          'CME,2026-02-13,12345',
        ].join('\n'),
      }),
    })).rejects.toThrow('source_payload_unusable');
  });
});
