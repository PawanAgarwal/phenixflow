const {
  parseRetryDelays,
  resolveUrl,
  inferRowCount,
  fetchWithRetry,
} = require('../../src/thetadata/smoke');

describe('thetadata smoke helpers', () => {
  it('parses retry delays from csv string', () => {
    expect(parseRetryDelays('100, 200,foo, 300')).toEqual([100, 200, 300]);
    expect(parseRetryDelays('')).toEqual([2000, 5000, 15000]);
  });

  it('resolves relative url with base and absolute url directly', () => {
    expect(resolveUrl('https://api.example.com', '/entitlements')).toBe('https://api.example.com/entitlements');
    expect(resolveUrl('https://api.example.com', 'https://other.example.com/p')).toBe('https://other.example.com/p');
  });

  it('infers row count from common payload shapes', () => {
    expect(inferRowCount([{ id: 1 }, { id: 2 }])).toBe(2);
    expect(inferRowCount({ data: [{ id: 1 }] })).toBe(1);
    expect(inferRowCount({ rows: [{ id: 1 }, { id: 2 }, { id: 3 }] })).toBe(3);
    expect(inferRowCount({})).toBe(null);
  });

  it('retries on retryable status and succeeds', async () => {
    let calls = 0;
    const fakeFetch = async () => {
      calls += 1;
      if (calls === 1) {
        return {
          ok: false,
          status: 503,
          headers: new Map(),
          arrayBuffer: async () => Buffer.from('temporary').buffer,
        };
      }

      return {
        ok: true,
        status: 200,
        headers: new Map([['content-type', 'application/json']]),
        arrayBuffer: async () => Buffer.from('{"data":[1]}').buffer,
      };
    };

    const logs = [];
    const result = await fetchWithRetry({
      fetchImpl: fakeFetch,
      url: 'https://example.com',
      timeoutMs: 1000,
      retryDelaysMs: [1],
      stepName: 'download_artifact',
      logs,
    });

    expect(result.ok).toBe(true);
    expect(result.status).toBe(200);
    expect(calls).toBe(2);
    expect(logs.length).toBe(2);
  });
});
