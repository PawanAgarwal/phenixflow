const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { queryFlow, __private } = require('../src/flow');

describe('MON-64 real-ingest parser + schema-drift guards', () => {
  describe('parser behavior', () => {
    it.each([
      ['top-level array', JSON.stringify([{ id: 'a' }, { id: 'b' }]), ['a', 'b']],
      ['rows envelope', JSON.stringify({ rows: [{ id: 'c' }] }), ['c']],
      ['data envelope', JSON.stringify({ data: [{ id: 'd' }] }), ['d']],
      ['additive metadata around rows', JSON.stringify({ version: 2, meta: { source: 'theta' }, rows: [{ id: 'e' }] }), ['e']],
    ])('parses %s', (_name, rawContent, expectedIds) => {
      const rows = __private.parseRealIngestRows(rawContent);
      expect(rows.map((row) => row.id)).toEqual(expectedIds);
    });

    it('throws artifact_missing_rows when payload shape is unsupported', () => {
      const unsupportedShape = JSON.stringify({ results: [{ id: 'x' }] });

      expect(() => __private.parseRealIngestRows(unsupportedShape)).toThrow('artifact_missing_rows');
    });
  });

  describe('failure handling + schema drift safety', () => {
    const writeArtifact = (content) => {
      const artifactDir = fs.mkdtempSync(path.join(os.tmpdir(), 'mon-64-parser-'));
      const artifactPath = path.join(artifactDir, 'flow-read.json');
      fs.writeFileSync(artifactPath, content, 'utf8');
      return artifactPath;
    };

    it('falls back to fixtures when artifact JSON is invalid', () => {
      const artifactPath = writeArtifact('{not-json');

      const result = queryFlow({ source: 'real-ingest', artifactPath });

      expect(result.meta.observability).toEqual({
        source: 'fixtures',
        artifactPath: path.resolve(artifactPath),
        rowCount: 10,
        fallbackReason: 'artifact_read_error',
      });
      expect(result.page.total).toBe(10);
    });

    it('falls back to fixtures with explicit rows-missing reason when envelope drifts', () => {
      const artifactPath = writeArtifact(JSON.stringify({ results: [{ id: 'drifted' }] }));

      const result = queryFlow({ source: 'real-ingest', artifactPath });

      expect(result.meta.observability).toEqual({
        source: 'fixtures',
        artifactPath: path.resolve(artifactPath),
        rowCount: 10,
        fallbackReason: 'artifact_rows_missing',
      });
      expect(result.page.total).toBe(10);
    });

    it('continues using real-ingest when shape remains compatible despite additive fields', () => {
      const artifactPath = writeArtifact(
        JSON.stringify({
          schemaVersion: '2026-02-16',
          producer: 'upstream-vNext',
          rows: [
            {
              id: 'trade_1',
              symbol: 'AAPL',
              strategy: 'breakout',
              status: 'open',
              timeframe: '1m',
              pnl: 3,
              volume: 2,
              createdAt: '2026-02-15T16:00:00.000Z',
              updatedAt: '2026-02-15T16:00:01.000Z',
              unknown_additive_field: 'safe',
            },
          ],
        }),
      );

      const result = queryFlow({ source: 'real-ingest', artifactPath });

      expect(result.meta.observability).toEqual({
        source: 'real-ingest',
        artifactPath: path.resolve(artifactPath),
        rowCount: 1,
        fallbackReason: null,
      });
      expect(result.data.map((row) => row.id)).toEqual(['trade_1']);
    });
  });
});
