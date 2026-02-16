const {
  resolveThetaBaseUrl,
  resolveThetaIngestPath,
} = require('../config/env');

function parseColumnarRows(parsed) {
  if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) return [];

  const entries = Object.entries(parsed).filter(([, value]) => Array.isArray(value));
  if (!entries.length) return [];

  const rowCount = entries[0][1].length;
  if (!entries.every(([, values]) => values.length === rowCount)) return [];

  return Array.from({ length: rowCount }, (_unused, index) => {
    const row = {};
    entries.forEach(([key, values]) => {
      row[key] = values[index];
    });
    return row;
  });
}

function parseThetaRows(rawBody) {
  let parsed;
  try {
    parsed = JSON.parse(rawBody);
  } catch {
    return { rows: [], watermark: null };
  }

  if (Array.isArray(parsed)) {
    return { rows: parsed.filter((row) => row && typeof row === 'object'), watermark: null };
  }

  if (parsed && Array.isArray(parsed.rows)) {
    return {
      rows: parsed.rows.filter((row) => row && typeof row === 'object'),
      watermark: parsed.watermark || parsed.next_watermark || parsed.nextWatermark || null,
    };
  }

  if (parsed && Array.isArray(parsed.data)) {
    return {
      rows: parsed.data.filter((row) => row && typeof row === 'object'),
      watermark: parsed.watermark || parsed.next_watermark || parsed.nextWatermark || null,
    };
  }

  const columnarRows = parseColumnarRows(parsed);
  if (columnarRows.length) {
    return {
      rows: columnarRows,
      watermark: parsed.watermark || parsed.next_watermark || parsed.nextWatermark || null,
    };
  }

  return { rows: [], watermark: parsed && typeof parsed === 'object' ? (parsed.watermark || null) : null };
}

class ThetaDataClient {
  constructor(options = {}) {
    this.env = options.env || process.env;
    this.fetchImpl = options.fetchImpl || global.fetch;
    this.baseUrl = options.baseUrl || resolveThetaBaseUrl(this.env);
    this.ingestPath = options.ingestPath || resolveThetaIngestPath(this.env);
  }

  buildIngestUrl({ symbol, watermark, limit } = {}) {
    if (!this.baseUrl) {
      throw new Error('thetadata_base_url_missing');
    }

    const url = new URL(`${this.baseUrl}${this.ingestPath}`);
    url.searchParams.set('format', 'json');

    if (symbol) url.searchParams.set('symbol', symbol);
    if (watermark !== undefined && watermark !== null && watermark !== '') {
      url.searchParams.set('watermark', String(watermark));
    }
    if (Number.isFinite(Number(limit)) && Number(limit) > 0) {
      url.searchParams.set('limit', String(Math.trunc(Number(limit))));
    }

    return url.toString();
  }

  async fetchIngestBatch({ symbol, watermark, limit } = {}) {
    if (typeof this.fetchImpl !== 'function') {
      throw new Error('fetch_unavailable');
    }

    const endpoint = this.buildIngestUrl({ symbol, watermark, limit });
    const response = await this.fetchImpl(endpoint);
    const body = await response.text();

    if (!response.ok) {
      throw new Error(`thetadata_request_failed:${response.status}`);
    }

    const parsed = parseThetaRows(body);
    return {
      endpoint,
      rows: parsed.rows,
      watermark: parsed.watermark,
    };
  }
}

module.exports = {
  ThetaDataClient,
  parseThetaRows,
};
