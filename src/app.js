const express = require('express');
const {
  queryFlow,
  buildFlowFacets,
  buildFlowSummary,
  buildFlowFiltersCatalog,
  buildFlowStream,
  getFlowDetail,
} = require('./flow');
const { queryHistoricalFlow } = require('./historical-flow');
const { getReadiness } = require('./readiness');
const {
  createSaved,
  getSaved,
  serializeRecord,
  resolvePayloadVersion,
} = require('./saved-filters-alerts');

function createApp() {
  const app = express();
  app.use(express.json());

  const wantsSseStream = (req) => {
    if (typeof req.query.transport === 'string' && req.query.transport.trim().toLowerCase() === 'sse') {
      return true;
    }

    const acceptHeader = typeof req.headers.accept === 'string' ? req.headers.accept : '';
    return acceptHeader.toLowerCase().includes('text/event-stream');
  };

  const buildSseWatermark = (baseWatermark, offset) => {
    if (baseWatermark === undefined || baseWatermark === null || baseWatermark === '') {
      return String(offset);
    }

    const parsedNumber = Number(baseWatermark);
    if (Number.isFinite(parsedNumber)) {
      return String(parsedNumber + offset);
    }

    return `${String(baseWatermark)}:${offset}`;
  };

  app.get('/health', (_req, res) => {
    res.status(200).json({ status: 'ok' });
  });

  app.get('/ready', async (_req, res) => {
    const readiness = await getReadiness(process.env);
    res.status(readiness.statusCode).json(readiness.body);
  });

  const listFlowHandler = (req, res) => {
    res.status(200).json(queryFlow(req.query, { filterVersion: req.query.filterVersion }));
  };

  const facetsFlowHandler = (req, res) => {
    res.status(200).json(buildFlowFacets(req.query, { filterVersion: req.query.filterVersion }));
  };

  const streamFlowHandler = (req, res) => {
    const streamPayload = buildFlowStream(req.query, { filterVersion: req.query.filterVersion });

    if (!wantsSseStream(req)) {
      return res.status(200).json(streamPayload);
    }

    res.status(200);
    res.setHeader('Content-Type', 'text/event-stream; charset=utf-8');
    res.setHeader('Cache-Control', 'no-cache');
    res.setHeader('Connection', 'keep-alive');
    if (typeof res.flushHeaders === 'function') {
      res.flushHeaders();
    }

    const baseWatermark = req.query.watermark;
    streamPayload.data.forEach((event, index) => {
      const sequence = event.sequence;
      const watermark = buildSseWatermark(baseWatermark, index + 1);
      const payload = {
        sequence,
        watermark,
        eventType: 'flow.updated',
        flow: event.flow,
      };

      res.write('event: flow.updated\n');
      res.write(`data: ${JSON.stringify(payload)}\n\n`);
    });

    const keepaliveSequence = streamPayload.data.length + 1;
    const keepalivePayload = {
      sequence: keepaliveSequence,
      watermark: buildSseWatermark(baseWatermark, keepaliveSequence),
      eventType: 'keepalive',
    };

    res.write('event: keepalive\n');
    res.write(`data: ${JSON.stringify(keepalivePayload)}\n\n`);
    res.end();
    return undefined;
  };

  const summaryFlowHandler = (req, res) => {
    res.status(200).json(buildFlowSummary(req.query, { filterVersion: req.query.filterVersion }));
  };

  const filtersCatalogHandler = (req, res) => {
    res.status(200).json(buildFlowFiltersCatalog(req.query, { filterVersion: req.query.filterVersion }));
  };

  const detailFlowHandler = (req, res) => {
    const flow = getFlowDetail(req.params.id, req.query || {});
    if (!flow) {
      return res.status(404).json({ error: { code: 'not_found', message: 'Flow not found' } });
    }
    return res.status(200).json({ data: flow });
  };

  const historicalFlowHandler = async (req, res) => {
    const result = await queryHistoricalFlow(req.query);
    if (result.error) {
      return res.status(result.status || 500).json({ error: result.error });
    }
    return res.status(200).json(result);
  };

  const createSavedHandler = (kind) => (req, res) => {
    try {
      const record = createSaved(kind, req.body || {});
      res.status(201).json({ data: serializeRecord(record, 'v2') });
    } catch (error) {
      res.status(503).json({
        error: {
          code: 'db_unavailable',
          message: error.message,
          details: [],
        },
      });
    }
  };

  const getSavedHandler = (kind, defaultVersion) => (req, res) => {
    let record;
    try {
      record = getSaved(kind, req.params.id);
    } catch (error) {
      return res.status(503).json({
        error: {
          code: 'db_unavailable',
          message: error.message,
          details: [],
        },
      });
    }

    if (!record) {
      return res.status(404).json({
        error: {
          code: 'not_found',
          message: `${kind.slice(0, -1)} not found`,
          details: [],
        },
      });
    }
    const payloadVersion = resolvePayloadVersion(req.query.payloadVersion, defaultVersion);
    return res.status(200).json({ data: serializeRecord(record, payloadVersion) });
  };

  app.get('/api/flow', listFlowHandler);
  app.get('/api/flow/facets', facetsFlowHandler);
  app.get('/api/flow/summary', summaryFlowHandler);
  app.get('/api/flow/filters/catalog', filtersCatalogHandler);
  app.get('/api/flow/stream', streamFlowHandler);
  app.get('/api/flow/historical', historicalFlowHandler);
  app.get('/api/flow/:id', detailFlowHandler);
  app.post('/api/flow/presets', createSavedHandler('presets'));
  app.get('/api/flow/presets/:id', getSavedHandler('presets', 'v2'));
  app.post('/api/flow/alerts', createSavedHandler('alerts'));
  app.get('/api/flow/alerts/:id', getSavedHandler('alerts', 'v2'));

  // Backward-compatible API v1 aliases.
  app.get('/api/v1/flow', listFlowHandler);
  app.get('/api/v1/flow/facets', facetsFlowHandler);
  app.get('/api/v1/flow/summary', summaryFlowHandler);
  app.get('/api/v1/flow/filters/catalog', filtersCatalogHandler);
  app.get('/api/v1/flow/stream', streamFlowHandler);
  app.get('/api/v1/flow/historical', historicalFlowHandler);
  app.get('/api/v1/flow/:id', detailFlowHandler);
  app.post('/api/v1/flow/presets', createSavedHandler('presets'));
  app.get('/api/v1/flow/presets/:id', getSavedHandler('presets', 'legacy'));
  app.post('/api/v1/flow/alerts', createSavedHandler('alerts'));
  app.get('/api/v1/flow/alerts/:id', getSavedHandler('alerts', 'legacy'));

  return app;
}

module.exports = { createApp };
