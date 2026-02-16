const express = require('express');
const { queryFlow, buildFlowFacets, buildFlowStream, getFlowDetail } = require('./flow');
const { queryHistoricalFlow } = require('./historical-flow');
const {
  createSaved,
  getSaved,
  serializeRecord,
  resolvePayloadVersion,
} = require('./saved-filters-alerts');

function createApp() {
  const app = express();
  app.use(express.json());

  app.get('/health', (_req, res) => {
    res.status(200).json({ status: 'ok' });
  });

  const listFlowHandler = (req, res) => {
    res.status(200).json(queryFlow(req.query, { filterVersion: req.query.filterVersion }));
  };

  const facetsFlowHandler = (req, res) => {
    res.status(200).json(buildFlowFacets(req.query, { filterVersion: req.query.filterVersion }));
  };

  const streamFlowHandler = (req, res) => {
    res.status(200).json(buildFlowStream(req.query, { filterVersion: req.query.filterVersion }));
  };

  const detailFlowHandler = (req, res) => {
    const flow = getFlowDetail(req.params.id);
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
    const record = createSaved(kind, req.body || {});
    res.status(201).json({ data: serializeRecord(record, 'v2') });
  };

  const getSavedHandler = (kind, defaultVersion) => (req, res) => {
    const record = getSaved(kind, req.params.id);
    if (!record) {
      return res.status(404).json({ error: { code: 'not_found', message: `${kind.slice(0, -1)} not found` } });
    }
    const payloadVersion = resolvePayloadVersion(req.query.payloadVersion, defaultVersion);
    return res.status(200).json({ data: serializeRecord(record, payloadVersion) });
  };

  app.get('/api/flow', listFlowHandler);
  app.get('/api/flow/facets', facetsFlowHandler);
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
