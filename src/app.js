const express = require('express');
const { queryFlow, buildFlowFacets, buildFlowStream, getFlowDetail } = require('./flow');

function createApp() {
  const app = express();

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

  app.get('/api/flow', listFlowHandler);
  app.get('/api/flow/facets', facetsFlowHandler);
  app.get('/api/flow/stream', streamFlowHandler);
  app.get('/api/flow/:id', detailFlowHandler);

  // Backward-compatible API v1 aliases.
  app.get('/api/v1/flow', listFlowHandler);
  app.get('/api/v1/flow/facets', facetsFlowHandler);
  app.get('/api/v1/flow/stream', streamFlowHandler);
  app.get('/api/v1/flow/:id', detailFlowHandler);

  return app;
}

module.exports = { createApp };
