const express = require('express');
const { queryFlow } = require('./flow');

function createApp() {
  const app = express();

  app.get('/health', (_req, res) => {
    res.status(200).json({ status: 'ok' });
  });

  app.get('/api/flow', (req, res) => {
    res.status(200).json(queryFlow(req.query));
  });

  return app;
}

module.exports = { createApp };
