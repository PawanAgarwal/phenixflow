const express = require('express');
const { queryFlow, FlowQueryValidationError } = require('./flow');

function createApp() {
  const app = express();

  app.get('/health', (_req, res) => {
    res.status(200).json({ status: 'ok' });
  });

  app.get('/api/flow', (req, res) => {
    try {
      res.status(200).json(queryFlow(req.query));
    } catch (error) {
      if (error instanceof FlowQueryValidationError) {
        res.status(400).json({ error: error.message });
        return;
      }
      res.status(500).json({ error: 'Internal server error.' });
    }
  });

  return app;
}

module.exports = { createApp };
