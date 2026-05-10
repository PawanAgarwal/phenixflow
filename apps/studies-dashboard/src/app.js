const path = require('node:path');

const express = require('express');

const DEFAULT_STRATEGY_API_URL = 'http://localhost:3120';

function baseUrlFrom(value) {
  return String(value || DEFAULT_STRATEGY_API_URL).replace(/\/+$/, '');
}

async function readUpstreamText(response) {
  try {
    return await response.text();
  } catch {
    return '';
  }
}

function createProxyHandler({ strategyApiUrl, fetchImpl = fetch }) {
  const baseUrl = baseUrlFrom(strategyApiUrl);

  return async function proxyStrategyApi(req, res, next) {
    try {
      const headers = {
        accept: req.get('accept') || 'application/json',
      };
      const hasJsonBody = !['GET', 'HEAD'].includes(req.method) && req.body && Object.keys(req.body).length > 0;
      const init = {
        method: req.method,
        headers,
      };
      if (hasJsonBody) {
        headers['content-type'] = 'application/json';
        init.body = JSON.stringify(req.body);
      }

      const response = await fetchImpl(`${baseUrl}${req.originalUrl}`, init);
      const body = await readUpstreamText(response);
      const contentType = response.headers.get('content-type') || 'application/json';
      res.status(response.status).type(contentType).send(body);
    } catch (error) {
      next(error);
    }
  };
}

function createApp(options = {}) {
  const app = express();
  const strategyApiUrl = baseUrlFrom(options.strategyApiUrl || process.env.STRATEGY_API_URL);
  const publicRoot = options.publicRoot || path.resolve(__dirname, '..', 'public');
  const fetchImpl = options.fetchImpl || fetch;

  app.use(express.json({ limit: '1mb' }));

  app.get('/api/health', async (_req, res) => {
    const payload = {
      status: 'ok',
      service: 'phenixflow-studies-dashboard',
      strategyApiUrl,
      upstream: null,
    };
    try {
      const response = await fetchImpl(`${strategyApiUrl}/api/health`, { headers: { accept: 'application/json' } });
      payload.upstream = {
        ok: response.ok,
        status: response.status,
      };
    } catch (error) {
      payload.upstream = {
        ok: false,
        error: error.message,
      };
    }
    res.status(payload.upstream?.ok === false ? 503 : 200).json(payload);
  });

  app.get('/api/dashboard/config', (_req, res) => {
    res.status(200).json({
      defaultStrategyId: process.env.STUDIES_DEFAULT_STRATEGY || 'pym-v5',
      strategyApiUrl,
    });
  });

  app.use('/api/strategies', createProxyHandler({ strategyApiUrl, fetchImpl }));

  app.use((req, res, next) => {
    if (req.path === '/' || req.path === '/index.html' || req.path.endsWith('.js') || req.path.endsWith('.css')) {
      res.set('Cache-Control', 'no-store');
    }
    next();
  });

  app.use(express.static(publicRoot));

  app.get('*', (_req, res) => {
    res.sendFile(path.join(publicRoot, 'index.html'));
  });

  app.use((error, _req, res, _next) => {
    res.status(502).json({
      error: {
        code: 'studies_dashboard_upstream_error',
        message: error.message,
      },
    });
  });

  return app;
}

module.exports = {
  createApp,
  createProxyHandler,
  baseUrlFrom,
};
