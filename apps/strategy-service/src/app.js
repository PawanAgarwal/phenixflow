const express = require('express');

const { createDefaultRegistry } = require('./default-registry');
const { filterByRange, lastOrNull, normalizeDate, parseLimit } = require('./range');
const { snapshotResponse } = require('./portfolio');

function asyncRoute(handler) {
  return (req, res, next) => Promise.resolve(handler(req, res, next)).catch(next);
}

function strategyFromReq(registry, req) {
  return registry.getStrategy(req.params.strategyId);
}

function publicStrategySummary(strategy) {
  const report = strategy.getReport();
  return {
    metadata: strategy.getMetadata(),
    loadedAt: strategy.state?.loadedAt || null,
    generatedAt: report.generatedAt,
    source: report.source,
    settings: report.settings,
    summary: report.summary,
    refresh: strategy.state?.refresh || null,
  };
}

function previousSnapshot(report, date) {
  const index = report.snapshots.findIndex((snapshot) => snapshot.date === date);
  if (index <= 0) return null;
  return report.snapshots[index - 1];
}

function snapshotByDate(report, date) {
  if (!date || date === 'latest') return report.latest;
  return report.snapshots.find((snapshot) => snapshot.date === date) || null;
}

function chartPayload(strategy, query) {
  const report = strategy.getReport();
  const start = normalizeDate(query.start);
  const end = normalizeDate(query.end);
  const points = filterByRange(report.equitySeries, { start, end }).map((point) => ({
    date: point.date,
    signalDate: point.signalDate,
    equity: point.equity,
    totalReturn: point.totalReturn,
    totalReturnPct: point.totalReturn * 100,
    benchmarks: {
      spy: point.spyReturn,
      spyPct: Number.isFinite(point.spyReturn) ? point.spyReturn * 100 : null,
      qqq: point.qqqReturn,
      qqqPct: Number.isFinite(point.qqqReturn) ? point.qqqReturn * 100 : null,
    },
  }));
  const first = points[0] || null;
  const last = lastOrNull(points);
  return {
    strategy: strategy.getMetadata(),
    range: {
      requestedStart: start,
      requestedEnd: end,
      actualStart: first?.date || null,
      actualEnd: last?.date || null,
      points: points.length,
    },
    summary: {
      startEquity: first?.equity || null,
      endEquity: last?.equity || null,
      totalReturn: first && last ? (last.equity / first.equity) - 1 : null,
      totalReturnPct: first && last ? ((last.equity / first.equity) - 1) * 100 : null,
      latestStrategyReturn: last?.totalReturn ?? null,
      latestSpyReturn: last?.benchmarks.spy ?? null,
      latestQqqReturn: last?.benchmarks.qqq ?? null,
    },
    data: points,
  };
}

function valuesPayload(strategy, query) {
  const report = strategy.getReport();
  const start = normalizeDate(query.start);
  const end = normalizeDate(query.end);
  const limit = parseLimit(query.limit, report.snapshots.length);
  const data = filterByRange(report.snapshots, { start, end }).slice(-limit).map((snapshot) => ({
    date: snapshot.date,
    nextDate: snapshot.nextDate,
    equityBeforeNextSession: snapshot.equityBeforeNextSession,
    grossExposure: snapshot.grossExposure,
    turnover: snapshot.turnover,
    turnoverPct: snapshot.turnoverPct,
    topHoldings: snapshot.topHoldings,
    holdingCount: snapshot.holdings.length,
    netReturn: snapshot.realized?.netReturn ?? null,
    netReturnPct: snapshot.realized?.netReturnPct ?? null,
    endEquity: snapshot.realized?.endEquity ?? null,
    spyReturn: snapshot.benchmarkReturns.spy,
    qqqReturn: snapshot.benchmarkReturns.qqq,
  }));
  return {
    strategy: strategy.getMetadata(),
    range: { requestedStart: start, requestedEnd: end, count: data.length, total: report.snapshots.length },
    data,
  };
}

function portfolioPayload(strategy, date = 'latest') {
  const report = strategy.getReport();
  const snapshot = snapshotByDate(report, date);
  if (!snapshot) {
    const error = new Error(`portfolio_date_not_found:${date}`);
    error.statusCode = 404;
    error.code = 'portfolio_date_not_found';
    throw error;
  }
  return {
    strategy: strategy.getMetadata(),
    data: snapshotResponse(snapshot, previousSnapshot(report, snapshot.date)),
  };
}

function createApp(options = {}) {
  const app = express();
  const registry = options.registry || createDefaultRegistry(options);

  app.use(express.json());

  app.get('/', (_req, res) => {
    res.status(200).json({
      service: 'phenixflow-strategy-service',
      version: '0.1.0',
      endpoints: [
        'GET /api/strategies',
        'GET /api/strategies/:strategyId',
        'GET /api/strategies/:strategyId/chart?start=YYYY-MM-DD&end=YYYY-MM-DD',
        'GET /api/strategies/:strategyId/values?start=YYYY-MM-DD&end=YYYY-MM-DD',
        'GET /api/strategies/:strategyId/portfolio/latest',
        'GET /api/strategies/:strategyId/portfolio/:date',
        'GET /api/strategies/:strategyId/changes/latest',
        'POST /api/strategies/:strategyId/recompute',
        'POST /api/strategies/:strategyId/refresh-data',
      ],
    });
  });

  app.get('/api/health', (_req, res) => {
    res.status(200).json({ status: 'ok', service: 'phenixflow-strategy-service' });
  });

  app.get('/api/strategies', (_req, res) => {
    res.status(200).json({ data: registry.listStrategies() });
  });

  app.get('/api/strategies/:strategyId', asyncRoute(async (req, res) => {
    res.status(200).json(publicStrategySummary(strategyFromReq(registry, req)));
  }));

  app.get('/api/strategies/:strategyId/chart', asyncRoute(async (req, res) => {
    res.status(200).json(chartPayload(strategyFromReq(registry, req), req.query));
  }));

  app.get('/api/strategies/:strategyId/values', asyncRoute(async (req, res) => {
    res.status(200).json(valuesPayload(strategyFromReq(registry, req), req.query));
  }));

  app.get('/api/strategies/:strategyId/portfolio/latest', asyncRoute(async (req, res) => {
    res.status(200).json(portfolioPayload(strategyFromReq(registry, req), 'latest'));
  }));

  app.get('/api/strategies/:strategyId/portfolio/:date', asyncRoute(async (req, res) => {
    res.status(200).json(portfolioPayload(strategyFromReq(registry, req), req.params.date));
  }));

  app.get('/api/strategies/:strategyId/changes/latest', asyncRoute(async (req, res) => {
    const payload = portfolioPayload(strategyFromReq(registry, req), 'latest');
    res.status(200).json({
      strategy: payload.strategy,
      data: payload.data.changeFromPrevious,
    });
  }));

  app.post('/api/strategies/:strategyId/recompute', asyncRoute(async (req, res) => {
    const strategy = strategyFromReq(registry, req);
    const report = strategy.recompute();
    res.status(200).json({
      strategy: strategy.getMetadata(),
      loadedAt: strategy.state?.loadedAt || null,
      summary: report.summary,
    });
  }));

  app.post('/api/strategies/:strategyId/refresh-data', asyncRoute(async (req, res) => {
    const strategy = strategyFromReq(registry, req);
    if (typeof strategy.refreshData !== 'function') {
      res.status(405).json({ error: { code: 'refresh_not_supported', message: 'Strategy does not support data refresh.' } });
      return;
    }
    const result = strategy.refreshData();
    res.status(result.accepted ? 202 : 409).json({
      strategy: strategy.getMetadata(),
      refresh: result.status,
    });
  }));

  app.use((error, _req, res, _next) => {
    const statusCode = error.statusCode || 503;
    res.status(statusCode).json({
      error: {
        code: error.code || 'strategy_service_error',
        message: error.message,
      },
    });
  });

  return app;
}

module.exports = {
  createApp,
  chartPayload,
  valuesPayload,
  portfolioPayload,
};
