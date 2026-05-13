const express = require('express');

const { createDefaultRegistry } = require('./default-registry');
const { filterByRange, lastOrNull, normalizeDate, parseLimit } = require('./range');
const { snapshotResponse } = require('./portfolio');
const { getExecutionManifest, listExecutionManifests } = require('./strategies/execution');

function asyncRoute(handler) {
  return (req, res, next) => Promise.resolve(handler(req, res, next)).catch(next);
}

function strategyFromReq(registry, req) {
  return registry.getStrategy(req.params.strategyId);
}

function finiteOrNull(value) {
  return Number.isFinite(value) ? value : null;
}

function uniqueStrings(values) {
  return [...new Set(values.filter((value) => typeof value === 'string' && value.trim()))];
}

function reportPathHints(report) {
  return uniqueStrings([
    report?.source?.reportPath,
    report?.source?.report?.path,
    report?.source?.sourceArtifact,
    report?.source?.mlReport?.path,
    report?.source?.optionReport?.path,
    report?.source?.riskOverlayReport?.path,
    report?.source?.dataset?.path,
  ]);
}

function manifestWithReportContext(manifest, report, reportError = null) {
  const next = JSON.parse(JSON.stringify(manifest));
  const summary = report?.summary || {};
  const latest = report?.latest || {};

  next.theoreticalPerformance = {
    ...next.theoreticalPerformance,
    summaryStats: {
      ...next.theoreticalPerformance.summaryStats,
      totalReturnPct: finiteOrNull(summary.totalReturnPct),
      maxDrawdownPct: finiteOrNull(summary.maxDrawdownPct),
      sharpe: finiteOrNull(summary.sharpe),
      hitRatePct: finiteOrNull(summary.hitRatePct),
      winRate: finiteOrNull(summary.winRate),
      trades: finiteOrNull(summary.trades),
      pnlPer1000Shares: finiteOrNull(summary.pnlPer1000Shares),
      snapshots: finiteOrNull(summary.snapshots),
    },
    latestExpectedSignalDate: latest.date || summary.latestRebalanceDate || summary.latestCompletedDate || null,
    latestExpectedTargetDate: latest.nextDate || summary.latestCompletedDate || summary.latestRebalanceDate || null,
  };

  next.provenance = {
    ...next.provenance,
    sourceArtifactPaths: uniqueStrings([
      ...(next.provenance.sourceArtifactPaths || []),
      ...reportPathHints(report),
    ]),
    generatedAt: report?.generatedAt || next.provenance.generatedAt || null,
    backtestWindow: {
      startDate: report?.settings?.startDate || summary.startDate || next.provenance.backtestWindow?.startDate || null,
      endDate: report?.settings?.endDate || summary.endDate || summary.latestCompletedDate
        || next.provenance.backtestWindow?.endDate || null,
    },
    commit: process.env.GIT_COMMIT || process.env.GITHUB_SHA || process.env.VERCEL_GIT_COMMIT_SHA
      || next.provenance.commit || null,
  };
  if (reportError) next.provenance.reportLoadError = reportError.message;
  return next;
}

function reportForManifest(strategy) {
  try {
    return { report: strategy.getReport(), error: null };
  } catch (error) {
    return { report: null, error };
  }
}

function executionManifestFromReq(registry, req) {
  const strategy = registry.getStrategy(req.params.strategyId);
  const manifest = getExecutionManifest(strategy.getMetadata().id);
  if (!manifest) {
    const error = new Error(`execution_manifest_not_found:${req.params.strategyId}`);
    error.statusCode = 404;
    error.code = 'execution_manifest_not_found';
    throw error;
  }
  const { report, error } = reportForManifest(strategy);
  return manifestWithReportContext(manifest, report, error);
}

function executionManifestList(registry) {
  const strategyIds = registry.listStrategies().map((strategy) => strategy.id);
  return listExecutionManifests({ strategyIds }).map((manifest) => {
    try {
      const strategy = registry.getStrategy(manifest.strategyId);
      const { report, error } = reportForManifest(strategy);
      return manifestWithReportContext(manifest, report, error);
    } catch {
      return manifest;
    }
  });
}

function deriveDailyReturns(equitySeries) {
  if (!Array.isArray(equitySeries) || equitySeries.length < 2) return [];
  const out = [];
  for (let i = 1; i < equitySeries.length; i += 1) {
    const prev = equitySeries[i - 1]?.equity;
    const curr = equitySeries[i]?.equity;
    if (!Number.isFinite(prev) || prev <= 0 || !Number.isFinite(curr)) continue;
    out.push(curr / prev - 1);
  }
  return out;
}

function computeSharpe(dailyReturns) {
  if (!dailyReturns.length) return null;
  const mean = dailyReturns.reduce((a, x) => a + x, 0) / dailyReturns.length;
  const variance = dailyReturns.reduce((a, x) => a + ((x - mean) ** 2), 0) / dailyReturns.length;
  const sd = Math.sqrt(variance);
  if (!Number.isFinite(sd) || sd === 0) return null;
  return (mean / sd) * Math.sqrt(252);
}

function computeHitRate(snapshots) {
  if (!Array.isArray(snapshots) || snapshots.length === 0) return null;
  let active = 0; let wins = 0;
  for (const s of snapshots) {
    const r = s.realized?.netReturn;
    if (!Number.isFinite(r)) continue;
    if (r === 0) continue; // skip flat days
    active += 1;
    if (r > 0) wins += 1;
  }
  if (active === 0) return null;
  return wins / active;
}

function enrichSummary(report) {
  // Add Sharpe and hitRate to every strategy's summary by computing from equity / snapshots
  // when the strategy didn't precompute them. Never overwrite values the strategy already set.
  const summary = { ...(report.summary || {}) };
  if (!Number.isFinite(summary.sharpe)) {
    const sharpe = computeSharpe(deriveDailyReturns(report.equitySeries));
    if (Number.isFinite(sharpe)) summary.sharpe = sharpe;
  }
  if (!Number.isFinite(summary.hitRate)) {
    const hr = computeHitRate(report.snapshots);
    if (Number.isFinite(hr)) {
      summary.hitRate = hr;
      summary.hitRatePct = hr * 100;
    }
  }
  // Derive activeDays / tradingDays for the UI's Trades/Days metric when missing
  if (!Number.isFinite(summary.tradingDays) && Array.isArray(report.snapshots)) {
    summary.tradingDays = report.snapshots.length;
  }
  if (!Number.isFinite(summary.activeDays) && Array.isArray(report.snapshots)) {
    summary.activeDays = report.snapshots.filter((s) => {
      const r = s.realized?.netReturn;
      return Number.isFinite(r) && r !== 0;
    }).length;
  }
  return summary;
}

function publicStrategySummary(strategy) {
  const report = strategy.getReport();
  return {
    metadata: strategy.getMetadata(),
    loadedAt: strategy.state?.loadedAt || null,
    generatedAt: report.generatedAt,
    source: report.source,
    settings: report.settings,
    summary: enrichSummary(report),
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

function openPositionsPayload(strategy) {
  const report = strategy.getReport();
  const data = Array.isArray(report.openPositions) ? report.openPositions : [];
  return {
    strategy: strategy.getMetadata(),
    asOf: report.summary?.endDate || report.summary?.latestRebalanceDate || null,
    count: data.length,
    data,
  };
}

function tradesPayload(strategy, query) {
  const report = strategy.getReport();
  const trades = Array.isArray(report.trades) ? report.trades : [];
  const start = normalizeDate(query.start);
  const end = normalizeDate(query.end);
  const limit = parseLimit(query.limit, trades.length);
  const filtered = trades.filter((trade) => {
    const date = trade.date;
    if (start && date < start) return false;
    if (end && date > end) return false;
    return true;
  });
  // Trades are already sorted by date in the artifact; preserve order and apply limit to most recent.
  const data = filtered.slice(-limit);
  return {
    strategy: strategy.getMetadata(),
    range: {
      requestedStart: start,
      requestedEnd: end,
      count: data.length,
      total: trades.length,
    },
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
        'GET /api/execution-manifests',
        'GET /api/execution-manifests/:strategyId',
        'GET /api/strategies/:strategyId',
        'GET /api/strategies/:strategyId/chart?start=YYYY-MM-DD&end=YYYY-MM-DD',
        'GET /api/strategies/:strategyId/values?start=YYYY-MM-DD&end=YYYY-MM-DD',
        'GET /api/strategies/:strategyId/trades?start=YYYY-MM-DD&end=YYYY-MM-DD&limit=N',
        'GET /api/strategies/:strategyId/open-positions',
        'GET /api/strategies/:strategyId/portfolio/latest',
        'GET /api/strategies/:strategyId/portfolio/:date',
        'GET /api/strategies/:strategyId/changes/latest',
        'POST /api/strategies/:strategyId/recompute',
        'POST /api/strategies/:strategyId/refresh-data',
        'POST /api/refresh-all',
        'GET /api/refresh-status',
      ],
    });
  });

  app.get('/api/health', (_req, res) => {
    res.status(200).json({ status: 'ok', service: 'phenixflow-strategy-service' });
  });

  app.get('/api/strategies', (_req, res) => {
    res.status(200).json({ data: registry.listStrategies() });
  });

  app.get('/api/execution-manifests', (_req, res) => {
    res.status(200).json({ data: executionManifestList(registry) });
  });

  app.get('/api/execution-manifests/:strategyId', asyncRoute(async (req, res) => {
    res.status(200).json({ data: executionManifestFromReq(registry, req) });
  }));

  app.get('/api/strategies/:strategyId', asyncRoute(async (req, res) => {
    res.status(200).json(publicStrategySummary(strategyFromReq(registry, req)));
  }));

  app.get('/api/strategies/:strategyId/chart', asyncRoute(async (req, res) => {
    res.status(200).json(chartPayload(strategyFromReq(registry, req), req.query));
  }));

  app.get('/api/strategies/:strategyId/values', asyncRoute(async (req, res) => {
    res.status(200).json(valuesPayload(strategyFromReq(registry, req), req.query));
  }));

  app.get('/api/strategies/:strategyId/trades', asyncRoute(async (req, res) => {
    res.status(200).json(tradesPayload(strategyFromReq(registry, req), req.query));
  }));

  app.get('/api/strategies/:strategyId/open-positions', asyncRoute(async (req, res) => {
    res.status(200).json(openPositionsPayload(strategyFromReq(registry, req)));
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

  // Refresh all registered strategies. Each strategy's refreshData runs
  // its own sequence of build steps in the background, so this endpoint
  // returns 202 immediately with a list of accept/reject statuses. Use
  // GET /api/refresh-status to poll progress.
  app.post('/api/refresh-all', asyncRoute(async (_req, res) => {
    const strategies = registry.listStrategies();
    const triggered = [];
    strategies.forEach((meta) => {
      const strategy = registry.getStrategy(meta.id);
      if (typeof strategy.refreshData !== 'function') {
        triggered.push({ id: meta.id, accepted: false, reason: 'refresh_not_supported' });
        return;
      }
      const result = strategy.refreshData();
      triggered.push({ id: meta.id, accepted: result.accepted, status: result.status });
    });
    res.status(202).json({ triggered });
  }));

  app.get('/api/refresh-status', asyncRoute(async (_req, res) => {
    const strategies = registry.listStrategies();
    const statuses = strategies.map((meta) => {
      const strategy = registry.getStrategy(meta.id);
      const refresh = strategy.state?.refresh || null;
      return {
        id: meta.id,
        loadedAt: strategy.state?.loadedAt || null,
        refresh: refresh ? {
          running: refresh.running || false,
          startedAt: refresh.startedAt || null,
          finishedAt: refresh.finishedAt || null,
          exitCode: refresh.exitCode ?? null,
          currentStep: refresh.currentStep || null,
          completedSteps: refresh.completedSteps || [],
          plannedSteps: refresh.plannedSteps || [],
          error: refresh.error || null,
        } : null,
      };
    });
    res.status(200).json({ data: statuses });
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
