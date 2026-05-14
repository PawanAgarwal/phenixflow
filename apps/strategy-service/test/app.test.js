const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const request = require('supertest');

const { createApp } = require('../src/app');
const { createDefaultRegistry } = require('../src/default-registry');
const { ensureStrategyResultContract } = require('../src/strategy-contract');
const { openStrategyResultStore, persistStrategyReport } = require('../src/result-store');
const {
  ACTIONABLE_STATUSES,
  executionSummaryFromManifest,
  getExecutionManifest,
  getExecutionManifestDefinition,
  validateExecutionManifestDefinition,
} = require('../src/strategies/execution');

const PYM_EOD_EXECUTION = {
  manifestVersion: 'execution-manifest.v1',
  strategyVersion: 'pym-v5.execution.v1',
  status: 'paper_enabled',
  promotion: {
    authorized: true,
    domain: 'production_candidate',
    authorizedStatuses: ['paper_enabled', 'live_enabled'],
  },
  timingClass: 'EOD',
  timezone: 'America/New_York',
  session: 'REGULAR',
  activation: {
    type: 'after_market_close',
    time: '16:05',
  },
  signalCadence: 'daily_eod',
  idempotencyKeyFields: ['strategyId', 'strategyVersion', 'signalDate'],
};

const TSLL_SCALP_EXECUTION = {
  manifestVersion: 'execution-manifest.v1',
  strategyVersion: 'tsll-seconds-passive-scalper.execution.v1',
  status: 'paper_enabled',
  promotion: {
    authorized: true,
    domain: 'production_candidate',
    authorizedStatuses: ['paper_enabled', 'live_enabled'],
  },
  timingClass: 'SCALP',
  timezone: 'America/New_York',
  session: 'REGULAR',
  activation: {
    type: 'regular_session_window',
    startTime: '09:35',
    endTime: '15:50',
  },
  signalCadence: 'continuous_intraday',
  idempotencyKeyFields: ['strategyId', 'strategyVersion', 'signalTimestamp'],
};

function fakeStrategy() {
  const metadata = {
    id: 'fake-strategy',
    name: 'Fake Strategy',
    cadence: 'daily_eod',
    execution: PYM_EOD_EXECUTION,
    supports: ['chart', 'latest_portfolio'],
  };
  const report = {
    generatedAt: '2026-05-08T00:00:00.000Z',
    source: { provider: 'test' },
    settings: { startDate: '2026-05-06' },
    summary: {
      latestRebalanceDate: '2026-05-07',
      snapshots: 2,
      totalReturn: 0.1,
      totalReturnPct: 10,
    },
    latest: null,
    equitySeries: [
      {
        date: '2026-05-06',
        signalDate: '2026-05-05',
        equity: 100,
        totalReturn: 0,
        spyReturn: 0,
        qqqReturn: 0,
      },
      {
        date: '2026-05-07',
        signalDate: '2026-05-06',
        equity: 110,
        totalReturn: 0.1,
        spyReturn: 0.03,
        qqqReturn: 0.04,
      },
    ],
    snapshots: [
      {
        date: '2026-05-06',
        nextDate: '2026-05-07',
        equityBeforeNextSession: 100,
        grossExposure: 1,
        turnover: 1,
        turnoverPct: 100,
        topHoldings: 'AAA',
        benchmarkReturns: { spy: 0, qqq: 0 },
        holdings: [{ ticker: 'AAA', weight: 1, weightPct: 100, dollars: 100 }],
        realized: { netReturn: 0.1, netReturnPct: 10, endEquity: 110 },
      },
      {
        date: '2026-05-07',
        nextDate: null,
        equityBeforeNextSession: 110,
        grossExposure: 1,
        turnover: 1,
        turnoverPct: 100,
        topHoldings: 'BBB',
        benchmarkReturns: { spy: 0.03, qqq: 0.04 },
        holdings: [{ ticker: 'BBB', weight: 1, weightPct: 100, dollars: 110 }],
        realized: null,
      },
    ],
  };
  report.latest = report.snapshots[1];
  return {
    state: { loadedAt: '2026-05-08T00:01:00.000Z', refresh: { running: false } },
    getMetadata: () => metadata,
    getReport: () => report,
    recompute: () => report,
  };
}

function fakeRegistry() {
  const strategy = fakeStrategy();
  return {
    listStrategies: () => [strategy.getMetadata()],
    getStrategy: (id) => {
      if (id !== 'fake-strategy') {
        const error = new Error(`unknown_strategy:${id}`);
        error.statusCode = 404;
        error.code = 'unknown_strategy';
        throw error;
      }
      return strategy;
    },
  };
}

describe('strategy-service API', () => {
  it('registers the PYM studies for dashboard discovery', () => {
    const registry = createDefaultRegistry();
    const strategies = registry.listStrategies();
    expect(strategies.map((strategy) => strategy.id)).toEqual([
      'pym-v5',
      'pym-v5-option-rank-top8',
      'pym-v5-ml-two-speed-attention',
      'pym-v5-ml-calm-trend-router',
      'pym-v5-ml-option-top8-50-50',
      'pym-v5-two-speed-option-meta21',
      'pym-v5-spy-put-pressure-bil',
      'pym-v5-sleeve-meta-21d-cap25',
      'pym-v5-cap25-lgbm-blend40',
      'pym-v5-cap25-lgbm-blend40-stress',
      'option-income-wheel-trend-ivrv',
      'tsll-seconds-passive-scalper',
      'pym-gated-intraday-baseline',
      'pym-gated-intraday-lev3x',
      'pym-gated-intraday-overnight-1x',
      'pym-gated-intraday-best-combo',
      'pym-gated-intraday-deadzone-biasprop-1500exit-3x',
      'occ-pc-contrarian-intraday-1x-long-only',
      'occ-pc-contrarian-intraday-3x',
      'vix-term-contrarian-intraday-vix3m-1x',
      'vix-term-contrarian-intraday-inv-long-3x-overnight',
      'vvix-spike-contrarian-overnight-3x',
      'gap-down-fade-intraday-3x',
      'fear-extreme-portfolio-equalweight-4x',
      'fear-basket-vvix-occ3x-vix3xon-3x',
      'fear-basket-vvix-vix3xon-3x',
      'gap-fade-vix3xon-hedge-3x',
    ]);
    const byId = Object.fromEntries(strategies.map((strategy) => [strategy.id, strategy]));
    expect(strategies.every((strategy) => strategy.execution)).toBe(true);
    expect(byId['pym-v5'].execution).toEqual(PYM_EOD_EXECUTION);
    expect(byId['tsll-seconds-passive-scalper'].execution).toEqual(TSLL_SCALP_EXECUTION);
    expect(byId['pym-v5'].execution).toEqual(executionSummaryFromManifest(getExecutionManifest('pym-v5')));
    expect(byId['tsll-seconds-passive-scalper'].execution).toEqual(
      executionSummaryFromManifest(getExecutionManifest('tsll-seconds-passive-scalper')),
    );
    const productionAuthorized = strategies
      .filter((strategy) => strategy.execution.promotion.authorized)
      .map((strategy) => strategy.id);
    expect(productionAuthorized).toEqual(['pym-v5', 'tsll-seconds-passive-scalper']);
    strategies
      .filter((strategy) => !productionAuthorized.includes(strategy.id))
      .forEach((strategy) => {
        expect(strategy.execution.status).toBe('research_only');
        expect(strategy.execution.promotion).toEqual({
          authorized: false,
          domain: 'research',
          authorizedStatuses: ['research_only'],
        });
      });
    expect(byId['pym-v5'].sourceLinks.map((link) => link.label)).toEqual([
      'Original Study / Notion',
      'Composer Factsheet',
      'Composer Source',
    ]);
    expect(byId['pym-v5-spy-put-pressure-bil'].ruleSummary).toContain(
      'if SPY option put-pressure z-score >= 2.5: hold 100% BIL',
    );
    expect(byId['pym-v5-ml-calm-trend-router'].ruleSummary.join(' ')).toContain('Calm-trend conditions');
    expect(byId['pym-v5-ml-option-top8-50-50'].ruleSummary.join(' ')).toContain('50% option top-8');
    expect(byId['pym-v5-two-speed-option-meta21'].ruleSummary.join(' ')).toContain('prior 21-day');
    expect(byId['pym-v5-sleeve-meta-21d-cap25'].ruleSummary.join(' ')).toContain('cap any single sleeve');
    expect(byId['pym-v5-sleeve-meta-21d-cap25'].displayName).toContain('25% cap');
    expect(byId['pym-v5-cap25-lgbm-blend40'].ruleSummary.join(' ')).toContain('60% cap25 + 40% LGBM');
    expect(byId['pym-v5-cap25-lgbm-blend40'].displayName).toContain('LightGBM');
    expect(byId['pym-v5-cap25-lgbm-blend40-stress'].ruleSummary.join(' ')).toContain('options-stress signal');
    expect(byId['pym-v5-cap25-lgbm-blend40-stress'].displayName).toContain('Options Stress');
    expect(byId['option-income-wheel-trend-ivrv'].ruleSummary.join(' ')).toContain('IV/RV >= 1.10');
    expect(byId['tsll-seconds-passive-scalper'].ruleSummary.join(' ')).toContain('buy limit 3c');
    expect(byId['pym-gated-intraday-baseline'].supports).toContain('trade_log');
    expect(byId['vvix-spike-contrarian-overnight-3x'].ruleSummary.join(' ')).toContain('VVIX');
    expect(byId['gap-down-fade-intraday-3x'].ruleSummary.join(' ')).toContain('gap');
    expect(byId['fear-extreme-portfolio-equalweight-4x'].ruleSummary.join(' ')).toContain('Equal');
    expect(byId['fear-basket-vvix-occ3x-vix3xon-3x'].displayName).toContain('VVIX');
    expect(byId['fear-basket-vvix-vix3xon-3x'].components).toHaveLength(2);
    expect(byId['gap-fade-vix3xon-hedge-3x'].components).toHaveLength(2);
  });

  it('lists strategies and serves chart ranges', async () => {
    const app = createApp({ registry: fakeRegistry() });
    const strategies = await request(app).get('/api/strategies').expect(200);
    expect(strategies.body.data).toEqual([expect.objectContaining({ id: 'fake-strategy' })]);

    const chart = await request(app)
      .get('/api/strategies/fake-strategy/chart?start=2026-05-07&end=2026-05-07')
      .expect(200);
    expect(chart.body.range.points).toBe(1);
    expect(chart.body.data[0]).toEqual(expect.objectContaining({ date: '2026-05-07', equity: 110 }));

    const trades = await request(app)
      .get('/api/strategies/fake-strategy/trades?limit=3')
      .expect(200);
    expect(trades.body.range.count).toBe(0);
    expect(trades.body.data).toEqual([]);
  });

  it('returns latest portfolio change from the previous rebalance', async () => {
    const app = createApp({ registry: fakeRegistry() });
    const response = await request(app).get('/api/strategies/fake-strategy/portfolio/latest').expect(200);
    expect(response.body.data.snapshot.date).toBe('2026-05-07');
    expect(response.body.data.changeFromPrevious.added[0]).toEqual(expect.objectContaining({ ticker: 'BBB' }));
    expect(response.body.data.changeFromPrevious.removed[0]).toEqual(expect.objectContaining({ ticker: 'AAA' }));
  });

  it('emits and stores explicit daily result contracts without equity-series derivation', () => {
    const strategy = fakeStrategy();
    const metadata = strategy.getMetadata();
    const report = ensureStrategyResultContract(strategy.getReport(), metadata);
    expect(report.latestDailyResult).toEqual(expect.objectContaining({
      strategyId: 'fake-strategy',
      date: '2026-05-07',
      signalDate: '2026-05-06',
      basis: 'eod_prior_holdings_next_close',
      netReturnPct: 10,
      startEquity: 100,
      endEquity: 110,
    }));

    const dbPath = path.join(fs.mkdtempSync(path.join(os.tmpdir(), 'strategy-results-')), 'results.sqlite');
    const db = openStrategyResultStore(dbPath);
    const persisted = persistStrategyReport(db, { metadata, report, importedAt: '2026-05-08T00:02:00.000Z' });
    expect(persisted).toEqual(expect.objectContaining({
      strategyId: 'fake-strategy',
      dailyResultCount: 1,
      holdingCount: 2,
      tradeCount: 0,
    }));
    expect(db.prepare('select net_return_pct, basis from strategy_daily_results where strategy_id=? and date=?')
      .get('fake-strategy', '2026-05-07')).toEqual({
      net_return_pct: 10,
      basis: 'eod_prior_holdings_next_close',
    });
    expect(db.prepare('select count(*) as count from strategy_holdings where strategy_id=?').get('fake-strategy').count)
      .toBe(2);
    db.close();
  });

  it('serves execution manifest list and details for promoted strategies', async () => {
    const app = createApp();
    const list = await request(app).get('/api/execution-manifests').expect(200);
    expect(list.body.data.map((manifest) => manifest.strategyId)).toEqual([
      'pym-v5',
      'tsll-seconds-passive-scalper',
    ]);
    expect(list.body.data.every((manifest) => manifest.promoted === undefined)).toBe(true);

    const pym = await request(app).get('/api/execution-manifests/pym-v5').expect(200);
    expect(pym.body.data).toEqual(expect.objectContaining({
      manifestVersion: 'execution-manifest.v1',
      strategyId: 'pym-v5',
      strategyVersion: 'pym-v5.execution.v1',
      status: 'paper_enabled',
      promotion: {
        authorized: true,
        domain: 'production_candidate',
        authorizedStatuses: ['paper_enabled', 'live_enabled'],
      },
      timingClass: 'EOD',
      signalEndpoint: '/api/portfolio-targets/pym-v5/latest',
      idempotencyKeyFields: ['strategyId', 'strategyVersion', 'signalDate'],
    }));
    expect(pym.body.data.activation).toEqual({ type: 'after_market_close', time: '16:05' });
    expect(pym.body.data.executionDefaults.orderType).toBe('market');
    expect(pym.body.data.riskDefaults.allowedSymbols).toContain('SPY');
    expect(pym.body.data.provenance.sourceArtifactPaths).toContain(
      'projects/pym-v5-replication/artifacts/pym-v5-rebalance-report.json',
    );

    const tsll = await request(app).get('/api/execution-manifests/tsll-seconds-passive-scalper').expect(200);
    expect(tsll.body.data).toEqual(expect.objectContaining({
      strategyId: 'tsll-seconds-passive-scalper',
      strategyVersion: 'tsll-seconds-passive-scalper.execution.v1',
      status: 'paper_enabled',
      promotion: {
        authorized: true,
        domain: 'production_candidate',
        authorizedStatuses: ['paper_enabled', 'live_enabled'],
      },
      timingClass: 'SCALP',
      symbols: ['TSLL'],
      idempotencyKeyFields: ['strategyId', 'strategyVersion', 'signalTimestamp'],
    }));
    expect(tsll.body.data.activation).toEqual({
      type: 'regular_session_window',
      startTime: '09:35',
      endTime: '15:50',
    });
    expect(tsll.body.data.executionDefaults).toEqual(expect.objectContaining({
      orderType: 'limit',
      buyBelowCloseCents: 3,
      targetCents: 3,
      stopCents: 5,
      maxHoldSeconds: 10,
      maxHoldBars: 10,
      barSeconds: 1,
      maxQuoteAgeSeconds: 2,
    }));
    expect(tsll.body.data.signalEndpoint).toBe('/api/kernels/tsll-seconds-passive-scalper.execution.v1/manifest');
    expect(tsll.body.data.kernel).toEqual(expect.objectContaining({
      schemaVersion: 'phenixflow.strategyKernel.v1',
      artifactUri: '/api/kernels/tsll-seconds-passive-scalper.execution.v1/manifest',
      settingsSha256: '624b14f4e8cfe581159a9c84953d1c44720acd779d1cabf6e1501714bef3ddc1',
      fixtureSuiteSha256: '19403ba6fe5bb6486d08fb50d78e241ac0b4fbc6a5e48ddc91098823104d9bfd',
    }));
    expect(tsll.body.data.provenance.baseline).toEqual(expect.objectContaining({
      datasetId: 'tsll-1s-2025-01-02-2026-05-12-massive-rest-1s-barSeconds1-nodaily',
      expectedSummarySha256: '3c4de269eb8d5014ae8585b93d2998f4925acb38749ad1e9e42e27f4c50669dd',
    }));
  });

  it('serves kernel bundle metadata and versioned PYM target snapshots', async () => {
    const app = createApp();
    const kernel = await request(app).get('/api/kernels/tsll-seconds-passive-scalper.execution.v1/manifest').expect(200);
    expect(kernel.body.data.manifest.strategy.id).toBe('tsll-seconds-passive-scalper');
    expect(kernel.body.data.manifest.settings.sha256).toBe('624b14f4e8cfe581159a9c84953d1c44720acd779d1cabf6e1501714bef3ddc1');
    expect(kernel.body.data.artifact.files).toContain('dist/kernel.mjs');

    const target = await request(app).get('/api/portfolio-targets/pym-v5/latest').expect(200);
    expect(target.body.data).toEqual(expect.objectContaining({
      schemaVersion: 'phenixflow.portfolioTarget.v1',
      strategyId: 'pym-v5',
      strategyVersion: 'pym-v5.execution.v1',
      targetType: 'portfolio_weights',
    }));
    expect(target.body.data.targetWeights.length).toBeGreaterThan(0);
    expect(target.body.data.hashes.snapshotSha256).toMatch(/^[a-f0-9]{64}$/);
  });

  it('keeps metadata execution summaries in sync with manifest definitions', async () => {
    const app = createApp();
    const strategies = await request(app).get('/api/strategies').expect(200);
    const byId = Object.fromEntries(strategies.body.data.map((strategy) => [strategy.id, strategy]));
    for (const strategyId of ['pym-v5', 'tsll-seconds-passive-scalper']) {
      const manifest = await request(app).get(`/api/execution-manifests/${strategyId}`).expect(200);
      expect(byId[strategyId].execution).toEqual(executionSummaryFromManifest(manifest.body.data));
    }
  });

  it('does not expose paper/live manifests for non-promoted strategies', async () => {
    const app = createApp();
    await request(app).get('/api/execution-manifests/pym-v5-option-rank-top8').expect(404);

    const list = await request(app).get('/api/execution-manifests').expect(200);
    const actionable = list.body.data.filter((manifest) => ACTIONABLE_STATUSES.includes(manifest.status));
    expect(actionable.map((manifest) => manifest.strategyId)).toEqual([
      'pym-v5',
      'tsll-seconds-passive-scalper',
    ]);
  });

  it('rejects invalid execution manifest definitions', () => {
    const base = getExecutionManifestDefinition('pym-v5');
    expect(() => validateExecutionManifestDefinition({
      ...base,
      strategyId: 'not-promoted',
      promoted: false,
      status: 'paper_enabled',
    })).toThrow(/actionable_status_requires_promoted_strategy/);

    expect(() => validateExecutionManifestDefinition({
      ...base,
      timingClass: 'NOT_A_TIMING_CLASS',
    })).toThrow(/timing_class_unknown/);

    expect(() => validateExecutionManifestDefinition({
      ...base,
      promotion: { authorized: false, domain: 'research', authorizedStatuses: ['research_only'] },
    })).toThrow(/actionable_status_requires_promotion_authorization/);

    expect(() => validateExecutionManifestDefinition({
      ...base,
      activation: { type: 'after_market_close' },
    })).toThrow(/activation\.time_must_be_non_empty_string/);
  });
});
