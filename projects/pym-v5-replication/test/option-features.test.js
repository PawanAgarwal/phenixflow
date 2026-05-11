const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');

const { parseOpraTicker, opraRoot, daysBetween } = require('../src/option-symbol');
const {
  addOptionBarFeature,
  emptyRootFeature,
  finalizeRootFeature,
  latestOptionBarsDate,
  optionBarsParquetPath,
  parseOptionBarLine,
  resolveOptionBarsSource,
  withRollingOptionStats,
} = require('../src/option-features');
const { resolveStockBarsSource } = require('../src/market-data');
const { optionMomentumScore } = require('../src/option-overlay-suite');

describe('option symbol parsing', () => {
  it('parses OPRA roots, expirations, rights, and strikes', () => {
    const parsed = parseOpraTicker('O:SPY250117C00588000');
    expect(parsed).toEqual({
      ticker: 'O:SPY250117C00588000',
      root: 'SPY',
      expiration: '2025-01-17',
      right: 'CALL',
      strike: 588,
    });
    expect(opraRoot('O:SPXW250103P05850000')).toBe('SPXW');
    expect(daysBetween('2025-01-02', '2025-01-17')).toBe(15);
  });
});

describe('daily option feature aggregation', () => {
  it('aggregates call and put premium, DTE, and ATM flow proxies', () => {
    const rows = [
      'O:SPY250102C00590000,10,1,1.2,1.3,0.9,1735842600000000000,4',
      'O:SPY250102P00590000,5,2,2.1,2.2,1.9,1735842660000000000,2',
    ].map(parseOptionBarLine);
    const features = new Map();
    const closes = new Map([['SPY', 590]]);
    rows.forEach((row) => addOptionBarFeature(features, '2025-01-02', row, closes));
    const spy = finalizeRootFeature(features.get('SPY'));
    expect(spy.totalVolume).toBe(15);
    expect(spy.callVolume).toBe(10);
    expect(spy.putVolume).toBe(5);
    expect(spy.zeroDteVolumeShare).toBeCloseTo(1);
    expect(spy.atmVolumeShare).toBeCloseTo(1);
    expect(spy.shortDatedAtmFlowProxy).toBeGreaterThan(0);
    expect(spy.premiumImbalance).toBeGreaterThan(0);
  });

  it('adds causal rolling stats before pushing the current day', () => {
    const rows = [];
    for (let day = 1; day <= 7; day += 1) {
      const feature = finalizeRootFeature({
        ...emptyRootFeature('SPY'),
        totalPremium: 100 + day,
        callPremium: 60 + day,
        putPremium: 40,
        totalVolume: 100,
        callVolume: 60,
        putVolume: 40,
      });
      rows.push({ date: `2025-01-0${day}`, roots: { SPY: feature } });
    }
    const enriched = withRollingOptionStats(rows, { window: 5 });
    expect(enriched[0].roots.SPY.rolling.premiumImbalanceZ5).toBe(0);
    expect(enriched[6].roots.SPY.rolling.premiumImbalanceZ5).toBeGreaterThan(0);
  });

  it('scores option momentum from rolling flow and call premium momentum', () => {
    const score = optionMomentumScore({
      optionFeatures: {
        roots: {
          SPY: {
            totalPremium: 1_000_000,
            premiumImbalance: 0.4,
            rolling: {
              premiumImbalanceZ20: 1,
              callPremiumMomentum5: 0.5,
              putPremiumMomentum5: -0.2,
              shortDatedAtmFlowProxyZ20: 0.3,
            },
          },
        },
      },
    }, 'SPY');
    expect(score).toBeGreaterThan(1);
  });

  it('falls back to live parquet option bars and finds the latest available option date', () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), 'pym-option-features-'));
    const historical = path.join(root, 'historical');
    const liveParquet = path.join(root, 'live-parquet');
    const config = {
      roots: { historical, liveParquet },
      datasets: { optionBars: 'option_quotes_1m' },
    };
    const parquetDir = path.join(liveParquet, 'option_quotes_1m', 'date=2026-05-08');
    const csvDir = path.join(historical, 'option_quotes_1m', 'date=2026-05-07');
    fs.mkdirSync(parquetDir, { recursive: true });
    fs.mkdirSync(csvDir, { recursive: true });
    fs.writeFileSync(path.join(parquetDir, '2026-05-08.live.parquet'), '');
    fs.writeFileSync(path.join(csvDir, '2026-05-07.csv.gz'), '');

    expect(optionBarsParquetPath(config, '2026-05-08')).toBe(path.join(parquetDir, '2026-05-08.live.parquet'));
    expect(resolveOptionBarsSource(config, '2026-05-08')).toEqual(expect.objectContaining({
      format: 'parquet',
      filePath: path.join(parquetDir, '2026-05-08.live.parquet'),
    }));
    expect(resolveOptionBarsSource(config, '2026-05-07')).toEqual(expect.objectContaining({
      format: 'csv.gz',
      filePath: path.join(csvDir, '2026-05-07.csv.gz'),
    }));
    expect(latestOptionBarsDate(config)).toBe('2026-05-08');
  });

  it('prefers historical option bars when both historical and live files exist', () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), 'pym-option-prefer-historical-'));
    const historical = path.join(root, 'historical');
    const liveParquet = path.join(root, 'live-parquet');
    const csvDir = path.join(historical, 'option_quotes_1m', 'date=2026-05-11');
    const parquetDir = path.join(liveParquet, 'option_quotes_1m', 'date=2026-05-11');
    fs.mkdirSync(csvDir, { recursive: true });
    fs.mkdirSync(parquetDir, { recursive: true });
    fs.writeFileSync(path.join(csvDir, '2026-05-11.csv.gz'), '');
    fs.writeFileSync(path.join(parquetDir, '2026-05-11.live.parquet'), '');

    const config = {
      roots: { historical, liveParquet },
      datasets: { optionBars: 'option_quotes_1m' },
    };

    expect(resolveOptionBarsSource(config, '2026-05-11')).toEqual(expect.objectContaining({
      format: 'csv.gz',
      filePath: path.join(csvDir, '2026-05-11.csv.gz'),
    }));
  });

  it('falls back to live parquet stock bars when historical csv is absent', () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), 'pym-stock-live-'));
    const historical = path.join(root, 'historical');
    const liveParquet = path.join(root, 'live-parquet');
    const parquetDir = path.join(liveParquet, 'stock_quotes_1m', 'date=2026-05-11');
    fs.mkdirSync(parquetDir, { recursive: true });
    fs.writeFileSync(path.join(parquetDir, '2026-05-11.live.parquet'), '');

    const config = {
      roots: { historical, liveParquet },
      datasets: { stockBars: 'stock_quotes_1m' },
    };

    expect(resolveStockBarsSource(config, '2026-05-11')).toEqual(expect.objectContaining({
      format: 'parquet',
      filePath: path.join(parquetDir, '2026-05-11.live.parquet'),
    }));
  });

  it('prefers historical stock bars when both historical and live files exist', () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), 'pym-stock-prefer-historical-'));
    const historical = path.join(root, 'historical');
    const liveParquet = path.join(root, 'live-parquet');
    const csvDir = path.join(historical, 'stock_quotes_1m', 'date=2026-05-11');
    const parquetDir = path.join(liveParquet, 'stock_quotes_1m', 'date=2026-05-11');
    fs.mkdirSync(csvDir, { recursive: true });
    fs.mkdirSync(parquetDir, { recursive: true });
    fs.writeFileSync(path.join(csvDir, '2026-05-11.csv.gz'), '');
    fs.writeFileSync(path.join(parquetDir, '2026-05-11.live.parquet'), '');

    const config = {
      roots: { historical, liveParquet },
      datasets: { stockBars: 'stock_quotes_1m' },
    };

    expect(resolveStockBarsSource(config, '2026-05-11')).toEqual(expect.objectContaining({
      format: 'csv.gz',
      filePath: path.join(csvDir, '2026-05-11.csv.gz'),
    }));
  });
});
