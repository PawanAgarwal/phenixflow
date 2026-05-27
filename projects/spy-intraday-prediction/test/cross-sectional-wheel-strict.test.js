const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');

const {
  DEFAULT_INITIAL_CAPITAL,
  STRICT_BASE_STRATEGY,
  STRICT_VIX_OVERLAY_STRATEGY,
  constituentsForDate,
  inspectOptionBidAskHeader,
  loadPointInTimeMembership,
  preflightStrictWheelBacktest,
} = require('../src/cross-sectional-wheel-strict');

function makeTempDir() {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'strict-wheel-'));
}

function writeJson(filePath, value) {
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  fs.writeFileSync(filePath, `${JSON.stringify(value, null, 2)}\n`, 'utf8');
}

function writeText(filePath, value) {
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  fs.writeFileSync(filePath, value, 'utf8');
}

function makeConfig(root, dates = ['2025-01-02']) {
  ['stock_quotes_1m', 'option_quotes_1m', 'indices_1m'].forEach((dataset) => {
    dates.forEach((dayIso) => {
      fs.mkdirSync(path.join(root, 'massive', dataset, `date=${dayIso}`), { recursive: true });
    });
  });
  const calendarPath = path.join(root, 'calendar.json');
  writeJson(calendarPath, {
    days: dates.map((date) => ({ date, isOpen: true })),
  });
  return {
    dataPolicy: { provider: 'Massive' },
    roots: {
      historical: path.join(root, 'massive'),
      calendar: calendarPath,
    },
    datasets: {
      stockBars: 'stock_quotes_1m',
      optionBars: 'option_quotes_1m',
      indexBars: 'indices_1m',
    },
  };
}

describe('strict cross-sectional wheel gate', () => {
  it('captures the required base strategy and first VIX variant', () => {
    expect(DEFAULT_INITIAL_CAPITAL).toBe(100_000);
    expect(STRICT_BASE_STRATEGY.entryScan).toBe('daily_close');
    expect(STRICT_BASE_STRATEGY.putMoneyness).toBe(0.95);
    expect(STRICT_BASE_STRATEGY.callMoneyness).toBe(0.95);
    expect(STRICT_BASE_STRATEGY.maxCommittedPositions).toBe(5);
    expect(STRICT_VIX_OVERLAY_STRATEGY.vixOverlay.highVixPutMoneyness).toBe(1.05);
  });

  it('loads point-in-time membership snapshots with carry-forward semantics', () => {
    const dir = makeTempDir();
    const membershipPath = path.join(dir, 'sp500-pit.json');
    writeJson(membershipPath, {
      byDate: {
        '2025-01-01': ['AAPL', 'MSFT'],
        '2025-01-06': ['AAPL', 'NVDA'],
      },
    });

    const membership = loadPointInTimeMembership(membershipPath);
    expect(constituentsForDate(membership, '2025-01-02')).toEqual(['AAPL', 'MSFT']);
    expect(constituentsForDate(membership, '2025-01-07')).toEqual(['AAPL', 'NVDA']);
  });

  it('loads quoted daily CSV membership snapshots', () => {
    const dir = makeTempDir();
    const membershipPath = path.join(dir, 'sp500-pit.csv');
    writeText(membershipPath, [
      'date,tickers',
      '2025-01-01,"AAPL,MSFT,BRK.B"',
      '2025-01-06,"AAPL,NVDA"',
      '',
    ].join('\n'));

    const membership = loadPointInTimeMembership(membershipPath);
    expect(constituentsForDate(membership, '2025-01-02')).toEqual(['AAPL', 'BRK.B', 'MSFT']);
    expect(constituentsForDate(membership, '2025-01-07')).toEqual(['AAPL', 'NVDA']);
  });

  it('detects executable option bid/ask headers', () => {
    expect(inspectOptionBidAskHeader(['date', 'ticker', 'bid', 'ask'])).toEqual({
      hasAsk: true,
      hasBid: true,
      hasOptionIdentifier: true,
    });
    expect(inspectOptionBidAskHeader(['ticker', 'open', 'close', 'high', 'low']).hasBid).toBe(false);
  });

  it('fails loudly when hard data requirements are absent', async () => {
    const dir = makeTempDir();
    const config = makeConfig(dir);

    const report = await preflightStrictWheelBacktest({
      config,
      startDate: '2025-01-02',
      endDate: '2025-01-02',
      minHeadlineTradingDays: 1,
      targetHistoryTradingDays: 1,
      minConstituents: 1,
    });

    const codes = report.errors.map((error) => error.code);
    expect(report.status).toBe('FAIL');
    expect(codes).toContain('missing_point_in_time_sp500_membership');
    expect(codes).toContain('missing_executable_option_pricing_source');
    expect(codes).toContain('missing_dividend_source');
    expect(codes).toContain('missing_risk_free_source');
  });

  it('passes preflight with strict fixture sources', async () => {
    const dir = makeTempDir();
    const config = makeConfig(dir);
    const membershipPath = path.join(dir, 'sp500-pit.json');
    const bidAskPath = path.join(dir, 'option-bid-ask.csv');
    const dividendsPath = path.join(dir, 'dividends.csv');
    const ratesPath = path.join(dir, 'rates.csv');

    writeJson(membershipPath, {
      byDate: {
        '2025-01-01': ['AAPL'],
      },
    });
    writeText(bidAskPath, 'date,ticker,bid,ask\n2025-01-02,O:AAPL250124P00230000,1.00,1.05\n');
    writeText(dividendsPath, 'date,symbol,amount\n2025-01-02,AAPL,0.24\n');
    writeText(ratesPath, 'date,rate\n2025-01-02,0.05\n');

    const report = await preflightStrictWheelBacktest({
      config,
      startDate: '2025-01-02',
      endDate: '2025-01-02',
      membershipPath,
      optionBidAskPath: bidAskPath,
      dividendsPath,
      riskFreePath: ratesPath,
      slippageBps: 1,
      commissionPerContract: 0.65,
      minHeadlineTradingDays: 1,
      targetHistoryTradingDays: 1,
      minConstituents: 1,
    });

    expect(report.status).toBe('PASS');
    expect(report.errors).toEqual([]);
    expect(report.dataSources.optionPricing.mode).toBe('bid_ask');
  });
});
