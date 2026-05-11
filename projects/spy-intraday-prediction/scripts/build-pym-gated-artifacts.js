#!/usr/bin/env node
// Build artifact JSONs for the 4 PYM-gated intraday strategies, in the schema
// expected by apps/strategy-service. Output: one JSON per variant under
// projects/spy-intraday-prediction/artifacts/.
//
// Variants:
//   pym-gated-baseline       — 1x SPY, intraday only (11:30 → 15:55), bias ±0.20
//   pym-gated-lev3x          — 3x via SPXL/SPXU, intraday only, bias ±0.20
//   pym-gated-overnight-1x   — 1x SPY, overnight hold on |bias| ≥ 0.40, intraday otherwise
//   pym-gated-best-combo     — 3x via TQQQ/SQQQ (overnight on extreme) + SPXL/SPXU (intraday), bias ±0.20

const path = require('node:path');
const fs = require('node:fs');
const zlib = require('node:zlib');
const readline = require('node:readline');

const { PROJECT_ROOT, loadConfig } = require('../src/config');
const { loadPymHoldings, pymBias } = require('../src/pym-bias-strategy');
const { defaultFeaturesPath, loadStockBars } = require('../src/build-features-1m');
const { getEtParts } = require('../src/time');

const DEFAULT_PYM_ARTIFACTS_DIR = '/Users/pawanagarwal/github/phenixflow/projects/pym-v5-replication/artifacts';
const ARTIFACTS_OUT_DIR = path.join(PROJECT_ROOT, 'artifacts');

const INITIAL_CAPITAL = 10_000;

function findLatestPymArtifact() {
  if (!fs.existsSync(DEFAULT_PYM_ARTIFACTS_DIR)) return null;
  const candidates = fs.readdirSync(DEFAULT_PYM_ARTIFACTS_DIR)
    .filter((n) => /^pym-v5-backtest-massive-eod-rsi-wilder-next_close-\d{4}-\d{2}-\d{2}-\d{4}-\d{2}-\d{2}\.json$/.test(n))
    .sort();
  if (!candidates.length) return null;
  return path.join(DEFAULT_PYM_ARTIFACTS_DIR, candidates[candidates.length - 1]);
}

function isWeekend(d) {
  const x = new Date(`${d}T00:00:00.000Z`).getUTCDay();
  return x === 0 || x === 6;
}

// Cache the loaded universe config so we can fall back to raw stock_quotes_1m for days
// where features-1m JSONL hasn't been built yet (typically today, before EOD aggregation).
let cachedUniverseConfig = null;
function getUniverseConfig() {
  if (!cachedUniverseConfig) cachedUniverseConfig = loadConfig();
  return cachedUniverseConfig;
}

// Build a synthetic per-minute features row set from raw SPY 1m bars. This is the minimum
// data the PYM-gated variants need (spy_open / spy_close / spy_high / spy_low at the entry
// and exit minutes), enabling today's signal without rebuilding the full features-1m chain.
async function loadFeaturesFromRawStockBars(day) {
  const universe = getUniverseConfig();
  const byMinute = await loadStockBars(universe, day, 'SPY');
  if (byMinute.size === 0) return null;
  const rows = [];
  for (const [minuteMs, bar] of byMinute.entries()) {
    const et = getEtParts(minuteMs);
    if (et.dateEt !== day) continue;
    if (et.minuteOfDayEt < 570 || et.minuteOfDayEt >= 960) continue;
    rows.push({
      minute_ms: minuteMs,
      date_et: et.dateEt,
      minute_of_day_et: et.minuteOfDayEt,
      spy_open: bar.open,
      spy_high: bar.high,
      spy_low: bar.low,
      spy_close: bar.close,
      spy_volume: bar.volume,
      __source: 'raw_stock_bars',
    });
  }
  if (rows.length === 0) return null;
  rows.sort((a, b) => a.minute_ms - b.minute_ms);
  return rows;
}

async function loadFeatures(day) {
  const p = defaultFeaturesPath(PROJECT_ROOT, 'SPY', day);
  if (fs.existsSync(p)) {
    const stream = fs.createReadStream(p).pipe(zlib.createGunzip());
    const rl = readline.createInterface({ input: stream, crlfDelay: Infinity });
    const out = [];
    for await (const line of rl) {
      if (!line) continue;
      out.push(JSON.parse(line));
    }
    if (out.length > 0) return out;
  }
  // Fallback: synthesize from raw SPY 1m bars (historical CSV or today's live parquet via DuckDB).
  return loadFeaturesFromRawStockBars(day);
}

// Strategy variants — each takes a per-day decision context and returns a trade plan (or null).
const VARIANTS = [
  {
    id: 'pym-gated-intraday-baseline',
    name: 'PYM-Gated Intraday SPY (Baseline 1x)',
    displayName: 'PYM-Gated Intraday SPY — 1x Baseline',
    description: 'Use PYM v5 daily bias as direction; enter SPY at 11:30 ET when |bias| >= 0.20; exit at 15:55 ET (intraday only, no overnight).',
    ruleSummary: [
      'Each day: compute PYM v5 directional bias from its EOD portfolio (risk_on − defensive − inverse weights).',
      'If bias ≥ +0.20 at the prior close: LONG SPY at 11:30 ET, exit 15:55 ET.',
      'If bias ≤ -0.20 at the prior close: SHORT SPY at 11:30 ET, exit 15:55 ET.',
      'Otherwise: flat (cash).',
    ],
    costBpsRoundTrip: 2,
    entryMinuteEt: 690,
    exitMinuteEt: 955,
    biasLong: 0.20,
    biasShort: -0.20,
    leverage: 1.0,
    overnightExtremeBias: null, // no overnight
    longTicker: 'SPY',
    shortTicker: 'SH',
    overnightLongTicker: null,
    overnightShortTicker: null,
  },
  {
    id: 'pym-gated-intraday-lev3x',
    name: 'PYM-Gated Intraday 3x (SPXL/SPXU)',
    displayName: 'PYM-Gated Intraday 3x — SPXL/SPXU',
    description: 'Same PYM bias gate but uses 3x leveraged ETFs (SPXL long / SPXU short) for amplified intraday exposure. Intraday only (11:30 → 15:55 ET); no overnight.',
    ruleSummary: [
      'PYM v5 bias direction as in baseline.',
      'If bias ≥ +0.20: buy SPXL (3x SPY long) at 11:30 ET, sell at 15:55 ET.',
      'If bias ≤ -0.20: buy SPXU (3x SPY short) at 11:30 ET, sell at 15:55 ET.',
      'Effective 3x exposure with realistic 3 bps RT cost on leveraged ETFs.',
    ],
    costBpsRoundTrip: 3,
    entryMinuteEt: 690,
    exitMinuteEt: 955,
    biasLong: 0.20,
    biasShort: -0.20,
    leverage: 3.0,
    overnightExtremeBias: null,
    longTicker: 'SPXL',
    shortTicker: 'SPXU',
    overnightLongTicker: null,
    overnightShortTicker: null,
  },
  {
    id: 'pym-gated-intraday-overnight-1x',
    name: 'PYM-Gated Intraday + Overnight 1x (SPY)',
    displayName: 'PYM-Gated Intraday + Overnight — 1x SPY',
    description: 'PYM bias gates SPY trades; when |bias| ≥ 0.40 enter at prior-day 15:55 and hold overnight through next session close. Otherwise intraday-only 11:30 → 15:55.',
    ruleSummary: [
      'PYM v5 bias direction as in baseline.',
      'If |bias| ≥ 0.40 (extreme conviction): enter SPY (or SH) at PRIOR day 15:55, exit 15:55 next session — captures the overnight gap.',
      'Else if 0.20 ≤ |bias| < 0.40: intraday only (11:30 → 15:55) as in baseline.',
      'Otherwise: flat.',
    ],
    costBpsRoundTrip: 2,
    entryMinuteEt: 690,
    exitMinuteEt: 955,
    biasLong: 0.20,
    biasShort: -0.20,
    leverage: 1.0,
    overnightExtremeBias: 0.40,
    longTicker: 'SPY',
    shortTicker: 'SH',
    overnightLongTicker: 'SPY',
    overnightShortTicker: 'SH',
  },
  {
    id: 'pym-gated-intraday-best-combo',
    name: 'PYM-Gated Intraday + Overnight 3x (Production)',
    displayName: 'PYM-Gated Intraday + Overnight 3x — Production',
    description: 'Production strategy combining 3x leverage (TQQQ/SQQQ overnight, SPXL/SPXU intraday 11:30 → 15:55) with overnight-on-extreme — matches PYM\'s 16-month risk-adjusted return.',
    ruleSummary: [
      'PYM v5 bias direction as in baseline.',
      'If |bias| ≥ 0.30 (extreme conviction): enter TQQQ (long) or SQQQ (short) at PRIOR day 15:55, exit at 15:55 next session — overnight + intraday with 3x leverage.',
      'Else if 0.20 ≤ |bias| < 0.30: intraday only, 11:30 → 15:55, via SPXL/SPXU (3x SPY).',
      'Otherwise: flat (BIL).',
      'Total exposure: 3x intraday, with overnight on the strongest signals.',
    ],
    costBpsRoundTrip: 3,
    entryMinuteEt: 690,
    exitMinuteEt: 955,
    biasLong: 0.20,
    biasShort: -0.20,
    leverage: 3.0,
    overnightExtremeBias: 0.30,
    longTicker: 'SPXL',
    shortTicker: 'SPXU',
    overnightLongTicker: 'TQQQ',
    overnightShortTicker: 'SQQQ',
  },
];

function listTradingDays(pymByDate, startDate, endDate) {
  const out = [];
  const cur = new Date(`${startDate}T00:00:00.000Z`);
  const stop = new Date(`${endDate}T00:00:00.000Z`);
  while (cur <= stop) {
    const d = cur.toISOString().slice(0, 10);
    if (!isWeekend(d) && pymByDate.has(d)) out.push(d);
    cur.setUTCDate(cur.getUTCDate() + 1);
  }
  return out;
}

async function runVariantForRange({ variant, pymByDate, days }) {
  // Run the strategy day-by-day. Return:
  //   trades: per-trade detail with entry/exit
  //   dailyEquity: cumulative equity time series
  //   snapshots: per-day snapshot for the strategy-service contract
  const tradesByDay = new Map();
  const featuresCache = new Map();
  async function getFeatures(day) {
    if (!featuresCache.has(day)) featuresCache.set(day, await loadFeatures(day));
    return featuresCache.get(day);
  }
  for (let i = 0; i < days.length; i += 1) {
    const day = days[i];
    const pe = pymByDate.get(day);
    if (!pe || !Number.isFinite(pe.bias)) continue;
    const signal = pe.bias;
    let side = null;
    if (signal >= variant.biasLong) side = 'LONG';
    else if (signal <= variant.biasShort) side = 'SHORT';
    if (!side) continue;
    const isExtreme = variant.overnightExtremeBias != null && Math.abs(signal) >= variant.overnightExtremeBias;
    // eslint-disable-next-line no-await-in-loop
    const todayRows = await getFeatures(day);
    if (!todayRows || todayRows.length === 0) continue;
    const exitRow = todayRows.find((r) => r.minute_of_day_et === variant.exitMinuteEt) || todayRows[todayRows.length - 1];
    if (!Number.isFinite(exitRow.spy_close)) continue;
    let entryRow = null; let entryDay = day; let entryMode = 'intraday'; let ticker = side === 'LONG' ? variant.longTicker : variant.shortTicker;
    if (isExtreme && variant.overnightLongTicker) {
      const prev = days[i - 1];
      if (!prev) continue;
      // eslint-disable-next-line no-await-in-loop
      const prevRows = await getFeatures(prev);
      if (!prevRows || prevRows.length === 0) continue;
      const prevExit = prevRows.find((r) => r.minute_of_day_et === variant.exitMinuteEt) || prevRows[prevRows.length - 1];
      if (!Number.isFinite(prevExit.spy_close)) continue;
      entryRow = prevExit; entryDay = prev; entryMode = 'overnight';
      ticker = side === 'LONG' ? variant.overnightLongTicker : variant.overnightShortTicker;
    } else {
      entryRow = todayRows.find((r) => r.minute_of_day_et === variant.entryMinuteEt);
      if (!entryRow || !Number.isFinite(entryRow.spy_open)) continue;
    }
    const entryPrice = entryMode === 'overnight' ? entryRow.spy_close : entryRow.spy_open;
    const exitPrice = exitRow.spy_close;
    const sign = side === 'LONG' ? +1 : -1;
    const gross = sign * variant.leverage * (exitPrice / entryPrice - 1);
    const cost = variant.costBpsRoundTrip / 10_000;
    const net = gross - cost;
    tradesByDay.set(day, {
      date: day,
      entryDay,
      entryMode,
      entryMinuteEt: entryMode === 'overnight' ? variant.exitMinuteEt : variant.entryMinuteEt,
      exitMinuteEt: variant.exitMinuteEt,
      ticker,
      side,
      leverage: variant.leverage,
      bias: pe.bias,
      entryPrice,
      exitPrice,
      grossReturn: gross,
      cost,
      netReturn: net,
      isWin: net > 0,
      isExtreme,
    });
  }
  return tradesByDay;
}

function maxDrawdown(equityPoints) {
  let peak = equityPoints[0]?.equity || INITIAL_CAPITAL;
  let dd = 0;
  for (const p of equityPoints) {
    peak = Math.max(peak, p.equity);
    if (peak > 0) dd = Math.min(dd, p.equity / peak - 1);
  }
  return dd;
}

function annualizedSharpe(dailyReturns) {
  if (!dailyReturns.length) return 0;
  const m = dailyReturns.reduce((a, x) => a + x, 0) / dailyReturns.length;
  const variance = dailyReturns.reduce((a, x) => a + ((x - m) ** 2), 0) / dailyReturns.length;
  const sd = Math.sqrt(variance);
  if (sd === 0) return 0;
  return (m / sd) * Math.sqrt(252);
}

function buildReportForVariant({ variant, tradesByDay, days, spyByDay, qqqByDay, pymByDate, sourceMeta }) {
  // For each calendar trading day in range, produce a snapshot. Use trade if exists, otherwise cash.
  const equitySeries = [];
  const snapshots = [];
  let equity = INITIAL_CAPITAL;
  let spyEquity = INITIAL_CAPITAL;
  let qqqEquity = INITIAL_CAPITAL;
  const dailyReturns = [];
  let priorSpyClose = null; let priorQqqClose = null;
  let prevHoldings = [];
  for (let i = 0; i < days.length; i += 1) {
    const day = days[i];
    const t = tradesByDay.get(day);
    const todaySpy = spyByDay.get(day);
    const todayQqq = qqqByDay.get(day);
    const dailyReturn = t ? t.netReturn : 0;
    equity = equity * (1 + dailyReturn);
    dailyReturns.push(dailyReturn);
    const spyReturn = todaySpy && priorSpyClose ? (todaySpy / priorSpyClose) - 1 : 0;
    const qqqReturn = todayQqq && priorQqqClose ? (todayQqq / priorQqqClose) - 1 : 0;
    spyEquity *= (1 + spyReturn);
    qqqEquity *= (1 + qqqReturn);
    priorSpyClose = todaySpy ?? priorSpyClose;
    priorQqqClose = todayQqq ?? priorQqqClose;
    const holdings = t
      ? [{
        ticker: t.ticker,
        weight: t.side === 'LONG' ? +variant.leverage : -variant.leverage,
        weightPct: (t.side === 'LONG' ? +variant.leverage : -variant.leverage) * 100,
        dollars: equity * variant.leverage * (t.side === 'LONG' ? 1 : -1),
      }]
      : [{ ticker: 'CASH', weight: 1, weightPct: 100, dollars: equity }];
    // Turnover: change of weights vs previous
    const prevByTicker = new Map(prevHoldings.map((h) => [h.ticker, h.weight]));
    const newByTicker = new Map(holdings.map((h) => [h.ticker, h.weight]));
    const allTickers = new Set([...prevByTicker.keys(), ...newByTicker.keys()]);
    let turnover = 0;
    for (const tk of allTickers) {
      turnover += Math.abs((newByTicker.get(tk) || 0) - (prevByTicker.get(tk) || 0));
    }
    snapshots.push({
      date: day,
      signalDate: day, // PYM bias was set by prior EOD; we mark signalDate=today since the trade happens today
      rebalanceDate: day,
      execution: t ? (t.entryMode === 'overnight' ? 'pym_gated_overnight' : 'pym_gated_intraday') : 'flat',
      nextDate: days[i + 1] || null,
      equityBeforeNextSession: equity,
      grossExposure: holdings.reduce((s, h) => s + Math.abs(h.weight), 0),
      turnover,
      turnoverPct: turnover * 100,
      estimatedRebalanceCost: t ? t.cost : 0,
      estimatedRebalanceCostPct: (t ? t.cost : 0) * 100,
      holdings,
      topHoldings: holdings.slice(0, 3).map((h) => h.ticker).join(', '),
      benchmarkReturns: { spy: spyReturn, qqq: qqqReturn },
      realized: {
        date: day,
        startEquity: equity / (1 + dailyReturn),
        endEquity: equity,
        grossReturn: t ? t.grossReturn : 0,
        grossReturnPct: (t ? t.grossReturn : 0) * 100,
        netReturn: dailyReturn,
        netReturnPct: dailyReturn * 100,
        costReturn: t ? -t.cost : 0,
        costReturnPct: (t ? -t.cost : 0) * 100,
      },
      trade: t || null,
      pymBias: pymByDate.get(day)?.bias ?? null,
    });
    equitySeries.push({
      date: day,
      signalDate: day,
      equity,
      dailyReturn,
      totalReturn: equity / INITIAL_CAPITAL - 1,
      spyReturn: spyEquity / INITIAL_CAPITAL - 1,
      qqqReturn: qqqEquity / INITIAL_CAPITAL - 1,
    });
    prevHoldings = holdings;
  }

  const trades = Array.from(tradesByDay.values()).sort((a, b) => a.date.localeCompare(b.date));
  const tradeNetReturns = trades.map((t) => t.netReturn);
  const tradeMean = tradeNetReturns.length ? tradeNetReturns.reduce((a, x) => a + x, 0) / tradeNetReturns.length : 0;
  const tradeSd = tradeNetReturns.length > 1
    ? Math.sqrt(tradeNetReturns.reduce((a, x) => a + ((x - tradeMean) ** 2), 0) / tradeNetReturns.length)
    : 0;
  const summary = {
    startDate: days[0] || null,
    endDate: days[days.length - 1] || null,
    initialCapital: INITIAL_CAPITAL,
    finalEquity: equity,
    totalReturn: equity / INITIAL_CAPITAL - 1,
    totalReturnPct: (equity / INITIAL_CAPITAL - 1) * 100,
    cagr: null,
    maxDrawdown: maxDrawdown(equitySeries),
    maxDrawdownPct: maxDrawdown(equitySeries) * 100,
    annualizedVolatility: Math.sqrt(252) * Math.sqrt(dailyReturns.reduce((a, x) => a + (x*x), 0) / Math.max(1, dailyReturns.length)),
    sharpe: annualizedSharpe(dailyReturns),
    sharpePerTrade: tradeSd > 0 ? (tradeMean / tradeSd) * Math.sqrt(252) : 0,
    tradingDays: days.length,
    activeDays: trades.length,
    tradeCount: trades.length,
    longCount: trades.filter((t) => t.side === 'LONG').length,
    shortCount: trades.filter((t) => t.side === 'SHORT').length,
    overnightCount: trades.filter((t) => t.entryMode === 'overnight').length,
    intradayCount: trades.filter((t) => t.entryMode === 'intraday').length,
    winCount: trades.filter((t) => t.isWin).length,
    hitRate: trades.length ? trades.filter((t) => t.isWin).length / trades.length : 0,
    hitRatePct: trades.length ? (trades.filter((t) => t.isWin).length / trades.length) * 100 : 0,
    avgNetReturnBps: trades.length ? (trades.reduce((a, t) => a + t.netReturn, 0) / trades.length) * 10_000 : 0,
    spyReturn: spyEquity / INITIAL_CAPITAL - 1,
    qqqReturn: qqqEquity / INITIAL_CAPITAL - 1,
    todayReturn: equitySeries.at(-1)?.dailyReturn ?? 0,
    todayReturnPct: (equitySeries.at(-1)?.dailyReturn ?? 0) * 100,
    todayDate: equitySeries.at(-1)?.date ?? null,
  };

  return {
    generatedAt: new Date().toISOString(),
    source: {
      provider: 'Massive stock_quotes_1m + OPRA flow + OCC OI overlay',
      pymArtifactPath: sourceMeta.pymArtifactPath,
      featuresDir: sourceMeta.featuresDir,
      generatedAt: sourceMeta.pymGeneratedAt,
    },
    settings: {
      variant: variant.id,
      biasLong: variant.biasLong,
      biasShort: variant.biasShort,
      leverage: variant.leverage,
      entryMinuteEt: variant.entryMinuteEt,
      exitMinuteEt: variant.exitMinuteEt,
      overnightExtremeBias: variant.overnightExtremeBias,
      costBpsRoundTrip: variant.costBpsRoundTrip,
      longTicker: variant.longTicker,
      shortTicker: variant.shortTicker,
      overnightLongTicker: variant.overnightLongTicker,
      overnightShortTicker: variant.overnightShortTicker,
    },
    summary,
    latest: snapshots.at(-1) || null,
    snapshots,
    equitySeries,
    trades,
    skippedDays: [],
  };
}

async function loadBenchmarkCloses(days, symbol) {
  // Extract daily close from per-minute features file for SPY (using exit minute)
  // For QQQ we need a separate run; for simplicity we use SPY features file only and approximate QQQ
  // by reading QQQ features if present.
  const closeByDay = new Map();
  for (const day of days) {
    // eslint-disable-next-line no-await-in-loop
    const p = defaultFeaturesPath(PROJECT_ROOT, symbol, day);
    if (!fs.existsSync(p)) continue;
    // eslint-disable-next-line no-await-in-loop
    const stream = fs.createReadStream(p).pipe(zlib.createGunzip());
    const rl = readline.createInterface({ input: stream, crlfDelay: Infinity });
    let lastClose = null;
    // eslint-disable-next-line no-await-in-loop, no-restricted-syntax
    for await (const line of rl) {
      if (!line) continue;
      const r = JSON.parse(line);
      if (Number.isFinite(r.spy_close)) lastClose = r.spy_close;
    }
    if (lastClose != null) closeByDay.set(day, lastClose);
  }
  return closeByDay;
}

function parseArgs(argv) {
  const o = {};
  for (let i = 0; i < argv.length; i += 1) {
    const a = argv[i];
    if (a === '--start') o.start = argv[++i];
    else if (a === '--end') o.end = argv[++i];
    else if (a === '--pym-artifact') o.pymArtifact = argv[++i];
    else if (a === '--variant') o.variantId = argv[++i];
  }
  return o;
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const pymArtifactPath = args.pymArtifact || findLatestPymArtifact();
  if (!pymArtifactPath) {
    process.stderr.write('No PYM artifact found. Run pym-v5:backtest first.\n');
    process.exit(1);
  }
  process.stdout.write(`Using PYM artifact: ${pymArtifactPath}\n`);
  const pymObj = JSON.parse(fs.readFileSync(pymArtifactPath, 'utf8'));
  const pymByDate = loadPymHoldings(pymArtifactPath);
  process.stdout.write(`Loaded ${pymByDate.size} PYM trading days\n`);

  // Date range: from PYM start to either --end or the latest PYM day (or whichever is earlier)
  const allPymDays = Array.from(pymByDate.keys()).sort();
  const startDate = args.start || allPymDays[0];
  const endDate = args.end || allPymDays[allPymDays.length - 1];
  const days = listTradingDays(pymByDate, startDate, endDate);
  process.stdout.write(`Backtest window: ${startDate} → ${endDate} (${days.length} trading days)\n`);

  // Benchmarks
  process.stdout.write('Loading SPY benchmark closes...\n');
  const spyByDay = await loadBenchmarkCloses(days, 'SPY');
  process.stdout.write('Loading QQQ benchmark closes (best-effort)...\n');
  const qqqByDay = await loadBenchmarkCloses(days, 'QQQ');

  const sourceMeta = {
    pymArtifactPath,
    featuresDir: path.relative(PROJECT_ROOT, defaultFeaturesPath(PROJECT_ROOT, 'SPY', '').replace(/\/date=\/?\.jsonl\.gz$/, '')),
    pymGeneratedAt: pymObj.generatedAt || null,
  };

  fs.mkdirSync(ARTIFACTS_OUT_DIR, { recursive: true });

  const variantsToRun = args.variantId
    ? VARIANTS.filter((v) => v.id === args.variantId)
    : VARIANTS;

  for (const variant of variantsToRun) {
    process.stdout.write(`\n=== ${variant.id} ===\n`);
    process.stdout.write(`  ${variant.description}\n`);
    // eslint-disable-next-line no-await-in-loop
    const tradesByDay = await runVariantForRange({ variant, pymByDate, days });
    const report = buildReportForVariant({
      variant, tradesByDay, days, spyByDay, qqqByDay, pymByDate, sourceMeta,
    });
    report.metadata = {
      id: variant.id,
      name: variant.name,
      displayName: variant.displayName,
      description: variant.description,
      ruleSummary: variant.ruleSummary,
    };
    const outPath = path.join(ARTIFACTS_OUT_DIR, `${variant.id}-report.json`);
    fs.writeFileSync(outPath, JSON.stringify(report, null, 2));
    process.stdout.write(`  trades=${report.summary.tradeCount} net=${report.summary.totalReturnPct.toFixed(2)}% Sharpe(day)=${report.summary.sharpe.toFixed(2)} maxDD=${report.summary.maxDrawdownPct.toFixed(2)}% hit=${report.summary.hitRatePct.toFixed(1)}% → ${outPath}\n`);
  }

  process.stdout.write('\nDone.\n');
}

main().catch((err) => { process.stderr.write(`${err.stack || err.message}\n`); process.exit(1); });
