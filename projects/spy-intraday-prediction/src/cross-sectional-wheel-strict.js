const fs = require('node:fs');
const path = require('node:path');
const readline = require('node:readline');
const zlib = require('node:zlib');

const { openCalendarDays, listDatasetDates } = require('./coverage');

const DEFAULT_INITIAL_CAPITAL = 100_000;
const TRADING_DAYS_PER_YEAR = 252;
const TARGET_HISTORY_TRADING_DAYS = 5 * TRADING_DAYS_PER_YEAR;
const MIN_HEADLINE_TRADING_DAYS = TRADING_DAYS_PER_YEAR;
const MIN_SP500_CONSTITUENTS = 450;

const STRICT_BASE_STRATEGY = Object.freeze({
  id: 'cs_wheel_base_rsi14_21td_095p_095c',
  label: 'Cross-sectional RSI-gated S&P 500 wheel baseline',
  initialCapital: DEFAULT_INITIAL_CAPITAL,
  rsiPeriod: 14,
  rsiSmoothing: 'wilder',
  putRsiMax: 30,
  callRsiMin: 70,
  putMoneyness: 0.95,
  callMoneyness: 0.95,
  expiryTradingDays: 21,
  maxCommittedPositions: 5,
  maxPositionPct: 0.30,
  contractMultiplier: 100,
  entryScan: 'daily_close',
  assignment: 'expiration_only',
});

const STRICT_VIX_OVERLAY_STRATEGY = Object.freeze({
  ...STRICT_BASE_STRATEGY,
  id: 'cs_wheel_vix_overlay_rsi14_21td',
  label: 'Cross-sectional RSI-gated wheel with prior-day VIX overlay',
  putMoneyness: 0.95,
  vixOverlay: {
    threshold: 22,
    highVixPutMoneyness: 1.05,
    calmPutMoneyness: 0.95,
    vixObservation: 'prior_trading_day_close',
  },
});

const STRICT_EXPERIMENT_KNOBS = Object.freeze({
  putMoneyness: { base: 0.95, values: [0.90, 0.95, 1.00, 1.05] },
  callMoneyness: { base: 0.95, values: [0.90, 0.95, 1.00, 1.05] },
  putRsiMax: { base: 30, values: [20, 25, 30, 35, 40] },
  callRsiMin: { base: 70, values: [60, 65, 70, 75, 80] },
  expiryTradingDays: { base: 21, values: [10, 15, 21, 30, 45] },
  maxCommittedPositions: { base: 5, values: [3, 5, 8, 10] },
  maxPositionPct: { base: 0.30, values: [0.10, 0.20, 0.30] },
  universe: { base: 'point_in_time_sp500', values: ['point_in_time_sp500'] },
  vixOverlay: {
    base: null,
    values: [
      null,
      { threshold: 22, calmPutMoneyness: 0.95, highVixPutMoneyness: 1.05 },
    ],
  },
});

function round(value, digits = 6) {
  return Number.isFinite(value) ? Number(value.toFixed(digits)) : value;
}

function parseCsvLine(line) {
  const values = [];
  let current = '';
  let quoted = false;
  const raw = String(line || '');
  for (let index = 0; index < raw.length; index += 1) {
    const char = raw[index];
    const next = raw[index + 1];
    if (char === '"' && quoted && next === '"') {
      current += '"';
      index += 1;
      continue;
    }
    if (char === '"') {
      quoted = !quoted;
      continue;
    }
    if (char === ',' && !quoted) {
      values.push(current.trim());
      current = '';
      continue;
    }
    current += char;
  }
  values.push(current.trim());
  return values;
}

function normalizeSymbol(symbol) {
  return String(symbol || '').trim().toUpperCase();
}

function uniqueSorted(values) {
  return [...new Set(values.filter(Boolean))].sort();
}

function existingPathOrNull(filePath) {
  if (!filePath) return null;
  const resolved = path.resolve(String(filePath));
  return fs.existsSync(resolved) ? resolved : null;
}

function addIssue(issues, code, message, detail = {}) {
  issues.push({ code, message, ...detail });
}

function datasetDatesAcrossRoots(config, datasetKey) {
  const datasetId = config.datasets?.[datasetKey] || datasetKey;
  const historicalDates = listDatasetDates(config.roots.historical, datasetId);
  const liveDates = config.roots.liveParquet
    ? listDatasetDates(config.roots.liveParquet, datasetId)
    : [];
  return uniqueSorted([...historicalDates, ...liveDates]);
}

function commonDateRange(config, datasetKeys) {
  const dateSets = datasetKeys.map((key) => new Set(datasetDatesAcrossRoots(config, key)));
  if (!dateSets.length || dateSets.some((set) => set.size === 0)) {
    return { startDate: null, endDate: null, dates: [] };
  }
  const common = [...dateSets[0]].filter((day) => dateSets.every((set) => set.has(day))).sort();
  return {
    startDate: common[0] || null,
    endDate: common[common.length - 1] || null,
    dates: common,
  };
}

function resolveStrictBacktestWindow(config, requested = {}) {
  const common = commonDateRange(config, ['stockBars', 'optionBars']);
  const endDate = requested.endDate || common.endDate || config.dataPolicy?.historicalCutoffDate;
  const startDate = requested.startDate || common.startDate || config.windows?.sensitivityTrain?.startDate;
  if (!startDate || !endDate) {
    throw new Error('strict_wheel_window_unavailable');
  }
  return { startDate, endDate, commonDataStartDate: common.startDate, commonDataEndDate: common.endDate };
}

async function readFirstLine(filePath) {
  return new Promise((resolve, reject) => {
    const input = fs.createReadStream(filePath);
    const stream = filePath.endsWith('.gz') ? input.pipe(zlib.createGunzip()) : input;
    const reader = readline.createInterface({ input: stream, crlfDelay: Infinity });
    let settled = false;

    reader.on('line', (line) => {
      if (settled) return;
      settled = true;
      reader.close();
      input.destroy();
      resolve(line);
    });
    reader.on('close', () => {
      if (!settled) {
        settled = true;
        resolve('');
      }
    });
    reader.on('error', reject);
    input.on('error', reject);
    stream.on('error', reject);
  });
}

async function readDelimitedHeader(filePath) {
  const firstLine = await readFirstLine(filePath);
  return parseCsvLine(firstLine).map((header) => header.toLowerCase());
}

function hasAny(headers, candidates) {
  return candidates.some((candidate) => headers.includes(candidate));
}

function inspectOptionBidAskHeader(headers) {
  return {
    hasBid: hasAny(headers, ['bid', 'bid_price', 'best_bid', 'nbbo_bid', 'close_bid']),
    hasAsk: hasAny(headers, ['ask', 'ask_price', 'best_ask', 'nbbo_ask', 'close_ask']),
    hasOptionIdentifier: hasAny(headers, ['ticker', 'option_ticker', 'opra', 'option_symbol'])
      || (hasAny(headers, ['root', 'symbol', 'underlying']) && hasAny(headers, ['expiration', 'expiry']) && hasAny(headers, ['strike'])),
  };
}

function inspectIvSurfaceHeader(headers) {
  return {
    hasIv: hasAny(headers, ['iv', 'implied_vol', 'implied_volatility', 'sigma']),
    hasOptionIdentifier: hasAny(headers, ['ticker', 'option_ticker', 'opra', 'option_symbol'])
      || (hasAny(headers, ['root', 'symbol', 'underlying']) && hasAny(headers, ['expiration', 'expiry']) && hasAny(headers, ['strike'])),
    hasDate: hasAny(headers, ['date', 'as_of', 'asof', 'quote_date']),
  };
}

function rowsFromJsonMembership(raw) {
  if (Array.isArray(raw)) return raw;
  if (Array.isArray(raw.days)) return raw.days;
  if (Array.isArray(raw.snapshots)) return raw.snapshots;
  if (Array.isArray(raw.memberships)) return raw.memberships;
  if (raw.byDate && typeof raw.byDate === 'object') {
    return Object.entries(raw.byDate).map(([date, symbols]) => ({ date, symbols }));
  }
  return Object.entries(raw)
    .filter(([, value]) => Array.isArray(value))
    .map(([date, symbols]) => ({ date, symbols }));
}

function normalizeMembershipRows(rows) {
  const snapshotByDate = new Map();
  const intervals = [];

  rows.forEach((row) => {
    const date = row.date || row.asOf || row.as_of || row.effectiveDate || row.effective_date;
    const rawSymbols = row.symbols || row.constituents || row.members || row.tickers;
    const symbols = typeof rawSymbols === 'string'
      ? rawSymbols.split(',').map(normalizeSymbol).filter(Boolean)
      : rawSymbols;
    if (date && Array.isArray(symbols)) {
      snapshotByDate.set(String(date).slice(0, 10), uniqueSorted(symbols.map(normalizeSymbol)));
      return;
    }

    const symbol = normalizeSymbol(row.symbol || row.ticker || row.constituent || row.member);
    const startDate = row.startDate || row.start_date || row.from || row.addedDate || row.added_date;
    const endDate = row.endDate || row.end_date || row.to || row.removedDate || row.removed_date || null;
    if (symbol && startDate) {
      intervals.push({
        symbol,
        startDate: String(startDate).slice(0, 10),
        endDate: endDate ? String(endDate).slice(0, 10) : null,
      });
      return;
    }

    if (date && symbol) {
      const key = String(date).slice(0, 10);
      const list = snapshotByDate.get(key) || [];
      list.push(symbol);
      snapshotByDate.set(key, uniqueSorted(list));
    }
  });

  return {
    kind: intervals.length ? 'interval' : 'snapshot',
    snapshotByDate,
    intervals,
  };
}

function loadCsvMembership(filePath) {
  const lines = fs.readFileSync(filePath, 'utf8').split(/\r?\n/).filter(Boolean);
  if (!lines.length) return normalizeMembershipRows([]);
  const headers = parseCsvLine(lines[0]).map((header) => header.trim());
  const rows = lines.slice(1).map((line) => {
    const values = parseCsvLine(line);
    return Object.fromEntries(headers.map((header, index) => [header, values[index] || '']));
  });
  return normalizeMembershipRows(rows);
}

function loadPointInTimeMembership(filePath) {
  const resolved = existingPathOrNull(filePath);
  if (!resolved) throw new Error(`missing_point_in_time_sp500_membership:${filePath || ''}`);
  if (resolved.endsWith('.json')) {
    const raw = JSON.parse(fs.readFileSync(resolved, 'utf8'));
    return normalizeMembershipRows(rowsFromJsonMembership(raw));
  }
  if (resolved.endsWith('.csv')) return loadCsvMembership(resolved);
  throw new Error(`unsupported_point_in_time_membership_format:${resolved}`);
}

function constituentsForDate(membership, dayIso) {
  if (membership.intervals.length) {
    return membership.intervals
      .filter((row) => row.startDate <= dayIso && (!row.endDate || row.endDate >= dayIso))
      .map((row) => row.symbol)
      .sort();
  }

  const snapshotDates = [...membership.snapshotByDate.keys()].filter((date) => date <= dayIso).sort();
  const latestDate = snapshotDates.at(-1);
  return latestDate ? membership.snapshotByDate.get(latestDate) || [] : [];
}

function summarizeMembership(membership, openDays) {
  const counts = openDays.map((dayIso) => ({
    date: dayIso,
    count: constituentsForDate(membership, dayIso).length,
  }));
  const uniqueSymbols = uniqueSorted(openDays.flatMap((dayIso) => constituentsForDate(membership, dayIso)));
  const countValues = counts.map((row) => row.count).filter(Number.isFinite);
  return {
    kind: membership.kind,
    snapshotDates: [...membership.snapshotByDate.keys()].sort(),
    intervalCount: membership.intervals.length,
    uniqueSymbolCount: uniqueSymbols.length,
    minConstituentCount: countValues.length ? Math.min(...countValues) : 0,
    maxConstituentCount: countValues.length ? Math.max(...countValues) : 0,
    missingDates: counts.filter((row) => row.count === 0).map((row) => row.date),
    thinDates: counts.filter((row) => row.count > 0 && row.count < MIN_SP500_CONSTITUENTS).map((row) => row.date),
  };
}

async function validateOptionPricingSource({
  optionBidAskPath,
  ivSurfacePath,
  modeledSpreadBps,
  modeledSlippageBps,
  slippageBps,
  commissionPerContract,
  errors,
  warnings,
}) {
  const bidAsk = existingPathOrNull(optionBidAskPath);
  const ivSurface = existingPathOrNull(ivSurfacePath);
  const effectiveSlippageBps = Number.isFinite(slippageBps) ? slippageBps : modeledSlippageBps;

  if (bidAsk) {
    const stat = fs.statSync(bidAsk);
    if (stat.isFile()) {
      const headers = await readDelimitedHeader(bidAsk);
      const inspection = inspectOptionBidAskHeader(headers);
      if (!inspection.hasBid || !inspection.hasAsk || !inspection.hasOptionIdentifier) {
        addIssue(errors, 'invalid_option_bid_ask_source', 'Option source must include option identity plus bid and ask columns.', {
          path: bidAsk,
          headers,
          inspection,
        });
      }
    } else {
      addIssue(warnings, 'option_bid_ask_directory_not_sampled', 'Option bid/ask source is a directory; header validation will happen per daily file in the full runner.', {
        path: bidAsk,
      });
    }
    if (!(effectiveSlippageBps > 0)) {
      addIssue(errors, 'missing_slippage_cost', 'Executable bid/ask pricing must still subtract explicit slippage.');
    }
    if (!(commissionPerContract > 0)) {
      addIssue(errors, 'missing_commission_cost', 'Executable bid/ask pricing must include commission per contract.');
    }
    return { mode: 'bid_ask', path: bidAsk };
  }

  if (ivSurface) {
    const stat = fs.statSync(ivSurface);
    if (stat.isFile()) {
      const headers = await readDelimitedHeader(ivSurface);
      const inspection = inspectIvSurfaceHeader(headers);
      if (!inspection.hasIv || !inspection.hasOptionIdentifier || !inspection.hasDate) {
        addIssue(errors, 'invalid_iv_surface_source', 'Modeled premium source must include date, option identity, and per-name implied volatility columns.', {
          path: ivSurface,
          headers,
          inspection,
        });
      }
    } else {
      addIssue(warnings, 'iv_surface_directory_not_sampled', 'IV surface source is a directory; header validation will happen per daily file in the full runner.', {
        path: ivSurface,
      });
    }

    if (!(modeledSpreadBps > 0)) {
      addIssue(errors, 'missing_modeled_spread_cost', 'Modeled IV-surface pricing must still subtract an explicit bid/ask spread cost.');
    }
    if (!(effectiveSlippageBps > 0)) {
      addIssue(errors, 'missing_modeled_slippage_cost', 'Modeled IV-surface pricing must still subtract explicit slippage.');
    }
    if (!(commissionPerContract > 0)) {
      addIssue(errors, 'missing_commission_cost', 'Modeled IV-surface pricing must include commission per contract.');
    }
    return { mode: 'iv_surface', path: ivSurface };
  }

  addIssue(errors, 'missing_executable_option_pricing_source', 'No executable option pricing source was supplied. Massive option_quotes_1m aggregate bars are not bid/ask quotes and are not accepted for the strict baseline.');
  return { mode: 'missing', path: null };
}

function validateRequiredFile({ filePath, code, label, errors }) {
  const resolved = existingPathOrNull(filePath);
  if (!resolved) {
    addIssue(errors, code, `${label} is required for the strict baseline.`, {
      path: filePath || null,
    });
    return null;
  }
  return resolved;
}

async function preflightStrictWheelBacktest({
  config,
  startDate,
  endDate,
  membershipPath,
  optionBidAskPath = null,
  ivSurfacePath = null,
  dividendsPath = null,
  riskFreePath = null,
  modeledSpreadBps = null,
  modeledSlippageBps = null,
  slippageBps = null,
  commissionPerContract = null,
  minHeadlineTradingDays = MIN_HEADLINE_TRADING_DAYS,
  targetHistoryTradingDays = TARGET_HISTORY_TRADING_DAYS,
  minConstituents = MIN_SP500_CONSTITUENTS,
}) {
  const errors = [];
  const warnings = [];
  const resolvedWindow = resolveStrictBacktestWindow(config, { startDate, endDate });
  const openDays = openCalendarDays(config.roots.calendar, resolvedWindow.startDate, resolvedWindow.endDate);
  const stockDates = datasetDatesAcrossRoots(config, 'stockBars');
  const optionAggregateDates = datasetDatesAcrossRoots(config, 'optionBars');
  const indexDates = datasetDatesAcrossRoots(config, 'indexBars');

  if (!openDays.length) {
    addIssue(errors, 'empty_open_day_window', 'No open exchange days were found for the requested window.', resolvedWindow);
  }
  if (openDays.length < minHeadlineTradingDays) {
    addIssue(errors, 'window_too_short_for_headline', 'The strict baseline will not headline a sub-1-year backtest window.', {
      openDayCount: openDays.length,
      requiredOpenDays: minHeadlineTradingDays,
    });
  } else if (openDays.length < targetHistoryTradingDays) {
    addIssue(warnings, 'history_short_of_5y_target', 'Available/requested history is shorter than the target 5 years; report it as limited history, not a durable edge.', {
      openDayCount: openDays.length,
      targetOpenDays: targetHistoryTradingDays,
    });
  }

  const missingStockDays = openDays.filter((dayIso) => !stockDates.includes(dayIso));
  if (missingStockDays.length) {
    addIssue(errors, 'missing_massive_stock_days', 'Strict daily-close scanning needs Massive stock bars for every open day.', {
      missingCount: missingStockDays.length,
      sample: missingStockDays.slice(0, 10),
    });
  }

  const missingOptionAggregateDays = openDays.filter((dayIso) => !optionAggregateDates.includes(dayIso));
  if (missingOptionAggregateDays.length) {
    addIssue(warnings, 'missing_local_option_aggregate_days', 'Local aggregate option bars are not a strict fill source, but gaps limit non-compliant diagnostics.', {
      missingCount: missingOptionAggregateDays.length,
      sample: missingOptionAggregateDays.slice(0, 10),
    });
  }

  const missingIndexDays = openDays.filter((dayIso) => !indexDates.includes(dayIso));
  if (missingIndexDays.length) {
    addIssue(errors, 'missing_vix_index_days', 'The VIX overlay requires prior-day VIX close from Massive indices_1m for every open day after the first.', {
      missingCount: missingIndexDays.length,
      sample: missingIndexDays.slice(0, 10),
    });
  }

  let membershipSummary = null;
  const resolvedMembershipPath = existingPathOrNull(membershipPath);
  if (!resolvedMembershipPath) {
    addIssue(errors, 'missing_point_in_time_sp500_membership', 'No full point-in-time S&P 500 constituent file was found; refusing to fall back to current members or a proxy universe.', {
      path: membershipPath || null,
    });
  } else {
    try {
      const membership = loadPointInTimeMembership(resolvedMembershipPath);
      membershipSummary = summarizeMembership(membership, openDays);
      if (membershipSummary.missingDates.length) {
        addIssue(errors, 'pit_membership_missing_dates', 'Point-in-time membership did not cover every requested open day.', {
          missingCount: membershipSummary.missingDates.length,
          sample: membershipSummary.missingDates.slice(0, 10),
        });
      }
      if (membershipSummary.minConstituentCount < minConstituents) {
        addIssue(errors, 'pit_membership_too_thin', 'Point-in-time S&P 500 membership has too few constituents for at least one open day.', {
          minConstituentCount: membershipSummary.minConstituentCount,
          requiredMinConstituents: minConstituents,
          sample: membershipSummary.thinDates.slice(0, 10),
        });
      }
      if (membershipSummary.uniqueSymbolCount <= membershipSummary.maxConstituentCount) {
        addIssue(warnings, 'pit_membership_no_removed_names_detected', 'The membership file does not appear to include removed/delisted names over the window; confirm it is genuinely point-in-time.', {
          uniqueSymbolCount: membershipSummary.uniqueSymbolCount,
          maxConstituentCount: membershipSummary.maxConstituentCount,
        });
      }
    } catch (error) {
      addIssue(errors, 'pit_membership_parse_failed', error.message);
    }
  }

  const optionPricing = await validateOptionPricingSource({
    optionBidAskPath,
    ivSurfacePath,
    modeledSpreadBps,
    modeledSlippageBps,
    slippageBps,
    commissionPerContract,
    errors,
    warnings,
  });

  const resolvedDividendsPath = validateRequiredFile({
    filePath: dividendsPath,
    code: 'missing_dividend_source',
    label: 'Dividend source for held shares',
    errors,
  });
  const resolvedRiskFreePath = validateRequiredFile({
    filePath: riskFreePath,
    code: 'missing_risk_free_source',
    label: 'Daily risk-free/T-bill rate source for idle cash',
    errors,
  });

  return {
    generatedAt: new Date().toISOString(),
    status: errors.length ? 'FAIL' : 'PASS',
    window: {
      startDate: resolvedWindow.startDate,
      endDate: resolvedWindow.endDate,
      openDayCount: openDays.length,
      commonDataStartDate: resolvedWindow.commonDataStartDate,
      commonDataEndDate: resolvedWindow.commonDataEndDate,
    },
    strictStrategy: {
      base: STRICT_BASE_STRATEGY,
      firstVariant: STRICT_VIX_OVERLAY_STRATEGY,
      experimentKnobs: STRICT_EXPERIMENT_KNOBS,
    },
    dataSources: {
      provider: config.dataPolicy?.provider || 'Massive',
      stockBars: config.datasets?.stockBars || 'stockBars',
      localOptionAggregateBars: config.datasets?.optionBars || 'optionBars',
      indices: config.datasets?.indexBars || 'indexBars',
      membershipPath: resolvedMembershipPath,
      optionPricing,
      dividendsPath: resolvedDividendsPath,
      riskFreePath: resolvedRiskFreePath,
    },
    membership: membershipSummary,
    errors,
    warnings,
  };
}

function pct(value) {
  return Number.isFinite(value) ? `${(value * 100).toFixed(2)}%` : 'n/a';
}

function formatStrictPreflightMarkdown(report) {
  const lines = [];
  lines.push('# Cross-Sectional Options-Income Wheel Strict Preflight');
  lines.push('');
  lines.push(`Generated: ${report.generatedAt}`);
  lines.push(`Status: ${report.status}`);
  lines.push(`Window: ${report.window.startDate} through ${report.window.endDate} (${report.window.openDayCount} open days)`);
  lines.push(`Initial capital: $${report.strictStrategy.base.initialCapital.toLocaleString('en-US')}`);
  lines.push('');
  lines.push('## Base Strategy');
  lines.push('');
  lines.push(`- Universe: full point-in-time S&P 500 membership only.`);
  lines.push(`- Entry: daily close scan; sell ${report.strictStrategy.base.expiryTradingDays}-trading-day puts when Wilder RSI(${report.strictStrategy.base.rsiPeriod}) < ${report.strictStrategy.base.putRsiMax}.`);
  lines.push(`- Put strike: ${pct(report.strictStrategy.base.putMoneyness)} of spot; call strike: ${pct(report.strictStrategy.base.callMoneyness)} of spot.`);
  lines.push(`- Risk: at most ${report.strictStrategy.base.maxCommittedPositions} committed names and ${pct(report.strictStrategy.base.maxPositionPct)} of equity per position.`);
  lines.push(`- First variant: when prior-day VIX > ${report.strictStrategy.firstVariant.vixOverlay.threshold}, sell ITM puts at ${pct(report.strictStrategy.firstVariant.vixOverlay.highVixPutMoneyness)} of spot.`);
  lines.push('');
  lines.push('## Gate Results');
  lines.push('');
  if (report.errors.length) {
    lines.push('### Blocking Errors');
    lines.push('');
    report.errors.forEach((error) => {
      lines.push(`- ${error.code}: ${error.message}`);
    });
    lines.push('');
  }
  if (report.warnings.length) {
    lines.push('### Warnings');
    lines.push('');
    report.warnings.forEach((warning) => {
      lines.push(`- ${warning.code}: ${warning.message}`);
    });
    lines.push('');
  }
  if (!report.errors.length && !report.warnings.length) {
    lines.push('- All strict data gates passed.');
    lines.push('');
  }
  lines.push('## Data Sources');
  lines.push('');
  lines.push(`- Stock bars: ${report.dataSources.stockBars}`);
  lines.push(`- Local option aggregate bars: ${report.dataSources.localOptionAggregateBars} (not accepted as strict bid/ask fills)`);
  lines.push(`- Point-in-time membership: ${report.dataSources.membershipPath || 'missing'}`);
  lines.push(`- Option pricing mode: ${report.dataSources.optionPricing.mode}`);
  lines.push(`- Dividends: ${report.dataSources.dividendsPath || 'missing'}`);
  lines.push(`- Risk-free: ${report.dataSources.riskFreePath || 'missing'}`);
  lines.push('');
  lines.push('## Experiment Knobs');
  lines.push('');
  Object.entries(report.strictStrategy.experimentKnobs).forEach(([key, spec]) => {
    lines.push(`- ${key}: base=${JSON.stringify(spec.base)} values=${JSON.stringify(spec.values)}`);
  });
  lines.push('');
  lines.push('This preflight intentionally fails before any backtest result can be headlined if survivorship-free membership, executable option pricing, dividends, or risk-free inputs are missing.');
  lines.push('');
  return `${lines.join('\n')}\n`;
}

module.exports = {
  DEFAULT_INITIAL_CAPITAL,
  STRICT_BASE_STRATEGY,
  STRICT_VIX_OVERLAY_STRATEGY,
  STRICT_EXPERIMENT_KNOBS,
  resolveStrictBacktestWindow,
  loadPointInTimeMembership,
  constituentsForDate,
  summarizeMembership,
  inspectOptionBidAskHeader,
  inspectIvSurfaceHeader,
  preflightStrictWheelBacktest,
  formatStrictPreflightMarkdown,
  round,
};
