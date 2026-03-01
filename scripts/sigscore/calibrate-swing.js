#!/usr/bin/env node
const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
const Database = require('better-sqlite3');
const { resolveDbPath } = require('../../src/config/env');
const { DEFAULT_SWING_WEIGHTS } = require('../../src/scoring/rule-config');

const HORIZONS = [1, 3, 5];
const HORIZON_WEIGHTS = { 1: 0.5, 3: 0.3, 5: 0.2 };

function toFinite(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function parseArgs(argv) {
  const args = {
    outputPath: process.env.SIGSCORE_SWING_CALIBRATION_OUTPUT
      || path.resolve(process.cwd(), 'artifacts', 'reports', 'sigscore-swing-calibration.json'),
    writeRuleSql: null,
    candidateVersionId: null,
    minRows: toFinite(process.env.SIGSCORE_SWING_MIN_ROWS) || 500,
    minPrecisionProxy: toFinite(process.env.SIGSCORE_SWING_MIN_PRECISION_PROXY) || 0.2,
    maxRows: Math.max(1000, Math.trunc(toFinite(process.env.SIGSCORE_SWING_MAX_ROWS) || 100000)),
  };

  argv.slice(2).forEach((token) => {
    if (token.startsWith('--output=')) {
      args.outputPath = path.resolve(token.slice('--output='.length).trim());
    } else if (token.startsWith('--write-rule-sql=')) {
      args.writeRuleSql = path.resolve(token.slice('--write-rule-sql='.length).trim());
    } else if (token.startsWith('--candidate-version-id=')) {
      args.candidateVersionId = token.slice('--candidate-version-id='.length).trim() || null;
    } else if (token.startsWith('--min-rows=')) {
      args.minRows = toFinite(token.slice('--min-rows='.length).trim()) || args.minRows;
    } else if (token.startsWith('--min-precision-proxy=')) {
      args.minPrecisionProxy = toFinite(token.slice('--min-precision-proxy='.length).trim()) || args.minPrecisionProxy;
    } else if (token.startsWith('--max-rows=')) {
      const parsed = toFinite(token.slice('--max-rows='.length).trim());
      if (parsed !== null) args.maxRows = Math.max(1000, Math.trunc(parsed));
    }
  });

  return args;
}

function normalizeTimestamp(rawValue, fallbackIso) {
  if (rawValue === null || rawValue === undefined || rawValue === '') return fallbackIso;
  if (typeof rawValue === 'number') {
    const millis = rawValue > 1e12 ? rawValue : rawValue * 1000;
    const dt = new Date(millis);
    return Number.isNaN(dt.getTime()) ? fallbackIso : dt.toISOString();
  }

  const raw = String(rawValue).trim();
  const numeric = Number(raw);
  if (Number.isFinite(numeric)) {
    const millis = numeric > 1e12 ? numeric : numeric * 1000;
    const dt = new Date(millis);
    if (!Number.isNaN(dt.getTime())) return dt.toISOString();
  }

  const hasOffset = /[zZ]|[+-]\d\d:\d\d$/.test(raw);
  const dt = new Date(hasOffset ? raw : `${raw}Z`);
  if (!Number.isNaN(dt.getTime())) return dt.toISOString();
  return fallbackIso;
}

function parseJsonRows(rawJson) {
  let parsed;
  try {
    parsed = JSON.parse(rawJson);
  } catch {
    return [];
  }

  if (Array.isArray(parsed)) return parsed.filter((row) => row && typeof row === 'object');
  if (parsed && Array.isArray(parsed.rows)) return parsed.rows.filter((row) => row && typeof row === 'object');
  if (parsed && Array.isArray(parsed.data)) return parsed.data.filter((row) => row && typeof row === 'object');
  if (parsed && Array.isArray(parsed.response) && Array.isArray(parsed.header)) {
    return parsed.response
      .filter((row) => Array.isArray(row))
      .map((values) => {
        const out = {};
        parsed.header.forEach((key, index) => {
          out[key] = values[index];
        });
        return out;
      });
  }

  if (parsed && typeof parsed === 'object') {
    const entries = Object.entries(parsed).filter(([, value]) => Array.isArray(value));
    if (entries.length > 0) {
      const rowCount = entries[0][1].length;
      if (rowCount > 0 && entries.every(([, value]) => value.length === rowCount)) {
        return Array.from({ length: rowCount }, (_unused, index) => {
          const out = {};
          entries.forEach(([key, values]) => {
            out[key] = values[index];
          });
          return out;
        });
      }
    }
  }

  return [];
}

function normalizeStockBars(rows, dayIso) {
  const fallbackTs = `${dayIso}T00:00:00.000Z`;
  return rows.map((row) => {
    const ts = normalizeTimestamp(
      row.timestamp ?? row.time ?? row.datetime ?? row.trade_timestamp,
      fallbackTs,
    );
    const close = toFinite(row.close ?? row.c ?? row.price ?? row.last);
    if (!ts || close === null || close <= 0) return null;
    return {
      tsIso: ts,
      tsMs: Date.parse(ts),
      close,
      dateIso: ts.slice(0, 10),
    };
  }).filter(Boolean);
}

function buildStockData(db, options = {}) {
  const symbols = Array.isArray(options.symbols) ? options.symbols.filter(Boolean) : [];
  const minDate = options.minDate || null;
  const maxDate = options.maxDate || null;
  let rows = [];
  try {
    const symbolFilter = symbols.length
      ? `AND (${symbols.map((_, index) => `cache_key GLOB @symbolGlob${index}`).join(' OR ')})`
      : '';
    const params = {};
    symbols.forEach((symbol, index) => {
      params[`symbolGlob${index}`] = `${symbol}|*`;
    });
    rows = db.prepare(`
      SELECT
        cache_key AS cacheKey,
        value_json AS valueJson
      FROM supplemental_metric_cache
      WHERE metric_kind = 'stock_ohlc_symbol_day'
      ${symbolFilter}
    `).all(params);
  } catch {
    return new Map();
  }

  const bySymbol = new Map();

  rows.forEach((row) => {
    const [symbol, dayIso] = String(row.cacheKey || '').split('|');
    if (!symbol || !dayIso) return;
    if (minDate && dayIso < minDate) return;
    if (maxDate && dayIso > maxDate) return;
    const bars = normalizeStockBars(parseJsonRows(row.valueJson), dayIso);
    if (!bars.length) return;

    const symbolBars = bySymbol.get(symbol) || [];
    symbolBars.push(...bars);
    bySymbol.set(symbol, symbolBars);
  });

  const normalized = new Map();
  bySymbol.forEach((bars, symbol) => {
    const sorted = bars
      .slice()
      .sort((left, right) => left.tsMs - right.tsMs);
    const unique = [];
    let prevTs = null;
    sorted.forEach((bar) => {
      if (prevTs === bar.tsMs) {
        unique[unique.length - 1] = bar;
        return;
      }
      unique.push(bar);
      prevTs = bar.tsMs;
    });

    const byDate = new Map();
    unique.forEach((bar) => {
      const list = byDate.get(bar.dateIso) || [];
      list.push(bar);
      byDate.set(bar.dateIso, list);
    });

    const dates = Array.from(byDate.keys()).sort();
    const dailyClose = new Map();
    dates.forEach((dateIso) => {
      const dayBars = byDate.get(dateIso);
      if (!dayBars || !dayBars.length) return;
      dailyClose.set(dateIso, dayBars[dayBars.length - 1].close);
    });

    const atrPctByDate = new Map();
    const orderedDates = Array.from(dailyClose.keys()).sort();
    const absReturns = [];
    for (let i = 1; i < orderedDates.length; i += 1) {
      const previous = dailyClose.get(orderedDates[i - 1]);
      const current = dailyClose.get(orderedDates[i]);
      if (!previous || !current || previous <= 0 || current <= 0) continue;
      absReturns.push(Math.abs(Math.log(current / previous)));
      const tail = absReturns.slice(Math.max(0, absReturns.length - 20));
      const meanAbs = tail.length ? (tail.reduce((acc, value) => acc + value, 0) / tail.length) : null;
      atrPctByDate.set(orderedDates[i], meanAbs);
    }

    normalized.set(symbol, {
      bars: unique,
      byDate,
      tradingDates: dates,
      atrPctByDate,
    });
  });

  return normalized;
}

function findPriceAtOrBefore(series, tsMs) {
  if (!series.length) return null;
  let left = 0;
  let right = series.length - 1;
  let candidate = null;
  while (left <= right) {
    const mid = Math.floor((left + right) / 2);
    if (series[mid].tsMs <= tsMs) {
      candidate = series[mid];
      left = mid + 1;
    } else {
      right = mid - 1;
    }
  }
  return candidate ? candidate.close : null;
}

function addTradingDays(tradingDates, currentDate, delta) {
  const index = tradingDates.indexOf(currentDate);
  if (index < 0) return null;
  const targetIndex = index + delta;
  if (targetIndex < 0 || targetIndex >= tradingDates.length) return null;
  return tradingDates[targetIndex];
}

function pickPriceForTargetDay(byDate, targetDateIso, tradeTsIso) {
  const dayBars = byDate.get(targetDateIso);
  if (!dayBars || !dayBars.length) return null;

  const trade = new Date(tradeTsIso);
  if (Number.isNaN(trade.getTime())) return dayBars[dayBars.length - 1].close;
  const minuteOfDay = (trade.getUTCHours() * 60) + trade.getUTCMinutes();

  let best = dayBars[dayBars.length - 1];
  let bestDistance = Infinity;
  dayBars.forEach((bar) => {
    const dt = new Date(bar.tsIso);
    if (Number.isNaN(dt.getTime())) return;
    const barMinute = (dt.getUTCHours() * 60) + dt.getUTCMinutes();
    const distance = Math.abs(barMinute - minuteOfDay);
    if (distance < bestDistance) {
      bestDistance = distance;
      best = bar;
    }
  });
  return best ? best.close : null;
}

function percentile(sortedValues, p) {
  if (!sortedValues.length) return null;
  const index = Math.min(sortedValues.length - 1, Math.max(0, Math.floor((p / 100) * sortedValues.length)));
  return sortedValues[index];
}

function mean(values) {
  if (!values.length) return null;
  return values.reduce((acc, value) => acc + value, 0) / values.length;
}

function computeCandidateWeights(evaluatedRows) {
  const prior = { ...DEFAULT_SWING_WEIGHTS };
  const componentKeys = Object.keys(prior);
  const positive = evaluatedRows.filter((row) => row.composite >= 0.5);
  const negative = evaluatedRows.filter((row) => row.composite < 0.5);

  const raw = {};
  componentKeys.forEach((key) => {
    const pos = mean(positive.map((row) => toFinite(row.components[key])).filter((value) => value !== null)) || 0;
    const neg = mean(negative.map((row) => toFinite(row.components[key])).filter((value) => value !== null)) || 0;
    const delta = Math.abs(pos - neg);
    raw[key] = prior[key] < 0 ? -delta : delta;
  });

  const targetAbs = Object.values(prior).reduce((acc, value) => acc + Math.abs(value), 0);
  const rawAbs = Object.values(raw).reduce((acc, value) => acc + Math.abs(value), 0);
  const scaled = {};
  componentKeys.forEach((key) => {
    if (rawAbs <= 0) {
      scaled[key] = prior[key];
      return;
    }
    scaled[key] = raw[key] * (targetAbs / rawAbs);
  });

  const blended = {};
  componentKeys.forEach((key) => {
    const candidate = (0.7 * prior[key]) + (0.3 * scaled[key]);
    if (prior[key] < 0) {
      blended[key] = Math.min(candidate, -0.000001);
    } else {
      blended[key] = Math.max(candidate, 0.000001);
    }
    blended[key] = Number(blended[key].toFixed(6));
  });

  return { prior, raw: scaled, blended };
}

function computePrecisionAtTopDecile(rows) {
  if (!rows.length) return 0;
  const sorted = rows.slice().sort((left, right) => right.sigScore - left.sigScore);
  const cutoffIndex = Math.max(1, Math.floor(sorted.length * 0.1));
  const top = sorted.slice(0, cutoffIndex);
  const successes = top.filter((row) => row.composite >= 0.5).length;
  return successes / top.length;
}

function run() {
  const args = parseArgs(process.argv);
  const dbPath = resolveDbPath(process.env);
  const db = new Database(dbPath, { readonly: true });

  try {
    const availableColumns = new Set(
      db.prepare('PRAGMA table_info(option_trade_enriched)').all().map((row) => row.name),
    );
    const selectColumn = (column, alias) => (
      availableColumns.has(column) ? `${column} AS ${alias}` : `NULL AS ${alias}`
    );

    const rows = db.prepare(`
      SELECT
        trade_id AS tradeId,
        ${selectColumn('trade_ts_utc', 'tradeTsUtc')},
        symbol,
        ${selectColumn('sig_score', 'sigScore')},
        ${selectColumn('sentiment', 'sentiment')},
        ${selectColumn('rule_version', 'ruleVersion')},
        ${selectColumn('score_quality', 'scoreQuality')},
        ${selectColumn('value_shock_norm', 'valueShockNorm')},
        ${selectColumn('vol_oi_ratio', 'volOiRatio')},
        ${selectColumn('repeat3m', 'repeat3m')},
        ${selectColumn('otm_pct', 'otmPct')},
        ${selectColumn('dte_swing_norm', 'dteSwingNorm')},
        ${selectColumn('flow_imbalance_norm', 'flowImbalanceNorm')},
        ${selectColumn('delta_pressure_norm', 'deltaPressureNorm')},
        ${selectColumn('cp_oi_pressure_norm', 'cpOiPressureNorm')},
        ${selectColumn('iv_skew_surface_norm', 'ivSkewSurfaceNorm')},
        ${selectColumn('iv_term_slope_norm', 'ivTermSlopeNorm')},
        ${selectColumn('underlying_trend_confirm_norm', 'underlyingTrendConfirmNorm')},
        ${selectColumn('liquidity_quality_norm', 'liquidityQualityNorm')},
        ${selectColumn('is_sweep', 'isSweep')},
        ${selectColumn('multileg_penalty_norm', 'multilegPenaltyNorm')}
      FROM option_trade_enriched
      WHERE ${availableColumns.has('sig_score') ? 'sig_score' : 'NULL'} IS NOT NULL
      ORDER BY ${availableColumns.has('trade_ts_utc') ? 'trade_ts_utc' : 'trade_id'} ASC
      LIMIT @maxRows
    `).all({ maxRows: args.maxRows });

    const symbols = [...new Set(rows.map((row) => row.symbol).filter(Boolean))];
    const rowDates = rows
      .map((row) => String(row.tradeTsUtc || '').slice(0, 10))
      .filter((value) => /^\d{4}-\d{2}-\d{2}$/.test(value))
      .sort();
    const minDate = rowDates.length ? rowDates[0] : null;
    const maxDate = rowDates.length ? rowDates[rowDates.length - 1] : null;
    const stockData = buildStockData(db, {
      symbols,
      minDate,
      maxDate,
    });

    const evaluatedRows = [];
    const skipped = {
      neutralSentiment: 0,
      noStockSeries: 0,
      noEntryPrice: 0,
      noForwardPrice: 0,
    };

    rows.forEach((row) => {
      const sentiment = String(row.sentiment || '').toLowerCase();
      const direction = sentiment === 'bullish' ? 1 : (sentiment === 'bearish' ? -1 : 0);
      if (direction === 0) {
        skipped.neutralSentiment += 1;
        return;
      }

      const symbolSeries = stockData.get(row.symbol);
      if (!symbolSeries) {
        skipped.noStockSeries += 1;
        return;
      }

      const tradeTsIso = normalizeTimestamp(row.tradeTsUtc, null);
      if (!tradeTsIso) {
        skipped.noEntryPrice += 1;
        return;
      }
      const tradeTsMs = Date.parse(tradeTsIso);
      const tradeDate = tradeTsIso.slice(0, 10);
      const p0 = findPriceAtOrBefore(symbolSeries.bars, tradeTsMs);
      if (!p0 || p0 <= 0) {
        skipped.noEntryPrice += 1;
        return;
      }

      const atrPct = toFinite(symbolSeries.atrPctByDate.get(tradeDate)) || 0;
      const horizonOutputs = {};
      let composite = 0;
      let foundAny = false;
      HORIZONS.forEach((horizon) => {
        const targetDate = addTradingDays(symbolSeries.tradingDates, tradeDate, horizon);
        if (!targetDate) return;
        const pH = pickPriceForTargetDay(symbolSeries.byDate, targetDate, tradeTsIso);
        if (!pH || pH <= 0) return;

        const directionalReturn = direction * Math.log(pH / p0);
        const threshold = Math.max(0.0075, 0.5 * atrPct * Math.sqrt(horizon));
        const label = directionalReturn >= threshold ? 1 : 0;
        composite += (HORIZON_WEIGHTS[horizon] * label);
        horizonOutputs[`h${horizon}`] = {
          p0,
          pH,
          directionalReturn,
          threshold,
          label,
          targetDate,
        };
        foundAny = true;
      });

      if (!foundAny) {
        skipped.noForwardPrice += 1;
        return;
      }

      evaluatedRows.push({
        tradeId: row.tradeId,
        symbol: row.symbol,
        tradeTsUtc: tradeTsIso,
        sigScore: toFinite(row.sigScore) || 0,
        composite,
        ruleVersion: row.ruleVersion || null,
        scoreQuality: row.scoreQuality || null,
        horizons: horizonOutputs,
        components: {
          valueShockNorm: toFinite(row.valueShockNorm) || 0,
          volOiNorm: toFinite(row.volOiRatio) === null ? 0 : Math.min(1, Math.max(0, (toFinite(row.volOiRatio) / 5))),
          repeatNorm: toFinite(row.repeat3m) === null ? 0 : Math.min(1, Math.max(0, toFinite(row.repeat3m) / 20)),
          otmNorm: toFinite(row.otmPct) === null ? 0 : Math.min(1, Math.max(0, Math.exp(-Math.pow((toFinite(row.otmPct) - 10) / 10, 2)))),
          dteSwingNorm: toFinite(row.dteSwingNorm) || 0,
          flowImbalanceNorm: toFinite(row.flowImbalanceNorm) || 0,
          deltaPressureNorm: toFinite(row.deltaPressureNorm) || 0,
          cpOiPressureNorm: toFinite(row.cpOiPressureNorm) || 0,
          ivSkewSurfaceNorm: toFinite(row.ivSkewSurfaceNorm) || 0,
          ivTermSlopeNorm: toFinite(row.ivTermSlopeNorm) || 0,
          underlyingTrendConfirmNorm: toFinite(row.underlyingTrendConfirmNorm) || 0,
          liquidityQualityNorm: toFinite(row.liquidityQualityNorm) || 0,
          sweepNorm: toFinite(row.isSweep) ? 1 : 0,
          multilegPenaltyNorm: toFinite(row.multilegPenaltyNorm) || 0,
        },
      });
    });

    const candidate = computeCandidateWeights(evaluatedRows);
    const precisionProxy = computePrecisionAtTopDecile(evaluatedRows);
    const scoreVsOutcome = evaluatedRows.length
      ? mean(evaluatedRows.map((row) => (row.sigScore * row.composite)))
      : 0;
    const scores = evaluatedRows.map((row) => row.sigScore).sort((a, b) => a - b);

    const generatedAt = new Date().toISOString();
    const candidateVersionId = args.candidateVersionId
      || `v5_swing_candidate_${generatedAt.replace(/[-:.TZ]/g, '').slice(0, 14)}`;
    const candidateRuleConfig = {
      version: candidateVersionId,
      sigScoreModel: 'v5_swing',
      targetSpec: {
        horizon: 'swing_1_5d',
        label: 'directional_plus_magnitude',
        calibrationWindowDays: 120,
      },
      weightBlend: { prior: 0.7, calibrated: 0.3 },
      calibrationWindowDays: 120,
      componentWeightsV5: candidate.blended,
      sigScoreWeights: candidate.blended,
    };
    const candidateChecksum = crypto
      .createHash('sha256')
      .update(JSON.stringify(candidateRuleConfig), 'utf8')
      .digest('hex');

    const report = {
      generatedAt,
      dbPath,
      totalRows: rows.length,
      maxRows: args.maxRows,
      evaluatedRows: evaluatedRows.length,
      skipped,
      scoreDistribution: {
        p50: percentile(scores, 50),
        p75: percentile(scores, 75),
        p90: percentile(scores, 90),
        p95: percentile(scores, 95),
      },
      calibrationTarget: {
        horizons: HORIZONS,
        horizonWeights: HORIZON_WEIGHTS,
        baseThreshold: 0.0075,
        atrScale: 0.5,
      },
      metrics: {
        precisionAtTopDecile: precisionProxy,
        scoreTimesCompositeMean: scoreVsOutcome,
      },
      precisionProxy,
      gate: {
        minRows: args.minRows,
        minPrecisionProxy: args.minPrecisionProxy,
        pass: evaluatedRows.length >= args.minRows && precisionProxy >= args.minPrecisionProxy,
      },
      candidateWeights: candidate,
      candidateRule: {
        versionId: candidateVersionId,
        checksum: candidateChecksum,
        configJson: candidateRuleConfig,
      },
    };

    fs.mkdirSync(path.dirname(args.outputPath), { recursive: true });
    fs.writeFileSync(args.outputPath, `${JSON.stringify(report, null, 2)}\n`, 'utf8');

    if (args.writeRuleSql) {
      const sql = [
        'PRAGMA foreign_keys = ON;',
        'BEGIN TRANSACTION;',
        `INSERT INTO filter_rule_versions (version_id, config_json, checksum, is_active, activated_at_utc) VALUES (` +
          `'${candidateVersionId}', ` +
          `'${JSON.stringify(candidateRuleConfig).replace(/'/g, "''")}', ` +
          `'${candidateChecksum}', 0, NULL)`,
        'ON CONFLICT(version_id) DO UPDATE SET',
        '  config_json = excluded.config_json,',
        '  checksum = excluded.checksum,',
        '  is_active = 0,',
        '  activated_at_utc = NULL;',
        'COMMIT;',
        '',
      ].join('\n');
      fs.mkdirSync(path.dirname(args.writeRuleSql), { recursive: true });
      fs.writeFileSync(args.writeRuleSql, sql, 'utf8');
    }

    console.log(JSON.stringify({
      status: 'ok',
      outputPath: args.outputPath,
      writeRuleSql: args.writeRuleSql,
      maxRows: args.maxRows,
      evaluatedRows: report.evaluatedRows,
      precisionProxy: report.precisionProxy,
      candidateVersionId,
      checksum: candidateChecksum,
    }, null, 2));
  } finally {
    db.close();
  }
}

run();
