const crypto = require('node:crypto');
const fs = require('node:fs');
const path = require('node:path');

const Database = require('better-sqlite3');

const REPO_ROOT = path.resolve(__dirname, '..', '..', '..');
const DEFAULT_DB_PATH = path.join(REPO_ROOT, 'apps', 'strategy-service', 'runtime', 'strategy-results.sqlite');

function defaultDbPath() {
  return process.env.STRATEGY_RESULTS_DB_PATH
    ? path.resolve(process.env.STRATEGY_RESULTS_DB_PATH)
    : DEFAULT_DB_PATH;
}

function stableJson(value) {
  if (value === null || typeof value !== 'object') return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map(stableJson).join(',')}]`;
  return `{${Object.keys(value).sort().map((key) => `${JSON.stringify(key)}:${stableJson(value[key])}`).join(',')}}`;
}

function sha256(value) {
  return crypto.createHash('sha256').update(stableJson(value)).digest('hex');
}

function json(value) {
  return value === undefined ? null : JSON.stringify(value);
}

function finite(value) {
  return Number.isFinite(value) ? value : null;
}

function openStrategyResultStore(dbPath = defaultDbPath()) {
  fs.mkdirSync(path.dirname(dbPath), { recursive: true });
  const db = new Database(dbPath);
  db.pragma('journal_mode = WAL');
  db.pragma('foreign_keys = ON');
  ensureSchema(db);
  return db;
}

function ensureSchema(db) {
  db.exec(`
    CREATE TABLE IF NOT EXISTS strategies (
      strategy_id TEXT PRIMARY KEY,
      name TEXT,
      family TEXT,
      cadence TEXT,
      action_type TEXT,
      metadata_json TEXT,
      updated_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS strategy_import_runs (
      import_id TEXT PRIMARY KEY,
      strategy_id TEXT NOT NULL,
      report_generated_at TEXT,
      imported_at TEXT NOT NULL,
      daily_result_count INTEGER NOT NULL DEFAULT 0,
      holding_count INTEGER NOT NULL DEFAULT 0,
      trade_count INTEGER NOT NULL DEFAULT 0,
      source_json TEXT
    );

    CREATE TABLE IF NOT EXISTS strategy_daily_results (
      strategy_id TEXT NOT NULL,
      date TEXT NOT NULL,
      signal_date TEXT,
      target_date TEXT,
      next_date TEXT,
      cadence TEXT,
      basis TEXT NOT NULL,
      source TEXT,
      start_equity REAL,
      end_equity REAL,
      pnl_dollars REAL,
      gross_return REAL,
      gross_return_pct REAL,
      net_return REAL,
      net_return_pct REAL,
      cost_return REAL,
      cost_return_pct REAL,
      missing_return_count INTEGER,
      trade_count INTEGER,
      holding_count INTEGER,
      top_holdings TEXT,
      report_generated_at TEXT,
      updated_at TEXT NOT NULL,
      raw_json TEXT,
      PRIMARY KEY (strategy_id, date)
    );

    CREATE TABLE IF NOT EXISTS strategy_holdings (
      strategy_id TEXT NOT NULL,
      target_date TEXT NOT NULL,
      ticker TEXT NOT NULL,
      next_date TEXT,
      realized_date TEXT,
      weight REAL,
      weight_pct REAL,
      dollars REAL,
      previous_weight REAL,
      weight_change REAL,
      weight_change_pct REAL,
      report_generated_at TEXT,
      updated_at TEXT NOT NULL,
      raw_json TEXT,
      PRIMARY KEY (strategy_id, target_date, ticker)
    );

    CREATE TABLE IF NOT EXISTS strategy_trades (
      strategy_id TEXT NOT NULL,
      trade_id TEXT NOT NULL,
      sequence INTEGER,
      date TEXT,
      signal_date TEXT,
      entry_date TEXT,
      exit_date TEXT,
      signal_time_utc TEXT,
      entry_time_utc TEXT,
      exit_time_utc TEXT,
      side TEXT,
      ticker TEXT,
      entry_price REAL,
      exit_price REAL,
      gross_return REAL,
      gross_return_pct REAL,
      net_return REAL,
      net_return_pct REAL,
      pnl_dollars REAL,
      quantity REAL,
      leverage REAL,
      carry_over INTEGER,
      reason TEXT,
      report_generated_at TEXT,
      updated_at TEXT NOT NULL,
      raw_json TEXT,
      PRIMARY KEY (strategy_id, trade_id)
    );

    CREATE INDEX IF NOT EXISTS idx_strategy_daily_results_date
      ON strategy_daily_results(date);
    CREATE INDEX IF NOT EXISTS idx_strategy_holdings_target_date
      ON strategy_holdings(target_date);
    CREATE INDEX IF NOT EXISTS idx_strategy_trades_date
      ON strategy_trades(date);
  `);
}

function persistStrategyReport(db, { metadata, report, importedAt = new Date().toISOString() }) {
  if (!metadata?.id) throw new Error('persist_strategy_missing_id');
  if (!report?.resultContract) throw new Error(`persist_strategy_missing_result_contract:${metadata.id}`);

  const putStrategy = db.prepare(`
    INSERT INTO strategies (strategy_id, name, family, cadence, action_type, metadata_json, updated_at)
    VALUES (@strategyId, @name, @family, @cadence, @actionType, @metadataJson, @updatedAt)
    ON CONFLICT(strategy_id) DO UPDATE SET
      name = excluded.name,
      family = excluded.family,
      cadence = excluded.cadence,
      action_type = excluded.action_type,
      metadata_json = excluded.metadata_json,
      updated_at = excluded.updated_at
  `);
  const putRun = db.prepare(`
    INSERT INTO strategy_import_runs (
      import_id, strategy_id, report_generated_at, imported_at,
      daily_result_count, holding_count, trade_count, source_json
    ) VALUES (
      @importId, @strategyId, @reportGeneratedAt, @importedAt,
      @dailyResultCount, @holdingCount, @tradeCount, @sourceJson
    )
    ON CONFLICT(import_id) DO UPDATE SET
      imported_at = excluded.imported_at,
      daily_result_count = excluded.daily_result_count,
      holding_count = excluded.holding_count,
      trade_count = excluded.trade_count,
      source_json = excluded.source_json
  `);
  const putDaily = db.prepare(`
    INSERT INTO strategy_daily_results (
      strategy_id, date, signal_date, target_date, next_date, cadence, basis, source,
      start_equity, end_equity, pnl_dollars,
      gross_return, gross_return_pct, net_return, net_return_pct,
      cost_return, cost_return_pct, missing_return_count,
      trade_count, holding_count, top_holdings,
      report_generated_at, updated_at, raw_json
    ) VALUES (
      @strategyId, @date, @signalDate, @targetDate, @nextDate, @cadence, @basis, @source,
      @startEquity, @endEquity, @pnlDollars,
      @grossReturn, @grossReturnPct, @netReturn, @netReturnPct,
      @costReturn, @costReturnPct, @missingReturnCount,
      @tradeCount, @holdingCount, @topHoldings,
      @reportGeneratedAt, @updatedAt, @rawJson
    )
    ON CONFLICT(strategy_id, date) DO UPDATE SET
      signal_date = excluded.signal_date,
      target_date = excluded.target_date,
      next_date = excluded.next_date,
      cadence = excluded.cadence,
      basis = excluded.basis,
      source = excluded.source,
      start_equity = excluded.start_equity,
      end_equity = excluded.end_equity,
      pnl_dollars = excluded.pnl_dollars,
      gross_return = excluded.gross_return,
      gross_return_pct = excluded.gross_return_pct,
      net_return = excluded.net_return,
      net_return_pct = excluded.net_return_pct,
      cost_return = excluded.cost_return,
      cost_return_pct = excluded.cost_return_pct,
      missing_return_count = excluded.missing_return_count,
      trade_count = excluded.trade_count,
      holding_count = excluded.holding_count,
      top_holdings = excluded.top_holdings,
      report_generated_at = excluded.report_generated_at,
      updated_at = excluded.updated_at,
      raw_json = excluded.raw_json
  `);
  const deleteHoldingsForDate = db.prepare(`
    DELETE FROM strategy_holdings WHERE strategy_id = ? AND target_date = ?
  `);
  const putHolding = db.prepare(`
    INSERT INTO strategy_holdings (
      strategy_id, target_date, ticker, next_date, realized_date,
      weight, weight_pct, dollars,
      previous_weight, weight_change, weight_change_pct,
      report_generated_at, updated_at, raw_json
    ) VALUES (
      @strategyId, @targetDate, @ticker, @nextDate, @realizedDate,
      @weight, @weightPct, @dollars,
      @previousWeight, @weightChange, @weightChangePct,
      @reportGeneratedAt, @updatedAt, @rawJson
    )
    ON CONFLICT(strategy_id, target_date, ticker) DO UPDATE SET
      next_date = excluded.next_date,
      realized_date = excluded.realized_date,
      weight = excluded.weight,
      weight_pct = excluded.weight_pct,
      dollars = excluded.dollars,
      previous_weight = excluded.previous_weight,
      weight_change = excluded.weight_change,
      weight_change_pct = excluded.weight_change_pct,
      report_generated_at = excluded.report_generated_at,
      updated_at = excluded.updated_at,
      raw_json = excluded.raw_json
  `);
  const putTrade = db.prepare(`
    INSERT INTO strategy_trades (
      strategy_id, trade_id, sequence, date, signal_date, entry_date, exit_date,
      signal_time_utc, entry_time_utc, exit_time_utc,
      side, ticker, entry_price, exit_price,
      gross_return, gross_return_pct, net_return, net_return_pct,
      pnl_dollars, quantity, leverage, carry_over, reason,
      report_generated_at, updated_at, raw_json
    ) VALUES (
      @strategyId, @tradeId, @sequence, @date, @signalDate, @entryDate, @exitDate,
      @signalTimeUtc, @entryTimeUtc, @exitTimeUtc,
      @side, @ticker, @entryPrice, @exitPrice,
      @grossReturn, @grossReturnPct, @netReturn, @netReturnPct,
      @pnlDollars, @quantity, @leverage, @carryOver, @reason,
      @reportGeneratedAt, @updatedAt, @rawJson
    )
    ON CONFLICT(strategy_id, trade_id) DO UPDATE SET
      sequence = excluded.sequence,
      date = excluded.date,
      signal_date = excluded.signal_date,
      entry_date = excluded.entry_date,
      exit_date = excluded.exit_date,
      signal_time_utc = excluded.signal_time_utc,
      entry_time_utc = excluded.entry_time_utc,
      exit_time_utc = excluded.exit_time_utc,
      side = excluded.side,
      ticker = excluded.ticker,
      entry_price = excluded.entry_price,
      exit_price = excluded.exit_price,
      gross_return = excluded.gross_return,
      gross_return_pct = excluded.gross_return_pct,
      net_return = excluded.net_return,
      net_return_pct = excluded.net_return_pct,
      pnl_dollars = excluded.pnl_dollars,
      quantity = excluded.quantity,
      leverage = excluded.leverage,
      carry_over = excluded.carry_over,
      reason = excluded.reason,
      report_generated_at = excluded.report_generated_at,
      updated_at = excluded.updated_at,
      raw_json = excluded.raw_json
  `);

  const snapshots = Array.isArray(report.snapshots) ? report.snapshots : [];
  const dailyResults = Array.isArray(report.dailyResults) ? report.dailyResults : [];
  const trades = Array.isArray(report.normalizedTrades) ? report.normalizedTrades : [];
  const holdings = [];
  snapshots.forEach((snapshot) => {
    if (!snapshot?.date || !Array.isArray(snapshot.holdings)) return;
    snapshot.holdings.forEach((holding) => holdings.push({ snapshot, holding }));
  });

  const write = db.transaction(() => {
    putStrategy.run({
      strategyId: metadata.id,
      name: metadata.displayName || metadata.name || metadata.id,
      family: metadata.family || null,
      cadence: metadata.cadence || null,
      actionType: metadata.actionType || null,
      metadataJson: json(metadata),
      updatedAt: importedAt,
    });
    putRun.run({
      importId: sha256({
        strategyId: metadata.id,
        reportGeneratedAt: report.generatedAt || null,
        importedAt,
        source: report.source || null,
      }),
      strategyId: metadata.id,
      reportGeneratedAt: report.generatedAt || null,
      importedAt,
      dailyResultCount: dailyResults.length,
      holdingCount: holdings.length,
      tradeCount: trades.length,
      sourceJson: json(report.source || null),
    });
    dailyResults.forEach((result) => putDaily.run({
      strategyId: metadata.id,
      date: result.date,
      signalDate: result.signalDate || null,
      targetDate: result.targetDate || null,
      nextDate: result.nextDate || null,
      cadence: result.cadence || metadata.cadence || null,
      basis: result.basis,
      source: result.source || null,
      startEquity: finite(result.startEquity),
      endEquity: finite(result.endEquity),
      pnlDollars: finite(result.pnlDollars),
      grossReturn: finite(result.grossReturn),
      grossReturnPct: finite(result.grossReturnPct),
      netReturn: finite(result.netReturn),
      netReturnPct: finite(result.netReturnPct),
      costReturn: finite(result.costReturn),
      costReturnPct: finite(result.costReturnPct),
      missingReturnCount: finite(result.missingReturnCount),
      tradeCount: finite(result.tradeCount),
      holdingCount: finite(result.holdingCount),
      topHoldings: result.topHoldings || null,
      reportGeneratedAt: report.generatedAt || null,
      updatedAt: importedAt,
      rawJson: json(result),
    }));
    const targetDates = [...new Set(snapshots.map((snapshot) => snapshot?.date).filter(Boolean))];
    targetDates.forEach((targetDate) => deleteHoldingsForDate.run(metadata.id, targetDate));
    holdings.forEach(({ snapshot, holding }) => putHolding.run({
      strategyId: metadata.id,
      targetDate: snapshot.date,
      ticker: holding.ticker || holding.symbol || 'UNKNOWN',
      nextDate: snapshot.nextDate || null,
      realizedDate: snapshot.realized?.date || null,
      weight: finite(holding.weight),
      weightPct: finite(holding.weightPct),
      dollars: finite(holding.dollars),
      previousWeight: finite(holding.previousWeight),
      weightChange: finite(holding.weightChange),
      weightChangePct: finite(holding.weightChangePct),
      reportGeneratedAt: report.generatedAt || null,
      updatedAt: importedAt,
      rawJson: json(holding),
    }));
    trades.forEach((trade) => {
      const tradeId = sha256({
        strategyId: metadata.id,
        sequence: trade.sequence,
        date: trade.date,
        signalDate: trade.signalDate,
        entryDate: trade.entryDate,
        exitDate: trade.exitDate,
        entryTimeUtc: trade.entryTimeUtc,
        exitTimeUtc: trade.exitTimeUtc,
        ticker: trade.ticker,
        side: trade.side,
        entryPrice: trade.entryPrice,
        exitPrice: trade.exitPrice,
        netReturn: trade.netReturn,
        pnlDollars: trade.pnlDollars,
      });
      putTrade.run({
        strategyId: metadata.id,
        tradeId,
        sequence: trade.sequence,
        date: trade.date || null,
        signalDate: trade.signalDate || null,
        entryDate: trade.entryDate || null,
        exitDate: trade.exitDate || null,
        signalTimeUtc: trade.signalTimeUtc || null,
        entryTimeUtc: trade.entryTimeUtc || null,
        exitTimeUtc: trade.exitTimeUtc || null,
        side: trade.side || null,
        ticker: trade.ticker || null,
        entryPrice: finite(trade.entryPrice),
        exitPrice: finite(trade.exitPrice),
        grossReturn: finite(trade.grossReturn),
        grossReturnPct: finite(trade.grossReturnPct),
        netReturn: finite(trade.netReturn),
        netReturnPct: finite(trade.netReturnPct),
        pnlDollars: finite(trade.pnlDollars),
        quantity: finite(trade.quantity),
        leverage: finite(trade.leverage),
        carryOver: trade.carryOver ? 1 : 0,
        reason: trade.reason || null,
        reportGeneratedAt: report.generatedAt || null,
        updatedAt: importedAt,
        rawJson: json(trade.raw || trade),
      });
    });
  });

  write();
  return {
    strategyId: metadata.id,
    dailyResultCount: dailyResults.length,
    holdingCount: holdings.length,
    tradeCount: trades.length,
  };
}

module.exports = {
  defaultDbPath,
  ensureSchema,
  openStrategyResultStore,
  persistStrategyReport,
};
