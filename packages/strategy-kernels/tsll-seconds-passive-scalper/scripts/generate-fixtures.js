#!/usr/bin/env node
const fs = require('node:fs');
const path = require('node:path');

const settings = require('../settings/default.json');
const {
  createKernel,
  onEvent,
  replay,
} = require('../src/kernel');
const {
  jsonlFromRecords,
  sha256Jsonl,
} = require('../src/canonical');

const ROOT = path.resolve(__dirname, '..');

function writeJsonl(relativePath, records) {
  const filePath = path.join(ROOT, relativePath);
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  fs.writeFileSync(filePath, jsonlFromRecords(records), 'utf8');
}

function event(eventType, sequence, eventTime, payload = {}, extra = {}) {
  return {
    schemaVersion: 'phenixflow.kernelEvent.v1',
    eventType,
    strategyId: 'tsll-seconds-passive-scalper',
    strategyVersion: '2026.05.13',
    kernelVersion: 'tsll-seconds-passive-scalper.execution.v1',
    symbol: extra.symbol || payload.symbol || 'TSLL',
    sequence,
    eventTime,
    observedAt: eventTime,
    source: extra.source || 'fixture',
    quality: {
      delayed: false,
      stale: false,
      complete: true,
      ...(extra.quality || {}),
    },
    payload,
  };
}

function passBar(sequence, close, overrides = {}) {
  const ms = Date.parse('2026-05-13T13:35:00.000Z') + (sequence * 1000);
  return event('BAR_1S_CLOSED', sequence, new Date(ms).toISOString(), {
    tradeDate: '2026-05-13',
    tsUtc: new Date(ms).toISOString(),
    open: close,
    high: close + 0.01,
    low: close - 0.01,
    close,
    volume: 1000,
    trade_count: 3,
    minutes_from_open: 5,
    minutes_to_close: 350,
    range_60s_cents: 5,
    ret_60s_cents: 0,
    ret_1bar_cents: 0,
    market_ok_1m: 1,
    spy_ret_1m: 0,
    qqq_ret_1m: 0,
    tsla_ret_1m: 0,
    ...overrides,
  });
}

function runBacktestFixture() {
  const events = [
    event('SESSION_STARTED', 0, '2026-05-13T13:35:00.000Z', { tradeDate: '2026-05-13' }),
    passBar(1, 10, { minutes_from_open: 4 }),
    passBar(2, 10.02),
    event('ORDER_CANCELLED', 3, '2026-05-13T13:35:03.000Z', { reason: 'entry_not_touched_next_bar' }),
    passBar(3, 10.03),
    event('ORDER_FILLED', 4, '2026-05-13T13:35:04.000Z', {
      side: 'BUY',
      fillType: 'entry',
      fillPrice: 10,
      tradeDate: '2026-05-13',
      signalSequence: 3,
      signalTsUtc: '2026-05-13T13:35:03.000Z',
      signalClose: 10.03,
      entrySequence: 4,
      entryTsUtc: '2026-05-13T13:35:04.000Z',
    }),
    passBar(4, 10.01),
    event('TIMER', 14, '2026-05-13T13:35:14.000Z', { reason: 'max_hold_check' }),
    event('ORDER_FILLED', 14, '2026-05-13T13:35:14.000Z', {
      side: 'SELL',
      fillType: 'exit',
      fillPrice: 10.01,
      exitReason: 'timeout',
      exitSequence: 14,
    }),
    passBar(15, 10.02),
    passBar(17, 10.04),
    event('SESSION_ENDED', 1000, '2026-05-13T20:00:00.000Z', { tradeDate: '2026-05-13' }),
  ];
  const created = createKernel({
    settings,
    mode: 'backtest',
    clock: { timezone: 'America/New_York', sessionDate: '2026-05-13' },
  });
  let state = created.state;
  const decisions = [];
  const traces = [];
  events.forEach((item) => {
    const result = onEvent(state, item);
    state = result.state;
    decisions.push(...result.decisions);
    traces.push(...result.traces);
  });
  return { events, decisions, traces };
}

function runPaperMissingContextFixture() {
  const events = [
    event('SESSION_STARTED', 0, '2026-05-13T13:35:00.000Z', { tradeDate: '2026-05-13' }),
    event('BAR_1S_CLOSED', 1, '2026-05-13T13:35:01.000Z', {
      tradeDate: '2026-05-13',
      tsUtc: '2026-05-13T13:35:01.000Z',
      open: 10,
      high: 10.02,
      low: 9.99,
      close: 10.01,
      volume: 1000,
      trade_count: 4,
      minutes_from_open: 5,
      minutes_to_close: 350,
      range_60s_cents: 5,
      ret_60s_cents: 0,
      ret_1bar_cents: 0,
    }),
  ];
  const created = createKernel({
    settings,
    mode: 'paper',
    clock: { timezone: 'America/New_York', sessionDate: '2026-05-13' },
  });
  let state = created.state;
  const decisions = [];
  const traces = [];
  events.forEach((item) => {
    const result = onEvent(state, item);
    state = result.state;
    decisions.push(...result.decisions);
    traces.push(...result.traces);
  });
  return { events, decisions, traces };
}

function main() {
  const fixture = runBacktestFixture();
  const missingContext = runPaperMissingContextFixture();
  writeJsonl('fixtures/replay-input.jsonl', fixture.events);
  writeJsonl('fixtures/expected-decisions.jsonl', fixture.decisions);
  writeJsonl('fixtures/expected-traces.jsonl', fixture.traces);
  writeJsonl('fixtures/edge-cases/missing-context-paper-input.jsonl', missingContext.events);
  writeJsonl('fixtures/edge-cases/missing-context-paper-expected-decisions.jsonl', missingContext.decisions);
  writeJsonl('fixtures/edge-cases/missing-context-paper-expected-traces.jsonl', missingContext.traces);
  const replayResult = replay({
    settings,
    events: fixture.events,
    mode: 'backtest',
    clock: { timezone: 'America/New_York', sessionDate: '2026-05-13' },
    expectedDecisionSha256: sha256Jsonl(fixture.decisions),
    expectedTraceSha256: sha256Jsonl(fixture.traces),
  });
  if (!replayResult.passed) throw new Error('generated_fixture_replay_failed');
  console.log(JSON.stringify({
    events: fixture.events.length,
    decisions: fixture.decisions.length,
    traces: fixture.traces.length,
    decisionSha256: sha256Jsonl(fixture.decisions),
    traceSha256: sha256Jsonl(fixture.traces),
  }, null, 2));
}

main();
