const MARKET_TIMEZONE = 'America/New_York';
const REGULAR_SESSION = 'REGULAR';

const DAILY_IDEMPOTENCY_FIELDS = Object.freeze(['strategyId', 'signalDate']);
const INTRADAY_IDEMPOTENCY_FIELDS = Object.freeze(['strategyId', 'signalDate', 'signalTimestamp']);

function dailyEodExecution({ time = '16:05' } = {}) {
  return {
    timingClass: 'EOD',
    timezone: MARKET_TIMEZONE,
    session: REGULAR_SESSION,
    activation: {
      type: 'after_market_close',
      time,
    },
    signalCadence: 'daily_eod',
    idempotencyKeyFields: [...DAILY_IDEMPOTENCY_FIELDS],
  };
}

function regularSessionExecution({
  timingClass = 'INTRADAY',
  startTime = '09:35',
  endTime = '15:55',
} = {}) {
  return {
    timingClass,
    timezone: MARKET_TIMEZONE,
    session: REGULAR_SESSION,
    activation: {
      type: 'regular_session_window',
      startTime,
      endTime,
    },
    signalCadence: 'continuous_intraday',
    idempotencyKeyFields: [...INTRADAY_IDEMPOTENCY_FIELDS],
  };
}

module.exports = {
  dailyEodExecution,
  regularSessionExecution,
};
