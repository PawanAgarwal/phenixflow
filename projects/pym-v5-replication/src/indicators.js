function finite(value) {
  return Number.isFinite(value) ? value : null;
}

function windowParam(node, side) {
  const direct = node[`${side}-fn-params`]?.window
    ?? node[`${side}-window-days`]
    ?? node[`${side}-by-window-days`]
    ?? node['window-days'];
  const parsed = Number(direct);
  return Number.isFinite(parsed) ? parsed : null;
}

function closesFor(ctx, ticker) {
  return ctx.closes.get(String(ticker || '').toUpperCase()) || [];
}

function closeAt(ctx, ticker, index) {
  return finite(closesFor(ctx, ticker)[index]);
}

function sliceWindow(ctx, ticker, index, window) {
  if (!Number.isFinite(window) || window <= 0) return null;
  const values = closesFor(ctx, ticker);
  const start = index - window + 1;
  if (start < 0) return null;
  const out = values.slice(start, index + 1);
  return out.length === window && out.every(Number.isFinite) ? out : null;
}

function dailyReturns(ctx, ticker, index, window) {
  if (!Number.isFinite(window) || window <= 0) return null;
  const values = closesFor(ctx, ticker);
  const start = index - window;
  if (start < 0) return null;
  const out = [];
  for (let i = start + 1; i <= index; i += 1) {
    const previous = values[i - 1];
    const current = values[i];
    if (!Number.isFinite(previous) || !Number.isFinite(current) || previous <= 0) return null;
    out.push((current / previous) - 1);
  }
  return out;
}

function average(values) {
  if (!values?.length) return null;
  return values.reduce((sum, value) => sum + value, 0) / values.length;
}

function movingAveragePrice(ctx, ticker, index, window) {
  const values = sliceWindow(ctx, ticker, index, window);
  return values ? average(values) : null;
}

function exponentialMovingAveragePrice(ctx, ticker, index, window) {
  const values = sliceWindow(ctx, ticker, index, window);
  if (!values) return null;
  const alpha = 2 / (window + 1);
  return values.slice(1).reduce((ema, value) => (value * alpha) + (ema * (1 - alpha)), values[0]);
}

function cumulativeReturn(ctx, ticker, index, window) {
  const values = closesFor(ctx, ticker);
  const previousIndex = index - window;
  if (previousIndex < 0) return null;
  const previous = values[previousIndex];
  const current = values[index];
  if (!Number.isFinite(previous) || !Number.isFinite(current) || previous <= 0) return null;
  return ((current / previous) - 1) * 100;
}

function movingAverageReturn(ctx, ticker, index, window) {
  const returns = dailyReturns(ctx, ticker, index, window);
  return returns ? average(returns) * 100 : null;
}

function standardDeviationReturn(ctx, ticker, index, window) {
  const returns = dailyReturns(ctx, ticker, index, window);
  if (!returns) return null;
  const mean = average(returns);
  const variance = average(returns.map((value) => (value - mean) ** 2));
  return Math.sqrt(variance) * 100;
}

function maxDrawdown(ctx, ticker, index, window) {
  const values = sliceWindow(ctx, ticker, index, window);
  if (!values) return null;
  let peak = values[0];
  let worst = 0;
  values.forEach((value) => {
    if (value > peak) peak = value;
    if (peak > 0) worst = Math.min(worst, (value / peak) - 1);
  });
  return worst * 100;
}

function simpleRelativeStrengthIndex(ctx, ticker, index, window) {
  const returns = dailyReturns(ctx, ticker, index, window);
  if (!returns) return null;
  let gain = 0;
  let loss = 0;
  returns.forEach((value) => {
    if (value > 0) gain += value;
    else loss -= value;
  });
  const avgGain = gain / returns.length;
  const avgLoss = loss / returns.length;
  if (avgLoss === 0 && avgGain === 0) return 50;
  if (avgLoss === 0) return 100;
  const rs = avgGain / avgLoss;
  return 100 - (100 / (1 + rs));
}

function wilderRelativeStrengthIndex(ctx, ticker, index, window) {
  const values = closesFor(ctx, ticker);
  if (!Number.isFinite(window) || window <= 0 || index < window) return null;
  let avgGain = 0;
  let avgLoss = 0;
  for (let i = 1; i <= window; i += 1) {
    const ret = values[i] / values[i - 1] - 1;
    if (!Number.isFinite(ret)) return null;
    if (ret > 0) avgGain += ret;
    else avgLoss -= ret;
  }
  avgGain /= window;
  avgLoss /= window;
  for (let i = window + 1; i <= index; i += 1) {
    const ret = values[i] / values[i - 1] - 1;
    if (!Number.isFinite(ret)) return null;
    const gain = ret > 0 ? ret : 0;
    const loss = ret < 0 ? -ret : 0;
    avgGain = ((avgGain * (window - 1)) + gain) / window;
    avgLoss = ((avgLoss * (window - 1)) + loss) / window;
  }
  if (avgLoss === 0 && avgGain === 0) return 50;
  if (avgLoss === 0) return 100;
  const rs = avgGain / avgLoss;
  return 100 - (100 / (1 + rs));
}

function relativeStrengthIndex(ctx, ticker, index, window) {
  if (ctx.rsiMode === 'wilder') return wilderRelativeStrengthIndex(ctx, ticker, index, window);
  return simpleRelativeStrengthIndex(ctx, ticker, index, window);
}

function indicatorValue(ctx, fnName, ticker, index, window) {
  switch (fnName) {
    case 'current-price':
      return closeAt(ctx, ticker, index);
    case 'moving-average-price':
      return movingAveragePrice(ctx, ticker, index, window);
    case 'exponential-moving-average-price':
      return exponentialMovingAveragePrice(ctx, ticker, index, window);
    case 'cumulative-return':
      return cumulativeReturn(ctx, ticker, index, window);
    case 'moving-average-return':
      return movingAverageReturn(ctx, ticker, index, window);
    case 'standard-deviation-return':
      return standardDeviationReturn(ctx, ticker, index, window);
    case 'max-drawdown':
      return maxDrawdown(ctx, ticker, index, window);
    case 'relative-strength-index':
      return relativeStrengthIndex(ctx, ticker, index, window);
    default:
      throw new Error(`Unsupported Composer indicator: ${fnName}`);
  }
}

module.exports = {
  windowParam,
  indicatorValue,
  relativeStrengthIndex,
  cumulativeReturn,
  movingAveragePrice,
  maxDrawdown,
};
