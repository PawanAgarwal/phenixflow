const fs = require('node:fs');

const { readDailyBarsJsonl, tickerReturn } = require('./backtest');
const { evaluateSymphony } = require('./symphony');

const SECTOR_TICKERS = Object.freeze(['XLB', 'XLC', 'XLE', 'XLF', 'XLI', 'XLK', 'XLP', 'XLRE', 'XLU', 'XLV', 'XLY']);
const SAFE_TICKER = 'BIL';

function mergeDailyBars(primaryPath, extraPath) {
  const primary = readDailyBarsJsonl(primaryPath);
  if (!extraPath || !fs.existsSync(extraPath)) return primary;
  const extra = fs.readFileSync(extraPath, 'utf8').split('\n').filter(Boolean).map((line) => JSON.parse(line));
  const dateSet = new Set(primary.dates);
  const tickerSet = new Set(primary.tickers);
  const byDate = primary.byDate;
  extra.forEach((row) => {
    if (!dateSet.has(row.date)) return;
    if (!byDate.has(row.date)) byDate.set(row.date, new Map());
    byDate.get(row.date).set(row.ticker, row);
    tickerSet.add(row.ticker);
  });
  const tickers = [...tickerSet].sort();
  const closes = new Map(primary.closes);
  const newTickers = [...tickerSet].filter((ticker) => !primary.closes.has(ticker));
  newTickers.forEach((ticker) => {
    const series = primary.dates.map((date) => byDate.get(date)?.get(ticker)?.close ?? null);
    closes.set(ticker, series);
  });
  return { ...primary, tickers, closes };
}

function cleanWeights(weights) {
  const out = new Map();
  let sum = 0;
  weights.forEach((weight, ticker) => {
    if (Number.isFinite(weight) && weight > 1e-10) {
      out.set(ticker, weight);
      sum += weight;
    }
  });
  if (sum <= 0) return new Map();
  if (Math.abs(sum - 1) > 1e-6) {
    const normalized = new Map();
    out.forEach((weight, ticker) => normalized.set(ticker, weight / sum));
    return normalized;
  }
  return out;
}

function blendWeights(primary, secondary, secondaryWeight) {
  const out = new Map();
  primary.forEach((weight, ticker) => out.set(ticker, weight * (1 - secondaryWeight)));
  secondary.forEach((weight, ticker) => out.set(ticker, (out.get(ticker) || 0) + weight * secondaryWeight));
  return cleanWeights(out);
}

function blendWithBil(weights, scale) {
  const out = new Map();
  weights.forEach((weight, ticker) => out.set(ticker, weight * scale));
  out.set(SAFE_TICKER, (out.get(SAFE_TICKER) || 0) + (1 - scale));
  return cleanWeights(out);
}

function holdTicker(ticker, weight = 1) {
  return new Map([[ticker, weight]]);
}

function rsiWilder(closes, index, window) {
  if (!Array.isArray(closes) || index < window) return null;
  let avgGain = 0;
  let avgLoss = 0;
  for (let i = 1; i <= window; i += 1) {
    const ret = closes[i] / closes[i - 1] - 1;
    if (!Number.isFinite(ret)) return null;
    if (ret > 0) avgGain += ret; else avgLoss -= ret;
  }
  avgGain /= window;
  avgLoss /= window;
  for (let i = window + 1; i <= index; i += 1) {
    const ret = closes[i] / closes[i - 1] - 1;
    if (!Number.isFinite(ret)) return null;
    const gain = ret > 0 ? ret : 0;
    const loss = ret < 0 ? -ret : 0;
    avgGain = ((avgGain * (window - 1)) + gain) / window;
    avgLoss = ((avgLoss * (window - 1)) + loss) / window;
  }
  if (avgGain === 0 && avgLoss === 0) return 50;
  if (avgLoss === 0) return 100;
  return 100 - (100 / (1 + avgGain / avgLoss));
}

function sma(closes, index, window) {
  if (index + 1 < window) return null;
  let sum = 0;
  let count = 0;
  for (let i = index - window + 1; i <= index; i += 1) {
    const value = closes[i];
    if (!Number.isFinite(value)) return null;
    sum += value;
    count += 1;
  }
  return count === window ? sum / window : null;
}

function priceReturn(closes, index, window) {
  const previousIndex = index - window;
  if (previousIndex < 0) return null;
  const previous = closes[previousIndex];
  const current = closes[index];
  if (!Number.isFinite(previous) || !Number.isFinite(current) || previous <= 0) return null;
  return (current / previous) - 1;
}

function rollingZ(values, window) {
  if (values.length < window) return null;
  const slice = values.slice(-window);
  const valid = slice.filter(Number.isFinite);
  if (valid.length < Math.max(5, Math.floor(window / 2))) return null;
  const mean = valid.reduce((sum, value) => sum + value, 0) / valid.length;
  const variance = valid.reduce((sum, value) => sum + (value - mean) ** 2, 0) / valid.length;
  const std = Math.sqrt(variance);
  if (std === 0) return null;
  const last = values.at(-1);
  return Number.isFinite(last) ? (last - mean) / std : null;
}

function precomputeContext(market, score, rsiMode = 'wilder') {
  const baseWeights = new Array(market.dates.length).fill(null);
  const subStrategyWeights = new Array(market.dates.length).fill(null);
  const subStrategyNames = (score.children?.[0]?.children || []).map((node, idx) => node.name || `sleeve_${idx}`);
  for (let index = 0; index < market.dates.length; index += 1) {
    baseWeights[index] = cleanWeights(evaluateSymphony(score, market, index, { rsiMode }));
    const sleeves = (score.children?.[0]?.children || []).map((child) => (
      cleanWeights(evaluateSymphony(child, market, index, { rsiMode }))
    ));
    subStrategyWeights[index] = sleeves;
  }
  // Per-sleeve next-session realized returns for Sharpe-based meta reweighting.
  const sleeveDailyReturns = subStrategyNames.map(() => new Array(market.dates.length).fill(0));
  for (let index = 0; index < market.dates.length - 1; index += 1) {
    const sleeves = subStrategyWeights[index] || [];
    sleeves.forEach((weights, sleeveIndex) => {
      let ret = 0;
      weights.forEach((weight, ticker) => {
        const r = tickerReturn(market.closes, ticker, index + 1);
        if (r !== null) ret += weight * r;
      });
      sleeveDailyReturns[sleeveIndex][index + 1] = ret;
    });
  }
  return { baseWeights, subStrategyWeights, subStrategyNames, sleeveDailyReturns };
}

// ---------- Strategy definitions ----------

function strategyBase() {
  return {
    id: 'base_pym',
    family: 'baseline',
    name: 'Base PYM V5',
    fn: (ctx) => ctx.baseWeights,
  };
}

function strategySpyBuyHold() {
  return {
    id: 'spy_buy_hold',
    family: 'baseline',
    name: 'SPY buy and hold',
    fn: () => holdTicker('SPY'),
  };
}

function strategyBilCash() {
  return {
    id: 'bil_cash',
    family: 'baseline',
    name: 'BIL cash proxy',
    fn: () => holdTicker('BIL'),
  };
}

// 1. Credit-spread risk-off overlay using HYG/LQD ratio.
function strategyCreditSpread({ ratioPair, threshold, mode }) {
  const [num, den] = ratioPair;
  const id = `credit_${num.toLowerCase()}_${den.toLowerCase()}_z_lt_${String(threshold).replace('-', 'm').replace('.', 'p')}_${mode}`;
  return {
    id,
    family: 'credit_overlay',
    name: `Credit ${num}/${den} risk-off, 5d-z < ${threshold}, ${mode}`,
    description: `When the 5-day return of the ${num}/${den} ratio falls below ${threshold} on a 20-day rolling z-score, ${mode === 'to_bil' ? 'rotate fully to BIL' : 'cut PYM exposure in half and add BIL'}.`,
    fn: (ctx) => {
      const numCloses = ctx.market.closes.get(num);
      const denCloses = ctx.market.closes.get(den);
      if (!numCloses || !denCloses) return ctx.baseWeights;
      const idx = ctx.signalIndex;
      const ratios = [];
      for (let i = Math.max(20, idx - 40); i <= idx; i += 1) {
        const n = numCloses[i];
        const d = denCloses[i];
        if (Number.isFinite(n) && Number.isFinite(d) && d > 0) ratios.push({ index: i, value: n / d });
        else ratios.push({ index: i, value: null });
      }
      const fiveDayReturns = ratios.map(({ index }) => {
        const current = numCloses[index] / denCloses[index];
        const prevIndex = index - 5;
        if (prevIndex < 0) return null;
        const previous = numCloses[prevIndex] / denCloses[prevIndex];
        if (!Number.isFinite(current) || !Number.isFinite(previous) || previous <= 0) return null;
        return (current / previous) - 1;
      });
      const z = rollingZ(fiveDayReturns, 21);
      if (z === null) return ctx.baseWeights;
      if (z < threshold) {
        if (mode === 'to_bil') return holdTicker(SAFE_TICKER);
        if (mode === 'half_bil') return blendWithBil(ctx.baseWeights, 0.5);
      }
      return ctx.baseWeights;
    },
  };
}

// 2. Sector momentum sleeve blended with base PYM.
function strategySectorMomentum({ blendShare, topN, momWindow }) {
  const id = `sector_mom_top${topN}_${momWindow}d_blend${Math.round(blendShare * 100)}`;
  return {
    id,
    family: 'sector_momentum',
    name: `Sector top-${topN} ${momWindow}d, blend ${Math.round(blendShare * 100)}% with base`,
    description: `Each day score sector ETFs (${SECTOR_TICKERS.length}) by ${momWindow}-day return, equal-weight the top ${topN}, blend ${Math.round(blendShare * 100)}% sector + ${Math.round((1 - blendShare) * 100)}% base PYM.`,
    fn: (ctx) => {
      const idx = ctx.signalIndex;
      const scored = SECTOR_TICKERS.map((ticker) => {
        const closes = ctx.market.closes.get(ticker);
        return { ticker, score: closes ? priceReturn(closes, idx, momWindow) : null };
      }).filter((row) => Number.isFinite(row.score));
      if (scored.length < topN) return ctx.baseWeights;
      scored.sort((a, b) => b.score - a.score);
      const top = scored.slice(0, topN);
      const sectorWeights = new Map(top.map((row) => [row.ticker, 1 / top.length]));
      return blendWeights(ctx.baseWeights, sectorWeights, blendShare);
    },
  };
}

// 3. RSI horizon-diversified gate using SPY at three horizons.
function strategyRsiHorizonGate({ rsi50Floor, rsi2Cap, defenseScale, capScale }) {
  const id = `rsi_horizon_floor${rsi50Floor}_cap${rsi2Cap}_def${Math.round(defenseScale * 100)}_cap${Math.round(capScale * 100)}`;
  return {
    id,
    family: 'rsi_horizon',
    name: `RSI horizon gate: SPY RSI(50)>${rsi50Floor}, RSI(2)<${rsi2Cap}, def=${defenseScale}, cap=${capScale}`,
    description: `Defensive (${Math.round(defenseScale * 100)}% PYM, rest BIL) when SPY RSI(50)<${rsi50Floor}; capped (${Math.round(capScale * 100)}% PYM, rest BIL) when SPY RSI(2)>${rsi2Cap}; otherwise full PYM.`,
    fn: (ctx) => {
      const closes = ctx.market.closes.get('SPY');
      if (!closes) return ctx.baseWeights;
      const idx = ctx.signalIndex;
      const rsi50 = rsiWilder(closes, idx, 50);
      const rsi2 = rsiWilder(closes, idx, 2);
      if (Number.isFinite(rsi50) && rsi50 < rsi50Floor) return blendWithBil(ctx.baseWeights, defenseScale);
      if (Number.isFinite(rsi2) && rsi2 > rsi2Cap) return blendWithBil(ctx.baseWeights, capScale);
      return ctx.baseWeights;
    },
  };
}

// 4. Breadth filter using % of sector ETFs above their SMA(50) as a proxy.
function strategyBreadthFilter({ lowThreshold, midThreshold, midScale }) {
  const id = `breadth_low${lowThreshold}_mid${midThreshold}_scale${Math.round(midScale * 100)}`;
  return {
    id,
    family: 'breadth_filter',
    name: `Breadth filter: <${lowThreshold}% → BIL, <${midThreshold}% → ${Math.round(midScale * 100)}% PYM`,
    description: `Compute % of ${SECTOR_TICKERS.length} sector ETFs above their SMA(50). When breadth < ${lowThreshold}% rotate fully to BIL; when below ${midThreshold}% scale base PYM to ${Math.round(midScale * 100)}% with the rest in BIL.`,
    fn: (ctx) => {
      const idx = ctx.signalIndex;
      let above = 0;
      let counted = 0;
      SECTOR_TICKERS.forEach((ticker) => {
        const closes = ctx.market.closes.get(ticker);
        if (!closes) return;
        const value = closes[idx];
        const average = sma(closes, idx, 50);
        if (Number.isFinite(value) && Number.isFinite(average)) {
          counted += 1;
          if (value > average) above += 1;
        }
      });
      if (counted < 6) return ctx.baseWeights;
      const breadthPct = (above / counted) * 100;
      if (breadthPct < lowThreshold) return holdTicker(SAFE_TICKER);
      if (breadthPct < midThreshold) return blendWithBil(ctx.baseWeights, midScale);
      return ctx.baseWeights;
    },
  };
}

function trailingSleeveSharpes(sleeveDailyReturns, idx, lookback) {
  return sleeveDailyReturns.map((dailySeries) => {
    const start = Math.max(1, idx - lookback + 1);
    const slice = dailySeries.slice(start, idx + 1);
    if (slice.length < Math.max(10, Math.floor(lookback / 3))) return 0;
    const mean = slice.reduce((sum, value) => sum + value, 0) / slice.length;
    const variance = slice.reduce((sum, value) => sum + (value - mean) ** 2, 0) / slice.length;
    const std = Math.sqrt(variance);
    if (std === 0) return 0;
    return (mean * 252) / (std * Math.sqrt(252));
  });
}

function applyMetaWeights(sleeves, meta) {
  const merged = new Map();
  sleeves.forEach((weights, sleeveIndex) => {
    weights.forEach((weight, ticker) => {
      merged.set(ticker, (merged.get(ticker) || 0) + weight * meta[sleeveIndex]);
    });
  });
  return cleanWeights(merged);
}

// 5. Meta-reweight the 8 base sub-strategies by trailing realized Sharpe.
function strategySleeveMeta({ lookback, floor }) {
  const id = `sleeve_meta_${lookback}d_floor${Math.round(floor * 1000)}bp`;
  return {
    id,
    family: 'sleeve_meta',
    name: `Sleeve meta-reweight by trailing ${lookback}d Sharpe (floor ${(floor * 100).toFixed(2)}%)`,
    description: `Each day reweight the 8 base PYM sub-strategies by their max(0, trailing ${lookback}-day Sharpe), with a per-sleeve floor of ${(floor * 100).toFixed(2)}%; sleeves below floor stay at floor and the rest is normalized.`,
    fn: (ctx) => {
      const idx = ctx.signalIndex;
      const sleeves = ctx.subStrategyWeights[idx] || [];
      if (!sleeves.length) return ctx.baseWeights;
      const sharpes = trailingSleeveSharpes(ctx.sleeveDailyReturns, idx, lookback);
      const positive = sharpes.map((value) => Math.max(0, value));
      const sleeveCount = sleeves.length;
      const residual = Math.max(0, 1 - floor * sleeveCount);
      const positiveSum = positive.reduce((sum, value) => sum + value, 0);
      const meta = positive.map((value) => floor + (positiveSum > 0 ? (value / positiveSum) * residual : residual / sleeveCount));
      return applyMetaWeights(sleeves, meta);
    },
  };
}

// Sleeve-meta with no floor, but cap any single sleeve at maxWeight.
function strategySleeveMetaCap({ lookback, maxWeight }) {
  const id = `sleeve_meta_${lookback}d_cap${Math.round(maxWeight * 100)}`;
  return {
    id,
    family: 'sleeve_meta_dynamic',
    name: `Sleeve meta ${lookback}d, no floor, single-sleeve cap ${Math.round(maxWeight * 100)}%`,
    description: `Pure positive-Sharpe weighting, no floor, but no single sleeve exceeds ${Math.round(maxWeight * 100)}% — overflow redistributes proportionally to the remaining sleeves.`,
    fn: (ctx) => {
      const idx = ctx.signalIndex;
      const sleeves = ctx.subStrategyWeights[idx] || [];
      if (!sleeves.length) return ctx.baseWeights;
      const sharpes = trailingSleeveSharpes(ctx.sleeveDailyReturns, idx, lookback);
      const positive = sharpes.map((value) => Math.max(0, value));
      let total = positive.reduce((sum, value) => sum + value, 0);
      let weights;
      if (total <= 0) {
        weights = new Array(sleeves.length).fill(1 / sleeves.length);
      } else {
        weights = positive.map((value) => value / total);
        // Iteratively cap and redistribute.
        for (let iteration = 0; iteration < 10; iteration += 1) {
          const overflow = weights.map((value) => Math.max(0, value - maxWeight));
          const overflowSum = overflow.reduce((sum, value) => sum + value, 0);
          if (overflowSum < 1e-9) break;
          weights = weights.map((value) => Math.min(value, maxWeight));
          const eligible = weights.map((value) => (value < maxWeight - 1e-9 && positive[weights.indexOf(value)] > 0 ? 1 : 0));
          const eligibleSharpes = positive.map((value, sleeveIndex) => (weights[sleeveIndex] < maxWeight - 1e-9 ? value : 0));
          const eligibleSum = eligibleSharpes.reduce((sum, value) => sum + value, 0);
          if (eligibleSum <= 0) {
            const slack = 1 - weights.reduce((sum, value) => sum + value, 0);
            const eligibleCount = eligible.reduce((sum, value) => sum + value, 0);
            if (eligibleCount === 0) break;
            weights = weights.map((value, sleeveIndex) => value + (eligible[sleeveIndex] ? slack / eligibleCount : 0));
          } else {
            const slack = 1 - weights.reduce((sum, value) => sum + value, 0);
            weights = weights.map((value, sleeveIndex) => value + (eligibleSharpes[sleeveIndex] / eligibleSum) * slack);
          }
        }
      }
      return applyMetaWeights(sleeves, weights);
    },
  };
}

// Floor scales with how distinguishable the sleeves are.
// When all sleeves have similar Sharpe (no signal), use higher floor (closer to equal weight).
// When one sleeve clearly dominates, use lower floor (lean into the winner).
function strategySleeveMetaDispersion({ lookback, minFloor = 0, maxFloor = 0.125 }) {
  const id = `sleeve_meta_${lookback}d_disp_${Math.round(minFloor * 100)}_${Math.round(maxFloor * 100)}`;
  return {
    id,
    family: 'sleeve_meta_dynamic',
    name: `Sleeve meta ${lookback}d dispersion-aware floor [${(minFloor * 100).toFixed(1)}%, ${(maxFloor * 100).toFixed(1)}%]`,
    description: `Per-sleeve floor scales linearly between ${(maxFloor * 100).toFixed(1)}% (when sleeves are similar) and ${(minFloor * 100).toFixed(1)}% (when sleeves are very dispersed) using the coefficient of variation of positive trailing Sharpes.`,
    fn: (ctx) => {
      const idx = ctx.signalIndex;
      const sleeves = ctx.subStrategyWeights[idx] || [];
      if (!sleeves.length) return ctx.baseWeights;
      const sharpes = trailingSleeveSharpes(ctx.sleeveDailyReturns, idx, lookback);
      const positive = sharpes.map((value) => Math.max(0, value));
      const positiveValues = positive.filter((value) => value > 0);
      let dispersionScore = 0;
      if (positiveValues.length >= 2) {
        const mean = positiveValues.reduce((sum, value) => sum + value, 0) / positiveValues.length;
        const std = Math.sqrt(positiveValues.reduce((sum, value) => sum + (value - mean) ** 2, 0) / positiveValues.length);
        dispersionScore = mean > 0 ? Math.min(1, std / mean) : 0;
      }
      const floor = maxFloor - (maxFloor - minFloor) * dispersionScore;
      const sleeveCount = sleeves.length;
      const residual = Math.max(0, 1 - floor * sleeveCount);
      const positiveSum = positive.reduce((sum, value) => sum + value, 0);
      const meta = positive.map((value) => floor + (positiveSum > 0 ? (value / positiveSum) * residual : residual / sleeveCount));
      return applyMetaWeights(sleeves, meta);
    },
  };
}

// Walk-forward selector: each day pick the floor (from a discrete grid) that would have maximized
// trailing N-day realized portfolio return on prior data.
function strategySleeveMetaAutoFloor({ lookback, autoLookback = 63, floorGrid = [0, 0.025, 0.05, 0.075, 0.1] }) {
  const id = `sleeve_meta_${lookback}d_autofloor_${autoLookback}d`;
  return {
    id,
    family: 'sleeve_meta_dynamic',
    name: `Sleeve meta ${lookback}d auto-floor (${autoLookback}d walk-forward selector)`,
    description: `Each day choose the per-sleeve floor from {${floorGrid.map((f) => (f * 100).toFixed(1) + '%').join(', ')}} that would have produced the highest trailing ${autoLookback}-day Sharpe on prior data; apply that floor to the sleeve-meta-${lookback}d mixer.`,
    fn: (ctx) => {
      const idx = ctx.signalIndex;
      const sleeves = ctx.subStrategyWeights[idx] || [];
      if (!sleeves.length) return ctx.baseWeights;
      const start = Math.max(1, idx - autoLookback + 1);
      const evalDays = [];
      for (let d = start; d <= idx; d += 1) evalDays.push(d);
      let bestFloor = floorGrid[0];
      let bestScore = -Infinity;
      floorGrid.forEach((floor) => {
        const dailyReturns = [];
        evalDays.forEach((d) => {
          const sharpesAtD = trailingSleeveSharpes(ctx.sleeveDailyReturns, d - 1, lookback);
          const positive = sharpesAtD.map((value) => Math.max(0, value));
          const positiveSum = positive.reduce((sum, value) => sum + value, 0);
          const sleeveCount = ctx.sleeveDailyReturns.length;
          const residual = Math.max(0, 1 - floor * sleeveCount);
          const meta = positive.map((value) => floor + (positiveSum > 0 ? (value / positiveSum) * residual : residual / sleeveCount));
          let portReturn = 0;
          ctx.sleeveDailyReturns.forEach((series, sleeveIndex) => {
            portReturn += meta[sleeveIndex] * (series[d] || 0);
          });
          dailyReturns.push(portReturn);
        });
        if (!dailyReturns.length) return;
        const mean = dailyReturns.reduce((sum, value) => sum + value, 0) / dailyReturns.length;
        const variance = dailyReturns.reduce((sum, value) => sum + (value - mean) ** 2, 0) / dailyReturns.length;
        const std = Math.sqrt(variance);
        const score = std > 0 ? (mean * 252) / (std * Math.sqrt(252)) : 0;
        if (score > bestScore) {
          bestScore = score;
          bestFloor = floor;
        }
      });
      const sharpes = trailingSleeveSharpes(ctx.sleeveDailyReturns, idx, lookback);
      const positive = sharpes.map((value) => Math.max(0, value));
      const positiveSum = positive.reduce((sum, value) => sum + value, 0);
      const sleeveCount = sleeves.length;
      const residual = Math.max(0, 1 - bestFloor * sleeveCount);
      const meta = positive.map((value) => bestFloor + (positiveSum > 0 ? (value / positiveSum) * residual : residual / sleeveCount));
      return applyMetaWeights(sleeves, meta);
    },
  };
}

function defaultStrategies() {
  return [
    strategyBase(),
    strategySpyBuyHold(),
    strategyBilCash(),
    // Credit overlay: HYG/LQD and HYG/TLT pairs at multiple thresholds and modes.
    strategyCreditSpread({ ratioPair: ['HYG', 'LQD'], threshold: -1, mode: 'to_bil' }),
    strategyCreditSpread({ ratioPair: ['HYG', 'LQD'], threshold: -1.5, mode: 'to_bil' }),
    strategyCreditSpread({ ratioPair: ['HYG', 'LQD'], threshold: -1, mode: 'half_bil' }),
    strategyCreditSpread({ ratioPair: ['HYG', 'TLT'], threshold: -1, mode: 'to_bil' }),
    strategyCreditSpread({ ratioPair: ['JNK', 'LQD'], threshold: -1, mode: 'to_bil' }),
    // Sector momentum sleeve.
    strategySectorMomentum({ blendShare: 0.2, topN: 3, momWindow: 21 }),
    strategySectorMomentum({ blendShare: 0.2, topN: 3, momWindow: 63 }),
    strategySectorMomentum({ blendShare: 0.2, topN: 3, momWindow: 126 }),
    strategySectorMomentum({ blendShare: 0.3, topN: 3, momWindow: 63 }),
    strategySectorMomentum({ blendShare: 0.5, topN: 3, momWindow: 63 }),
    // RSI horizon gate variants.
    strategyRsiHorizonGate({ rsi50Floor: 35, rsi2Cap: 95, defenseScale: 0.4, capScale: 0.5 }),
    strategyRsiHorizonGate({ rsi50Floor: 30, rsi2Cap: 95, defenseScale: 0.3, capScale: 0.5 }),
    strategyRsiHorizonGate({ rsi50Floor: 40, rsi2Cap: 90, defenseScale: 0.5, capScale: 0.5 }),
    // Breadth filter variants.
    strategyBreadthFilter({ lowThreshold: 25, midThreshold: 45, midScale: 0.6 }),
    strategyBreadthFilter({ lowThreshold: 30, midThreshold: 50, midScale: 0.5 }),
    strategyBreadthFilter({ lowThreshold: 20, midThreshold: 40, midScale: 0.7 }),
    // Sleeve meta-reweighting variants.
    strategySleeveMeta({ lookback: 21, floor: 0.05 }),
    strategySleeveMeta({ lookback: 21, floor: 0.0 }),
    strategySleeveMeta({ lookback: 63, floor: 0.05 }),
    strategySleeveMeta({ lookback: 63, floor: 0.025 }),
    strategySleeveMeta({ lookback: 126, floor: 0.05 }),
  ];
}

function weightTurnover(previous, next) {
  const keys = new Set([...previous.keys(), ...next.keys()]);
  let turnover = 0;
  keys.forEach((ticker) => {
    turnover += Math.abs((next.get(ticker) || 0) - (previous.get(ticker) || 0));
  });
  return turnover;
}

function maxDrawdown(equityCurve) {
  let peak = equityCurve[0]?.equity || 1;
  let drawdown = 0;
  equityCurve.forEach((point) => {
    if (point.equity > peak) peak = point.equity;
    if (peak > 0) drawdown = Math.min(drawdown, (point.equity / peak) - 1);
  });
  return drawdown;
}

function summarizeState(state, initialCapital) {
  const { dailyReturns, equityCurve, totalTurnover } = state;
  const totalReturn = (state.equity / initialCapital) - 1;
  const days = dailyReturns.length;
  const avgDaily = days ? dailyReturns.reduce((sum, value) => sum + value, 0) / days : 0;
  const variance = days ? dailyReturns.reduce((sum, value) => sum + (value - avgDaily) ** 2, 0) / days : 0;
  const std = Math.sqrt(variance);
  const annualizedVolatility = std * Math.sqrt(252);
  const cagr = days ? ((1 + totalReturn) ** (252 / days)) - 1 : 0;
  return {
    id: state.strategy.id,
    family: state.strategy.family,
    name: state.strategy.name,
    description: state.strategy.description || '',
    finalEquity: state.equity,
    totalReturn,
    totalReturnPct: totalReturn * 100,
    cagr,
    cagrPct: cagr * 100,
    maxDrawdown: maxDrawdown(equityCurve),
    maxDrawdownPct: maxDrawdown(equityCurve) * 100,
    annualizedVolatilityPct: annualizedVolatility * 100,
    sharpe: annualizedVolatility > 0 ? (avgDaily * 252) / annualizedVolatility : 0,
    averageDailyTurnoverPct: days ? (totalTurnover / days) * 100 : 0,
    tradingDays: days,
    winRatePct: days ? (dailyReturns.filter((value) => value > 0).length / days) * 100 : 0,
  };
}

function runExtensionStrategiesSuite({
  primaryDailyBarsPath,
  extraDailyBarsPath,
  scorePath,
  startDate = '2025-01-02',
  endDate = null,
  initialCapital = 10000,
  costBps = 2,
  rsiMode = 'wilder',
  strategies = defaultStrategies(),
}) {
  if (!fs.existsSync(primaryDailyBarsPath)) throw new Error(`missing_primary_bars:${primaryDailyBarsPath}`);
  if (!fs.existsSync(scorePath)) throw new Error(`missing_score:${scorePath}`);
  const market = mergeDailyBars(primaryDailyBarsPath, extraDailyBarsPath);
  const score = JSON.parse(fs.readFileSync(scorePath, 'utf8'));
  const ctx = precomputeContext(market, score, rsiMode);
  const states = strategies.map((strategy) => ({
    strategy,
    equity: initialCapital,
    previousWeights: new Map(),
    dailyReturns: [],
    equityCurve: [],
    totalTurnover: 0,
  }));
  const finalEndDate = endDate || market.dates.at(-2);
  let firstSignalIndex = null;
  let lastRealizedIndex = null;
  for (let signalIndex = 0; signalIndex < market.dates.length - 1; signalIndex += 1) {
    const signalDate = market.dates[signalIndex];
    if (signalDate < startDate || signalDate > finalEndDate) continue;
    const baseWeights = ctx.baseWeights[signalIndex];
    const subStrategyWeights = ctx.subStrategyWeights;
    const sleeveDailyReturns = ctx.sleeveDailyReturns;
    states.forEach((state) => {
      const desired = cleanWeights(state.strategy.fn({
        market,
        signalIndex,
        signalDate,
        baseWeights,
        subStrategyWeights,
        sleeveDailyReturns,
      }));
      const turnover = weightTurnover(state.previousWeights, desired);
      let grossReturn = 0;
      desired.forEach((weight, ticker) => {
        const r = tickerReturn(market.closes, ticker, signalIndex + 1);
        if (r !== null) grossReturn += weight * r;
      });
      const netReturn = grossReturn - (turnover * costBps / 10000);
      state.equity *= (1 + netReturn);
      state.totalTurnover += turnover;
      state.dailyReturns.push(netReturn);
      state.equityCurve.push({ date: market.dates[signalIndex + 1], equity: state.equity, dailyReturn: netReturn });
      state.previousWeights = desired;
    });
    if (firstSignalIndex === null) firstSignalIndex = signalIndex;
    lastRealizedIndex = signalIndex + 1;
  }
  const summaries = states.map((state) => summarizeState(state, initialCapital));
  const benchmarks = firstSignalIndex === null ? {} : {
    spy: priceReturn(market.closes.get('SPY') || [], lastRealizedIndex, lastRealizedIndex - firstSignalIndex),
    qqq: priceReturn(market.closes.get('QQQ') || [], lastRealizedIndex, lastRealizedIndex - firstSignalIndex),
  };
  return {
    generatedAt: new Date().toISOString(),
    settings: { startDate, endDate: finalEndDate, costBps, initialCapital, rsiMode },
    source: { primaryDailyBarsPath, extraDailyBarsPath, scorePath },
    benchmarks,
    summaries: summaries.slice().sort((left, right) => right.sharpe - left.sharpe),
    summariesByReturn: summaries.slice().sort((left, right) => right.totalReturn - left.totalReturn),
    strategies: states.map((state) => ({
      summary: summarizeState(state, initialCapital),
      equityCurve: state.equityCurve,
    })),
  };
}

function pct(value) {
  return Number.isFinite(value) ? value * 100 : null;
}

function weightsToHoldingsArray(weights, equity, previousWeights) {
  return [...weights.entries()]
    .map(([ticker, weight]) => ({
      ticker,
      weight,
      weightPct: pct(weight),
      previousWeight: previousWeights.get(ticker) || 0,
      weightChange: weight - (previousWeights.get(ticker) || 0),
      weightChangePct: pct(weight - (previousWeights.get(ticker) || 0)),
      dollars: Number.isFinite(equity) ? equity * weight : null,
    }))
    .sort((left, right) => right.weight - left.weight || left.ticker.localeCompare(right.ticker));
}

function topHoldingsLabel(holdings, limit = 4) {
  return holdings.slice(0, limit).map((holding) => holding.ticker).join(', ');
}

function benchmarkPoint(market, ticker, baseIndex, index) {
  const closes = market.closes.get(ticker);
  if (!closes) return null;
  const base = closes[baseIndex];
  const current = closes[index];
  if (!Number.isFinite(base) || !Number.isFinite(current) || base <= 0) return null;
  return (current / base) - 1;
}

function buildExtensionRebalanceReport({
  market,
  score,
  strategy,
  startDate = '2025-01-02',
  endDate = null,
  rsiMode = 'wilder',
  initialCapital = 10000,
  transactionCostBps = 1,
  slippageBps = 1,
  source = {},
  generatedAt = new Date().toISOString(),
  ctx = null,
}) {
  if (!market?.dates?.length) throw new Error('missing_market_dates');
  if (!strategy?.fn) throw new Error('missing_strategy_fn');
  const finalEnd = endDate || market.dates.at(-1);
  const totalCostBps = (transactionCostBps || 0) + (slippageBps || 0);
  const computed = ctx || precomputeContext(market, score, rsiMode);
  const startIndex = market.dates.findIndex((date) => date >= startDate);
  if (startIndex === -1) throw new Error(`no_market_dates_on_or_after:${startDate}`);
  const snapshots = [];
  const equitySeries = [];
  let equity = initialCapital;
  let previousWeights = new Map();
  let totalTurnover = 0;
  let investedDays = 0;
  let firstSignalIndex = null;
  for (let index = startIndex; index < market.dates.length; index += 1) {
    const date = market.dates[index];
    if (date > finalEnd) break;
    const desired = cleanWeights(strategy.fn({
      market,
      signalIndex: index,
      signalDate: date,
      baseWeights: computed.baseWeights[index],
      subStrategyWeights: computed.subStrategyWeights,
      sleeveDailyReturns: computed.sleeveDailyReturns,
    }));
    if (firstSignalIndex === null) firstSignalIndex = index;
    const holdings = weightsToHoldingsArray(desired, equity, previousWeights);
    const grossExposure = [...desired.values()].reduce((sum, weight) => sum + weight, 0);
    const turnover = (() => {
      const keys = new Set([...previousWeights.keys(), ...desired.keys()]);
      let value = 0;
      keys.forEach((ticker) => { value += Math.abs((desired.get(ticker) || 0) - (previousWeights.get(ticker) || 0)); });
      return value;
    })();
    const costReturn = turnover * totalCostBps / 10000;
    const nextDate = market.dates[index + 1] || null;
    const snapshot = {
      date,
      rebalanceDate: date,
      execution: 'eod_close',
      nextDate,
      equityBeforeNextSession: equity,
      grossExposure,
      turnover,
      turnoverPct: pct(turnover),
      estimatedRebalanceCost: equity * costReturn,
      estimatedRebalanceCostPct: pct(costReturn),
      holdings,
      topHoldings: topHoldingsLabel(holdings),
      benchmarkReturns: {
        spy: benchmarkPoint(market, 'SPY', startIndex, index),
        qqq: benchmarkPoint(market, 'QQQ', startIndex, index),
      },
      realized: null,
    };
    if (nextDate) {
      let grossReturn = 0;
      let missingCount = 0;
      desired.forEach((weight, ticker) => {
        const r = tickerReturn(market.closes, ticker, index + 1);
        if (r === null) missingCount += 1; else grossReturn += weight * r;
      });
      const netReturn = grossReturn - costReturn;
      const startEquity = equity;
      equity *= (1 + netReturn);
      totalTurnover += turnover;
      if (grossExposure > 0.001) investedDays += 1;
      snapshot.realized = {
        date: nextDate,
        startEquity,
        endEquity: equity,
        grossReturn,
        grossReturnPct: pct(grossReturn),
        netReturn,
        netReturnPct: pct(netReturn),
        costReturn,
        costReturnPct: pct(costReturn),
        missingReturnCount: missingCount,
      };
      equitySeries.push({
        date: nextDate,
        signalDate: date,
        equity,
        totalReturn: (equity / initialCapital) - 1,
        spyReturn: benchmarkPoint(market, 'SPY', startIndex, index + 1),
        qqqReturn: benchmarkPoint(market, 'QQQ', startIndex, index + 1),
      });
    }
    snapshots.push(snapshot);
    previousWeights = desired;
  }
  const completed = snapshots.filter((snapshot) => snapshot.realized);
  const latest = snapshots.at(-1);
  const latestCompleted = completed.at(-1) || null;
  const finalEquity = latestCompleted?.realized?.endEquity ?? initialCapital;
  const totalReturn = (finalEquity / initialCapital) - 1;
  let peak = initialCapital;
  let maxDd = 0;
  equitySeries.forEach((point) => {
    if (point.equity > peak) peak = point.equity;
    if (peak > 0) maxDd = Math.min(maxDd, (point.equity / peak) - 1);
  });
  return {
    generatedAt,
    source: { ...source, strategyId: strategy.id, strategyFamily: strategy.family, strategyName: strategy.name },
    settings: {
      startDate,
      endDate: finalEnd,
      rsiMode,
      timing: 'signal_eod_close_then_next_close',
      initialCapital,
      transactionCostBps,
      slippageBps,
    },
    summary: {
      startDate,
      firstRebalanceDate: snapshots[0]?.date || null,
      latestRebalanceDate: latest?.date || null,
      latestCompletedDate: latestCompleted?.realized?.date || null,
      snapshots: snapshots.length,
      completedSessions: completed.length,
      finalEquity,
      totalReturn,
      totalReturnPct: pct(totalReturn),
      maxDrawdown: maxDd,
      maxDrawdownPct: pct(maxDd),
      investedDays,
      investedShare: completed.length ? investedDays / completed.length : 0,
      averageDailyTurnover: completed.length ? totalTurnover / completed.length : 0,
      spyReturn: equitySeries.at(-1)?.spyReturn ?? null,
      qqqReturn: equitySeries.at(-1)?.qqqReturn ?? null,
    },
    latest,
    snapshots,
    equitySeries,
  };
}

// Apply a stress overlay on top of an inner strategy. The overlay scales
// gross exposure by `scaleFn(stress)` and routes the slack to BIL.
// `stressByDate` is a Map<signalDate, stressNumber>; days with no stress
// signal pass through unchanged. `scaleFn(stress)` should return a number
// in [0, 1.5] (1.0 = full position, <1 = defensive, >1 = leveraged).
function strategyWithStressOverlay({ id, name, family, description, innerStrategy, stressByDate, scaleFn, safeTicker = SAFE_TICKER }) {
  return {
    id,
    name: name || `${innerStrategy.name} + stress overlay`,
    family: family || 'stress_overlay',
    description: description || `${innerStrategy.name} with a stress-derived gross-exposure overlay; slack routed to ${safeTicker}.`,
    fn: (ctx) => {
      const innerWeights = innerStrategy.fn(ctx);
      const stress = stressByDate.get(ctx.signalDate);
      const scale = (stress != null && Number.isFinite(stress)) ? scaleFn(stress) : 1.0;
      if (scale >= 0.999) return innerWeights;
      const out = new Map();
      innerWeights.forEach((weight, ticker) => out.set(ticker, weight * scale));
      out.set(safeTicker, (out.get(safeTicker) || 0) + Math.max(0, 1 - scale));
      return cleanWeights(out);
    },
  };
}

// Default "aggressive" scale function — used by the stress strategy.
//   stress < 0      → 1.0 (full)
//   stress in [0,1] → linear 1.0 → 0.6
//   stress in [1,2] → linear 0.6 → 0.2
//   stress > 2      → 0.2 (max defensive)
function aggressiveStressScale(stress) {
  if (!Number.isFinite(stress) || stress < 0) return 1.0;
  if (stress < 1) return 1.0 - 0.4 * stress;
  if (stress < 2) return 0.6 - 0.4 * (stress - 1);
  return 0.2;
}

// Blend an inner strategy's daily weights with an external map of holdings
// keyed by signalDate. Used to mix the cap25 sleeve-meta strategy with an ML
// model's daily holdings drawn from a precomputed walk-forward artifact.
function strategyBlendWithExternal({ id, name, family, description, innerStrategy, externalWeightsByDate, blendWeight }) {
  return {
    id,
    name: name || `${innerStrategy.name} blended ${Math.round(blendWeight * 100)}% with external`,
    family: family || 'blend_with_external',
    description: description || `${innerStrategy.name} blended ${Math.round((1 - blendWeight) * 100)}% with external daily holdings at ${Math.round(blendWeight * 100)}%.`,
    fn: (ctx) => {
      const innerWeights = innerStrategy.fn(ctx);
      const external = externalWeightsByDate.get(ctx.signalDate);
      if (!external || !Object.keys(external).length) return innerWeights;
      const merged = new Map();
      innerWeights.forEach((weight, ticker) => {
        merged.set(ticker, weight * (1 - blendWeight));
      });
      Object.entries(external).forEach(([ticker, weight]) => {
        if (!Number.isFinite(weight) || weight <= 0) return;
        merged.set(ticker, (merged.get(ticker) || 0) + weight * blendWeight);
      });
      return cleanWeights(merged);
    },
  };
}

module.exports = {
  SECTOR_TICKERS,
  SAFE_TICKER,
  aggressiveStressScale,
  buildExtensionRebalanceReport,
  defaultStrategies,
  mergeDailyBars,
  precomputeContext,
  runExtensionStrategiesSuite,
  strategyBlendWithExternal,
  strategyWithStressOverlay,
  strategySleeveMeta,
  strategySleeveMetaCap,
  strategySleeveMetaDispersion,
  strategySleeveMetaAutoFloor,
  strategyCreditSpread,
  strategySectorMomentum,
  strategyBreadthFilter,
  strategyRsiHorizonGate,
  summarizeState,
};
