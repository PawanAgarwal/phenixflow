const { StrategyRegistry } = require('./registry');
const { createPymV5Strategy } = require('./strategies/pym-v5');
const {
  createPymV5OptionRankStrategy,
  createPymV5SpyPutPressureStrategy,
} = require('./strategies/pym-v5-option-rank');
const {
  createPymV5MlCalmTrendRouterStrategy,
  createPymV5MlOptionTop85050Strategy,
  createPymV5MlTwoSpeedStrategy,
  createPymV5TwoSpeedOptionMeta21Strategy,
} = require('./strategies/pym-v5-ml-artifact');
const { createTsllSecondsPassiveScalperStrategy } = require('./strategies/tsll-scalping-artifact');
const { createWheelOptionIncomeStrategy } = require('./strategies/wheel-option-income');
const {
  createPymV5SleeveMetaCapStrategy,
  createPymV5Cap25LgbmBlendStrategy,
  createPymV5Cap25LgbmBlendStressStrategy,
} = require('./strategies/pym-v5-extension');
const { createPymGatedIntradayStrategy } = require('./strategies/pym-gated-intraday');

function createDefaultRegistry(options = {}) {
  return new StrategyRegistry([
    createPymV5Strategy(options.pymV5 || {}),
    createPymV5OptionRankStrategy(options.pymV5OptionRank || {}),
    createPymV5MlTwoSpeedStrategy(options.pymV5MlTwoSpeed || {}),
    createPymV5MlCalmTrendRouterStrategy(options.pymV5MlCalmTrendRouter || {}),
    createPymV5MlOptionTop85050Strategy(options.pymV5MlOptionTop85050 || {}),
    createPymV5TwoSpeedOptionMeta21Strategy(options.pymV5TwoSpeedOptionMeta21 || {}),
    createPymV5SpyPutPressureStrategy(options.pymV5SpyPutPressure || {}),
    createPymV5SleeveMetaCapStrategy({ lookback: 21, maxWeight: 0.25, ...(options.pymV5SleeveMetaCap || {}) }),
    createPymV5Cap25LgbmBlendStrategy({ blendWeight: 0.40, ...(options.pymV5Cap25LgbmBlend || {}) }),
    createPymV5Cap25LgbmBlendStressStrategy({ blendWeight: 0.40, ...(options.pymV5Cap25LgbmBlendStress || {}) }),
    createWheelOptionIncomeStrategy(options.wheelOptionIncome || {}),
    createTsllSecondsPassiveScalperStrategy(options.tsllSecondsPassiveScalper || {}),
    // PYM-gated intraday strategies (4 leverage variants + 2 Phase 24 orthogonal survivors)
    createPymGatedIntradayStrategy({ variantId: 'pym-gated-intraday-baseline', ...(options.pymGatedBaseline || {}) }),
    createPymGatedIntradayStrategy({ variantId: 'pym-gated-intraday-lev3x', ...(options.pymGatedLev3x || {}) }),
    createPymGatedIntradayStrategy({ variantId: 'pym-gated-intraday-overnight-1x', ...(options.pymGatedOvernight1x || {}) }),
    createPymGatedIntradayStrategy({ variantId: 'pym-gated-intraday-best-combo', ...(options.pymGatedBestCombo || {}) }),
    createPymGatedIntradayStrategy({ variantId: 'pym-gated-intraday-tight-bias', ...(options.pymGatedTightBias || {}) }),
    createPymGatedIntradayStrategy({ variantId: 'pym-gated-intraday-flow-weighted', ...(options.pymGatedFlowWeighted || {}) }),
  ]);
}

module.exports = {
  createDefaultRegistry,
};
