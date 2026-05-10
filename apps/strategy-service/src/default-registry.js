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
} = require('./strategies/pym-v5-extension');

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
    createWheelOptionIncomeStrategy(options.wheelOptionIncome || {}),
    createTsllSecondsPassiveScalperStrategy(options.tsllSecondsPassiveScalper || {}),
  ]);
}

module.exports = {
  createDefaultRegistry,
};
