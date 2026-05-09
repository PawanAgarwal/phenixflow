const { StrategyRegistry } = require('./registry');
const { createPymV5Strategy } = require('./strategies/pym-v5');
const { createPymV5OptionRankStrategy } = require('./strategies/pym-v5-option-rank');

function createDefaultRegistry(options = {}) {
  return new StrategyRegistry([
    createPymV5Strategy(options.pymV5 || {}),
    createPymV5OptionRankStrategy(options.pymV5OptionRank || {}),
  ]);
}

module.exports = {
  createDefaultRegistry,
};
