const { StrategyRegistry } = require('./registry');
const { createPymV5Strategy } = require('./strategies/pym-v5');

function createDefaultRegistry(options = {}) {
  return new StrategyRegistry([
    createPymV5Strategy(options.pymV5 || {}),
  ]);
}

module.exports = {
  createDefaultRegistry,
};
