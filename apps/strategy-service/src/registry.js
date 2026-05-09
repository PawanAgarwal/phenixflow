class StrategyRegistry {
  constructor(strategies = []) {
    this.strategies = new Map();
    strategies.forEach((strategy) => this.register(strategy));
  }

  register(strategy) {
    const metadata = strategy.getMetadata();
    if (!metadata?.id) throw new Error('strategy_missing_id');
    this.strategies.set(metadata.id, strategy);
  }

  listStrategies() {
    return [...this.strategies.values()].map((strategy) => strategy.getMetadata());
  }

  getStrategy(id) {
    const strategy = this.strategies.get(String(id || '').trim());
    if (!strategy) {
      const error = new Error(`unknown_strategy:${id}`);
      error.statusCode = 404;
      error.code = 'unknown_strategy';
      throw error;
    }
    return strategy;
  }
}

module.exports = {
  StrategyRegistry,
};
