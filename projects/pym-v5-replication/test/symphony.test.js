const { compareValues, evaluateSymphony } = require('../src/symphony');

describe('Composer rule-tree evaluator', () => {
  it('routes if/else children in order', () => {
    const score = {
      step: 'if',
      children: [
        {
          step: 'if-child',
          'lhs-fn': 'current-price',
          'lhs-val': 'SPY',
          comparator: 'gt',
          'rhs-fixed-value?': true,
          'rhs-val': '100',
          children: [{ step: 'asset', ticker: 'TQQQ' }],
        },
        {
          step: 'if-child',
          'is-else-condition?': true,
          children: [{ step: 'asset', ticker: 'BIL' }],
        },
      ],
    };
    const market = { closes: new Map([['SPY', [99, 101]]]) };
    expect(Object.fromEntries(evaluateSymphony(score, market, 0))).toEqual({ BIL: 1 });
    expect(Object.fromEntries(evaluateSymphony(score, market, 1))).toEqual({ TQQQ: 1 });
  });

  it('normalizes equal-weight filters over selected assets', () => {
    const score = {
      step: 'filter',
      'select-fn': 'top',
      'select-n': '2',
      'sort-by-fn': 'cumulative-return',
      'sort-by-fn-params': { window: 1 },
      children: [
        { step: 'asset', ticker: 'AAA' },
        { step: 'asset', ticker: 'BBB' },
        { step: 'asset', ticker: 'CCC' },
      ],
    };
    const market = {
      closes: new Map([
        ['AAA', [100, 110]],
        ['BBB', [100, 101]],
        ['CCC', [100, 120]],
      ]),
    };
    expect(Object.fromEntries(evaluateSymphony(score, market, 1))).toEqual({ CCC: 0.5, AAA: 0.5 });
  });

  it('supports Composer comparators', () => {
    expect(compareValues(2, 'gt', 1)).toBe(true);
    expect(compareValues(2, 'gte', 2)).toBe(true);
    expect(compareValues(1, 'lt', 2)).toBe(true);
    expect(compareValues(2, 'lte', 2)).toBe(true);
  });
});
