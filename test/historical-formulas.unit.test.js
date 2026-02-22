const {
  computeExecutionFlags,
  computeSentiment,
  isStandardMonthly,
  computeValue,
  computeDte,
  computeSigScore,
  isAmSpikeWindow,
  isSweep,
  isMultilegByCode,
  computeOtmNormBellCurve,
  computeMinuteOfDayEt,
  computeTimeNorm,
  computeIvSkewNorm,
} = require('../src/historical-formulas');

describe('historical formulas', () => {
  it('classifies bid/ask/AA boundaries correctly', () => {
    const aa = computeExecutionFlags({ right: 'CALL', price: 1.13, bid: 1.0, ask: 1.1 });
    const ask = computeExecutionFlags({ right: 'CALL', price: 1.1, bid: 1.0, ask: 1.1 });
    const bid = computeExecutionFlags({ right: 'PUT', price: 1.0, bid: 1.0, ask: 1.1 });

    expect(aa).toMatchObject({ aa: true, ask: false, bid: false, calls: true });
    expect(ask).toMatchObject({ aa: false, ask: true, bid: false });
    expect(bid).toMatchObject({ aa: false, ask: false, bid: true, puts: true });
  });

  it('maps right + side to sentiment', () => {
    expect(computeSentiment({ right: 'CALL', executionSide: 'ASK' })).toBe('bullish');
    expect(computeSentiment({ right: 'PUT', executionSide: 'ASK' })).toBe('bearish');
    expect(computeSentiment({ right: 'CALL', executionSide: 'OTHER' })).toBe('neutral');
  });

  it('detects standard monthly options and weeklies', () => {
    expect(isStandardMonthly('2026-02-20')).toBe(true);
    expect(isStandardMonthly('2026-02-13')).toBe(false);
  });

  it('computes deterministic numeric formulas', () => {
    expect(computeValue(1.25, 100)).toBe(12500);
    expect(computeDte('2026-02-13T14:35:00.000Z', '2026-02-20')).toBeGreaterThanOrEqual(6);

    const score = computeSigScore({ valuePctile: 0.8, volOiNorm: 0.5, repeatNorm: 1, otmNorm: 0.2, sideConfidence: 0.9, dteNorm: 0.5, spreadNorm: 0.3 });
    expect(score).toBeGreaterThan(0.4);
    expect(score).toBeLessThan(1);

    // Verify new components contribute to score
    const scoreWithoutNew = computeSigScore({ valuePctile: 0.8, volOiNorm: 0.5, repeatNorm: 1, otmNorm: 0.2, sideConfidence: 0.9, dteNorm: 0, spreadNorm: 0 });
    const scoreWithNew = computeSigScore({ valuePctile: 0.8, volOiNorm: 0.5, repeatNorm: 1, otmNorm: 0.2, sideConfidence: 0.9, dteNorm: 1, spreadNorm: 1 });
    expect(scoreWithNew).toBeGreaterThan(scoreWithoutNew);
  });

  it('detects AM spike window in ET', () => {
    expect(isAmSpikeWindow('2026-02-13T14:45:00.000Z')).toBe(true);
    expect(isAmSpikeWindow('2026-02-13T17:45:00.000Z')).toBe(false);
  });

  it('detects sweep condition codes', () => {
    expect(isSweep('95')).toBe(true);
    expect(isSweep('126')).toBe(true);
    expect(isSweep('128')).toBe(true);
    expect(isSweep(95)).toBe(true);
    expect(isSweep(' 95 ')).toBe(true);
    expect(isSweep('18')).toBe(false);
    expect(isSweep('130')).toBe(false);
    expect(isSweep(null)).toBe(false);
    expect(isSweep(undefined)).toBe(false);
  });

  it('detects multileg condition codes 130-143', () => {
    expect(isMultilegByCode('130')).toBe(true);
    expect(isMultilegByCode('143')).toBe(true);
    expect(isMultilegByCode('136')).toBe(true);
    expect(isMultilegByCode(135)).toBe(true);
    expect(isMultilegByCode('129')).toBe(false);
    expect(isMultilegByCode('144')).toBe(false);
    expect(isMultilegByCode('18')).toBe(false);
    expect(isMultilegByCode(null)).toBe(false);
    expect(isMultilegByCode(undefined)).toBe(false);
  });

  it('computes OTM bell curve peaking at 10%', () => {
    const at10 = computeOtmNormBellCurve(10);
    const at5 = computeOtmNormBellCurve(5);
    const at20 = computeOtmNormBellCurve(20);
    const at30 = computeOtmNormBellCurve(30);

    expect(at10).toBe(1.0);
    expect(at5).toBeCloseTo(0.7788, 3);
    expect(at20).toBeCloseTo(0.3679, 3);
    expect(at30).toBeLessThan(0.05);

    // ITM returns 0
    expect(computeOtmNormBellCurve(0)).toBe(0);
    expect(computeOtmNormBellCurve(-5)).toBe(0);
    expect(computeOtmNormBellCurve(null)).toBe(0);
    expect(computeOtmNormBellCurve(undefined)).toBe(0);
  });

  it('computes minute of day in ET', () => {
    // 14:45 UTC in winter = 9:45 ET = 585 minutes
    const minute = computeMinuteOfDayEt('2026-02-13T14:45:00.000Z');
    expect(minute).toBe(585);

    // Invalid input
    expect(computeMinuteOfDayEt('invalid')).toBe(null);
    expect(computeMinuteOfDayEt(null)).toBe(null);
  });

  it('sweep boosts sigScore and multileg penalizes', () => {
    const base = {
      valuePctile: 0.8,
      volOiNorm: 0.5,
      repeatNorm: 0.5,
      otmNorm: 0.5,
      sideConfidence: 0.8,
      dteNorm: 0.5,
      spreadNorm: 0.3,
      sweepNorm: 0,
      multilegNorm: 0,
      timeNorm: 0,
      deltaNorm: 0,
      ivSkewNorm: 0,
    };

    const baseScore = computeSigScore(base);
    const sweepScore = computeSigScore({ ...base, sweepNorm: 1 });
    const multilegScore = computeSigScore({ ...base, multilegNorm: 1 });

    expect(sweepScore).toBeGreaterThan(baseScore);
    expect(multilegScore).toBeLessThan(baseScore);

    // Sweep adds 0.12, multileg subtracts 0.12
    expect(sweepScore - baseScore).toBeCloseTo(0.12, 5);
    expect(baseScore - multilegScore).toBeCloseTo(0.12, 5);

    // Positive weights sum to 0.96 — max score without penalty is 0.96
    const maxScore = computeSigScore({
      valuePctile: 1, volOiNorm: 1, repeatNorm: 1, otmNorm: 1,
      sideConfidence: 1, dteNorm: 1, spreadNorm: 1, sweepNorm: 1, multilegNorm: 0,
      timeNorm: 1, deltaNorm: 1, ivSkewNorm: 1,
    });
    expect(maxScore).toBe(0.96);

    const minScore = computeSigScore({
      valuePctile: 0, volOiNorm: 0, repeatNorm: 0, otmNorm: 0,
      sideConfidence: 0, dteNorm: 0, spreadNorm: 0, sweepNorm: 0, multilegNorm: 1,
      timeNorm: 0, deltaNorm: 0, ivSkewNorm: 0,
    });
    expect(minScore).toBe(0);
  });

  it('computeTimeNorm peaks at 10:45 ET and decays', () => {
    // Peak at minute 645 (10:45 ET)
    expect(computeTimeNorm(645)).toBe(1.0);

    // Decay at 10:00 ET (minute 600) and 11:30 ET (minute 690) — 45 min from peak, exp(-1) ≈ 0.3679
    expect(computeTimeNorm(600)).toBeCloseTo(0.3679, 3);
    expect(computeTimeNorm(690)).toBeCloseTo(0.3679, 3);

    // Low at open (570 = 75 min from peak, exp(-2.78) ≈ 0.062), near zero at close
    expect(computeTimeNorm(570)).toBeLessThan(0.1);
    expect(computeTimeNorm(960)).toBeLessThan(0.001);

    // Zero outside market hours
    expect(computeTimeNorm(569)).toBe(0);
    expect(computeTimeNorm(961)).toBe(0);
    expect(computeTimeNorm(0)).toBe(0);

    // Null/invalid handling
    expect(computeTimeNorm(null)).toBe(0);
    expect(computeTimeNorm(undefined)).toBe(0);
    expect(computeTimeNorm(NaN)).toBe(0);
  });

  it('computeIvSkewNorm measures call/put IV divergence', () => {
    // Symmetric — no skew
    expect(computeIvSkewNorm(0.30, 0.30)).toBe(0);

    // Moderate skew
    const skew = computeIvSkewNorm(0.35, 0.25);
    expect(skew).toBeCloseTo(0.3333, 3);

    // Large skew — clamped to 1
    expect(computeIvSkewNorm(0.60, 0.10)).toBeLessThanOrEqual(1);
    expect(computeIvSkewNorm(0.60, 0.10)).toBeGreaterThan(0.9);

    // Null handling — returns 0 when either side missing
    expect(computeIvSkewNorm(null, 0.30)).toBe(0);
    expect(computeIvSkewNorm(0.30, null)).toBe(0);
    expect(computeIvSkewNorm(null, null)).toBe(0);

    // Zero avg returns 0
    expect(computeIvSkewNorm(0, 0)).toBe(0);
  });

  it('time/delta/iv components boost sigScore', () => {
    const base = {
      valuePctile: 0.5, volOiNorm: 0.5, repeatNorm: 0.5, otmNorm: 0.5,
      sideConfidence: 0.5, dteNorm: 0.5, spreadNorm: 0.5,
      sweepNorm: 0, multilegNorm: 0,
      timeNorm: 0, deltaNorm: 0, ivSkewNorm: 0,
    };

    const baseScore = computeSigScore(base);
    const withTime = computeSigScore({ ...base, timeNorm: 1 });
    const withDelta = computeSigScore({ ...base, deltaNorm: 1 });
    const withIvSkew = computeSigScore({ ...base, ivSkewNorm: 1 });
    const withAll = computeSigScore({ ...base, timeNorm: 1, deltaNorm: 1, ivSkewNorm: 1 });

    expect(withTime).toBeGreaterThan(baseScore);
    expect(withDelta).toBeGreaterThan(baseScore);
    expect(withIvSkew).toBeGreaterThan(baseScore);
    expect(withAll).toBeGreaterThan(withTime);

    // Verify individual contributions
    expect(withTime - baseScore).toBeCloseTo(0.07, 5);
    expect(withDelta - baseScore).toBeCloseTo(0.08, 5);
    expect(withIvSkew - baseScore).toBeCloseTo(0.06, 5);
  });
});
