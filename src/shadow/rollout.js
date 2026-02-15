const { oldFilter, newFilter } = require('./filters');

function diffArrays(oldArr, newArr) {
  const oldSet = new Set(oldArr);
  const newSet = new Set(newArr);

  const removedByNew = [...oldSet].filter((item) => !newSet.has(item));
  const addedByNew = [...newSet].filter((item) => !oldSet.has(item));

  return {
    removedByNew,
    addedByNew,
  };
}

function validateSessions(sessions) {
  if (!Array.isArray(sessions) || sessions.length < 3) {
    throw new Error('Shadow mode requires at least 3 market sessions.');
  }
}

function runShadowRollout(sessions) {
  validateSessions(sessions);

  const perSession = sessions.map((session) => {
    const oldOutput = oldFilter(session.symbols);
    const newOutput = newFilter(session.symbols);
    const diff = diffArrays(oldOutput, newOutput);

    return {
      sessionId: session.sessionId,
      marketDate: session.marketDate,
      oldOutput,
      newOutput,
      diff,
      oldCount: oldOutput.length,
      newCount: newOutput.length,
      removedCount: diff.removedByNew.length,
      addedCount: diff.addedByNew.length,
    };
  });

  const summary = perSession.reduce(
    (acc, session) => {
      acc.totalSessions += 1;
      acc.totalOld += session.oldCount;
      acc.totalNew += session.newCount;
      acc.totalRemoved += session.removedCount;
      acc.totalAdded += session.addedCount;
      return acc;
    },
    {
      totalSessions: 0,
      totalOld: 0,
      totalNew: 0,
      totalRemoved: 0,
      totalAdded: 0,
    }
  );

  return {
    generatedAt: new Date().toISOString(),
    mode: 'shadow',
    summary,
    sessions: perSession,
  };
}

module.exports = {
  runShadowRollout,
  validateSessions,
  diffArrays,
};
