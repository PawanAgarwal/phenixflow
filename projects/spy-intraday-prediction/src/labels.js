function rollingRealizedVol(dayRows, index, lookbackMinutes) {
  const start = Math.max(1, index - lookbackMinutes + 1);
  let sumSq = 0;
  let count = 0;
  for (let cursor = start; cursor <= index; cursor += 1) {
    const current = dayRows[cursor]?.spy_close;
    const previous = dayRows[cursor - 1]?.spy_close;
    if (Number.isFinite(current) && Number.isFinite(previous) && previous > 0) {
      const ret = (current / previous) - 1;
      sumSq += ret * ret;
      count += 1;
    }
  }
  return count ? Math.sqrt(sumSq) : 0;
}

function assignFixedForwardLabels(dayRows, horizons) {
  const eodClose = dayRows.length ? dayRows[dayRows.length - 1].spy_close : null;
  dayRows.forEach((row, index) => {
    horizons.forEach((horizon) => {
      if (horizon.minutes === 'eod') {
        const usable = index < dayRows.length - 1 && row.spy_close > 0 && eodClose > 0;
        row[`label_${horizon.name}_close`] = usable ? eodClose : null;
        row[`label_${horizon.name}_return`] = usable ? (eodClose / row.spy_close) - 1 : null;
        return;
      }
      const future = dayRows[index + Number(horizon.minutes)];
      const usable = future && row.spy_close > 0 && future.spy_close > 0;
      row[`label_${horizon.name}_close`] = usable ? future.spy_close : null;
      row[`label_${horizon.name}_return`] = usable ? (future.spy_close / row.spy_close) - 1 : null;
    });
  });
}

function assignMagnitudeLabels(dayRows) {
  const windows = [
    ['5m', 5],
    ['30m', 30],
    ['60m', 60],
  ];
  dayRows.forEach((row, index) => {
    windows.forEach(([name, minutes]) => {
      const future = dayRows[index + minutes];
      const usable = future && row.spy_close > 0 && future.spy_close > 0;
      const ret = usable ? (future.spy_close / row.spy_close) - 1 : null;
      row[`label_abs_return_${name}_return`] = ret === null ? null : Math.abs(ret);
      row[`label_abs_return_${name}_direction`] = ret === null ? null : (Math.abs(ret) > 0 ? 1 : 0);
    });
    const eodRet = row.label_eod_close_return;
    row.label_abs_return_eod_return = Number.isFinite(eodRet) ? Math.abs(eodRet) : null;
    row.label_abs_return_eod_direction = Number.isFinite(eodRet) && Math.abs(eodRet) > 0 ? 1 : null;
  });
}

function assignLastThirtyLabels(dayRows, entryMinuteEt) {
  const eodClose = dayRows.length ? dayRows[dayRows.length - 1].spy_close : null;
  dayRows.forEach((row) => {
    const isEntry = row.minuteOfDayEt === entryMinuteEt;
    const usable = isEntry && row.spy_close > 0 && eodClose > 0;
    row.label_last_30m_close = usable ? eodClose : null;
    row.label_last_30m_return = usable ? (eodClose / row.spy_close) - 1 : null;
  });
}

function assignTripleBarrierLabels(dayRows, settings = {}) {
  const horizons = settings.horizons || [5, 30, 60];
  const volatilityMultiple = Number(settings.volatilityMultiple || 1.25);
  const minimumBarrier = Number(settings.minimumBarrierBps || 2) / 10_000;
  const lookback = Number(settings.volatilityLookbackMinutes || 30);

  dayRows.forEach((row, index) => {
    const baseVol = rollingRealizedVol(dayRows, index, lookback);
    const barrier = Math.max(minimumBarrier, baseVol * volatilityMultiple);
    horizons.forEach((minutes) => {
      let hit = 'vertical';
      let exitIndex = Math.min(dayRows.length - 1, index + minutes);
      let exitClose = dayRows[exitIndex]?.spy_close;
      if (!(row.spy_close > 0) || index + 1 >= dayRows.length) {
        row[`label_tb_${minutes}m_direction`] = null;
        row[`label_tb_${minutes}m_return`] = null;
        row[`label_tb_${minutes}m_hit`] = null;
        row[`label_tb_${minutes}m_profitable_long`] = null;
        return;
      }
      const upper = row.spy_close * (1 + barrier);
      const lower = row.spy_close * (1 - barrier);
      for (let cursor = index + 1; cursor <= Math.min(dayRows.length - 1, index + minutes); cursor += 1) {
        const future = dayRows[cursor];
        if (Number.isFinite(future.high) && future.high >= upper) {
          hit = 'upper';
          exitIndex = cursor;
          exitClose = upper;
          break;
        }
        if (Number.isFinite(future.low) && future.low <= lower) {
          hit = 'lower';
          exitIndex = cursor;
          exitClose = lower;
          break;
        }
      }
      const usable = exitIndex > index && Number.isFinite(exitClose) && exitClose > 0;
      const ret = usable ? (exitClose / row.spy_close) - 1 : null;
      let direction = null;
      if (hit === 'upper') direction = 1;
      else if (hit === 'lower') direction = -1;
      else if (ret !== null) direction = ret > 0 ? 1 : (ret < 0 ? -1 : 0);
      row[`label_tb_${minutes}m_direction`] = direction;
      row[`label_tb_${minutes}m_return`] = ret;
      row[`label_tb_${minutes}m_hit`] = usable ? hit : null;
      row[`label_tb_${minutes}m_profitable_long`] = usable ? (ret > 0 ? 1 : 0) : null;
    });
  });
}

function assignForwardLabels(rows, horizons, settings = {}) {
  const rowsByDay = new Map();
  rows.forEach((row) => {
    const dayRows = rowsByDay.get(row.tradeDate) || [];
    dayRows.push(row);
    rowsByDay.set(row.tradeDate, dayRows);
  });

  rowsByDay.forEach((dayRows) => {
    dayRows.sort((left, right) => left.minuteMs - right.minuteMs);
    assignFixedForwardLabels(dayRows, horizons);
    assignMagnitudeLabels(dayRows);
    assignLastThirtyLabels(dayRows, settings.lastThirtyEntryMinuteEt || 930);
    assignTripleBarrierLabels(dayRows, settings.tripleBarrier);
  });

  return rows;
}

module.exports = {
  rollingRealizedVol,
  assignForwardLabels,
  assignLastThirtyLabels,
  assignTripleBarrierLabels,
};
