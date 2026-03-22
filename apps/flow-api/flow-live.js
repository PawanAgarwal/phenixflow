function buildReconnectUrl(baseUrl, watermark) {
  if (watermark === null || watermark === undefined) {
    return baseUrl;
  }

  const separator = baseUrl.includes('?') ? '&' : '?';
  return `${baseUrl}${separator}watermark=${encodeURIComponent(String(watermark))}`;
}

function normalizePayload(raw) {
  if (!raw || typeof raw !== 'object') {
    return { watermark: null, events: [] };
  }

  if (Array.isArray(raw.events)) {
    return {
      watermark: raw.watermark ?? null,
      events: raw.events,
    };
  }

  return {
    watermark: raw.watermark ?? null,
    events: [raw],
  };
}

function buildFacets(rows) {
  const facets = { symbol: {}, status: {} };

  rows.forEach((row) => {
    facets.symbol[row.symbol] = (facets.symbol[row.symbol] || 0) + 1;
    facets.status[row.status] = (facets.status[row.status] || 0) + 1;
  });

  return facets;
}

function createFlowLiveController(options = {}) {
  const {
    baseUrl = '/api/flow/stream',
    eventSourceFactory = (url) => new EventSource(url),
    reconnectDelayMs = 250,
    setTimeoutFn = setTimeout,
    clearTimeoutFn = clearTimeout,
  } = options;

  const rowsById = new Map();
  let facets = { symbol: {}, status: {} };
  let lastWatermark = null;
  let eventSource = null;
  let reconnectTimer = null;
  let started = false;

  const refreshFacets = () => {
    facets = buildFacets(Array.from(rowsById.values()));
  };

  const applyPayload = (payload) => {
    const { watermark, events } = normalizePayload(payload);

    if (watermark !== null && watermark !== undefined && lastWatermark !== null && watermark <= lastWatermark) {
      return;
    }

    events.forEach((event) => {
      if (!event || event.eventType !== 'flow.updated' || !event.flow || !event.flow.id) return;
      rowsById.set(event.flow.id, event.flow);
    });

    if (watermark !== null && watermark !== undefined) {
      lastWatermark = watermark;
    }
    refreshFacets();
  };

  const connect = () => {
    const url = buildReconnectUrl(baseUrl, lastWatermark);
    eventSource = eventSourceFactory(url);

    eventSource.onmessage = (message) => {
      try {
        const payload = JSON.parse(message.data);
        applyPayload(payload);
      } catch {
        // Ignore malformed SSE messages and continue streaming.
      }
    };

    eventSource.onerror = () => {
      if (!started) return;
      if (eventSource && typeof eventSource.close === 'function') {
        eventSource.close();
      }
      reconnectTimer = setTimeoutFn(connect, reconnectDelayMs);
    };
  };

  return {
    start() {
      if (started) return;
      started = true;
      connect();
    },

    stop() {
      started = false;
      if (reconnectTimer) {
        clearTimeoutFn(reconnectTimer);
        reconnectTimer = null;
      }
      if (eventSource && typeof eventSource.close === 'function') {
        eventSource.close();
      }
      eventSource = null;
    },

    getState() {
      const rows = Array.from(rowsById.values());
      return {
        rows,
        facets,
        watermark: lastWatermark,
      };
    },
  };
}

module.exports = { createFlowLiveController, buildReconnectUrl };