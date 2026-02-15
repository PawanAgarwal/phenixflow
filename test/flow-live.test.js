const { createFlowLiveController } = require('../src/flow-live');

class FakeEventSource {
  constructor(url) {
    this.url = url;
    this.closed = false;
    this.onmessage = null;
    this.onerror = null;
  }

  close() {
    this.closed = true;
  }

  emitMessage(payload) {
    if (this.onmessage) {
      this.onmessage({ data: JSON.stringify(payload) });
    }
  }

  emitError() {
    if (this.onerror) {
      this.onerror(new Error('stream disconnected'));
    }
  }
}

describe('flow live SSE controller', () => {
  it('subscribes to SSE and applies incremental rows', () => {
    const streams = [];
    const controller = createFlowLiveController({
      eventSourceFactory: (url) => {
        const stream = new FakeEventSource(url);
        streams.push(stream);
        return stream;
      },
    });

    controller.start();

    streams[0].emitMessage({
      watermark: 101,
      events: [{ eventType: 'flow.updated', flow: { id: 'flow_1', symbol: 'AAPL', status: 'open' } }],
    });
    streams[0].emitMessage({
      watermark: 102,
      events: [{ eventType: 'flow.updated', flow: { id: 'flow_2', symbol: 'NVDA', status: 'closed' } }],
    });
    streams[0].emitMessage({
      watermark: 103,
      events: [{ eventType: 'flow.updated', flow: { id: 'flow_1', symbol: 'AAPL', status: 'closed' } }],
    });

    const state = controller.getState();
    expect(state.watermark).toBe(103);
    expect(state.rows).toEqual([
      { id: 'flow_1', symbol: 'AAPL', status: 'closed' },
      { id: 'flow_2', symbol: 'NVDA', status: 'closed' },
    ]);
  });

  it('reconnects with last watermark and ignores duplicate/replayed watermarks', () => {
    const streams = [];
    const pendingTimers = [];
    const controller = createFlowLiveController({
      baseUrl: '/api/flow/stream?mode=sse',
      reconnectDelayMs: 10,
      setTimeoutFn: (fn) => {
        pendingTimers.push(fn);
        return fn;
      },
      clearTimeoutFn: () => {},
      eventSourceFactory: (url) => {
        const stream = new FakeEventSource(url);
        streams.push(stream);
        return stream;
      },
    });

    controller.start();
    streams[0].emitMessage({
      watermark: 200,
      events: [{ eventType: 'flow.updated', flow: { id: 'flow_1', symbol: 'AAPL', status: 'open' } }],
    });

    streams[0].emitError();
    pendingTimers.shift()();

    expect(streams[1].url).toContain('watermark=200');

    streams[1].emitMessage({
      watermark: 200,
      events: [{ eventType: 'flow.updated', flow: { id: 'flow_1', symbol: 'AAPL', status: 'closed' } }],
    });
    streams[1].emitMessage({
      watermark: 201,
      events: [{ eventType: 'flow.updated', flow: { id: 'flow_1', symbol: 'AAPL', status: 'closed' } }],
    });

    const state = controller.getState();
    expect(state.watermark).toBe(201);
    expect(state.rows).toEqual([{ id: 'flow_1', symbol: 'AAPL', status: 'closed' }]);
  });

  it('keeps table rows and facets consistent while streaming', () => {
    const streams = [];
    const controller = createFlowLiveController({
      eventSourceFactory: (url) => {
        const stream = new FakeEventSource(url);
        streams.push(stream);
        return stream;
      },
    });

    controller.start();
    streams[0].emitMessage({
      watermark: 1,
      events: [
        { eventType: 'flow.updated', flow: { id: 'flow_1', symbol: 'AAPL', status: 'open' } },
        { eventType: 'flow.updated', flow: { id: 'flow_2', symbol: 'AAPL', status: 'closed' } },
        { eventType: 'flow.updated', flow: { id: 'flow_3', symbol: 'TSLA', status: 'open' } },
      ],
    });

    streams[0].emitMessage({
      watermark: 2,
      events: [{ eventType: 'flow.updated', flow: { id: 'flow_3', symbol: 'TSLA', status: 'closed' } }],
    });

    const state = controller.getState();
    expect(state.rows).toHaveLength(3);
    expect(state.facets).toEqual({
      symbol: { AAPL: 2, TSLA: 1 },
      status: { open: 1, closed: 2 },
    });
  });
});