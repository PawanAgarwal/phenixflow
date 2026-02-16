#!/usr/bin/env node

const { createIngestWorker } = require('../src/ingest/worker');

const worker = createIngestWorker();
worker.start();

process.on('SIGINT', () => {
  worker.stop();
  process.exit(0);
});

process.on('SIGTERM', () => {
  worker.stop();
  process.exit(0);
});

console.log('ingest worker started', worker.getState());
