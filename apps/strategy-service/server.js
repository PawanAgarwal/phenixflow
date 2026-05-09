#!/usr/bin/env node
const { createApp } = require('./src/app');

const port = Number(process.env.PORT || process.env.STRATEGY_SERVICE_PORT || 3120);
const host = process.env.STRATEGY_SERVICE_HOST || '0.0.0.0';
const app = createApp();

app.listen(port, host, () => {
  console.log(`phenixflow strategy service listening on http://${host}:${port}`);
});
