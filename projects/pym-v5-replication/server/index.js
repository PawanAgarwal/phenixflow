#!/usr/bin/env node
const { createApp } = require('./app');

const port = Number(process.env.PORT || process.env.PYM_V5_PORT || 3117);
const app = createApp();

app.listen(port, () => {
  console.log(`pym-v5 rebalance service listening on http://localhost:${port}`);
});
