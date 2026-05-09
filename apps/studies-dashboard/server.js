#!/usr/bin/env node
const { createApp } = require('./src/app');

const port = Number(process.env.PORT || process.env.STUDIES_DASHBOARD_PORT || 3130);
const host = process.env.STUDIES_DASHBOARD_HOST || '0.0.0.0';
const app = createApp();

app.listen(port, host, () => {
  console.log(`phenixflow studies dashboard listening on http://${host}:${port}`);
});
