# phenixflow

Monorepo for Project phenixflow.

## Layout

- `apps/flow-api`: the main options-flow backend app and its operational scripts.
- `packages/clickhouse-core`: shared ClickHouse client helpers for repo projects.
- `packages/theta-client`: shared ThetaData client and response parsing helpers.
- `infra/clickhouse`: ClickHouse config and machine-level administration scripts.
- `projects/vixregime`: SPX/VIX regime research project.
- `projects/podcast-prediction`: podcast extraction and prediction workflows.
- `projects/yieldmax`: YieldMax-related screening and analysis outputs.
- `docs`: shared architecture and operational documentation.
- `artifacts`, `data`, `output`: repo-level runtime assets and generated outputs.

## Common Commands

```bash
npm start
npm test
npm run lint
npm run clickhouse:start
npm run vixregime:check
```

The root `package.json` keeps the repo-level entry points stable while the app and project code live under their own folders.
