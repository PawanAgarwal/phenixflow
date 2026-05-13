# TSLL Seconds Passive Scalper Kernel

Executable strategy kernel for the promoted TSLL seconds passive limit scalper.

The kernel is deterministic and owns only feature/filter evaluation, strategy
state, entry decisions, position rule registration, cooldown/session state, and
trace generation. It does not read market data, files, credentials, HTTP
services, brokers, or wall-clock time. All inputs arrive as normalized events.

Promoted baseline:

- Report: `projects/tsll-scalping/reports/tsll-seconds-passive-fixed-2025-01-02-2026-05-12.json`
- Source artifact: `projects/tsll-scalping/artifacts/tsll-seconds-passive-mm-fixed-2025-01-02-2026-05-12-cost0.json`
- Dataset: `tsll-1s-2025-01-02-2026-05-12-massive-rest-1s-barSeconds1-nodaily`
- Cost: `0` cents per side
- Fill adapter: `ohlc_1s_proxy.v1`
- Kernel version: `tsll-seconds-passive-scalper.execution.v1`
- PhenixFlow SHA: `c573cb91a87edde7d1be68e2756d79ab3033876c`

The complete executable default settings are pinned in `settings/default.json`.
Artifact builds and fixture replay use that file as the only default.

Useful commands:

```bash
node packages/strategy-kernels/tsll-seconds-passive-scalper/scripts/generate-fixtures.js
node packages/strategy-kernels/tsll-seconds-passive-scalper/scripts/build-artifact.js
node packages/strategy-kernels/tsll-seconds-passive-scalper/scripts/replay-fixtures.js
```
