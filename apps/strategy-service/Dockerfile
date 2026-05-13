FROM node:20-slim

WORKDIR /app

ENV NODE_ENV=production
ENV STRATEGY_SERVICE_PORT=3120
ENV PYM_V5_ML_PYTHON=/opt/pym-v5-ml/bin/python

COPY package.json package-lock.json ./
RUN npm ci --omit=dev

RUN apt-get update \
  && apt-get install -y --no-install-recommends ca-certificates curl unzip python3 python3-venv libgomp1 \
  && arch="$(dpkg --print-architecture)" \
  && case "$arch" in amd64) duckdb_arch="amd64" ;; arm64) duckdb_arch="arm64" ;; *) echo "unsupported duckdb arch: $arch" >&2; exit 1 ;; esac \
  && curl -fsSL -o /tmp/duckdb.zip "https://github.com/duckdb/duckdb/releases/download/v1.3.2/duckdb_cli-linux-${duckdb_arch}.zip" \
  && unzip -q /tmp/duckdb.zip -d /usr/local/bin \
  && chmod +x /usr/local/bin/duckdb \
  && rm -f /tmp/duckdb.zip \
  && rm -rf /var/lib/apt/lists/* \
  && python3 -m venv /opt/pym-v5-ml \
  && /opt/pym-v5-ml/bin/pip install --no-cache-dir --upgrade pip \
  && /opt/pym-v5-ml/bin/pip install --no-cache-dir numpy scikit-learn lightgbm

COPY apps/strategy-service ./apps/strategy-service
COPY packages/strategy-kernels ./packages/strategy-kernels
COPY projects/pym-v5-replication/config ./projects/pym-v5-replication/config
COPY projects/pym-v5-replication/scripts/build-massive-eod-daily-bars.js ./projects/pym-v5-replication/scripts/build-massive-eod-daily-bars.js
COPY projects/pym-v5-replication/scripts/build-option-features.js ./projects/pym-v5-replication/scripts/build-option-features.js
COPY projects/pym-v5-replication/scripts/refresh-eod-inputs.js ./projects/pym-v5-replication/scripts/refresh-eod-inputs.js
COPY projects/pym-v5-replication/src ./projects/pym-v5-replication/src
COPY projects/pym-v5-ml-experiments/scripts/build-options-stress-signal.js ./projects/pym-v5-ml-experiments/scripts/build-options-stress-signal.js
COPY projects/pym-v5-ml-experiments/scripts/export-walkforward-dataset.js ./projects/pym-v5-ml-experiments/scripts/export-walkforward-dataset.js
COPY projects/pym-v5-ml-experiments/scripts/refresh-lgbm-artifact.js ./projects/pym-v5-ml-experiments/scripts/refresh-lgbm-artifact.js
COPY projects/pym-v5-ml-experiments/python ./projects/pym-v5-ml-experiments/python
COPY projects/pym-v5-ml-experiments/src ./projects/pym-v5-ml-experiments/src
COPY projects/spy-intraday-prediction/config ./projects/spy-intraday-prediction/config
COPY projects/spy-intraday-prediction/scripts/backtest-wheel-strategy.js ./projects/spy-intraday-prediction/scripts/backtest-wheel-strategy.js
COPY projects/spy-intraday-prediction/scripts/refresh-wheel-option-income.js ./projects/spy-intraday-prediction/scripts/refresh-wheel-option-income.js
COPY projects/spy-intraday-prediction/scripts/build-pym-gated-artifacts.js ./projects/spy-intraday-prediction/scripts/build-pym-gated-artifacts.js
COPY projects/spy-intraday-prediction/scripts/build-features-1m.js ./projects/spy-intraday-prediction/scripts/build-features-1m.js
COPY projects/spy-intraday-prediction/scripts/build-greeks-1m.js ./projects/spy-intraday-prediction/scripts/build-greeks-1m.js
COPY projects/spy-intraday-prediction/scripts/build-trade-flow-1m.js ./projects/spy-intraday-prediction/scripts/build-trade-flow-1m.js
COPY projects/spy-intraday-prediction/src ./projects/spy-intraday-prediction/src
COPY projects/pym-v5-replication/scripts/run-backtest.js ./projects/pym-v5-replication/scripts/run-backtest.js
COPY apps/flow-api/config/top100-universe.json ./apps/flow-api/config/top100-universe.json
COPY projects/tsll-scalping/config ./projects/tsll-scalping/config
COPY projects/tsll-scalping/scripts/run-second-passive-mm.js ./projects/tsll-scalping/scripts/run-second-passive-mm.js
COPY projects/tsll-scalping/scripts/export-second-passive-report.js ./projects/tsll-scalping/scripts/export-second-passive-report.js
COPY projects/tsll-scalping/scripts/refresh-second-passive-report.js ./projects/tsll-scalping/scripts/refresh-second-passive-report.js
COPY projects/tsll-scalping/src ./projects/tsll-scalping/src
COPY projects/tsll-scalping/artifacts ./projects/tsll-scalping/artifacts
COPY projects/tsll-scalping/reports ./projects/tsll-scalping/reports

EXPOSE 3120

CMD ["node", "apps/strategy-service/server.js"]
