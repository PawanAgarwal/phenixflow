FROM node:20-slim

WORKDIR /app

ENV NODE_ENV=production
ENV STRATEGY_SERVICE_PORT=3120
ENV PYM_V5_ML_PYTHON=/opt/pym-v5-ml/bin/python

COPY package.json package-lock.json ./
RUN npm ci --omit=dev

RUN apt-get update \
  && apt-get install -y --no-install-recommends python3 python3-venv libgomp1 \
  && rm -rf /var/lib/apt/lists/* \
  && python3 -m venv /opt/pym-v5-ml \
  && /opt/pym-v5-ml/bin/pip install --no-cache-dir --upgrade pip \
  && /opt/pym-v5-ml/bin/pip install --no-cache-dir numpy scikit-learn lightgbm

COPY apps/strategy-service ./apps/strategy-service
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
COPY projects/tsll-scalping/reports ./projects/tsll-scalping/reports

EXPOSE 3120

CMD ["node", "apps/strategy-service/server.js"]
