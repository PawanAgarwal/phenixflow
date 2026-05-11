FROM node:20-slim

WORKDIR /app

ENV NODE_ENV=production
ENV STRATEGY_SERVICE_PORT=3120

COPY package.json package-lock.json ./
RUN npm ci --omit=dev

COPY apps/strategy-service ./apps/strategy-service
COPY projects/pym-v5-replication/config ./projects/pym-v5-replication/config
COPY projects/pym-v5-replication/scripts/build-massive-eod-daily-bars.js ./projects/pym-v5-replication/scripts/build-massive-eod-daily-bars.js
COPY projects/pym-v5-replication/scripts/build-option-features.js ./projects/pym-v5-replication/scripts/build-option-features.js
COPY projects/pym-v5-replication/scripts/refresh-eod-inputs.js ./projects/pym-v5-replication/scripts/refresh-eod-inputs.js
COPY projects/pym-v5-replication/src ./projects/pym-v5-replication/src
COPY projects/pym-v5-ml-experiments/scripts/build-options-stress-signal.js ./projects/pym-v5-ml-experiments/scripts/build-options-stress-signal.js
COPY projects/tsll-scalping/reports ./projects/tsll-scalping/reports

EXPOSE 3120

CMD ["node", "apps/strategy-service/server.js"]
