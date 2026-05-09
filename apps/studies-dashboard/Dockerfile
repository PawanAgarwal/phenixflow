FROM node:20-slim

WORKDIR /app

ENV NODE_ENV=production
ENV STUDIES_DASHBOARD_PORT=3130

COPY package.json package-lock.json ./
RUN npm ci --omit=dev

COPY apps/studies-dashboard ./apps/studies-dashboard

EXPOSE 3130

CMD ["node", "apps/studies-dashboard/server.js"]
