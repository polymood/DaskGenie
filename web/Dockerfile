# DaskGenie dashboard (Next.js, standalone output). Reads COLLECTOR_URL at
# runtime to reach the collector service.

FROM node:22-slim AS build
WORKDIR /web
COPY package.json package-lock.json* ./
RUN npm ci || npm install
COPY . .
RUN npm run build

FROM node:22-slim
WORKDIR /web
ENV NODE_ENV=production \
    PORT=3000 \
    HOSTNAME=0.0.0.0
COPY --from=build /web/.next/standalone ./
COPY --from=build /web/.next/static ./.next/static
EXPOSE 3000
CMD ["node", "server.js"]
