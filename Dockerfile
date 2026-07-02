# Two-stage build: compile the SPA with Node, then serve it (and the API) from
# the Python collector. The result is a single always-on container: FastAPI
# serves /api + /ingest + /metrics and the built dashboard at /.

# ---- stage 1: build the SPA ----
FROM node:22-slim AS web
WORKDIR /web
COPY web/package.json web/package-lock.json* ./
RUN npm install
COPY web/ ./
RUN npm run build

# ---- stage 2: python collector ----
FROM python:3.12-slim AS app
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app
# Install only the deps needed to run the collector (project + collector extra).
COPY pyproject.toml README.md ./
COPY src/ ./src/
RUN uv pip install --system --no-cache ".[collector]"

# Bring in the built dashboard.
COPY --from=web /web/dist ./web/dist

# Persist the SQLite DB on a volume so runs survive restarts.
ENV DASKGENIE_DB=/data/daskgenie.db \
    DASKGENIE_STATIC_DIR=/app/web/dist \
    DASKGENIE_HOST=0.0.0.0 \
    DASKGENIE_PORT=8765
VOLUME /data
EXPOSE 8765

CMD ["python", "-m", "daskgenie.collector"]
