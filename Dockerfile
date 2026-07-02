# Collector service: JSON API + Prometheus /metrics + SQLite store. The
# dashboard is a separate Next.js container (see web/Dockerfile).

FROM python:3.12-slim
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app
COPY pyproject.toml README.md ./
COPY src/ ./src/
RUN uv pip install --system --no-cache ".[collector]"

# Persist the SQLite DB on a volume so runs survive restarts.
ENV DASKGENIE_DB=/data/daskgenie.db \
    DASKGENIE_HOST=0.0.0.0 \
    DASKGENIE_PORT=8765
VOLUME /data
EXPOSE 8765

CMD ["python", "-m", "daskgenie.collector"]
