FROM ghcr.io/astral-sh/uv:0.6-python3.12-bookworm-slim AS builder
WORKDIR /build
COPY pyproject.toml uv.lock* ./
COPY app/ ./app/
RUN uv pip install --system --no-cache .

FROM python:3.12-slim-bookworm AS runtime
RUN groupadd -r appuser && useradd -r -g appuser -d /app -s /sbin/nologin appuser
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /usr/local/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin
COPY app/ ./app/
USER appuser
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]