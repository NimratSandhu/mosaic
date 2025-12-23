# Multi-stage build for optimized image size
FROM python:3.11-slim AS builder

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Install pip-tools and compile requirements
RUN python -m pip install --upgrade pip pip-tools
COPY requirements.in requirements.txt ./
RUN pip-sync requirements.txt

# Final stage - minimal runtime image
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PREFECT_HOME=/opt/prefect \
    PYTHONPATH=/app/src:/app \
    DUCKDB_THREADS=2 \
    PREFECT_API_REQUEST_TIMEOUT=30

WORKDIR /app

# Install only runtime dependencies (no build tools)
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# Copy Python packages from builder
COPY --from=builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

# Copy application code
COPY src/ /app/src/
COPY dash_app/ /app/dash_app/
COPY config/ /app/config/
COPY great_expectations/ /app/great_expectations/

# Copy scripts directory (must exist in build context)
COPY scripts/ /app/scripts/

# Create data directories
RUN mkdir -p /app/data/raw /app/data/curated /app/data/marts/duckdb

# Default command (can be overridden)
CMD ["python", "-m", "dash_app.app"]

# Expose dashboard port
EXPOSE 8050

# Health check for dashboard
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
    CMD curl -f http://localhost:8050/api/health || exit 1
