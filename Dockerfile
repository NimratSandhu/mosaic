FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PREFECT_HOME=/opt/prefect \
    PYTHONPATH=/app/src

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    unzip \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Install pip-tools first
RUN python -m pip install --upgrade pip pip-tools

COPY requirements.in requirements.txt ./
RUN pip-sync requirements.txt

COPY . .

CMD ["python", "-m", "flows.ingest_prices"]

