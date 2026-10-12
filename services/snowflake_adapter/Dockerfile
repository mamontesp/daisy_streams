FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Copy dependency specifications
COPY requirements.txt pyproject.toml ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy service source code and scripts
COPY snowflake_adapter/ ./snowflake_adapter/
COPY scripts/ ./scripts/

ENV PYTHONPATH=/app \
    PYTHONUNBUFFERED=1

CMD ["python", "scripts/run_adapter.py", "--loop", "--interval-sec", "60"]
