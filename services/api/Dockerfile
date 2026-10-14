FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Install API dependencies
COPY services/api/requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy required service modules
COPY services/common/ ./services/common/
COPY services/postgres/ ./services/postgres/
COPY services/api/ ./services/api/

ENV PYTHONPATH=/app \
    PYTHONUNBUFFERED=1

EXPOSE 8000

HEALTHCHECK --interval=10s --timeout=3s --retries=3 \
    CMD curl -f http://localhost:8000/healthz || exit 1

CMD ["python", "services/api/scripts/run_api.py"]
