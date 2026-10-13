FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Install consumer dependencies
COPY services/consumer/requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy required service modules
COPY services/postgres/ ./services/postgres/
COPY services/consumer/ ./services/consumer/

ENV PYTHONPATH=/app \
    PYTHONUNBUFFERED=1

CMD ["python", "services/consumer/scripts/run_consumer.py", "--loop"]
