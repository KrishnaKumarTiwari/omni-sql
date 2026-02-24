# Multi-stage Dockerfile supporting both prototype and production gateways.
# Build: docker build -t omnisql .
# Run prototype:  docker run -p 8001:8001 omnisql python prototype/main.py
# Run production: docker run -p 8002:8002 omnisql (default CMD)

FROM python:3.11-slim

WORKDIR /app

# System deps for DuckDB native bindings + healthcheck
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Python deps first (Docker layer cache)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy full application
COPY . .

# Expose both gateways
EXPOSE 8001 8002

# Health check against production gateway
HEALTHCHECK --interval=30s --timeout=10s --retries=3 \
    CMD curl -f http://localhost:8002/health || exit 1

# Default: run production gateway on port 8002
ENV PYTHONPATH=/app
CMD ["python", "-m", "uvicorn", "omnisql.gateway.main:app", "--host", "0.0.0.0", "--port", "8002"]
