FROM python:3.11-slim

WORKDIR /app

# Install dependencies first (layer cache)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY strategies/ strategies/
COPY config/ config/
COPY utilities/ utilities/
COPY ai_manager/ ai_manager/

# Railway persistent volume mount point
ENV ALGO_OUTPUT_DIR=/data

# Run from strategies/ so relative imports work
WORKDIR /app/strategies
CMD ["python", "-m", "engine.main"]
