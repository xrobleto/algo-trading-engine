# Horizon Engine — Railway deployment image.
#
# Deploy: create a Railway service whose Root Directory is `horizon/` so this
# Dockerfile and railway.json apply. Mount a volume at /data — ALGO_OUTPUT_DIR
# is set to /data below so the ownership ledger and engine state survive
# restarts. Set secrets (POLYGON_API_KEY, ALPACA_*, SMTP_*, etc.) as Railway
# environment variables — never bake them into the image.

FROM python:3.12-slim

WORKDIR /app
ENV PYTHONUNBUFFERED=1 \
    ALGO_OUTPUT_DIR=/data

COPY requirements.txt /app/horizon/requirements.txt
RUN pip install --no-cache-dir -r /app/horizon/requirements.txt

COPY . /app/horizon/

# Defaults to DRY-RUN (logs orders, submits nothing). To trade live, override
# the start command to add --live and set LIVE_TRADING=1 +
# I_UNDERSTAND_LIVE_TRADING=YES in the Railway environment.
CMD ["python", "-m", "horizon.engine.main", "--daily"]
