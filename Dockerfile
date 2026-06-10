FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PORT=8080

ARG APP_VERSION=
ARG GIT_SHA=unknown
ARG BUILD_TIME=unknown
ENV APP_VERSION=$APP_VERSION \
    GIT_SHA=$GIT_SHA \
    BUILD_TIME=$BUILD_TIME

RUN apt-get update && apt-get install -y --no-install-recommends curl gosu && rm -rf /var/lib/apt/lists/*

# Non-root account for the long-lived app process (blast-radius reduction).
RUN useradd --system --create-home --uid 10001 appuser

WORKDIR /app

COPY backend/requirements.txt ./backend/requirements.txt
RUN pip install --no-cache-dir -r backend/requirements.txt

COPY package.json ./package.json
COPY backend ./backend
COPY frontend ./frontend
COPY docker-entrypoint.sh ./docker-entrypoint.sh
RUN chmod +x ./docker-entrypoint.sh && chown -R appuser:appuser /app

EXPOSE 8080
# Entrypoint starts as root only to set up the mounted volume, then drops to
# the unprivileged `appuser` (via gosu) before exec'ing uvicorn.
CMD ["./docker-entrypoint.sh"]
