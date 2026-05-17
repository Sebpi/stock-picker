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

WORKDIR /app

COPY backend/requirements.txt ./backend/requirements.txt
RUN pip install --no-cache-dir -r backend/requirements.txt

COPY package.json ./package.json
COPY backend ./backend
COPY frontend ./frontend
COPY docker-entrypoint.sh ./docker-entrypoint.sh
RUN chmod +x ./docker-entrypoint.sh

EXPOSE 8080
CMD ["./docker-entrypoint.sh"]
