#!/bin/sh
set -eu

DATA_DIR="${DATA_DIR:-/app/data}"
mkdir -p "$DATA_DIR"

# The Fly volume mounts as root-owned; hand it to the unprivileged runtime user
# so the dropped-privilege uvicorn process can read/write state files.
APP_USER="${APP_USER:-appuser}"
if [ "$(id -u)" = "0" ] && id "$APP_USER" >/dev/null 2>&1; then
  chown -R "$APP_USER":"$APP_USER" "$DATA_DIR" 2>/dev/null || true
fi

for file in \
  stockpicker.db \
  users.json \
  watchlist.json \
  predictions.json \
  portfolio.json \
  paper_portfolio.json \
  alerts.json \
  settings.json \
  lockout_state.json \
  sentiment_agent_state.json \
  audit.log
do
  if [ -e "/app/backend/$file" ] && [ ! -e "$DATA_DIR/$file" ]; then
    cp "/app/backend/$file" "$DATA_DIR/$file"
  fi
  rm -f "/app/backend/$file"
  ln -s "$DATA_DIR/$file" "/app/backend/$file"
done

cd /app/backend
# Drop root for the long-lived server process when possible.
if [ "$(id -u)" = "0" ] && id "$APP_USER" >/dev/null 2>&1 && command -v gosu >/dev/null 2>&1; then
  exec gosu "$APP_USER" uvicorn main:app --host 0.0.0.0 --port "${PORT:-8080}"
fi
exec uvicorn main:app --host 0.0.0.0 --port "${PORT:-8080}"
