#!/bin/sh
set -eu

DATA_DIR="${DATA_DIR:-/app/data}"
mkdir -p "$DATA_DIR"

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
exec uvicorn main:app --host 0.0.0.0 --port "${PORT:-8080}"
