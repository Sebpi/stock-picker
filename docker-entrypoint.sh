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

# Pre-flight: verify critical Python imports before starting uvicorn.
# Errors here appear in Fly logs and explain why the app never binds to the port.
echo "[entrypoint] Running pre-flight import check..."
PREFLIGHT_SCRIPT='
import sys, os
try:
    from cryptography.fernet import Fernet
    from jose import jwt
    import bcrypt, fastapi, uvicorn, pypdf
    import cryptography, cffi
    print(f"[preflight] cryptography={cryptography.__version__} cffi={cffi.__version__}", flush=True)
    print("[preflight] All library imports OK", flush=True)
except Exception as e:
    print(f"[preflight] IMPORT FAILED: {e}", file=sys.stderr, flush=True)
    sys.exit(1)
try:
    import main
    print("[preflight] main.py module loaded OK", flush=True)
except Exception as e:
    print(f"[preflight] main.py LOAD FAILED: {e}", file=sys.stderr, flush=True)
    sys.exit(1)
'

if [ "$(id -u)" = "0" ] && id "$APP_USER" >/dev/null 2>&1 && command -v gosu >/dev/null 2>&1; then
  gosu "$APP_USER" python3 -c "$PREFLIGHT_SCRIPT" || { echo "[entrypoint] Pre-flight failed — aborting startup"; exit 1; }
  exec gosu "$APP_USER" uvicorn main:app --host 0.0.0.0 --port "${PORT:-8080}"
fi

python3 -c "$PREFLIGHT_SCRIPT" || { echo "[entrypoint] Pre-flight failed — aborting startup"; exit 1; }
exec uvicorn main:app --host 0.0.0.0 --port "${PORT:-8080}"
