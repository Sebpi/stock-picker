import asyncio
import math
import csv as csv_mod
import io
import logging
import pypdf
import json
import os
import random
import re
import secrets
import smtplib
import statistics
import subprocess
import sys
import tempfile
import time as time_mod
import uuid
import xml.etree.ElementTree as ET
from contextlib import asynccontextmanager
from datetime import date, datetime, time, timedelta, timezone
import zoneinfo
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from typing import Any, Optional

import anthropic
import httpx
import yfinance as yf
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from dotenv import load_dotenv
from fastapi import BackgroundTasks, Body, Depends, FastAPI, File, HTTPException, Request, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, Response
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.staticfiles import StaticFiles
from starlette.middleware.trustedhost import TrustedHostMiddleware
import bcrypt as _bcrypt
from jose import JWTError, jwt
import pyotp
from pydantic import BaseModel
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded

load_dotenv(dotenv_path=Path(__file__).parent / ".env")

def _clear_dead_local_proxy_env() -> None:
    dead_hosts = ("127.0.0.1:9", "localhost:9")
    proxy_keys = [
        "HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY",
        "http_proxy", "https_proxy", "all_proxy",
        "GIT_HTTP_PROXY", "GIT_HTTPS_PROXY",
    ]
    for key in proxy_keys:
        value = os.environ.get(key, "")
        if any(host in value for host in dead_hosts):
            os.environ.pop(key, None)

_clear_dead_local_proxy_env()

# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(Path(__file__).parent / "audit.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger("stockpicker")

# ── Input validation ───────────────────────────────────────────────────────────
_TICKER_RE = re.compile(r"^[A-Z0-9.\-]{1,10}$")

def _validate_ticker(ticker: str) -> str:
    """Uppercase and validate a ticker symbol. Raises 400 if invalid."""
    t = ticker.upper().strip()
    if not _TICKER_RE.match(t):
        raise HTTPException(status_code=400, detail="Invalid ticker symbol")
    return t

# ── Rate limiter ───────────────────────────────────────────────────────────────
def _client_ip(request) -> str:
    """Resolve the real client IP for rate-limiting.

    Behind Fly's edge, the original client IP arrives in `Fly-Client-IP`, which
    Fly sets and overwrites — a client cannot forge it. `request.client.host`
    is the proxy's address, so relying on it alone would make every request
    share one bucket. We deliberately do NOT trust `X-Forwarded-For` (client
    spoofable). Falls back to the socket peer for local/dev runs.
    """
    fly_ip = request.headers.get("fly-client-ip")
    if fly_ip:
        return fly_ip.strip()
    return get_remote_address(request)

limiter = Limiter(key_func=_client_ip)

# ── Account lockout ────────────────────────────────────────────────────────────
_MAX_ATTEMPTS   = 5
_LOCKOUT_MINS   = 15
_LOCKOUT_STATE_FILE = Path(__file__).parent / "lockout_state.json"
_failed_logins: dict[str, list] = {}   # username -> list of attempt datetimes
_lockout_until: dict[str, datetime] = {}

def _load_lockout_state() -> None:
    if not _LOCKOUT_STATE_FILE.exists():
        return
    try:
        data = json.loads(_LOCKOUT_STATE_FILE.read_text())
        now = datetime.now(timezone.utc)
        for username, iso in data.items():
            try:
                until = datetime.fromisoformat(iso)
                if until > now:
                    _lockout_until[username] = until
            except Exception:
                pass
    except Exception as exc:
        logger.warning("Could not load lockout state: %s", exc)

def _save_lockout_state() -> None:
    try:
        now = datetime.now(timezone.utc)
        active = {u: v.isoformat() for u, v in _lockout_until.items() if v > now}
        _LOCKOUT_STATE_FILE.write_text(json.dumps(active))
    except Exception as exc:
        logger.warning("Could not save lockout state: %s", exc)

def _check_lockout(username: str) -> None:
    """Raise 429 if account is locked out."""
    until = _lockout_until.get(username)
    if until and datetime.now(timezone.utc) < until:
        remaining = int((until - datetime.now(timezone.utc)).total_seconds() // 60) + 1
        raise HTTPException(status_code=429, detail=f"Account locked. Try again in {remaining} minute(s).")

def _record_failed_login(username: str) -> None:
    """Track failed login; lock account after _MAX_ATTEMPTS within 10 minutes."""
    now = datetime.now(timezone.utc)
    window = [t for t in _failed_logins.get(username, []) if (now - t).total_seconds() < 600]
    window.append(now)
    _failed_logins[username] = window
    if len(window) >= _MAX_ATTEMPTS:
        _lockout_until[username] = now + timedelta(minutes=_LOCKOUT_MINS)
        logger.warning("LOCKOUT username=%s after %d failed attempts", username, len(window))
        _save_lockout_state()

def _clear_failed_logins(username: str) -> None:
    _failed_logins.pop(username, None)
    _lockout_until.pop(username, None)
    _save_lockout_state()

def _norm_username(username: str) -> str:
    """Canonical form for lockout/lookup. Lower-cased so that varying the case
    (Admin/admin/ADMIN) cannot spawn independent lockout buckets or bypass the
    per-account attempt limit."""
    return username.strip().lower()

def _lookup_user_ci(users: dict, username: str) -> tuple[str | None, dict | None]:
    """Case-insensitive lookup into users.json. Returns (canonical_key, user)."""
    if username in users:
        return username, users[username]
    lo = username.strip().lower()
    for key, val in users.items():
        if key.lower() == lo:
            return key, val
    return None, None

# ── MFA verification throttle (per-user, in-memory) ──────────────────────────────
_MFA_MAX_ATTEMPTS = 5
_MFA_LOCKOUT_MINS = 15
_mfa_failures: dict[str, list] = {}      # user_id -> [attempt datetimes]
_mfa_lockout_until: dict[str, datetime] = {}

def _check_mfa_lockout(user_id: str) -> None:
    until = _mfa_lockout_until.get(user_id)
    if until and datetime.now(timezone.utc) < until:
        remaining = int((until - datetime.now(timezone.utc)).total_seconds() // 60) + 1
        raise HTTPException(status_code=429, detail=f"Too many MFA attempts. Try again in {remaining} minute(s).")

def _record_mfa_failure(user_id: str) -> None:
    now = datetime.now(timezone.utc)
    window = [t for t in _mfa_failures.get(user_id, []) if (now - t).total_seconds() < 600]
    window.append(now)
    _mfa_failures[user_id] = window
    if len(window) >= _MFA_MAX_ATTEMPTS:
        _mfa_lockout_until[user_id] = now + timedelta(minutes=_MFA_LOCKOUT_MINS)
        logger.warning("MFA_LOCKOUT user_id=%s after %d failed attempts", user_id, len(window))

def _clear_mfa_failures(user_id: str) -> None:
    _mfa_failures.pop(user_id, None)
    _mfa_lockout_until.pop(user_id, None)

# ── Auth setup ─────────────────────────────────────────────────────────────────
# Treat REQUIRE_HTTPS=true as the "production" signal (set on Fly via fly.toml).
_IS_PROD = os.getenv("REQUIRE_HTTPS", "false").lower() in {"1", "true", "yes", "on"}
_SECRET_KEY = os.getenv("SECRET_KEY")
if not _SECRET_KEY:
    if _IS_PROD:
        # Fail closed: an ephemeral key in prod silently invalidates every
        # session on restart and diverges across instances. Refuse to boot.
        raise RuntimeError(
            "SECRET_KEY is not set but REQUIRE_HTTPS=true (production). "
            "Set a persistent SECRET_KEY (python -c \"import secrets; print(secrets.token_hex(32))\") "
            "before starting. Refusing to start with an ephemeral signing key."
        )
    logger.warning("SECRET_KEY is not set; using an ephemeral key. Sessions will be invalidated on restart.")
    _SECRET_KEY = secrets.token_hex(32)
_SERVICE_KEY = (os.getenv("STOCK_PICKER_SERVICE_KEY") or os.getenv("INTERNAL_SERVICE_KEY") or "").strip()

# ── Secret-at-rest encryption (TOTP/MFA seeds) ───────────────────────────────────
# MFA secrets are encrypted before they touch the DB so a database/backup
# compromise does not hand an attacker working TOTP seeds. Key is derived from
# SECRET_KEY via SHA-256 → urlsafe base64 (Fernet key format). Values are
# prefixed so legacy plaintext base32 seeds (uppercase A-Z2-7, never lowercase)
# decrypt transparently for backward compatibility.
_MFA_ENC_PREFIX = "enc:v1:"

def _secret_cipher():
    import base64
    import hashlib
    from cryptography.fernet import Fernet
    key = base64.urlsafe_b64encode(hashlib.sha256(_SECRET_KEY.encode()).digest())
    return Fernet(key)

def _encrypt_secret(plaintext: str) -> str:
    if not plaintext:
        return plaintext
    return _MFA_ENC_PREFIX + _secret_cipher().encrypt(plaintext.encode()).decode()

def _decrypt_secret(stored: str | None) -> str:
    """Decrypt a stored secret. Plaintext (un-prefixed) values pass through so
    seeds written before encryption was added keep working."""
    if not stored:
        return stored or ""
    if not stored.startswith(_MFA_ENC_PREFIX):
        return stored  # legacy plaintext seed
    token = stored[len(_MFA_ENC_PREFIX):]
    return _secret_cipher().decrypt(token.encode()).decode()

# Portal-shared JWT (LENS → engines). Same value set on seb-portal +
# pick-shovels + LENS so a portal-minted JWT verifies here without a
# round-trip. Optional — when unset, only the local SECRET_KEY chain works.
_PORTAL_JWT_SECRET = (os.getenv("PORTAL_JWT_SECRET") or "").strip()
_PORTAL_JWT_ISSUER = "seb-portal"
_ALGORITHM  = "HS256"
_TOKEN_HOURS = 24
_ACCESS_TOKEN_MINUTES = int(os.getenv("ACCESS_TOKEN_MINUTES", "15"))   # short-lived access tokens
_STRIPE_SECRET_KEY = os.getenv("STRIPE_SECRET_KEY", "")
_STRIPE_WEBHOOK_SECRET = os.getenv("STRIPE_WEBHOOK_SECRET", "")
_APNS_KEY_ID = os.getenv("APNS_KEY_ID", "")
_APNS_TEAM_ID = os.getenv("APNS_TEAM_ID", "")
_APNS_BUNDLE_ID = os.getenv("APNS_BUNDLE_ID", "")
_APNS_KEY_PATH = os.getenv("APNS_KEY_PATH", "")

def _hash_pw(password: str) -> str:
    return _bcrypt.hashpw(password.encode(), _bcrypt.gensalt(rounds=12)).decode()

def _verify_pw(password: str, hashed: str) -> bool:
    return _bcrypt.checkpw(password.encode(), hashed.encode())

http_bearer = HTTPBearer(auto_error=False)
# Users live on the persistent Fly volume (DATA_DIR) so password resets
# survive redeploys. If a legacy users.json exists alongside the code
# (pre-v2.0.3 layout), migrate it on first boot.
_DATA_DIR = Path(os.getenv("DATA_DIR", str(Path(__file__).parent.parent / "data")))
_DATA_DIR.mkdir(parents=True, exist_ok=True)
USERS_FILE = _DATA_DIR / "users.json"
_legacy_users = Path(__file__).parent / "users.json"
if _legacy_users.exists() and not USERS_FILE.exists():
    USERS_FILE.write_text(_legacy_users.read_text())

# In-memory password reset tokens: sha256(token) -> (username, expiry).
# Keyed by hash so a process-memory disclosure yields no replayable tokens.
_reset_tokens: dict[str, tuple[str, datetime]] = {}

def _hash_reset_token(raw: str) -> str:
    import hashlib
    return hashlib.sha256(raw.encode()).hexdigest()

# Auth public routes — no JWT required
_AUTH_PUBLIC = {
    "/api/auth/login", "/api/auth/forgot-password", "/api/auth/reset-password",
    "/api/health", "/api/version",
    "/api/auth/register", "/api/auth/verify-email", "/api/auth/refresh",
    "/api/billing/stripe/webhook",
}
ANTHROPIC_MODEL = os.getenv("ANTHROPIC_MODEL", "claude-haiku-4-5-20251001").strip() or "claude-haiku-4-5-20251001"
STOCK_RESEARCH_MAX_TOKENS = max(256, int(os.getenv("STOCK_RESEARCH_MAX_TOKENS", "6000")))
RECOMMEND_MAX_TOKENS = max(256, int(os.getenv("RECOMMEND_MAX_TOKENS", "800")))
SEARCH_RESULTS_LIMIT = max(1, int(os.getenv("SEARCH_RESULTS_LIMIT", "12")))
SEARCH_INFO_TIMEOUT_SEC = max(0.5, float(os.getenv("SEARCH_INFO_TIMEOUT_SEC", "2.5")))
# Universe build (prewarm + cache-miss screen) is batched/concurrent and is
# allowed to wait longer per ticker, so each Index returns close to its full
# constituent list rather than the ~40-50% that survived a 2.5s timeout.
UNIVERSE_INFO_TIMEOUT_SEC = max(2.0, float(os.getenv("UNIVERSE_INFO_TIMEOUT_SEC", "18.0")))
# Screener universe data provider. 'yfinance' = the original per-ticker .info
# fan-out (rate-limited under load). 'yahooquery' = bulk multi-ticker calls
# that pull every ticker's modules in 3-5 HTTP requests per chunk, which
# avoids the per-call throttling pressure. Set to 'yfinance' on Fly to roll
# back instantly without redeploy.
UNIVERSE_PROVIDER = os.getenv("UNIVERSE_PROVIDER", "yahooquery").strip().lower()
RECOMMENDATION_INFO_TIMEOUT_SEC = max(1.0, float(os.getenv("RECOMMENDATION_INFO_TIMEOUT_SEC", "4.0")))
RECOMMENDATION_HISTORY_TIMEOUT_SEC = max(1.0, float(os.getenv("RECOMMENDATION_HISTORY_TIMEOUT_SEC", "4.0")))
PREDICTIONS_INCLUDE_STOCK_RESEARCH = os.getenv("PREDICTIONS_INCLUDE_STOCK_RESEARCH", "false").lower() in {"1", "true", "yes", "on"}
PREDICTIONS_UNIVERSE_FILL_LIMIT = max(0, int(os.getenv("PREDICTIONS_UNIVERSE_FILL_LIMIT", "6")))
PREDICTIONS_MAX_TOKENS = max(512, int(os.getenv("PREDICTIONS_MAX_TOKENS", "8192")))
PREDICTION_MODEL_VERSION = os.getenv("PREDICTION_MODEL_VERSION", "pred-v3.3.0").strip() or "pred-v3.3.0"
PREDICTION_PROMPT_VERSION = os.getenv("PREDICTION_PROMPT_VERSION", "prompt-v5").strip() or "prompt-v5"
PREDICTION_LEARNING_ENABLED = os.getenv("PREDICTION_LEARNING_ENABLED", "true").lower() in {"1", "true", "yes", "on"}
PREDICTION_CALIBRATION_MIN_SAMPLES = max(1, int(os.getenv("PREDICTION_CALIBRATION_MIN_SAMPLES", "3")))
THESIS_AUTO_RUN_ENABLED = os.getenv("THESIS_AUTO_RUN_ENABLED", "false").lower() in {"1", "true", "yes", "on"}
THESIS_AUTO_RUN_INTERVAL_MINUTES = max(15, int(os.getenv("THESIS_AUTO_RUN_INTERVAL_MINUTES", "1440")))
THESIS_AUTO_RUN_MAX_TICKERS = int(os.getenv("THESIS_AUTO_RUN_MAX_TICKERS", "0")) or None  # None = entire watchlist
EVALUATION_AUTO_RUN_ENABLED = os.getenv("EVALUATION_AUTO_RUN_ENABLED", "true").lower() in {"1", "true", "yes", "on"}
EVALUATION_AUTO_RUN_INTERVAL_MINUTES = max(60, int(os.getenv("EVALUATION_AUTO_RUN_INTERVAL_MINUTES", "1440")))

def load_users() -> dict:
    if USERS_FILE.exists():
        try:
            return json.loads(USERS_FILE.read_text())
        except Exception as exc:
            logger.error("Corrupt users file, returning empty: %s", exc)
    return {}

def _atomic_write(path: Path, data: str) -> None:
    """Write data atomically: write to system temp dir, then move into place.

    Using the system temp dir (instead of path.parent) avoids Windows
    PermissionError/antivirus locking on the destination dir that left
    orphaned .tmp_* files when os.replace failed after the fd was closed.
    shutil.move falls back to copy+delete when src and dst are on different
    drives, so this is safe even if TEMP is on a different volume.
    """
    import shutil
    fd, tmp = tempfile.mkstemp(prefix="sp_atomic_")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(data)
        for attempt in range(4):
            try:
                shutil.move(tmp, str(path))
                return
            except PermissionError:
                if attempt == 3:
                    raise
                time_mod.sleep(0.15 * (attempt + 1))
    except Exception:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise

def save_users(users: dict):
    _atomic_write(USERS_FILE, json.dumps(users, indent=2))

def create_access_token(username: str) -> str:
    expire = datetime.now(timezone.utc) + timedelta(hours=_TOKEN_HOURS)
    return jwt.encode({"sub": username, "exp": expire}, _SECRET_KEY, algorithm=_ALGORITHM)


def create_short_access_token(subject: str) -> str:
    """15-minute access token for multi-user auth flow."""
    expire = datetime.now(timezone.utc) + timedelta(minutes=_ACCESS_TOKEN_MINUTES)
    return jwt.encode({"sub": subject, "exp": expire, "typ": "access"}, _SECRET_KEY, algorithm=_ALGORITHM)

async def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(http_bearer)) -> str:
    """Verify the bearer token against (in order):
      1. _SERVICE_KEY  — service-account literal match.
      2. _SECRET_KEY   — local app JWT (existing login flow).
      3. _PORTAL_JWT_SECRET — portal-issued JWT from seb-portal /api/auth/jwt.
         Validated by signature + iss claim. The user does NOT need a local
         users.json entry — portal is the source of truth for cross-app calls.

    Returns the resolved identity string (username for local, "portal:<sub>"
    for portal-routed). Raises 401 on any failure.
    """
    exc = HTTPException(status_code=401, detail="Not authenticated")
    if not credentials:
        raise exc
    if _SERVICE_KEY and secrets.compare_digest(credentials.credentials, _SERVICE_KEY):
        return "service:pick-shovels"
    # 1. Local JWT first — existing login flow.
    try:
        payload = jwt.decode(credentials.credentials, _SECRET_KEY, algorithms=[_ALGORITHM])
        username: str = payload.get("sub", "")
        if username and username in load_users():
            return username
        # Also accept multi-user (SQLite app_users) tokens.
        if username:
            import db as _db
            app_user = _db.get_user_by_username(username)
            if app_user and app_user["is_active"]:
                return app_user["username"]
        # Local JWT decoded but user vanished — fall through to portal check
        # rather than raising, to keep portal-only users working.
    except JWTError:
        pass
    # 2. Portal-issued JWT — shared-session via seb-portal.
    if _PORTAL_JWT_SECRET:
        try:
            payload = jwt.decode(
                credentials.credentials,
                _PORTAL_JWT_SECRET,
                algorithms=[_ALGORITHM],
            )
            if payload.get("iss") == _PORTAL_JWT_ISSUER and payload.get("sub"):
                return f"portal:{payload['sub']}"
        except JWTError:
            pass
    raise exc

@asynccontextmanager
async def lifespan(app):
    await startup()
    await _init_multiagent_db()
    yield
    await shutdown()


app = FastAPI(title="StockLens API", lifespan=lifespan)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
scheduler = AsyncIOScheduler()

_allowed_origins = [o.strip() for o in os.getenv("ALLOWED_ORIGINS", "http://localhost:8000").split(",") if o.strip()]
_allowed_hosts = [h.strip() for h in os.getenv("ALLOWED_HOSTS", "localhost,127.0.0.1,*.ngrok-free.app,*.ngrok-free.dev,*.ngrok.io").split(",") if h.strip()]
_require_https = os.getenv("REQUIRE_HTTPS", "false").lower() in {"1", "true", "yes", "on"}
_allow_ngrok_origins = os.getenv("ALLOW_NGROK_ORIGINS", "false").lower() in {"1", "true", "yes", "on"}
_ngrok_origin_regex = r"^https://[a-z0-9-]+\.(ngrok-free\.app|ngrok-free\.dev|ngrok\.io)$" if _allow_ngrok_origins else None


def _is_local_origin(origin: str) -> bool:
    return bool(re.match(r"^https?://(localhost|127\.0\.0\.1)(:\d+)?$", origin, re.IGNORECASE))


def _is_origin_allowed(origin: str) -> bool:
    if origin in _allowed_origins:
        return True
    if _is_local_origin(origin):
        return True
    if _ngrok_origin_regex and re.match(_ngrok_origin_regex, origin, re.IGNORECASE):
        return True
    return False

app.add_middleware(TrustedHostMiddleware, allowed_hosts=_allowed_hosts)
app.add_middleware(
    CORSMiddleware,
    allow_origins=_allowed_origins,
    allow_origin_regex=_ngrok_origin_regex,
    allow_credentials=True,
    allow_methods=["GET", "POST", "DELETE", "PUT", "PATCH"],
    allow_headers=["Authorization", "Content-Type"],
)

@app.middleware("http")
async def security_headers(request: Request, call_next):
    forwarded_proto = request.headers.get("x-forwarded-proto", "")
    is_secure = request.url.scheme == "https" or forwarded_proto.lower() == "https"
    if _require_https and not is_secure and request.client and request.client.host not in {"127.0.0.1", "::1", "localhost"}:
        return JSONResponse(status_code=403, content={"detail": "HTTPS is required"})
    response = await call_next(request)
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
    response.headers["Permissions-Policy"] = "camera=(), microphone=(), geolocation=()"
    response.headers["Cross-Origin-Opener-Policy"] = "same-origin"
    response.headers["Cross-Origin-Embedder-Policy"] = "unsafe-none"
    response.headers["Cross-Origin-Resource-Policy"] = "same-origin"
    if is_secure:
        response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
    response.headers["Content-Security-Policy"] = (
        "default-src 'self'; "
        "script-src 'self' "
        "'sha256-G0xAtLNSRFwbaVvJoMXIL39pcsyt3wPr8A0Gt4fNPx4=' "
        "https://unpkg.com; "
        "style-src 'self' 'sha256-JY2CPnAQYQylZcG0tTBIGoRtNV0dsTWGI3U7cL/c9Rc='; "
        "connect-src 'self' https://pick-shovels-wistful-morning-252.fly.dev; "
        "img-src 'self' data: blob:; "
        "font-src 'self'; "
        "worker-src blob:; "
        "frame-ancestors 'none'; "
        "base-uri 'self'; "
        "form-action 'self';"
    )
    # Suppress server version disclosure (uvicorn sets this at ASGI level)
    try:
        del response.headers["server"]
    except KeyError:
        pass
    return response


@app.middleware("http")
async def origin_guard(request: Request, call_next):
    if request.method in {"POST", "PUT", "PATCH", "DELETE"}:
        origin = request.headers.get("origin")
        if origin and not _is_origin_allowed(origin):
            return JSONResponse(status_code=403, content={"detail": "Origin not allowed"})
    return await call_next(request)

@app.middleware("http")
async def auth_middleware(request: Request, call_next):
    path = request.url.path
    # Defense-in-depth backstop: require a valid bearer token for every /api/
    # and /v1/ route (except the public auth/health set), so a route that
    # forgets its per-route Depends(get_current_user) is not silently exposed.
    protected = path.startswith("/api/") or path.startswith("/v1/")
    if not protected or path in _AUTH_PUBLIC:
        return await call_next(request)
    auth_header = request.headers.get("Authorization", "")
    if not auth_header.startswith("Bearer "):
        return JSONResponse(status_code=401, content={"detail": "Not authenticated"})
    token = auth_header[7:]
    # Service-account literal match — must mirror get_current_user so the
    # middleware backstop is never stricter than the per-route dependency.
    if _SERVICE_KEY and secrets.compare_digest(token, _SERVICE_KEY):
        return await call_next(request)
    # Try local SECRET_KEY first (existing login flow).
    try:
        payload = jwt.decode(token, _SECRET_KEY, algorithms=[_ALGORITHM])
        username = payload.get("sub", "")
        if username and username in load_users():
            return await call_next(request)
        # Also accept multi-user (SQLite app_users) tokens.
        if username:
            import db as _db
            app_user = _db.get_user_by_username(username)
            if app_user and app_user["is_active"]:
                return await call_next(request)
        # Decoded but user vanished — fall through to portal check.
    except JWTError:
        pass
    # Portal-issued JWT (shared-session via seb-portal). Same logic as
    # get_current_user — we only accept iss="seb-portal" so a random JWT
    # signed with the same secret elsewhere can't pass.
    if _PORTAL_JWT_SECRET:
        try:
            payload = jwt.decode(token, _PORTAL_JWT_SECRET, algorithms=[_ALGORITHM])
            if payload.get("iss") == _PORTAL_JWT_ISSUER and payload.get("sub"):
                return await call_next(request)
        except JWTError:
            pass
    return JSONResponse(status_code=401, content={"detail": "Invalid or expired token"})

FRONTEND_DIR = Path(__file__).parent.parent / "frontend"
PACKAGE_FILE = Path(__file__).parent.parent / "package.json"

def _package_version() -> str:
    try:
        return json.loads(PACKAGE_FILE.read_text(encoding="utf-8")).get("version") or "0.0.0"
    except Exception:
        return "0.0.0"

APP_VERSION = os.getenv("APP_VERSION") or _package_version()
GIT_SHA = (os.getenv("GIT_SHA") or os.getenv("FLY_IMAGE_REF", "")[-7:] or "unknown")[:12]
BUILD_TIME = os.getenv("BUILD_TIME") or "unknown"

def _stamp_frontend(text: str) -> str:
    return (
        text
        .replace("__APP_VERSION__", APP_VERSION)
        .replace("__GIT_SHA__", GIT_SHA)
        .replace("__BUILD_TIME__", BUILD_TIME)
    )

@app.get("/api/health", include_in_schema=False)
async def health_check():
    checks: dict[str, str] = {}

    # SQLite
    try:
        import db as _db
        with _db.get_conn() as conn:
            conn.execute("SELECT 1")
        checks["sqlite"] = "ok"
    except Exception as exc:
        logger.error("HEALTH_SQLITE_FAIL: %s", exc)
        checks["sqlite"] = "error"  # detail kept server-side; endpoint is public

    # yfinance spot check
    try:
        import yfinance as yf
        info = yf.Ticker("AAPL").fast_info
        checks["yfinance"] = "ok" if info else "no data"
    except Exception as exc:
        logger.error("HEALTH_YFINANCE_FAIL: %s", exc)
        checks["yfinance"] = "error"  # detail kept server-side; endpoint is public

    # Anthropic key present
    checks["anthropic_key"] = "configured" if os.getenv("ANTHROPIC_API_KEY") else "missing"

    overall = "ok" if all(v in {"ok", "configured"} for v in checks.values()) else "degraded"
    return {
        "status": overall,
        "checks": checks,
        "frontend_ready": (FRONTEND_DIR / "index.html").exists(),
        "scheduler_running": bool(getattr(scheduler, "running", False)),
    }

@app.get("/api/version", include_in_schema=False)
async def version_check():
    return {
        "app": "stock-picker",
        "version": APP_VERSION,
        "git_sha": GIT_SHA,
        "build_time": BUILD_TIME,
    }

@app.get("/", include_in_schema=False)
def serve_index():
    return HTMLResponse(
        _stamp_frontend((FRONTEND_DIR / "index.html").read_text(encoding="utf-8")),
        headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
    )

@app.get("/legacy", include_in_schema=False)
def serve_legacy_index():
    from fastapi.responses import RedirectResponse
    return RedirectResponse(url="/", status_code=301)

@app.get("/static/{filename:path}", include_in_schema=False)
def serve_static(filename: str):
    file_path = FRONTEND_DIR / filename
    if not file_path.exists():
        raise HTTPException(status_code=404)
    if filename == "react-app.js":
        return Response(
            _stamp_frontend(file_path.read_text(encoding="utf-8")),
            media_type="application/javascript",
            headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
        )
    return FileResponse(
        file_path,
        headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
    )

WATCHLIST_FILE       = _DATA_DIR / "watchlist.json"
PREDICTIONS_FILE     = _DATA_DIR / "predictions.json"
ALERTS_FILE          = _DATA_DIR / "alerts.json"
PORTFOLIO_FILE       = _DATA_DIR / "portfolio.json"
SETTINGS_FILE        = _DATA_DIR / "settings.json"
PAPER_PORTFOLIO_FILE = _DATA_DIR / "paper_portfolio.json"

def _migrate_legacy_json_data_files() -> None:
    """
    One-time migration of JSON state from the old backend-local paths
    into DATA_DIR-backed persistent storage.
    """
    legacy_dir = Path(__file__).parent
    files = [
        "watchlist.json",
        "predictions.json",
        "alerts.json",
        "portfolio.json",
        "settings.json",
        "paper_portfolio.json",
    ]
    for name in files:
        src = legacy_dir / name
        dst = _DATA_DIR / name
        if dst.exists() or not src.exists():
            continue
        try:
            _atomic_write(dst, src.read_text())
            logger.info("MIGRATE_DATA_FILE file=%s from=%s to=%s", name, src, dst)
        except Exception as exc:
            logger.warning("MIGRATE_DATA_FILE_FAIL file=%s err=%s", name, exc)

_migrate_legacy_json_data_files()

PAPER_INITIAL_FLOAT = 200_000.0

def load_paper_portfolio() -> list[dict]:
    if PAPER_PORTFOLIO_FILE.exists():
        try:
            return json.loads(PAPER_PORTFOLIO_FILE.read_text())
        except Exception as exc:
            logger.error("Corrupt paper portfolio file: %s", exc)
    return []

def save_paper_portfolio(transactions: list[dict]):
    _atomic_write(PAPER_PORTFOLIO_FILE, json.dumps(transactions, indent=2))

def load_settings() -> dict:
    defaults = {
        "initial_float": 200000.0,
        "target": 200000.0,
        "target_months": 12,
        # Alert thresholds
        "alert_price_swing_pct": 3.0,
        "alert_cooldown_hours": 12,
        "alert_top_buys": 3,
        "alert_top_sells": 3,
        "alert_buy_min_score": 72,
        "alert_sell_max_score": 42,
        # Phase 2: regime gate — block BUYs when broad market is risk-off
        "prediction_accuracy_goal": 55.0,
        "min_buy_confidence": "medium",
        "regime_gate_enabled": True,
        "regime_spy_dma_period": 200,
        "regime_vix_max": 25.0,
        # Phase 2: require this many of 9 agents to be net-positive before BUY
        "alert_min_positive_agents": 5,
        # Phase 3: portfolio-relative sizing + risk budget caps
        "target_positions_count": 10,      # ~1/N_target base weight per name
        "position_max_pct": 0.12,          # max 12% of portfolio in any one name
        "sector_max_pct": 0.30,            # max 30% of portfolio in any one sector
        "portfolio_var_max_pct": 0.08,     # max 8% portfolio 95% 1-month VaR
    }
    if SETTINGS_FILE.exists():
        try:
            return {**defaults, **json.loads(SETTINGS_FILE.read_text())}
        except Exception as exc:
            logger.error("Corrupt settings file, using defaults: %s", exc)
    return defaults

def save_settings(s: dict):
    _atomic_write(SETTINGS_FILE, json.dumps(s, indent=2))


def _get_db_user(username: str) -> Optional[dict]:
    """Returns the app_users record for this username, or None for the legacy admin."""
    import db as _db
    return _db.get_user_by_username(username)


def _load_user_settings_merged(user_id: str) -> dict:
    """Merge global defaults with per-user overrides stored in user_settings table."""
    import db as _db
    merged = load_settings()
    overrides = _db.get_user_settings(user_id)
    for k, v in overrides.items():
        if v is None:
            continue
        try:
            merged[k] = float(v) if "." in str(v) else int(v)
        except (ValueError, TypeError):
            merged[k] = v
    return merged


# Phase 2: market-regime gate. Block BUYs when SPY is below its long-term DMA
# (trend-following filter) OR VIX is elevated (stress filter). Cached for an
# hour because regime doesn't change minute-by-minute and the yfinance call is
# the slowest part of the recommendation pipeline.
_regime_cache: dict = {"ts": 0.0, "data": None}
_REGIME_TTL_SEC = 3600

def _fetch_regime_data_sync(dma_period: int) -> dict:
    """Synchronous yfinance fetch — run inside asyncio.to_thread."""
    days_needed = max(220, dma_period + 20)
    spy_hist = yf.Ticker("SPY").history(period=f"{days_needed}d")
    vix_hist = yf.Ticker("^VIX").history(period="5d")
    if spy_hist.empty or len(spy_hist) < dma_period:
        raise RuntimeError(f"SPY history too short ({len(spy_hist)} bars, need {dma_period})")
    spy_close = float(spy_hist["Close"].iloc[-1])
    spy_dma   = float(spy_hist["Close"].iloc[-dma_period:].mean())
    vix_close = float(vix_hist["Close"].iloc[-1]) if not vix_hist.empty else float("nan")
    return {"spy_close": spy_close, "spy_dma": spy_dma, "vix": vix_close, "dma_period": dma_period}

async def get_market_regime() -> dict:
    """
    Return regime status used by both recommendation engines to gate BUYs.

    Output keys:
      ok:              True if regime is risk-on enough to allow BUYs.
      reason:          Human-readable explanation (used in BUY-blocked messaging).
      spy_close, spy_dma, vix, dma_period: raw inputs for telemetry.
      gate_enabled:    False if the gate is disabled in settings (ok=True always).
      stale/error:     True on yfinance failure — fails OPEN so a data outage
                       doesn't freeze trading; this is intentional, the gate is
                       a quality filter not a safety lock.
    """
    settings = load_settings()
    if not settings.get("regime_gate_enabled", True):
        return {"ok": True, "reason": "regime gate disabled in settings", "gate_enabled": False}

    now = time_mod.time()
    if _regime_cache["data"] and (now - _regime_cache["ts"]) < _REGIME_TTL_SEC:
        return _regime_cache["data"]

    dma_period = int(settings.get("regime_spy_dma_period", 200))
    vix_max    = float(settings.get("regime_vix_max", 25.0))
    try:
        raw = await asyncio.to_thread(_fetch_regime_data_sync, dma_period)
    except Exception as exc:
        logger.warning("Regime fetch failed (%s) — failing open", exc)
        # Fail open: don't block trading on a data outage, but flag it.
        result = {"ok": True, "reason": f"regime data unavailable ({exc})", "stale": True, "error": str(exc), "gate_enabled": True}
        # Don't cache failures — retry on next call
        return result

    spy_above = raw["spy_close"] >= raw["spy_dma"]
    vix_ok    = raw["vix"] <= vix_max if raw["vix"] == raw["vix"] else True  # NaN guard
    ok        = spy_above and vix_ok

    if ok:
        reason = (f"SPY {raw['spy_close']:.2f} ≥ {dma_period}-DMA {raw['spy_dma']:.2f}, "
                  f"VIX {raw['vix']:.1f} ≤ {vix_max:.0f}")
    else:
        parts = []
        if not spy_above:
            parts.append(f"SPY {raw['spy_close']:.2f} below {dma_period}-DMA {raw['spy_dma']:.2f}")
        if not vix_ok:
            parts.append(f"VIX {raw['vix']:.1f} above {vix_max:.0f} threshold")
        reason = "Market regime risk-off: " + "; ".join(parts)

    result = {
        "ok": ok,
        "reason": reason,
        "spy_close": raw["spy_close"],
        "spy_dma": raw["spy_dma"],
        "vix": raw["vix"],
        "dma_period": dma_period,
        "vix_max": vix_max,
        "spy_above_dma": spy_above,
        "vix_below_max": vix_ok,
        "gate_enabled": True,
    }
    _regime_cache["ts"]   = now
    _regime_cache["data"] = result
    return result


# Phase 5: portfolio trajectory math. Probability the portfolio hits target
# value by the deadline, under a normal-return approximation. Inputs:
#   v0       — current portfolio value (£)
#   target   — target value (£)
#   months   — months to deadline
#   weights  — list of (weight, annual_return_pct, annual_vol_pct) tuples for
#              each position currently held (or proposed). Weights are relative
#              to v0; sum need not equal 1 (the remainder is treated as cash:
#              zero return, zero vol).
# Output: probability in [0, 1]. Uses lognormal-ish drift on linear returns.
def _p_hit_target(v0: float, target: float, months: float,
                  weights: list[tuple[float, float, float]]) -> float:
    if v0 <= 0 or months <= 0:
        return 0.0
    if target <= v0:
        return 1.0  # already there
    horizon_yrs = months / 12.0

    # Portfolio drift = sum(w_i × r_i) over the horizon (linear approx).
    invested_weight = sum(w for w, _, _ in weights)
    mu = sum(w * (r / 100.0) for w, r, _ in weights) * horizon_yrs

    # Portfolio variance (uncorrelated assumption — consistent with Phase 3 VaR).
    var_total = sum((w ** 2) * ((v / 100.0) ** 2) for w, _, v in weights) * horizon_yrs
    sigma = math.sqrt(max(var_total, 1e-9))

    if sigma <= 0:
        # Pure cash — P depends only on whether μ alone gets there
        return 1.0 if (1.0 + mu) * v0 >= target else 0.0

    # P(final/v0 ≥ target/v0) under Normal(1+μ, σ)
    z = (target / v0 - (1.0 + mu)) / sigma
    # 1 - Φ(z) using erf
    return float(0.5 * (1.0 - math.erf(z / math.sqrt(2.0))))


# Convert a prediction's signal_score + horizon into an annualised expected
# return estimate suitable for _p_hit_target. Mirrors orchestrator's
# SCORE_TO_12M_RETURN anchors but is purely a Signals-tab approximation since
# this engine doesn't call the orchestrator.
def _expected_return_from_score(signal_score: float, confidence: str) -> float:
    """Annualised expected return % given a signal score and confidence."""
    if signal_score is None:
        return 0.0
    # Anchor table — must stay in sync with orchestrator.SCORE_TO_12M_RETURN
    anchors = [(0, -30.0), (30, -15.0), (50, -2.0), (60, 4.0), (70, 10.0), (80, 18.0), (100, 28.0)]
    s = max(0.0, min(100.0, float(signal_score)))
    base = anchors[-1][1]
    for (s0, r0), (s1, r1) in zip(anchors, anchors[1:]):
        if s0 <= s <= s1:
            frac = (s - s0) / (s1 - s0) if s1 > s0 else 0.0
            base = r0 + frac * (r1 - r0)
            break
    # Confidence multiplier: low=0.6, medium=0.8, high=1.0 (matches calibration logic)
    conf_mult = {"high": 1.0, "medium": 0.8, "low": 0.6}.get(confidence, 0.8)
    return base * conf_mult


SP500_TICKERS = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "TSLA", "BRK-B", "JPM", "JNJ",
    "V", "PG", "UNH", "HD", "MA", "MRK", "ABBV", "CVX", "LLY", "PEP",
    "KO", "AVGO", "COST", "MCD", "TMO", "ACN", "WMT", "DHR", "BAC", "ADBE",
    "CRM", "NEE", "TXN", "PM", "ORCL", "LIN", "RTX", "QCOM", "AMD", "HON",
    "AMGN", "IBM", "CAT", "SBUX", "GS", "SPGI", "BLK", "AXP", "ISRG", "GILD",
    "XOM", "ABT", "PFE", "T", "VZ", "F", "GM", "GE", "BA", "LMT",
    "NOC", "GD", "MMM", "DE", "EMR", "ITW", "DOW", "SHW", "ECL", "APD",
    "FCX", "SLB", "HAL", "USB", "TFC", "PNC", "WFC", "C", "MS", "ICE",
    "CME", "MCO", "COF", "ALL", "PGR", "MET", "PRU", "AFL", "CB", "DUK",
    "SO", "AEP", "D", "WEC", "AMT", "PLD", "SPG", "EQIX", "CCI", "PSA",
    "DLR", "O", "WELL", "HCA", "CVS", "MCK", "BMY", "BIIB", "REGN", "VRTX",
    "MDT", "SYK", "BSX", "EW", "DXCM", "NFLX", "SCHW", "BK", "STT", "TROW",
    "PANW", "CRWD", "ZS", "NOW", "INTU", "SNOW", "PLTR", "NET", "DDOG", "WDAY",
]

NASDAQ100_TICKERS = [
    "AAPL", "MSFT", "NVDA", "AMZN", "META", "TSLA", "GOOGL", "AVGO", "COST", "NFLX",
    "ASML", "TMUS", "AMD", "PEP", "LIN", "CSCO", "ADBE", "QCOM", "TXN", "INTU",
    "AMGN", "HON", "AMAT", "ISRG", "BKNG", "VRTX", "ADP", "PANW", "ADI", "MU",
    "GILD", "LRCX", "SBUX", "MELI", "REGN", "KLAC", "MDLZ", "SNPS", "CDNS", "PYPL",
    "CTAS", "ORLY", "FTNT", "CEG", "MNST", "MRVL", "CRWD", "MAR", "ABNB", "PCAR",
    "NXPI", "ROP", "WDAY", "IDXX", "CPRT", "ODFL", "PAYX", "MCHP", "FAST", "DXCM",
    "EA", "VRSK", "BIIB", "TTWO", "ON", "ILMN", "ZS", "ANSS", "XEL", "KDP",
    "EXC", "TEAM", "DDOG", "ZM", "MRNA", "GEHC", "TTD", "CTSH", "INTC", "FISV",
    "WBD", "ENPH", "ALGN", "DLTR", "NTES", "FANG",
]

FTSE100_TICKERS = [
    "AAL.L", "ABF.L", "ADM.L", "AHT.L", "ANTO.L", "AZN.L", "BA.L", "BARC.L",
    "BATS.L", "BHPB.L", "BP.L", "BVIC.L", "CCH.L", "CPG.L", "CRH.L", "DCC.L",
    "DGE.L", "DPLM.L", "EDV.L", "ENT.L", "EXPN.L", "FERG.L", "FLTR.L", "FRES.L",
    "GLEN.L", "GSK.L", "HIK.L", "HLMA.L", "HSBA.L", "HWDN.L", "IAG.L", "IHG.L",
    "III.L", "IMB.L", "INF.L", "ITRK.L", "JD.L", "KGF.L", "LAND.L", "LGEN.L",
    "LLOY.L", "LMP.L", "LSE.L", "LTIM.L", "MKS.L", "MNDI.L", "MNG.L", "MRO.L",
    "NG.L", "NWG.L", "NXT.L", "OCDO.L", "PHNX.L", "PRU.L", "PSH.L", "PSN.L",
    "PSON.L", "REL.L", "RIO.L", "RKT.L", "RMV.L", "RR.L", "RS1.L", "SBRY.L",
    "SDR.L", "SGE.L", "SHEL.L", "SKG.L", "SMDS.L", "SMIN.L", "SMT.L", "SN.L",
    "SPX.L", "SSE.L", "STAN.L", "SVT.L", "TSCO.L", "TW.L", "ULVR.L", "UTG.L",
    "UU.L", "VOD.L", "WEIR.L", "WPP.L", "WTB.L",
]

FTSE250_TICKERS = [
    "AJB.L", "BBOX.L", "BEZ.L", "BOWL.L", "BWY.L", "CARD.L", "CCC.L",
    "COA.L", "CTEC.L", "DARK.L", "DFS.L", "DLG.L", "DNLM.L", "DOM.L",
    "DOCS.L", "EMG.L", "ENOG.L", "FGP.L", "GRG.L", "HAS.L", "HFD.L",
    "HTG.L", "IBST.L", "INCH.L", "ITV.L", "JDW.L", "JET2.L", "KIE.L",
    "LRE.L", "MGNS.L", "MSLH.L", "MOON.L", "NCC.L", "OSB.L", "PAGE.L",
    "PETS.L", "PLUS.L", "PFD.L", "RSW.L", "SAFE.L", "SCT.L", "SRP.L",
    "SXS.L", "TRN.L", "TLW.L", "VCT.L", "VTY.L", "VSVS.L", "WKP.L", "WOSG.L",
]

UNIVERSE = list(dict.fromkeys(SP500_TICKERS + NASDAQ100_TICKERS + FTSE100_TICKERS + FTSE250_TICKERS + ["TSM", "BE"]))


_WIKI_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
}


def _wiki_constituents(url: str, symbol_headers: tuple[str, ...], min_rows: int = 50) -> list[str]:
    """Pull a Wikipedia constituents page and return the values from the first
    `wikitable` whose header row contains one of `symbol_headers`. Uses bs4
    directly because the runtime image has neither `lxml` nor `html5lib`, so
    `pandas.read_html` silently fails and returns nothing."""
    import requests
    from bs4 import BeautifulSoup

    html = requests.get(url, headers=_WIKI_HEADERS, timeout=15).text
    soup = BeautifulSoup(html, "html.parser")
    wanted = {h.lower() for h in symbol_headers}

    for table in soup.select("table.wikitable"):
        head_row = table.find("tr")
        if not head_row:
            continue
        headers = [th.get_text(strip=True).lower() for th in head_row.find_all(["th", "td"])]
        try:
            col_idx = next(i for i, h in enumerate(headers) if h in wanted)
        except StopIteration:
            continue

        values: list[str] = []
        for row in table.find_all("tr")[1:]:
            cells = row.find_all(["td", "th"])
            if col_idx >= len(cells):
                continue
            text = cells[col_idx].get_text(strip=True)
            if text and text not in {"-", "—"}:
                values.append(text)
        if len(values) >= min_rows:
            return values
    return []


def _fetch_sp500_from_wiki() -> list:
    try:
        rows = _wiki_constituents(
            "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies",
            ("symbol", "ticker"),
        )
        # yfinance uses '-' instead of '.' (e.g. BRK-B not BRK.B).
        return [s.replace(".", "-").upper() for s in rows]
    except Exception as e:
        logger.warning(f"Failed to fetch S&P 500 from Wikipedia: {e}")
        return []


def _fetch_nasdaq100_from_wiki() -> list:
    try:
        rows = _wiki_constituents(
            "https://en.wikipedia.org/wiki/Nasdaq-100",
            ("ticker", "symbol"),
        )
        return [s.upper() for s in rows]
    except Exception as e:
        logger.warning(f"Failed to fetch NASDAQ 100 from Wikipedia: {e}")
        return []


def _fetch_ftse100_from_wiki() -> list:
    """Current FTSE 100 constituents with the yfinance-compatible `.L` suffix."""
    try:
        rows = _wiki_constituents(
            "https://en.wikipedia.org/wiki/FTSE_100_Index",
            ("ticker", "epic", "symbol", "code"),
            min_rows=50,
        )
        cleaned: list[str] = []
        for sym in rows:
            s = sym.strip().upper().replace(" ", "")
            if not s:
                continue
            if "." not in s:
                s += ".L"
            cleaned.append(s)
        return cleaned
    except Exception as e:
        logger.warning(f"Failed to fetch FTSE 100 from Wikipedia: {e}")
        return []


def _fetch_ftse250_from_wiki() -> list:
    """Current FTSE 250 constituents with the yfinance-compatible `.L` suffix.
    Falls back to an empty list so the caller can keep the hardcoded seed."""
    try:
        rows = _wiki_constituents(
            "https://en.wikipedia.org/wiki/FTSE_250_Index",
            ("ticker", "epic", "symbol", "code"),
        )
        cleaned: list[str] = []
        for sym in rows:
            s = sym.strip().upper().replace(" ", "")
            if not s:
                continue
            if "." not in s:
                s += ".L"
            cleaned.append(s)
        return cleaned
    except Exception as e:
        logger.warning(f"Failed to fetch FTSE 250 from Wikipedia: {e}")
        return []

TICKER_NAMES = {
    "AAPL": "Apple Inc.", "MSFT": "Microsoft Corporation", "GOOGL": "Alphabet Inc.",
    "AMZN": "Amazon.com Inc.", "NVDA": "NVIDIA Corporation", "META": "Meta Platforms Inc.",
    "TSLA": "Tesla Inc.", "BRK-B": "Berkshire Hathaway Inc.", "JPM": "JPMorgan Chase & Co.",
    "JNJ": "Johnson & Johnson", "V": "Visa Inc.", "PG": "Procter & Gamble Co.",
    "UNH": "UnitedHealth Group Inc.", "HD": "The Home Depot Inc.", "MA": "Mastercard Inc.",
    "MRK": "Merck & Co. Inc.", "ABBV": "AbbVie Inc.", "CVX": "Chevron Corporation",
    "LLY": "Eli Lilly and Company", "PEP": "PepsiCo Inc.", "KO": "The Coca-Cola Company",
    "AVGO": "Broadcom Inc.", "COST": "Costco Wholesale Corporation", "MCD": "McDonald's Corporation",
    "TMO": "Thermo Fisher Scientific", "ACN": "Accenture plc", "WMT": "Walmart Inc.",
    "DHR": "Danaher Corporation", "BAC": "Bank of America Corp.", "ADBE": "Adobe Inc.",
    "CRM": "Salesforce Inc.", "NEE": "NextEra Energy Inc.", "TXN": "Texas Instruments Inc.",
    "PM": "Philip Morris International", "ORCL": "Oracle Corporation", "LIN": "Linde plc",
    "RTX": "RTX Corporation", "QCOM": "Qualcomm Incorporated", "AMD": "Advanced Micro Devices",
    "HON": "Honeywell International", "AMGN": "Amgen Inc.", "IBM": "IBM Corporation",
    "CAT": "Caterpillar Inc.", "SBUX": "Starbucks Corporation", "GS": "Goldman Sachs Group",
    "SPGI": "S&P Global Inc.", "BLK": "BlackRock Inc.", "AXP": "American Express Co.",
    "ISRG": "Intuitive Surgical Inc.", "GILD": "Gilead Sciences Inc.", "CI": "The Cigna Group",
    "TSM": "Taiwan Semiconductor Manufacturing Company Limited",
    "BE": "Bloom Energy Corporation",
}

SEARCH_ALIASES = {
    "TSMC": "TSM",
    "TAIWAN SEMICONDUCTOR": "TSM",
    "TAIWAN SEMICONDUCTOR MANUFACTURING": "TSM",
    "GOOGLE": "GOOGL",
    "FACEBOOK": "META",
    "BLOOM ENERGY": "BE",
    "BLOOM": "BE",
}

MACRO_SYMBOLS = {
    "SPY": "S&P 500 ETF",
    "QQQ": "NASDAQ ETF",
    "^VIX": "Volatility Index",
    "^TNX": "10Y Treasury Yield",
    "^DJI": "Dow Jones",
    "GLD": "Gold ETF",
    "UUP": "USD Index ETF",
}

RSS_FEEDS = [
    ("Yahoo Finance", "https://finance.yahoo.com/rss/topstories"),
    ("MarketWatch", "https://feeds.content.dowjones.io/public/rss/mw_realtimeheadlines"),
    ("Seeking Alpha", "https://seekingalpha.com/market_currents.xml"),
]

# In-memory state for monitoring
price_cache: dict[str, float] = {}       # ticker -> last seen price
alert_cooldown: dict[str, datetime] = {} # alert key -> last alert time
alert_price_cache: dict[str, float] = {}  # "ACTION:ticker" -> price at last sent alert
monitor_status = {"last_check": None, "active": False, "checks_run": 0}
recommendation_alert_snapshot: dict[str, object] = {"generated_at": None, "data": None}
thesis_scheduler_status = {
    "enabled": THESIS_AUTO_RUN_ENABLED,
    "last_run": None,
    "active": False,
    "runs_started": 0,
    "last_error": None,
}
evaluation_scheduler_status = {
    "enabled": EVALUATION_AUTO_RUN_ENABLED,
    "last_run": None,
    "active": False,
    "runs_started": 0,
    "last_evaluated_count": None,
    "last_prediction_evaluated_count": None,
    "last_error": None,
}
prediction_scheduler_status = {
    "enabled": True,
    "last_run": None,
    "active": False,
    "runs_started": 0,
    "last_error": None,
}
monitor_scheduler_status = {
    "enabled": True,
    "last_run": None,
    "active": False,
    "runs_started": 0,
    "last_error": None,
}
_bg_consecutive_failures: dict[str, int] = {"thesis": 0, "evaluation": 0}
_BG_ALERT_THRESHOLD = 3

def _maybe_alert_bg_failure(job: str, error: str) -> None:
    """Fire a WhatsApp alert after _BG_ALERT_THRESHOLD consecutive failures."""
    _bg_consecutive_failures[job] = _bg_consecutive_failures.get(job, 0) + 1
    count = _bg_consecutive_failures[job]
    if count >= _BG_ALERT_THRESHOLD:
        try:
            from sentiment_agent import send_whatsapp
            send_whatsapp(
                f"[StockLens] Scheduled {job} job has failed {count} times in a row.\nLast error: {error}"
            )
            logger.warning("[BG] Alert fired: %s job failed %d consecutive times", job, count)
        except Exception as exc:
            logger.error("[BG] Could not send failure alert: %s", exc)

def _reset_bg_failures(job: str) -> None:
    _bg_consecutive_failures[job] = 0
_auto_thesis_running = False
_auto_evaluation_running = False

# yfinance info cache — 5 minute TTL to avoid redundant network calls
_info_cache: dict[str, tuple[dict, datetime]] = {}
_INFO_TTL = 300  # seconds

# Portfolio price cache — refresh held-stock prices at most every 15 minutes
_portfolio_price_cache: dict[str, tuple[float, datetime]] = {}
_PORTFOLIO_PRICE_TTL = 900  # seconds

async def get_info(ticker: str) -> dict:
    now = datetime.now(timezone.utc)
    if ticker in _info_cache:
        cached, ts = _info_cache[ticker]
        if (now - ts).total_seconds() < _INFO_TTL:
            return cached

    def _fetch():
        t = yf.Ticker(ticker)
        info = dict(t.info) if t.info else {}
        # Patch with the freshest available price via fast_info (more reliable than .info)
        try:
            fi = t.fast_info
            live_price = getattr(fi, "last_price", None)
            if live_price and float(live_price) > 0:
                info["currentPrice"] = float(live_price)
                info["regularMarketPrice"] = float(live_price)
        except Exception:
            pass
        return info

    info = await asyncio.to_thread(_fetch)
    _info_cache[ticker] = (info, now)
    return info


async def get_info_with_timeout(ticker: str, timeout_sec: float = SEARCH_INFO_TIMEOUT_SEC) -> dict:
    return await asyncio.wait_for(get_info(ticker), timeout=timeout_sec)


# ── File helpers ──────────────────────────────────────────────────────────────

def load_watchlist() -> list[str]:
    if WATCHLIST_FILE.exists():
        try:
            raw = json.loads(WATCHLIST_FILE.read_text())
            valid = []
            for t in raw:
                try:
                    valid.append(_validate_ticker(t))
                except Exception:
                    logger.warning("Watchlist: skipping invalid ticker %r", t)
            return valid
        except Exception as exc:
            logger.error("Corrupt watchlist file, returning empty: %s", exc)
    return []

def save_watchlist(tickers: list[str]):
    _atomic_write(WATCHLIST_FILE, json.dumps(tickers))


def sanitize_jsonable(value):
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, dict):
        return {k: sanitize_jsonable(v) for k, v in value.items()}
    if isinstance(value, list):
        return [sanitize_jsonable(v) for v in value]
    return value


def prediction_direction(predicted_pct: Optional[float]) -> str:
    if predicted_pct is None:
        return "pending"
    if predicted_pct >= 0.35:
        return "bullish"
    if predicted_pct <= -0.35:
        return "bearish"
    return "neutral"


def prediction_score(predicted_pct: Optional[float], confidence: str = "medium") -> int | None:
    if predicted_pct is None:
        return None
    conf_bonus = {"low": 0, "medium": 4, "high": 8}.get((confidence or "medium").lower(), 4)
    base = 50 + max(-35, min(35, predicted_pct * 14))
    if base > 50:
        base += conf_bonus
    elif base < 50:
        base -= conf_bonus
    return int(max(0, min(100, round(base))))


def legacy_predicted_pct(direction: str | None, score: Optional[float]) -> float:
    if score is None:
        return 0.0
    magnitude = max(0.0, min(3.5, abs(float(score) - 50.0) / 14.0))
    label = (direction or "neutral").lower()
    if label == "bullish":
        return round(magnitude, 2)
    if label == "bearish":
        return round(-magnitude, 2)
    return 0.0

PREDICTION_HORIZON_MONTHS = (3, 6, 12, 24, 36)

def prediction_horizon_returns(
    predicted_pct: Optional[float],
    direction: str | None = None,
    score: Optional[float] = None,
    confidence: str = "medium",
) -> dict[str, float | None]:
    if predicted_pct is None and score is None:
        return {f"predicted_{months}m_pct": None for months in PREDICTION_HORIZON_MONTHS}

    label = (direction or prediction_direction(predicted_pct)).lower()
    conf_mult = {"low": 0.85, "medium": 1.0, "high": 1.15}.get((confidence or "medium").lower(), 1.0)
    effective_score = score if score is not None else prediction_score(predicted_pct, confidence)
    strength = 0.0 if effective_score is None else min(1.0, abs(float(effective_score) - 50.0) / 35.0)
    daily_signal = 0.0 if predicted_pct is None else min(1.0, abs(float(predicted_pct)) / 3.5)
    blend = max(strength, daily_signal)
    annual_return = 0.0
    if label == "bullish":
        annual_return = (6.0 + 24.0 * blend) * conf_mult
    elif label == "bearish":
        annual_return = -(4.0 + 18.0 * blend) * conf_mult

    result: dict[str, float | None] = {}
    for months in PREDICTION_HORIZON_MONTHS:
        if annual_return <= -99.0:
            projected = -95.0
        else:
            projected = ((1 + (annual_return / 100.0)) ** (months / 12.0) - 1) * 100.0
        result[f"predicted_{months}m_pct"] = round(projected, 2)
    return result


def prediction_short_horizon_returns(
    predicted_pct: Optional[float],
    direction: str | None = None,
    score: Optional[float] = None,
    confidence: str = "medium",
) -> dict[str, float | None]:
    """Create 1D/1W/1M forecasts from the daily signal and 3M curve."""
    if predicted_pct is None and score is None:
        return {"predicted_1d_pct": None, "predicted_1w_pct": None, "predicted_1m_pct": None}

    medium_horizons = prediction_horizon_returns(predicted_pct, direction, score, confidence)
    three_month = medium_horizons.get("predicted_3m_pct")
    one_week = None
    one_month = None
    try:
        three_month_float = float(three_month)
        if three_month_float > -99.0:
            one_month = ((1 + three_month_float / 100.0) ** (1 / 3.0) - 1) * 100.0
            one_week = ((1 + one_month / 100.0) ** (7 / 30.0) - 1) * 100.0
    except (TypeError, ValueError):
        pass

    return {
        "predicted_1d_pct": round(float(predicted_pct), 2) if predicted_pct is not None else None,
        "predicted_1w_pct": round(one_week, 2) if one_week is not None else None,
        "predicted_1m_pct": round(one_month, 2) if one_month is not None else None,
    }


def prediction_hit(prediction: dict) -> bool | None:
    actual_pct = prediction.get("actual_pct")
    if actual_pct is None:
        return None
    direction = prediction.get("direction") or prediction_direction(prediction.get("predicted_pct"))
    actual_direction = prediction_direction(actual_pct)
    if direction == "neutral":
        return abs(actual_pct) < 0.35
    return direction == actual_direction


def normalize_prediction(pred: dict) -> dict:
    normalized = dict(pred)
    predicted_pct = normalized.get("predicted_pct")
    try:
        predicted_pct = round(float(predicted_pct), 2) if predicted_pct is not None else None
    except (TypeError, ValueError):
        predicted_pct = None

    confidence = str(normalized.get("confidence") or "medium").lower()
    if confidence not in {"low", "medium", "high"}:
        confidence = "medium"

    direction = str(normalized.get("direction") or "").lower()
    if direction not in {"bullish", "neutral", "bearish"}:
        direction = prediction_direction(predicted_pct)

    score = normalized.get("score")
    try:
        score = int(round(float(score))) if score is not None else None
    except (TypeError, ValueError):
        score = None
    if score is None:
        score = prediction_score(predicted_pct, confidence)

    normalized["predicted_pct"] = predicted_pct
    normalized["confidence"] = confidence
    normalized["direction"] = direction
    normalized["score"] = score

    horizons = {
        **prediction_short_horizon_returns(predicted_pct, direction, score, confidence),
        **prediction_horizon_returns(predicted_pct, direction, score, confidence),
    }
    for key, value in horizons.items():
        if normalized.get(key) is None:
            normalized[key] = value
    return normalized


def get_sentiment_scanner_path() -> Path:
    configured = os.getenv("SENTIMENT_SCANNER_PATH", "").strip()
    if configured:
        return Path(configured).expanduser()
    return Path(__file__).parent / "sentiment_scanner.py"


def run_sentiment_scanner(ticker: Optional[str] = None, watchlist_only: bool = False, refresh: bool = False) -> dict:
    script_path = get_sentiment_scanner_path()
    if not script_path.exists():
        watchlist = load_watchlist()
        if watchlist_only:
            return {"status": "ok", "watchlist": watchlist}
        if ticker:
            return {
                "status": "ok",
                "ticker": ticker,
                "message": f"Would scan ticker {ticker} (sentiment scanner not found at {script_path}).",
                "watchlist": watchlist,
            }
        return {
            "status": "ok",
            "message": f"Would scan full watchlist (sentiment scanner not found at {script_path}).",
            "watchlist": watchlist,
        }

    cache_key = ticker.upper() if ticker else ("__watchlist_only__" if watchlist_only else "__all__")
    ttl = _SENTIMENT_TICKER_TTL if ticker else _SENTIMENT_WATCHLIST_TTL
    if not refresh:
        entry = _sentiment_cache.get(cache_key)
        if entry and (datetime.now() - entry["ts"]).total_seconds() < ttl:
            return {**entry["data"], "cached": True, "scanned_at": entry["ts"].isoformat()}

    cmd = [sys.executable, str(script_path)]
    if watchlist_only:
        cmd.append("--list")
    elif ticker:
        cmd.extend(["--ticker", ticker])

    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=240)
        try:
            parsed = json.loads(proc.stdout)
        except Exception:
            parsed = {}
        now = datetime.now()
        result = {"status": "ok", "cached": False, "scanned_at": now.isoformat(), **parsed}
        _sentiment_cache[cache_key] = {"ts": now, "data": result}
        return result
    except subprocess.TimeoutExpired:
        raise HTTPException(status_code=504, detail="Sentiment scan timed out")
    except Exception as e:
        logger.error("SENTIMENT_SCAN_FAIL: %s", e)
        raise HTTPException(status_code=500, detail="Sentiment scan failed")


def load_predictions() -> list[dict]:
    if PREDICTIONS_FILE.exists():
        try:
            return json.loads(PREDICTIONS_FILE.read_text())
        except Exception as exc:
            logger.error("Corrupt predictions file, returning empty: %s", exc)
    return []

_predictions_cache: dict[str, object] = {
    "date": None,
    "mtime_ns": None,
    "data": None,
}
_screen_universe_cache: dict[str, tuple[list[dict], datetime]] = {}
_sentiment_cache: dict[str, dict] = {}  # key → {"ts": datetime, "data": dict}
_SENTIMENT_WATCHLIST_TTL = 1800   # 30 min
_SENTIMENT_TICKER_TTL   = 900     # 15 min
_earnings_calendar_cache: dict[int, dict] = {}  # days_ahead → {"data": list, "fetched_at": datetime}
_EARNINGS_CALENDAR_TTL = 14400    # 4 hours — dates don't change intraday
_PREWARM_BATCH = 25          # tickers per batch
_PREWARM_DELAY = 1.2         # seconds between batches (avoids yfinance rate-limit)
# Match the cache TTL to the prewarm cadence (6h, see scheduler in startup()).
# Each cache miss otherwise fans out hundreds of concurrent yfinance calls,
# which Yahoo rate-limits, leaving the cache populated with a bad partial set
# for the next TTL window. Letting the gently-batched prewarm dominate gives
# the screener a steady, full ticker count.
_SCREEN_TTL    = 6 * 60 * 60  # 6 hours


def _build_universe_row(ticker: str, info: dict) -> dict | None:
    """Convert a yfinance info dict into a screener row. Returns None if unusable."""
    try:
        price       = info.get("currentPrice") or info.get("regularMarketPrice")
        market_cap  = info.get("marketCap")
        fcf         = info.get("freeCashflow")
        rev_growth_raw = info.get("revenueGrowth")
        return {
            "ticker":    ticker,
            "name":      info.get("shortName", ticker),
            "sector":    info.get("sector", ""),
            "price":     round(price, 2) if price else None,
            "pe":        round(info["trailingPE"], 2) if info.get("trailingPE") else None,
            "peg":       round(info["pegRatio"], 2) if info.get("pegRatio") else None,
            "pb":        round(info["priceToBook"], 2) if info.get("priceToBook") else None,
            "ev_ebitda": round(info["enterpriseToEbitda"], 2) if info.get("enterpriseToEbitda") else None,
            "fcf_yield": calc_fcf_yield(fcf, market_cap),
            "rev_growth": round(rev_growth_raw * 100, 1) if rev_growth_raw is not None else None,
            "market_cap": market_cap,
            "volume":    info.get("averageVolume"),
        }
    except Exception:
        return None


# ── yahooquery bulk provider (alternative to per-ticker yfinance.info) ───────
# Pull every ticker's modules in a handful of HTTP calls instead of one per
# ticker, which sidesteps Yahoo's per-call throttling.

_YQ_CHUNK = 50   # symbols per yahooquery request


def _yq_pick(d: dict | None, key: str):
    """yahooquery returns either raw numbers or {raw, fmt} dicts depending on
    field/version. Unwrap both shapes."""
    if not isinstance(d, dict):
        return None
    v = d.get(key)
    if isinstance(v, dict) and "raw" in v:
        return v.get("raw")
    return v


def _yq_build_row(ticker: str, modules: dict) -> dict | None:
    if not isinstance(modules, dict):
        return None
    try:
        price_m = modules.get("price") or {}
        sd      = modules.get("summaryDetail") or {}
        ks      = modules.get("defaultKeyStatistics") or {}
        fd      = modules.get("financialData") or {}
        ap      = modules.get("assetProfile") or modules.get("summaryProfile") or {}

        price       = _yq_pick(price_m, "regularMarketPrice") or _yq_pick(sd, "regularMarketPrice") or _yq_pick(fd, "currentPrice")
        market_cap  = _yq_pick(price_m, "marketCap") or _yq_pick(sd, "marketCap")
        fcf         = _yq_pick(fd, "freeCashflow")
        rev_growth  = _yq_pick(fd, "revenueGrowth")
        pe          = _yq_pick(sd, "trailingPE")
        peg         = _yq_pick(ks, "pegRatio") or _yq_pick(sd, "pegRatio")
        pb          = _yq_pick(ks, "priceToBook") or _yq_pick(sd, "priceToBook")
        ev_ebitda   = _yq_pick(ks, "enterpriseToEbitda")
        volume      = _yq_pick(sd, "averageVolume") or _yq_pick(sd, "averageVolume10days")
        name        = price_m.get("shortName") or price_m.get("longName") or ticker
        sector      = ap.get("sector", "") if isinstance(ap, dict) else ""

        return {
            "ticker":     ticker,
            "name":       name,
            "sector":     sector,
            "price":      round(price, 2) if isinstance(price, (int, float)) else None,
            "pe":         round(pe, 2) if isinstance(pe, (int, float)) else None,
            "peg":        round(peg, 2) if isinstance(peg, (int, float)) else None,
            "pb":         round(pb, 2) if isinstance(pb, (int, float)) else None,
            "ev_ebitda":  round(ev_ebitda, 2) if isinstance(ev_ebitda, (int, float)) else None,
            "fcf_yield":  calc_fcf_yield(fcf, market_cap),
            "rev_growth": round(rev_growth * 100, 1) if isinstance(rev_growth, (int, float)) else None,
            "market_cap": market_cap,
            "volume":     volume,
        }
    except Exception:
        return None


def _yq_fetch_chunk_sync(symbols: list[str]) -> dict:
    """Blocking yahooquery call. Returns {ticker: {module: data}}. Errors are
    swallowed so a chunk failure doesn't poison the whole pool."""
    try:
        from yahooquery import Ticker  # local import keeps cold-start light
        t = Ticker(symbols, asynchronous=True, max_workers=8, validate=False)
        data = t.get_modules([
            "price", "summaryDetail", "defaultKeyStatistics",
            "financialData", "assetProfile",
        ])
        return data if isinstance(data, dict) else {}
    except Exception as exc:
        logger.warning(f"yahooquery chunk failed ({len(symbols)} symbols): {exc}")
        return {}


async def _build_universe_rows_yahooquery(pool: list[str]) -> list[dict]:
    rows: list[dict] = []
    for i in range(0, len(pool), _YQ_CHUNK):
        chunk = pool[i: i + _YQ_CHUNK]
        data = await asyncio.to_thread(_yq_fetch_chunk_sync, chunk)
        for sym in chunk:
            row = _yq_build_row(sym, data.get(sym) if isinstance(data, dict) else None)
            if row:
                rows.append(row)
        if i + _YQ_CHUNK < len(pool):
            await asyncio.sleep(_PREWARM_DELAY)
    return rows


async def _build_universe_rows_yfinance(pool: list[str]) -> list[dict]:
    """Fetch yfinance info for every ticker in `pool` in small concurrent
    batches and return the screener rows. Batching + the inter-batch delay
    are what keep the screener inside Yahoo's rate-limit envelope — fanning
    all 500+ tickers out concurrently triggers 429s and silently empties
    most of the result set."""
    rows: list[dict] = []
    for i in range(0, len(pool), _PREWARM_BATCH):
        batch = pool[i: i + _PREWARM_BATCH]
        infos = await asyncio.gather(
            *[get_info_with_timeout(t, UNIVERSE_INFO_TIMEOUT_SEC) for t in batch],
            return_exceptions=True,
        )
        for ticker, info in zip(batch, infos):
            if isinstance(info, Exception) or not isinstance(info, dict):
                continue
            row = _build_universe_row(ticker, info)
            if row:
                rows.append(row)
        # Skip the pacing delay after the final batch.
        if i + _PREWARM_BATCH < len(pool):
            await asyncio.sleep(_PREWARM_DELAY)
    return rows


async def _build_universe_rows(pool: list[str]) -> list[dict]:
    """Dispatch to whichever upstream provider is configured."""
    if UNIVERSE_PROVIDER == "yahooquery":
        return await _build_universe_rows_yahooquery(pool)
    return await _build_universe_rows_yfinance(pool)


async def _prewarm_universe_cache():
    """Fetch universe data in small batches so the screener cache is always full."""
    await asyncio.sleep(5)          # let startup finish first
    index_pools = {
        "sp500":      SP500_TICKERS,
        "nasdaq100":  NASDAQ100_TICKERS,
        "ftse100":    FTSE100_TICKERS,
        "ftse250":    FTSE250_TICKERS,
        "__all__":    UNIVERSE,
    }
    for pool_key, pool in index_pools.items():
        rows = await _build_universe_rows(pool)
        _screen_universe_cache[pool_key] = (rows, datetime.now(timezone.utc))
        logger.info("Screener cache pre-warmed: %s (%d rows)", pool_key, len(rows))


def invalidate_predictions_cache() -> None:
    _predictions_cache["date"] = None
    _predictions_cache["mtime_ns"] = None
    _predictions_cache["data"] = None


def save_predictions(predictions: list[dict]):
    invalidate_predictions_cache()
    _atomic_write(PREDICTIONS_FILE, json.dumps(predictions[:1000], indent=2))


_recommendation_jobs: dict[str, dict] = {}
_recommendation_stage_averages_ms: dict[str, int] = {
    "load_predictions": 1200,
    "load_portfolio": 800,
    "fetch_market_data": 9000,
    "fetch_histories": 2500,
    "build_recommendations": 1200,
    "finalize": 400,
}
_recommendation_stage_weights: dict[str, int] = {
    "load_predictions": 8,
    "load_portfolio": 8,
    "fetch_market_data": 36,
    "fetch_histories": 18,
    "build_recommendations": 24,
    "finalize": 6,
}


def _recommendation_eta_ms(current_stage: str) -> int:
    stages = list(_recommendation_stage_averages_ms.keys())
    if current_stage not in stages:
        return sum(_recommendation_stage_averages_ms.values())
    idx = stages.index(current_stage)
    return sum(_recommendation_stage_averages_ms[s] for s in stages[idx:])


def _update_recommendation_job(job_id: str, *, status: Optional[str] = None, stage: Optional[str] = None,
                               message: Optional[str] = None, result: Optional[dict] = None,
                               error: Optional[str] = None, completed: Optional[int] = None,
                               total: Optional[int] = None) -> None:
    job = _recommendation_jobs.get(job_id)
    if not job:
        return
    if status is not None:
        job["status"] = status
    if stage is not None:
        job["stage"] = stage
        job["eta_ms"] = _recommendation_eta_ms(stage)
    if message is not None:
        job["message"] = message
    if total is not None:
        job["total"] = total
    if completed is not None:
        job["completed"] = completed
    if result is not None:
        job["result"] = result
    if error is not None:
        job["error"] = error
    elapsed_ms = int((datetime.now(timezone.utc) - job["started_at"]).total_seconds() * 1000)
    job["elapsed_ms"] = elapsed_ms


def _recommendation_percent(stage: str, completed: int = 0, total: int = 0) -> int:
    stages = list(_recommendation_stage_weights.keys())
    done_weight = 0
    for s in stages:
        if s == stage:
            break
        done_weight += _recommendation_stage_weights[s]
    stage_weight = _recommendation_stage_weights.get(stage, 0)
    stage_ratio = 0.0
    if total and total > 0:
        stage_ratio = max(0.0, min(1.0, completed / total))
    elif stage in {"load_predictions", "load_portfolio", "finalize"}:
        stage_ratio = 1.0
    pct = done_weight + stage_weight * stage_ratio
    return int(max(0, min(100, round(pct))))


def _record_recommendation_stage_duration(stage: str, duration_ms: int) -> None:
    if stage not in _recommendation_stage_averages_ms:
        return
    current = _recommendation_stage_averages_ms[stage]
    _recommendation_stage_averages_ms[stage] = int((current * 0.7) + (duration_ms * 0.3))

def calc_fcf_yield(fcf, market_cap) -> Optional[float]:
    return round((fcf / market_cap) * 100, 2) if fcf and market_cap else None


# ── Quant helpers ──────────────────────────────────────────────────────────────

def compute_rsi(prices, period: int = 14) -> Optional[float]:
    """Compute RSI from a pandas Series of closing prices. Returns last RSI value."""
    try:
        delta = prices.diff()
        gain = delta.clip(lower=0).rolling(period).mean()
        loss = (-delta.clip(upper=0)).rolling(period).mean()
        rs = gain / loss
        rsi = 100 - 100 / (1 + rs)
        val = rsi.iloc[-1]
        return round(float(val), 1) if math.isfinite(float(val)) else None
    except Exception:
        return None


def _score_band(value, bands: list) -> int:
    """Return a score from a list of (threshold, points) bands, highest matching threshold wins.
    bands should be ordered descending: [(threshold, pts), ...]  e.g. [(8, 20), (5, 15), ...]
    For 'lower is better' metrics pass negative value."""
    for threshold, pts in bands:
        if value >= threshold:
            return pts
    return bands[-1][1] if bands else 0


def compute_factor_scores(info: dict, hist) -> dict:
    """Compute Value / Momentum / Quality / Growth factor scores (0-100 each).
    hist: yfinance history DataFrame with at least 14 rows."""
    def safe(key, default=None):
        v = info.get(key)
        return v if v is not None and isinstance(v, (int, float)) and math.isfinite(float(v)) else default

    price     = safe("currentPrice") or safe("regularMarketPrice") or 0.0
    mc        = safe("marketCap") or 0.0
    fcf       = safe("freeCashflow") or 0.0
    fcf_yield = (fcf / mc * 100) if mc > 0 and fcf > 0 else 0.0

    # ── VALUE ──────────────────────────────────────────────────────
    pe       = safe("trailingPE") or 0
    pb       = safe("priceToBook") or 0
    ev_ebitda = safe("enterpriseToEbitda") or 0
    peg      = safe("pegRatio") or 0

    v_pe   = _score_band(pe,       [(0.01, 20)] if pe == 0 else []) or \
             (20 if 0 < pe < 12 else 15 if pe < 18 else 10 if pe < 25 else 5 if pe < 35 else 0)
    v_pb   = (20 if 0 < pb < 1 else 15 if pb < 2 else 10 if pb < 3 else 5 if pb < 5 else 0) if pb else 10
    v_ev   = (20 if 0 < ev_ebitda < 7 else 15 if ev_ebitda < 11 else 10 if ev_ebitda < 16 else 5 if ev_ebitda < 22 else 0) if ev_ebitda else 10
    v_fcf  = (20 if fcf_yield > 8 else 15 if fcf_yield > 5 else 10 if fcf_yield > 3 else 5 if fcf_yield > 1 else 0)
    v_peg  = (20 if 0 < peg < 0.7 else 15 if peg < 1.1 else 10 if peg < 1.8 else 5 if peg < 2.5 else 0) if peg else 10
    value_score = min(100, v_pe + v_pb + v_ev + v_fcf + v_peg)

    # ── MOMENTUM ───────────────────────────────────────────────────
    rsi = None
    price_vs_50sma = 0.0
    day5_chg = 0.0
    w52_position = 50.0  # default midpoint

    if hist is not None and len(hist) >= 2:
        closes = hist["Close"].dropna()
        if len(closes) >= 15:
            rsi = compute_rsi(closes)
        if len(closes) >= 50:
            sma50 = float(closes.iloc[-50:].mean())
            price_vs_50sma = ((price - sma50) / sma50 * 100) if sma50 else 0.0
        if len(closes) >= 5:
            p5 = float(closes.iloc[-5])
            p0 = float(closes.iloc[-1])
            day5_chg = ((p0 - p5) / p5 * 100) if p5 else 0.0

    hi52 = safe("fiftyTwoWeekHigh") or price
    lo52 = safe("fiftyTwoWeekLow") or price
    if hi52 > lo52 and price:
        w52_position = (price - lo52) / (hi52 - lo52) * 100

    short_float = (safe("shortPercentOfFloat") or 0) * 100  # convert to %

    m_rsi    = (20 if rsi and 55 <= rsi <= 68 else 15 if rsi and (45 <= rsi < 55 or 68 < rsi <= 75)
                else 10 if rsi and 38 <= rsi < 45 else 5 if rsi and rsi < 38 else 5) if rsi else 10
    m_52w    = (20 if w52_position > 80 else 15 if w52_position > 60 else 10 if w52_position > 40 else 5 if w52_position > 20 else 2)
    m_sma50  = (20 if price_vs_50sma > 5 else 15 if price_vs_50sma > 2 else 10 if price_vs_50sma > -1 else 5 if price_vs_50sma > -5 else 0)
    m_5d     = (20 if day5_chg > 3 else 15 if day5_chg > 1 else 10 if day5_chg > -1 else 5 if day5_chg > -3 else 2)
    m_short  = (20 if short_float < 4 else 15 if short_float < 8 else 10 if short_float < 13 else 5 if short_float < 20 else 2)
    momentum_score = min(100, m_rsi + m_52w + m_sma50 + m_5d + m_short)

    # ── QUALITY ────────────────────────────────────────────────────
    roe          = (safe("returnOnEquity") or 0) * 100
    gross_margin = (safe("grossMargins") or 0) * 100
    net_margin   = (safe("profitMargins") or 0) * 100
    de_ratio     = safe("debtToEquity") or 0
    current_ratio = safe("currentRatio") or 1.5

    q_roe  = (20 if roe > 20 else 15 if roe > 14 else 10 if roe > 8 else 5 if roe > 2 else 0)
    q_gm   = (20 if gross_margin > 50 else 15 if gross_margin > 35 else 10 if gross_margin > 22 else 5 if gross_margin > 10 else 0)
    q_nm   = (20 if net_margin > 20 else 15 if net_margin > 12 else 10 if net_margin > 6 else 5 if net_margin > 1 else 0)
    q_de   = (20 if de_ratio < 30 else 15 if de_ratio < 60 else 10 if de_ratio < 120 else 5 if de_ratio < 200 else 0) if de_ratio else 15
    q_cr   = (20 if current_ratio > 2.5 else 15 if current_ratio > 1.8 else 10 if current_ratio > 1.2 else 5 if current_ratio > 0.8 else 2)
    quality_score = min(100, q_roe + q_gm + q_nm + q_de + q_cr)

    # ── GROWTH ─────────────────────────────────────────────────────
    rev_growth  = (safe("revenueGrowth") or 0) * 100
    eps_growth  = (safe("earningsGrowth") or 0) * 100
    op_margin   = (safe("operatingMargins") or 0) * 100
    trailing_pe = safe("trailingPE") or 0
    forward_pe  = safe("forwardPE") or 0

    g_rev   = (20 if rev_growth > 25 else 15 if rev_growth > 15 else 10 if rev_growth > 7 else 5 if rev_growth > 0 else 0)
    g_eps   = (20 if eps_growth > 25 else 15 if eps_growth > 15 else 10 if eps_growth > 7 else 5 if eps_growth > 0 else 0)
    g_fwdpe = (20 if forward_pe and trailing_pe and forward_pe < trailing_pe * 0.85
               else 15 if forward_pe and trailing_pe and forward_pe < trailing_pe * 0.95
               else 10 if not forward_pe or not trailing_pe else 5)
    g_opm   = (20 if op_margin > 25 else 15 if op_margin > 15 else 10 if op_margin > 8 else 5 if op_margin > 2 else 0)
    g_short = (20 if short_float < 4 else 12 if short_float < 10 else 8)  # low short = growth conviction
    growth_score = min(100, g_rev + g_eps + g_fwdpe + g_opm + g_short)

    composite = round((value_score + momentum_score + quality_score + growth_score) / 4, 1)

    return {
        "value":     value_score,
        "momentum":  momentum_score,
        "quality":   quality_score,
        "growth":    growth_score,
        "composite": composite,
        # sub-details for UI display
        "_rsi":            rsi,
        "_52w_position":   round(w52_position, 1),
        "_price_vs_50sma": round(price_vs_50sma, 2),
        "_fcf_yield":      round(fcf_yield, 2),
    }


def compute_volatility(hist) -> Optional[float]:
    """Annualised historical volatility (%) from a price history DataFrame."""
    try:
        closes = hist["Close"].dropna()
        if len(closes) < 5:
            return None
        log_ret = (closes / closes.shift(1)).apply(math.log).dropna()
        vol = float(log_ret.std()) * math.sqrt(252) * 100
        return round(vol, 1) if math.isfinite(vol) else None
    except Exception:
        return None


def compute_max_drawdown(hist) -> Optional[float]:
    """Maximum peak-to-trough drawdown (%) from a price history DataFrame."""
    try:
        closes = hist["Close"].dropna()
        if len(closes) < 2:
            return None
        peak = closes.cummax()
        dd = (closes - peak) / peak * 100
        return round(float(dd.min()), 1)
    except Exception:
        return None


def compute_dcf_valuation(info: dict) -> Optional[dict]:
    """Simple 5-year DCF + terminal value. Returns intrinsic value per share and margin of safety."""
    try:
        fcf    = info.get("freeCashflow") or 0
        shares = info.get("sharesOutstanding") or 0
        beta   = info.get("beta") or 1.0
        rev_g  = info.get("revenueGrowth") or 0.05
        price  = info.get("currentPrice") or info.get("regularMarketPrice") or 0

        if fcf <= 0 or shares <= 0 or price <= 0:
            return None

        wacc          = 0.045 + max(0.5, min(2.5, float(beta))) * 0.05
        terminal_g    = 0.025
        fcf_growth    = min(float(rev_g) * 0.7, 0.25)

        pv = sum(fcf * (1 + fcf_growth) ** t / (1 + wacc) ** t for t in range(1, 6))
        terminal_fcf  = fcf * (1 + fcf_growth) ** 5 * (1 + terminal_g)
        terminal_val  = terminal_fcf / (wacc - terminal_g)
        total_val     = pv + terminal_val / (1 + wacc) ** 5

        intrinsic_per_share = total_val / shares
        mos_pct = (intrinsic_per_share - price) / price * 100

        return {
            "intrinsic_per_share": round(intrinsic_per_share, 2),
            "margin_of_safety_pct": round(mos_pct, 1),
            "wacc_pct": round(wacc * 100, 1),
            "fcf_growth_assumed_pct": round(fcf_growth * 100, 1),
        }
    except Exception:
        return None


# Risk metrics cache
_risk_cache: dict = {"ts": None, "data": None}
_RISK_TTL = 1800  # 30 minutes

def load_alerts() -> list[dict]:
    if ALERTS_FILE.exists():
        try:
            return json.loads(ALERTS_FILE.read_text())
        except Exception as exc:
            logger.error("Corrupt alerts file, returning empty: %s", exc)
    return []

def save_alerts(alerts: list[dict]):
    _atomic_write(ALERTS_FILE, json.dumps(alerts, indent=2))

def append_alert(entry: dict):
    alerts = load_alerts()
    alerts.insert(0, entry)
    save_alerts(alerts[:500])  # keep latest 500


def load_portfolio() -> list[dict]:
    if PORTFOLIO_FILE.exists():
        try:
            return json.loads(PORTFOLIO_FILE.read_text())
        except Exception as exc:
            logger.error("Corrupt portfolio file, returning empty: %s", exc)
    return []

def save_portfolio(transactions: list[dict]):
    _atomic_write(PORTFOLIO_FILE, json.dumps(transactions, indent=2))

def compute_positions(transactions: list[dict]) -> dict:
    """Average cost basis P&L per ticker."""
    positions: dict[str, dict] = {}
    for tx in sorted(transactions, key=lambda x: x["timestamp"]):
        t = tx["ticker"]
        if t not in positions:
            positions[t] = {"shares": 0.0, "avg_cost": 0.0, "realised_pnl": 0.0, "name": tx.get("name", t)}
        pos = positions[t]
        if tx["type"] == "buy":
            total = pos["shares"] * pos["avg_cost"] + tx["qty"] * tx["price"]
            pos["shares"] += tx["qty"]
            pos["avg_cost"] = total / pos["shares"] if pos["shares"] > 0 else 0.0
        elif tx["type"] == "sell":
            sell_qty = min(tx["qty"], pos["shares"])
            pos["realised_pnl"] += (tx["price"] - pos["avg_cost"]) * sell_qty
            pos["shares"] -= sell_qty
            if pos["shares"] <= 0:
                pos["shares"] = 0.0
    return positions


def annotate_transactions_with_realised_pnl(transactions: list[dict]) -> list[dict]:
    """Attach realised P&L for sell trades using rolling average cost."""
    positions: dict[str, dict] = {}
    annotated: list[dict] = []

    for tx in sorted(transactions, key=lambda x: x["timestamp"]):
        item = dict(tx)
        ticker = item["ticker"]
        qty = float(item.get("qty", 0) or 0)
        price = float(item.get("price", 0) or 0)

        if ticker not in positions:
            positions[ticker] = {"shares": 0.0, "avg_cost": 0.0}
        pos = positions[ticker]

        if item["type"] == "buy":
            total = pos["shares"] * pos["avg_cost"] + qty * price
            pos["shares"] += qty
            pos["avg_cost"] = total / pos["shares"] if pos["shares"] > 0 else 0.0
            item["realised_pnl"] = None
        elif item["type"] == "sell":
            sell_qty = min(qty, pos["shares"])
            realised_pnl = (price - pos["avg_cost"]) * sell_qty
            item["realised_pnl"] = round(realised_pnl, 2)
            pos["shares"] -= sell_qty
            if pos["shares"] <= 0:
                pos["shares"] = 0.0
        else:
            item["realised_pnl"] = None

        annotated.append(item)

    return annotated


def compute_portfolio_state(transactions: list[dict], price_map: Optional[dict[str, float]] = None) -> dict:
    """Portfolio summary with ledger-style cash tracking."""
    settings = load_settings()
    initial_float = float(settings["initial_float"])
    positions = compute_positions(transactions)
    price_map = price_map or {}

    total_buy_cost = 0.0
    total_sell_proceeds = 0.0
    for tx in transactions:
        qty = float(tx.get("qty", 0) or 0)
        price = float(tx.get("price", 0) or 0)
        gross = qty * price
        if tx.get("type") == "buy":
            total_buy_cost += gross
        elif tx.get("type") == "sell":
            total_sell_proceeds += gross

    available_cash = initial_float - total_buy_cost + total_sell_proceeds
    total_invested = 0.0
    total_current = 0.0
    total_realised = 0.0

    for ticker, pos in positions.items():
        cost_basis = pos["shares"] * pos["avg_cost"]
        current_value = pos["shares"] * float(price_map.get(ticker, 0) or 0)
        total_invested += cost_basis
        total_current += current_value
        total_realised += pos["realised_pnl"]

    total_portfolio_value = available_cash + total_current
    total_unrealised_pnl = total_current - total_invested
    total_pnl = total_portfolio_value - initial_float

    return {
        "positions": positions,
        "summary": {
            "initial_float": round(initial_float, 2),
            "available_cash": round(available_cash, 2),
            "total_buy_cost": round(total_buy_cost, 2),
            "total_sell_proceeds": round(total_sell_proceeds, 2),
            "total_invested": round(total_invested, 2),
            "total_current_value": round(total_current, 2),
            "total_portfolio_value": round(total_portfolio_value, 2),
            "total_unrealised_pnl": round(total_unrealised_pnl, 2),
            "total_realised_pnl": round(total_realised, 2),
            "total_pnl": round(total_pnl, 2),
        },
    }


# ── Notifications ─────────────────────────────────────────────────────────────

def send_email(subject: str, body: str, to_email: Optional[str] = None) -> bool:
    smtp_host = os.getenv("SMTP_HOST", "smtp.gmail.com")
    smtp_port = int(os.getenv("SMTP_PORT", "587"))
    smtp_user = os.getenv("SMTP_USER", "")
    smtp_pass = os.getenv("SMTP_PASS", "")
    target_email = (to_email or os.getenv("ALERT_EMAIL", "")).strip()

    if not all([smtp_user, smtp_pass, target_email]):
        return False

    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = smtp_user
        msg["To"] = target_email
        msg.attach(MIMEText(body, "plain"))

        with smtplib.SMTP(smtp_host, smtp_port) as server:
            server.ehlo()
            server.starttls()
            server.login(smtp_user, smtp_pass)
            server.send_message(msg)
        return True
    except Exception as e:
        print(f"[Email] Failed: {e}")
        return False


def send_sms(message: str) -> bool:
    try:
        from twilio.rest import Client
        account_sid = os.getenv("TWILIO_ACCOUNT_SID", "")
        auth_token  = os.getenv("TWILIO_AUTH_TOKEN", "")
        from_number = os.getenv("TWILIO_FROM_NUMBER", "")
        to_number   = os.getenv("TWILIO_TO_NUMBER", "")

        if not all([account_sid, auth_token, from_number, to_number]):
            return False

        client = Client(account_sid, auth_token)
        client.messages.create(body=message[:1600], from_=from_number, to=to_number)
        return True
    except Exception as e:
        print(f"[SMS] Failed: {e}")
        return False


# ── Market hours helper ───────────────────────────────────────────────────────

ET = zoneinfo.ZoneInfo("America/New_York")

def is_market_open() -> bool:
    """True during US market hours: Mon–Fri 09:30–16:00 ET."""
    now_et = datetime.now(ET)
    if now_et.weekday() >= 5:          # Saturday=5, Sunday=6
        return False
    t = now_et.time()
    return time(9, 30) <= t < time(16, 0)


def _price_from_info_for_alerts(info: Optional[dict]) -> float:
    if not isinstance(info, dict):
        return 0.0
    for key in (
        "currentPrice",
        "regularMarketPrice",
        "regularMarketPreviousClose",
        "previousClose",
        "fiftyDayAverage",
        "twoHundredDayAverage",
        "ask",
        "bid",
    ):
        value = info.get(key)
        try:
            value = float(value)
        except (TypeError, ValueError):
            value = 0.0
        if value > 0:
            return value
    return 0.0


async def _get_portfolio_price_map(held: list[str], positions: dict[str, dict]) -> dict[str, float]:
    now = datetime.now(timezone.utc)
    price_map: dict[str, float] = {}
    stale_tickers: list[str] = []

    for ticker in held:
        cached_entry = _portfolio_price_cache.get(ticker)
        if cached_entry:
            cached_price, cached_at = cached_entry
            if (now - cached_at).total_seconds() < _PORTFOLIO_PRICE_TTL and cached_price > 0:
                price_map[ticker] = cached_price
                continue
        stale_tickers.append(ticker)

    if stale_tickers:
        infos = await asyncio.gather(
            *[get_info_with_timeout(ticker, SEARCH_INFO_TIMEOUT_SEC) for ticker in stale_tickers],
            return_exceptions=True,
        )
        for ticker, info in zip(stale_tickers, infos):
            if not isinstance(info, Exception):
                live_price = _price_from_info_for_alerts(info)
                if live_price > 0:
                    price_map[ticker] = live_price
                    _portfolio_price_cache[ticker] = (live_price, now)
                    continue

            cached_entry = _portfolio_price_cache.get(ticker)
            if cached_entry and cached_entry[0] > 0:
                price_map[ticker] = cached_entry[0]

    for ticker in held:
        if not price_map.get(ticker):
            fallback_price = positions.get(ticker, {}).get("avg_cost", 0) or 0
            if fallback_price:
                price_map[ticker] = float(fallback_price)

    return price_map


# Per-user alert cooldown state (in-memory, keyed by user_id)
_user_alert_cooldowns: dict[str, dict[str, datetime]] = {}
_user_alert_price_cache: dict[str, dict[str, float]] = {}


def _build_user_signals(
    thesis_map: dict,
    buy_tickers: list,
    owned_positions: dict,
    paper_held: set,
    allow_buys: bool,
    strong_buy_min_score: int,
    sell_max_score: int,
    initial_float: float,
    min_positive_agents: int,
    buy_limit: int = 3,
    sell_limit: int = 3,
) -> dict:
    """Build BUY/SELL signals from a pre-computed thesis_map for a specific user context."""
    sell_tickers = list(owned_positions.keys())
    strong_buys: list[dict] = []
    strong_sells: list[dict] = []

    for ticker in buy_tickers:
        if not allow_buys:
            break
        if ticker in owned_positions or ticker in paper_held:
            continue
        thesis = thesis_map.get(ticker)
        if not thesis:
            continue
        positive_agents = sum(
            1 for meta in (thesis.agent_meta or {}).values()
            if meta.get("direction") == "positive" and meta.get("usable", True)
        )
        if positive_agents < min_positive_agents:
            continue
        composite     = float(thesis.composite_score)
        forecast_12m  = thesis.forecast.get("12m")
        forecast_6m   = thesis.forecast.get("6m")
        projected_12m = float(forecast_12m.base_return_pct) if forecast_12m else 0.0
        projected_6m  = float(forecast_6m.base_return_pct)  if forecast_6m  else 0.0
        confidence_f  = float(forecast_12m.confidence)       if forecast_12m else 0.5
        confidence_s  = "high" if confidence_f >= 0.7 else "medium" if confidence_f >= 0.45 else "low"
        if confidence_s == "low":
            continue
        if composite < strong_buy_min_score:
            continue
        if projected_12m < 12 and projected_6m < 7:
            continue
        alloc_pct = 0.05 + (composite - strong_buy_min_score) / (100 - strong_buy_min_score) * 0.10
        est_cost  = round(initial_float * min(alloc_pct, 0.15), 2)
        strong_buys.append({
            "ticker": ticker,
            "name": thesis.company_name or ticker,
            "price": thesis.current_price or 0.0,
            "action": "BUY",
            "type": "buy_opportunity",
            "trigger": "AGENT CONSENSUS",
            "signal": f"Score {composite:.0f}/100 — {projected_12m:+.1f}% 12m base",
            "score_value": int(composite),
            "confidence": confidence_s,
            "projected_12m_pct": round(projected_12m, 1),
            "projected_24m_pct": round(projected_12m * 1.8, 1),
            "est_cost": est_cost,
            "reasoning": (thesis.narrative or "")[:400] if thesis.narrative else "",
            "agent_scores": {k: v.get("score") for k, v in (thesis.agent_meta or {}).items()},
        })

    for ticker in sell_tickers:
        thesis = thesis_map.get(ticker)
        pos    = owned_positions.get(ticker, {})
        if not thesis:
            continue
        composite     = float(thesis.composite_score)
        if composite > sell_max_score:
            continue
        forecast_12m  = thesis.forecast.get("12m")
        projected_12m = float(forecast_12m.base_return_pct) if forecast_12m else 0.0
        avg_cost      = pos.get("avg_cost", 0)
        current_price = thesis.current_price or avg_cost
        unrealised_pct = ((current_price - avg_cost) / avg_cost * 100) if avg_cost else 0
        strong_sells.append({
            "ticker": ticker,
            "name": thesis.company_name or ticker,
            "price": current_price,
            "action": "SELL",
            "type": "sell_signal",
            "trigger": "AGENT SELL",
            "signal": f"Score {composite:.0f}/100 — weak thesis",
            "score_value": int(composite),
            "confidence": "medium",
            "projected_12m_pct": round(projected_12m, 1),
            "projected_24m_pct": round(projected_12m * 1.8, 1),
            "unrealised_pct": round(unrealised_pct, 1),
            "reasoning": (thesis.narrative or "")[:400] if thesis.narrative else "",
            "agent_scores": {k: v.get("score") for k, v in (thesis.agent_meta or {}).items()},
        })

    strong_buys.sort(key=lambda x: x["score_value"], reverse=True)
    strong_sells.sort(key=lambda x: x["score_value"])
    return {"buys": strong_buys[:buy_limit], "sells": strong_sells[:sell_limit], "scored_by": "multi-agent"}


async def _build_recommendation_alert_snapshot(buy_limit: int = 3, sell_limit: int = 3) -> dict:
    """
    Build buy/sell alert candidates using the full 21-agent thesis pipeline.
    Agent signals are cached in SQLite (max_age_hours=26) so repeated calls
    within the snapshot TTL reuse stored signals without hitting Claude again.
    Position sizing respects the initial_float from Alert Settings.
    """
    from agents.orchestrator import OrchestratorAgent

    _s = load_settings()
    strong_buy_min_score = max(55, int(_s.get("alert_buy_min_score") or os.getenv("ALERT_BUY_MIN_SCORE", "72")))
    sell_max_score       = min(50, int(_s.get("alert_sell_max_score") or os.getenv("ALERT_SELL_MAX_SCORE", "42")))
    initial_float        = float(_s.get("initial_float") or 100_000.0)

    # Load watchlist (buy candidates) and owned positions (sell candidates).
    # Phase 1: unify with paper portfolio so the same ticker cannot appear as
    # BUY here (watchlist-driven) and SELL in the Signals tab (paper-driven).
    watchlist        = load_watchlist()  # list of ticker strings
    buy_tickers      = [t for t in watchlist if isinstance(t, str) and t]
    owned_positions  = {t: pos for t, pos in compute_positions(load_portfolio()).items() if pos.get("shares", 0) > 0}
    paper_held_alert = {t for t, p in compute_positions(load_paper_portfolio()).items() if p.get("shares", 0) > 0}
    sell_tickers     = list(owned_positions.keys())

    all_tickers = list(dict.fromkeys(buy_tickers + sell_tickers))  # deduplicated, order preserved
    if not all_tickers:
        return {"buys": [], "sells": [], "scored_by": "multi-agent"}

    # Run agent theses concurrently — max 3 at a time to avoid overloading Claude
    orchestrator = OrchestratorAgent()
    semaphore    = asyncio.Semaphore(3)

    async def _run_thesis(ticker: str):
        async with semaphore:
            try:
                # run_fresh=False: reuse cached signals (<26h) before calling Claude
                thesis = await asyncio.to_thread(orchestrator.run_thesis, ticker, False)
                return ticker, thesis
            except Exception as exc:
                logger.warning("[AlertSnapshot] %s thesis failed: %s", ticker, exc)
                return ticker, None

    results = await asyncio.gather(*[_run_thesis(t) for t in all_tickers])
    thesis_map = {ticker: thesis for ticker, thesis in results if thesis is not None}

    # Phase 2: regime gate. Same rule as Signals tab — risk-off market kills new BUYs.
    regime = await get_market_regime()
    allow_buys = regime.get("ok", True)
    min_positive_agents = int(_s.get("alert_min_positive_agents", 5))

    # ── BUY candidates — watchlist tickers not already owned ──────────────────
    strong_buys = []
    for ticker in buy_tickers:
        if not allow_buys:
            break  # regime risk-off, no new BUYs
        if ticker in owned_positions or ticker in paper_held_alert:
            continue  # already held in real or paper portfolio — skip buy signal
        thesis = thesis_map.get(ticker)
        if not thesis:
            continue

        # Phase 2: require ≥ N of 9 agents to be net-positive. A 3-of-9 thesis
        # that scrapes a passing composite is fragile — demand consensus.
        positive_agents = sum(
            1 for meta in (thesis.agent_meta or {}).values()
            if meta.get("direction") == "positive" and meta.get("usable", True)
        )
        if positive_agents < min_positive_agents:
            continue

        composite    = float(thesis.composite_score)
        forecast_12m = thesis.forecast.get("12m")
        forecast_6m  = thesis.forecast.get("6m")
        projected_12m = float(forecast_12m.base_return_pct) if forecast_12m else 0.0
        projected_6m  = float(forecast_6m.base_return_pct)  if forecast_6m  else 0.0
        confidence_f  = float(forecast_12m.confidence)       if forecast_12m else 0.5
        confidence_s  = "high" if confidence_f >= 0.7 else "medium" if confidence_f >= 0.45 else "low"

        if confidence_s == "low":
            continue  # low-confidence thesis excluded from alert BUYs

        if composite < strong_buy_min_score:
            continue
        if projected_12m < 12 and projected_6m < 7:
            continue  # Phase 1: tighten upside floor — agents agree but reward too thin

        # Position sizing: scale allocation 5-15% of float by composite score
        alloc_pct   = 0.05 + (composite - strong_buy_min_score) / (100 - strong_buy_min_score) * 0.10
        est_cost    = round(initial_float * min(alloc_pct, 0.15), 2)

        # Narrative: prefer agent narrative summary, fall back to drivers
        narrative   = thesis.narrative or {}
        reasoning   = (
            narrative.get("summary")
            or narrative.get("base")
            or ("; ".join(thesis.drivers[:2]) if thesis.drivers else "")
            or f"21-agent composite score {composite:.0f}/100 across fundamentals, valuation, growth, macro, sentiment and more."
        )

        strong_buys.append({
            "ticker":           ticker,
            "name":             ticker,  # watchlist is tickers only; name resolved by yfinance if needed
            "action":           "BUY",
            "type":             "buy_opportunity",
            "trigger":          "AGENT CONSENSUS",
            "signal":           f"21-agent composite score {composite:.0f}/100 — BUY signal confirmed.",
            "price":            float(thesis.current_price or 0.0),
            "score_value":      int(round(composite)),
            "confidence":       confidence_s,
            "projected_12m_pct": round(projected_12m, 2),
            "projected_24m_pct": round(projected_12m * 1.6, 2),  # rough 24m extrapolation
            "est_cost":         est_cost,
            "reasoning":        reasoning,
            "agent_scores":     thesis.agent_scores,
        })

    strong_buys.sort(key=lambda x: (x["score_value"], x["projected_12m_pct"]), reverse=True)

    # ── SELL candidates — owned tickers ───────────────────────────────────────
    total_market_value = 0.0
    current_value_map: dict[str, float] = {}
    for ticker, pos in owned_positions.items():
        thesis       = thesis_map.get(ticker)
        current_price = (float(thesis.current_price) if thesis and thesis.current_price else None) or pos.get("avg_cost", 0.0)
        current_value = float(pos.get("shares", 0.0)) * float(current_price or 0.0)
        current_value_map[ticker] = current_value
        total_market_value += current_value

    strong_sells = []
    for ticker, pos in owned_positions.items():
        thesis = thesis_map.get(ticker)
        if not thesis:
            continue

        composite     = float(thesis.composite_score)
        forecast_12m  = thesis.forecast.get("12m")
        projected_12m = float(forecast_12m.base_return_pct) if forecast_12m else 0.0
        confidence_f  = float(forecast_12m.confidence)       if forecast_12m else 0.5
        confidence_s  = "high" if confidence_f >= 0.7 else "medium" if confidence_f >= 0.45 else "low"

        current_price  = (float(thesis.current_price) if thesis.current_price else None) or pos.get("avg_cost", 0.0)
        cost_basis     = float(pos.get("shares", 0.0)) * float(pos.get("avg_cost", 0.0))
        current_value  = current_value_map.get(ticker, 0.0)
        unrealised_pct = ((current_value - cost_basis) / cost_basis * 100) if cost_basis else 0.0

        narrative  = thesis.narrative or {}
        bear_text  = narrative.get("bear") or ""
        trigger    = None
        severity   = 0.0
        reasoning  = ""

        # Primary: composite score has fallen below sell threshold
        if composite <= sell_max_score and projected_12m <= -5:
            trigger   = "AGENT SELL"
            severity  = (50 - composite) + abs(projected_12m)
            reasoning = (
                bear_text
                or ("; ".join(thesis.risks[:2]) if thesis.risks else "")
                or f"21-agent composite fell to {composite:.0f}/100 with a {projected_12m:.1f}% 12-month base return."
            )

        # Secondary: composite is bearish even if not fully below threshold
        elif composite < 50 and projected_12m < -8:
            trigger   = "AGENT BEARISH"
            severity  = (50 - composite) + abs(projected_12m) * 0.5
            reasoning = bear_text or f"Agents are net-bearish ({composite:.0f}/100) on a position you hold."

        # Tertiary: concentration risk — >25% of portfolio, agents not bullish
        if not trigger and total_market_value > 0 and composite < 60:
            conc = current_value / total_market_value if total_market_value > 0 else 0.0
            if conc > 0.25:
                trigger   = "CONCENTRATION"
                severity  = conc * 100
                reasoning = f"This holding is {conc*100:.0f}% of portfolio value with agent score only {composite:.0f}/100."

        if not trigger:
            continue

        strong_sells.append({
            "ticker":            ticker,
            "name":              pos.get("name", ticker),
            "action":            "SELL",
            "type":              "sell_signal",
            "trigger":           trigger,
            "signal":            f"SELL signal: {trigger} — agent composite {composite:.0f}/100.",
            "price":             float(current_price or 0.0),
            "score_value":       int(round(composite)),
            "confidence":        confidence_s,
            "projected_12m_pct": round(projected_12m, 2),
            "projected_24m_pct": round(projected_12m * 1.6, 2),
            "unrealised_pct":    round(unrealised_pct, 2),
            "reasoning":         reasoning,
            "severity":          round(severity, 4),
            "agent_scores":      thesis.agent_scores,
        })

    strong_sells.sort(key=lambda x: x["severity"], reverse=True)

    return {
        "buys":  strong_buys[:buy_limit],
        "sells": strong_sells[:sell_limit],
        "scored_by": "multi-agent",
        "regime": regime,
        "min_positive_agents": min_positive_agents,
    }


# ── Email body builder ────────────────────────────────────────────────────────

def _build_alert_email(buy_alerts: list, sell_alerts: list, time_str: str, preview: bool = False) -> tuple[str, str, str]:
    """Return (subject, body, sms_body) for a recommendation alert email."""
    buy_tickers  = ", ".join(a["ticker"] for a in buy_alerts)
    sell_tickers = ", ".join(a["ticker"] for a in sell_alerts)
    parts = []
    if buy_tickers:
        parts.append(f"BUY: {buy_tickers}")
    if sell_tickers:
        parts.append(f"SELL: {sell_tickers}")
    prefix = "[PREVIEW] " if preview else ""
    subject = f"{prefix}StockPicker Alert — {' | '.join(parts)}"

    W = 56
    SEP  = "=" * W
    DASH = "-" * W

    def conviction_bar(score: int) -> str:
        filled = round(score / 10)
        return "[" + "█" * filled + "░" * (10 - filled) + f"] {score}/100"

    def format_alert(rank: int, a: dict, action: str) -> list[str]:
        price        = a.get("price") or 0.0
        p12          = float(a.get("projected_12m_pct") or 0)
        p24          = float(a.get("projected_24m_pct") or 0)
        score        = int(a.get("score_value") or 0)
        confidence   = str(a.get("confidence") or "medium").title()
        trigger      = (a.get("signals") or [{}])[0].get("signal") or a.get("trigger") or ""
        reasoning    = (a.get("reasoning") or "").strip()
        target_12m   = price * (1 + p12 / 100) if price else 0
        action_label = f"▲ {action}" if action == "BUY" else f"▼ {action}"

        lines = [
            f"#{rank}  {a.get('name', a['ticker'])} ({a['ticker']})   {action_label}",
            f"    Current price  : ${price:,.2f}",
        ]
        if target_12m:
            lines.append(f"    12-month target: ~${target_12m:,.0f}  ({p12:+.1f}%)")
        else:
            lines.append(f"    12-month view  : {p12:+.1f}%")
        lines.append(f"    24-month view  : {p24:+.1f}%")
        lines.append(f"    Conviction     : {conviction_bar(score)} · {confidence} confidence")
        if trigger:
            lines.append(f"    Trigger        : {trigger}")
        if reasoning:
            # Wrap long reasoning lines at ~52 chars
            words, line_buf = reasoning.split(), []
            wrapped = []
            for w in words:
                if sum(len(x) + 1 for x in line_buf) + len(w) > 52:
                    wrapped.append("    " + " ".join(line_buf))
                    line_buf = [w]
                else:
                    line_buf.append(w)
            if line_buf:
                wrapped.append("    " + " ".join(line_buf))
            lines.append("")
            lines.append("    Why now:")
            lines.extend(wrapped)
        return lines

    body_lines = [
        SEP,
        f"  StockPicker Recommendation Alert",
        f"  {time_str}",
        SEP,
        "",
    ]

    # Summary
    summary_parts = []
    if buy_alerts:
        summary_parts.append(f"  ▲ {len(buy_alerts)} BUY opportunit{'y' if len(buy_alerts)==1 else 'ies'} identified")
    if sell_alerts:
        summary_parts.append(f"  ▼ {len(sell_alerts)} SELL signal{'s' if len(sell_alerts)>1 else ''} on held position{'s' if len(sell_alerts)>1 else ''}")
    if summary_parts:
        body_lines += ["SUMMARY", ""] + summary_parts + ["", SEP, ""]

    if buy_alerts:
        body_lines += ["▲ BUY OPPORTUNITIES", DASH, ""]
        for rank, alert in enumerate(buy_alerts, start=1):
            body_lines += format_alert(rank, alert, "BUY")
            body_lines += ["", DASH, ""]

    if sell_alerts:
        body_lines += ["▼ SELL SIGNALS  (positions you hold)", DASH, ""]
        for rank, alert in enumerate(sell_alerts, start=1):
            body_lines += format_alert(rank, alert, "SELL")
            body_lines += ["", DASH, ""]

    if preview:
        body_lines += ["", "  *** This is a preview email using sample data. ***", ""]

    body_lines += [
        "",
        SEP,
        "  Alerts are driven by model conviction scores, ownership",
        "  signals, and longer-horizon return projections — not",
        "  intraday noise. This is not financial advice.",
        "  Always do your own research before acting.",
        SEP,
    ]

    sms_parts = [f"StockPicker {time_str}"]
    for a in buy_alerts:
        sms_parts.append(f"BUY {a['ticker']} {float(a.get('projected_12m_pct') or 0):+.1f}% 12M")
    for a in sell_alerts:
        sms_parts.append(f"SELL {a['ticker']} {float(a.get('projected_12m_pct') or 0):+.1f}% 12M")

    return subject, "\n".join(body_lines), "\n".join(sms_parts)


# ── Stock monitoring (runs every 5 min) ───────────────────────────────────────

async def monitor_stocks():
    monitor_scheduler_status["active"] = True
    monitor_scheduler_status["runs_started"] += 1
    monitor_scheduler_status["last_run"] = datetime.now(timezone.utc).isoformat()
    monitor_scheduler_status["last_error"] = None
    if not is_market_open():
        monitor_scheduler_status["active"] = False
        return
    now = datetime.now(timezone.utc)
    monitor_status["last_check"] = now.isoformat()
    monitor_status["checks_run"] += 1
    time_str = now.astimezone(ET).strftime("%d %b %Y, %H:%M ET")

    import db as _db
    active_users = _db.get_all_active_users_with_email()

    # Collect unique tickers from all users (thesis runs are cached per-ticker)
    all_unique: set[str] = set()
    for u in active_users:
        for t in _db.get_user_watchlist(u["user_id"]):
            all_unique.add(t)
        for pos in compute_positions(_db.get_user_transactions(u["user_id"], "real")).values():
            if pos.get("shares", 0) > 0 and pos.get("name"):
                pass  # ticker is the key, tracked above
        for t, pos in compute_positions(_db.get_user_transactions(u["user_id"], "real")).items():
            if pos.get("shares", 0) > 0:
                all_unique.add(t)

    # Also include legacy admin watchlist tickers
    global_watchlist = load_watchlist()
    legacy_owned_map = {t: pos for t, pos in compute_positions(load_portfolio()).items() if pos.get("shares", 0) > 0}
    legacy_paper_held = {t for t, p in compute_positions(load_paper_portfolio()).items() if p.get("shares", 0) > 0}
    all_unique |= set(global_watchlist) | set(legacy_owned_map.keys())
    all_unique_tickers = [t for t in all_unique if t]

    if not all_unique_tickers:
        monitor_status["active"] = True
        monitor_scheduler_status["active"] = False
        return

    # Run agent theses once for all unique tickers (cached — won't re-call Claude if fresh)
    from agents.orchestrator import OrchestratorAgent
    orchestrator = OrchestratorAgent()
    semaphore    = asyncio.Semaphore(3)

    async def _run_thesis(ticker: str):
        async with semaphore:
            try:
                thesis = await asyncio.to_thread(orchestrator.run_thesis, ticker, False)
                return ticker, thesis
            except Exception as exc:
                logger.warning("[AlertSnapshot] %s thesis failed: %s", ticker, exc)
                return ticker, None

    results = await asyncio.gather(*[_run_thesis(t) for t in all_unique_tickers])
    thesis_map = {ticker: thesis for ticker, thesis in results if thesis is not None}

    regime     = await get_market_regime()
    allow_buys = regime.get("ok", True)

    # ── Per-user alert delivery ───────────────────────────────────────────────
    for u in active_users:
        uid = u["user_id"]
        _s = _load_user_settings_merged(uid)
        cooldown_minutes = int(float(_s.get("alert_cooldown_hours") or int(os.getenv("ALERT_RECOMMENDATION_COOLDOWN_MINUTES", "720")) // 60) * 60)
        price_swing_pct  = float(_s.get("alert_price_swing_pct") or os.getenv("ALERT_PRICE_SWING_PCT", "3.0"))
        strong_buy_min   = max(55, int(_s.get("alert_buy_min_score") or os.getenv("ALERT_BUY_MIN_SCORE", "72")))
        sell_max_score   = min(50, int(_s.get("alert_sell_max_score") or os.getenv("ALERT_SELL_MAX_SCORE", "42")))
        initial_float    = float(_s.get("initial_float") or 100_000.0)
        min_pos_agents   = int(_s.get("alert_min_positive_agents", 5))
        buy_limit        = max(1, int(_s.get("alert_top_buys") or os.getenv("ALERT_TOP_BUYS", "3")))
        sell_limit       = max(1, int(_s.get("alert_top_sells") or os.getenv("ALERT_TOP_SELLS", "3")))

        user_watchlist   = _db.get_user_watchlist(uid)
        user_owned       = {t: pos for t, pos in compute_positions(_db.get_user_transactions(uid, "real")).items() if pos.get("shares", 0) > 0}
        user_paper_held  = {t for t, p in compute_positions(_db.get_user_transactions(uid, "paper")).items() if p.get("shares", 0) > 0}

        if not user_watchlist and not user_owned:
            continue

        snapshot = _build_user_signals(
            thesis_map=thesis_map,
            buy_tickers=user_watchlist,
            owned_positions=user_owned,
            paper_held=user_paper_held,
            allow_buys=allow_buys,
            strong_buy_min_score=strong_buy_min,
            sell_max_score=sell_max_score,
            initial_float=initial_float,
            min_positive_agents=min_pos_agents,
            buy_limit=buy_limit,
            sell_limit=sell_limit,
        )

        user_cooldown    = _user_alert_cooldowns.setdefault(uid, {})
        user_price_cache = _user_alert_price_cache.setdefault(uid, {})
        pending_alerts: list[dict] = []

        for item in snapshot.get("buys", []) + snapshot.get("sells", []):
            alert_key = f"{item.get('action', 'ALERT')}:{item['ticker']}:{item.get('trigger', '')}"
            last_alerted = user_cooldown.get(alert_key)
            if last_alerted and (now - last_alerted).total_seconds() < cooldown_minutes * 60:
                continue
            ticker_action_key = f"{item.get('action', 'ALERT')}:{item['ticker']}"
            current_price = item.get("price", 0.0)
            last_price = user_price_cache.get(ticker_action_key)
            if last_price and current_price > 0:
                swing_pct = abs(current_price - last_price) / last_price * 100
                if swing_pct < price_swing_pct:
                    continue
            user_cooldown[alert_key] = now
            user_price_cache[ticker_action_key] = current_price if current_price > 0 else (last_price or 0.0)
            pending_alerts.append(item)

        if not pending_alerts:
            continue

        buy_alerts  = [a for a in pending_alerts if a.get("action") == "BUY"]
        sell_alerts = [a for a in pending_alerts if a.get("action") == "SELL"]
        subject, body, sms_body = _build_alert_email(buy_alerts, sell_alerts, time_str)
        to_email = u.get("email") or os.getenv("ALERT_EMAIL", "")
        emailed  = send_email(subject, body, to_email=to_email) if to_email else False
        texted   = send_sms(sms_body[:1600])

        for alert in pending_alerts:
            record = {
                "id": str(uuid.uuid4()),
                "timestamp": now.isoformat(),
                "ticker": alert["ticker"],
                "name": alert["name"],
                "price": alert["price"],
                "action": alert.get("action"),
                "signals": [{"type": alert.get("type"), "signal": alert.get("signal"), "change_pct": alert.get("projected_12m_pct")}],
                "score_value": alert.get("score_value"),
                "confidence": alert.get("confidence"),
                "projected_12m_pct": alert.get("projected_12m_pct"),
                "projected_24m_pct": alert.get("projected_24m_pct"),
                "reasoning": alert.get("reasoning"),
                "notified_email": emailed,
                "notified_sms": texted,
            }
            _db.append_user_alert(uid, record)

    # ── Legacy admin path (global watchlist / ALERT_EMAIL) ────────────────────
    _s = load_settings()
    cooldown_minutes = int(float(_s.get("alert_cooldown_hours") or int(os.getenv("ALERT_RECOMMENDATION_COOLDOWN_MINUTES", "720")) // 60) * 60)
    price_swing_pct  = float(_s.get("alert_price_swing_pct") or os.getenv("ALERT_PRICE_SWING_PCT", "3.0"))
    strong_buy_min   = max(55, int(_s.get("alert_buy_min_score") or os.getenv("ALERT_BUY_MIN_SCORE", "72")))
    sell_max_score   = min(50, int(_s.get("alert_sell_max_score") or os.getenv("ALERT_SELL_MAX_SCORE", "42")))
    initial_float    = float(_s.get("initial_float") or 100_000.0)
    buy_limit        = max(1, int(_s.get("alert_top_buys") or os.getenv("ALERT_TOP_BUYS", "3")))
    sell_limit       = max(1, int(_s.get("alert_top_sells") or os.getenv("ALERT_TOP_SELLS", "3")))
    min_pos_agents   = int(_s.get("alert_min_positive_agents", 5))

    if global_watchlist or legacy_owned_map:
        snapshot = _build_user_signals(
            thesis_map=thesis_map,
            buy_tickers=global_watchlist,
            owned_positions=legacy_owned_map,
            paper_held=legacy_paper_held,
            allow_buys=allow_buys,
            strong_buy_min_score=strong_buy_min,
            sell_max_score=sell_max_score,
            initial_float=initial_float,
            min_positive_agents=min_pos_agents,
            buy_limit=buy_limit,
            sell_limit=sell_limit,
        )
        # Update the cached snapshot for /api/recommendations/status
        recommendation_alert_snapshot["generated_at"] = now
        recommendation_alert_snapshot["data"] = snapshot

        pending_alerts = []
        for item in snapshot.get("buys", []) + snapshot.get("sells", []):
            alert_key = f"{item.get('action', 'ALERT')}:{item['ticker']}:{item.get('trigger', '')}"
            last_alerted = alert_cooldown.get(alert_key)
            if last_alerted and (now - last_alerted).total_seconds() < cooldown_minutes * 60:
                continue
            ticker_action_key = f"{item.get('action', 'ALERT')}:{item['ticker']}"
            current_price = item.get("price", 0.0)
            last_price = alert_price_cache.get(ticker_action_key)
            if last_price and current_price > 0:
                swing_pct = abs(current_price - last_price) / last_price * 100
                if swing_pct < price_swing_pct:
                    continue
            alert_cooldown[alert_key] = now
            alert_price_cache[ticker_action_key] = current_price if current_price > 0 else (last_price or 0.0)
            pending_alerts.append(item)

        if pending_alerts:
            buy_alerts  = [a for a in pending_alerts if a.get("action") == "BUY"]
            sell_alerts = [a for a in pending_alerts if a.get("action") == "SELL"]
            subject, body, sms_body = _build_alert_email(buy_alerts, sell_alerts, time_str)
            emailed = send_email(subject, body)
            texted  = send_sms(sms_body[:1600])
            for alert in pending_alerts:
                record = {
                    "id": str(uuid.uuid4()),
                    "timestamp": now.isoformat(),
                    "ticker": alert["ticker"],
                    "name": alert["name"],
                    "price": alert["price"],
                    "action": alert.get("action"),
                    "signals": [{"type": alert.get("type"), "signal": alert.get("signal"), "change_pct": alert.get("projected_12m_pct")}],
                    "score_value": alert.get("score_value"),
                    "confidence": alert.get("confidence"),
                    "projected_12m_pct": alert.get("projected_12m_pct"),
                    "projected_24m_pct": alert.get("projected_24m_pct"),
                    "reasoning": alert.get("reasoning"),
                    "notified_email": emailed,
                    "notified_sms": texted,
                }
                append_alert(record)

    monitor_status["active"] = True
    monitor_scheduler_status["active"] = False


# ── App lifecycle ─────────────────────────────────────────────────────────────

async def auto_predict():
    """Refresh predictions every 15 mins during market hours."""
    prediction_scheduler_status["active"] = True
    prediction_scheduler_status["runs_started"] += 1
    prediction_scheduler_status["last_run"] = datetime.now(timezone.utc).isoformat()
    prediction_scheduler_status["last_error"] = None
    if not is_market_open():
        prediction_scheduler_status["active"] = False
        return
    try:
        await _generate_predictions_impl()
        print("[Predictions] Auto-refreshed during market hours.")
    except Exception as e:
        prediction_scheduler_status["last_error"] = str(e)
        print(f"[Predictions] Auto-refresh failed: {e}")
    finally:
        prediction_scheduler_status["active"] = False


async def auto_thesis():
    """Run the multi-agent thesis pipeline for the watchlist on a schedule."""
    global _auto_thesis_running
    if not THESIS_AUTO_RUN_ENABLED:
        return
    if _auto_thesis_running:
        logger.info("[Thesis] Auto-thesis skipped; previous run still active.")
        return

    _wl = load_watchlist()
    tickers = [_validate_ticker(t) for t in (_wl[:THESIS_AUTO_RUN_MAX_TICKERS] if THESIS_AUTO_RUN_MAX_TICKERS else _wl)]
    if not tickers:
        thesis_scheduler_status["last_error"] = "watchlist_empty"
        return

    _auto_thesis_running = True
    thesis_scheduler_status["active"] = True
    thesis_scheduler_status["runs_started"] += 1
    thesis_scheduler_status["last_error"] = None
    thesis_scheduler_status["last_run"] = datetime.now(timezone.utc).isoformat()
    run_id = str(uuid.uuid4())

    try:
        import db as _db

        _db.init_db()
        _db.create_thesis_run(run_id, tickers, run_fresh=True, requested_by="scheduler")
        _db.update_thesis_run(run_id, status="running")

        orch = _get_orchestrator()
        completed: list[str] = []
        failed: list[str] = []
        loop = asyncio.get_event_loop()
        for ticker in tickers:
            try:
                await loop.run_in_executor(None, lambda t=ticker: orch.run_thesis(t, run_fresh=True))
                completed.append(ticker)
            except Exception as exc:
                failed.append(ticker)
                logger.error("[Thesis] Auto-thesis failed for %s: %s", ticker, exc)
            _db.update_thesis_run(run_id, status="running", completed=completed, failed=failed)

        final_status = "completed"
        if failed and completed:
            final_status = "partial"
        elif failed and not completed:
            final_status = "failed"
        _db.update_thesis_run(run_id, status=final_status, completed=completed, failed=failed)
        logger.info("[Thesis] Auto-thesis %s: completed=%s failed=%s", final_status, completed, failed)
        if final_status == "failed":
            _maybe_alert_bg_failure("thesis", f"all {len(failed)} tickers failed")
        else:
            _reset_bg_failures("thesis")
    except Exception as exc:
        thesis_scheduler_status["last_error"] = str(exc)
        _maybe_alert_bg_failure("thesis", str(exc))
        try:
            import db as _db
            _db.update_thesis_run(run_id, status="failed", completed=[], failed=tickers)
        except Exception:
            pass
        logger.exception("[Thesis] Auto-thesis run failed: %s", exc)
    finally:
        _auto_thesis_running = False
        thesis_scheduler_status["active"] = False


# ── Auth & trade request models ───────────────────────────────────────────────
async def run_evaluation_job(source: str = "manual") -> int:
    """Evaluate matured forecast outcomes and update operational status."""
    global _auto_evaluation_running
    if _auto_evaluation_running:
        logger.info("[Evaluation] Skipped %s evaluation; previous job still active.", source)
        return 0

    _auto_evaluation_running = True
    evaluation_scheduler_status["active"] = True
    evaluation_scheduler_status["runs_started"] += 1
    evaluation_scheduler_status["last_run"] = datetime.now(timezone.utc).isoformat()
    evaluation_scheduler_status["last_error"] = None
    try:
        import evaluation

        loop = asyncio.get_event_loop()
        thesis_count = await loop.run_in_executor(None, evaluation.evaluate_pending_outcomes)
        prediction_count = await evaluate_prediction_outcomes(limit=200)
        evaluation_scheduler_status["last_evaluated_count"] = thesis_count
        evaluation_scheduler_status["last_prediction_evaluated_count"] = prediction_count
        logger.info(
            "[Evaluation] %s job evaluated thesis=%d prediction=%d outcomes",
            source,
            thesis_count,
            prediction_count,
        )
        # Auto-rebuild calibration whenever new prediction outcomes matured so
        # the next prediction run gets fresh bias-correction data.
        if prediction_count > 0:
            try:
                _sync_prediction_history(load_predictions())
                cal = _build_prediction_calibration_model(store=True)
                logger.info(
                    "[Evaluation] Auto-rebuilt calibration model: samples=%s status=%s",
                    cal.get("sample_count"),
                    _prediction_governance_status(cal).get("status"),
                )
            except Exception as _cal_exc:
                logger.warning("[Evaluation] Calibration auto-rebuild failed: %s", _cal_exc)
        # Rebuild agent accuracy stats whenever new thesis outcomes are evaluated
        if thesis_count > 0:
            try:
                import agent_accuracy as _aa
                _aa.rebuild_all()
                logger.info("[Evaluation] Agent accuracy stats rebuilt")
            except Exception as _aa_exc:
                logger.warning("[Evaluation] Agent accuracy rebuild failed: %s", _aa_exc)
        _reset_bg_failures("evaluation")
        return thesis_count + prediction_count
    except Exception as exc:
        evaluation_scheduler_status["last_error"] = str(exc)
        if source == "scheduler":
            _maybe_alert_bg_failure("evaluation", str(exc))
        logger.exception("[Evaluation] %s job failed: %s", source, exc)
        raise
    finally:
        evaluation_scheduler_status["active"] = False
        _auto_evaluation_running = False


async def auto_evaluate():
    """Run forecast-outcome evaluation on a production schedule."""
    if not EVALUATION_AUTO_RUN_ENABLED:
        return
    await run_evaluation_job("scheduler")


async def auto_recalibrate_weights():
    """Weekly weight recalibration — Monday 02:00 UTC."""
    try:
        import agent_accuracy as _aa
        from agents.orchestrator import rebuild_score_return_table
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(None, _aa.apply_weight_adjustments)
        logger.info("[Learning] Weekly weight recalibration: n_adjusted=%s applied=%s",
                    result.get("n_adjusted"), result.get("applied"))
        updated = await loop.run_in_executor(None, rebuild_score_return_table)
        if updated:
            logger.info("[Learning] SCORE_TO_12M_RETURN table rebuilt from realised returns")
    except Exception as exc:
        logger.warning("[Learning] Weight recalibration failed: %s", exc)


_earnings_watcher_running = False

async def auto_check_earnings():
    """Check EDGAR for new earnings press releases every 20 min Mon-Fri."""
    global _earnings_watcher_running
    if _earnings_watcher_running:
        return
    _earnings_watcher_running = True
    try:
        import earnings_watcher as _ew
        watchlist = load_watchlist()
        if not watchlist:
            return
        loop = asyncio.get_event_loop()
        new_events = await loop.run_in_executor(
            None, lambda: _ew.check_all_watchlist(watchlist, since_days=3)
        )
        if new_events:
            logger.info("[EarningsWatcher] Processed %d new earnings event(s)", len(new_events))
    except Exception as exc:
        logger.error("[EarningsWatcher] auto_check_earnings failed: %s", exc)
    finally:
        _earnings_watcher_running = False


async def auto_earnings_morning_reminders():
    """Send WhatsApp earnings reminders at 11:30 UTC (06:30 ET) Mon-Fri."""
    try:
        import earnings_watcher as _ew
        watchlist = load_watchlist()
        if not watchlist:
            return
        loop = asyncio.get_event_loop()
        sent = await loop.run_in_executor(None, lambda: _ew.send_morning_reminders(watchlist))
        if sent:
            logger.info("[EarningsWatcher] Morning reminders sent: %d", sent)
    except Exception as exc:
        logger.error("[EarningsWatcher] Morning reminders failed: %s", exc)


class LoginRequest(BaseModel):
    username: str
    password: str

class ChangePasswordRequest(BaseModel):
    current_password: str
    new_password: str

class ForgotPasswordRequest(BaseModel):
    username: str

class ResetPasswordRequest(BaseModel):
    token: str
    new_password: str


class RegisterRequest(BaseModel):
    username: str
    email: str
    password: str


class RefreshRequest(BaseModel):
    refresh_token: str


class MfaSetupVerifyRequest(BaseModel):
    code: str


class DeviceTokenRequest(BaseModel):
    device_token: str
    platform: str = "ios"


class UserWatchlistAddRequest(BaseModel):
    ticker: str


class UserPortfolioUpsertRequest(BaseModel):
    ticker: str
    shares: float
    cost_basis: Optional[float] = None
    purchase_date: Optional[str] = None
    paper: bool = False


async def startup():
    _load_lockout_state()
    # Refresh index constituent lists from Wikipedia
    global SP500_TICKERS, NASDAQ100_TICKERS, FTSE100_TICKERS, FTSE250_TICKERS, UNIVERSE
    loop = asyncio.get_event_loop()
    sp500   = await loop.run_in_executor(None, _fetch_sp500_from_wiki)
    nasdaq  = await loop.run_in_executor(None, _fetch_nasdaq100_from_wiki)
    ftse100 = await loop.run_in_executor(None, _fetch_ftse100_from_wiki)
    ftse250 = await loop.run_in_executor(None, _fetch_ftse250_from_wiki)
    if sp500:
        SP500_TICKERS = sp500
        logger.info(f"S&P 500 tickers refreshed from Wikipedia ({len(sp500)} stocks)")
    if nasdaq:
        NASDAQ100_TICKERS = nasdaq
        logger.info(f"NASDAQ 100 tickers refreshed from Wikipedia ({len(nasdaq)} stocks)")
    if ftse100:
        FTSE100_TICKERS = ftse100
        logger.info(f"FTSE 100 tickers refreshed from Wikipedia ({len(ftse100)} stocks)")
    if ftse250:
        FTSE250_TICKERS = ftse250
        logger.info(f"FTSE 250 tickers refreshed from Wikipedia ({len(ftse250)} stocks)")
    UNIVERSE = list(dict.fromkeys(SP500_TICKERS + NASDAQ100_TICKERS + FTSE100_TICKERS + FTSE250_TICKERS + ["TSM", "BE"]))

    # Create default admin account on first run
    users = load_users()
    if not users:
        # Prefer an operator-supplied bootstrap password; only generate one as a
        # last resort. Never print the password to stdout — container/CI logs are
        # widely readable. Generated passwords are written to a 0600 file on the
        # data volume so an operator can retrieve it over a secure channel, then
        # delete it.
        default_pass = (os.getenv("ADMIN_BOOTSTRAP_PASSWORD") or "").strip()
        generated = False
        if not default_pass:
            default_pass = secrets.token_urlsafe(16)
            generated = True
        users["admin"] = {
            "hashed_password": _hash_pw(default_pass),
            "email": os.getenv("ALERT_EMAIL", ""),
        }
        save_users(users)
        if generated:
            try:
                cred_file = _DATA_DIR / "admin_initial_password.txt"
                cred_file.write_text(
                    f"username: admin\npassword: {default_pass}\n"
                    "Change this immediately after first login, then delete this file.\n"
                )
                os.chmod(cred_file, 0o600)
                logger.warning(
                    "First-run admin account created. Initial password written to %s (mode 0600) — "
                    "retrieve it securely, change it, then delete the file.", cred_file,
                )
            except Exception as exc:
                logger.error("First-run admin created but could not persist initial password file: %s", exc)
        else:
            logger.info("First-run admin account created from ADMIN_BOOTSTRAP_PASSWORD")

    # Load persisted scheduler settings (overrides .env)
    import scheduler_settings as _ss
    _sched_cfg = _ss.load()
    _mon_enabled = _sched_cfg["monitor_auto_run_enabled"]
    _mon_interval = _sched_cfg["monitor_auto_run_interval_minutes"]
    _pred_enabled = _sched_cfg["prediction_auto_run_enabled"]
    _pred_interval = _sched_cfg["prediction_auto_run_interval_minutes"]
    _thesis_enabled  = _sched_cfg["thesis_auto_run_enabled"]
    _thesis_interval = _sched_cfg["thesis_auto_run_interval_minutes"]
    _eval_enabled    = _sched_cfg["evaluation_auto_run_enabled"]
    _eval_interval   = _sched_cfg["evaluation_auto_run_interval_minutes"]
    monitor_scheduler_status["enabled"] = _mon_enabled
    prediction_scheduler_status["enabled"] = _pred_enabled
    thesis_scheduler_status["enabled"] = _thesis_enabled
    evaluation_scheduler_status["enabled"] = _eval_enabled

    if _mon_enabled:
        scheduler.add_job(monitor_stocks, "interval", minutes=_mon_interval, id="monitor")
    # Pass the coroutine function directly — AsyncIOScheduler awaits it on the
    # app's event loop. Wrapping in a lambda + asyncio.ensure_future fails
    # because APScheduler's ThreadPoolExecutor has no current event loop.
    scheduler.add_job(
        _prewarm_universe_cache,
        "interval", hours=6, id="screener_prewarm", max_instances=1, coalesce=True,
    )
    if _pred_enabled:
        scheduler.add_job(auto_predict, "interval", minutes=_pred_interval, id="predictions")

    if _thesis_enabled:
        scheduler.add_job(
            auto_thesis,
            "interval",
            minutes=_thesis_interval,
            id="multiagent_thesis",
            max_instances=1,
            coalesce=True,
        )
    if _eval_enabled:
        scheduler.add_job(
            auto_evaluate,
            "interval",
            minutes=_eval_interval,
            id="forecast_evaluation",
            max_instances=1,
            coalesce=True,
        )
    scheduler.add_job(
        auto_recalibrate_weights,
        "cron",
        day_of_week="mon",
        hour=2,
        minute=0,
        id="weight_recalibration",
        max_instances=1,
        coalesce=True,
    )
    # Earnings intelligence jobs
    scheduler.add_job(
        auto_check_earnings,
        "cron",
        day_of_week="mon-fri",
        hour="8-22",
        minute="*/20",
        id="earnings_watcher",
        max_instances=1,
        coalesce=True,
    )
    scheduler.add_job(
        auto_earnings_morning_reminders,
        "cron",
        day_of_week="mon-fri",
        hour=11,
        minute=30,
        id="earnings_reminders",
        max_instances=1,
        coalesce=True,
    )
    scheduler.start()

    # Start Finnhub real-time price feed (no-op if FINNHUB_API_KEY unset)
    import finnhub_prices as _fp
    _fp.start(load_watchlist())
    asyncio.ensure_future(_prewarm_universe_cache())
    print("[Monitor] Stock monitor started — checking every 5 minutes during market hours.")
    print("[Predictions] Auto-prediction scheduled every 15 minutes during market hours.")
    if _thesis_enabled:
        print(f"[Thesis] Multi-agent thesis scheduled every {_thesis_interval} minutes.")
    if _eval_enabled:
        print(f"[Evaluation] Forecast evaluation scheduled every {_eval_interval} minutes.")
    print("[Earnings] Earnings watcher runs every 20 min Mon-Fri 08:00-22:00 UTC.")

async def shutdown():
    scheduler.shutdown()


# ── Auth endpoints ────────────────────────────────────────────────────────────

@app.post("/api/auth/login")
@limiter.limit("10/minute")
async def login(req: LoginRequest, request: Request):
    import db as _db
    raw_username = req.username.strip()
    lock_key = _norm_username(raw_username)
    _check_lockout(lock_key)
    client_ip = request.client.host if request.client else "unknown"
    # Legacy users.json path (admin login — unchanged).
    users = load_users()
    ukey, user = _lookup_user_ci(users, raw_username)
    if user and _verify_pw(req.password, user["hashed_password"]):
        _clear_failed_logins(lock_key)
        logger.info("LOGIN_OK username=%s ip=%s", ukey, client_ip)
        token = create_access_token(ukey)
        return {"access_token": token, "token_type": "bearer"}
    # Multi-user SQLite path (registered accounts).
    app_user = _db.get_user_by_username(raw_username)
    if app_user and _verify_pw(req.password, app_user["password_hash"]):
        if not app_user["is_active"]:
            raise HTTPException(status_code=403, detail="Account disabled")
        _clear_failed_logins(lock_key)
        _db.update_user(app_user["user_id"], last_login_at=datetime.now(timezone.utc).isoformat())
        logger.info("LOGIN_OK username=%s ip=%s (multi-user)", app_user["username"], client_ip)
        access_token = create_short_access_token(app_user["username"])
        refresh_token = _db.create_refresh_token(app_user["user_id"])
        return {
            "access_token": access_token,
            "refresh_token": refresh_token,
            "token_type": "bearer",
            "expires_in": _ACCESS_TOKEN_MINUTES * 60,
        }
    _record_failed_login(lock_key)
    logger.warning("LOGIN_FAIL username=%s ip=%s", lock_key, client_ip)
    raise HTTPException(status_code=401, detail="Invalid username or password")

@app.post("/api/auth/forgot-password")
@limiter.limit("5/minute")
async def forgot_password(req: ForgotPasswordRequest, request: Request):
    username = req.username.strip()
    client_ip = request.client.host if request.client else "unknown"
    origin = request.headers.get("origin", "")
    logger.info("PASSWORD_RESET_ATTEMPT username=%s ip=%s origin=%s", username, client_ip, origin or "none")
    users = load_users()
    ukey, user = _lookup_user_ci(users, username)
    if user:
        token = secrets.token_urlsafe(32)
        # Store only a hash of the token so a memory disclosure can't be replayed.
        _reset_tokens[_hash_reset_token(token)] = (ukey, datetime.now(timezone.utc) + timedelta(minutes=15))
        app_url = os.getenv("APP_URL", "http://localhost:8000").rstrip("/")
        reset_link = f"{app_url}/?reset_token={token}"
        reset_email = (user.get("email") or "").strip()
        result = send_email(
            "StockLens - Password Reset",
            f"Click the link below to reset your password (valid 15 minutes):\n\n{reset_link}\n\nIf you did not request this, ignore this email.",
            to_email=reset_email,
        )
        logger.info("PASSWORD_RESET_REQUESTED username=%s email=%s email_sent=%s app_url=%s", username, reset_email or "missing", result, app_url)
    else:
        logger.info("PASSWORD_RESET_SKIPPED username=%s reason=user_not_found", username)
    # Always return ok to avoid revealing valid usernames
    return {"ok": True, "message": "If that username exists, a reset link has been sent to the registered email."}

@app.post("/api/auth/reset-password")
@limiter.limit("5/minute")
async def reset_password(req: ResetPasswordRequest, request: Request):
    if len(req.new_password) < 12:
        raise HTTPException(status_code=400, detail="Password must be at least 12 characters")
    # Pop atomically so the token is single-use even on retries/races.
    entry = _reset_tokens.pop(_hash_reset_token(req.token), None)
    if not entry:
        raise HTTPException(status_code=400, detail="Invalid or expired reset token")
    username, expiry = entry
    if datetime.now(timezone.utc) > expiry:
        raise HTTPException(status_code=400, detail="Reset token has expired")
    users = load_users()
    if username in users:
        users[username]["hashed_password"] = _hash_pw(req.new_password)
        save_users(users)
    else:
        import db as _db
        app_user = _db.get_user_by_username(username)
        if not app_user:
            raise HTTPException(status_code=400, detail="User not found — token may be stale")
        _db.update_user(app_user["user_id"], password_hash=_hash_pw(req.new_password))
    logger.info("PASSWORD_RESET_OK username=%s", username)
    return {"ok": True}

@app.post("/api/auth/change-password")
async def change_password(req: ChangePasswordRequest, current_user: str = Depends(get_current_user)):
    if len(req.new_password) < 12:
        raise HTTPException(status_code=400, detail="Password must be at least 12 characters")
    users = load_users()
    user = users[current_user]
    if not _verify_pw(req.current_password, user["hashed_password"]):
        logger.warning("CHANGE_PASSWORD_FAIL username=%s", current_user)
        raise HTTPException(status_code=400, detail="Current password is incorrect")
    users[current_user]["hashed_password"] = _hash_pw(req.new_password)
    save_users(users)
    logger.info("CHANGE_PASSWORD_OK username=%s", current_user)
    return {"ok": True}

@app.get("/api/auth/me")
async def me(current_user: str = Depends(get_current_user)):
    import db as _db
    app_user = _db.get_user_by_username(current_user)
    if app_user:
        return {
            "username": current_user,
            "user_id": app_user["user_id"],
            "email": app_user["email"],
            "role": app_user["role"],
            "tier": app_user["tier"],
            "email_verified": bool(app_user["email_verified"]),
            "mfa_enabled": bool(app_user["mfa_enabled"]),
            "monthly_thesis_count": app_user["monthly_thesis_count"],
        }
    return {"username": current_user, "role": "admin" if _is_admin_user(current_user) else "user"}


# ── Multi-user auth endpoints ─────────────────────────────────────────────────

@app.post("/api/auth/register")
@limiter.limit("5/minute")
async def register(req: RegisterRequest, request: Request, background_tasks: BackgroundTasks):
    import db as _db
    if len(req.username) < 3 or len(req.username) > 30:
        raise HTTPException(status_code=400, detail="Username must be 3-30 characters")
    if not re.match(r"^[a-zA-Z0-9_\-]+$", req.username):
        raise HTTPException(status_code=400, detail="Username may only contain letters, numbers, _ and -")
    if len(req.password) < 12:
        raise HTTPException(status_code=400, detail="Password must be at least 12 characters")
    if not re.match(r"^[^@]+@[^@]+\.[^@]+$", req.email):
        raise HTTPException(status_code=400, detail="Invalid email address")
    if _db.get_user_by_username(req.username):
        raise HTTPException(status_code=409, detail="Username already taken")
    if _db.get_user_by_email(req.email):
        raise HTTPException(status_code=409, detail="Email already registered")
    pw_hash = _hash_pw(req.password)
    user = _db.create_user(req.username, req.email, pw_hash)
    token = _db.create_email_verification_token(user["user_id"])
    app_url = os.getenv("APP_URL", "http://localhost:8000").rstrip("/")
    verify_link = f"{app_url}/?verify_token={token}"
    background_tasks.add_task(
        send_email,
        "StockLens - Verify your email",
        f"Welcome! Click to verify your email:\n\n{verify_link}\n\nLink valid for 24 hours.",
        to_email=req.email,
    )
    logger.info("REGISTER_OK username=%s email=%s", req.username, req.email)
    return {"ok": True, "user_id": user["user_id"], "message": "Verification email sent"}


@app.get("/api/auth/verify-email")
async def verify_email(token: str):
    import db as _db
    user_id = _db.consume_email_verification_token(token)
    if not user_id:
        raise HTTPException(status_code=400, detail="Invalid or expired verification token")
    logger.info("EMAIL_VERIFIED user_id=%s", user_id)
    return {"ok": True, "message": "Email verified. You can now log in."}


@app.post("/api/auth/refresh")
@limiter.limit("30/minute")
async def refresh_token_endpoint(req: RefreshRequest, request: Request):
    import db as _db
    result = _db.rotate_refresh_token(req.refresh_token)
    if not result:
        raise HTTPException(status_code=401, detail="Invalid or expired refresh token")
    new_raw, user_id = result
    user = _db.get_user_by_id(user_id)
    if not user or not user["is_active"]:
        raise HTTPException(status_code=401, detail="Account disabled")
    access_token = create_short_access_token(user["username"])
    return {
        "access_token": access_token,
        "refresh_token": new_raw,
        "token_type": "bearer",
        "expires_in": _ACCESS_TOKEN_MINUTES * 60,
    }


@app.post("/api/auth/logout-all")
async def logout_all(current_user: str = Depends(get_current_user)):
    import db as _db
    user = _db.get_user_by_username(current_user)
    if user:
        _db.revoke_all_refresh_tokens(user["user_id"])
    return {"ok": True}


# MFA endpoints

@app.post("/api/auth/mfa/setup")
async def mfa_setup(current_user: str = Depends(get_current_user)):
    import db as _db
    user = _db.get_user_by_username(current_user)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    if user["mfa_enabled"]:
        raise HTTPException(status_code=400, detail="MFA already enabled")
    secret = pyotp.random_base32()
    _db.update_user(user["user_id"], mfa_secret=_encrypt_secret(secret))
    totp = pyotp.TOTP(secret)
    provisioning_uri = totp.provisioning_uri(name=user["email"], issuer_name="StockLens")
    return {"secret": secret, "provisioning_uri": provisioning_uri}


@app.post("/api/auth/mfa/verify")
@limiter.limit("5/minute")
async def mfa_verify(req: MfaSetupVerifyRequest, request: Request, current_user: str = Depends(get_current_user)):
    import db as _db
    user = _db.get_user_by_username(current_user)
    if not user or not user.get("mfa_secret"):
        raise HTTPException(status_code=400, detail="MFA setup not started")
    _check_mfa_lockout(user["user_id"])
    totp = pyotp.TOTP(_decrypt_secret(user["mfa_secret"]))
    if not totp.verify(req.code, valid_window=1):
        _record_mfa_failure(user["user_id"])
        raise HTTPException(status_code=400, detail="Invalid TOTP code")
    _clear_mfa_failures(user["user_id"])
    _db.update_user(user["user_id"], mfa_enabled=1)
    logger.info("MFA_ENABLED user_id=%s", user["user_id"])
    return {"ok": True}


@app.post("/api/auth/mfa/disable")
@limiter.limit("5/minute")
async def mfa_disable(req: MfaSetupVerifyRequest, request: Request, current_user: str = Depends(get_current_user)):
    import db as _db
    user = _db.get_user_by_username(current_user)
    if not user or not user.get("mfa_enabled"):
        raise HTTPException(status_code=400, detail="MFA not enabled")
    _check_mfa_lockout(user["user_id"])
    totp = pyotp.TOTP(_decrypt_secret(user["mfa_secret"]))
    if not totp.verify(req.code, valid_window=1):
        _record_mfa_failure(user["user_id"])
        raise HTTPException(status_code=400, detail="Invalid TOTP code")
    _clear_mfa_failures(user["user_id"])
    _db.update_user(user["user_id"], mfa_enabled=0, mfa_secret=None)
    logger.info("MFA_DISABLED user_id=%s", user["user_id"])
    return {"ok": True}


# ── User management endpoints ─────────────────────────────────────────────────

def _is_admin_user(current_user: str) -> bool:
    """Authoritative admin check. Portal/service identities are never admin.
    A user is admin if: (1) they are in users.json with role=='admin' or are
    the bootstrap 'admin' account, OR (2) their app_users row has role=='admin'."""
    if current_user.startswith(("portal:", "service:")):
        return False
    user_obj = load_users().get(current_user)
    if user_obj is not None:
        return user_obj.get("role") == "admin" or current_user == "admin"
    # Fall through to app_users for registered accounts not in users.json
    import db as _db
    app_user = _db.get_user_by_username(current_user)
    return bool(app_user and app_user.get("role") == "admin")


@app.get("/v1/users")
async def list_users_endpoint(
    limit: int = 100, offset: int = 0,
    current_user: str = Depends(get_current_user),
):
    import db as _db
    if not _is_admin_user(current_user):
        raise HTTPException(status_code=403, detail="Admin only")
    rows = _db.list_users_with_stats(limit=min(limit, 500), offset=offset)
    safe = [{k: v for k, v in r.items() if k not in ("password_hash", "mfa_secret")} for r in rows]
    return {"users": safe, "count": len(safe)}


class UpdateTierRequest(BaseModel):
    tier: str

@app.put("/v1/users/{user_id}/tier")
async def update_user_tier(user_id: str, req: UpdateTierRequest, current_user: str = Depends(get_current_user)):
    import db as _db
    if not _is_admin_user(current_user):
        raise HTTPException(status_code=403, detail="Admin only")
    if req.tier not in ("free", "pro", "premium"):
        raise HTTPException(status_code=400, detail="Invalid tier")
    target = _db.get_user_by_id(user_id)
    if not target:
        raise HTTPException(status_code=404, detail="User not found")
    _db.update_user(user_id, tier=req.tier)
    return {"ok": True, "user_id": user_id, "tier": req.tier}


@app.delete("/v1/users/{user_id}")
async def admin_delete_user(user_id: str, current_user: str = Depends(get_current_user)):
    import db as _db
    if not _is_admin_user(current_user):
        raise HTTPException(status_code=403, detail="Admin only")
    target = _db.get_user_by_id(user_id)
    if not target:
        raise HTTPException(status_code=404, detail="User not found")
    deleted = _db.delete_user(user_id)
    logger.info("ADMIN_DELETE_USER admin=%s deleted_user=%s", current_user, target["username"])
    return {"ok": deleted}


@app.post("/v1/users/{user_id}/revoke-sessions")
async def admin_revoke_sessions(user_id: str, current_user: str = Depends(get_current_user)):
    import db as _db
    if not _is_admin_user(current_user):
        raise HTTPException(status_code=403, detail="Admin only")
    target = _db.get_user_by_id(user_id)
    if not target:
        raise HTTPException(status_code=404, detail="User not found")
    _db.revoke_all_refresh_tokens(user_id)
    logger.info("ADMIN_REVOKE_SESSIONS admin=%s target_user=%s", current_user, target["username"])
    return {"ok": True}


@app.post("/v1/users/{user_id}/reset-password")
async def admin_reset_password(user_id: str, current_user: str = Depends(get_current_user)):
    import db as _db
    if not _is_admin_user(current_user):
        raise HTTPException(status_code=403, detail="Admin only")
    target = _db.get_user_by_id(user_id)
    if not target:
        raise HTTPException(status_code=404, detail="User not found")
    if not target.get("email"):
        raise HTTPException(status_code=400, detail="User has no email address on file")
    token = secrets.token_urlsafe(32)
    _reset_tokens[_hash_reset_token(token)] = (
        target["username"],
        datetime.now(timezone.utc) + timedelta(minutes=60),
    )
    app_url = os.getenv("APP_URL", "http://localhost:8000").rstrip("/")
    reset_link = f"{app_url}/?reset_token={token}"
    sent = send_email(
        "StockLens - Password Reset",
        f"An administrator has requested a password reset for your account.\n\n"
        f"Click the link below to set a new password (valid 60 minutes):\n\n{reset_link}\n\n"
        f"If you did not expect this, contact support.",
        target["email"],
    )
    logger.info("ADMIN_PASSWORD_RESET admin=%s target_user=%s email_sent=%s", current_user, target["username"], sent)
    return {"ok": True, "email_sent": sent}


@app.get("/v1/users/me")
async def users_me(current_user: str = Depends(get_current_user)):
    import db as _db
    app_user = _db.get_user_by_username(current_user)
    if not app_user:
        # No multi-user row: only the genuine local admin gets admin; portal /
        # service / unknown identities default to a non-privileged user.
        is_admin = _is_admin_user(current_user)
        return {
            "username": current_user,
            "tier": "admin" if is_admin else "free",
            "role": "admin" if is_admin else "user",
        }
    return {k: v for k, v in app_user.items() if k not in ("password_hash", "mfa_secret")}


@app.get("/v1/users/me/watchlist")
async def user_watchlist_get(current_user: str = Depends(get_current_user)):
    import db as _db
    user = _db.get_user_by_username(current_user)
    if not user:
        raise HTTPException(status_code=404, detail="User not found in multi-user table")
    return {"tickers": _db.get_user_watchlist(user["user_id"])}


@app.post("/v1/users/me/watchlist")
async def user_watchlist_add(req: UserWatchlistAddRequest, current_user: str = Depends(get_current_user)):
    import db as _db
    ticker = _validate_ticker(req.ticker)
    user = _db.get_user_by_username(current_user)
    if not user:
        raise HTTPException(status_code=404, detail="User not found in multi-user table")
    ok, reason = _db.add_to_user_watchlist(user["user_id"], ticker)
    if not ok:
        raise HTTPException(status_code=400, detail=reason)
    return {"ok": True, "reason": reason}


@app.delete("/v1/users/me/watchlist/{ticker}")
async def user_watchlist_remove(ticker: str, current_user: str = Depends(get_current_user)):
    import db as _db
    ticker = _validate_ticker(ticker)
    user = _db.get_user_by_username(current_user)
    if not user:
        raise HTTPException(status_code=404, detail="User not found in multi-user table")
    removed = _db.remove_from_user_watchlist(user["user_id"], ticker)
    return {"ok": removed}


@app.get("/v1/users/me/portfolio")
async def user_portfolio_get(paper: bool = False, current_user: str = Depends(get_current_user)):
    import db as _db
    user = _db.get_user_by_username(current_user)
    if not user:
        raise HTTPException(status_code=404, detail="User not found in multi-user table")
    return {"positions": _db.get_user_portfolio(user["user_id"], paper=paper)}


@app.post("/v1/users/me/portfolio")
async def user_portfolio_upsert(req: UserPortfolioUpsertRequest, current_user: str = Depends(get_current_user)):
    import db as _db
    ticker = _validate_ticker(req.ticker)
    user = _db.get_user_by_username(current_user)
    if not user:
        raise HTTPException(status_code=404, detail="User not found in multi-user table")
    _db.upsert_user_portfolio_position(
        user["user_id"], ticker, req.shares,
        cost_basis=req.cost_basis, purchase_date=req.purchase_date, paper=req.paper,
    )
    return {"ok": True}


@app.delete("/v1/users/me/portfolio/{ticker}")
async def user_portfolio_remove(ticker: str, paper: bool = False, current_user: str = Depends(get_current_user)):
    import db as _db
    ticker = _validate_ticker(ticker)
    user = _db.get_user_by_username(current_user)
    if not user:
        raise HTTPException(status_code=404, detail="User not found in multi-user table")
    removed = _db.remove_user_portfolio_position(user["user_id"], ticker, paper=paper)
    return {"ok": removed}


@app.get("/v1/users/me/settings")
async def user_settings_get(current_user: str = Depends(get_current_user)):
    import db as _db
    user = _db.get_user_by_username(current_user)
    if not user:
        return {"settings": {}}
    return {"settings": _db.get_user_settings(user["user_id"])}


@app.put("/v1/users/me/settings/{key}")
async def user_settings_set(key: str, value: str = Body(...), current_user: str = Depends(get_current_user)):
    import db as _db
    user = _db.get_user_by_username(current_user)
    if not user:
        raise HTTPException(status_code=404, detail="User not found in multi-user table")
    _db.set_user_setting(user["user_id"], key[:64], str(value)[:1024])
    return {"ok": True}


# ── APNs device token endpoints ───────────────────────────────────────────────

@app.post("/v1/users/me/device-tokens")
async def register_device_token_endpoint(req: DeviceTokenRequest, current_user: str = Depends(get_current_user)):
    import db as _db
    user = _db.get_user_by_username(current_user)
    if not user:
        raise HTTPException(status_code=404, detail="User not found in multi-user table")
    if user["tier"] not in ("pro", "premium"):
        raise HTTPException(status_code=403, detail="Push notifications require Pro or Premium tier")
    if not re.match(r"^[a-f0-9]{64}$", req.device_token.lower()):
        raise HTTPException(status_code=400, detail="Invalid APNs device token format")
    _db.register_device_token(user["user_id"], req.device_token, req.platform)
    return {"ok": True}


@app.delete("/v1/users/me/device-tokens/{device_token}")
async def unregister_device_token_endpoint(device_token: str, current_user: str = Depends(get_current_user)):
    import db as _db
    user = _db.get_user_by_username(current_user)
    if not user:
        raise HTTPException(status_code=404, detail="User not found in multi-user table")
    removed = _db.unregister_device_token(user["user_id"], device_token)
    return {"ok": removed}


# ── Stripe billing endpoints ──────────────────────────────────────────────────

@app.post("/api/billing/stripe/webhook")
async def stripe_webhook(request: Request):
    if not _STRIPE_SECRET_KEY or not _STRIPE_WEBHOOK_SECRET:
        raise HTTPException(status_code=503, detail="Stripe not configured")
    import stripe as stripe_lib
    stripe_lib.api_key = _STRIPE_SECRET_KEY
    payload = await request.body()
    sig = request.headers.get("stripe-signature", "")
    try:
        event = stripe_lib.Webhook.construct_event(payload, sig, _STRIPE_WEBHOOK_SECRET)
    except Exception as exc:
        logger.warning("STRIPE_WEBHOOK_INVALID sig=%s err=%s", sig[:20], exc)
        raise HTTPException(status_code=400, detail="Invalid signature")
    import db as _db
    etype = event["type"]
    if etype in ("customer.subscription.created", "customer.subscription.updated"):
        sub = event["data"]["object"]
        customer_id = sub.get("customer")
        status = sub.get("status")
        plan_id = (sub.get("items", {}).get("data") or [{}])[0].get("price", {}).get("lookup_key", "free")
        tier = "premium" if "premium" in plan_id else "pro" if "pro" in plan_id else "free"
        if status not in ("active", "trialing"):
            tier = "free"
        with _db.get_conn() as conn:
            row = conn.execute(
                "SELECT user_id FROM app_users WHERE stripe_customer_id = ?", (customer_id,)
            ).fetchone()
        if row:
            _db.update_user(row["user_id"], tier=tier)
            logger.info("STRIPE_TIER_UPDATE customer=%s tier=%s", customer_id, tier)
    elif etype == "customer.subscription.deleted":
        sub = event["data"]["object"]
        customer_id = sub.get("customer")
        with _db.get_conn() as conn:
            row = conn.execute(
                "SELECT user_id FROM app_users WHERE stripe_customer_id = ?", (customer_id,)
            ).fetchone()
        if row:
            _db.update_user(row["user_id"], tier="free")
            logger.info("STRIPE_SUB_CANCELLED customer=%s", customer_id)
    return {"ok": True}


# ── APNs push helper (used by earnings scheduler) ─────────────────────────────

_apns_token_cache: dict[str, Any] = {"jwt": None, "minted_at": 0.0}

def _apns_provider_jwt() -> str:
    """Return a cached APNs provider JWT, regenerating only when older than
    ~50 minutes. Apple requires the token to carry an `exp`-equivalent lifetime
    of < 1 hour and rejects tokens refreshed more than once every 20 minutes,
    so caching is both a security and a correctness requirement."""
    import time as _time
    from pathlib import Path as _Path
    from jose import jwt as _jwt
    now = int(_time.time())
    cached = _apns_token_cache.get("jwt")
    if cached and (now - _apns_token_cache.get("minted_at", 0)) < 3000:
        return cached
    key_data = _Path(_APNS_KEY_PATH).read_text()
    header_payload = {"alg": "ES256", "kid": _APNS_KEY_ID}
    # `iat`/`exp` bound the token to a 50-minute validity window.
    claims = {"iss": _APNS_TEAM_ID, "iat": now, "exp": now + 3000}
    token = _jwt.encode(claims, key_data, algorithm="ES256", headers=header_payload)
    _apns_token_cache["jwt"] = token
    _apns_token_cache["minted_at"] = now
    return token

async def _send_apns_push(device_token: str, title: str, body: str, data: dict | None = None) -> bool:
    """Send a single APNs push notification via HTTP/2. Returns True on success."""
    if not all([_APNS_KEY_ID, _APNS_TEAM_ID, _APNS_BUNDLE_ID, _APNS_KEY_PATH]):
        return False
    try:
        apns_jwt = _apns_provider_jwt()
        url = f"https://api.push.apple.com/3/device/{device_token}"
        payload = {
            "aps": {
                "alert": {"title": title, "body": body},
                "sound": "default",
                "badge": 1,
            }
        }
        if data:
            payload["data"] = data
        headers = {
            "authorization": f"bearer {apns_jwt}",
            "apns-topic": _APNS_BUNDLE_ID,
            "apns-push-type": "alert",
        }
        async with httpx.AsyncClient(http2=True) as client:
            resp = await client.post(url, json=payload, headers=headers, timeout=10)
        return resp.status_code == 200
    except Exception as exc:
        logger.warning("APNS_PUSH_FAIL token=%s err=%s", device_token[:16], exc)
        return False


async def _broadcast_earnings_push(ticker: str, company: str, report_date: str) -> None:
    """Push earnings reminder to all eligible device tokens for this ticker."""
    import db as _db
    tokens = _db.get_all_active_device_tokens()
    title = f"{ticker} Earnings"
    body = f"{company} reports on {report_date}"
    results = await asyncio.gather(
        *[_send_apns_push(t["device_token"], title, body, {"ticker": ticker}) for t in tokens],
        return_exceptions=True,
    )
    sent = sum(1 for r in results if r is True)
    logger.info("APNS_BROADCAST ticker=%s sent=%d/%d", ticker, sent, len(tokens))


# ── Existing endpoints ────────────────────────────────────────────────────────

@app.get("/api/search")
async def search_stocks(q: str = ""):
    q = q.strip()[:50]
    q = re.sub(r"[^a-zA-Z0-9\-\. ]", "", q).lower()
    if not q or len(q) < 1:
        return []
    q_upper = q.upper()

    def _search_rank(ticker: str) -> tuple[int, int, str]:
        lowered = ticker.lower()
        name = str(TICKER_NAMES.get(ticker, ticker)).lower()
        alias_exact = SEARCH_ALIASES.get(q_upper) == ticker
        if lowered == q:
            return (0, len(ticker), ticker)
        if alias_exact:
            return (0, len(ticker), ticker)
        if lowered.startswith(q):
            return (1, len(ticker), ticker)
        if name.startswith(q):
            return (2, len(ticker), ticker)
        return (3, len(ticker), ticker)

    # Broad one-letter searches can fan out into dozens of yfinance calls, so cap them.
    matched = sorted((
        t for t in UNIVERSE
        if q in t.lower()
        or q in str(TICKER_NAMES.get(t, t)).lower()
        or SEARCH_ALIASES.get(q_upper) == t
    ), key=_search_rank)[:SEARCH_RESULTS_LIMIT]
    if not matched:
        return []
    started = datetime.now(timezone.utc)
    infos = await asyncio.gather(*[get_info_with_timeout(t) for t in matched], return_exceptions=True)
    results = []
    for ticker, info in zip(matched, infos):
        if isinstance(info, Exception):
            results.append({
                "ticker": ticker,
                "name": TICKER_NAMES.get(ticker, ticker),
                "sector": "",
                "price": None,
                "pe": None,
                "peg": None,
                "pb": None,
                "ev_ebitda": None,
                "fcf_yield": None,
                "market_cap": None,
                "volume": None,
            })
            continue
        try:
            market_cap = info.get("marketCap")
            pe         = info.get("trailingPE")
            peg        = info.get("pegRatio")
            pb         = info.get("priceToBook")
            ev_ebitda  = info.get("enterpriseToEbitda")
            fcf        = info.get("freeCashflow")
            volume     = info.get("averageVolume")
            price      = info.get("currentPrice") or info.get("regularMarketPrice")
            name       = info.get("shortName", ticker)
            sector     = info.get("sector", "")
            fcf_yield  = calc_fcf_yield(fcf, market_cap)
            results.append({
                "ticker": ticker, "name": name, "sector": sector,
                "price": round(price, 2) if price else None,
                "pe": round(pe, 2) if pe else None,
                "peg": round(peg, 2) if peg else None,
                "pb": round(pb, 2) if pb else None,
                "ev_ebitda": round(ev_ebitda, 2) if ev_ebitda else None,
                "fcf_yield": fcf_yield,
                "market_cap": market_cap, "volume": volume,
            })
        except Exception:
            results.append({
                "ticker": ticker,
                "name": TICKER_NAMES.get(ticker, ticker),
                "sector": "",
                "price": None,
                "pe": None,
                "peg": None,
                "pb": None,
                "ev_ebitda": None,
                "fcf_yield": None,
                "market_cap": None,
                "volume": None,
            })
    elapsed_ms = int((datetime.now(timezone.utc) - started).total_seconds() * 1000)
    logger.info("SEARCH query=%s matched=%s returned=%s duration_ms=%s", q, len(matched), len(results), elapsed_ms)
    return results


@app.get("/api/screen")
async def screen_stocks(
    index: Optional[str] = None,
    q: Optional[str] = None,
    sector: Optional[str] = None,
    min_pe: Optional[float] = None,
    max_pe: Optional[float] = None,
    min_peg: Optional[float] = None,
    max_peg: Optional[float] = None,
    min_pb: Optional[float] = None,
    max_pb: Optional[float] = None,
    min_ev_ebitda: Optional[float] = None,
    max_ev_ebitda: Optional[float] = None,
    min_fcf_yield: Optional[float] = None,
    max_fcf_yield: Optional[float] = None,
    min_market_cap: Optional[float] = None,
    max_market_cap: Optional[float] = None,
    min_volume: Optional[float] = None,
    max_volume: Optional[float] = None,
    min_rev_growth: Optional[float] = None,
    max_rev_growth: Optional[float] = None,
):
    if q:
        q = re.sub(r"[^a-zA-Z0-9\-\. ]", "", q.strip()[:50]).lower()
    index_map = {
        "sp500":     SP500_TICKERS,
        "nasdaq100": NASDAQ100_TICKERS,
        "ftse100":   FTSE100_TICKERS,
        "ftse250":   FTSE250_TICKERS,
    }
    pool_key = index or "__all__"
    cached_universe = _screen_universe_cache.get(pool_key)
    now = datetime.now(timezone.utc)
    if cached_universe and (now - cached_universe[1]).total_seconds() < _SCREEN_TTL:
        universe_rows = cached_universe[0]
    else:
        # Cache miss: batch the fetch (same pacing as prewarm) so we don't
        # rate-limit Yahoo by opening hundreds of concurrent connections.
        pool = index_map.get(index, UNIVERSE) if index else UNIVERSE
        universe_rows = await _build_universe_rows(pool)
        _screen_universe_cache[pool_key] = (universe_rows, now)

    results = []
    q_norm = (q or "").strip().lower()
    q_alias = SEARCH_ALIASES.get(q_norm.upper())  # e.g. "tsmc" → "TSM"
    for row in universe_rows:
        if q_norm:
            ticker = str(row.get("ticker", ""))
            ticker_match = q_norm in ticker.lower()
            name_match = q_norm in str(row.get("name", "")).lower()
            alias_match = q_alias is not None and q_alias == ticker
            if not ticker_match and not name_match and not alias_match:
                continue
        stock_sector = row.get("sector", "")
        pe = row.get("pe")
        peg = row.get("peg")
        pb = row.get("pb")
        ev_ebitda = row.get("ev_ebitda")
        fcf_yield = row.get("fcf_yield")
        market_cap = row.get("market_cap")
        volume = row.get("volume")
        rev_growth = row.get("rev_growth")
        if sector and sector.lower() not in stock_sector.lower():
            continue
        if min_pe is not None and (pe is None or pe < min_pe):
            continue
        if max_pe is not None and (pe is None or pe > max_pe):
            continue
        if min_peg is not None and (peg is None or peg < min_peg):
            continue
        if max_peg is not None and (peg is None or peg > max_peg):
            continue
        if min_pb is not None and (pb is None or pb < min_pb):
            continue
        if max_pb is not None and (pb is None or pb > max_pb):
            continue
        if min_ev_ebitda is not None and (ev_ebitda is None or ev_ebitda < min_ev_ebitda):
            continue
        if max_ev_ebitda is not None and (ev_ebitda is None or ev_ebitda > max_ev_ebitda):
            continue
        if min_fcf_yield is not None and (fcf_yield is None or fcf_yield < min_fcf_yield):
            continue
        if max_fcf_yield is not None and (fcf_yield is None or fcf_yield > max_fcf_yield):
            continue
        if min_market_cap is not None and (market_cap is None or market_cap < min_market_cap):
            continue
        if max_market_cap is not None and (market_cap is None or market_cap > max_market_cap):
            continue
        if min_volume is not None and (volume is None or volume < min_volume):
            continue
        if max_volume is not None and (volume is None or volume > max_volume):
            continue
        if min_rev_growth is not None and (rev_growth is None or rev_growth < min_rev_growth):
            continue
        if max_rev_growth is not None and (rev_growth is None or rev_growth > max_rev_growth):
            continue
        results.append(row)
    return results


@app.get("/api/stock/{ticker}")
def get_stock(ticker: str):
    ticker = _validate_ticker(ticker)
    try:
        t = yf.Ticker(ticker)
        info = t.info
        hist = t.history(period="1y")
        history = [
            {"date": str(row.name.date()), "close": round(row["Close"], 2)}
            for _, row in hist.iterrows()
        ]
        market_cap = info.get("marketCap")
        fcf_yield = calc_fcf_yield(info.get("freeCashflow"), market_cap)

        return {
            "ticker": ticker.upper(),
            "name": info.get("shortName", ticker),
            "sector": info.get("sector", "N/A"),
            "price": info.get("currentPrice") or info.get("regularMarketPrice"),
            "change_pct": info.get("regularMarketChangePercent"),
            "market_cap": market_cap,
            "pe": info.get("trailingPE"),
            "peg": info.get("pegRatio"),
            "pb": info.get("priceToBook"),
            "ev_ebitda": info.get("enterpriseToEbitda"),
            "fcf_yield": fcf_yield,
            "eps_growth": info.get("earningsGrowth"),
            "revenue_growth": info.get("revenueGrowth"),
            "profit_margin": info.get("profitMargins"),
            "debt_to_equity": info.get("debtToEquity"),
            "beta": info.get("beta"),
            "week_52_high": info.get("fiftyTwoWeekHigh"),
            "week_52_low": info.get("fiftyTwoWeekLow"),
            "description": info.get("longBusinessSummary", ""),
            "history": history,
        }
    except HTTPException:
        raise
    except Exception:
        raise HTTPException(status_code=404, detail="Could not fetch stock data")


@app.get("/api/stock/{ticker}/peers")
async def get_peer_valuation(ticker: str):
    ticker = _validate_ticker(ticker)
    try:
        info = await get_info(ticker)
    except HTTPException:
        raise
    except Exception:
        raise HTTPException(status_code=404, detail="Could not fetch stock data")

    sector = info.get("sector", "")
    target = {
        "pe":        info.get("trailingPE"),
        "peg":       info.get("pegRatio"),
        "pb":        info.get("priceToBook"),
        "ev_ebitda": info.get("enterpriseToEbitda"),
        "fcf_yield": calc_fcf_yield(info.get("freeCashflow"), info.get("marketCap")),
    }

    if not sector:
        return {"peers_count": 0, "sector": None, "comparison": {}}

    peer_candidates = [t for t in UNIVERSE if t != ticker]
    peer_infos = await asyncio.gather(*[get_info_with_timeout(t, SEARCH_INFO_TIMEOUT_SEC) for t in peer_candidates], return_exceptions=True)
    results = list(zip(peer_candidates, peer_infos))

    peer_data = []
    for t, pinfo in results:
        if isinstance(pinfo, Exception):
            continue
        if pinfo.get("sector", "") != sector:
            continue
        mc = pinfo.get("marketCap")
        peer_data.append({
            "pe":        pinfo.get("trailingPE"),
            "peg":       pinfo.get("pegRatio"),
            "pb":        pinfo.get("priceToBook"),
            "ev_ebitda": pinfo.get("enterpriseToEbitda"),
            "fcf_yield": calc_fcf_yield(pinfo.get("freeCashflow"), mc),
        })

    if not peer_data:
        return {"peers_count": 0, "sector": sector, "comparison": {}}

    def peer_median(key):
        vals = [p[key] for p in peer_data if p.get(key) is not None]
        return statistics.median(vals) if vals else None

    medians = {k: peer_median(k) for k in ["pe", "peg", "pb", "ev_ebitda", "fcf_yield"]}

    comparison = {}
    for key in ["pe", "peg", "pb", "ev_ebitda"]:
        if target[key] is not None and medians[key] is not None:
            comparison[key] = "undervalued" if target[key] < medians[key] else "overvalued"
        else:
            comparison[key] = None

    if target["fcf_yield"] is not None and medians["fcf_yield"] is not None:
        comparison["fcf_yield"] = "undervalued" if target["fcf_yield"] > medians["fcf_yield"] else "overvalued"
    else:
        comparison["fcf_yield"] = None

    return {
        "peers_count": len(peer_data),
        "sector": sector,
        "medians": {k: round(v, 2) if v is not None else None for k, v in medians.items()},
        "comparison": comparison,
    }


@app.get("/api/watchlist")
async def get_watchlist(names_only: bool = False, current_user: str = Depends(get_current_user)):
    import db as _db
    db_user = _get_db_user(current_user)
    if db_user:
        tickers = _db.get_user_watchlist(db_user["user_id"])
    else:
        tickers = load_watchlist()
    if names_only:
        return tickers
    infos = await asyncio.gather(*[get_info(t) for t in tickers], return_exceptions=True)
    results = []
    for ticker, info in zip(tickers, infos):
        if isinstance(info, Exception):
            results.append({"ticker": ticker, "name": ticker, "price": None, "change_pct": None})
            continue
        price      = info.get("currentPrice") or info.get("regularMarketPrice")
        change_pct = info.get("regularMarketChangePercent")
        results.append({
            "ticker": ticker,
            "name": info.get("shortName", ticker),
            "price": round(price, 2) if price else None,
            "change_pct": round(change_pct, 2) if change_pct else None,
        })
    return results


@app.get("/api/sentiment")
@limiter.limit("20/minute")
async def sentiment_scan(request: Request, ticker: Optional[str] = None, watchlist: bool = False, refresh: bool = False):
    """Run sentiment scanner. Results cached 30 min (watchlist) / 15 min (ticker). Pass refresh=true to bypass."""
    if ticker:
        ticker = _validate_ticker(ticker)
    return run_sentiment_scanner(ticker=ticker, watchlist_only=watchlist, refresh=refresh)


async def _run_predictions_bg():
    try:
        print("[Predictions] Background generation triggered by watchlist add...")
        await generate_predictions()
        print("[Predictions] Background generation completed.")
    except Exception as e:
        print(f"[Predictions] Background generation failed: {e}")


@app.post("/api/watchlist/{ticker}")
async def add_to_watchlist(ticker: str, background_tasks: BackgroundTasks, current_user: str = Depends(get_current_user)):
    import db as _db
    ticker = _validate_ticker(ticker)
    db_user = _get_db_user(current_user)
    if db_user:
        ok, reason = _db.add_to_user_watchlist(db_user["user_id"], ticker)
        if not ok:
            raise HTTPException(status_code=400, detail=reason)
        tickers = _db.get_user_watchlist(db_user["user_id"])
    else:
        tickers = load_watchlist()
        if ticker not in tickers:
            tickers.append(ticker)
            save_watchlist(tickers)
    background_tasks.add_task(_run_predictions_bg)
    import finnhub_prices as _fp
    _fp.subscribe([ticker])
    return {"watchlist": tickers}


@app.delete("/api/watchlist/{ticker}")
def remove_from_watchlist(ticker: str, current_user: str = Depends(get_current_user)):
    import db as _db
    ticker = _validate_ticker(ticker)
    db_user = _get_db_user(current_user)
    if db_user:
        _db.remove_from_user_watchlist(db_user["user_id"], ticker)
        tickers = _db.get_user_watchlist(db_user["user_id"])
    else:
        tickers = load_watchlist()
        tickers = [t for t in tickers if t != ticker]
        save_watchlist(tickers)
    import finnhub_prices as _fp
    _fp.unsubscribe(ticker)
    return {"watchlist": tickers}


@app.get("/api/prices")
async def get_live_prices(current_user: str = Depends(get_current_user)):
    """Live prices from Finnhub WebSocket + REST, falling back to yfinance cache."""
    import finnhub_prices as _fp
    watchlist = load_watchlist()
    live = _fp.get_prices()
    result: dict[str, dict] = {}
    for ticker in watchlist:
        if ticker in live:
            result[ticker] = live[ticker]
        else:
            # Fall back to yfinance in-memory cache (up to 5 min stale)
            cached = _info_cache.get(ticker)
            if cached:
                info, _ = cached
                price = info.get("currentPrice") or info.get("regularMarketPrice")
                change_pct = info.get("regularMarketChangePercent") or 0.0
                if price:
                    result[ticker] = {
                        "price": round(float(price), 2),
                        "change_pct": round(float(change_pct), 2),
                        "source": "delayed",
                    }
    return result


class RecommendRequest(BaseModel):
    query: str

class TradeRequest(BaseModel):
    ticker: str
    qty: float
    price: float
    date: Optional[str] = None

@app.post("/api/recommend")
def recommend(req: RecommendRequest):
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        raise HTTPException(status_code=500, detail="ANTHROPIC_API_KEY not set in .env")
    client = anthropic.Anthropic(api_key=api_key)
    try:
        started = datetime.now(timezone.utc)
        message = client.messages.create(
            model=ANTHROPIC_MODEL,
            max_tokens=RECOMMEND_MAX_TOKENS,
            messages=[{"role": "user", "content": (
                "You are a helpful stock market analyst. The user is looking for stock recommendations. "
                "Provide thoughtful analysis and suggest specific tickers with reasoning. "
                "Always include a disclaimer that this is not financial advice.\n\n"
                f"User request: {req.query}"
            )}],
        )
        elapsed_ms = int((datetime.now(timezone.utc) - started).total_seconds() * 1000)
        logger.info("RECOMMEND query=%s model=%s duration_ms=%s", req.query, ANTHROPIC_MODEL, elapsed_ms)
        return {"response": message.content[0].text}
    except anthropic.AuthenticationError:
        raise HTTPException(status_code=500, detail="Anthropic API key is invalid or expired. Check ANTHROPIC_API_KEY in your .env file.")
    except anthropic.RateLimitError:
        raise HTTPException(status_code=429, detail="Anthropic rate limit reached. Please wait a moment and try again.")
    except anthropic.BadRequestError as e:
        detail = getattr(e, "message", "") or str(e)
        if "credit balance is too low" in detail.lower():
            raise HTTPException(status_code=402, detail="Anthropic credit balance is too low. Add credits or switch this feature to a non-Claude fallback.")
        raise HTTPException(status_code=400, detail=f"Anthropic request error: {detail}")
    except anthropic.APIStatusError as e:
        raise HTTPException(status_code=500, detail=f"Anthropic API error: {e.message}")


def fetch_stock_research_for_ticker(ticker: str) -> str:
    """Run the stock research agent in-process for a single ticker and return the response text."""
    try:
        if not ticker:
            return "" 
        req = RecommendRequest(query=ticker)
        result = _stock_research_impl(req)
        return result.get("response", "") if isinstance(result, dict) else str(result)
    except Exception as e:
        logger.warning("Stock research fetch failed for %s: %s", ticker, e)
        return f"Failed to fetch stock research for {ticker}: {e}"


def _extract_research_tickers(query: str) -> list[str]:
    raw = (query or "").strip()
    if not raw:
        return []

    import re as _re

    tokens = [_re.sub(r"[^A-Za-z0-9.]", "", tok).upper().strip(".") for tok in raw.replace(",", " ").split()]
    tokens = [tok for tok in tokens if tok]
    if not tokens:
        return []

    if all(1 <= len(tok) <= 5 and tok.replace(".", "").isalnum() for tok in tokens):
        return list(dict.fromkeys(tokens))

    if len(tokens) == 1 and 1 <= len(tokens[0]) <= 5 and tokens[0].replace(".", "").isalnum():
        return tokens

    return []


def _build_live_research_context(ticker_symbol: str) -> tuple[str, bool]:
    import re as _re
    from datetime import datetime as _dt

    def _fmt_large(v):
        try:
            v = float(v)
            if v <= 0:
                return "N/A"
            if v >= 1e12:
                return f"${v/1e12:.2f}T"
            if v >= 1e9:
                return f"${v/1e9:.2f}B"
            if v >= 1e6:
                return f"${v/1e6:.2f}M"
            return f"${v:,.0f}"
        except Exception:
            return "N/A"

    def _fmt_pct(v):
        try:
            return f"{float(v) * 100:.1f}%"
        except Exception:
            return "N/A"

    def _safe(v, fmt=None):
        if v is None:
            return "N/A"
        try:
            return fmt(v) if fmt else str(v)
        except Exception:
            return "N/A"

    as_of = _dt.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    lines = [f"## Live Market Data for {ticker_symbol} (as of {as_of})"]
    has_live_data = False

    try:
        ticker = yf.Ticker(ticker_symbol)
        hist = ticker.history(period="1y", interval="1d")
        info = {}
        try:
            info = dict(ticker.info) if ticker.info else {}
        except Exception:
            pass

        if not hist.empty:
            has_live_data = True
            try:
                fi = ticker.fast_info
                lp = getattr(fi, "last_price", None)
                current_price = float(lp) if lp and float(lp) > 0 else float(hist["Close"].iloc[-1])
            except Exception:
                current_price = float(hist["Close"].iloc[-1])

            prev_close = float(hist["Close"].iloc[-2]) if len(hist) >= 2 else current_price
            day_chg_pct = (current_price - prev_close) / prev_close * 100 if prev_close else 0
            year_high = float(hist["Close"].max())
            year_low = float(hist["Close"].min())
            sma_50 = float(hist["Close"].iloc[-50:].mean()) if len(hist) >= 50 else None
            sma_200 = float(hist["Close"].iloc[-200:].mean()) if len(hist) >= 200 else None
            avg_vol_30 = int(hist["Volume"].iloc[-30:].mean()) if "Volume" in hist.columns and len(hist) >= 30 else None
            last_vol = int(hist["Volume"].iloc[-1]) if "Volume" in hist.columns else None
            vol_ratio = round(last_vol / avg_vol_30, 2) if last_vol and avg_vol_30 else None

            rsi_14 = None
            try:
                closes = hist["Close"]
                delta = closes.diff()
                gain = delta.clip(lower=0).rolling(14).mean()
                loss = (-delta.clip(upper=0)).rolling(14).mean()
                rs = gain / loss.replace(0, float("nan"))
                rsi_series = 100 - (100 / (1 + rs))
                rsi_14 = round(float(rsi_series.iloc[-1]), 1)
            except Exception:
                pass

            rec_key = info.get("recommendationKey") or ""
            lines += [
                f"**Current Price**: ${current_price:.2f}  ({day_chg_pct:+.2f}% vs prev close)",
                f"**52-Week Range**: ${year_low:.2f} - ${year_high:.2f}",
                f"**Market Cap**: {_fmt_large(info.get('marketCap'))}",
                f"**Enterprise Value**: {_fmt_large(info.get('enterpriseValue'))}",
                f"**Revenue (TTM)**: {_fmt_large(info.get('totalRevenue'))}",
                f"**Revenue Growth (YoY)**: {_fmt_pct(info.get('revenueGrowth'))}",
                f"**Gross Margin**: {_fmt_pct(info.get('grossMargins'))}",
                f"**Operating Margin**: {_fmt_pct(info.get('operatingMargins'))}",
                f"**Net Margin**: {_fmt_pct(info.get('profitMargins'))}",
                f"**EPS (TTM)**: {_safe(info.get('trailingEps'))}",
                f"**P/E (TTM)**: {_safe(info.get('trailingPE'))}",
                f"**Forward P/E**: {_safe(info.get('forwardPE'))}",
                f"**P/S Ratio**: {_safe(info.get('priceToSalesTrailing12Months'))}",
                f"**EV/Revenue**: {_safe(info.get('enterpriseToRevenue'))}",
                f"**EV/EBITDA**: {_safe(info.get('enterpriseToEbitda'))}",
                f"**Total Cash**: {_fmt_large(info.get('totalCash'))}",
                f"**Total Debt**: {_fmt_large(info.get('totalDebt'))}",
                f"**Free Cash Flow**: {_fmt_large(info.get('freeCashflow'))}",
                f"**Beta**: {_safe(info.get('beta'))}",
                f"**Shares Outstanding**: {_fmt_large(info.get('sharesOutstanding'))}",
                f"**Short % of Float**: {_fmt_pct(info.get('shortPercentOfFloat'))}",
                f"**Analyst Target Price**: {_safe(info.get('targetMeanPrice'))}",
                f"**Analyst Recommendation**: {rec_key.upper() if rec_key else 'N/A'}",
                f"**50-Day SMA**: {'${:.2f}'.format(sma_50) if sma_50 else 'N/A'}",
                f"**200-Day SMA**: {'${:.2f}'.format(sma_200) if sma_200 else 'N/A'}",
                f"**Avg Volume (30d)**: {avg_vol_30:,}" if avg_vol_30 else "**Avg Volume (30d)**: N/A",
                f"**Last Session Volume**: {last_vol:,}" if last_vol else "**Last Session Volume**: N/A",
                f"**Volume vs 30d Avg**: {vol_ratio}x" if vol_ratio else "**Volume vs 30d Avg**: N/A",
                (
                    f"**RSI (14)**: {rsi_14}"
                    + (" - overbought" if rsi_14 and rsi_14 > 70 else " - oversold" if rsi_14 and rsi_14 < 30 else " - neutral")
                )
                if rsi_14
                else "**RSI (14)**: N/A",
                (
                    f"**Price vs 50d SMA**: {'above' if current_price > sma_50 else 'below'} "
                    f"(${abs(current_price - sma_50):.2f} {'above' if current_price > sma_50 else 'below'})"
                )
                if sma_50
                else "**Price vs 50d SMA**: N/A",
                (
                    f"**Price vs 200d SMA**: {'above' if current_price > sma_200 else 'below'} "
                    f"(${abs(current_price - sma_200):.2f} {'above' if current_price > sma_200 else 'below'})"
                )
                if sma_200
                else "**Price vs 200d SMA**: N/A",
            ]
        else:
            lines.append("*Live price history could not be fetched for this ticker.*")

        try:
            news_items = ticker.news or []
            news_lines = []
            for item in news_items[:10]:
                content = item.get("content", item)
                title = content.get("title", "")
                pub_str = content.get("pubDate", "")
                pub_ts = item.get("providerPublishTime", 0)
                pub_date = pub_str[:10] if pub_str else (_dt.utcfromtimestamp(int(pub_ts)).strftime("%Y-%m-%d") if pub_ts else "recent")
                provider = content.get("provider", {})
                publisher = provider.get("displayName", "") if isinstance(provider, dict) else content.get("publisher", "")
                summary = _re.sub(r"<[^>]+>", "", content.get("summary", "")).strip()
                if title:
                    entry = f"- [{pub_date}] {title}" + (f" ({publisher})" if publisher else "")
                    if summary and len(summary) < 250:
                        entry += f"\n  {summary}"
                    news_lines.append(entry)
            if news_lines:
                has_live_data = True
                lines.append("\n## Recent News Headlines (live from Yahoo Finance)")
                lines.extend(news_lines)
            else:
                lines.append("\n## Recent News Headlines\n*No live headlines returned for this ticker.*")
        except Exception as news_error:
            logger.warning("news fetch failed for %s: %s", ticker_symbol, news_error)
            lines.append("\n## Recent News Headlines\n*Live news fetch failed.*")

        try:
            qis = ticker.quarterly_income_stmt
            if qis is not None and not qis.empty:
                has_live_data = True
                lines.append("\n## Quarterly Income Statement (last 4 quarters, from recent filings)")
                for row_name in ["Total Revenue", "Gross Profit", "Operating Income", "Net Income", "EBITDA", "Basic EPS"]:
                    if row_name in qis.index:
                        vals = []
                        for c in qis.columns[:4]:
                            try:
                                vals.append(f"{str(c)[:10]}: {_fmt_large(float(qis.loc[row_name, c]))}")
                            except Exception:
                                vals.append(f"{str(c)[:10]}: N/A")
                        lines.append(f"**{row_name}**: " + " | ".join(vals))
        except Exception as income_error:
            logger.debug("quarterly_income_stmt failed for %s: %s", ticker_symbol, income_error)

        try:
            qbs = ticker.quarterly_balance_sheet
            if qbs is not None and not qbs.empty:
                has_live_data = True
                col = qbs.columns[0]
                lines.append(f"\n## Quarterly Balance Sheet (most recent: {str(col)[:10]})")
                for row_name in ["Total Assets", "Total Liabilities Net Minority Interest", "Stockholders Equity", "Cash And Cash Equivalents", "Total Debt", "Net Debt"]:
                    if row_name in qbs.index:
                        try:
                            lines.append(f"**{row_name}**: {_fmt_large(float(qbs.loc[row_name, col]))}")
                        except Exception:
                            pass
        except Exception as balance_error:
            logger.debug("quarterly_balance_sheet failed for %s: %s", ticker_symbol, balance_error)

        try:
            qcf = ticker.quarterly_cashflow
            if qcf is not None and not qcf.empty:
                has_live_data = True
                lines.append("\n## Quarterly Cash Flow (last 4 quarters)")
                for row_name in ["Operating Cash Flow", "Free Cash Flow", "Capital Expenditure", "Issuance Of Debt", "Repurchase Of Capital Stock"]:
                    if row_name in qcf.index:
                        vals = []
                        for c in qcf.columns[:4]:
                            try:
                                vals.append(f"{str(c)[:10]}: {_fmt_large(float(qcf.loc[row_name, c]))}")
                            except Exception:
                                vals.append(f"{str(c)[:10]}: N/A")
                        lines.append(f"**{row_name}**: " + " | ".join(vals))
        except Exception as cashflow_error:
            logger.debug("quarterly_cashflow failed for %s: %s", ticker_symbol, cashflow_error)

        try:
            recs = ticker.recommendations
            if recs is not None and not recs.empty:
                has_live_data = True
                lines.append("\n## Analyst Rating Changes (published reports)")
                for _, row in recs.tail(10).iterrows():
                    period = str(row.name)[:10]
                    firm = str(row.get("Firm", row.get("firm", "")) or "")
                    to_grade = str(row.get("To Grade", row.get("toGrade", "")) or "")
                    from_grade = str(row.get("From Grade", row.get("fromGrade", "")) or "")
                    action = str(row.get("Action", row.get("action", "")) or "")
                    parts = [f"[{period}]"]
                    if firm:
                        parts.append(firm)
                    if action:
                        parts.append(action.upper())
                    if to_grade:
                        parts.append(f"-> {to_grade}")
                    if from_grade and from_grade != to_grade:
                        parts.append(f"(was {from_grade})")
                    lines.append("- " + " ".join(parts))
        except Exception as recs_error:
            logger.debug("recommendations failed for %s: %s", ticker_symbol, recs_error)

        try:
            apt = ticker.analyst_price_targets
            if apt is not None:
                has_live_data = True
                lines.append("\n## Analyst Price Targets (consensus)")
                for label, key in [("Current", "current"), ("Low", "low"), ("High", "high"), ("Mean", "mean"), ("Median", "median")]:
                    try:
                        value = apt.get(key)
                        if value:
                            lines.append(f"**{label} Target**: ${float(value):.2f}")
                    except Exception:
                        pass
        except Exception as targets_error:
            logger.debug("analyst_price_targets failed for %s: %s", ticker_symbol, targets_error)

        try:
            eh = ticker.earnings_history
            if eh is not None and not eh.empty:
                has_live_data = True
                lines.append("\n## Earnings History (actual vs estimate)")
                for _, row in eh.tail(6).iterrows():
                    period = str(row.get("quarter", row.name))[:10]
                    est = row.get("epsEstimate")
                    actual = row.get("epsActual")
                    surp_pct = row.get("surprisePercent")
                    parts = [f"[{period}]"]
                    if est is not None:
                        parts.append(f"Est: {est:.2f}")
                    if actual is not None:
                        parts.append(f"Actual: {actual:.2f}")
                    if surp_pct is not None:
                        parts.append(f"Surprise: {surp_pct:+.1f}%")
                    lines.append("- " + " | ".join(parts))
        except Exception as earnings_error:
            logger.debug("earnings_history failed for %s: %s", ticker_symbol, earnings_error)
    except Exception as core_error:
        logger.warning("live research context build failed for %s: %s", ticker_symbol, core_error)
        lines.append("*Live market data could not be fetched for this ticker.*")

    lines.append(
        "\n**CRITICAL INSTRUCTION**: Base your entire report on the live data above. "
        "Do NOT substitute training-data figures for any metric. If a value shows N/A, "
        "report it as unavailable. Do not add facts that are not present in the live data package."
    )
    return "\n".join(lines), has_live_data


def _stock_research_impl(req: RecommendRequest):
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        raise HTTPException(status_code=500, detail="ANTHROPIC_API_KEY not set in .env")
    client = anthropic.Anthropic(api_key=api_key)

    tickers = _extract_research_tickers(req.query)
    if not tickers:
        raise HTTPException(
            status_code=400,
            detail=(
                "Real-time stock research now requires one or more explicit ticker symbols "
                "(for example: NVDA or NVDA AMD TSM). Sector, theme, or company-description "
                "queries without tickers are disabled because reports must be built from live market data, not training data."
            ),
        )

    ticker_contexts = []
    live_tickers = []
    unavailable_tickers = []
    for ticker in tickers[:8]:
        context, has_live_data = _build_live_research_context(ticker)
        ticker_contexts.append(context)
        if has_live_data:
            live_tickers.append(ticker)
        else:
            unavailable_tickers.append(ticker)

    if not live_tickers:
        raise HTTPException(
            status_code=503,
            detail=(
                "Live market data was unavailable for the requested ticker(s), so no report was generated. "
                "Please try again in a moment and only rely on reports when live data is present."
            ),
        )

    ticker_context = "\n\n".join(ticker_contexts)
    logger.info("RESEARCH_CONTEXT built for %s: %d tickers, live=%s unavailable=%s", req.query, len(tickers), live_tickers, unavailable_tickers)

    stock_research_prompt = f'''# Stock Research Command

{ticker_context}

User input: {req.query}

Only analyze these explicit ticker symbols when they have a live data package above: {", ".join(tickers)}.
Tickers with confirmed live market data: {", ".join(live_tickers)}.
Tickers without live data: {", ".join(unavailable_tickers) if unavailable_tickers else "None"}.

Hard rules:
- Do NOT use model memory or training data to fill gaps.
- Do NOT introduce companies or tickers that are not in the explicit ticker list.
- If a ticker lacks live data, say it was unavailable and stop there for that ticker.
- If a metric is not present in the live package, mark it unavailable.
- If live news is absent, say live news was unavailable.

## Research Steps

For each requested ticker, perform the following:

### 1. Company Overview
- Full company name, ticker, exchange
- Sector, industry, and sub-sector
- Business model summary based only on the live package
- Stage: early-stage, growth, mature, or turnaround only if the live package supports that conclusion

### 2. Recent News & Catalysts
Use the live news headlines provided above (if any). Summarise the most relevant:
- Earnings results or guidance updates
- Product launches, partnerships, or contracts
- Regulatory approvals or government contracts
- Executive changes or insider activity
- Analyst upgrades/downgrades
If no live headlines were provided, state clearly that live news was unavailable. Do NOT invent or recall news from training data.

### 3. Financial Snapshot
Use the live financial data provided above as the primary source. Summarise:
- Market cap, enterprise value, revenue (TTM) and YoY growth
- Gross margin, operating margin, net margin
- Cash position, total debt, free cash flow
- P/E, forward P/E, P/S, EV/Revenue, EV/EBITDA
Do not invent numbers; use exactly what was provided. Mark any field shown as N/A accordingly.

### 4. Technical Analysis
Use the live price data provided above:
- Current price vs. 52-week range and vs. 50/200-day SMAs
- Trend direction based on price vs. SMAs
- Volume: compare last session volume vs. 30-day average
- Note if price is extended or near support/resistance only as an inference from the live range and moving averages above

### 5. Bull Case
List 3-5 reasons this stock could outperform, but only if supported by the live package.

### 6. Bear Case
List 3-5 risks, but only if supported by the live package.

### 7. Verdict
Provide a concise investment summary:
- **Outlook**: Bullish / Neutral / Bearish
- **Time horizon**: Short-term trade vs. long-term hold
- **Entry considerations**: Current price attractive, wait for pullback, or avoid
- **Key thing to watch**: The single most important live metric or event to monitor

---

## Output Format

Present results as a structured report. If multiple tickers were given, use a separate section per ticker, then finish with a **Comparative Summary** table:

| Ticker | Sector | Market Cap | Revenue Growth | Outlook | Key Catalyst |
|--------|--------|------------|----------------|---------|---------------|
| ...    | ...    | ...        | ...            | ...     | ...           |

Be data-driven and concise. Cite the live package as the source. Flag any data that could not be verified from the live package.'''

    try:
        started = datetime.now(timezone.utc)
        message = client.messages.create(
            model=ANTHROPIC_MODEL,
            max_tokens=STOCK_RESEARCH_MAX_TOKENS,
            messages=[{"role": "user", "content": stock_research_prompt}],
        )
        elapsed_ms = int((datetime.now(timezone.utc) - started).total_seconds() * 1000)
        logger.info("STOCK_RESEARCH query=%s model=%s duration_ms=%s", req.query, ANTHROPIC_MODEL, elapsed_ms)
        return {"response": message.content[0].text}
    except anthropic.AuthenticationError:
        raise HTTPException(status_code=500, detail="Anthropic API key is invalid or expired. Check ANTHROPIC_API_KEY in your .env file.")
    except anthropic.RateLimitError:
        raise HTTPException(status_code=429, detail="Anthropic rate limit reached. Please wait a moment and try again.")
    except anthropic.APIStatusError as e:
        raise HTTPException(status_code=500, detail=f"Anthropic API error: {e.message}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Stock research failed: {str(e)}")


@app.post("/api/stock-research")
@limiter.limit("8/hour")
def stock_research(req: RecommendRequest, request: Request):
    return _stock_research_impl(req)


# ── Predictions endpoints ─────────────────────────────────────────────────────

def update_actuals(predictions: list[dict]) -> tuple[list[dict], bool]:
    today = str(date.today())
    updated = False
    for pred in predictions:
        if pred.get("actual_pct") is not None:
            continue
        if pred["date"] > today:
            continue
        try:
            hist = yf.Ticker(pred["ticker"]).history(period="10d")
            dates = [str(d.date()) for d in hist.index]
            # Find nearest trading day on or before the prediction date
            # (handles weekends/holidays when markets were closed)
            anchor = pred["date"]
            if anchor not in dates:
                prior = [d for d in dates if d <= anchor]
                if not prior:
                    continue
                anchor = prior[-1]
            idx = dates.index(anchor)
            # Use prev-close → this-day-close (day's actual move)
            # Works for today (no next-day data needed) and past dates
            if idx > 0:
                p0 = float(hist["Close"].iloc[idx - 1])
                p1 = float(hist["Close"].iloc[idx])
                if p0 and math.isfinite(p0) and math.isfinite(p1):
                    pred["actual_pct"] = round(((p1 - p0) / p0) * 100, 2)
                updated = True
        except Exception:
            continue
    return predictions, updated


async def fetch_rss_headlines(feeds: list[tuple], limit_per_feed: int = 8) -> list[str]:
    headlines = []
    async with httpx.AsyncClient(timeout=10, follow_redirects=True) as client:
        for source, url in feeds:
            try:
                r = await client.get(url, headers={"User-Agent": "Mozilla/5.0"})
                root = ET.fromstring(r.text)  # nosec B314 — RSS text only, no entity expansion
                for item in root.findall(".//item")[:limit_per_feed]:
                    title = item.find("title")
                    if title is not None and title.text:
                        headlines.append(f"[{source}] {title.text.strip()}")
            except Exception:
                continue
    return headlines


async def fetch_macro_data() -> dict:
    async def _fetch_one(sym: str, label: str):
        try:
            hist = await asyncio.to_thread(lambda: yf.Ticker(sym).history(period="5d"))
            if len(hist) >= 2:
                latest = float(hist["Close"].iloc[-1])
                prev = float(hist["Close"].iloc[-2])
                chg = ((latest - prev) / prev) * 100
                return label, {"price": round(latest, 2), "change_pct": round(chg, 2)}
        except Exception:
            pass
        return label, None

    results = await asyncio.gather(*[_fetch_one(sym, label) for sym, label in MACRO_SYMBOLS.items()])
    return {label: data for label, data in results if data is not None}


# ── Confidence- and score-bucket calibration (Phase: accuracy ops) ──────────
# Answers "does 'high confidence' actually hit more often than 'medium'?" and
# "do score=80 predictions outperform score=70?". Both questions go through
# `compute_calibration` per-ticker; this is the cross-ticker breakdown.
#
# Probability priors per confidence label (used for the Brier-style score):
# directional hit rate we'd implicitly be claiming when we label a call.
# Tuned to match _expected_return_from_score's confidence multipliers.
CONFIDENCE_PRIOR_HIT_RATE = {"high": 0.75, "medium": 0.62, "low": 0.55}
SCORE_BUCKETS = [(0, 50), (50, 60), (60, 70), (70, 80), (80, 90), (90, 101)]

def _bucket_calibration_metrics(rows: list[dict]) -> dict:
    """Return {count, win_rate, mean_signed_error, mean_abs_error, brier} for rows.
    Each row must have predicted_pct + actual_pct + confidence."""
    if not rows:
        return {
            "count": 0, "win_rate": None,
            "mean_signed_error_pct": None, "mean_abs_error_pct": None,
            "brier": None,
        }
    errors  = [r["actual_pct"] - r["predicted_pct"] for r in rows]
    correct = [1 if (r["predicted_pct"] > 0) == (r["actual_pct"] > 0) else 0 for r in rows]
    win_rate = sum(correct) / len(correct)
    # Brier-like score: mean((prior_prob - outcome)^2). Lower = better calibrated.
    # Compares each row's implicit prior (from its confidence label) to the realised
    # 0/1 directional hit. Perfect calibration → mirrors the prior, brier ≈ prior(1−prior).
    brier_terms = [
        (CONFIDENCE_PRIOR_HIT_RATE.get((r.get("confidence") or "medium").lower(), 0.62) - outcome) ** 2
        for r, outcome in zip(rows, correct)
    ]
    return {
        "count": len(rows),
        "win_rate":                round(win_rate, 3),
        "mean_signed_error_pct":   round(sum(errors) / len(errors), 3),
        "mean_abs_error_pct":      round(sum(abs(e) for e in errors) / len(errors), 3),
        "brier":                   round(sum(brier_terms) / len(brier_terms), 4),
    }


def compute_confidence_calibration(predictions: list[dict]) -> dict:
    """Cross-ticker calibration broken down by confidence label and score bucket.

    Returns:
      by_confidence: {high|medium|low: bucket_metrics, plus prior_hit_rate +
                      delta_vs_prior so the UI can flag mis-calibration}.
      by_score:      {0-50, 50-60, ..., 90-100: bucket_metrics}
      overall:       single bucket_metrics across all eligible predictions.
      ece:           expected calibration error across confidence buckets —
                     sum(|win_rate - prior| × bucket_weight). Lower = better.
    """
    completed = [
        p for p in predictions
        if p.get("actual_pct") is not None and p.get("predicted_pct") is not None
    ]

    # By confidence
    by_conf: dict[str, list] = {"high": [], "medium": [], "low": []}
    for p in completed:
        label = (p.get("confidence") or "medium").lower()
        if label in by_conf:
            by_conf[label].append(p)
    by_confidence = {}
    for label, rows in by_conf.items():
        m = _bucket_calibration_metrics(rows)
        m["prior_hit_rate"]  = CONFIDENCE_PRIOR_HIT_RATE[label]
        m["delta_vs_prior"]  = (
            round(m["win_rate"] - m["prior_hit_rate"], 3) if m["win_rate"] is not None else None
        )
        by_confidence[label] = m

    # By score bucket
    by_score = {}
    for lo, hi in SCORE_BUCKETS:
        bucket_label = f"{lo}-{hi if hi != 101 else 100}"
        rows = [p for p in completed if lo <= (p.get("score") or 0) < hi]
        by_score[bucket_label] = _bucket_calibration_metrics(rows)

    overall = _bucket_calibration_metrics(completed)

    # Expected calibration error — how far is each bucket's realised hit rate
    # from the prior we were implicitly claiming, weighted by bucket size.
    total = sum(m["count"] for m in by_confidence.values())
    if total > 0:
        ece = sum(
            (m["count"] / total) * abs(m["delta_vs_prior"])
            for m in by_confidence.values()
            if m["count"] > 0 and m["delta_vs_prior"] is not None
        )
        ece = round(ece, 3)
    else:
        ece = None

    return {
        "by_confidence": by_confidence,
        "by_score":      by_score,
        "overall":       overall,
        "ece":           ece,
        "sample_count":  len(completed),
    }


def compute_calibration(predictions: list[dict]) -> dict:
    """
    Per-stock calibration from completed predictions.
    Returns mean_bias (actual - predicted), directional accuracy, and inversion flag.
    Requires >= 3 samples to include a stock; inversion requires >= 5.
    """
    completed = [
        p for p in predictions
        if p.get("actual_pct") is not None and p.get("predicted_pct") is not None
    ]
    by_stock: dict[str, list] = {}
    for p in completed:
        by_stock.setdefault(p["ticker"], []).append(p)

    cal = {}
    for ticker, preds in by_stock.items():
        if len(preds) < 3:
            continue
        biases = [p["actual_pct"] - p["predicted_pct"] for p in preds]
        mean_bias = round(sum(biases) / len(biases), 3)
        correct = sum(1 for p in preds if (p["predicted_pct"] > 0) == (p["actual_pct"] > 0))
        acc = correct / len(preds)
        cal[ticker] = {
            "count":        len(preds),
            "mean_bias":    mean_bias,       # positive = we under-predict; negative = over-predict
            "accuracy_pct": round(acc * 100, 1),
            "inverted":     acc < 0.45 and len(preds) >= 5,
        }
    return cal


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def _downgrade_confidence(confidence: str, steps: int = 1) -> str:
    order = ["low", "medium", "high"]
    label = (confidence or "medium").lower()
    if label not in order:
        label = "medium"
    return order[max(0, order.index(label) - max(0, steps))]


def _build_prediction_calibration_model(store: bool = False) -> dict:
    if not PREDICTION_LEARNING_ENABLED:
        return {"enabled": False, "sample_count": 0, "recommendations": ["Prediction learning is disabled."]}
    try:
        import db as _db
        model = _db.build_prediction_calibration_model(PREDICTION_CALIBRATION_MIN_SAMPLES)
        model["enabled"] = True
        if store:
            model["calibration_id"] = _db.store_prediction_calibration(
                PREDICTION_MODEL_VERSION,
                PREDICTION_PROMPT_VERSION,
                model,
            )
        return model
    except Exception as exc:
        logger.warning("Prediction calibration model unavailable: %s", exc)
        return {"enabled": False, "sample_count": 0, "error": str(exc), "recommendations": []}


def _prediction_governance_status(calibration_model: dict) -> dict:
    if not PREDICTION_LEARNING_ENABLED:
        return {
            "status": "disabled",
            "tone": "muted",
            "message": "Prediction learning is disabled.",
            "gates": [],
        }

    global_1d = (calibration_model.get("global") or {}).get("1d") or {}
    samples = int(global_1d.get("samples") or 0)
    hit_rate = global_1d.get("directional_hit_rate_pct")
    mae = global_1d.get("mean_absolute_error_pct")
    gates = [
        {
            "name": "1D sample minimum",
            "passed": samples >= PREDICTION_CALIBRATION_MIN_SAMPLES,
            "value": samples,
            "target": PREDICTION_CALIBRATION_MIN_SAMPLES,
        },
        {
            "name": "Directional hit rate",
            "passed": hit_rate is None or hit_rate >= 50,
            "value": hit_rate,
            "target": ">= 50%",
        },
        {
            "name": "Forecast error",
            "passed": mae is None or mae <= 5.0,
            "value": mae,
            "target": "<= 5% MAE",
        },
    ]

    if samples < PREDICTION_CALIBRATION_MIN_SAMPLES:
        status = "warming"
        tone = "amber"
        message = f"Learning is collecting outcomes; {samples}/{PREDICTION_CALIBRATION_MIN_SAMPLES} 1D samples ready."
    elif hit_rate is not None and hit_rate < 45:
        status = "caution"
        tone = "red"
        message = f"Calibration active, but 1D hit rate is weak at {hit_rate:.1f}%."
    elif mae is not None and mae > 5.0:
        status = "watch"
        tone = "amber"
        message = f"Calibration active, but 1D error is high at {mae:.1f}% MAE."
    else:
        status = "active"
        tone = "green"
        message = "Calibration is active and within governance gates."

    return {
        "status": status,
        "tone": tone,
        "message": message,
        "gates": gates,
    }


def _format_prediction_learning_for_prompt(calibration_model: dict) -> str:
    if not calibration_model or not calibration_model.get("enabled"):
        return "=== SELF-LEARNING CALIBRATION ===\nNo durable calibration available yet.\n"

    global_1d = (calibration_model.get("global") or {}).get("1d") or {}
    lines = ["=== SELF-LEARNING CALIBRATION ==="]
    if not global_1d.get("eligible"):
        lines.append(
            f"Calibration warming up: {global_1d.get('samples', 0)} evaluated 1d outcomes. "
            "Do not overfit; use factor rules conservatively."
        )
    else:
        mean_error = global_1d.get("mean_error_pct")
        mean_error_text = f"{mean_error:+.2f}%" if isinstance(mean_error, (int, float)) else "N/A"
        lines.append(
            "1d calibration: "
            f"{global_1d.get('samples', 0)} samples, "
            f"{global_1d.get('directional_hit_rate_pct', 'N/A')}% directional hit rate, "
            f"{mean_error_text} mean error, "
            f"{global_1d.get('mean_absolute_error_pct', 'N/A')} MAE."
        )

    factor_learning = (calibration_model.get("factor_learning") or {}).get("1d") or {}
    eligible_factors = [
        (name, data)
        for name, data in factor_learning.items()
        if data.get("eligible") and data.get("correlation") is not None
    ]
    if eligible_factors:
        lines.append("Observed 1d factor relationships:")
        for name, data in sorted(eligible_factors, key=lambda item: abs(item[1].get("correlation") or 0), reverse=True)[:5]:
            lines.append(
                f"- {name}: corr {data['correlation']:+.3f}, {data['samples']} samples "
                f"({data.get('direction', 'weak')})."
            )
    else:
        lines.append("Factor calibration does not have enough evaluated samples yet.")

    recs = calibration_model.get("recommendations") or []
    if recs:
        lines.append("Calibration guidance:")
        lines.extend([f"- {rec}" for rec in recs[:4]])
    return "\n".join(lines) + "\n"


def _learning_adjustment_for_stock(ticker: str, stock_data: dict, calibration_model: dict) -> dict:
    if not PREDICTION_LEARNING_ENABLED or not calibration_model or not calibration_model.get("enabled"):
        return {
            "total_adjustment": 0.0,
            "bias_adjustment": 0.0,
            "factor_adjustment": 0.0,
            "invert_signal": False,
            "confidence_steps": 0,
            "source": "none",
            "notes": [],
        }

    notes: list[str] = []
    source = "global"
    ticker_stats = ((calibration_model.get("by_ticker") or {}).get(ticker) or {}).get("1d") or {}
    global_stats = (calibration_model.get("global") or {}).get("1d") or {}
    stat = ticker_stats if ticker_stats.get("eligible") else global_stats
    if ticker_stats.get("eligible"):
        source = "ticker"
    elif not global_stats.get("eligible"):
        stat = {}
        source = "warming"

    bias_adjustment = 0.0
    invert_signal = False
    confidence_steps = 0
    if stat:
        mean_error = stat.get("mean_error_pct")
        if isinstance(mean_error, (int, float)):
            weight = 0.55 if source == "ticker" else 0.25
            bias_adjustment = _clamp(mean_error * weight, -1.25, 1.25)
            if abs(bias_adjustment) >= 0.1:
                notes.append(f"{source} bias {bias_adjustment:+.2f}%")
        invert_signal = bool(stat.get("invert_signal"))
        if invert_signal:
            notes.append(f"{source} hit rate {stat.get('directional_hit_rate_pct')}%, invert")
        if stat.get("downshift_confidence"):
            confidence_steps = 1
            notes.append(f"{source} hit rate {stat.get('directional_hit_rate_pct')}%, confidence downshift")

    factor_adjustment = 0.0
    factors = stock_data.get("factor_scores") or {}
    factor_learning = (calibration_model.get("factor_learning") or {}).get("1d") or {}
    weighted_sum = 0.0
    weight_total = 0.0
    for factor in ("value", "momentum", "quality", "growth"):
        learned = factor_learning.get(factor) or {}
        corr = learned.get("correlation")
        score = factors.get(factor)
        if not learned.get("eligible") or corr is None or score is None:
            continue
        try:
            normalized = (float(score) - 50.0) / 50.0
            corr_float = float(corr)
        except (TypeError, ValueError):
            continue
        weighted_sum += normalized * corr_float
        weight_total += abs(corr_float)

    if weight_total > 0:
        factor_adjustment = _clamp((weighted_sum / weight_total) * 0.45, -0.45, 0.45)
        if abs(factor_adjustment) >= 0.1:
            notes.append(f"factor tilt {factor_adjustment:+.2f}%")

    total_adjustment = round(_clamp(bias_adjustment + factor_adjustment, -1.5, 1.5), 2)
    return {
        "total_adjustment": total_adjustment,
        "bias_adjustment": round(bias_adjustment, 2),
        "factor_adjustment": round(factor_adjustment, 2),
        "invert_signal": invert_signal,
        "confidence_steps": confidence_steps,
        "source": source,
        "notes": notes,
        "ticker_hit_rate_pct": ticker_stats.get("directional_hit_rate_pct"),
        "global_hit_rate_pct": global_stats.get("directional_hit_rate_pct"),
    }


_POSITIVE_KEYWORDS = {"upgrade", "beat", "beats", "strong", "raised", "record", "buyback",
                       "partnership", "growth", "acquisition", "outperform", "buy", "bullish",
                       "rally", "surge", "profit", "dividend", "approved", "wins"}
_NEGATIVE_KEYWORDS = {"downgrade", "miss", "misses", "recall", "loss", "cut", "cuts", "lawsuit",
                       "investigation", "layoffs", "warning", "disappoints", "underperform",
                       "sell", "bearish", "decline", "debt", "penalty", "fine", "probe"}


def calculate_sentiment_scores(stocks_data: list[dict], macro: dict) -> list[dict]:
    """Pre-calculate a structured sentiment score for each stock before the AI call."""

    # 1. VIX adjustment
    vix_price = macro.get("VIX (Fear Index)", {}).get("price", 20)
    if vix_price < 12:
        vix_adj = 0.4
    elif vix_price < 15:
        vix_adj = 0.2
    elif vix_price < 20:
        vix_adj = 0.0
    elif vix_price < 25:
        vix_adj = -0.3
    elif vix_price < 30:
        vix_adj = -0.6
    else:
        vix_adj = -1.0

    # 2. Market momentum adjustment (use S&P 500 5d change if available)
    sp_chg = macro.get("S&P 500", {}).get("change_pct", 0)
    if sp_chg > 3:
        momentum_adj = 0.4
    elif sp_chg > 1:
        momentum_adj = 0.2
    elif sp_chg > -1:
        momentum_adj = 0.0
    elif sp_chg > -3:
        momentum_adj = -0.2
    else:
        momentum_adj = -0.4

    market_base = round(vix_adj + momentum_adj, 2)

    for stock in stocks_data:
        beta = stock.get("beta") or 1.0

        # 3. Beta multiplier — amplifies market move for high/low beta stocks
        beta_adj = round(market_base * (beta - 1.0) * 0.5, 2)

        # 4. Headline sentiment — keyword scan of recent news titles
        headline_adj = 0.0
        news_titles = stock.get("recent_news", [])
        for title in news_titles:
            words = set(title.lower().split())
            pos_hits = words & _POSITIVE_KEYWORDS
            neg_hits = words & _NEGATIVE_KEYWORDS
            headline_adj += len(pos_hits) * 0.3
            headline_adj -= len(neg_hits) * 0.3
        headline_adj = round(max(-0.6, min(0.6, headline_adj)), 2)  # cap ±0.6%

        total = round(market_base + beta_adj + headline_adj, 2)

        stock["sentiment_score"] = total
        stock["sentiment_breakdown"] = {
            "vix_adj": vix_adj,
            "market_momentum_adj": momentum_adj,
            "beta_adj": beta_adj,
            "headline_adj": headline_adj,
            "total": total,
        }

    return stocks_data


async def _attach_current_prices_to_predictions(predictions: list[dict]) -> list[dict]:
    if not predictions:
        return []

    enriched = [dict(prediction) for prediction in predictions]
    tickers = sorted({prediction.get("ticker") for prediction in enriched if prediction.get("ticker")})
    if not tickers:
        return enriched

    infos = await asyncio.gather(
        *[get_info_with_timeout(ticker, SEARCH_INFO_TIMEOUT_SEC) for ticker in tickers],
        return_exceptions=True,
    )

    price_map: dict[str, float] = {}
    for ticker, info in zip(tickers, infos):
        if isinstance(info, Exception):
            continue
        price = _price_from_info_for_alerts(info)
        if price:
            price_map[ticker] = float(price)

    for prediction in enriched:
        ticker = prediction.get("ticker")
        live_price = price_map.get(ticker)
        prediction["current_price"] = live_price if live_price else prediction.get("price_at_prediction")

    return enriched


def _sync_prediction_history(predictions: list[dict]) -> int:
    try:
        import db as _db
        return _db.sync_prediction_history(
            predictions,
            model_version=PREDICTION_MODEL_VERSION,
            prompt_version=PREDICTION_PROMPT_VERSION,
        )
    except Exception as exc:
        logger.warning("Prediction history sync failed: %s", exc)
        return 0


def _parse_iso_date(value: str | None) -> date | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value)[:10]).date()
    except Exception:
        return None


def _close_on_or_before(price_points: list[tuple[date, float]], target: date) -> float | None:
    eligible = [price for day, price in price_points if day <= target]
    return eligible[-1] if eligible else None


def _close_on_or_after(price_points: list[tuple[date, float]], target: date) -> float | None:
    for day, price in price_points:
        if day >= target:
            return price
    return None


async def evaluate_prediction_outcomes(limit: int = 100) -> int:
    """Evaluate matured prediction horizons against realised closes."""
    try:
        import db as _db
        due = _db.list_due_prediction_outcomes(limit=limit)
    except Exception as exc:
        logger.warning("Prediction outcome query failed: %s", exc)
        return 0
    if not due:
        return 0

    by_ticker: dict[str, list[dict]] = {}
    for row in due:
        by_ticker.setdefault(str(row.get("ticker") or "").upper(), []).append(row)

    evaluated = 0
    for ticker, rows in by_ticker.items():
        parsed_dates = [
            d for row in rows
            for d in (_parse_iso_date(row.get("prediction_date")), _parse_iso_date(row.get("target_date")))
            if d is not None
        ]
        if not ticker or not parsed_dates:
            continue
        start = min(parsed_dates) - timedelta(days=7)
        end = max(parsed_dates) + timedelta(days=10)
        try:
            hist = await asyncio.to_thread(
                lambda t=ticker, s=start, e=end: yf.Ticker(t).history(start=s.isoformat(), end=e.isoformat())
            )
            if hist is None or hist.empty:
                continue
            price_points = [
                (idx.date(), float(close))
                for idx, close in zip(hist.index, hist["Close"])
                if close is not None and math.isfinite(float(close))
            ]
            price_points.sort(key=lambda item: item[0])
        except Exception as exc:
            logger.warning("Prediction outcome price fetch failed for %s: %s", ticker, exc)
            continue

        for row in rows:
            prediction_date = _parse_iso_date(row.get("prediction_date"))
            target_date = _parse_iso_date(row.get("target_date"))
            if not prediction_date or not target_date:
                continue
            start_price = row.get("price_at_prediction")
            try:
                start_price = float(start_price) if start_price is not None else None
            except (TypeError, ValueError):
                start_price = None
            if not start_price or not math.isfinite(start_price):
                start_price = _close_on_or_before(price_points, prediction_date)
            end_price = _close_on_or_after(price_points, target_date)
            if not start_price or not end_price:
                continue

            realised = ((end_price - start_price) / start_price) * 100.0
            forecast = row.get("forecast_return_pct")
            try:
                forecast_float = float(forecast) if forecast is not None else None
            except (TypeError, ValueError):
                forecast_float = None
            direction_match = None if forecast_float is None else (realised >= 0) == (forecast_float >= 0)
            forecast_error = None if forecast_float is None else realised - forecast_float
            try:
                _db.update_prediction_outcome(
                    row["outcome_id"],
                    realised_return_pct=realised,
                    direction_match=direction_match,
                    forecast_error=forecast_error,
                )
                evaluated += 1
            except Exception as exc:
                logger.warning("Prediction outcome update failed for %s %s: %s", ticker, row.get("horizon"), exc)
    return evaluated


@app.get("/api/predictions")
async def get_predictions(current_user: str = Depends(get_current_user)):
    today = str(date.today())
    mtime_ns = PREDICTIONS_FILE.stat().st_mtime_ns if PREDICTIONS_FILE.exists() else None
    if (
        _predictions_cache["data"] is not None
        and _predictions_cache["date"] == today
        and _predictions_cache["mtime_ns"] == mtime_ns
    ):
        return await _attach_current_prices_to_predictions(_predictions_cache["data"])

    predictions = load_predictions()
    cleaned_predictions = sanitize_jsonable(predictions)
    if cleaned_predictions != predictions:
        predictions = cleaned_predictions
        try:
            save_predictions(predictions)
        except Exception as e:
            logger.warning("Could not persist cleaned predictions during GET /api/predictions: %s", e)
    predictions, updated = update_actuals(predictions)
    # Backfill missing names from lookup table
    for p in predictions:
        if not p.get("name") or p.get("name") == p.get("ticker"):
            p["name"] = TICKER_NAMES.get(p["ticker"], p["ticker"])
            updated = True
        derived_direction = prediction_direction(p.get("predicted_pct"))
        derived_score = prediction_score(p.get("predicted_pct"), p.get("confidence", "medium"))
        derived_horizons = {
            **prediction_short_horizon_returns(
                p.get("predicted_pct"),
                p.get("direction"),
                p.get("score"),
                p.get("confidence", "medium"),
            ),
            **prediction_horizon_returns(
                p.get("predicted_pct"),
                p.get("direction"),
                p.get("score"),
                p.get("confidence", "medium"),
            ),
        }
        if p.get("predicted_pct") is not None:
            if p.get("direction") != derived_direction:
                p["direction"] = derived_direction
                updated = True
            if p.get("score") != derived_score:
                p["score"] = derived_score
                updated = True
        for key, value in derived_horizons.items():
            if p.get(key) != value:
                p[key] = value
                updated = True
    if updated:
        try:
            save_predictions(predictions)
        except Exception as e:
            logger.warning("Could not persist updated predictions during GET /api/predictions: %s", e)
    import db as _db
    db_user = _get_db_user(current_user)
    if db_user:
        watchlist_set = set(_db.get_user_watchlist(db_user["user_id"]))
    else:
        watchlist_set = set(load_watchlist())
    calibration = compute_calibration(predictions)

    def _prediction_sort_key(p: dict):
        predicted_pct = p.get("predicted_pct")
        confidence = (p.get("confidence") or "pending").lower()
        direction = p.get("direction") or prediction_direction(predicted_pct)
        score = p.get("score")
        if score is None:
            score = prediction_score(predicted_pct, confidence)
        accuracy_pct = calibration.get(p.get("ticker", ""), {}).get("accuracy_pct", 50.0)
        confidence_rank = {"high": 0, "medium": 1, "low": 2, "pending": 3}.get(confidence, 3)
        direction_rank = {"bullish": 0, "neutral": 1, "bearish": 2, "pending": 3}.get(direction, 3)
        return (
            p.get("date") != today,
            predicted_pct is None,
            direction_rank,
            -float(accuracy_pct),
            -(score if score is not None else 0),
            -(predicted_pct if predicted_pct is not None else -999),
            confidence_rank,
            p.get("ticker", ""),
        )

    sorted_preds = sorted(
        predictions,
        key=_prediction_sort_key,
        reverse=False,
    )

    # Always show watchlist stocks — add stub rows for any never analysed
    predicted_tickers = {p["ticker"] for p in predictions}
    for ticker in watchlist_set:
        if ticker not in predicted_tickers:
            sorted_preds.append({
                "date": "",
                "ticker": ticker,
                "name": TICKER_NAMES.get(ticker, ticker),
                "predicted_pct": None,
                "direction": "pending",
                "score": None,
                "confidence": "pending",
                "reasoning": "Not yet analysed. Click Generate Predictions to include this stock.",
                "actual_pct": None,
                "price_at_prediction": None,
                **prediction_short_horizon_returns(None),
                **prediction_horizon_returns(None),
            })
    response = sanitize_jsonable(sorted_preds)
    _sync_prediction_history(response)
    _predictions_cache["date"] = today
    _predictions_cache["mtime_ns"] = PREDICTIONS_FILE.stat().st_mtime_ns if PREDICTIONS_FILE.exists() else None
    _predictions_cache["data"] = response
    return await _attach_current_prices_to_predictions(response)


@app.get("/api/predictions/learning")
@limiter.limit("20/hour")
async def get_predictions_learning(request: Request, evaluate: bool = True):
    """Return durable prediction outcome learning metrics."""
    try:
        import db as _db
        predictions = load_predictions()
        synced = _sync_prediction_history(predictions)
        evaluated_now = await evaluate_prediction_outcomes(limit=100) if evaluate else 0
        summary = _db.get_prediction_learning_summary()
        calibration = _build_prediction_calibration_model(store=False)
        governance = _prediction_governance_status(calibration)
        summary.update({
            "synced_predictions": synced,
            "evaluated_now": evaluated_now,
            "model_version": PREDICTION_MODEL_VERSION,
            "prompt_version": PREDICTION_PROMPT_VERSION,
            "learning_enabled": PREDICTION_LEARNING_ENABLED,
            "calibration": calibration,
            "governance": governance,
        })
        return summary
    except Exception as exc:
        logger.exception("Prediction learning summary failed: %s", exc)
        raise HTTPException(status_code=500, detail="Prediction learning summary failed")


@app.get("/api/insider/{ticker}")
@limiter.limit("60/hour")
async def get_insider_activity(request: Request, ticker: str, days: int = 30, refresh: bool = False):
    """SEC Form 4 insider transactions for a ticker.

    Returns:
      summary: 30-day rollup (net $, purchase/sale counts, unique insiders,
               director/officer breakdown, cluster_buying flag).
      transactions: most recent transactions (signed shares + total_value).

    Pass ?refresh=true to pull fresh filings from SEC EDGAR before reading
    (rate-limited; first call for a ticker can take 10-20s as it walks the
    last N filings). Subsequent calls hit the SQLite cache instantly.
    """
    from insider_transactions import refresh_ticker, summarize_ticker, list_transactions
    ticker = _validate_ticker(ticker)
    days = max(1, min(365, int(days)))
    refresh_info = None
    if refresh:
        # Run sync (httpx is sync). Wrap in to_thread so we don't block the loop.
        refresh_info = await asyncio.to_thread(refresh_ticker, ticker)
    return {
        "summary":      summarize_ticker(ticker, days=days),
        "transactions": list_transactions(ticker, days=days, limit=200),
        "refresh":      refresh_info,
    }


@app.get("/api/predictions/calibration")
@limiter.limit("20/hour")
async def get_predictions_calibration(request: Request, store: bool = False):
    """Return the adaptive calibration model used by future prediction runs.

    Also includes confidence-bucket + score-bucket realised hit rates so the
    UI can show "X% of high-confidence calls hit" / "Y ECE across buckets".
    """
    model = _build_prediction_calibration_model(store=store)
    model.update({
        "model_version": PREDICTION_MODEL_VERSION,
        "prompt_version": PREDICTION_PROMPT_VERSION,
        "learning_enabled": PREDICTION_LEARNING_ENABLED,
        "governance": _prediction_governance_status(model),
        # New breakdowns — by confidence label and signal-score bucket.
        "buckets": compute_confidence_calibration(load_predictions()),
    })
    return model


@app.post("/api/predictions/calibration/rebuild")
@limiter.limit("6/hour")
async def rebuild_predictions_calibration(request: Request, current_user: str = Depends(get_current_user)):
    """Evaluate due outcomes, rebuild calibration, persist a model card, and return governance status."""
    try:
        import db as _db
        predictions = load_predictions()
        synced = _sync_prediction_history(predictions)
        evaluated_now = await evaluate_prediction_outcomes(limit=250)
        calibration = _build_prediction_calibration_model(store=True)
        governance = _prediction_governance_status(calibration)
        history = _db.list_prediction_calibrations(limit=5)
        logger.info(
            "PREDICTION_CALIBRATION_REBUILD user=%s synced=%s evaluated=%s status=%s samples=%s",
            current_user,
            synced,
            evaluated_now,
            governance["status"],
            calibration.get("sample_count"),
        )
        return {
            "status": "ok",
            "synced_predictions": synced,
            "evaluated_now": evaluated_now,
            "model_version": PREDICTION_MODEL_VERSION,
            "prompt_version": PREDICTION_PROMPT_VERSION,
            "calibration": calibration,
            "governance": governance,
            "history": history,
        }
    except Exception as exc:
        logger.exception("Prediction calibration rebuild failed: %s", exc)
        raise HTTPException(status_code=500, detail="Prediction calibration rebuild failed")


async def _generate_predictions_impl():
    def _clean_for_json(value):
        if isinstance(value, float):
            return value if math.isfinite(value) else None
        if isinstance(value, dict):
            return {k: _clean_for_json(v) for k, v in value.items()}
        if isinstance(value, list):
            return [_clean_for_json(v) for v in value]
        return value

    def _extract_json_array(raw_text: str):
        text = (raw_text or "").strip()
        if "```" in text:
            parts = text.split("```")
            for part in parts:
                part = part.strip()
                if part.startswith("json"):
                    part = part[4:].strip()
                if part.startswith("[") and part.endswith("]"):
                    text = part
                    break
        try:
            parsed = json.loads(text)
            return parsed if isinstance(parsed, list) else None
        except json.JSONDecodeError:
            start = text.find("[")
            end = text.rfind("]")
            if start != -1 and end != -1 and end > start:
                candidate = text[start:end + 1]
                try:
                    parsed = json.loads(candidate)
                    return parsed if isinstance(parsed, list) else None
                except json.JSONDecodeError:
                    return None
        return None

    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        raise HTTPException(status_code=500, detail="ANTHROPIC_API_KEY not set in .env")

    today = str(date.today())
    predictions = load_predictions()
    predictions, updated = update_actuals(predictions)

    already_predicted = {p["ticker"] for p in predictions if p["date"] == today}
    watchlist_tickers = load_watchlist()

    # Watchlist stocks always get a prediction; UNIVERSE fills remaining slots
    watchlist_missing = [t for t in watchlist_tickers if t not in already_predicted]
    universe_fill = [t for t in UNIVERSE[:PREDICTIONS_UNIVERSE_FILL_LIMIT] if t not in already_predicted and t not in watchlist_tickers]
    to_analyze = list(dict.fromkeys(watchlist_missing + universe_fill))[:max(len(watchlist_missing) + PREDICTIONS_UNIVERSE_FILL_LIMIT, 1)]

    if not to_analyze:
        if updated:
            save_predictions(predictions)
        _sync_prediction_history(predictions)
        return {
            "message": "Predictions already generated for today.",
            "predictions": [p for p in predictions if p["date"] == today],
        }

    prediction_run_id = str(uuid.uuid4())
    prediction_run_created = False
    try:
        import db as _db
        _db.create_prediction_run(
            prediction_run_id,
            to_analyze,
            model_version=PREDICTION_MODEL_VERSION,
            prompt_version=PREDICTION_PROMPT_VERSION,
            source="generate",
            meta={
                "include_stock_research": PREDICTIONS_INCLUDE_STOCK_RESEARCH,
                "universe_fill_limit": PREDICTIONS_UNIVERSE_FILL_LIMIT,
            },
        )
        prediction_run_created = True
    except Exception as exc:
        logger.warning("Could not create prediction run record: %s", exc)

    try:
        macro = await fetch_macro_data()
    except Exception as e:
        logger.warning("Prediction macro fetch failed: %s", e)
        macro = {}
    try:
        headlines = await fetch_rss_headlines(RSS_FEEDS)
    except Exception as e:
        logger.warning("Prediction headlines fetch failed: %s", e)
        headlines = []

    async def _fetch_pred_ticker(ticker: str) -> dict | None:
        for attempt in range(3):
            try:
                def _blocking():
                    t = yf.Ticker(ticker)
                    info = t.info
                    hist = t.history(period="60d")   # extended for RSI, SMA50, drawdown
                    news = t.news[:3] if hasattr(t, "news") else []
                    return info, hist, news
                info, hist, news = await asyncio.wait_for(
                    asyncio.to_thread(_blocking), timeout=20.0
                )
                if not isinstance(info, dict):
                    info = {}
                recent_chg = 0.0
                if hist is not None and len(hist) >= 2:
                    prev_close = float(hist["Close"].iloc[-2])
                    last_close = float(hist["Close"].iloc[-1])
                    if prev_close and math.isfinite(prev_close) and math.isfinite(last_close):
                        recent_chg = ((last_close - prev_close) / prev_close) * 100
                mc = info.get("marketCap")
                fcf_yield = calc_fcf_yield(info.get("freeCashflow"), mc)

                # Quant metrics
                factors  = compute_factor_scores(info, hist)
                vol      = compute_volatility(hist)
                drawdown = compute_max_drawdown(hist)
                dcf      = compute_dcf_valuation(info)

                payload = {
                    "ticker": ticker,
                    "name": info.get("shortName", TICKER_NAMES.get(ticker, ticker)),
                    "sector": info.get("sector", ""),
                    "price": info.get("currentPrice") or info.get("regularMarketPrice"),
                    # Valuation
                    "pe": info.get("trailingPE"),
                    "forward_pe": info.get("forwardPE"),
                    "peg": info.get("pegRatio"),
                    "pb": info.get("priceToBook"),
                    "ev_ebitda": info.get("enterpriseToEbitda"),
                    "fcf_yield_pct": fcf_yield,
                    # Growth
                    "eps_growth_yoy": info.get("earningsGrowth"),
                    "revenue_growth_yoy": info.get("revenueGrowth"),
                    # Quality
                    "profit_margin": info.get("profitMargins"),
                    "gross_margin": info.get("grossMargins"),
                    "operating_margin": info.get("operatingMargins"),
                    "roe": info.get("returnOnEquity"),
                    "current_ratio": info.get("currentRatio"),
                    "debt_to_equity": info.get("debtToEquity"),
                    # Risk / momentum
                    "beta": info.get("beta"),
                    "short_float": info.get("shortPercentOfFloat"),
                    "fifty_two_week_high": info.get("fiftyTwoWeekHigh"),
                    "fifty_two_week_low": info.get("fiftyTwoWeekLow"),
                    "recent_5d_change_pct": round(recent_chg, 2),
                    "recent_news": [n.get("title", "") for n in news if isinstance(n, dict) and n.get("title")],
                    # Quant factor scores
                    "factor_scores": factors,
                    "annualised_vol_pct": vol,
                    "max_drawdown_pct": drawdown,
                    "dcf": dcf,
                }
                return _clean_for_json(payload)
            except Exception as e:
                logger.warning("Prediction ticker fetch failed for %s (attempt %s): %s", ticker, attempt + 1, e)
        return {
            "ticker": ticker,
            "name": TICKER_NAMES.get(ticker, ticker),
            "sector": "",
            "price": None,
            "pe": None, "forward_pe": None, "peg": None, "pb": None, "ev_ebitda": None,
            "fcf_yield_pct": None, "eps_growth_yoy": None, "revenue_growth_yoy": None,
            "profit_margin": None, "gross_margin": None, "operating_margin": None,
            "roe": None, "current_ratio": None, "debt_to_equity": None,
            "beta": 1.0, "short_float": None,
            "fifty_two_week_high": None, "fifty_two_week_low": None,
            "recent_5d_change_pct": 0.0, "recent_news": [],
            "factor_scores": None, "annualised_vol_pct": None, "max_drawdown_pct": None, "dcf": None,
        }

    _pred_raw = await asyncio.gather(*[_fetch_pred_ticker(t) for t in to_analyze], return_exceptions=True)
    stocks_data = [r for r in _pred_raw if isinstance(r, dict)]

    calibration = compute_calibration(predictions)
    durable_calibration = _build_prediction_calibration_model(store=True)
    durable_learning_summary = _format_prediction_learning_for_prompt(durable_calibration)

    completed = [p for p in predictions if p.get("actual_pct") is not None]
    accuracy_summary = "\n=== HISTORICAL CALIBRATION — apply these corrections to your predictions ===\n"
    if calibration:
        for ticker, c in calibration.items():
            if abs(c["mean_bias"]) >= 0.1:
                bias_note = (f"you under-predict by {c['mean_bias']:+.2f}% on avg — add this to your prediction"
                             if c["mean_bias"] > 0
                             else f"you over-predict by {abs(c['mean_bias']):.2f}% on avg — subtract this")
            else:
                bias_note = "magnitude well-calibrated"
            inv_note = " *** SIGNAL INVERTED — flip your direction for this stock ***" if c["inverted"] else ""
            accuracy_summary += (
                f"  {ticker}: {c['count']} predictions, {c['accuracy_pct']}% directional accuracy, "
                f"mean bias {c['mean_bias']:+.3f}% ({bias_note}){inv_note}\n"
            )
    else:
        accuracy_summary += "  No completed predictions yet — no calibration data available.\n"

    if completed:
        correct_dir = sum(1 for p in completed if (p["predicted_pct"] > 0) == (p["actual_pct"] > 0))
        pct_acc = correct_dir / len(completed) * 100
        accuracy_summary += (
            f"\nOverall directional accuracy: {pct_acc:.0f}% over {len(completed)} resolved predictions.\n"
            "Most recent outcomes:\n"
        )
        for p in completed[-10:]:
            direction = "CORRECT" if (p["predicted_pct"] > 0) == (p["actual_pct"] > 0) else "WRONG"
            accuracy_summary += (
                f"  {p['ticker']} {p['date']}: predicted {p['predicted_pct']:+.2f}%, "
                f"actual {p['actual_pct']:+.2f}% [{direction}]\n"
            )

    stocks_data = calculate_sentiment_scores(stocks_data, macro)

    watchlist_set = set(watchlist_tickers)
    must_predict = [s["ticker"] for s in stocks_data if s["ticker"] in watchlist_set]
    also_consider = [s["ticker"] for s in stocks_data if s["ticker"] not in watchlist_set]

    # Optional deep research context. Disabled by default because it adds major latency and API cost.
    if PREDICTIONS_INCLUDE_STOCK_RESEARCH and watchlist_tickers:
        _summaries = await asyncio.gather(
            *[asyncio.to_thread(fetch_stock_research_for_ticker, t) for t in watchlist_tickers],
            return_exceptions=True
        )
        research_inputs = []
        for t, s in zip(watchlist_tickers, _summaries):
            if isinstance(s, Exception):
                logger.warning("Watchlist research fetch failed for %s: %s", t, s)
                s = f"Research unavailable for {t}."
            research_inputs.append(f"=== {t} Research ===\n{s}")
        watchlist_research_context = "\n\n".join(research_inputs)
    elif watchlist_tickers:
        watchlist_research_context = "Deep stock research skipped for faster prediction generation."
    else:
        watchlist_research_context = "No watchlist research available."

    # Build thesis context for watchlist tickers — injected as the primary signal anchor.
    _thesis_lines: list[str] = []
    if watchlist_tickers:
        try:
            import db as _db_pred
            for _t in watchlist_tickers:
                _th = _db_pred.get_latest_thesis(_t)
                if _th:
                    _age_h = round((datetime.now(timezone.utc) - _th.generated_at).total_seconds() / 3600, 1)
                    _f12 = _th.forecast.get("12m")
                    _drivers = "; ".join(_th.drivers[:2]) if _th.drivers else "none"
                    _risks   = "; ".join(_th.risks[:2])   if _th.risks   else "none"
                    _thesis_lines.append(
                        f"{_t}: score={_th.composite_score:.0f}/100, "
                        f"12m_base={_f12.base_return_pct:+.1f}% (bull={_f12.bull_return_pct:+.1f}%, bear={_f12.bear_return_pct:+.1f}%), "
                        f"risk={_th.risk_rating.value}, evidence={_th.evidence_quality.value}, "
                        f"age={_age_h}h | drivers: {_drivers} | risks: {_risks}"
                    )
        except Exception as _exc:
            logger.debug("Thesis context lookup failed: %s", _exc)
    thesis_context = "\n".join(_thesis_lines) if _thesis_lines else "No recent thesis available for watchlist tickers — rely on factor scores."

    prompt = f"""You are a quantitative analyst making 4-week directional predictions. Your goal is CALIBRATED ACCURACY — predict what the market will actually do, not what you wish it would do. Being wrong 60% of the time with high confidence is worse than being right 55% of the time with measured confidence.

Today: {today}

=== MULTI-AGENT THESIS CONTEXT (primary anchor — use this first) ===
Where present, each line shows the 21-agent thesis score (0-100), 12-month base/bull/bear return forecasts, risk rating, evidence quality, age, and key drivers/risks. This is your STRONGEST signal — it integrates fundamentals, valuation, growth, insider activity, options flow, credit risk, momentum, earnings quality, analyst consensus, Piotroski, Altman Z-Score, and more.
{thesis_context}

=== MACROECONOMIC CONDITIONS ===
{json.dumps(macro, indent=2)}

=== MARKET & FINANCIAL NEWS ===
{chr(10).join(headlines[:20]) if headlines else "No headlines fetched."}

=== WATCHLIST STOCK RESEARCH INPUTS ===
{watchlist_research_context}

=== STOCKS TO ANALYZE ===
Each stock includes a pre-calculated `sentiment_score` (%) (VIX + S&P momentum + beta + headline keywords) and `factor_scores` (0-100 each: value, momentum, quality, growth, composite).
Also per-stock when available: `dcf.margin_of_safety_pct`, `annualised_vol_pct`, `max_drawdown_pct`.

{json.dumps(stocks_data, indent=2)}
{accuracy_summary}
{durable_learning_summary}
=== REASONING PROCESS — follow for every stock ===
1. THESIS: Start from the multi-agent thesis score and 12m forecast (above). This is the primary signal.
2. FUNDAMENTALS: Cross-check with factor_scores. composite ≥ 70 supports bullish; ≤ 35 supports bearish.
3. MACRO/SENTIMENT: Does the VIX/momentum environment support or contradict the fundamental thesis? Sentiment is a secondary modifier, not the primary signal.
4. CALIBRATION: Apply historical bias corrections (accuracy_summary above).
5. DIRECTION + CONFIDENCE: Assign direction and confidence strictly per the rules below.

FACTOR GUIDANCE:
- composite ≥ 70: strong bullish signal; composite ≤ 35: strong bearish signal
- Strong VALUE + QUALITY but weak MOMENTUM → cap confidence at "medium" (good company, bad timing)
- DCF margin_of_safety_pct > 15% → bullish support; < -25% → cap confidence at "medium", note overvaluation
- annualised_vol_pct > 45% → cap confidence at "medium"; > 60% → cap at "low"
- max_drawdown_pct < -25% → note near-term pressure in reasoning

CONFIDENCE RULES — apply strictly, no exceptions:
- "high" (bullish): thesis_score ≥ 65 OR (composite ≥ 68 AND dcf_mos > 5% AND vol ≤ 45%). Multiple signals align. No contradictions.
- "high" (bearish): thesis_score ≤ 35 OR (composite ≤ 32 AND no major positive catalysts). No thesis = can't be high.
- "medium": Mixed thesis/factor signals, one risk factor present, or thesis is stale (> 48h).
- "low": Contradictory signals, high uncertainty, VIX > 25, vol > 60%, drawdown > 25%, or no data.
- AUTOMATIC DOWNGRADE: if reasoning contains any of — "uncertain", "mixed", "volatile", "could", "might", "risk", "concern", "drawdown", "expensive" — confidence cannot be "high".

IMPORTANT: You MUST return a prediction for EVERY stock in the watchlist: {must_predict}.
For any remaining stocks {also_consider}, only include the 2-3 with the strongest outlook.

Return ONLY a valid JSON array, no explanation, no markdown:
[
  {{
    "ticker": "AAPL",
    "direction": "bullish",
    "score": 74,
    "confidence": "high",
    "reasoning": "Thesis score 72/100 with +14% base case. Composite 71, DCF margin of safety +18%. Bull case: strong FCF and product cycle. Bear case: macro headwinds and premium valuation. Bull outweighs — fundamentals and thesis aligned."
  }}
]
Rules:
- `direction`: "bullish" | "neutral" | "bearish"
- `score`: integer 0–100 (0-39 bearish, 40-60 neutral, 61-100 bullish)
- `confidence`: "low" | "medium" | "high" — must follow the strict rules above
- `reasoning`: must briefly state (a) primary bullish signal, (b) primary bearish risk, (c) why one wins"""

    client = anthropic.Anthropic(api_key=api_key)
    claude_preds = None
    raw = ""
    ai_rate_limited = False
    for attempt in range(2):
        try:
            retry_prompt = prompt
            if attempt == 1:
                retry_prompt += "\n\nYour previous response was not valid JSON. Return ONLY a valid JSON array with no prose."
            started = datetime.now(timezone.utc)
            msg = client.messages.create(
                model=ANTHROPIC_MODEL, max_tokens=PREDICTIONS_MAX_TOKENS,
                messages=[{"role": "user", "content": retry_prompt}],
            )
            raw = msg.content[0].text.strip()
            elapsed_ms = int((datetime.now(timezone.utc) - started).total_seconds() * 1000)
            logger.info("PREDICTIONS model=%s stocks=%s duration_ms=%s research=%s", ANTHROPIC_MODEL, len(stocks_data), elapsed_ms, PREDICTIONS_INCLUDE_STOCK_RESEARCH)
            claude_preds = _extract_json_array(raw)
            if claude_preds is not None:
                break
            logger.warning("Predictions JSON parse retry needed (attempt %s). Raw first 500: %s", attempt + 1, raw[:500])
        except anthropic.AuthenticationError:
            raise HTTPException(status_code=500, detail="Anthropic API key is invalid or expired. Check ANTHROPIC_API_KEY in your .env file.")
        except anthropic.RateLimitError:
            logger.warning("Anthropic rate limit reached during predictions attempt %s", attempt + 1)
            ai_rate_limited = True
            break
        except anthropic.APIStatusError as e:
            logger.warning("Anthropic API status error during predictions attempt %s: %s", attempt + 1, e.message)
            if attempt == 1:
                raise HTTPException(status_code=500, detail=f"Anthropic API error: {e.message}")
        except Exception as e:
            logger.warning("Prediction generation attempt %s failed: %s", attempt + 1, e)
            if attempt == 1:
                raise HTTPException(status_code=500, detail=f"Prediction generation failed: {str(e)}")

    if claude_preds is None:
        if ai_rate_limited:
            logger.warning("Using fallback predictions because Anthropic rate limit was reached.")
        else:
            logger.error("Predictions JSON parse error after retries | raw (first 500): %s", raw[:500])
        claude_preds = []
        for stock in stocks_data:
            if stock["ticker"] in set(watchlist_tickers):
                claude_preds.append({
                    "ticker": stock["ticker"],
                    "predicted_pct": round(float(stock.get("sentiment_score") or 0.0), 2),
                    "direction": prediction_direction(round(float(stock.get("sentiment_score") or 0.0), 2)),
                    "score": prediction_score(round(float(stock.get("sentiment_score") or 0.0), 2), "low"),
                    "confidence": "low",
                    "reasoning": (
                        "Fallback prediction used because Anthropic was temporarily rate limited. "
                        "This uses the app's local sentiment and fundamentals baseline only. Review manually before acting."
                        if ai_rate_limited else
                        "Fallback prediction used because the AI response format was invalid. Review manually before acting."
                    ),
                })
        if not claude_preds:
            if ai_rate_limited:
                raise HTTPException(status_code=429, detail="Anthropic rate limit reached and no fallback predictions could be created.")
            raise HTTPException(status_code=500, detail="AI returned an unexpected response format. Please try again.")
    price_map = {s["ticker"]: s["price"] for s in stocks_data}
    name_map  = {s["ticker"]: s["name"]  for s in stocks_data}
    stock_map = {s["ticker"]: s for s in stocks_data}
    returned_tickers = {
        str(cp.get("ticker", "")).upper()
        for cp in claude_preds
        if isinstance(cp, dict) and cp.get("ticker")
    }
    missing_watchlist_predictions = [t for t in watchlist_missing if t not in returned_tickers]
    for ticker in missing_watchlist_predictions:
        stock = stock_map.get(ticker, {})
        fallback_pct = round(float(stock.get("sentiment_score") or 0.0), 2)
        claude_preds.append({
            "ticker": ticker,
            "predicted_pct": fallback_pct,
            "direction": prediction_direction(fallback_pct),
            "score": prediction_score(fallback_pct, "low"),
            "confidence": "low",
            "reasoning": "Auto-filled because the AI response did not include this watchlist stock. Review manually before acting.",
        })
    if missing_watchlist_predictions:
        logger.warning(
            "Predictions auto-filled missing watchlist tickers: %s",
            ", ".join(missing_watchlist_predictions),
        )
    new_preds = []
    seen_today = {p["ticker"] for p in predictions if p["date"] == today}
    for cp in claude_preds:
        ticker = cp["ticker"].upper()
        if ticker in seen_today:
            continue  # skip duplicates
        seen_today.add(ticker)

        cp_direction = (cp.get("direction") or "").lower() or None
        cp_score = cp.get("score")
        if cp_score is not None:
            try:
                cp_score = int(round(float(cp_score)))
            except (TypeError, ValueError):
                cp_score = None
        raw_pct = cp.get("predicted_pct")
        if raw_pct is None:
            raw_pct = legacy_predicted_pct(cp_direction, cp_score)
        try:
            raw_pct = round(float(raw_pct), 2)
        except (TypeError, ValueError):
            raw_pct = 0.0
        cal     = calibration.get(ticker, {})
        stock_data = stock_map.get(ticker, {})
        learning_adjustment = _learning_adjustment_for_stock(ticker, stock_data, durable_calibration)

        # 1. Bias correction — shift prediction by historical mean error.
        # JSON actuals give a short local loop; durable SQLite outcomes add versioned self-learning.
        legacy_bias = cal.get("mean_bias", 0.0)
        learning_bias = learning_adjustment.get("total_adjustment", 0.0)
        bias = round(float(legacy_bias or 0.0) + float(learning_bias or 0.0), 2)
        bias_applied = abs(bias) >= 0.1   # only correct if systematic (>=0.1%)
        corrected    = round(raw_pct + bias, 2) if bias_applied else raw_pct

        # 2. Signal inversion — flip direction if model has been consistently wrong
        inverted  = bool(cal.get("inverted", False) or learning_adjustment.get("invert_signal"))
        final_pct = round(-corrected, 2) if inverted else corrected

        # Append calibration notes to reasoning
        cal_note = ""
        if bias_applied:
            cal_note += f" [Bias corrected {bias:+.2f}%: raw={raw_pct:+.2f}%]"
        if inverted:
            hist_acc = cal.get("accuracy_pct") or learning_adjustment.get("ticker_hit_rate_pct") or learning_adjustment.get("global_hit_rate_pct")
            cal_note += f" [Direction INVERTED — historical accuracy {hist_acc}%, signal flipped]"
        if learning_adjustment.get("notes"):
            cal_note += " [Learning: " + "; ".join(learning_adjustment["notes"]) + "]"

        confidence = cp.get("confidence", "medium")
        if learning_adjustment.get("confidence_steps"):
            confidence = _downgrade_confidence(confidence, learning_adjustment["confidence_steps"])

        entry = {
            "date":               today,
            "ticker":             ticker,
            "name":               name_map.get(ticker, ""),
            "predicted_pct":      final_pct,
            "raw_predicted_pct":  raw_pct,
            "direction":          prediction_direction(final_pct),
            "score":              prediction_score(final_pct, confidence),
            "bias_correction":    round(bias, 3) if bias_applied else 0.0,
            "inverted":           inverted,
            "confidence":         confidence,
            "reasoning":          cp.get("reasoning", "") + cal_note,
            "actual_pct":         None,
            "price_at_prediction": price_map.get(ticker),
            "generated_at":       datetime.utcnow().isoformat(),
            "model_version":      PREDICTION_MODEL_VERSION,
            "prompt_version":     PREDICTION_PROMPT_VERSION,
            "learning_adjustment": learning_adjustment,
            # Quant data
            "factor_scores":      stock_data.get("factor_scores"),
            "dcf":                stock_data.get("dcf"),
            "annualised_vol_pct": stock_data.get("annualised_vol_pct"),
            "max_drawdown_pct":   stock_data.get("max_drawdown_pct"),
        }
        entry.update(
            prediction_short_horizon_returns(
                entry["predicted_pct"],
                entry["direction"],
                entry["score"],
                entry["confidence"],
            )
        )
        entry.update(
            prediction_horizon_returns(
                entry["predicted_pct"],
                entry["direction"],
                entry["score"],
                entry["confidence"],
            )
        )
        predictions.append(entry)
        new_preds.append(entry)

    save_predictions(predictions)
    stored_predictions = 0
    try:
        import db as _db
        for entry in new_preds:
            prediction_id = _db.store_prediction_snapshot(
                entry,
                run_id=prediction_run_id if prediction_run_created else None,
                model_version=PREDICTION_MODEL_VERSION,
                prompt_version=PREDICTION_PROMPT_VERSION,
                macro=macro,
            )
            entry["prediction_id"] = prediction_id
            stored_predictions += 1
        if stored_predictions:
            save_predictions(predictions)
        if prediction_run_created:
            _db.complete_prediction_run(prediction_run_id, "completed", stored_predictions)
    except Exception as exc:
        logger.warning("Could not store prediction snapshots: %s", exc)
        if prediction_run_created:
            try:
                import db as _db
                _db.complete_prediction_run(prediction_run_id, "failed", stored_predictions, str(exc))
            except Exception:
                pass

    # Schedule background backfill for any entries that came back with no factor_scores
    missing = [e["ticker"] for e in new_preds if not e.get("factor_scores")]
    if missing:
        asyncio.ensure_future(_backfill_factor_scores(missing, today))

    return {"predictions": new_preds}


async def _backfill_factor_scores(tickers: list[str], pred_date: str):
    """Sequentially retry factor score fetches for tickers that timed out during generation."""
    await asyncio.sleep(5)
    logger.info("Factor score backfill starting for %d tickers: %s", len(tickers), tickers)
    for ticker in tickers:
        try:
            def _blocking(t=ticker):
                yft = yf.Ticker(t)
                return yft.info, yft.history(period="60d")
            info, hist = await asyncio.wait_for(asyncio.to_thread(_blocking), timeout=25.0)
            if not isinstance(info, dict) or not info:
                continue
            factors  = compute_factor_scores(info, hist)
            vol      = compute_volatility(hist)
            drawdown = compute_max_drawdown(hist)
            dcf      = compute_dcf_valuation(info)
            preds = load_predictions()
            updated = False
            for p in preds:
                if p.get("ticker") == ticker and p.get("date") == pred_date and not p.get("factor_scores"):
                    p["factor_scores"]      = factors
                    p["annualised_vol_pct"] = vol
                    p["max_drawdown_pct"]   = drawdown
                    p["dcf"]                = dcf
                    updated = True
            if updated:
                save_predictions(preds)
                _sync_prediction_history(preds)
                logger.info("Factor score backfill complete for %s", ticker)
        except Exception as exc:
            logger.warning("Factor score backfill failed for %s: %s", ticker, exc)
        await asyncio.sleep(1.5)  # gentle pacing between tickers


_predictions_job: dict = {"running": False, "started_at": None, "finished_at": None, "error": None, "count": None}

async def _generate_predictions_bg():
    _predictions_job["running"] = True
    _predictions_job["started_at"] = datetime.now(timezone.utc).isoformat()
    _predictions_job["error"] = None
    _predictions_job["count"] = None
    try:
        result = await _generate_predictions_impl()
        _predictions_job["count"] = len(result.get("predictions", [])) if isinstance(result, dict) else None
    except Exception as exc:
        _predictions_job["error"] = str(exc)
        logger.exception("[Predictions] Background generate failed: %s", exc)
    finally:
        _predictions_job["running"] = False
        _predictions_job["finished_at"] = datetime.now(timezone.utc).isoformat()

@app.post("/api/predictions/generate")
@limiter.limit("8/hour")
async def generate_predictions(request: Request, background_tasks: BackgroundTasks):
    if _predictions_job["running"]:
        return {"status": "already_running", "started_at": _predictions_job["started_at"]}
    background_tasks.add_task(_generate_predictions_bg)
    return {"status": "started"}

@app.get("/api/predictions/generate/status")
async def generate_predictions_status():
    return _predictions_job


@app.post("/api/predictions/backfill-factors")
@limiter.limit("4/hour")
async def backfill_factors(request: Request, current_user: str = Depends(get_current_user)):
    """Trigger a background backfill of factor scores for today's predictions that are missing them."""
    preds = load_predictions()
    today = date.today().isoformat()
    missing = list({p["ticker"] for p in preds if p.get("date") == today and not p.get("factor_scores")})
    if not missing:
        return {"status": "ok", "message": "No missing factor scores for today.", "tickers": []}
    asyncio.ensure_future(_backfill_factor_scores(missing, today))
    return {"status": "ok", "message": f"Backfill started for {len(missing)} tickers.", "tickers": missing}


@app.get("/api/predictions/backtest")
async def backtest_predictions():
    """Replay 4 weeks of historical data through the sentiment scoring model and compare against actual returns."""
    watchlist = load_watchlist()
    end_date   = date.today()
    start_date = end_date - timedelta(weeks=6)  # 6 weeks buffer to get clean 4-week window

    # Fetch historical VIX, S&P 500, and all watchlist tickers in parallel
    async def _fetch_bt_index(symbol: str):
        try:
            hist = await asyncio.to_thread(
                lambda: yf.Ticker(symbol).history(start=str(start_date), end=str(end_date))
            )
            if hist is None or len(hist) == 0:
                return None
            return hist
        except Exception as e:
            logger.warning("Backtest index fetch failed for %s: %s", symbol, e)
            return None

    async def _fetch_bt_ticker(ticker: str):
        try:
            def _blocking():
                t = yf.Ticker(ticker)
                return t.info, t.history(start=str(start_date), end=str(end_date))
            info, hist = await asyncio.to_thread(_blocking)
            return ticker, info, hist
        except Exception as e:
            logger.warning("Backtest ticker fetch failed for %s: %s", ticker, e)
            return ticker, None, None

    _vix_fetch = _fetch_bt_index("^VIX")
    _sp_fetch  = _fetch_bt_index("^GSPC")
    _all = await asyncio.gather(_vix_fetch, _sp_fetch, *[_fetch_bt_ticker(t) for t in watchlist])
    vix_hist, sp_hist, *ticker_results = _all

    vix_dates  = [d.date() for d in vix_hist.index] if vix_hist is not None else []
    sp_dates   = [d.date() for d in sp_hist.index] if sp_hist is not None else []
    vix_closes = list(vix_hist["Close"]) if vix_hist is not None else []
    sp_closes  = list(sp_hist["Close"]) if sp_hist is not None else []
    vix_dict   = {d: v for d, v in zip(vix_dates, vix_closes)}
    sp_lookup  = {d: i for i, d in enumerate(sp_dates)}

    results = []

    for ticker, info, hist in ticker_results:
        try:
            if info is None or hist is None or len(hist) < 3:
                continue

            beta      = info.get("beta") or 1.0
            pe        = info.get("trailingPE")
            peg       = info.get("pegRatio")
            fcf_yield = calc_fcf_yield(info.get("freeCashflow"), info.get("marketCap"))
            profit_m  = info.get("profitMargins") or 0
            debt_eq   = info.get("debtToEquity") or 0
            rev_growth = info.get("revenueGrowth") or 0

            # Static fundamental adjustment (same logic as live scoring)
            fund_adj = 0.0
            if peg and peg < 1:    fund_adj += 0.4
            elif peg and peg > 2:  fund_adj -= 0.3
            if fcf_yield:
                if fcf_yield > 6:  fund_adj += 0.3
                elif fcf_yield < 2: fund_adj -= 0.2
            if pe:
                if pe < 15:        fund_adj += 0.2
                elif pe > 30:      fund_adj -= 0.3
            if profit_m > 0.20:    fund_adj += 0.1
            if debt_eq > 200:      fund_adj -= 0.1
            if rev_growth > 0.10:  fund_adj += 0.1
            fund_adj = round(max(-1.0, min(1.0, fund_adj)), 2)

            ticker_dates  = [d.date() for d in hist.index]
            ticker_closes = list(hist["Close"])

            for i in range(5, len(ticker_dates) - 1):
                trade_date = ticker_dates[i]

                # VIX on this day — O(1) dict lookup
                vix_val = vix_dict.get(trade_date, 20.0)
                if vix_val < 12:   vix_adj = 0.4
                elif vix_val < 15: vix_adj = 0.2
                elif vix_val < 20: vix_adj = 0.0
                elif vix_val < 25: vix_adj = -0.3
                elif vix_val < 30: vix_adj = -0.6
                else:              vix_adj = -1.0

                # S&P 500 5-day momentum on this day — O(1) dict lookup
                sp_5d_chg = 0.0
                if trade_date in sp_lookup:
                    si = sp_lookup[trade_date]
                    if si >= 5:
                        sp_5d_chg = ((sp_closes[si] - sp_closes[si - 5]) / sp_closes[si - 5]) * 100
                if sp_5d_chg > 3:    mom_adj = 0.4
                elif sp_5d_chg > 1:  mom_adj = 0.2
                elif sp_5d_chg > -1: mom_adj = 0.0
                elif sp_5d_chg > -3: mom_adj = -0.2
                else:                mom_adj = -0.4

                market_base   = vix_adj + mom_adj
                beta_adj      = round(market_base * (beta - 1.0) * 0.5, 2)
                sentiment     = round(market_base + beta_adj, 2)
                predicted     = round(sentiment + fund_adj, 2)

                actual = round(((ticker_closes[i + 1] - ticker_closes[i]) / ticker_closes[i]) * 100, 2)
                variance = round(actual - predicted, 2)
                correct  = (predicted > 0) == (actual > 0)

                results.append({
                    "date":            str(trade_date),
                    "ticker":          ticker,
                    "vix":             round(vix_val, 1),
                    "sp_5d_chg":       round(sp_5d_chg, 2),
                    "sentiment_score": sentiment,
                    "fund_adj":        fund_adj,
                    "predicted_pct":   predicted,
                    "actual_pct":      actual,
                    "variance":        variance,
                    "correct":         correct,
                })
        except Exception as e:
            print(f"[Backtest] {ticker} error: {e}")
            continue

    if not results:
        return {"results": [], "summary": {}, "by_ticker": {}, "factor_ic": {}}

    total   = len(results)
    correct = sum(1 for r in results if r["correct"])
    avg_abs_var = round(sum(abs(r["variance"]) for r in results) / total, 2)
    avg_pred    = round(sum(r["predicted_pct"] for r in results) / total, 2)
    avg_actual  = round(sum(r["actual_pct"] for r in results) / total, 2)

    by_ticker = {}
    for ticker in watchlist:
        rows = [r for r in results if r["ticker"] == ticker]
        if not rows:
            continue
        t_correct = sum(1 for r in rows if r["correct"])
        by_ticker[ticker] = {
            "total":        len(rows),
            "correct":      t_correct,
            "accuracy_pct": round(t_correct / len(rows) * 100, 1),
            "avg_variance": round(sum(r["variance"] for r in rows) / len(rows), 2),
            "avg_abs_variance": round(sum(abs(r["variance"]) for r in rows) / len(rows), 2),
        }

    # ── Condition-level accuracy (goal & reward model) ────────────────────────
    def _cond_acc(rows: list) -> dict | None:
        if not rows:
            return None
        c = sum(1 for r in rows if r["correct"])
        return {"total": len(rows), "correct": c, "accuracy_pct": round(c / len(rows) * 100, 1)}

    by_vix_regime = {
        "calm_lt15":      _cond_acc([r for r in results if r["vix"] < 15]),
        "moderate_15_20": _cond_acc([r for r in results if 15 <= r["vix"] < 20]),
        "elevated_20_25": _cond_acc([r for r in results if 20 <= r["vix"] < 25]),
        "fearful_gt25":   _cond_acc([r for r in results if r["vix"] >= 25]),
    }
    by_momentum_regime = {
        "bullish_gt1":     _cond_acc([r for r in results if r["sp_5d_chg"] > 1.0]),
        "neutral":         _cond_acc([r for r in results if -1.0 <= r["sp_5d_chg"] <= 1.0]),
        "bearish_lt_neg1": _cond_acc([r for r in results if r["sp_5d_chg"] < -1.0]),
    }
    by_direction = {
        "long_calls":  _cond_acc([r for r in results if r["predicted_pct"] > 0]),
        "short_calls": _cond_acc([r for r in results if r["predicted_pct"] <= 0]),
    }
    filtered_rows = [r for r in results if r["vix"] < 20 and r["sp_5d_chg"] > -1.0]
    filtered_accuracy = _cond_acc(filtered_rows)

    goal_target = load_settings().get("prediction_accuracy_goal", 55.0)
    overall_acc = round(correct / total * 100, 1) if total else 0.0

    recommendations: list[str] = []
    # Systematic directional bias
    if abs(avg_pred - avg_actual) >= 0.4 and (avg_pred > 0) != (avg_actual > 0):
        recommendations.append(
            f"SYSTEMATIC BIAS: model averages {avg_pred:+.2f}% predicted but market averaged "
            f"{avg_actual:+.2f}% — signals are pointing the wrong direction on average. "
            "Consider recalibrating the fundamental adjustment or adding a baseline correction."
        )
    # VIX gate recommendations
    for rk, rl in [("fearful_gt25", "VIX > 25 (fear)"), ("elevated_20_25", "VIX 20–25 (elevated)")]:
        c = by_vix_regime.get(rk)
        if c and c["total"] >= 5 and c["accuracy_pct"] < 48:
            recommendations.append(
                f"Gate signals during {rl} — accuracy is only {c['accuracy_pct']}% "
                f"({c['correct']}/{c['total']}). The regime gate already blocks BUYs above "
                "VIX 25; consider lowering the threshold to 20."
            )
    for rk, rl in [("calm_lt15", "VIX < 15 (calm)"), ("moderate_15_20", "VIX 15–20 (normal)")]:
        c = by_vix_regime.get(rk)
        if c and c["total"] >= 5 and c["accuracy_pct"] >= goal_target:
            recommendations.append(
                f"Prioritise signals during {rl} — accuracy is {c['accuracy_pct']}% "
                f"({c['correct']}/{c['total']}), above the {goal_target}% goal."
            )
    # Direction bias
    lc = by_direction.get("long_calls")
    sc = by_direction.get("short_calls")
    if lc and sc and lc["total"] >= 5 and sc["total"] >= 5:
        if lc["accuracy_pct"] > sc["accuracy_pct"] + 10:
            recommendations.append(
                f"Long-call accuracy ({lc['accuracy_pct']}%) significantly outperforms "
                f"short-call accuracy ({sc['accuracy_pct']}%). Model's sell signals are unreliable — "
                "consider suppressing SELL signals when accuracy gap persists."
            )
        elif sc["accuracy_pct"] > lc["accuracy_pct"] + 10:
            recommendations.append(
                f"Short-call accuracy ({sc['accuracy_pct']}%) outperforms long-call accuracy "
                f"({lc['accuracy_pct']}%). Model is better at spotting downside than upside."
            )
    # Filtered accuracy improvement
    if filtered_accuracy and filtered_accuracy["total"] >= 5:
        gain = round(filtered_accuracy["accuracy_pct"] - overall_acc, 1)
        if gain >= 2.0:
            recommendations.append(
                f"Filtering to VIX < 20 + S&P not bearish improves accuracy by {gain:+.1f}pp "
                f"to {filtered_accuracy['accuracy_pct']}% over {filtered_accuracy['total']} signals. "
                "Apply this as a secondary signal quality gate."
            )
    if not recommendations:
        recommendations.append(
            f"No dominant regime pattern detected yet ({total} signals). Run more predictions "
            "and re-backtest to build a richer condition profile."
        )

    # Factor IC: load latest predictions and compute Pearson correlation
    # between each factor score and actual next-day returns from backtest
    factor_ic: dict = {}
    try:
        preds_list = load_predictions()
        # Build a map: ticker -> factor_scores
        factor_map: dict = {}
        for p in preds_list:
            t = p.get("ticker")
            fs = p.get("factor_scores") or {}
            if t and fs:
                factor_map[t] = fs
        if factor_map:
            import numpy as _np
            for factor_name in ("value", "momentum", "quality", "growth", "composite"):
                xs, ys = [], []
                for r in results:
                    fs = factor_map.get(r["ticker"]) or {}
                    score = fs.get(factor_name)
                    if score is not None:
                        xs.append(float(score))
                        ys.append(float(r["actual_pct"]))
                if len(xs) >= 10:
                    xs_arr = _np.array(xs)
                    ys_arr = _np.array(ys)
                    # Pearson IC
                    std_x = _np.std(xs_arr)
                    std_y = _np.std(ys_arr)
                    if std_x > 0 and std_y > 0:
                        ic = float(_np.corrcoef(xs_arr, ys_arr)[0, 1])
                        factor_ic[factor_name] = {
                            "ic": round(ic, 4),
                            "n":  len(xs),
                            "signal": "strong" if abs(ic) > 0.1 else ("useful" if abs(ic) > 0.05 else "weak"),
                        }
    except Exception as e:
        logger.warning("FACTOR_IC_ERROR err=%s", e)

    return {
        "results": sorted(results, key=lambda x: x["date"], reverse=True),
        "summary": {
            "total":           total,
            "correct":         correct,
            "accuracy_pct":    round(correct / total * 100, 1),
            "avg_abs_variance": avg_abs_var,
            "avg_predicted":   avg_pred,
            "avg_actual":      avg_actual,
            "note":            "Headline sentiment not included in backtest (historical news unavailable). Scores use VIX + S&P momentum + beta + fundamentals only.",
        },
        "by_ticker": by_ticker,
        "factor_ic": factor_ic,
        "by_vix_regime":     by_vix_regime,
        "by_momentum_regime": by_momentum_regime,
        "by_direction":      by_direction,
        "filtered_accuracy": filtered_accuracy,
        "recommendations":   recommendations,
        "goal_target_pct":   goal_target,
    }


@app.get("/api/predictions/goal")
@limiter.limit("60/hour")
async def get_prediction_goal(request: Request, current_user: str = Depends(get_current_user)):
    """Return the accuracy goal and current LLM prediction accuracy from resolved predictions."""
    settings = load_settings()
    goal_target = settings.get("prediction_accuracy_goal", 55.0)
    predictions = load_predictions()
    completed = [
        p for p in predictions
        if p.get("actual_pct") is not None and p.get("predicted_pct") is not None
    ]
    if completed:
        c = sum(1 for p in completed if (p["predicted_pct"] > 0) == (p["actual_pct"] > 0))
        llm_accuracy_pct = round(c / len(completed) * 100, 1)
    else:
        llm_accuracy_pct = None
    gap = round((llm_accuracy_pct or 0) - goal_target, 1) if llm_accuracy_pct is not None else None
    if llm_accuracy_pct is None:
        status = "no_data"
    elif llm_accuracy_pct >= goal_target:
        status = "on_track"
    elif llm_accuracy_pct >= goal_target - 5:
        status = "approaching"
    else:
        status = "below_target"
    return {
        "goal_target_pct": goal_target,
        "llm_accuracy_pct": llm_accuracy_pct,
        "llm_sample_count": len(completed),
        "status": status,
        "gap_pct": gap,
    }


@app.post("/api/predictions/goal")
@limiter.limit("20/hour")
async def set_prediction_goal(request: Request, current_user: str = Depends(get_current_user)):
    """Update the accuracy goal target (40–80%)."""
    body = await request.json()
    try:
        target = float(body.get("target_pct", 55.0))
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail="target_pct must be a number")
    if not (40.0 <= target <= 80.0):
        raise HTTPException(status_code=400, detail="target_pct must be between 40 and 80")
    settings = load_settings()
    settings["prediction_accuracy_goal"] = target
    save_settings(settings)
    logger.info("PREDICTION_GOAL_SET user=%s target=%s", current_user, target)
    return {"ok": True, "goal_target_pct": target}


@app.get("/api/predictions/simulate")
@limiter.limit("20/hour")
async def simulate_predictions(request: Request):
    """
    Replay 4-week backtest through position sizing model to compute real P&L,
    then run 1000 Monte Carlo simulations projected to 12 months.
    """
    settings      = load_settings()
    initial_float = settings["initial_float"]
    target        = settings["target"]
    completed_predictions = [
        p for p in load_predictions()
        if p.get("actual_pct") is not None and p.get("predicted_pct") is not None
    ]

    def _build_simulation_response(raw_results):
        from collections import defaultdict

        def _finite(value, default=0.0):
            try:
                value = float(value)
            except (TypeError, ValueError):
                return default
            return value if math.isfinite(value) else default

        by_date = defaultdict(list)
        for result in raw_results:
            by_date[result["date"]].append(result)

        portfolio_value = initial_float
        equity_curve = [{"date": "start", "value": round(initial_float, 2)}]

        for day_date in sorted(by_date.keys()):
            day_signals = by_date[day_date]
            buy_signals = [r for r in day_signals if r["predicted_pct"] > 0.3]
            buy_signals.sort(key=lambda x: x["sentiment_score"], reverse=True)

            daily_pnl = 0.0
            total_alloc_pct = 0.0

            for sig in buy_signals:
                alloc_pct = 0.15 if sig["sentiment_score"] > 0.3 else 0.08
                if total_alloc_pct + alloc_pct > 0.80:
                    break
                pnl = portfolio_value * alloc_pct * (sig["actual_pct"] / 100)
                daily_pnl += pnl
                total_alloc_pct += alloc_pct

            portfolio_value = _finite(portfolio_value + daily_pnl, portfolio_value)
            equity_curve.append({"date": day_date, "value": round(portfolio_value, 2)})

        all_trades = [r for r in raw_results if r["predicted_pct"] > 0.3]
        wins = [r for r in all_trades if r["correct"]]
        losses = [r for r in all_trades if not r["correct"]]
        win_rate = len(wins) / len(all_trades) if all_trades else 0.5
        win_moves = [abs(_finite(r["actual_pct"], 0.0)) for r in wins if math.isfinite(_finite(r["actual_pct"], 0.0))]
        loss_moves = [abs(_finite(r["actual_pct"], 0.0)) for r in losses if math.isfinite(_finite(r["actual_pct"], 0.0))]
        avg_win_pct = statistics.mean(win_moves) if win_moves else 0.5
        avg_loss_pct = statistics.mean(loss_moves) if loss_moves else 0.5

        n_trading_days = len(by_date)
        avg_trades_per_day = len(all_trades) / n_trading_days if n_trading_days else 3
        avg_alloc_per_trade = 0.10

        N_SIMS = 1000
        N_DAYS = 252
        sim_finals = []
        sample_paths = []

        for sim_i in range(N_SIMS):
            port = initial_float
            path = [port]
            n_trades_day = max(1, round(avg_trades_per_day))
            for _ in range(N_DAYS):
                day_pnl = 0.0
                alloc_used = 0.0
                for _ in range(n_trades_day):
                    alloc = port * avg_alloc_per_trade
                    if alloc_used + avg_alloc_per_trade > 0.80:
                        break
                    if random.random() < win_rate:
                        day_pnl += alloc * (avg_win_pct / 100)
                    else:
                        day_pnl -= alloc * (avg_loss_pct / 100)
                    alloc_used += avg_alloc_per_trade
                port = _finite(port + day_pnl, port)
                port = max(port, 0)
                path.append(round(port, 2))
            sim_finals.append(port)
            if sim_i < 10:
                sample_paths.append(path)

        sim_finals.sort()
        p10 = _finite(sim_finals[int(0.10 * N_SIMS)], initial_float)
        p50 = _finite(sim_finals[int(0.50 * N_SIMS)], initial_float)
        p90 = _finite(sim_finals[int(0.90 * N_SIMS)], initial_float)
        prob_target = round(sum(1 for v in sim_finals if v >= target) / N_SIMS * 100, 1)

        hist_return_pct = round(_finite((portfolio_value - initial_float) / initial_float * 100), 2)
        weeks_observed = n_trading_days / 5
        ann_factor = 52 / weeks_observed if weeks_observed > 0 else 1
        base_ratio = _finite(portfolio_value / initial_float, 1.0)
        projected_12m = round(_finite(initial_float * (base_ratio ** ann_factor), initial_float), 2)

        return {
            "equity_curve": equity_curve,
            "monte_carlo": {
                "p10": round(p10, 2),
                "p50": round(p50, 2),
                "p90": round(p90, 2),
                "prob_target_pct": prob_target,
                "n_sims": N_SIMS,
                "n_days": N_DAYS,
                "sample_paths": sample_paths,
            },
            "stats": {
                "initial_float": round(initial_float, 2),
                "target": round(target, 2),
                "hist_final_value": round(portfolio_value, 2),
                "hist_return_pct": hist_return_pct,
                "hist_weeks": round(weeks_observed, 1),
                "projected_12m": projected_12m,
                "win_rate_pct": round(win_rate * 100, 1),
                "avg_win_pct": round(avg_win_pct, 2),
                "avg_loss_pct": round(avg_loss_pct, 2),
                "avg_trades_per_day": round(avg_trades_per_day, 1),
                "n_trades": len(all_trades),
            },
        }

    saved_raw_results = []
    for prediction in completed_predictions:
        try:
            predicted = float(prediction["predicted_pct"])
            actual = float(prediction["actual_pct"])
            saved_raw_results.append({
                "date": str(prediction.get("date") or date.today()),
                "ticker": prediction.get("ticker", "UNKNOWN"),
                "sentiment_score": predicted,
                "predicted_pct": predicted,
                "actual_pct": actual,
                "correct": (predicted > 0) == (actual > 0),
            })
        except (TypeError, ValueError, KeyError):
            continue

    if saved_raw_results:
        return _build_simulation_response(saved_raw_results)

    # ── Run backtest to get raw results ────────────────────────────────────
    watchlist  = load_watchlist()
    end_date   = date.today()
    start_date = end_date - timedelta(weeks=6)

    # Fetch VIX, S&P 500, and all watchlist tickers in parallel
    async def _fetch_index_history(symbol: str):
        try:
            hist = await asyncio.to_thread(
                lambda: yf.Ticker(symbol).history(start=str(start_date), end=str(end_date))
            )
            if hist is None or len(hist) == 0:
                return None
            return hist
        except Exception as e:
            logger.warning("Simulator index fetch failed for %s: %s", symbol, e)
            return None

    async def _fetch_sim_ticker(ticker: str):
        try:
            def _blocking():
                t = yf.Ticker(ticker)
                return t.info, t.history(start=str(start_date), end=str(end_date))
            info, hist = await asyncio.to_thread(_blocking)
            return ticker, info, hist
        except Exception as e:
            logger.warning("Simulator ticker fetch failed for %s: %s", ticker, e)
            return ticker, None, None

    _vix_fetch = _fetch_index_history("^VIX")
    _sp_fetch  = _fetch_index_history("^GSPC")
    _all = await asyncio.gather(_vix_fetch, _sp_fetch, *[_fetch_sim_ticker(t) for t in watchlist])
    vix_hist, sp_hist, *sim_ticker_results = _all

    vix_dates  = [d.date() for d in vix_hist.index] if vix_hist is not None else []
    sp_dates   = [d.date() for d in sp_hist.index] if sp_hist is not None else []
    vix_closes = list(vix_hist["Close"]) if vix_hist is not None else []
    sp_closes  = list(sp_hist["Close"]) if sp_hist is not None else []
    vix_dict   = {d: v for d, v in zip(vix_dates, vix_closes)}
    sp_lookup  = {d: i for i, d in enumerate(sp_dates)}

    raw_results = []
    for ticker, info, hist in sim_ticker_results:
        try:
            if info is None or hist is None or len(hist) < 3:
                continue
            beta       = info.get("beta") or 1.0
            pe         = info.get("trailingPE")
            peg        = info.get("pegRatio")
            fcf_yield  = calc_fcf_yield(info.get("freeCashflow"), info.get("marketCap"))
            profit_m   = info.get("profitMargins") or 0
            debt_eq    = info.get("debtToEquity") or 0
            rev_growth = info.get("revenueGrowth") or 0

            fund_adj = 0.0
            if peg and peg < 1:    fund_adj += 0.4
            elif peg and peg > 2:  fund_adj -= 0.3
            if fcf_yield:
                if fcf_yield > 6:  fund_adj += 0.3
                elif fcf_yield < 2: fund_adj -= 0.2
            if pe:
                if pe < 15:        fund_adj += 0.2
                elif pe > 30:      fund_adj -= 0.3
            if profit_m > 0.20:    fund_adj += 0.1
            if debt_eq > 200:      fund_adj -= 0.1
            if rev_growth > 0.10:  fund_adj += 0.1
            fund_adj = round(max(-1.0, min(1.0, fund_adj)), 2)

            ticker_dates  = [d.date() for d in hist.index]
            ticker_closes = list(hist["Close"])

            for i in range(5, len(ticker_dates) - 1):
                trade_date = ticker_dates[i]
                vix_val = vix_dict.get(trade_date, 20.0)
                if vix_val < 12:   vix_adj = 0.4
                elif vix_val < 15: vix_adj = 0.2
                elif vix_val < 20: vix_adj = 0.0
                elif vix_val < 25: vix_adj = -0.3
                elif vix_val < 30: vix_adj = -0.6
                else:              vix_adj = -1.0

                sp_5d_chg = 0.0
                if trade_date in sp_lookup:
                    si = sp_lookup[trade_date]
                    if si >= 5:
                        sp_5d_chg = ((sp_closes[si] - sp_closes[si - 5]) / sp_closes[si - 5]) * 100
                if sp_5d_chg > 3:    mom_adj = 0.4
                elif sp_5d_chg > 1:  mom_adj = 0.2
                elif sp_5d_chg > -1: mom_adj = 0.0
                elif sp_5d_chg > -3: mom_adj = -0.2
                else:                mom_adj = -0.4

                market_base = vix_adj + mom_adj
                beta_adj    = round(market_base * (beta - 1.0) * 0.5, 2)
                sentiment   = round(market_base + beta_adj, 2)
                predicted   = round(sentiment + fund_adj, 2)
                actual      = round(((ticker_closes[i + 1] - ticker_closes[i]) / ticker_closes[i]) * 100, 2)

                raw_results.append({
                    "date":            str(trade_date),
                    "ticker":          ticker,
                    "sentiment_score": sentiment,
                    "predicted_pct":   predicted,
                    "actual_pct":      actual,
                    "correct":         (predicted > 0) == (actual > 0),
                })
        except Exception as e:
            print(f"[Simulate] {ticker} error: {e}")
            continue

    if not raw_results:
        return {"error": "No backtest data available. Ensure watchlist stocks have sufficient price history."}

    # ── Historical portfolio simulation ────────────────────────────────────
    from collections import defaultdict
    by_date = defaultdict(list)
    for r in raw_results:
        by_date[r["date"]].append(r)

    portfolio_value = initial_float
    equity_curve    = [{"date": "start", "value": round(initial_float, 2)}]
    daily_returns   = []

    for day_date in sorted(by_date.keys()):
        day_signals   = by_date[day_date]
        buy_signals   = [r for r in day_signals if r["predicted_pct"] > 0.3]
        buy_signals.sort(key=lambda x: x["sentiment_score"], reverse=True)

        daily_pnl        = 0.0
        total_alloc_pct  = 0.0

        for sig in buy_signals:
            alloc_pct = 0.15 if sig["sentiment_score"] > 0.3 else 0.08
            if total_alloc_pct + alloc_pct > 0.80:
                break
            pnl = portfolio_value * alloc_pct * (sig["actual_pct"] / 100)
            daily_pnl       += pnl
            total_alloc_pct += alloc_pct

        portfolio_value += daily_pnl
        daily_returns.append(daily_pnl / (portfolio_value - daily_pnl) if (portfolio_value - daily_pnl) else 0)
        equity_curve.append({"date": day_date, "value": round(portfolio_value, 2)})

    # ── Observed stats for Monte Carlo ─────────────────────────────────────
    all_trades   = [r for r in raw_results if r["predicted_pct"] > 0.3]
    wins         = [r for r in all_trades if r["correct"]]
    losses       = [r for r in all_trades if not r["correct"]]
    win_rate     = len(wins) / len(all_trades) if all_trades else 0.5
    avg_win_pct  = statistics.mean([abs(r["actual_pct"]) for r in wins])   if wins   else 0.5
    avg_loss_pct = statistics.mean([abs(r["actual_pct"]) for r in losses]) if losses else 0.5

    n_trading_days    = len(by_date)
    avg_trades_per_day = len(all_trades) / n_trading_days if n_trading_days else 3
    avg_alloc_per_trade = 0.10  # rough average allocation per trade

    # ── Monte Carlo: 1000 simulations × 252 trading days ───────────────────
    N_SIMS     = 1000
    N_DAYS     = 252
    sim_finals = []
    # Store 10 sample paths for the chart
    sample_paths = []

    for sim_i in range(N_SIMS):
        port = initial_float
        path = [port]
        n_trades_day = max(1, round(avg_trades_per_day))
        for _ in range(N_DAYS):
            day_pnl      = 0.0
            alloc_used   = 0.0
            for _ in range(n_trades_day):
                alloc = port * avg_alloc_per_trade
                if alloc_used + avg_alloc_per_trade > 0.80:
                    break
                if random.random() < win_rate:
                    day_pnl += alloc * (avg_win_pct / 100)
                else:
                    day_pnl -= alloc * (avg_loss_pct / 100)
                alloc_used += avg_alloc_per_trade
            port += day_pnl
            port  = max(port, 0)
            path.append(round(port, 2))
        sim_finals.append(port)
        if sim_i < 10:
            sample_paths.append(path)

    sim_finals.sort()
    p10 = sim_finals[int(0.10 * N_SIMS)]
    p50 = sim_finals[int(0.50 * N_SIMS)]
    p90 = sim_finals[int(0.90 * N_SIMS)]
    prob_target = round(sum(1 for v in sim_finals if v >= target) / N_SIMS * 100, 1)

    hist_return_pct = round((portfolio_value - initial_float) / initial_float * 100, 2)
    weeks_observed  = n_trading_days / 5
    ann_factor      = 52 / weeks_observed if weeks_observed > 0 else 1
    projected_12m   = round(initial_float * ((portfolio_value / initial_float) ** ann_factor), 2)

    return {
        "equity_curve":   equity_curve,
        "monte_carlo": {
            "p10":         round(p10, 2),
            "p50":         round(p50, 2),
            "p90":         round(p90, 2),
            "prob_target_pct": prob_target,
            "n_sims":      N_SIMS,
            "n_days":      N_DAYS,
            "sample_paths": sample_paths,
        },
        "stats": {
            "initial_float":      initial_float,
            "target":             target,
            "hist_final_value":   round(portfolio_value, 2),
            "hist_return_pct":    hist_return_pct,
            "hist_weeks":         round(weeks_observed, 1),
            "projected_12m":      projected_12m,
            "win_rate_pct":       round(win_rate * 100, 1),
            "avg_win_pct":        round(avg_win_pct, 2),
            "avg_loss_pct":       round(avg_loss_pct, 2),
            "avg_trades_per_day": round(avg_trades_per_day, 1),
            "total_trades":       len(all_trades),
        },
    }


@app.delete("/api/predictions")
def clear_predictions(current_user: str = Depends(get_current_user)):
    save_predictions([])
    logger.info("PREDICTIONS_CLEARED user=%s", current_user)
    return {"message": "All predictions cleared."}


# ── Alerts endpoints ──────────────────────────────────────────────────────────

@app.get("/api/alerts")
def get_alerts(current_user: str = Depends(get_current_user)):
    import db as _db
    db_user = _get_db_user(current_user)
    if db_user:
        return _db.get_user_alerts(db_user["user_id"])
    return load_alerts()


@app.get("/api/alerts/status")
def get_monitor_status(current_user: str = Depends(get_current_user)):
    import db as _db
    db_user = _get_db_user(current_user)
    if db_user:
        watchlist = _db.get_user_watchlist(db_user["user_id"])
        owned = [t for t, pos in compute_positions(_db.get_user_transactions(db_user["user_id"], "real")).items() if pos.get("shares", 0) > 0]
    else:
        watchlist = load_watchlist()
        owned = [ticker for ticker, pos in compute_positions(load_portfolio()).items() if pos.get("shares", 0) > 0]
    email_configured = bool(os.getenv("SMTP_USER") and os.getenv("SMTP_PASS") and os.getenv("ALERT_EMAIL"))
    sms_configured   = bool(os.getenv("TWILIO_ACCOUNT_SID") and os.getenv("TWILIO_AUTH_TOKEN"))
    return {
        "active": monitor_status["active"],
        "last_check": monitor_status["last_check"],
        "checks_run": monitor_status["checks_run"],
        "watching": len(set(watchlist + owned)),
        "tickers": sorted(set(watchlist + owned)),
        "strategy": {
            "focus": "Strong BUY setups first, then owned-stock SELL signals",
            "buy_min_score": int(os.getenv("ALERT_BUY_MIN_SCORE", "72")),
            "sell_model_score_max": int(os.getenv("ALERT_SELL_MAX_SCORE", "42")),
            "snapshot_minutes": int(os.getenv("ALERT_RECOMMENDATION_SNAPSHOT_MINUTES", "30")),
        },
        "notifications": {
            "email": email_configured,
            "sms": sms_configured,
        },
    }


@app.post("/api/alerts/test")
def test_alert():
    emailed = send_email(
        "StockLens — Test Alert",
        "This is a test alert from your StockLens app.\nNotifications are working correctly."
    )
    texted = send_sms("StockPicker test alert — notifications working!")
    return {"email_sent": emailed, "sms_sent": texted}


@app.post("/api/alerts/test-preview")
def test_alert_preview(current_user: str = Depends(get_current_user)):
    """Send a formatted recommendation alert email using current alert history as sample data."""
    import db as _db
    db_user = _get_db_user(current_user)
    if db_user:
        alerts = _db.get_user_alerts(db_user["user_id"])
        to_email = db_user.get("email") or os.getenv("ALERT_EMAIL", "")
    else:
        alerts = load_alerts()
        to_email = os.getenv("ALERT_EMAIL", "")
    if not alerts:
        return {"email_sent": False, "sms_sent": False, "error": "No alert data to preview"}

    ET = zoneinfo.ZoneInfo("America/New_York")
    time_str = datetime.now(timezone.utc).astimezone(ET).strftime("%d %b %Y, %H:%M ET")
    buy_alerts  = [a for a in alerts if a.get("action") == "BUY"]
    sell_alerts = [a for a in alerts if a.get("action") == "SELL"]
    subject, body, sms_body = _build_alert_email(buy_alerts, sell_alerts, time_str, preview=True)
    emailed = send_email(subject, body, to_email=to_email)
    texted  = send_sms(sms_body[:1600])
    return {"email_sent": emailed, "sms_sent": texted}


# ── Recommendations endpoint ──────────────────────────────────────────────────

async def _build_recommendations(progress: Optional[callable] = None):
    def _tick(stage: str, message: str, completed: Optional[int] = None, total: Optional[int] = None):
        if progress:
            progress(stage, message, completed, total)

    def _price_from_info(info: Optional[dict]) -> float:
        if not isinstance(info, dict):
            return 0.0
        for key in (
            "currentPrice",
            "regularMarketPrice",
            "regularMarketPreviousClose",
            "previousClose",
            "fiftyDayAverage",
            "twoHundredDayAverage",
            "ask",
            "bid",
        ):
            value = info.get(key)
            try:
                value = float(value)
            except (TypeError, ValueError):
                value = 0.0
            if value > 0:
                return value
        return 0.0
    stage_started = datetime.now(timezone.utc)
    _tick("load_predictions", "Loading predictions and calibration…", 1, 1)
    settings       = load_settings()
    initial_float  = settings["initial_float"]
    target         = settings["target"]
    target_months  = settings.get("target_months", 12)

    predictions    = load_predictions()
    calibration    = compute_calibration(predictions)
    _record_recommendation_stage_duration("load_predictions", int((datetime.now(timezone.utc) - stage_started).total_seconds() * 1000))

    # Use today's predictions; fall back to most recent date
    dated = [p for p in predictions if p.get("score") is not None or p.get("predicted_pct") is not None]
    if not dated:
        return {"buys": [], "sells": [], "summary": {}, "explanation": "No predictions with score/return data found yet."}
    latest_date  = max(p["date"] for p in dated)
    latest_preds = [normalize_prediction(p) for p in dated if p["date"] == latest_date]

    # Compute paper portfolio positions (used for buy filtering + sell signals)
    stage_started = datetime.now(timezone.utc)
    _tick("load_portfolio", "Loading paper portfolio and cash position…", 1, 1)
    paper_txs        = load_paper_portfolio()
    paper_positions  = compute_positions(paper_txs)
    paper_held       = {t for t, p in paper_positions.items() if p["shares"] > 0}

    # Paper cash remaining
    paper_cash = PAPER_INITIAL_FLOAT
    for tx in paper_txs:
        qty   = float(tx.get("qty", 0))
        price = float(tx.get("price", 0))
        if tx["type"] == "buy":
            paper_cash -= qty * price
        elif tx["type"] == "sell":
            paper_cash += qty * price
    paper_cash = max(0.0, paper_cash)

    # First buy date per paper-held ticker — for capital inefficiency check
    first_buy_date: dict[str, str] = {}
    last_buy_timestamp: dict[str, str] = {}
    last_sell_timestamp: dict[str, str] = {}
    for tx in paper_txs:
        t     = tx["ticker"]
        tx_ts = tx.get("timestamp") or ""
        if tx.get("type") == "buy":
            tx_date = tx_ts[:10]
            if tx_date and (t not in first_buy_date or tx_date < first_buy_date[t]):
                first_buy_date[t] = tx_date
            if tx_ts and (t not in last_buy_timestamp or tx_ts > last_buy_timestamp[t]):
                last_buy_timestamp[t] = tx_ts
        elif tx.get("type") == "sell":
            if tx_ts and (t not in last_sell_timestamp or tx_ts > last_sell_timestamp[t]):
                last_sell_timestamp[t] = tx_ts
    _record_recommendation_stage_duration("load_portfolio", int((datetime.now(timezone.utc) - stage_started).total_seconds() * 1000))

    all_tickers = list(set(list(paper_held) + [p["ticker"] for p in latest_preds]))
    stage_started = datetime.now(timezone.utc)
    total_market = max(1, len(all_tickers))
    _tick("fetch_market_data", "Fetching live prices and market data…", 0, total_market)
    infos = [None] * len(all_tickers)
    async def _market_task(idx: int, ticker: str):
        try:
            return idx, await get_info_with_timeout(ticker, RECOMMENDATION_INFO_TIMEOUT_SEC)
        except Exception as e:
            return idx, e
    market_tasks = [asyncio.create_task(_market_task(idx, ticker)) for idx, ticker in enumerate(all_tickers)]
    completed_market = 0
    for task in asyncio.as_completed(market_tasks):
        idx, result = await task
        infos[idx] = result
        completed_market += 1
        _tick("fetch_market_data", f"Fetching live prices and market data… ({completed_market}/{total_market})", completed_market, total_market)
    price_map = {}
    info_map  = {}
    for ticker, info in zip(all_tickers, infos):
        if isinstance(info, Exception):
            logger.warning("[recommendations] Price fetch failed for %s: %s", ticker, info)
        else:
            price_map[ticker] = _price_from_info(info)
            info_map[ticker]  = info
    for ticker in paper_held:
        if not price_map.get(ticker):
            avg_cost = paper_positions.get(ticker, {}).get("avg_cost", 0) or 0
            pred_price = next((p.get("price_at_prediction") for p in latest_preds if p.get("ticker") == ticker and p.get("price_at_prediction")), 0) or 0
            fallback_price = pred_price or avg_cost
            if fallback_price:
                price_map[ticker] = fallback_price
    _record_recommendation_stage_duration("fetch_market_data", int((datetime.now(timezone.utc) - stage_started).total_seconds() * 1000))

    # 30-day price history for paper-held tickers — needed for 20-day SMA
    stage_started = datetime.now(timezone.utc)
    total_hist = max(1, len(paper_held))
    _tick("fetch_histories", "Checking trend history for held positions…", 0, total_hist)
    async def _fetch_hist_30d(t: str):
        try:
            h = await asyncio.wait_for(
                asyncio.to_thread(lambda: yf.Ticker(t).history(period="30d")),
                timeout=RECOMMENDATION_HISTORY_TIMEOUT_SEC,
            )
            return t, h
        except Exception:
            return t, None

    hist_results = []
    async def _history_task(idx: int, ticker: str):
        return idx, await _fetch_hist_30d(ticker)
    hist_tasks = [asyncio.create_task(_history_task(idx, t)) for idx, t in enumerate(paper_held)]
    hist_results_ordered = [None] * len(paper_held)
    completed_hist = 0
    for task in asyncio.as_completed(hist_tasks):
        idx, result = await task
        hist_results_ordered[idx] = result
        completed_hist += 1
        _tick("fetch_histories", f"Checking trend history for held positions… ({completed_hist}/{total_hist})", completed_hist, total_hist)
    hist_results = [r for r in hist_results_ordered if r is not None]
    hist_map: dict = {}
    for t, h in hist_results:
        if h is not None and not h.empty:
            hist_map[t] = h
    _record_recommendation_stage_duration("fetch_histories", int((datetime.now(timezone.utc) - stage_started).total_seconds() * 1000))

    portfolio_summary = compute_portfolio_state(paper_txs, price_map).get("summary", {})
    paper_total_value = float(portfolio_summary.get("total_portfolio_value") or 0.0)
    progress_pct = round(paper_total_value / target * 100, 1)

    # ── BUY recommendations ─────────────────────────────────────────────────
    stage_started = datetime.now(timezone.utc)
    build_total = max(1, len(latest_preds) + len(paper_held))
    build_done = 0
    _tick("build_recommendations", "Ranking buy and sell opportunities…", build_done, build_total)
    buys = []
    remaining_cash = paper_cash

    # Phase 2: regime gate. SPY below 200-DMA or VIX > 25 → no new BUYs.
    # SELL recommendations still produced — risk-off is a reason to trim, not freeze.
    regime = await get_market_regime()
    allow_buys = regime.get("ok", True)
    if not allow_buys:
        logger.info("[recommendations] BUYs suppressed by regime gate: %s", regime.get("reason"))

    # Phase 5: portfolio trajectory baseline (current holdings, no new buys).
    # Used for the "current trajectory" banner and as the floor against which
    # each candidate buy's P(hit) improvement is shown.
    current_weights: list[tuple[float, float, float]] = []
    portfolio_value_for_traj = max(paper_total_value, PAPER_INITIAL_FLOAT)
    for t in paper_held:
        pos = paper_positions.get(t, {})
        px  = float(price_map.get(t) or pos.get("avg_cost") or 0)
        val = float(pos.get("shares", 0)) * px
        if val <= 0:
            continue
        held_pred = next((p for p in latest_preds if p["ticker"] == t), None)
        h_score = (held_pred or {}).get("score") or 50
        h_conf  = (held_pred or {}).get("confidence") or "medium"
        h_vol   = float((held_pred or {}).get("annualised_vol_pct") or 30.0)
        current_weights.append((
            val / portfolio_value_for_traj,
            _expected_return_from_score(h_score, h_conf),
            h_vol,
        ))
    p_hit_current = _p_hit_target(
        portfolio_value_for_traj, target, target_months, current_weights
    )

    # Phase 5: cross-engine consistency. Use the cached alert snapshot to
    # decide whether the multi-agent thesis agrees with the Signals-engine
    # recommendation. Avoids paying per-Signals-call Claude cost.
    alert_data       = recommendation_alert_snapshot.get("data") or {}
    alert_generated  = recommendation_alert_snapshot.get("generated_at")
    alert_buy_tickers  = {b.get("ticker") for b in (alert_data.get("buys")  or [])}
    alert_sell_tickers = {s.get("ticker") for s in (alert_data.get("sells") or [])}
    snapshot_age_hr = None
    if isinstance(alert_generated, datetime):
        snapshot_age_hr = (datetime.now(timezone.utc) - alert_generated).total_seconds() / 3600

    def _consistency_for(ticker: str) -> dict:
        # No snapshot, or it's stale (>24h) — can't make a claim
        if not alert_data or snapshot_age_hr is None or snapshot_age_hr > 24:
            return {"badge": "stale", "label": "Cross-check stale"}
        if ticker in alert_sell_tickers:
            return {"badge": "contradiction", "label": "Agents say SELL"}
        if ticker in alert_buy_tickers:
            return {"badge": "agree", "label": "Agents agree BUY"}
        return {"badge": "no_thesis", "label": "No agent thesis"}

    # ── Phase 3: portfolio-relative sizing context ──────────────────────────
    # Computed once before the loop, then mutated as buys are approved so each
    # candidate sees the running sector/VaR state, not just the starting state.
    N_target          = max(4, int(settings.get("target_positions_count", 10)))
    position_max_pct  = float(settings.get("position_max_pct", 0.12))
    sector_max_pct    = float(settings.get("sector_max_pct", 0.30))
    portfolio_var_max = float(settings.get("portfolio_var_max_pct", 0.08))
    # Use max(paper_total_value, PAPER_INITIAL_FLOAT) so an empty portfolio
    # still gets meaningfully-sized buys (not starved by tiny initial value).
    base_portfolio_value = max(paper_total_value, PAPER_INITIAL_FLOAT)

    # 95% one-tailed normal; convert annual vol → monthly via /sqrt(12)
    VAR_Z_95          = 1.645
    MONTH_VOL_FACTOR  = 1.0 / math.sqrt(12.0)

    # Sector exposure of currently-held paper positions (running tally)
    sector_exposure: dict[str, float] = {}
    # Pairwise-correlated portfolio VaR. Use the correlation matrix from the
    # risk matrix cache (populated when the Risk tab is viewed, TTL 30 min).
    # Falls back to uncorrelated (rho=0 between positions) when cache is cold —
    # that's identical to the prior uncorrelated behaviour.
    corr_matrix: dict[str, dict[str, float]] = (
        (_risk_cache.get("data") or {}).get("correlation") or {}
    )
    held_var_terms: dict[str, float] = {}  # ticker -> individual dollar VaR
    existing_cov_sq = 0.0  # portfolio_var², updated incrementally
    for t in paper_held:
        pos = paper_positions.get(t, {})
        shares = float(pos.get("shares", 0))
        px = float(price_map.get(t) or pos.get("avg_cost") or 0)
        value = shares * px
        if value <= 0:
            continue
        sec = ((info_map.get(t) or {}).get("sector")) or "Unknown"
        sector_exposure[sec] = sector_exposure.get(sec, 0.0) + value
        held_pred = next((p for p in latest_preds if p["ticker"] == t), None)
        held_vol = float((held_pred or {}).get("annualised_vol_pct") or 30.0)
        ind_var = VAR_Z_95 * (held_vol / 100.0) * MONTH_VOL_FACTOR * value
        # cov_sq += 2 * Σⱼ∈held ρ_{t,j} * var_t * var_j + var_t²
        cross = sum(
            corr_matrix.get(t, {}).get(t2, 0.0) * ind_var * v2
            for t2, v2 in held_var_terms.items()
        )
        existing_cov_sq += 2.0 * cross + ind_var ** 2
        held_var_terms[t] = ind_var

    var_budget = portfolio_var_max * base_portfolio_value

    for pred in latest_preds:
        if not allow_buys:
            break
        ticker      = pred["ticker"]
        predicted   = pred.get("predicted_pct") or 0
        direction   = pred.get("direction") or prediction_direction(predicted)
        signal_score = pred.get("score")
        if signal_score is None:
            signal_score = prediction_score(predicted, pred.get("confidence", "medium")) or 50
        confidence  = pred.get("confidence", "medium")
        cal         = calibration.get(ticker, {})
        accuracy    = cal.get("accuracy_pct", 50) / 100

        # Skip if already held in paper portfolio
        if ticker in paper_held:
            continue

        # Phase 1: 21-day cooldown after a sell to prevent buy/sell churn.
        # Fail closed — any timestamp parse error keeps the cooldown active.
        last_sell_ts = last_sell_timestamp.get(ticker)
        if last_sell_ts:
            try:
                if (datetime.utcnow() - datetime.fromisoformat(last_sell_ts)).days < 21:
                    continue
            except Exception:
                continue

        # Phase 1: raise BUY floor — composite ≥ 72, matching the alert snapshot threshold.
        # Also gate by min_buy_confidence setting (default "medium" = accept medium+high).
        _conf_rank = {"low": 0, "medium": 1, "high": 2}
        _min_conf  = settings.get("min_buy_confidence", "medium").lower()
        if (direction != "bullish" or signal_score < 72
                or _conf_rank.get(confidence, 1) < _conf_rank.get(_min_conf, 1)
                or remaining_cash < 500):
            continue

        current_price = price_map.get(ticker, 0)
        if not current_price:
            current_price = (
                pred.get("price_at_prediction")
                or _price_from_info(info_map.get(ticker))
                or 0
            )
        if not current_price:
            continue

        # Factor score validation
        factors   = pred.get("factor_scores") or {}
        composite = factors.get("composite") or 50.0
        quality_s = factors.get("quality") or 50.0
        dcf_data  = pred.get("dcf") or {}
        mos_pct   = dcf_data.get("margin_of_safety_pct")
        vol_pct   = pred.get("annualised_vol_pct") or 30.0

        # Hard filters: reject fundamentally broken or massively overvalued stocks
        if quality_s < 30:
            continue  # Balance sheet too weak
        if mos_pct is not None and mos_pct < -30:
            continue  # DCF says massively overvalued

        # ── Phase 3: portfolio-relative sizing with sector + VaR caps ──────
        # Replaces cash-relative allocation (which starved late buys). Target
        # weight ≈ 1/N_target × conviction × confidence × vol_adj, clipped by
        # per-position, per-sector, and portfolio-VaR limits in that order.
        sec = ((info_map.get(ticker) or {}).get("sector")) or "Unknown"

        # Conviction factor 0.7–1.0 based on signal_score above the BUY floor (72).
        edge       = max(0.0, min(1.0, (signal_score - 72) / 28.0))
        conviction = 0.7 + 0.3 * edge
        conf_factor = 1.0 if confidence == "high" else 0.75
        # Vol-adjust: bidirectional. Low-vol names earn up to +30%, high-vol cut to -50%.
        vol_pct_clipped = max(15.0, min(60.0, float(vol_pct)))
        vol_adj = max(0.5, min(1.3, 30.0 / vol_pct_clipped))

        target_weight = (1.0 / N_target) * conviction * conf_factor * vol_adj
        target_weight = min(target_weight, position_max_pct)
        position_value = target_weight * base_portfolio_value

        # Sector cap — running tally including buys approved earlier in this loop.
        sector_headroom = max(0.0, sector_max_pct * base_portfolio_value - sector_exposure.get(sec, 0.0))
        if sector_headroom <= 0:
            continue
        position_value = min(position_value, sector_headroom)

        # Portfolio VaR cap (95% 1-month parametric). Uses pairwise correlations
        # from the risk matrix cache when available; uncorrelated otherwise.
        monthly_vol = (float(vol_pct) / 100.0) * MONTH_VOL_FACTOR
        if monthly_vol > 0:
            new_var = VAR_Z_95 * monthly_vol * position_value
            # Cross terms: Σⱼ∈held ρ_{ticker,j} * new_var * var_j
            cross_sum = sum(
                corr_matrix.get(ticker, {}).get(t2, 0.0) * new_var * v2
                for t2, v2 in held_var_terms.items()
            )
            projected_cov_sq = existing_cov_sq + 2.0 * cross_sum + new_var ** 2
            projected_var = math.sqrt(max(0.0, projected_cov_sq))
            if projected_var > var_budget:
                # Solve for max position size s.t. projected_var == var_budget.
                # existing_cov_sq + 2*rho_sum*x + x² = var_budget²
                # where rho_sum = Σⱼ ρ_{ticker,j}*var_j, x = new_var
                rho_sum = sum(
                    corr_matrix.get(ticker, {}).get(t2, 0.0) * v2
                    for t2, v2 in held_var_terms.items()
                )
                discriminant = rho_sum ** 2 - existing_cov_sq + var_budget ** 2
                if discriminant <= 0:
                    continue  # no VaR budget left
                max_ind_var = -rho_sum + math.sqrt(discriminant)
                if max_ind_var <= 0:
                    continue
                position_value = max_ind_var / (VAR_Z_95 * monthly_vol)

        # Cash cap (always last)
        position_value = min(position_value, remaining_cash)
        qty = int(position_value / current_price)
        if qty < 1:
            continue
        estimated_cost = round(qty * current_price, 2)
        if estimated_cost > remaining_cash:
            qty = int(remaining_cash / current_price)
            if qty < 1:
                continue
            estimated_cost = round(qty * current_price, 2)

        # Update running trackers AFTER size is finalised
        sector_exposure[sec] = sector_exposure.get(sec, 0.0) + estimated_cost
        actual_var = VAR_Z_95 * monthly_vol * estimated_cost if monthly_vol > 0 else 0.0
        cross = sum(
            corr_matrix.get(ticker, {}).get(t2, 0.0) * actual_var * v2
            for t2, v2 in held_var_terms.items()
        )
        existing_cov_sq += 2.0 * cross + actual_var ** 2
        held_var_terms[ticker] = actual_var

        # Factor-boosted score: composite 70 → +35%, composite 30 → -35%
        factor_boost = 1 + (composite - 50) / 200
        score = signal_score * accuracy * (1.15 if confidence == "high" else 1.0) * factor_boost

        # Phase 5: incremental impact of accepting THIS buy.
        # Expected £ contribution = position × expected_annualised_return × (months/12).
        annual_r = _expected_return_from_score(signal_score, confidence)
        delta_to_target_gbp = estimated_cost * (annual_r / 100.0) * (target_months / 12.0)
        target_gap = max(1.0, target - portfolio_value_for_traj)
        delta_to_target_pct_of_gap = (delta_to_target_gbp / target_gap) * 100.0

        # P(hit target) if this buy is accepted (existing holdings + this one)
        weights_with_buy = list(current_weights) + [(
            estimated_cost / portfolio_value_for_traj,
            annual_r,
            vol_pct,
        )]
        p_hit_with_this = _p_hit_target(
            portfolio_value_for_traj, target, target_months, weights_with_buy
        )

        buys.append({
            "ticker":         ticker,
            "name":           pred.get("name", ticker),
            "action":         "BUY",
            "trigger":        "PREDICTION" if confidence != "low" else "SPECULATIVE",
            "current_price":  round(current_price, 2),
            "qty":            qty,
            "estimated_cost": estimated_cost,
            "direction":      direction,
            "score_value":    signal_score,
            "predicted_pct":  predicted,
            "confidence":     confidence,
            "accuracy_pct":   cal.get("accuracy_pct"),
            "reasoning":      pred.get("reasoning", ""),
            "score":          round(score, 4),
            "factor_scores":  factors,
            "dcf":            dcf_data if dcf_data else None,
            "annualised_vol_pct": round(vol_pct, 1),
            # Phase 3: sizing metadata for the UI to surface
            "sector":              sec,
            "target_weight_pct":   round(target_weight * 100, 2),
            "actual_weight_pct":   round((estimated_cost / base_portfolio_value) * 100, 2) if base_portfolio_value > 0 else 0.0,
            "var_contribution_pct": round((actual_var / base_portfolio_value) * 100, 3) if base_portfolio_value > 0 else 0.0,
            # Phase 5: target-impact and cross-check
            "expected_annual_return_pct": round(annual_r, 1),
            "delta_to_target_gbp":        round(delta_to_target_gbp, 2),
            "delta_to_target_pct_of_gap": round(delta_to_target_pct_of_gap, 1),
            "p_hit_target_with_this":     round(p_hit_with_this, 4),
            "p_hit_target_delta":         round(p_hit_with_this - p_hit_current, 4),
            "consistency":                _consistency_for(ticker),
        })
        remaining_cash = max(0.0, remaining_cash - estimated_cost)
        build_done += 1
        _tick("build_recommendations", f"Ranking buy and sell opportunities… ({build_done}/{build_total})", build_done, build_total)

    if not latest_preds:
        _tick("build_recommendations", "Ranking buy and sell opportunities…", build_done, build_total)

    buys.sort(key=lambda x: x["score"], reverse=True)

    # Phase 1: TOP PICK fallback removed. Sitting in cash when no signal passes
    # the floor is the correct action, not a failure mode to paper over with a
    # relaxed-threshold candidate.

    # ── SELL recommendations (from paper portfolio holdings) ────────────────
    sells = []
    for ticker in paper_held:
        pos           = paper_positions[ticker]
        current_price = price_map.get(ticker, 0)
        if not current_price:
            current_price = next((p.get("price_at_prediction") for p in latest_preds if p.get("ticker") == ticker and p.get("price_at_prediction")), 0) or pos.get("avg_cost", 0)
        if not current_price:
            continue

        cost_basis      = pos["shares"] * pos["avg_cost"]
        current_value   = pos["shares"] * current_price
        unrealised_pnl  = current_value - cost_basis
        unrealised_pct  = (unrealised_pnl / cost_basis * 100) if cost_basis else 0

        pred      = next((p for p in latest_preds if p["ticker"] == ticker), None)
        trigger   = None
        reasoning = ""
        last_buy_ts = last_buy_timestamp.get(ticker)
        recent_buy_cooldown = False
        if last_buy_ts:
            try:
                recent_buy_cooldown = (datetime.utcnow() - datetime.fromisoformat(last_buy_ts)).days < 3
            except ValueError:
                recent_buy_cooldown = False
        latest_bullish = bool(
            pred
            and (pred.get("direction") or prediction_direction(pred.get("predicted_pct"))) == "bullish"
            and (pred.get("score") or prediction_score(pred.get("predicted_pct"), pred.get("confidence", "medium")) or 50) >= 60
            and pred.get("confidence") in ("high", "medium")
        )

        # ── Priority 1: hard risk controls ──────────────────────────────────
        # Phase 1: vol-scaled stop replaces fixed -5%. Daily-vol-driven so high-vol
        # names aren't shaken out by normal noise; floored at -8%, capped at -15%.
        ann_vol_pos = float((pred.get("annualised_vol_pct") if pred else None) or 30.0)
        daily_vol   = ann_vol_pos / math.sqrt(252)
        stop_pct    = max(-15.0, min(-8.0, -2.5 * daily_vol))
        if unrealised_pct <= stop_pct:
            trigger   = "STOP LOSS"
            reasoning = (f"Position down {unrealised_pct:.1f}% — vol-scaled stop at "
                         f"{stop_pct:.1f}% (daily vol {daily_vol:.1f}%) hit.")
        # Phase 1: fixed +8% TAKE PROFIT removed. Replaced by THESIS FLIPPED + TRAIL
        # STOP below — winners are held until the thesis turns or price falls 8%
        # off the recent peak, aligning sell timing with the 12-month forecast.
        elif pred and (pred.get("direction") or prediction_direction(pred.get("predicted_pct"))) == "bearish" and (pred.get("score") or prediction_score(pred.get("predicted_pct"), pred.get("confidence", "medium")) or 50) <= 45 and pred.get("confidence") in ("high", "medium"):
            trigger   = "PREDICTION"
            reasoning = pred.get("reasoning", "")

        # Phase 1: THESIS FLIPPED — composite signal turned weak even though price
        # hasn't broken the stop yet. Catches deteriorating fundamentals early.
        if not trigger and pred:
            composite_now = float((pred.get("factor_scores") or {}).get("composite") or 50.0)
            if composite_now < 55 and unrealised_pct > stop_pct:
                trigger   = "THESIS FLIPPED"
                reasoning = (f"Composite factor score dropped to {composite_now:.0f}/100 — "
                             f"the thesis that supported entry has weakened.")

        # Phase 1: TRAIL STOP — protect gains without capping them at +8%. Uses
        # the 30-day high already fetched into hist_map for held positions.
        if not trigger:
            hist = hist_map.get(ticker)
            if hist is not None and len(hist) >= 5 and unrealised_pct > 2.0:
                peak_30d = float(hist["High"].max())
                if peak_30d > 0 and current_price < peak_30d * 0.92:
                    drawdown_pct = (current_price / peak_30d - 1) * 100
                    trigger   = "TRAIL STOP"
                    reasoning = (f"Price {drawdown_pct:.1f}% off 30-day peak £{peak_30d:.2f} — "
                                 f"trailing stop locks in {unrealised_pct:+.1f}% gain.")

        # ── Priority 2: technical & fundamental signals ──────────────────────
        if not trigger and recent_buy_cooldown and latest_bullish:
            continue

        if not trigger:
            info = info_map.get(ticker, {})
            pe   = info.get("trailingPE")
            peg  = info.get("pegRatio")
            hist = hist_map.get(ticker)

            # Technical breakdown: price > 2% below 20-day SMA
            if hist is not None and len(hist) >= 20:
                sma_20 = float(hist["Close"].iloc[-20:].mean())
                if current_price < sma_20 * 0.98:
                    trigger   = "TECHNICAL BREAKDOWN"
                    reasoning = (f"Price £{current_price:.2f} is {((current_price / sma_20 - 1) * 100):.1f}% below "
                                 f"the 20-day moving average (£{sma_20:.2f}) — bearish technical signal.")

        # Negative prediction trend: last 3 predictions all negative
        if not trigger:
            ticker_preds = sorted(
                [p for p in predictions if p["ticker"] == ticker and p.get("predicted_pct") is not None],
                key=lambda x: x["date"], reverse=True,
            )
            if len(ticker_preds) >= 3 and all((p.get("direction") or prediction_direction(p.get("predicted_pct"))) == "bearish" for p in ticker_preds[:3]):
                recent    = [f"{(p.get('score') or prediction_score(p.get('predicted_pct'), p.get('confidence', 'medium')) or 50)}/100" for p in ticker_preds[:3]]
                trigger   = "NEGATIVE TREND"
                reasoning = (f"Last 3 model predictions all negative ({', '.join(recent)}) — "
                             f"consistent bearish signal from the model.")

        # Overvaluation: PE > 40 AND PEG > 2.5
        if not trigger and pe and pe > 40 and peg and peg > 2.5:
            trigger   = "OVERVALUED"
            reasoning = (f"Valuation stretched: P/E {pe:.1f} and PEG {peg:.2f} — "
                         f"fundamentals no longer support the current price relative to growth.")

        # Concentration risk: single position > 25% of portfolio
        if not trigger and paper_total_value > 0 and (current_value / paper_total_value) > 0.25:
            conc_pct  = current_value / paper_total_value * 100
            trigger   = "CONCENTRATION"
            reasoning = (f"Position is {conc_pct:.1f}% of total portfolio — exceeds 25% limit. "
                         f"Consider trimming to reduce single-stock concentration risk.")

        # Capital inefficiency: held 30+ days with < 3% return
        if not trigger and abs(unrealised_pct) < 3.0:
            first_date_str = first_buy_date.get(ticker)
            if first_date_str:
                try:
                    days_held = (date.today() - date.fromisoformat(first_date_str)).days
                    if days_held >= 30:
                        trigger   = "CAPITAL INEFFICIENCY"
                        reasoning = (f"Held {days_held} days with only {unrealised_pct:+.1f}% return — "
                                     f"consider redeploying capital to higher-conviction opportunities.")
                except ValueError:
                    pass

        # DCF overvaluation: model says massively overvalued AND prediction bearish
        if not trigger and pred:
            dcf_data = pred.get("dcf") or {}
            mos = dcf_data.get("margin_of_safety_pct")
            pred_dir = pred.get("direction") or ("bearish" if (pred.get("predicted_pct") or 0) < -0.5 else "bullish")
            if mos is not None and mos < -25 and pred_dir == "bearish":
                trigger   = "OVERVALUED_DCF"
                intrinsic = dcf_data.get("intrinsic_per_share", 0)
                reasoning = (f"DCF model estimates intrinsic value at ${intrinsic:.2f} — "
                             f"current price implies {abs(mos):.0f}% premium to fair value, "
                             f"compounded by a bearish model signal.")

        # Quality deterioration: balance sheet health score critically low
        if not trigger and pred:
            factors   = pred.get("factor_scores") or {}
            quality_s = int(factors.get("quality") or 50)
            if quality_s < 25:
                trigger   = "QUALITY_DETERIORATION"
                reasoning = (f"Balance sheet quality score has dropped to {quality_s}/100 — "
                             f"deteriorating margins, leverage, or liquidity signal elevated fundamental risk.")

        if trigger:
            sells.append({
                "ticker":              ticker,
                "name":                pos["name"],
                "action":              "SELL",
                "trigger":             trigger,
                "current_price":       round(current_price, 2),
                "qty":                 round(pos["shares"], 4),
                "estimated_proceeds":  round(pos["shares"] * current_price, 2),
                "cost_basis":          round(cost_basis, 2),
                "unrealised_pnl":      round(unrealised_pnl, 2),
                "unrealised_pct":      round(unrealised_pct, 2),
                "direction":           pred.get("direction") if pred else None,
                "score_value":         pred.get("score") if pred else None,
                "predicted_pct":       pred.get("predicted_pct") if pred else None,
                "confidence":          pred.get("confidence") if pred else None,
                "reasoning":           reasoning,
                "factor_scores":       (pred.get("factor_scores") or {}) if pred else {},
                "annualised_vol_pct":  pred.get("annualised_vol_pct") if pred else None,
                "max_drawdown_pct":    pred.get("max_drawdown_pct") if pred else None,
            })
        build_done += 1
        _tick("build_recommendations", f"Ranking buy and sell opportunities… ({build_done}/{build_total})", build_done, build_total)
    if not sells:
        review_candidates = []
        for ticker in paper_held:
            pos = paper_positions[ticker]
            current_price = price_map.get(ticker, 0) or pos.get("avg_cost", 0)
            if not current_price:
                continue
            cost_basis = pos["shares"] * pos["avg_cost"]
            current_value = pos["shares"] * current_price
            unrealised_pnl = current_value - cost_basis
            unrealised_pct = (unrealised_pnl / cost_basis * 100) if cost_basis else 0
            pred = next((p for p in latest_preds if p["ticker"] == ticker), None)
            if pred:
                weak_signal = (
                    pred.get("direction") == "bearish"
                    or (pred.get("score") is not None and pred.get("score") <= 48)
                    or (pred.get("predicted_pct") or 0) < -0.2
                )
                if not weak_signal:
                    continue
                reasoning = pred.get("reasoning") or "Latest model snapshot has weakened for this holding."
                review_score = 100 - int(pred.get("score") or 50)
            else:
                if unrealised_pct > -1.0:
                    continue
                reasoning = "This holding is slipping without a fresh bullish prediction to support staying in the position."
                review_score = min(100, int(abs(unrealised_pct) * 8) + 20)
            review_candidates.append((review_score, {
                "ticker":              ticker,
                "name":                pos["name"],
                "action":              "SELL",
                "trigger":             "REVIEW",
                "current_price":       round(current_price, 2),
                "qty":                 round(pos["shares"], 4),
                "estimated_proceeds":  round(pos["shares"] * current_price, 2),
                "cost_basis":          round(cost_basis, 2),
                "unrealised_pnl":      round(unrealised_pnl, 2),
                "unrealised_pct":      round(unrealised_pct, 2),
                "direction":           pred.get("direction") if pred else None,
                "score_value":         pred.get("score") if pred else None,
                "predicted_pct":       pred.get("predicted_pct") if pred else None,
                "confidence":          pred.get("confidence") if pred else None,
                "reasoning":           reasoning,
            }))
        sells = [item for _, item in sorted(review_candidates, key=lambda pair: pair[0], reverse=True)[:5]]
    _record_recommendation_stage_duration("build_recommendations", int((datetime.now(timezone.utc) - stage_started).total_seconds() * 1000))

    stage_started = datetime.now(timezone.utc)
    _tick("finalize", "Finalizing recommendation set…", 1, 1)
    explanation = None
    if not allow_buys and not buys:
        # Regime-blocked: explain why even if sells are present
        explanation = f"BUYs blocked — {regime.get('reason', 'market regime risk-off')}"
    elif not buys and not sells:
        if remaining_cash < 500:
            explanation = "No buy signals: available paper cash is below £500 minimum allocation."
        elif latest_preds:
            explanation = "No actionable signals: latest predictions did not pass confidence/quality/risk filters."
        else:
            explanation = "No actionable signals available."

    # Phase 5: portfolio-wide trajectory snapshot.
    # p_hit_target_if_all_buys = probability the portfolio reaches target if
    # every BUY in this response is accepted. p_hit_target_current is the
    # baseline before any new buys.
    weights_after_all_buys = list(current_weights) + [(
        b["estimated_cost"] / portfolio_value_for_traj,
        b["expected_annual_return_pct"],
        b["annualised_vol_pct"],
    ) for b in buys]
    p_hit_with_all_buys = _p_hit_target(
        portfolio_value_for_traj, target, target_months, weights_after_all_buys
    )
    trajectory = {
        "p_hit_target_current":      round(p_hit_current, 4),
        "p_hit_target_if_all_buys":  round(p_hit_with_all_buys, 4),
        "p_hit_target_delta":        round(p_hit_with_all_buys - p_hit_current, 4),
        "portfolio_value_for_calc":  round(portfolio_value_for_traj, 2),
        "target":                    target,
        "target_months":             target_months,
        "alert_snapshot_age_hours":  round(snapshot_age_hr, 1) if snapshot_age_hr is not None else None,
    }

    # Phase 3: aggregate risk view for the UI
    total_portfolio_var = math.sqrt(max(0.0, existing_cov_sq)) if existing_cov_sq > 0 else 0.0
    sector_breakdown = {
        s: round(v / base_portfolio_value * 100, 2)
        for s, v in sorted(sector_exposure.items(), key=lambda x: -x[1])
    } if base_portfolio_value > 0 else {}

    result = {
        "buys":            buys,
        "sells":           sells,
        "prediction_date": latest_date,
        "explanation":     explanation,
        "regime":          regime,
        "trajectory":      trajectory,
        "portfolio_risk": {
            "base_portfolio_value":      round(base_portfolio_value, 2),
            "target_positions_count":    N_target,
            "position_max_pct":          round(position_max_pct * 100, 1),
            "sector_max_pct":            round(sector_max_pct * 100, 1),
            "portfolio_var_max_pct":     round(portfolio_var_max * 100, 2),
            "projected_portfolio_var":   round(total_portfolio_var, 2),
            "projected_var_pct":         round(total_portfolio_var / base_portfolio_value * 100, 2) if base_portfolio_value > 0 else 0.0,
            "sector_exposure_pct":       sector_breakdown,
        },
        "summary": {
            "initial_float":         PAPER_INITIAL_FLOAT,
            "target":                target,
            "target_months":         target_months,
            "available_cash":        round(float(portfolio_summary.get("available_cash") or paper_cash), 2),
            "total_invested":        round(float(portfolio_summary.get("total_invested") or 0.0), 2),
            "total_current_value":   round(float(portfolio_summary.get("total_current_value") or 0.0), 2),
            "cash_after_buys":       round(remaining_cash, 2),
            "total_portfolio_value": round(float(portfolio_summary.get("total_portfolio_value") or paper_total_value), 2),
            "total_pnl":             round(float(portfolio_summary.get("total_pnl") or (paper_total_value - PAPER_INITIAL_FLOAT)), 2),
            "progress_pct":          progress_pct,
            "remaining_to_target":   round(target - paper_total_value, 2),
        },
    }
    _record_recommendation_stage_duration("finalize", int((datetime.now(timezone.utc) - stage_started).total_seconds() * 1000))
    return result


async def _run_recommendation_job(job_id: str) -> None:
    try:
        _update_recommendation_job(job_id, status="running", stage="load_predictions", message="Loading predictions and calibration…", completed=0, total=1)
        result = await _build_recommendations(
            lambda stage, message, completed=None, total=None: _update_recommendation_job(job_id, stage=stage, message=message, status="running", completed=completed, total=total)
        )
        _update_recommendation_job(job_id, status="completed", stage="completed", message="Recommendations ready.", result=result, completed=1, total=1)
    except Exception as e:
        logger.exception("Recommendation job %s failed", job_id)
        _update_recommendation_job(job_id, status="error", stage="error", error=str(e), message="Recommendations failed.")


@app.get("/api/recommendations")
@limiter.limit("30/hour")
async def get_recommendations(request: Request):
    return await _build_recommendations()


@app.post("/api/recommendations/start")
@limiter.limit("30/hour")
async def start_recommendations(request: Request):
    job_id = str(uuid.uuid4())
    _recommendation_jobs[job_id] = {
        "status": "queued",
        "stage": "load_predictions",
        "message": "Starting recommendations…",
        "started_at": datetime.now(timezone.utc),
        "elapsed_ms": 0,
        "eta_ms": _recommendation_eta_ms("load_predictions"),
        "completed": 0,
        "total": 1,
        "result": None,
        "error": None,
    }
    asyncio.create_task(_run_recommendation_job(job_id))
    return {"job_id": job_id}


# Phase 4: backtest harness for Phase 1-3 BUY/SELL rules. See backtest_phase4.py
# for the simulation; this endpoint is a thin wrapper that wires in current
# settings and exposes the threshold-sweep + score-curve-refit options.
@app.get("/api/recommendations/backtest")
@limiter.limit("10/hour")
async def recommendations_backtest(
    request: Request,
    lookback_days: int = 90,
    sensitivity: bool = False,
    refit_curve: bool = False,
    current_user: str = Depends(get_current_user),
):
    """
    Replay Phase 1-3 recommendation rules against historical predictions +
    yfinance prices. Returns trade ledger, aggregate metrics, and optionally
    a threshold sweep and refitted SCORE_TO_12M_RETURN suggestion.

    Query params:
      lookback_days  How far back to start (default 90, max 365).
      sensitivity    If true, also run a 4×3×3 grid over BUY floor /
                     stop multiplier / N_target_positions.
      refit_curve    If true, include a SCORE_TO_12M_RETURN refit suggestion
                     bucketed by entry_score.
    """
    from backtest_phase4 import (
        refit_score_to_12m_return,
        run_phase4_backtest,
        threshold_sensitivity_sweep,
    )

    lookback_days = max(7, min(365, int(lookback_days)))
    settings      = load_settings()
    predictions   = load_predictions()

    result = await run_phase4_backtest(
        predictions,
        lookback_days=lookback_days,
        initial_float=float(settings.get("initial_float", 200_000.0)),
        buy_floor_score=70,  # current Phase 1 floor
        stop_multiplier=2.5,
        n_target_positions=int(settings.get("target_positions_count", 10)),
        position_max_pct=float(settings.get("position_max_pct", 0.12)),
        sector_max_pct=float(settings.get("sector_max_pct", 0.30)),
        portfolio_var_max_pct=float(settings.get("portfolio_var_max_pct", 0.08)),
        regime_dma_period=int(settings.get("regime_spy_dma_period", 200)),
        regime_vix_max=float(settings.get("regime_vix_max", 25.0)),
        regime_check=bool(settings.get("regime_gate_enabled", True)),
    )

    if sensitivity:
        result["sensitivity"] = await threshold_sensitivity_sweep(
            predictions,
            lookback_days=lookback_days,
            initial_float=float(settings.get("initial_float", 200_000.0)),
        )

    if refit_curve:
        result["score_curve_refit"] = refit_score_to_12m_return(result["trades"])

    logger.info("BACKTEST_PHASE4 user=%s trades=%d", current_user, len(result.get("trades", [])))
    return result


@app.get("/api/recommendations/progress/{job_id}")
async def get_recommendations_progress(job_id: str):
    job = _recommendation_jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Recommendation job not found")
    elapsed_ms = int((datetime.now(timezone.utc) - job["started_at"]).total_seconds() * 1000)
    remaining_ms = max(0, int(job.get("eta_ms") or 0) - elapsed_ms) if job["status"] in {"queued", "running"} else 0
    completed = int(job.get("completed") or 0)
    total = int(job.get("total") or 0)
    percent = 100 if job["status"] == "completed" else _recommendation_percent(str(job.get("stage") or ""), completed, total)
    return {
        "status": job["status"],
        "stage": job.get("stage"),
        "message": job.get("message"),
        "elapsed_ms": elapsed_ms,
        "remaining_ms": remaining_ms,
        "completed": completed,
        "total": total,
        "percent": percent,
        "result": job.get("result"),
        "error": job.get("error"),
    }


@app.get("/api/settings")
def get_settings(current_user: str = Depends(get_current_user)):
    import db as _db
    db_user = _get_db_user(current_user)
    if db_user:
        return _load_user_settings_merged(db_user["user_id"])
    return load_settings()


@app.post("/api/settings")
def update_settings(s: dict, current_user: str = Depends(get_current_user)):
    import db as _db
    db_user = _get_db_user(current_user)
    if db_user:
        for k, v in s.items():
            if v is not None:
                _db.set_user_setting(db_user["user_id"], k, str(v))
        logger.info("SETTINGS_UPDATED user=%s", current_user)
        return _load_user_settings_merged(db_user["user_id"])
    save_settings(s)
    logger.info("SETTINGS_UPDATED user=%s", current_user)
    return load_settings()


@app.get("/api/risk")
def get_risk_dashboard():
    """Return per-ticker risk metrics and correlation matrix for portfolio + watchlist."""
    now = time.time()
    if _risk_cache["ts"] and now - _risk_cache["ts"] < _RISK_TTL and _risk_cache["data"]:
        return _risk_cache["data"]

    # Collect tickers: paper portfolio holdings + watchlist
    positions = compute_positions()
    held_tickers = list(positions.keys())
    try:
        wl = json.loads(WATCHLIST_FILE.read_text()) if WATCHLIST_FILE.exists() else []
        watchlist_tickers = [w["ticker"] if isinstance(w, dict) else w for w in wl]
    except Exception:
        watchlist_tickers = []
    all_tickers = list(dict.fromkeys(held_tickers + watchlist_tickers))

    if not all_tickers:
        return {"tickers": {}, "correlation": {}, "cached_at": None}

    # Fetch 90-day history for all tickers
    import yfinance as yf
    ticker_metrics: dict = {}
    returns_map: dict = {}
    for t in all_tickers:
        try:
            hist = yf.Ticker(t).history(period="90d", auto_adjust=True)
            if hist.empty or len(hist) < 5:
                continue
            prices = hist["Close"].dropna()
            vol   = compute_volatility(hist)
            dd    = compute_max_drawdown(hist)
            # Beta: correlation of stock returns vs SPY returns (use simple ratio)
            log_ret = np.log(prices / prices.shift(1)).dropna()
            returns_map[t] = log_ret
            info  = get_info_with_timeout(t, timeout=8)
            beta  = float(info.get("beta") or 1.0)
            ticker_metrics[t] = {
                "volatility_pct":   round(vol, 2) if vol is not None else None,
                "max_drawdown_pct": round(dd, 2) if dd is not None else None,
                "beta":             round(beta, 2),
                "in_portfolio":     t in held_tickers,
            }
        except Exception as e:
            logger.warning("RISK_FETCH_ERROR ticker=%s err=%s", t, e)

    # Correlation matrix for held tickers with enough overlapping data
    corr_matrix: dict = {}
    held_with_data = [t for t in held_tickers if t in returns_map]
    if len(held_with_data) >= 2:
        import pandas as pd
        df = pd.DataFrame({t: returns_map[t] for t in held_with_data}).dropna(how="all")
        if len(df) >= 20:
            corr = df.corr().round(3)
            corr_matrix = corr.to_dict()

    result = {
        "tickers":    ticker_metrics,
        "correlation": corr_matrix,
        "cached_at":  datetime.now(timezone.utc).isoformat(),
    }
    _risk_cache["ts"]   = now
    _risk_cache["data"] = result
    return result


# ── Portfolio endpoints ───────────────────────────────────────────────────────

@app.get("/api/portfolio")
async def get_portfolio(current_user: str = Depends(get_current_user)):
    import db as _db
    db_user = _get_db_user(current_user)
    if db_user:
        transactions = _db.get_user_transactions(db_user["user_id"], "real")
    else:
        transactions = load_portfolio()
    positions = compute_positions(transactions)

    held = [t for t, p in positions.items() if p["shares"] > 0]
    price_map = await _get_portfolio_price_map(held, positions) if held else {}

    # Compute cost basis of all sold shares (for per-position realised %)
    realised_cost_map: dict[str, float] = {}
    _run: dict[str, dict] = {}
    for tx in sorted(transactions, key=lambda x: x["timestamp"]):
        t = tx["ticker"]
        if t not in _run:
            _run[t] = {"shares": 0.0, "avg_cost": 0.0}
        r = _run[t]
        qty = float(tx.get("qty", 0))
        price = float(tx.get("price", 0))
        if tx["type"] == "buy":
            total = r["shares"] * r["avg_cost"] + qty * price
            r["shares"] += qty
            r["avg_cost"] = total / r["shares"] if r["shares"] > 0 else 0.0
        elif tx["type"] == "sell":
            sell_qty = min(qty, r["shares"])
            realised_cost_map[t] = realised_cost_map.get(t, 0.0) + r["avg_cost"] * sell_qty
            r["shares"] = max(0.0, r["shares"] - sell_qty)

    result = []
    total_invested = 0.0
    total_current_value = 0.0
    total_unrealised_pnl = 0.0
    total_realised_pnl = sum(pos["realised_pnl"] for pos in positions.values())

    for ticker, pos in positions.items():
        if pos["shares"] <= 0:
            continue
        current_price = price_map.get(ticker, 0) or pos["avg_cost"]
        cost_basis = pos["shares"] * pos["avg_cost"]
        current_value = pos["shares"] * current_price
        unrealised_pnl = current_value - cost_basis
        realised_pnl = pos["realised_pnl"]
        realised_cost = realised_cost_map.get(ticker, 0.0)
        total_invested += cost_basis
        total_current_value += current_value
        total_unrealised_pnl += unrealised_pnl

        result.append({
            "ticker": ticker,
            "name": pos["name"],
            "shares": round(pos["shares"], 4),
            "avg_cost": round(pos["avg_cost"], 2),
            "current_price": round(current_price, 2),
            "cost_basis": round(cost_basis, 2),
            "current_value": round(current_value, 2),
            "unrealised_pnl": round(unrealised_pnl, 2),
            "unrealised_pct": round((unrealised_pnl / cost_basis * 100) if cost_basis else 0, 2),
            "realised_pnl": round(realised_pnl, 2),
            "realised_pct": round((realised_pnl / realised_cost * 100) if realised_cost else 0, 2),
        })

    total_pnl = total_unrealised_pnl + total_realised_pnl
    summary = {
        "total_invested": round(total_invested, 2),
        "total_current_value": round(total_current_value, 2),
        "total_unrealised_pnl": round(total_unrealised_pnl, 2),
        "total_unrealised_pct": round((total_unrealised_pnl / total_invested * 100) if total_invested else 0, 2),
        "total_realised_pnl": round(total_realised_pnl, 2),
        "total_pnl": round(total_pnl, 2),
        "total_pnl_pct": round((total_pnl / total_invested * 100) if total_invested else 0, 2),
    }

    return {
        "positions": sorted(result, key=lambda x: x["ticker"]),
        "summary": summary,
    }


@app.post("/api/portfolio/buy")
async def portfolio_buy(req: TradeRequest, current_user: str = Depends(get_current_user)):
    import db as _db
    ticker = req.ticker.upper()
    try:
        info = await get_info_with_timeout(ticker, 2.5)
    except Exception:
        info = {}
    name = info.get("shortName", ticker) if isinstance(info, dict) else ticker
    tx = {
        "id": str(uuid.uuid4()),
        "type": "buy",
        "ticker": ticker,
        "name": name,
        "qty": req.qty,
        "price": req.price,
        "date": req.date or str(date.today()),
        "timestamp": datetime.utcnow().isoformat(),
    }
    db_user = _get_db_user(current_user)
    if db_user:
        _db.add_user_transaction(db_user["user_id"], "real", tx)
    else:
        transactions = load_portfolio()
        transactions.append(tx)
        save_portfolio(transactions)
    return {"ok": True}


@app.post("/api/portfolio/sell")
async def portfolio_sell(req: TradeRequest, current_user: str = Depends(get_current_user)):
    import db as _db
    ticker = req.ticker.upper()
    db_user = _get_db_user(current_user)
    if db_user:
        transactions = _db.get_user_transactions(db_user["user_id"], "real")
    else:
        transactions = load_portfolio()
    positions = compute_positions(transactions)
    held = positions.get(ticker, {}).get("shares", 0)
    if req.qty > held:
        raise HTTPException(status_code=400, detail=f"Cannot sell {req.qty} shares — only {held} held")
    try:
        info = await get_info_with_timeout(ticker, 2.5)
    except Exception:
        info = {}
    name = info.get("shortName", ticker) if isinstance(info, dict) else ticker
    tx = {
        "id": str(uuid.uuid4()),
        "type": "sell",
        "ticker": ticker,
        "name": name,
        "qty": req.qty,
        "price": req.price,
        "date": req.date or str(date.today()),
        "timestamp": datetime.utcnow().isoformat(),
    }
    if db_user:
        _db.add_user_transaction(db_user["user_id"], "real", tx)
    else:
        transactions.append(tx)
        save_portfolio(transactions)
    return {"ok": True}


@app.get("/api/portfolio/transactions")
def get_transactions(current_user: str = Depends(get_current_user)):
    import db as _db
    db_user = _get_db_user(current_user)
    if db_user:
        return _db.get_user_transactions(db_user["user_id"], "real")
    return load_portfolio()


@app.delete("/api/portfolio/transaction/{tx_id}")
def delete_transaction(tx_id: str, current_user: str = Depends(get_current_user)):
    import db as _db
    db_user = _get_db_user(current_user)
    if db_user:
        _db.delete_user_transaction(db_user["user_id"], tx_id)
    else:
        transactions = load_portfolio()
        transactions = [t for t in transactions if t["id"] != tx_id]
        save_portfolio(transactions)
    logger.info("TRANSACTION_DELETED user=%s tx_id=%s", current_user, tx_id)
    return {"ok": True}


_MAX_CSV_BYTES = 5 * 1024 * 1024   # 5 MB
_MAX_PDF_BYTES = 20 * 1024 * 1024  # 20 MB

@app.post("/api/portfolio/import")
@limiter.limit("10/hour")
async def import_portfolio(request: Request, file: UploadFile = File(...), replace: bool = False, current_user: str = Depends(get_current_user)):
    """
    Import transactions from a CSV file.
    Required columns: type, ticker, qty, price, date
    type must be 'buy' or 'sell'
    date format: YYYY-MM-DD

    replace=true  — clears all existing transactions before importing (idempotent full reload)
    replace=false — appends, skipping rows that are exact duplicates of existing transactions
    """
    content = await file.read(_MAX_CSV_BYTES + 1)
    if len(content) > _MAX_CSV_BYTES:
        raise HTTPException(status_code=413, detail="File too large — maximum CSV size is 5 MB")
    try:
        text = content.decode("utf-8-sig")  # handle BOM from Excel
    except Exception:
        raise HTTPException(status_code=400, detail="Could not decode file — ensure it is UTF-8 CSV.")

    reader     = csv_mod.DictReader(io.StringIO(text))
    required   = {"type", "ticker", "qty", "price", "date"}
    if not required.issubset({c.strip().lower() for c in (reader.fieldnames or [])}):
        raise HTTPException(status_code=400, detail=f"CSV must have columns: {', '.join(sorted(required))}")

    import db as _db
    db_user = _get_db_user(current_user)
    if db_user:
        existing_txns = [] if replace else _db.get_user_transactions(db_user["user_id"], "real")
        if replace:
            _db.clear_user_transactions(db_user["user_id"], "real")
    else:
        existing_txns = [] if replace else load_portfolio()
    transactions = list(existing_txns)
    imported, skipped = 0, []
    new_txns: list[dict] = []

    # Build dedup key set from whatever transactions we're starting with
    existing_keys: set[tuple] = {
        (t["type"], t["ticker"], float(t["qty"]), float(t["price"]), t.get("date", ""))
        for t in transactions
    }

    # Pre-build current positions for sell validation
    positions = compute_positions(transactions)

    for i, row in enumerate(reader, start=2):
        row = {k.strip().lower(): v.strip() for k, v in row.items()}
        try:
            tx_type = row["type"].lower()
            if tx_type not in ("buy", "sell"):
                skipped.append(f"Row {i}: type must be 'buy' or 'sell', got '{row['type']}'")
                continue

            ticker = row["ticker"].upper()
            qty    = float(row["qty"])
            price  = float(row["price"])
            tx_date = row["date"]

            if qty <= 0 or price <= 0:
                skipped.append(f"Row {i} ({ticker}): qty and price must be positive")
                continue

            # Validate date
            datetime.strptime(tx_date, "%Y-%m-%d")

            # Skip exact duplicates (same type+ticker+qty+price+date already present)
            dedup_key = (tx_type, ticker, qty, price, tx_date)
            if dedup_key in existing_keys:
                skipped.append(f"Row {i} ({ticker}): duplicate, skipped")
                continue

            # For sells, check sufficient shares
            if tx_type == "sell":
                held = positions.get(ticker, {}).get("shares", 0)
                if qty > held:
                    skipped.append(f"Row {i} ({ticker}): cannot sell {qty} — only {held} held")
                    continue

            # Fetch name (best effort)
            try:
                info = yf.Ticker(ticker).info
                name = info.get("shortName", ticker)
            except Exception:
                name = ticker

            tx = {
                "id":        str(uuid.uuid4()),
                "type":      tx_type,
                "ticker":    ticker,
                "name":      name,
                "qty":       qty,
                "price":     price,
                "date":      tx_date,
                "timestamp": datetime.utcnow().isoformat(),
            }
            transactions.append(tx)
            new_txns.append(tx)
            existing_keys.add(dedup_key)
            # Update positions dict for subsequent sell validation in same file
            if ticker not in positions:
                positions[ticker] = {"shares": 0, "avg_cost": 0, "realised_pnl": 0, "name": name}
            if tx_type == "buy":
                old_shares = positions[ticker]["shares"]
                old_cost   = positions[ticker]["avg_cost"]
                new_shares = old_shares + qty
                positions[ticker]["avg_cost"] = ((old_shares * old_cost) + (qty * price)) / new_shares
                positions[ticker]["shares"]   = new_shares
            else:
                positions[ticker]["shares"] -= qty

            imported += 1

        except ValueError as e:
            skipped.append(f"Row {i}: {e}")
            continue

    if imported > 0:
        if db_user:
            for tx in new_txns:
                _db.add_user_transaction(db_user["user_id"], "real", tx)
        else:
            save_portfolio(transactions)

    return {
        "imported": imported,
        "skipped":  len(skipped),
        "errors":   skipped,
    }


@app.delete("/api/portfolio/reset")
@limiter.limit("10/hour")
async def reset_portfolio(request: Request, current_user: str = Depends(get_current_user)):
    """Clear all portfolio transactions."""
    import db as _db
    db_user = _get_db_user(current_user)
    if db_user:
        _db.clear_user_transactions(db_user["user_id"], "real")
    else:
        save_portfolio([])
    logger.info("PORTFOLIO_RESET user=%s", current_user)
    return {"ok": True, "message": "Portfolio cleared."}


@app.get("/api/portfolio/template")
def portfolio_template():
    """Return a CSV template for portfolio import."""
    from fastapi.responses import Response
    template = "type,ticker,qty,price,date\nbuy,AAPL,10,175.50,2025-01-15\nbuy,MSFT,5,380.00,2025-02-01\nsell,AAPL,5,182.00,2025-03-10\n"
    return Response(content=template, media_type="text/csv",
                    headers={"Content-Disposition": "attachment; filename=portfolio_template.csv"})


@app.post("/api/portfolio/import-pdf")
@limiter.limit("5/hour")
async def import_portfolio_pdf(request: Request, file: UploadFile = File(...), replace: bool = False, current_user: str = Depends(get_current_user)):
    """
    Import transactions from a Saxo Bank PDF statement.
    Extracts text from the PDF and uses Claude AI to parse transactions.
    replace=true clears all existing transactions before importing.
    """
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        raise HTTPException(status_code=500, detail="ANTHROPIC_API_KEY not set")

    content = await file.read(_MAX_PDF_BYTES + 1)
    if len(content) > _MAX_PDF_BYTES:
        raise HTTPException(status_code=413, detail="File too large — maximum PDF size is 20 MB")
    try:
        reader = pypdf.PdfReader(io.BytesIO(content))
        pages_text = []
        for page in reader.pages:
            text = page.extract_text()
            if text:
                pages_text.append(text)
        full_text = "\n\n".join(pages_text)
    except Exception:
        raise HTTPException(status_code=400, detail="Could not read PDF — ensure the file is a valid PDF.")

    if not full_text.strip():
        raise HTTPException(status_code=400, detail="PDF appears to be empty or image-only — text could not be extracted.")

    # Truncate to avoid token limits (keep first ~12000 chars)
    truncated = full_text[:12000]

    prompt = f"""You are parsing a Saxo Bank brokerage statement to extract stock transactions.

Extract every BUY and SELL transaction from the text below.
For each transaction return:
- type: "buy" or "sell"
- ticker: the US stock ticker symbol (e.g. "AAPL"). If only a company name is given, infer the correct US ticker.
- qty: number of shares (positive number)
- price: price per share in the currency shown (use the per-share price, not total)
- date: transaction date in YYYY-MM-DD format

If the document shows current POSITIONS (not transactions), treat each position as a "buy" using the average cost/purchase price and most recent date available.

Return ONLY a valid JSON array, no explanation:
[
  {{"type": "buy", "ticker": "AAPL", "qty": 10, "price": 175.50, "date": "2025-01-15"}},
  {{"type": "sell", "ticker": "MSFT", "qty": 5, "price": 380.00, "date": "2025-02-01"}}
]

If no transactions can be found, return an empty array: []

PDF TEXT:
{truncated}"""

    client = anthropic.Anthropic(api_key=api_key)
    try:
        msg = client.messages.create(
            model=ANTHROPIC_MODEL,
            max_tokens=2048,
            messages=[{"role": "user", "content": prompt}],
        )
    except Exception as e:
        logger.error("PDF import Claude API error: %s", e)
        raise HTTPException(status_code=502, detail="AI service unavailable — please try again in a moment.")

    if not msg.content or not hasattr(msg.content[0], "text"):
        raise HTTPException(status_code=502, detail="AI returned an empty response — please try again.")

    raw = msg.content[0].text.strip()
    if "```" in raw:
        raw = raw.split("```")[1]
        if raw.startswith("json"):
            raw = raw[4:]
        raw = raw.strip().rstrip("```")

    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as e:
        logger.error("PDF import JSON parse error: %s — raw: %s", e, raw[:200])
        raise HTTPException(status_code=422, detail="Could not parse transactions from this PDF. Ensure it contains readable text.")

    if not parsed:
        return {"imported": 0, "skipped": 0, "errors": [], "preview": [], "message": "No transactions found in this PDF."}

    # Validate and import
    import db as _db
    db_user = _get_db_user(current_user)
    if db_user:
        existing_txns = [] if replace else _db.get_user_transactions(db_user["user_id"], "real")
        if replace:
            _db.clear_user_transactions(db_user["user_id"], "real")
    else:
        existing_txns = [] if replace else load_portfolio()
    transactions = list(existing_txns)
    positions    = compute_positions(transactions)
    imported, skipped = 0, []
    preview = []
    new_txns: list[dict] = []

    existing_keys: set[tuple] = {
        (t["type"], t["ticker"], float(t["qty"]), float(t["price"]), t.get("date", ""))
        for t in transactions
    }

    for i, row in enumerate(parsed, start=1):
        try:
            tx_type = str(row.get("type", "")).lower()
            ticker  = str(row.get("ticker", "")).upper()
            qty     = float(row.get("qty", 0))
            price   = float(row.get("price", 0))
            tx_date = str(row.get("date", ""))

            if tx_type not in ("buy", "sell"):
                skipped.append(f"Row {i}: invalid type '{tx_type}'"); continue
            if not ticker:
                skipped.append(f"Row {i}: missing ticker"); continue
            if qty <= 0 or price <= 0:
                skipped.append(f"Row {i} ({ticker}): qty and price must be positive"); continue

            datetime.strptime(tx_date, "%Y-%m-%d")

            dedup_key = (tx_type, ticker, qty, price, tx_date)
            if dedup_key in existing_keys:
                skipped.append(f"Row {i} ({ticker}): duplicate, skipped"); continue

            if tx_type == "sell":
                held = positions.get(ticker, {}).get("shares", 0)
                if qty > held:
                    skipped.append(f"Row {i} ({ticker}): cannot sell {qty} — only {held:.4f} held"); continue

            try:
                info = yf.Ticker(ticker).info
                name = info.get("shortName", ticker)
            except Exception:
                name = ticker

            tx = {
                "id": str(uuid.uuid4()), "type": tx_type, "ticker": ticker,
                "name": name, "qty": qty, "price": price,
                "date": tx_date, "timestamp": datetime.utcnow().isoformat(),
            }
            transactions.append(tx)
            new_txns.append(tx)
            existing_keys.add(dedup_key)
            preview.append({"type": tx_type, "ticker": ticker, "name": name, "qty": qty, "price": price, "date": tx_date})

            if ticker not in positions:
                positions[ticker] = {"shares": 0, "avg_cost": 0, "realised_pnl": 0, "name": name}
            if tx_type == "buy":
                old_s = positions[ticker]["shares"]
                old_c = positions[ticker]["avg_cost"]
                new_s = old_s + qty
                positions[ticker]["avg_cost"] = ((old_s * old_c) + (qty * price)) / new_s
                positions[ticker]["shares"]   = new_s
            else:
                positions[ticker]["shares"] -= qty

            imported += 1
        except ValueError as e:
            skipped.append(f"Row {i}: {e}")

    if imported > 0:
        if db_user:
            for tx in new_txns:
                _db.add_user_transaction(db_user["user_id"], "real", tx)
        else:
            save_portfolio(transactions)

    return {"imported": imported, "skipped": len(skipped), "errors": skipped, "preview": preview}


# ── Paper Portfolio endpoints ─────────────────────────────────────────────────

@app.get("/api/paper-portfolio")
async def get_paper_portfolio(current_user: str = Depends(get_current_user)):
    import db as _db
    db_user = _get_db_user(current_user)
    if db_user:
        transactions = _db.get_user_transactions(db_user["user_id"], "paper")
    else:
        transactions = load_paper_portfolio()
    annotated_transactions = annotate_transactions_with_realised_pnl(transactions)
    positions    = compute_positions(transactions)
    held         = [t for t, p in positions.items() if p["shares"] > 0]

    if held:
        infos = await asyncio.gather(*[get_info_with_timeout(t, SEARCH_INFO_TIMEOUT_SEC) for t in held], return_exceptions=True)
        price_map = {t: (i.get("currentPrice") or i.get("regularMarketPrice") or 0)
                     for t, i in zip(held, infos) if not isinstance(i, Exception)}
    else:
        price_map = {}

    for ticker in held:
        if not price_map.get(ticker):
            fallback_price = positions.get(ticker, {}).get("avg_cost", 0) or 0
            if fallback_price:
                price_map[ticker] = fallback_price

    # Cash tracking: start at £100k, subtract buys, add sells
    cash = PAPER_INITIAL_FLOAT
    for tx in transactions:
        qty   = float(tx.get("qty", 0))
        price = float(tx.get("price", 0))
        if tx["type"] == "buy":
            cash -= qty * price
        elif tx["type"] == "sell":
            cash += qty * price

    total_current   = sum(positions[t]["shares"] * price_map.get(t, 0) for t in held)
    total_invested  = sum(positions[t]["shares"] * positions[t]["avg_cost"] for t in held)
    total_value     = cash + total_current
    total_pnl       = total_value - PAPER_INITIAL_FLOAT
    total_pnl_pct   = round(total_pnl / PAPER_INITIAL_FLOAT * 100, 2)
    realised_pnl    = sum(positions[t]["realised_pnl"] for t in positions)

    result = []
    for ticker, pos in positions.items():
        if pos["shares"] <= 0:
            continue
        current_price  = price_map.get(ticker, 0) or pos["avg_cost"]
        cost_basis     = pos["shares"] * pos["avg_cost"]
        current_value  = pos["shares"] * current_price
        unrealised_pnl = current_value - cost_basis
        result.append({
            "ticker":        ticker,
            "name":          pos["name"],
            "shares":        round(pos["shares"], 4),
            "avg_cost":      round(pos["avg_cost"], 2),
            "current_price": round(current_price, 2),
            "cost_basis":    round(cost_basis, 2),
            "current_value": round(current_value, 2),
            "unrealised_pnl": round(unrealised_pnl, 2),
            "unrealised_pct": round((unrealised_pnl / cost_basis * 100) if cost_basis else 0, 2),
            "realised_pnl":  round(pos["realised_pnl"], 2),
        })

    return {
        "positions": sorted(result, key=lambda x: x["ticker"]),
        "transactions": list(reversed(annotated_transactions[-50:])),
        "summary": {
            "initial_float":   PAPER_INITIAL_FLOAT,
            "cash":            round(cash, 2),
            "total_invested":  round(total_invested, 2),
            "total_current_value": round(total_current, 2),
            "total_unrealised_pnl": round(total_current - total_invested, 2),
            "total_value":     round(total_value, 2),
            "total_pnl":       round(total_pnl, 2),
            "total_pnl_pct":   total_pnl_pct,
            "realised_pnl":    round(realised_pnl, 2),
        },
    }


@app.post("/api/paper-portfolio/buy")
async def paper_buy(req: TradeRequest, current_user: str = Depends(get_current_user)):
    import db as _db
    db_user = _get_db_user(current_user)
    if db_user:
        transactions = _db.get_user_transactions(db_user["user_id"], "paper")
    else:
        transactions = load_paper_portfolio()
    cash = PAPER_INITIAL_FLOAT
    for tx in transactions:
        qty   = float(tx.get("qty", 0))
        price = float(tx.get("price", 0))
        if tx["type"] == "buy":
            cash -= qty * price
        elif tx["type"] == "sell":
            cash += qty * price
    cost = req.qty * req.price
    if cost > cash + 1e-9:
        raise HTTPException(status_code=400, detail=f"Insufficient paper cash (£{cash:.2f} available)")
    try:
        info = await get_info_with_timeout(req.ticker.upper(), 2.5)
    except Exception:
        info = {}
    name = info.get("shortName", req.ticker.upper()) if isinstance(info, dict) else req.ticker.upper()
    tx = {
        "id":        str(uuid.uuid4()),
        "type":      "buy",
        "ticker":    req.ticker.upper(),
        "name":      name,
        "qty":       req.qty,
        "price":     req.price,
        "date":      req.date or str(date.today()),
        "timestamp": datetime.utcnow().isoformat(),
        "source":    "recommendation",
    }
    if db_user:
        _db.add_user_transaction(db_user["user_id"], "paper", tx)
    else:
        transactions.append(tx)
        save_paper_portfolio(transactions)
    return {"ok": True}


@app.post("/api/paper-portfolio/sell")
async def paper_sell(req: TradeRequest, current_user: str = Depends(get_current_user)):
    import db as _db
    db_user = _get_db_user(current_user)
    if db_user:
        transactions = _db.get_user_transactions(db_user["user_id"], "paper")
    else:
        transactions = load_paper_portfolio()
    positions    = compute_positions(transactions)
    held         = positions.get(req.ticker.upper(), {}).get("shares", 0)
    if req.qty > held + 1e-9:
        raise HTTPException(status_code=400, detail=f"Cannot sell {req.qty} — only {held:.4f} held in paper portfolio")
    try:
        info = await get_info_with_timeout(req.ticker.upper(), 2.5)
    except Exception:
        info = {}
    name = info.get("shortName", req.ticker.upper()) if isinstance(info, dict) else req.ticker.upper()
    tx = {
        "id":        str(uuid.uuid4()),
        "type":      "sell",
        "ticker":    req.ticker.upper(),
        "name":      name,
        "qty":       req.qty,
        "price":     req.price,
        "date":      req.date or str(date.today()),
        "timestamp": datetime.utcnow().isoformat(),
        "source":    "recommendation",
    }
    if db_user:
        _db.add_user_transaction(db_user["user_id"], "paper", tx)
    else:
        transactions.append(tx)
        save_paper_portfolio(transactions)
    return {"ok": True}


@app.delete("/api/paper-portfolio/reset")
def reset_paper_portfolio(current_user: str = Depends(get_current_user)):
    import db as _db
    db_user = _get_db_user(current_user)
    if db_user:
        _db.clear_user_transactions(db_user["user_id"], "paper")
    else:
        save_paper_portfolio([])
    logger.info("PAPER_PORTFOLIO_RESET user=%s", current_user)
    return {"ok": True}


@app.delete("/api/alerts")
def clear_alerts(current_user: str = Depends(get_current_user)):
    import db as _db
    db_user = _get_db_user(current_user)
    if db_user:
        _db.clear_user_alerts(db_user["user_id"])
    else:
        save_alerts([])
    logger.info("ALERTS_CLEARED user=%s", current_user)
    return {"message": "Alert history cleared."}


# ── Sentiment Agent endpoints ─────────────────────────────────────────────────

SENTIMENT_AGENT_STATE_FILE = Path(__file__).parent / "sentiment_agent_state.json"

@app.get("/api/sentiment-agent/status")
def sentiment_agent_status():
    """Return the last scan result from the sentiment agent state file."""
    if not SENTIMENT_AGENT_STATE_FILE.exists():
        return {"last_run": None, "last_alerts": {}, "seen_headlines_count": 0}
    try:
        state = json.loads(SENTIMENT_AGENT_STATE_FILE.read_text(encoding="utf-8"))
        return {
            "last_run": state.get("last_run"),
            "last_alerts": state.get("last_alerts", {}),
            "seen_headlines_count": len(state.get("seen_headlines", {})),
        }
    except Exception as e:
        return {"error": str(e)}


@app.post("/api/sentiment-agent/scan")
async def trigger_sentiment_scan(
    dry_run: bool = False,
    current_user: str = Depends(get_current_user)
):
    """Trigger an on-demand sentiment scan (runs in background thread)."""
    import subprocess
    script = Path(__file__).parent / "sentiment_agent.py"
    if not script.exists():
        raise HTTPException(status_code=500, detail="sentiment_agent.py not found")

    cmd = [sys.executable, str(script)]
    if dry_run:
        cmd.append("--dry-run")

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
        try:
            output = json.loads(result.stdout.strip())
        except Exception:
            output = {"raw": result.stdout[:2000]}
        logger.info("SENTIMENT_SCAN user=%s dry_run=%s result=%s", current_user, dry_run, output)
        return {"ok": True, "result": output, "stderr": result.stderr[:500] if result.stderr else ""}
    except subprocess.TimeoutExpired:
        raise HTTPException(status_code=504, detail="Scan timed out after 5 minutes")
    except Exception as e:
        logger.error("SENTIMENT_SCAN_DRYRUN_FAIL: %s", e)
        raise HTTPException(status_code=500, detail="Sentiment scan failed")


@app.post("/api/sentiment-agent/reset-state")
def reset_sentiment_state(current_user: str = Depends(get_current_user)):
    """Clear the agent's seen-headline cache and alert cooldowns."""
    SENTIMENT_AGENT_STATE_FILE.unlink(missing_ok=True)
    logger.info("SENTIMENT_STATE_RESET user=%s", current_user)
    return {"ok": True, "message": "Sentiment agent state reset."}


# ── Multi-Agent v1 API ────────────────────────────────────────────────────────
# These endpoints expose the new structured agent pipeline alongside the
# existing /api/* endpoints (fully backward-compatible).

def _get_orchestrator():
    """Lazy import to avoid circular imports at module load time."""
    import sys, os
    backend_dir = os.path.dirname(__file__)
    if backend_dir not in sys.path:
        sys.path.insert(0, backend_dir)
    from agents.orchestrator import OrchestratorAgent
    return OrchestratorAgent()


# Run state store (in-memory for MVP; replace with DB table if needed)
_v1_runs: dict = {}


class V1RunRequest(BaseModel):
    tickers: list[str] | None = None
    run_fresh: bool = False
    context_notes: str | None = None


@app.post("/v1/runs")
@limiter.limit("3/minute")
async def v1_create_run(
    request: Request,
    background_tasks: BackgroundTasks,
    req: V1RunRequest | None = Body(default=None),
    current_user: str = Depends(get_current_user),
):
    """Trigger thesis generation for one or more tickers."""
    import db as _db

    tickers = req.tickers if req else None
    run_fresh = req.run_fresh if req else False
    context_notes = req.context_notes if req else None
    if not tickers:
        tickers = load_watchlist()
    tickers = [_validate_ticker(t) for t in tickers]
    if not tickers:
        raise HTTPException(status_code=400, detail="No tickers specified and watchlist is empty")

    run_id = str(uuid.uuid4())
    _v1_runs[run_id] = {
        "run_id": run_id,
        "status": "queued",
        "tickers": tickers,
        "started_at": datetime.now(timezone.utc).isoformat(),
        "completed": [],
        "failed": [],
    }
    _db.create_thesis_run(run_id, tickers, run_fresh, current_user)

    async def _run_all():
        orch = _get_orchestrator()
        for ticker in tickers:
            try:
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(None, lambda t=ticker: orch.run_thesis(t, run_fresh=run_fresh, context_notes=context_notes))
                _v1_runs[run_id]["completed"].append(ticker)
                _db.update_thesis_run(
                    run_id,
                    status="running",
                    completed=_v1_runs[run_id]["completed"],
                    failed=_v1_runs[run_id]["failed"],
                )
            except Exception as exc:
                logger.error("[v1/runs] %s failed: %s", ticker, exc)
                _v1_runs[run_id]["failed"].append(ticker)
                _db.update_thesis_run(
                    run_id,
                    status="running",
                    completed=_v1_runs[run_id]["completed"],
                    failed=_v1_runs[run_id]["failed"],
                )
        final_status = "completed"
        if _v1_runs[run_id]["failed"] and _v1_runs[run_id]["completed"]:
            final_status = "partial"
        elif _v1_runs[run_id]["failed"] and not _v1_runs[run_id]["completed"]:
            final_status = "failed"
        _v1_runs[run_id]["status"] = final_status
        _v1_runs[run_id]["completed_at"] = datetime.now(timezone.utc).isoformat()
        _db.update_thesis_run(
            run_id,
            status=final_status,
            completed=_v1_runs[run_id]["completed"],
            failed=_v1_runs[run_id]["failed"],
        )

    _v1_runs[run_id]["status"] = "running"
    _db.update_thesis_run(run_id, status="running")
    background_tasks.add_task(_run_all)
    return {"run_id": run_id, "status": "running", "tickers": tickers}


@app.get("/v1/runs")
@limiter.limit("30/minute")
def v1_list_runs(request: Request, limit: int = 20, current_user: str = Depends(get_current_user)):
    """Return recent thesis generation runs."""
    import db as _db

    return {"runs": _db.list_thesis_runs(limit)}


@app.get("/v1/runs/{run_id}")
@limiter.limit("60/minute")
def v1_get_run(request: Request, run_id: str, current_user: str = Depends(get_current_user)):
    """Poll the status of a thesis generation run."""
    import db as _db

    run = _v1_runs.get(run_id)
    if not run:
        run = _db.get_thesis_run(run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")
    return run


@app.get("/v1/thesis/{ticker}/latest")
@limiter.limit("30/minute")
def v1_latest_thesis(request: Request, ticker: str, current_user: str = Depends(get_current_user)):
    """Return the latest InvestmentThesis for a ticker."""
    import db as _db
    symbol = _validate_ticker(ticker)
    thesis = _db.get_latest_thesis(symbol)
    if not thesis:
        raise HTTPException(status_code=404, detail=f"No thesis found for {ticker}. POST /v1/runs to generate one.")
    data = thesis.model_dump(mode="json")
    ticker_info = _db.get_ticker_info(symbol)
    data["company_name"] = (
        (ticker_info or {}).get("company_name")
        or TICKER_NAMES.get(symbol)
        or symbol
    )
    return data


@app.get("/v1/thesis/{ticker}/history")
@limiter.limit("20/minute")
def v1_thesis_history(request: Request, ticker: str, limit: int = 10, current_user: str = Depends(get_current_user)):
    """Return recent thesis snapshots for a ticker (newest first)."""
    import db as _db
    symbol = _validate_ticker(ticker)
    return {"ticker": symbol, "theses": _db.get_thesis_history(symbol, limit)}


@app.get("/v1/thesis/scores")
@limiter.limit("30/minute")
def v1_thesis_scores(request: Request, tickers: str = "", current_user: str = Depends(get_current_user)):
    """Return {ticker: composite_score} for the latest thesis of each requested ticker.
    tickers= is a comma-separated list; omit to use the full watchlist."""
    import db as _db
    if tickers.strip():
        ticker_list = [_validate_ticker(t.strip()) for t in tickers.split(",") if t.strip()]
    else:
        ticker_list = load_watchlist()
    return _db.get_latest_scores(ticker_list)


@app.get("/v1/thesis/{ticker}/export.pdf")
@limiter.limit("10/minute")
def v1_thesis_pdf(request: Request, ticker: str, current_user: str = Depends(get_current_user)):
    """Render the latest InvestmentThesis as a downloadable PDF."""
    import db as _db
    import thesis_pdf as _pdf
    from fastapi.responses import Response
    symbol = _validate_ticker(ticker)
    thesis = _db.get_latest_thesis(symbol)
    if not thesis:
        raise HTTPException(status_code=404, detail=f"No thesis found for {symbol}")
    pdf_bytes = _pdf.build_pdf(thesis)
    return Response(
        content=pdf_bytes,
        media_type="application/pdf",
        headers={"Content-Disposition": f"attachment; filename=thesis_{symbol}.pdf"},
    )


@app.get("/v1/thesis/id/{thesis_id}")
@limiter.limit("30/minute")
def v1_thesis_by_id(request: Request, thesis_id: str, current_user: str = Depends(get_current_user)):
    """Return a reproducible thesis snapshot by ID."""
    import db as _db
    thesis = _db.get_thesis_by_id(thesis_id)
    if not thesis:
        raise HTTPException(status_code=404, detail="Thesis not found")
    return thesis.model_dump(mode="json")


@app.get("/v1/thesis/compare")
@limiter.limit("10/minute")
def v1_thesis_compare(request: Request, tickers: str, current_user: str = Depends(get_current_user)):
    """Compare latest theses for a comma-separated list of tickers (max 10)."""
    import db as _db
    # Accept flexible separators: "AAPL,MSFT", "AAPL MSFT", "AAPL vs MSFT"
    raw_parts = re.split(r"\bVS\b|[,/\s]+", (tickers or "").upper())
    symbols = []
    for part in raw_parts:
        part = part.strip()
        if not part:
            continue
        symbols.append(_validate_ticker(part))
        if len(symbols) >= 10:
            break
    results = []
    for sym in symbols:
        thesis = _db.get_latest_thesis(sym)
        if thesis:
            results.append({
                "ticker": sym,
                "composite_score": thesis.composite_score,
                "risk_rating": thesis.risk_rating.value if hasattr(thesis.risk_rating, "value") else thesis.risk_rating,
                "evidence_quality": thesis.evidence_quality.value if hasattr(thesis.evidence_quality, "value") else thesis.evidence_quality,
                "current_price": thesis.current_price,
                "generated_at": thesis.generated_at.isoformat() if thesis.generated_at else None,
                "forecast_3m": thesis.forecast.get("3m", {}).model_dump() if thesis.forecast and thesis.forecast.get("3m") else None,
                "forecast_12m": thesis.forecast.get("12m", {}).model_dump() if thesis.forecast and thesis.forecast.get("12m") else None,
            })
        else:
            results.append({"ticker": sym, "error": "No thesis found"})
    return {"tickers": symbols, "comparison": results}


@app.get("/v1/thesis/compare/export.pdf")
@limiter.limit("10/minute")
def v1_thesis_compare_pdf(request: Request, tickers: str, current_user: str = Depends(get_current_user)):
    """Export side-by-side compare PDF for multiple tickers."""
    import db as _db
    import thesis_pdf as _pdf
    from fastapi.responses import Response
    raw_parts = re.split(r"\bVS\b|[,/\s]+", (tickers or "").upper())
    symbols = []
    for part in raw_parts:
        part = part.strip()
        if not part:
            continue
        symbols.append(_validate_ticker(part))
        if len(symbols) >= 10:
            break
    theses = []
    for sym in symbols:
        t = _db.get_latest_thesis(sym)
        if t:
            theses.append(t)
    if len(theses) < 2:
        raise HTTPException(status_code=400, detail="Need at least 2 tickers with thesis data to export compare PDF")
    pdf_bytes = _pdf.build_compare_pdf(theses)
    joined = "_".join([t.ticker for t in theses[:4]])
    return Response(
        content=pdf_bytes,
        media_type="application/pdf",
        headers={"Content-Disposition": f"attachment; filename=compare_{joined}.pdf"},
    )


@app.get("/v1/settings/scheduler")
@limiter.limit("30/minute")
def v1_get_scheduler_settings(request: Request, current_user: str = Depends(get_current_user)):
    """Return current scheduler configuration."""
    import scheduler_settings as _ss
    return _ss.load()


@app.patch("/v1/settings/scheduler")
@limiter.limit("20/minute")
def v1_patch_scheduler_settings(request: Request, body: dict, current_user: str = Depends(get_current_user)):
    """Update scheduler settings and apply to the running scheduler immediately."""
    import scheduler_settings as _ss
    allowed = {
        "thesis_auto_run_enabled", "thesis_auto_run_interval_minutes",
        "thesis_auto_run_max_tickers", "evaluation_auto_run_enabled",
        "evaluation_auto_run_interval_minutes",
        "prediction_auto_run_enabled", "prediction_auto_run_interval_minutes",
        "monitor_auto_run_enabled", "monitor_auto_run_interval_minutes",
    }
    patch = {k: v for k, v in body.items() if k in allowed}
    if not patch:
        raise HTTPException(status_code=400, detail="No valid settings fields provided")
    _ss.save(patch)
    cfg = _ss.load()

    # Apply thesis scheduler changes live
    if "thesis_auto_run_enabled" in patch or "thesis_auto_run_interval_minutes" in patch:
        try:
            scheduler.remove_job("multiagent_thesis")
        except Exception:
            pass
        if cfg["thesis_auto_run_enabled"]:
            scheduler.add_job(
                auto_thesis, "interval",
                minutes=cfg["thesis_auto_run_interval_minutes"],
                id="multiagent_thesis", max_instances=1, coalesce=True,
            )
        thesis_scheduler_status["enabled"] = cfg["thesis_auto_run_enabled"]

    # Apply evaluation scheduler changes live
    if "evaluation_auto_run_enabled" in patch or "evaluation_auto_run_interval_minutes" in patch:
        try:
            scheduler.remove_job("forecast_evaluation")
        except Exception:
            pass
        if cfg["evaluation_auto_run_enabled"]:
            scheduler.add_job(
                auto_evaluate, "interval",
                minutes=cfg["evaluation_auto_run_interval_minutes"],
                id="forecast_evaluation", max_instances=1, coalesce=True,
            )
        evaluation_scheduler_status["enabled"] = cfg["evaluation_auto_run_enabled"]

    if "prediction_auto_run_enabled" in patch or "prediction_auto_run_interval_minutes" in patch:
        try:
            scheduler.remove_job("predictions")
        except Exception:
            pass
        if cfg["prediction_auto_run_enabled"]:
            scheduler.add_job(
                auto_predict, "interval",
                minutes=cfg["prediction_auto_run_interval_minutes"],
                id="predictions", max_instances=1, coalesce=True,
            )
        prediction_scheduler_status["enabled"] = cfg["prediction_auto_run_enabled"]

    if "monitor_auto_run_enabled" in patch or "monitor_auto_run_interval_minutes" in patch:
        try:
            scheduler.remove_job("monitor")
        except Exception:
            pass
        if cfg["monitor_auto_run_enabled"]:
            scheduler.add_job(
                monitor_stocks, "interval",
                minutes=cfg["monitor_auto_run_interval_minutes"],
                id="monitor", max_instances=1, coalesce=True,
            )
        monitor_scheduler_status["enabled"] = cfg["monitor_auto_run_enabled"]

    return {"status": "ok", "settings": cfg}


@app.get("/v1/agents/health")
@limiter.limit("20/minute")
def v1_agents_health(request: Request, current_user: str = Depends(get_current_user)):
    """Return agent health, last run times and stale status."""
    import observability
    return observability.agent_health_report()


@app.get("/v1/operations/status")
@limiter.limit("20/minute")
def v1_operations_status(request: Request, current_user: str = Depends(get_current_user)):
    """Return a production operations snapshot for the multi-agent pipeline."""
    import observability

    return observability.operations_status(
        thesis_scheduler=thesis_scheduler_status,
        evaluation_scheduler=evaluation_scheduler_status,
        prediction_scheduler=prediction_scheduler_status,
        monitor_scheduler=monitor_scheduler_status,
        recent_run_limit=10,
    )


@app.get("/v1/metrics/latest")
@limiter.limit("30/minute")
def v1_metrics_latest(
    request: Request,
    limit: int = 100,
    metric: str | None = None,
    current_user: str = Depends(get_current_user),
):
    """Return the most recent structured metric entries from the in-process buffer."""
    import observability
    return {
        "metrics": observability.get_recent_metrics(limit=limit, metric=metric or None),
        "buffer_size": observability._METRICS_BUFFER_SIZE,
    }


@app.get("/v1/thesis/{ticker}/quality")
@limiter.limit("20/minute")
def v1_thesis_quality(request: Request, ticker: str, current_user: str = Depends(get_current_user)):
    """Return quality flag summary for the latest thesis of a ticker."""
    import observability
    return observability.thesis_quality_summary(ticker.upper())


@app.get("/v1/backtest/{ticker}")
@limiter.limit("20/minute")
def v1_backtest(request: Request, ticker: str, current_user: str = Depends(get_current_user)):
    """Return forecast accuracy metrics for a ticker."""
    import db as _db
    import evaluation

    symbol = _validate_ticker(ticker)
    return {
        "ticker": symbol,
        "summary": _db.get_backtest_summary(symbol),
        "calibration": evaluation.confidence_calibration(symbol),
    }


@app.get("/v1/evaluate/status")
@limiter.limit("20/minute")
def v1_evaluate_status(
    request: Request,
    ticker: str | None = None,
    current_user: str = Depends(get_current_user),
):
    """Return forecast outcome evaluation status."""
    import db as _db

    symbol = _validate_ticker(ticker) if ticker else None
    return {
        "scheduler": evaluation_scheduler_status,
        "outcomes": _db.get_forecast_outcome_status(symbol),
    }


@app.post("/v1/evaluate")
@limiter.limit("5/hour")
async def v1_evaluate(
    request: Request,
    background_tasks: BackgroundTasks,
    sync: bool = False,
    current_user: str = Depends(get_current_user),
):
    """Trigger evaluation of matured forecast outcomes."""
    if sync:
        n = await run_evaluation_job("manual_sync")
        return {"ok": True, "evaluated": n, "scheduler": evaluation_scheduler_status}

    background_tasks.add_task(run_evaluation_job, "manual")
    return {"ok": True, "message": "Evaluation job started in background"}


@app.get("/v1/learning/summary")
@limiter.limit("30/minute")
def v1_learning_summary(
    request: Request,
    window_days: int = 90,
    current_user: str = Depends(get_current_user),
):
    """Return per-agent accuracy stats and score-bucket performance for the learning system."""
    import agent_accuracy as _aa
    window_days = max(7, min(int(window_days), 365))
    return _aa.get_learning_summary(window_days=window_days)


@app.post("/v1/learning/rebuild")
@limiter.limit("5/hour")
async def v1_learning_rebuild(
    request: Request,
    background_tasks: BackgroundTasks,
    current_user: str = Depends(get_current_user),
):
    """Trigger a full rebuild of agent accuracy stats (all windows)."""
    import agent_accuracy as _aa
    background_tasks.add_task(_aa.rebuild_all)
    return {"ok": True, "message": "Agent accuracy rebuild started in background"}


@app.get("/v1/learning/weights")
@limiter.limit("30/minute")
def v1_learning_weights(
    request: Request,
    current_user: str = Depends(get_current_user),
):
    """Return current agent weights vs defaults, with per-agent delta info."""
    import agent_accuracy as _aa
    return _aa.get_weight_status()


@app.post("/v1/learning/weights/recalibrate")
@limiter.limit("5/hour")
async def v1_learning_weights_recalibrate(
    request: Request,
    background_tasks: BackgroundTasks,
    current_user: str = Depends(get_current_user),
):
    """Manually trigger a weight recalibration cycle (same as the Monday 02:00 job)."""
    import agent_accuracy as _aa
    background_tasks.add_task(_aa.apply_weight_adjustments)
    return {"ok": True, "message": "Weight recalibration started in background"}


@app.post("/v1/learning/weights/reset")
@limiter.limit("5/hour")
def v1_learning_weights_reset(
    request: Request,
    current_user: str = Depends(get_current_user),
):
    """Reset HORIZON_WEIGHTS to factory defaults."""
    import agent_accuracy as _aa
    _aa.reset_to_default_weights()
    return {"ok": True, "message": "Agent weights reset to defaults", "weights": _aa.get_weight_status()}


# ── Earnings Intelligence endpoints ───────────────────────────────────────────

@app.get("/v1/earnings/recent")
@limiter.limit("30/minute")
def v1_earnings_recent(
    request: Request,
    ticker: Optional[str] = None,
    days: int = 14,
    current_user: str = Depends(get_current_user),
):
    """Return recent earnings events with LLM analysis. ticker=None returns all watchlist events."""
    import db as _db
    if ticker:
        try:
            ticker = _validate_ticker(ticker)
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid ticker")
    days = max(1, min(int(days), 90))
    events = _db.get_recent_earnings_events(ticker, days=days)
    result = []
    for ev in events:
        analysis = {}
        try:
            analysis = json.loads(ev.get("analysis_json") or "{}")
        except Exception:
            pass
        result.append({
            "event_id": ev["event_id"],
            "ticker": ev["ticker"],
            "company_name": ev.get("company_name"),
            "report_date": ev.get("report_date"),
            "beat_miss": ev.get("beat_miss", "UNKNOWN"),
            "eps_actual": ev.get("eps_actual"),
            "eps_estimate": ev.get("eps_estimate"),
            "eps_surprise_pct": ev.get("eps_surprise_pct"),
            "revenue_actual": ev.get("revenue_actual"),
            "revenue_estimate": ev.get("revenue_estimate"),
            "guidance": ev.get("guidance", "UNKNOWN"),
            "thesis_impact": ev.get("thesis_impact", "NEUTRAL"),
            "pre_market_headline": analysis.get("pre_market_headline"),
            "thesis_reasoning": analysis.get("thesis_reasoning"),
            "key_highlights": analysis.get("key_highlights", []),
            "cross_sector_impacts": analysis.get("cross_sector_impacts", []),
            "press_release_url": ev.get("press_release_url"),
            "detected_at": ev.get("detected_at"),
            "analysed_at": ev.get("analysed_at"),
        })
    return {"events": result, "count": len(result)}


@app.get("/v1/earnings/calendar")
@limiter.limit("10/minute")
async def v1_earnings_calendar(
    request: Request,
    days_ahead: int = 7,
    current_user: str = Depends(get_current_user),
):
    """Return upcoming earnings dates for watchlist tickers."""
    import earnings_watcher as _ew
    days_ahead = max(1, min(int(days_ahead), 30))
    now = datetime.now(timezone.utc)
    entry = _earnings_calendar_cache.get(days_ahead)
    if entry and (now - entry["fetched_at"]).total_seconds() < _EARNINGS_CALENDAR_TTL:
        return {
            "calendar": entry["data"],
            "count": len(entry["data"]),
            "days_ahead": days_ahead,
            "cached": True,
            "cached_at": entry["fetched_at"].isoformat(),
        }
    watchlist = load_watchlist()
    loop = asyncio.get_event_loop()
    upcoming = await loop.run_in_executor(
        None, lambda: _ew.get_upcoming_earnings(watchlist, days_ahead=days_ahead)
    )
    _earnings_calendar_cache[days_ahead] = {"data": upcoming, "fetched_at": now}
    return {
        "calendar": upcoming,
        "count": len(upcoming),
        "days_ahead": days_ahead,
        "cached": False,
        "cached_at": now.isoformat(),
    }


@app.post("/v1/earnings/check")
@limiter.limit("5/hour")
async def v1_earnings_check(
    request: Request,
    background_tasks: BackgroundTasks,
    current_user: str = Depends(get_current_user),
):
    """Manually trigger an earnings filing check for the watchlist."""
    background_tasks.add_task(auto_check_earnings)
    return {"ok": True, "message": "Earnings check started in background"}


@app.post("/v1/earnings/test-notification")
@limiter.limit("5/hour")
def v1_earnings_test_notification(
    request: Request,
    current_user: str = Depends(get_current_user),
):
    """Send a mock earnings notification to verify WhatsApp + email are wired up correctly."""
    twilio_sid  = os.getenv("TWILIO_ACCOUNT_SID", "")
    twilio_tok  = os.getenv("TWILIO_AUTH_TOKEN", "")
    twilio_from = os.getenv("TWILIO_FROM_NUMBER", "")
    twilio_to   = os.getenv("TWILIO_TO_NUMBER", "")
    missing_twilio = [k for k, v in {
        "TWILIO_ACCOUNT_SID": twilio_sid,
        "TWILIO_AUTH_TOKEN":  twilio_tok,
        "TWILIO_FROM_NUMBER": twilio_from,
        "TWILIO_TO_NUMBER":   twilio_to,
    }.items() if not v]

    smtp_user   = os.getenv("SMTP_USER", "")
    smtp_pass   = os.getenv("SMTP_PASS", "")
    alert_email = os.getenv("ALERT_EMAIL", "")
    missing_email = [k for k, v in {
        "SMTP_USER":   smtp_user,
        "SMTP_PASS":   smtp_pass,
        "ALERT_EMAIL": alert_email,
    }.items() if not v]

    try:
        from sentiment_agent import send_whatsapp as _wa
    except Exception:
        _wa = None

    msg = (
        "🧪 [StockLens] Earnings Test Notification\n\n"
        "🔔 [ORCL] Earnings: BEAT\n"
        "EPS: $1.73 vs $1.58 est (+9.6%)\n"
        "Rev: $15.9B vs $15.1B est (+5.3%)\n"
        "Guidance: RAISED | Signal: POSITIVE ✅\n\n"
        "📊 Sentiment: Bullish (82/100) 📈\n"
        "Up 7.1% AH — strong cloud revenue beat driving after-hours rally.\n"
        "Market pricing in guidance raise as durable.\n\n"
        "Cross-sector: MSFT (POSITIVE) — validates cloud demand; "
        "SAP (NEUTRAL) — limited overlap.\n\n"
        "This is a test — no real filing detected."
    )

    wa_sent = False
    wa_error = None
    if missing_twilio:
        wa_error = f"Missing Fly secrets: {', '.join(missing_twilio)}"
    elif _wa:
        try:
            wa_sent = bool(_wa(msg))
            if not wa_sent:
                wa_error = (
                    "send_whatsapp returned False — check Twilio logs and ensure "
                    "your phone number has joined the sandbox by messaging "
                    "+14155238886 with your sandbox keyword"
                )
        except Exception as exc:
            wa_error = str(exc)

    email_sent = False
    email_error = None
    if missing_email:
        email_error = f"Missing Fly secrets: {', '.join(missing_email)}"
    else:
        email_sent = send_email(
            subject="[StockLens] Earnings Test Notification — ORCL BEAT ✅",
            body=msg,
        )
        if not email_sent:
            email_error = "send_email returned False — check SMTP credentials"

    result: dict = {
        "ok": True,
        "whatsapp_sent": wa_sent,
        "email_sent": email_sent,
        "message": msg,
    }
    if wa_error:
        result["whatsapp_error"] = wa_error
    if email_error:
        result["email_error"] = email_error
    if missing_twilio or missing_email:
        result["setup_guide"] = (
            "Set missing secrets on Fly with: "
            "flyctl secrets set TWILIO_ACCOUNT_SID=ACxxx TWILIO_AUTH_TOKEN=xxx "
            "TWILIO_FROM_NUMBER='whatsapp:+14155238886' "
            "TWILIO_TO_NUMBER='whatsapp:+44XXXXXXXXX' "
            "-a stock-picker-sp"
        )
    return result


@app.get("/api/earnings/{ticker}/report")
@limiter.limit("10/hour")
async def get_earnings_report(
    request: Request,
    ticker: str,
    force: bool = False,
    current_user: str = Depends(get_current_user),
):
    """Deep post-earnings analyst report for a ticker.

    Fetches 4 quarters of financials, price-action technicals, and market
    sentiment from yfinance, then synthesises an institutional-style narrative
    using Claude Sonnet. Results are cached in-memory for 4 hours.

    Pass ?force=true to bypass the cache and regenerate.
    """
    import earnings_report as _er
    symbol = _validate_ticker(ticker)
    loop = asyncio.get_event_loop()
    try:
        report = await loop.run_in_executor(None, lambda: _er.generate_report(symbol, force=force))
        return report
    except Exception as exc:
        logger.exception("[earnings_report] %s failed: %s", symbol, exc)
        raise HTTPException(status_code=500, detail="Earnings report generation failed")


async def _init_multiagent_db():
    """Initialise the SQLite database for the multi-agent system."""
    try:
        import sys, os
        backend_dir = os.path.dirname(__file__)
        if backend_dir not in sys.path:
            sys.path.insert(0, backend_dir)
        import db as _db
        _db.init_db()
        logger.info("[v1] Multi-agent SQLite DB initialised")
    except Exception as exc:
        logger.error("[v1] DB init failed: %s", exc)
        return
    try:
        import agent_accuracy as _aa
        loaded = _aa.load_calibrated_weights()
        if loaded:
            logger.info("[v1] Calibrated agent weights loaded from DB")
    except Exception as exc:
        logger.warning("[v1] Could not load calibrated weights: %s", exc)
