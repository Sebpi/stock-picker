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
from datetime import date, datetime, time, timedelta, timezone
import zoneinfo
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from typing import Optional

import anthropic
import httpx
import yfinance as yf
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from dotenv import load_dotenv
from fastapi import BackgroundTasks, Depends, FastAPI, File, HTTPException, Request, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.staticfiles import StaticFiles
from starlette.middleware.trustedhost import TrustedHostMiddleware
import bcrypt as _bcrypt
from jose import JWTError, jwt
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
limiter = Limiter(key_func=get_remote_address)

# ── Account lockout ────────────────────────────────────────────────────────────
_MAX_ATTEMPTS   = 5
_LOCKOUT_MINS   = 15
_failed_logins: dict[str, list] = {}   # username -> list of attempt datetimes
_lockout_until: dict[str, datetime] = {}

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

def _clear_failed_logins(username: str) -> None:
    _failed_logins.pop(username, None)
    _lockout_until.pop(username, None)

# ── Auth setup ─────────────────────────────────────────────────────────────────
_SECRET_KEY = os.getenv("SECRET_KEY")
if not _SECRET_KEY:
    logger.warning("SECRET_KEY is not set; using an ephemeral key. Sessions will be invalidated on restart.")
    _SECRET_KEY = secrets.token_hex(32)
_ALGORITHM  = "HS256"
_TOKEN_HOURS = 24

def _hash_pw(password: str) -> str:
    return _bcrypt.hashpw(password.encode(), _bcrypt.gensalt(rounds=12)).decode()

def _verify_pw(password: str, hashed: str) -> bool:
    return _bcrypt.checkpw(password.encode(), hashed.encode())

http_bearer = HTTPBearer(auto_error=False)
USERS_FILE  = Path(__file__).parent / "users.json"

# In-memory password reset tokens: token -> (username, expiry)
_reset_tokens: dict[str, tuple[str, datetime]] = {}

# Auth public routes — no JWT required
_AUTH_PUBLIC = {"/api/auth/login", "/api/auth/forgot-password", "/api/auth/reset-password", "/api/auth/unlock-test", "/api/alerts/log"}
ANTHROPIC_MODEL = os.getenv("ANTHROPIC_MODEL", "claude-haiku-4-5-20251001").strip() or "claude-haiku-4-5-20251001"
STOCK_RESEARCH_MAX_TOKENS = max(256, int(os.getenv("STOCK_RESEARCH_MAX_TOKENS", "3000")))
RECOMMEND_MAX_TOKENS = max(256, int(os.getenv("RECOMMEND_MAX_TOKENS", "800")))
SEARCH_RESULTS_LIMIT = max(1, int(os.getenv("SEARCH_RESULTS_LIMIT", "12")))
SEARCH_INFO_TIMEOUT_SEC = max(0.5, float(os.getenv("SEARCH_INFO_TIMEOUT_SEC", "2.5")))
RECOMMENDATION_INFO_TIMEOUT_SEC = max(1.0, float(os.getenv("RECOMMENDATION_INFO_TIMEOUT_SEC", "4.0")))
RECOMMENDATION_HISTORY_TIMEOUT_SEC = max(1.0, float(os.getenv("RECOMMENDATION_HISTORY_TIMEOUT_SEC", "4.0")))
PREDICTIONS_INCLUDE_STOCK_RESEARCH = os.getenv("PREDICTIONS_INCLUDE_STOCK_RESEARCH", "false").lower() in {"1", "true", "yes", "on"}
PREDICTIONS_UNIVERSE_FILL_LIMIT = max(0, int(os.getenv("PREDICTIONS_UNIVERSE_FILL_LIMIT", "6")))
PREDICTIONS_MAX_TOKENS = max(512, int(os.getenv("PREDICTIONS_MAX_TOKENS", "8192")))

def load_users() -> dict:
    if USERS_FILE.exists():
        return json.loads(USERS_FILE.read_text())
    return {}

def _atomic_write(path: Path, data: str) -> None:
    """Write data to a temp file then atomically replace the target."""
    last_error = None
    for attempt in range(4):
        fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=".tmp_")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                f.write(data)
            os.replace(tmp, path)
            return
        except PermissionError as exc:
            last_error = exc
            try:
                os.unlink(tmp)
            except OSError:
                pass
            time_mod.sleep(0.15 * (attempt + 1))
        except Exception:
            try:
                os.unlink(tmp)
            except OSError:
                pass
            raise

    if last_error is not None:
        try:
            path.write_text(data, encoding="utf-8")
            return
        except Exception:
            raise last_error

def save_users(users: dict):
    _atomic_write(USERS_FILE, json.dumps(users, indent=2))

def create_access_token(username: str) -> str:
    expire = datetime.now(timezone.utc) + timedelta(hours=_TOKEN_HOURS)
    return jwt.encode({"sub": username, "exp": expire}, _SECRET_KEY, algorithm=_ALGORITHM)

async def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(http_bearer)) -> str:
    exc = HTTPException(status_code=401, detail="Not authenticated")
    if not credentials:
        raise exc
    try:
        payload = jwt.decode(credentials.credentials, _SECRET_KEY, algorithms=[_ALGORITHM])
        username: str = payload.get("sub", "")
        if not username or username not in load_users():
            raise exc
        return username
    except JWTError:
        raise exc

app = FastAPI(title="StockLens API")
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
    if is_secure:
        response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
    response.headers["Content-Security-Policy"] = (
        "default-src 'self'; "
        "script-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net; "
        "style-src 'self' 'unsafe-inline' https://fonts.googleapis.com https://cdn.jsdelivr.net; "
        "connect-src 'self' https://cdn.jsdelivr.net; "
        "img-src 'self' data: https:; "
        "font-src 'self' data: https://fonts.gstatic.com https://cdn.jsdelivr.net; "
        "worker-src blob:; "
        "frame-ancestors 'none';"
    )
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
    # Allow static files, HTML root, and public auth endpoints
    if not path.startswith("/api/") or path in _AUTH_PUBLIC:
        return await call_next(request)
    auth_header = request.headers.get("Authorization", "")
    if not auth_header.startswith("Bearer "):
        return JSONResponse(status_code=401, content={"detail": "Not authenticated"})
    token = auth_header[7:]
    try:
        payload = jwt.decode(token, _SECRET_KEY, algorithms=[_ALGORITHM])
        username = payload.get("sub", "")
        if not username or username not in load_users():
            return JSONResponse(status_code=401, content={"detail": "Not authenticated"})
    except JWTError:
        return JSONResponse(status_code=401, content={"detail": "Invalid or expired token"})
    return await call_next(request)

FRONTEND_DIR = Path(__file__).parent.parent / "frontend"

@app.get("/", include_in_schema=False)
def serve_index():
    return FileResponse(
        FRONTEND_DIR / "index.html",
        headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
    )

@app.get("/static/{filename:path}", include_in_schema=False)
def serve_static(filename: str):
    file_path = FRONTEND_DIR / filename
    if not file_path.exists():
        raise HTTPException(status_code=404)
    return FileResponse(
        file_path,
        headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
    )

WATCHLIST_FILE       = Path(__file__).parent / "watchlist.json"
PREDICTIONS_FILE     = Path(__file__).parent / "predictions.json"
ALERTS_FILE          = Path(__file__).parent / "alerts.json"
PORTFOLIO_FILE       = Path(__file__).parent / "portfolio.json"
SETTINGS_FILE        = Path(__file__).parent / "settings.json"
PAPER_PORTFOLIO_FILE = Path(__file__).parent / "paper_portfolio.json"
ALERT_COOLDOWN_FILE  = Path(__file__).parent / "alert_cooldown_state.json"

PAPER_INITIAL_FLOAT = 200_000.0


def get_paper_initial_float() -> float:
    try:
        return float(load_settings().get("initial_float", PAPER_INITIAL_FLOAT))
    except Exception:
        return PAPER_INITIAL_FLOAT

def load_paper_portfolio() -> list[dict]:
    if PAPER_PORTFOLIO_FILE.exists():
        return json.loads(PAPER_PORTFOLIO_FILE.read_text())
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
    }
    if SETTINGS_FILE.exists():
        return {**defaults, **json.loads(SETTINGS_FILE.read_text())}
    return defaults

def save_settings(s: dict):
    _atomic_write(SETTINGS_FILE, json.dumps(s, indent=2))

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

FTSE250_TICKERS = [
    "AJB.L", "BBOX.L", "BEZ.L", "BOWL.L", "BWY.L", "CARD.L", "CCC.L",
    "COA.L", "CTEC.L", "DARK.L", "DFS.L", "DLG.L", "DNLM.L", "DOM.L",
    "DOCS.L", "EMG.L", "ENOG.L", "FGP.L", "GRG.L", "HAS.L", "HFD.L",
    "HTG.L", "IBST.L", "INCH.L", "ITV.L", "JDW.L", "JET2.L", "KIE.L",
    "LRE.L", "MGNS.L", "MSLH.L", "MOON.L", "NCC.L", "OSB.L", "PAGE.L",
    "PETS.L", "PLUS.L", "PFD.L", "RSW.L", "SAFE.L", "SCT.L", "SRP.L",
    "SXS.L", "TRN.L", "TLW.L", "VCT.L", "VTY.L", "VSVS.L", "WKP.L", "WOSG.L",
]

FTSE100_TICKERS = [
    "AAL.L", "ABF.L", "ADM.L", "AHT.L", "ANTO.L", "AUTO.L", "AV.L", "AZN.L",
    "BA.L", "BARC.L", "BATS.L", "BP.L", "BT-A.L", "CCEP.L", "CPG.L", "CRDA.L",
    "DGE.L", "ENT.L", "EXPN.L", "FLTR.L", "FRES.L", "GSK.L", "HLMA.L", "HSBA.L",
    "HSX.L", "IMB.L", "INF.L", "III.L", "IHG.L", "ITRK.L", "JD.L", "KGF.L",
    "LAND.L", "LGEN.L", "LLOY.L", "LSEG.L", "MKS.L", "NG.L", "NWG.L", "OCDO.L",
    "PHNX.L", "PRU.L", "PSH.L", "PSON.L", "REL.L", "RIO.L", "RKT.L", "RMV.L",
    "RR.L", "SBRY.L", "SDR.L", "SHEL.L", "SMIN.L", "SMT.L", "SN.L", "SPX.L",
    "SSE.L", "STAN.L", "SVT.L", "TSCO.L", "ULVR.L", "UU.L", "VOD.L", "WEIR.L",
    "WTB.L",
]

SCREENER_EXTRA_TICKERS = [
    "TSM", "BE", "ASML", "NVO", "SAP", "SHOP", "ARM", "RYCEY", "RKLB",
]

UNIVERSE = list(dict.fromkeys(
    SP500_TICKERS + NASDAQ100_TICKERS + FTSE100_TICKERS + FTSE250_TICKERS + SCREENER_EXTRA_TICKERS
))


_WIKI_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
}


def _fetch_sp500_from_wiki() -> list:
    try:
        import pandas as pd
        import requests
        html = requests.get(
            "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies",
            headers=_WIKI_HEADERS, timeout=15
        ).text
        tables = pd.read_html(io.StringIO(html), header=0)
        tickers = tables[0]["Symbol"].tolist()
        return [str(t).replace(".", "-") for t in tickers]
    except Exception as e:
        logger.warning(f"Failed to fetch S&P 500 from Wikipedia: {e}")
        return []


def _fetch_nasdaq100_from_wiki() -> list:
    try:
        import pandas as pd
        import requests
        html = requests.get(
            "https://en.wikipedia.org/wiki/Nasdaq-100",
            headers=_WIKI_HEADERS, timeout=15
        ).text
        tables = pd.read_html(io.StringIO(html), header=0)
        for table in tables:
            if "Ticker" in table.columns:
                return [str(t) for t in table["Ticker"].tolist()]
            if "Symbol" in table.columns:
                return [str(t) for t in table["Symbol"].tolist()]
        return []
    except Exception as e:
        logger.warning(f"Failed to fetch NASDAQ 100 from Wikipedia: {e}")
        return []


def _fetch_ftse100_from_wiki() -> list:
    try:
        import pandas as pd
        import requests
        html = requests.get(
            "https://en.wikipedia.org/wiki/FTSE_100_Index",
            headers=_WIKI_HEADERS, timeout=15
        ).text
        tables = pd.read_html(io.StringIO(html), header=0)
        for table in tables:
            cols = {str(c).strip().lower(): c for c in table.columns}
            symbol_col = cols.get("epic") or cols.get("ticker") or cols.get("symbol")
            if not symbol_col:
                continue
            tickers = []
            for raw in table[symbol_col].tolist():
                ticker = str(raw).strip().upper()
                if not ticker or ticker == "NAN":
                    continue
                if not ticker.endswith(".L"):
                    ticker = f"{ticker}.L"
                tickers.append(ticker)
            if tickers:
                return tickers
        return []
    except Exception as e:
        logger.warning(f"Failed to fetch FTSE 100 from Wikipedia: {e}")
        return []


def _normalize_search_text(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value or "").lower())

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
    "RR.L": "Rolls-Royce Holdings plc",
    "RYCEY": "Rolls-Royce Holdings plc ADR",
    "BT-A.L": "BT Group plc",
    "SHEL.L": "Shell plc",
}

SEARCH_ALIASES = {
    "TSMC": "TSM",
    "TAIWAN SEMICONDUCTOR": "TSM",
    "TAIWAN SEMICONDUCTOR MANUFACTURING": "TSM",
    "GOOGLE": "GOOGL",
    "FACEBOOK": "META",
    "BLOOM ENERGY": "BE",
    "BLOOM": "BE",
    "ROLLS ROYCE": "RR.L",
    "ROLLS-ROYCE": "RR.L",
    "ROLLSROYCE": "RR.L",
    "ROLLS ROYCE HOLDINGS": "RR.L",
    "ROLLS-ROYCE HOLDINGS": "RR.L",
    "RYCEY": "RYCEY",
    "RR.L": "RR.L",
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
monitor_status = {"last_check": None, "active": False, "checks_run": 0}
recommendation_alert_snapshot: dict[str, object] = {"generated_at": None, "data": None}


def _load_alert_cooldown_state() -> tuple[dict[str, datetime], dict[str, float]]:
    """Load persisted alert cooldown and price cache from disk."""
    if not ALERT_COOLDOWN_FILE.exists():
        return {}, {}
    try:
        raw = json.loads(ALERT_COOLDOWN_FILE.read_text(encoding="utf-8"))
        cooldown = {k: datetime.fromisoformat(v) for k, v in raw.get("cooldown", {}).items()}
        prices = {k: float(v) for k, v in raw.get("prices", {}).items()}
        return cooldown, prices
    except Exception:
        return {}, {}


def _save_alert_cooldown_state():
    try:
        data = {
            "cooldown": {k: v.isoformat() for k, v in alert_cooldown.items()},
            "prices": alert_price_cache,
        }
        _atomic_write(ALERT_COOLDOWN_FILE, json.dumps(data, indent=2))
    except Exception as e:
        logger.warning("Could not save alert cooldown state: %s", e)


_ac, _ap = _load_alert_cooldown_state()
alert_cooldown: dict[str, datetime] = _ac       # alert key -> last alert time
alert_price_cache: dict[str, float] = _ap       # "ACTION:ticker" -> price at last sent alert

# yfinance info cache — 5 minute TTL to avoid redundant network calls
_info_cache: dict[str, tuple[dict, datetime]] = {}
_INFO_TTL = 300  # seconds

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
        return json.loads(WATCHLIST_FILE.read_text())
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


def direction_from_score(score: Optional[float]) -> str:
    if score is None:
        return "neutral"
    if score >= 61:
        return "bullish"
    if score <= 39:
        return "bearish"
    return "neutral"


def refine_prediction_signal(
    raw_pct: Optional[float],
    raw_direction: str | None,
    score: Optional[float],
    confidence: str,
    stock_data: dict,
) -> tuple[str, float]:
    """
    Separate direction from magnitude so weak or conflicting signals default to neutral
    instead of producing spurious precise percentages.
    """
    llm_direction = (raw_direction or prediction_direction(raw_pct) or "neutral").lower()
    score_direction = direction_from_score(score)
    sentiment_score = float(stock_data.get("sentiment_score") or 0.0)
    factors = stock_data.get("factor_scores") or {}
    composite = float(factors.get("composite") or 50.0)
    momentum = float(factors.get("momentum") or 50.0)
    quality = float(factors.get("quality") or 50.0)
    vol_pct = float(stock_data.get("annualised_vol_pct") or 0.0)
    dte = stock_data.get("days_to_earnings")

    quant_direction = "neutral"
    if sentiment_score >= 0.35 and composite >= 55 and momentum >= 50:
        quant_direction = "bullish"
    elif sentiment_score <= -0.35 and composite <= 45 and momentum <= 50:
        quant_direction = "bearish"

    votes = [d for d in [llm_direction, score_direction, quant_direction] if d in {"bullish", "bearish", "neutral"}]
    bullish_votes = votes.count("bullish")
    bearish_votes = votes.count("bearish")
    neutral_votes = votes.count("neutral")

    final_direction = "neutral"
    if bullish_votes >= 2:
        final_direction = "bullish"
    elif bearish_votes >= 2:
        final_direction = "bearish"
    elif neutral_votes >= 2:
        final_direction = "neutral"
    elif llm_direction == score_direction and llm_direction in {"bullish", "bearish"}:
        final_direction = llm_direction

    # Force weaker calls to neutral around major uncertainty or poor quality.
    if quality < 35:
        final_direction = "neutral"
    if dte is not None and 0 <= int(dte) <= 7 and confidence == "low":
        final_direction = "neutral"

    if final_direction == "neutral":
        return "neutral", 0.0

    raw_magnitude = abs(float(raw_pct or 0.0))
    score_magnitude = 0.0 if score is None else min(2.5, abs(float(score) - 50.0) / 18.0)
    base_magnitude = max(raw_magnitude, score_magnitude)
    conf_cap = {"low": 0.6, "medium": 1.2, "high": 2.0}.get((confidence or "medium").lower(), 1.0)
    if composite >= 70 and momentum >= 60:
        conf_cap += 0.2
    if composite <= 40 or momentum <= 40:
        conf_cap = min(conf_cap, 0.8)
    if vol_pct >= 45:
        conf_cap = min(conf_cap, 0.8)
    if dte is not None and 0 <= int(dte) <= 7:
        conf_cap = min(conf_cap, 0.5)

    final_magnitude = min(max(base_magnitude, 0.35), conf_cap)
    signed_pct = final_magnitude if final_direction == "bullish" else -final_magnitude
    return final_direction, round(signed_pct, 2)

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

    horizons = prediction_horizon_returns(predicted_pct, direction, score, confidence)
    for key, value in horizons.items():
        if normalized.get(key) is None:
            normalized[key] = value
    return normalized


def get_sentiment_scanner_path() -> Path:
    configured = os.getenv("SENTIMENT_SCANNER_PATH", "").strip()
    if configured:
        return Path(configured).expanduser()
    return Path(__file__).parent / "sentiment_scanner.py"


def run_sentiment_scanner(ticker: Optional[str] = None, watchlist_only: bool = False) -> dict:
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
        return {"status": "ok", **parsed}
    except subprocess.TimeoutExpired:
        raise HTTPException(status_code=504, detail="Sentiment scan timed out")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Sentiment scan failed: {e}")


def load_predictions() -> list[dict]:
    if PREDICTIONS_FILE.exists():
        return json.loads(PREDICTIONS_FILE.read_text())
    return []

_predictions_cache: dict[str, object] = {
    "date": None,
    "mtime_ns": None,
    "data": None,
}
_screen_universe_cache: dict[str, tuple[list[dict], datetime]] = {}
_screen_loading: dict[str, bool] = {}          # pool_key -> True while background fetch is running
_screen_partial: dict[str, list[dict]] = {}    # pool_key -> partial rows accumulated so far


def invalidate_predictions_cache() -> None:
    _predictions_cache["date"] = None
    _predictions_cache["mtime_ns"] = None
    _predictions_cache["data"] = None


def save_predictions(predictions: list[dict]):
    invalidate_predictions_cache()
    _atomic_write(PREDICTIONS_FILE, json.dumps(predictions[-1000:], indent=2))


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

        # Sanity check 1: FCF/share must be positive and credible relative to price.
        # If FCF per share > 3x the current price the input data is almost certainly
        # distorted by one-off working capital movements or accounting items.
        fcf_per_share = fcf / shares
        if fcf_per_share > price * 3:
            return None

        # Sanity check 2: if the company is loss-making on a reported basis (negative EPS)
        # and FCF yield > 30% something unusual is inflating FCF — don't trust the DCF.
        trailing_eps = info.get("trailingEps") or 0
        fcf_yield = fcf / (price * shares) if price and shares else 0
        if trailing_eps < 0 and fcf_yield > 0.30:
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

        # Cap margin of safety display at ±150% — beyond that the inputs are too uncertain
        # to present a specific number without misleading the user
        if abs(mos_pct) > 150:
            return None

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
        return json.loads(ALERTS_FILE.read_text())
    return []

def save_alerts(alerts: list[dict]):
    _atomic_write(ALERTS_FILE, json.dumps(alerts, indent=2))

def append_alert(entry: dict):
    alerts = load_alerts()
    alerts.insert(0, entry)
    save_alerts(alerts[:500])  # keep latest 500


def load_portfolio() -> list[dict]:
    if PORTFOLIO_FILE.exists():
        return json.loads(PORTFOLIO_FILE.read_text())
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


def send_whatsapp_message(message: str) -> dict:
    try:
        from twilio.rest import Client
        account_sid = os.getenv("TWILIO_ACCOUNT_SID", "")
        auth_token  = os.getenv("TWILIO_AUTH_TOKEN", "")
        from_number = os.getenv("TWILIO_FROM_NUMBER", "")
        to_number   = os.getenv("TWILIO_TO_NUMBER", "")

        if not all([account_sid, auth_token, from_number, to_number]):
            return {"ok": False, "error": "Twilio WhatsApp is not fully configured."}

        if not from_number.startswith("whatsapp:"):
            from_number = f"whatsapp:{from_number}"
        if not to_number.startswith("whatsapp:"):
            to_number = f"whatsapp:{to_number}"

        client = Client(account_sid, auth_token)
        msg = client.messages.create(body=message[:1600], from_=from_number, to=to_number)
        return {
            "ok": True,
            "sid": getattr(msg, "sid", None),
            "status": getattr(msg, "status", None),
            "to": to_number,
            "from": from_number,
        }
    except Exception as e:
        print(f"[SMS] Failed: {e}")
        return {"ok": False, "error": str(e)}


def send_sms(message: str) -> bool:
    return bool(send_whatsapp_message(message).get("ok"))


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


async def _build_recommendation_alert_snapshot(buy_limit: int = 3, sell_limit: int = 3) -> dict:
    recommendations = await _build_recommendations()
    predictions = load_predictions()
    dated = [normalize_prediction(p) for p in predictions if p.get("score") is not None or p.get("predicted_pct") is not None]
    if not dated:
        return {"buys": [], "sells": []}

    latest_date = max(p["date"] for p in dated)
    latest_preds = [p for p in dated if p["date"] == latest_date]
    pred_map = {p["ticker"]: p for p in latest_preds}
    _s = load_settings()
    strong_buy_min_score = max(68, int(_s.get("alert_buy_min_score") or os.getenv("ALERT_BUY_MIN_SCORE", "72")))
    buy_min_score = max(55, strong_buy_min_score - 12)
    sell_max_score = min(50, int(_s.get("alert_sell_max_score") or os.getenv("ALERT_SELL_MAX_SCORE", "42")))

    strong_buys = []
    for rec in recommendations.get("buys", []):
        pred = pred_map.get(rec["ticker"], {})
        score_value = int(rec.get("score_value") or pred.get("score") or 0)
        confidence = str(rec.get("confidence") or pred.get("confidence") or "medium").lower()
        projected_12m = float(pred.get("predicted_12m_pct") or 0.0)
        projected_24m = float(pred.get("predicted_24m_pct") or 0.0)
        if score_value < strong_buy_min_score or confidence not in {"medium", "high"}:
            continue
        if projected_12m < 12 and projected_24m < 25:
            continue
        strong_buys.append({
            "ticker": rec["ticker"],
            "name": rec.get("name", rec["ticker"]),
            "action": "BUY",
            "type": "buy_opportunity",
            "trigger": rec.get("trigger", "BUY"),
            "signal": f"BUY signal strengthened: {rec.get('trigger', 'BUY')} with a {score_value}/100 conviction score.",
            "price": float(rec.get("current_price") or pred.get("price_at_prediction") or 0.0),
            "score_value": score_value,
            "confidence": confidence,
            "projected_12m_pct": projected_12m,
            "projected_24m_pct": projected_24m,
            "reasoning": rec.get("reasoning") or "Model score, confidence, and longer-horizon return profile all remain supportive.",
        })
    strong_buys.sort(key=lambda item: (item["score_value"], item["projected_12m_pct"], item["projected_24m_pct"]), reverse=True)

    if not strong_buys:
        fallback_buys = []
        for rec in recommendations.get("buys", []):
            pred = pred_map.get(rec["ticker"], {})
            score_value = int(rec.get("score_value") or pred.get("score") or 0)
            confidence = str(rec.get("confidence") or pred.get("confidence") or "medium").lower()
            projected_12m = float(pred.get("predicted_12m_pct") or 0.0)
            projected_24m = float(pred.get("predicted_24m_pct") or 0.0)
            if score_value < buy_min_score:
                continue
            if projected_12m < 8 and projected_24m < 18:
                continue
            fallback_buys.append({
                "ticker": rec["ticker"],
                "name": rec.get("name", rec["ticker"]),
                "action": "BUY",
                "type": "buy_opportunity",
                "trigger": rec.get("trigger", "BUY"),
                "signal": f"BUY candidate from the latest recommendation set: {rec.get('trigger', 'BUY')}.",
                "price": float(rec.get("current_price") or pred.get("price_at_prediction") or 0.0),
                "score_value": score_value,
                "confidence": confidence,
                "projected_12m_pct": projected_12m,
                "projected_24m_pct": projected_24m,
                "reasoning": rec.get("reasoning") or "This is the strongest currently available BUY candidate, even though the signal is not yet top-tier.",
            })
        fallback_buys.sort(key=lambda item: (item["score_value"], item["projected_12m_pct"], item["projected_24m_pct"]), reverse=True)
        strong_buys = fallback_buys[:buy_limit]

    owned_positions = {ticker: pos for ticker, pos in compute_positions(load_portfolio()).items() if pos.get("shares", 0) > 0}
    owned_tickers = list(owned_positions.keys())
    info_map: dict[str, dict] = {}
    hist_map: dict[str, object] = {}
    if owned_tickers:
        info_tasks = [asyncio.create_task(get_info_with_timeout(ticker, RECOMMENDATION_INFO_TIMEOUT_SEC)) for ticker in owned_tickers]
        info_results = await asyncio.gather(*info_tasks, return_exceptions=True)
        for ticker, info in zip(owned_tickers, info_results):
            if not isinstance(info, Exception):
                info_map[ticker] = info

        async def _fetch_hist_30d(ticker: str):
            try:
                hist = await asyncio.wait_for(
                    asyncio.to_thread(lambda: yf.Ticker(ticker).history(period="30d")),
                    timeout=RECOMMENDATION_HISTORY_TIMEOUT_SEC,
                )
                return ticker, hist
            except Exception:
                return ticker, None

        hist_results = await asyncio.gather(*[_fetch_hist_30d(ticker) for ticker in owned_tickers])
        for ticker, hist in hist_results:
            if hist is not None and not hist.empty:
                hist_map[ticker] = hist

    total_market_value = 0.0
    current_value_map: dict[str, float] = {}
    for ticker, pos in owned_positions.items():
        current_price = _price_from_info_for_alerts(info_map.get(ticker)) or pos.get("avg_cost", 0.0)
        current_value = float(pos.get("shares", 0.0)) * float(current_price or 0.0)
        current_value_map[ticker] = current_value
        total_market_value += current_value

    strong_sells = []
    for ticker, pos in owned_positions.items():
        pred = pred_map.get(ticker)
        if not pred:
            continue
        score_value = int(pred.get("score") or 0)
        confidence = str(pred.get("confidence") or "medium").lower()
        direction = pred.get("direction") or prediction_direction(pred.get("predicted_pct"))
        projected_12m = float(pred.get("predicted_12m_pct") or 0.0)
        projected_24m = float(pred.get("predicted_24m_pct") or 0.0)
        current_price = _price_from_info_for_alerts(info_map.get(ticker)) or pos.get("avg_cost", 0.0)
        cost_basis = float(pos.get("shares", 0.0)) * float(pos.get("avg_cost", 0.0))
        current_value = current_value_map.get(ticker, 0.0)
        unrealised_pct = ((current_value - cost_basis) / cost_basis * 100) if cost_basis else 0.0
        trigger = None
        reasoning = pred.get("reasoning") or ""
        severity = 0.0

        if direction == "bearish" and score_value <= sell_max_score and confidence in {"medium", "high"} and projected_12m <= -8:
            trigger = "MODEL SELL"
            severity = (50 - score_value) + abs(projected_12m)
            reasoning = reasoning or "The latest model snapshot has turned firmly bearish on a stock you hold."
        else:
            ticker_preds = sorted(
                [p for p in dated if p["ticker"] == ticker and p.get("date")],
                key=lambda item: item["date"],
                reverse=True,
            )
            if len(ticker_preds) >= 3 and all((p.get("direction") or prediction_direction(p.get("predicted_pct"))) == "bearish" for p in ticker_preds[:3]):
                trigger = "PERSISTENT BEARISH"
                severity = 18 + abs(projected_12m)
                reasoning = "The last three model snapshots for this holding have all stayed bearish."

        if not trigger:
            hist = hist_map.get(ticker)
            if hist is not None and len(hist) >= 20:
                sma_20 = float(hist["Close"].iloc[-20:].mean())
                if sma_20 > 0 and current_price < sma_20 * 0.97 and direction != "bullish":
                    trigger = "TECHNICAL BREAKDOWN"
                    severity = max(0.0, (1 - current_price / sma_20) * 100) + max(0.0, -projected_12m)
                    reasoning = "Price is trading below the 20-day trend while the model has stopped supporting the position."

        if not trigger and total_market_value > 0 and (current_value / total_market_value) > 0.25 and direction != "bullish":
            trigger = "CONCENTRATION"
            severity = (current_value / total_market_value) * 100
            reasoning = "This holding has grown into a large portfolio weight without a strong bullish model signal."

        if not trigger:
            continue

        strong_sells.append({
            "ticker": ticker,
            "name": pos.get("name", ticker),
            "action": "SELL",
            "type": "sell_signal",
            "trigger": trigger,
            "signal": f"SELL signal triggered: {trigger}.",
            "price": float(current_price or 0.0),
            "score_value": score_value,
            "confidence": confidence,
            "projected_12m_pct": projected_12m,
            "projected_24m_pct": projected_24m,
            "unrealised_pct": round(unrealised_pct, 2),
            "reasoning": reasoning,
            "severity": round(severity, 4),
        })
    strong_sells.sort(key=lambda item: (item["severity"], 100 - item["score_value"], -item["projected_12m_pct"]), reverse=True)

    return {
        "buys": strong_buys[:buy_limit],
        "sells": strong_sells[:sell_limit],
        "prediction_date": latest_date,
    }


# ── Email body builder ────────────────────────────────────────────────────────

def _build_alert_email(buy_alerts: list, sell_alerts: list, time_str: str, preview: bool = False) -> tuple[str, str]:
    """Return (subject, body) for a recommendation alert email."""
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

    def _short_reason(a: dict) -> str:
        """One-line reason: use trigger signal, else first sentence of reasoning."""
        trigger = (a.get("signals") or [{}])[0].get("signal", "").strip()
        if trigger:
            # Strip boilerplate prefixes like "BUY signal triggered: " / "SELL signal triggered: "
            trigger = trigger.split(": ", 1)[-1]
            return trigger[:80]
        reasoning = (a.get("reasoning") or "").strip()
        if reasoning:
            first = reasoning.split(".")[0].strip()
            return first[:80]
        return ""

    sms_parts = [f"StockPicker {time_str}"]
    for a in buy_alerts:
        line = f"▲ BUY {a['ticker']} {float(a.get('projected_12m_pct') or 0):+.1f}% 12M"
        reason = _short_reason(a)
        if reason:
            line += f"\n  {reason}"
        sms_parts.append(line)
    for a in sell_alerts:
        line = f"▼ SELL {a['ticker']} {float(a.get('projected_12m_pct') or 0):+.1f}% 12M"
        reason = _short_reason(a)
        if reason:
            line += f"\n  {reason}"
        sms_parts.append(line)

    return subject, "\n".join(body_lines), "\n".join(sms_parts)


# ── Stock monitoring (runs every 5 min) ───────────────────────────────────────

async def monitor_stocks():
    if not is_market_open():
        return
    now = datetime.now(timezone.utc)
    monitor_status["last_check"] = now.isoformat()
    monitor_status["checks_run"] += 1
    _s = load_settings()
    cooldown_hours = float(_s.get("alert_cooldown_hours") or int(os.getenv("ALERT_RECOMMENDATION_COOLDOWN_MINUTES", "720")) // 60)
    cooldown_minutes = int(cooldown_hours * 60)
    snapshot_ttl_minutes = int(os.getenv("ALERT_RECOMMENDATION_SNAPSHOT_MINUTES", "30"))
    buy_limit = max(1, int(_s.get("alert_top_buys") or os.getenv("ALERT_TOP_BUYS", "3")))
    sell_limit = max(1, int(_s.get("alert_top_sells") or os.getenv("ALERT_TOP_SELLS", "3")))

    generated_at = recommendation_alert_snapshot.get("generated_at")
    cached_data = recommendation_alert_snapshot.get("data")
    if (
        isinstance(generated_at, datetime)
        and cached_data
        and (now - generated_at).total_seconds() < snapshot_ttl_minutes * 60
    ):
        snapshot = cached_data
    else:
        snapshot = await _build_recommendation_alert_snapshot(buy_limit=buy_limit, sell_limit=sell_limit)
        recommendation_alert_snapshot["generated_at"] = now
        recommendation_alert_snapshot["data"] = snapshot

    price_swing_pct = float(_s.get("alert_price_swing_pct") or os.getenv("ALERT_PRICE_SWING_PCT", "3.0"))

    pending_alerts = []
    for item in snapshot.get("buys", []) + snapshot.get("sells", []):
        alert_key = f"{item.get('action', 'ALERT')}:{item['ticker']}:{item.get('trigger', '')}"
        last_alerted = alert_cooldown.get(alert_key)
        if last_alerted and (now - last_alerted).total_seconds() < cooldown_minutes * 60:
            continue

        # Skip if price hasn't moved enough since the last alert for this ticker+action
        ticker_action_key = f"{item.get('action', 'ALERT')}:{item['ticker']}"
        current_price = item.get("price", 0.0)
        last_price = alert_price_cache.get(ticker_action_key)
        if last_price and current_price > 0:
            swing_pct = abs(current_price - last_price) / last_price * 100
            if swing_pct < price_swing_pct:
                logger.debug(
                    "ALERT_SKIPPED ticker=%s action=%s current_price=%.4f last_price=%.4f swing_pct=%.2f threshold=%.1f",
                    item["ticker"], item.get("action"), current_price, last_price, swing_pct, price_swing_pct,
                )
                continue  # Don't update cooldown — re-evaluate next cycle

        alert_cooldown[alert_key] = now
        alert_price_cache[ticker_action_key] = current_price if current_price > 0 else (last_price or 0.0)
        pending_alerts.append(item)

    if pending_alerts:
        _save_alert_cooldown_state()
        time_str = now.astimezone(ET).strftime("%d %b %Y, %H:%M ET")
        buy_alerts  = [a for a in pending_alerts if a.get("action") == "BUY"]
        sell_alerts = [a for a in pending_alerts if a.get("action") == "SELL"]
        subject, body, sms_body = _build_alert_email(buy_alerts, sell_alerts, time_str)
        emailed = send_email(subject, body)
        texted  = send_sms(sms_body[:1600])

        channels = []
        if emailed:
            channels.append("Email")
        if texted:
            channels.append("WhatsApp")
        if not channels:
            channels.append("Log only")

        for alert in pending_alerts:
            action_label = alert.get("action", "ALERT")
            signal_text = alert.get("signal", "")
            message = f"{action_label}: {alert['ticker']} ({alert['name']}) — {signal_text}"
            trigger = alert.get("type", "recommendation")
            record = {
                "id": str(uuid.uuid4()),
                "timestamp": now.isoformat(),
                "ticker": alert["ticker"],
                "name": alert["name"],
                "price": alert["price"],
                "action": action_label,
                "message": message,
                "channels": channels,
                "trigger": trigger,
                "signals": [{
                    "type": alert.get("type"),
                    "signal": signal_text,
                    "change_pct": alert.get("projected_12m_pct"),
                }],
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


# ── App lifecycle ─────────────────────────────────────────────────────────────

async def auto_predict():
    """Refresh predictions every 15 mins during market hours."""
    if not is_market_open():
        return
    try:
        await _generate_predictions_impl()
        print("[Predictions] Auto-refreshed during market hours.")
    except Exception as e:
        print(f"[Predictions] Auto-refresh failed: {e}")


# ── Auth & trade request models ───────────────────────────────────────────────
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


@app.on_event("startup")
async def startup():
    # Refresh index constituent lists from Wikipedia
    global SP500_TICKERS, NASDAQ100_TICKERS, FTSE100_TICKERS, UNIVERSE
    loop = asyncio.get_event_loop()
    sp500   = await loop.run_in_executor(None, _fetch_sp500_from_wiki)
    nasdaq  = await loop.run_in_executor(None, _fetch_nasdaq100_from_wiki)
    ftse100 = await loop.run_in_executor(None, _fetch_ftse100_from_wiki)
    if sp500:
        SP500_TICKERS = sp500
        logger.info(f"S&P 500 tickers refreshed from Wikipedia ({len(sp500)} stocks)")
    if nasdaq:
        NASDAQ100_TICKERS = nasdaq
        logger.info(f"NASDAQ 100 tickers refreshed from Wikipedia ({len(nasdaq)} stocks)")
    if ftse100:
        FTSE100_TICKERS = ftse100
        logger.info(f"FTSE 100 tickers refreshed from Wikipedia ({len(ftse100)} stocks)")
    UNIVERSE = list(dict.fromkeys(
        SP500_TICKERS + NASDAQ100_TICKERS + FTSE100_TICKERS + FTSE250_TICKERS + SCREENER_EXTRA_TICKERS
    ))

    # Create default admin account on first run
    users = load_users()
    if not users:
        default_pass = secrets.token_urlsafe(16)
        users["admin"] = {
            "hashed_password": _hash_pw(default_pass),
            "email": os.getenv("ALERT_EMAIL", ""),
        }
        save_users(users)
        print("\n" + "="*55)
        print("[Auth] First run — default account created:")
        print("  Username : admin")
        print(f"  Password : {default_pass}")
        print("  Please change this password after first login!")
        print("="*55 + "\n")
        logger.info("First-run admin account created")

    scheduler.add_job(monitor_stocks, "interval", minutes=5,  id="monitor")
    scheduler.add_job(auto_predict,   "interval", minutes=15, id="predictions")
    scheduler.add_job(lambda: asyncio.create_task(prewarm_screener_cache(force=True)), "interval", hours=3, id="screener_prewarm")
    scheduler.start()
    print("[Monitor] Stock monitor started — checking every 5 minutes during market hours.")
    print("[Predictions] Auto-prediction scheduled every 15 minutes during market hours.")
    asyncio.create_task(prewarm_screener_cache())

@app.on_event("shutdown")
async def shutdown():
    scheduler.shutdown()


# ── Auth endpoints ────────────────────────────────────────────────────────────

@app.post("/api/auth/login")
@limiter.limit("10/minute")
async def login(req: LoginRequest, request: Request):
    username = req.username.strip()
    _check_lockout(username)
    users = load_users()
    user = users.get(username)
    if not user or not _verify_pw(req.password, user["hashed_password"]):
        _record_failed_login(username)
        logger.warning("LOGIN_FAIL username=%s ip=%s", username, request.client.host if request.client else "unknown")
        raise HTTPException(status_code=401, detail="Invalid username or password")
    _clear_failed_logins(username)
    logger.info("LOGIN_OK username=%s ip=%s", username, request.client.host if request.client else "unknown")
    token = create_access_token(username)
    return {"access_token": token, "token_type": "bearer"}

@app.post("/api/auth/unlock-test")
async def unlock_test(request: Request):
    """Dev/test only: clear all lockouts. Disabled unless TEST_UNLOCK_SECRET is set."""
    secret = os.getenv("TEST_UNLOCK_SECRET")
    if not secret:
        raise HTTPException(status_code=404, detail="Not found")
    body = await request.json()
    if body.get("secret") != secret:
        raise HTTPException(status_code=403, detail="Forbidden")
    _lockout_until.clear()
    _failed_logins.clear()
    return {"ok": True}

@app.post("/api/auth/forgot-password")
@limiter.limit("5/minute")
async def forgot_password(req: ForgotPasswordRequest, request: Request):
    username = req.username.strip()
    client_ip = request.client.host if request.client else "unknown"
    origin = request.headers.get("origin", "")
    logger.info("PASSWORD_RESET_ATTEMPT username=%s ip=%s origin=%s", username, client_ip, origin or "none")
    users = load_users()
    user = users.get(username)
    if user:
        token = secrets.token_urlsafe(32)
        _reset_tokens[token] = (username, datetime.now(timezone.utc) + timedelta(minutes=15))
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
async def reset_password(req: ResetPasswordRequest):
    if len(req.new_password) < 12:
        raise HTTPException(status_code=400, detail="Password must be at least 12 characters")
    entry = _reset_tokens.get(req.token)
    if not entry:
        raise HTTPException(status_code=400, detail="Invalid or expired reset token")
    username, expiry = entry
    if datetime.now(timezone.utc) > expiry:
        _reset_tokens.pop(req.token, None)
        raise HTTPException(status_code=400, detail="Reset token has expired")
    users = load_users()
    if username not in users:
        raise HTTPException(status_code=400, detail="User not found")
    users[username]["hashed_password"] = _hash_pw(req.new_password)
    save_users(users)
    _reset_tokens.pop(req.token, None)
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
    return {"username": current_user}

# ── Existing endpoints ────────────────────────────────────────────────────────

@app.get("/api/search")
async def search_stocks(q: str = ""):
    q = q.strip().lower()
    if not q or len(q) < 1:
        return []
    q_upper = q.upper()
    q_norm = _normalize_search_text(q)
    alias_target = SEARCH_ALIASES.get(q_upper)

    def _is_query_match(ticker: str) -> bool:
        lowered = ticker.lower()
        name = str(TICKER_NAMES.get(ticker, ticker)).lower()
        ticker_norm = _normalize_search_text(ticker)
        name_norm = _normalize_search_text(name)
        return (
            q in lowered
            or q in name
            or q_norm in ticker_norm
            or q_norm in name_norm
            or alias_target == ticker
        )

    def _search_rank(ticker: str) -> tuple[int, int, str]:
        lowered = ticker.lower()
        name = str(TICKER_NAMES.get(ticker, ticker)).lower()
        ticker_norm = _normalize_search_text(ticker)
        name_norm = _normalize_search_text(name)
        alias_exact = alias_target == ticker
        if lowered == q or ticker_norm == q_norm:
            return (0, len(ticker), ticker)
        if alias_exact:
            return (0, len(ticker), ticker)
        if lowered.startswith(q) or ticker_norm.startswith(q_norm):
            return (1, len(ticker), ticker)
        if name.startswith(q) or name_norm.startswith(q_norm):
            return (2, len(ticker), ticker)
        if all(token in name for token in q.split() if token):
            return (3, len(ticker), ticker)
        return (3, len(ticker), ticker)

    # Broad one-letter searches can fan out into dozens of yfinance calls, so cap them.
    matched = sorted((
        t for t in UNIVERSE
        if _is_query_match(t)
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


async def _build_screener_row(ticker: str, info: dict) -> dict | None:
    try:
        market_cap     = info.get("marketCap")
        pe             = info.get("trailingPE")
        peg            = info.get("pegRatio")
        pb             = info.get("priceToBook")
        ev_ebitda      = info.get("enterpriseToEbitda")
        fcf            = info.get("freeCashflow")
        volume         = info.get("averageVolume")
        price          = info.get("currentPrice") or info.get("regularMarketPrice")
        name           = info.get("shortName", ticker)
        stock_sector   = info.get("sector", "")
        fcf_yield      = calc_fcf_yield(fcf, market_cap)
        rev_growth_raw = info.get("revenueGrowth")
        rev_growth     = round(rev_growth_raw * 100, 1) if rev_growth_raw is not None else None
        return {
            "ticker": ticker, "name": name, "sector": stock_sector,
            "price": round(price, 2) if price else None,
            "pe": round(pe, 2) if pe else None,
            "peg": round(peg, 2) if peg else None,
            "pb": round(pb, 2) if pb else None,
            "ev_ebitda": round(ev_ebitda, 2) if ev_ebitda else None,
            "fcf_yield": fcf_yield,
            "rev_growth": rev_growth,
            "market_cap": market_cap, "volume": volume,
        }
    except Exception:
        return None


async def _fetch_universe_bg(pool_key: str, pool: list[str]):
    BATCH_SIZE = 25
    try:
        for i in range(0, len(pool), BATCH_SIZE):
            batch = pool[i:i + BATCH_SIZE]
            results = await asyncio.gather(
                *[get_info_with_timeout(t, SEARCH_INFO_TIMEOUT_SEC) for t in batch],
                return_exceptions=True,
            )
            for ticker, info in zip(batch, results):
                if isinstance(info, Exception) or not info:
                    continue
                row = await _build_screener_row(ticker, info)
                if row:
                    _screen_partial[pool_key].append(row)
            if i + BATCH_SIZE < len(pool):
                await asyncio.sleep(0.4)
        _screen_universe_cache[pool_key] = (list(_screen_partial[pool_key]), datetime.now(timezone.utc))
        logger.info("Screener cache warm: %d tickers loaded for pool '%s'", len(_screen_partial[pool_key]), pool_key)
    finally:
        _screen_loading[pool_key] = False


async def prewarm_screener_cache(force: bool = False):
    """Pre-warm the screener universe cache in the background."""
    pool_key = "__all__"
    if not force and (_screen_loading.get(pool_key) or _screen_universe_cache.get(pool_key)):
        return
    _screen_universe_cache.pop(pool_key, None)
    _screen_loading[pool_key] = True
    _screen_partial[pool_key] = []
    logger.info("Pre-warming screener cache (%d tickers)…", len(UNIVERSE))
    asyncio.create_task(_fetch_universe_bg(pool_key, UNIVERSE))


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
    index_map = {
        "sp500": SP500_TICKERS,
        "nasdaq100": NASDAQ100_TICKERS,
        "ftse100": FTSE100_TICKERS,
        "ftse250": FTSE250_TICKERS,
    }
    pool_key = index or "__all__"
    cached_universe = _screen_universe_cache.get(pool_key)
    now = datetime.now(timezone.utc)
    loading = False

    if cached_universe and (now - cached_universe[1]).total_seconds() < 1800:
        universe_rows = cached_universe[0]
    else:
        pool = index_map.get(index, UNIVERSE) if index else UNIVERSE

        if _screen_loading.get(pool_key):
            # Background fetch already running — return whatever has loaded so far
            universe_rows = list(_screen_partial.get(pool_key, []))
            loading = True
        else:
            # Kick off background progressive fetch; return first batch synchronously
            _screen_loading[pool_key] = True
            _screen_partial[pool_key] = []
            asyncio.create_task(_fetch_universe_bg(pool_key, pool))
            first_batch = pool[:25]
            first_infos = await asyncio.gather(
                *[get_info_with_timeout(t, SEARCH_INFO_TIMEOUT_SEC) for t in first_batch],
                return_exceptions=True,
            )
            first_rows = []
            for ticker, info in zip(first_batch, first_infos):
                if isinstance(info, Exception) or not info:
                    continue
                row = await _build_screener_row(ticker, info)
                if row:
                    first_rows.append(row)
                    _screen_partial[pool_key].append(row)
            universe_rows = first_rows
            loading = True

    results = []
    q_norm = (q or "").strip().lower()
    q_compact = _normalize_search_text(q_norm)
    for row in universe_rows:
        if q_norm:
            ticker_text = str(row.get("ticker", "")).lower()
            name_text = str(row.get("name", "")).lower()
            ticker_match = q_norm in ticker_text or q_compact in _normalize_search_text(ticker_text)
            name_match = q_norm in name_text or q_compact in _normalize_search_text(name_text)
            if not ticker_match and not name_match:
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
    return {"results": results, "loading": loading, "loaded": len(universe_rows)}


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
async def get_watchlist():
    tickers = load_watchlist()
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
async def sentiment_scan(ticker: Optional[str] = None, watchlist: bool = False):
    """Run sentiment scanner on watchlist, according to query params."""
    return run_sentiment_scanner(ticker=ticker, watchlist_only=watchlist)


async def _run_predictions_bg():
    try:
        print("[Predictions] Background generation triggered by watchlist add...")
        await generate_predictions()
        print("[Predictions] Background generation completed.")
    except Exception as e:
        print(f"[Predictions] Background generation failed: {e}")


@app.post("/api/watchlist/{ticker}")
async def add_to_watchlist(ticker: str, background_tasks: BackgroundTasks):
    ticker = _validate_ticker(ticker)
    tickers = load_watchlist()
    if ticker not in tickers:
        tickers.append(ticker)
        save_watchlist(tickers)
        background_tasks.add_task(_run_predictions_bg)
    return {"watchlist": tickers}


@app.delete("/api/watchlist/{ticker}")
def remove_from_watchlist(ticker: str):
    ticker = _validate_ticker(ticker)
    tickers = load_watchlist()
    tickers = [t for t in tickers if t != ticker]
    save_watchlist(tickers)
    return {"watchlist": tickers}


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


def _stock_research_impl(req: RecommendRequest):
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        raise HTTPException(status_code=500, detail="ANTHROPIC_API_KEY not set in .env")
    client = anthropic.Anthropic(api_key=api_key)

    # Extract ticker from query if it looks like a ticker or company name
    query = req.query.strip().upper()
    ticker_context = ""

    def _resolve_ticker(q: str) -> str | None:
        """Try to resolve a company name or partial name to a ticker symbol."""
        # Already looks like a ticker
        if len(q) <= 5 and q.replace(".", "").isalnum():
            return q
        _STRIP_SUFFIXES = {"technologies", "technology", "inc", "corp", "corporation",
                           "ltd", "limited", "group", "holdings", "co"}
        # Try full query, then first word as fallback for multi-word names
        attempts = [q]
        if " " in q:
            attempts.append(q.split()[0])
        for attempt in attempts:
            try:
                results = yf.Search(attempt, max_results=1).quotes
                if results:
                    t = results[0].get("symbol", "")
                    if t:
                        return t.upper()
            except Exception:
                pass
        # Strip trailing generic suffixes and retry
        words = q.lower().split()
        while words and words[-1] in _STRIP_SUFFIXES:
            words.pop()
            if not words:
                break
            try:
                results = yf.Search(" ".join(words), max_results=1).quotes
                if results:
                    t = results[0].get("symbol", "")
                    if t:
                        return t.upper()
            except Exception:
                pass
        return None

    resolved = _resolve_ticker(query)
    # If it looks like a single ticker (1-5 characters, all letters/numbers), fetch live data
    if resolved:
        import re as _re
        from datetime import datetime as _dt

        def _fmt_large(v):
            try:
                v = float(v)
                if v <= 0: return "N/A"
                if v >= 1e12: return f"${v/1e12:.2f}T"
                if v >= 1e9:  return f"${v/1e9:.2f}B"
                if v >= 1e6:  return f"${v/1e6:.2f}M"
                return f"${v:,.0f}"
            except Exception:
                return "N/A"

        def _fmt_pct(v):
            try:
                return f"{float(v)*100:.1f}%"
            except Exception:
                return "N/A"

        def _safe(v, fmt=None):
            if v is None: return "N/A"
            try:
                return fmt(v) if fmt else str(v)
            except Exception:
                return "N/A"

        query = resolved  # use resolved ticker (e.g. "MU" even if user typed "Micron")
        lines = [f"## Live Market Data for {query} (as of {date.today()})"]

        # ── Core price & market data ─────────────────────────────────────────
        try:
            ticker = yf.Ticker(query)
            hist   = ticker.history(period="1y")
            info   = {}
            try:
                info = dict(ticker.info) if ticker.info else {}
            except Exception:
                pass

            if not hist.empty:
                try:
                    fi = ticker.fast_info
                    lp = getattr(fi, "last_price", None)
                    current_price = float(lp) if lp and float(lp) > 0 else float(hist["Close"].iloc[-1])
                except Exception:
                    current_price = float(hist["Close"].iloc[-1])

                prev_close  = float(hist["Close"].iloc[-2]) if len(hist) >= 2 else current_price
                day_chg_pct = (current_price - prev_close) / prev_close * 100 if prev_close else 0
                year_high   = float(hist["Close"].max())
                year_low    = float(hist["Close"].min())
                sma_50      = float(hist["Close"].iloc[-50:].mean())  if len(hist) >= 50  else None
                sma_200     = float(hist["Close"].iloc[-200:].mean()) if len(hist) >= 200 else None
                avg_vol_30  = int(hist["Volume"].iloc[-30:].mean()) if "Volume" in hist.columns and len(hist) >= 30 else None
                last_vol    = int(hist["Volume"].iloc[-1])           if "Volume" in hist.columns else None

                rec_key = info.get("recommendationKey") or ""
                lines += [
                    f"**Current Price**: ${current_price:.2f}  ({day_chg_pct:+.2f}% vs prev close)",
                    f"**52-Week Range**: ${year_low:.2f} – ${year_high:.2f}",
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
                ]
        except Exception as _e:
            logger.warning("price/info fetch failed for %s: %s", query, _e)
            lines.append("*Price and info data could not be fetched.*")

        # ── News headlines ───────────────────────────────────────────────────
        try:
            news_items = ticker.news or []
            news_lines = []
            for item in news_items[:10]:
                content   = item.get("content", item)
                title     = content.get("title", "")
                pub_str   = content.get("pubDate", "")
                pub_ts    = item.get("providerPublishTime", 0)
                pub_date  = pub_str[:10] if pub_str else (_dt.utcfromtimestamp(int(pub_ts)).strftime("%Y-%m-%d") if pub_ts else "recent")
                provider  = content.get("provider", {})
                publisher = provider.get("displayName", "") if isinstance(provider, dict) else content.get("publisher", "")
                summary   = _re.sub(r"<[^>]+>", "", content.get("summary", "")).strip()
                if title:
                    entry = f"- [{pub_date}] {title}" + (f" ({publisher})" if publisher else "")
                    if summary and len(summary) < 250:
                        entry += f"\n  {summary}"
                    news_lines.append(entry)
            if news_lines:
                lines.append("\n## Recent News Headlines (live from Yahoo Finance)")
                lines.extend(news_lines)
            else:
                lines.append("\n## Recent News Headlines\n*No headlines returned by Yahoo Finance for this ticker.*")
        except Exception as _e:
            logger.warning("news fetch failed for %s: %s", query, _e)
            lines.append("\n## Recent News Headlines\n*News fetch failed.*")

        # ── Quarterly income statement ───────────────────────────────────────
        try:
            qis = ticker.quarterly_income_stmt
            if qis is not None and not qis.empty:
                lines.append("\n## Quarterly Income Statement (last 4 quarters, from SEC filings)")
                for row_name in ["Total Revenue", "Gross Profit", "Operating Income", "Net Income", "EBITDA", "Basic EPS"]:
                    if row_name in qis.index:
                        vals = []
                        for c in qis.columns[:4]:
                            try:
                                vals.append(f"{str(c)[:10]}: {_fmt_large(float(qis.loc[row_name, c]))}")
                            except Exception:
                                vals.append(f"{str(c)[:10]}: N/A")
                        lines.append(f"**{row_name}**: " + " | ".join(vals))
        except Exception as _e:
            logger.debug("quarterly_income_stmt failed: %s", _e)

        # ── Quarterly balance sheet ──────────────────────────────────────────
        try:
            qbs = ticker.quarterly_balance_sheet
            if qbs is not None and not qbs.empty:
                col = qbs.columns[0]
                lines.append(f"\n## Quarterly Balance Sheet (most recent: {str(col)[:10]})")
                for row_name in ["Total Assets", "Total Liabilities Net Minority Interest", "Stockholders Equity",
                                 "Cash And Cash Equivalents", "Total Debt", "Net Debt"]:
                    if row_name in qbs.index:
                        try:
                            lines.append(f"**{row_name}**: {_fmt_large(float(qbs.loc[row_name, col]))}")
                        except Exception:
                            pass
        except Exception as _e:
            logger.debug("quarterly_balance_sheet failed: %s", _e)

        # ── Quarterly cash flow ──────────────────────────────────────────────
        try:
            qcf = ticker.quarterly_cashflow
            if qcf is not None and not qcf.empty:
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
        except Exception as _e:
            logger.debug("quarterly_cashflow failed: %s", _e)

        # ── Analyst recommendations ──────────────────────────────────────────
        try:
            recs = ticker.recommendations
            if recs is not None and not recs.empty:
                lines.append("\n## Analyst Rating Changes (from published reports)")
                for _, row in recs.tail(10).iterrows():
                    period     = str(row.name)[:10]
                    firm       = str(row.get("Firm", row.get("firm", "")) or "")
                    to_grade   = str(row.get("To Grade", row.get("toGrade", "")) or "")
                    from_grade = str(row.get("From Grade", row.get("fromGrade", "")) or "")
                    action     = str(row.get("Action", row.get("action", "")) or "")
                    parts = [f"[{period}]"]
                    if firm:       parts.append(firm)
                    if action:     parts.append(action.upper())
                    if to_grade:   parts.append(f"→ {to_grade}")
                    if from_grade and from_grade != to_grade: parts.append(f"(was {from_grade})")
                    lines.append("- " + " ".join(parts))
        except Exception as _e:
            logger.debug("recommendations failed: %s", _e)

        try:
            apt = ticker.analyst_price_targets
            if apt is not None:
                lines.append("\n## Analyst Price Targets (consensus)")
                for label, key in [("Current", "current"), ("Low", "low"), ("High", "high"), ("Mean", "mean"), ("Median", "median")]:
                    try:
                        v = apt.get(key)
                        if v: lines.append(f"**{label} Target**: ${float(v):.2f}")
                    except Exception:
                        pass
        except Exception as _e:
            logger.debug("analyst_price_targets failed: %s", _e)

        # ── Earnings history ─────────────────────────────────────────────────
        try:
            eh = ticker.earnings_history
            if eh is not None and not eh.empty:
                lines.append("\n## Earnings History (actual vs estimate)")
                for _, row in eh.tail(6).iterrows():
                    period   = str(row.get("quarter", row.name))[:10]
                    est      = row.get("epsEstimate")
                    actual   = row.get("epsActual")
                    surp_pct = row.get("surprisePercent")
                    parts = [f"[{period}]"]
                    if est      is not None: parts.append(f"Est: {est:.2f}")
                    if actual   is not None: parts.append(f"Actual: {actual:.2f}")
                    if surp_pct is not None: parts.append(f"Surprise: {surp_pct:+.1f}%")
                    lines.append("- " + " | ".join(parts))
        except Exception as _e:
            logger.debug("earnings_history failed: %s", _e)

        lines.append("\n**CRITICAL INSTRUCTION**: Base your entire report on the live data above. Do NOT substitute training-data figures for any metric. If a value shows N/A, report it as unavailable.")
        ticker_context = "\n".join(lines)
        logger.info("RESEARCH_CONTEXT built for %s: %d lines", query, len(lines))

    # Stock research command content
    stock_research_prompt = f"""# Stock Research Command

{ticker_context}

Research stocks, sectors, or company types based on the provided input: {req.query}

Determine what was provided:
- If it looks like **stock tickers** (e.g. `IONQ RGTI QBTS`): research each ticker individually
- If it looks like a **sector or theme** (e.g. `quantum computing sector`): identify the top publicly traded companies in that space
- If it looks like a **company description** (e.g. `pre-IPO AI companies 2026`): find and evaluate matching candidates

## Research Steps

For each stock or company identified, perform the following:

### 1. Company Overview
- Full company name, ticker, exchange
- Sector, industry, and sub-sector
- Business model summary (what they do and how they make money)
- Stage: early-stage, growth, mature, or turnaround

### 2. Recent News & Catalysts
Use the live news headlines provided above (if any). Summarise the most relevant:
- Earnings results or guidance updates
- Product launches, partnerships, or contracts
- Regulatory approvals or government contracts
- Executive changes or insider activity
- Analyst upgrades/downgrades
If no live headlines were provided, state clearly that live news was unavailable for this query. Do NOT invent or recall news from training data.

### 3. Financial Snapshot
Use the live financial data provided above as the primary source. Summarise:
- Market cap, enterprise value, revenue (TTM) and YoY growth
- Gross margin, operating margin, net margin
- Cash position, total debt, free cash flow
- P/E, forward P/E, P/S, EV/Revenue, EV/EBITDA vs. sector peers
Do not invent numbers — use exactly what was provided. Mark any field shown as N/A accordingly.

### 4. Technical Analysis
Use the live price data provided above:
- Current price vs. 52-week range and vs. 50/200-day SMAs
- Trend direction based on price vs. SMAs
- Volume: compare last session volume vs. 30-day average
- Note if price is extended or near support/resistance

### 5. Bull Case
List 3–5 reasons this stock could outperform:
- TAM expansion or market share gains
- Upcoming catalysts (product launch, FDA decision, contract win, etc.)
- Competitive moat or proprietary technology
- Improving unit economics or path to profitability

### 6. Bear Case
List 3–5 risks:
- Competition or commoditization risk
- Dilution risk (frequent share issuances)
- Regulatory, macro, or geopolitical headwinds
- Execution risk or management credibility
- Valuation stretched relative to fundamentals

### 7. Verdict
Provide a concise investment summary:
- **Outlook**: Bullish / Neutral / Bearish
- **Time horizon**: Short-term trade vs. long-term hold
- **Entry considerations**: Current price attractive, wait for pullback, or avoid
- **Key thing to watch**: The single most important metric or event to monitor

---

## Output Format

Present results as a structured report. If multiple tickers were given, use a separate section per ticker, then finish with a **Comparative Summary** table:

| Ticker | Sector | Market Cap | Revenue Growth | Outlook | Key Catalyst |
|--------|--------|------------|----------------|---------|---------------|
| ...    | ...    | ...        | ...            | ...     | ...           |

Be data-driven and concise. Cite sources where possible. Flag any data that could not be verified."""

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

ACTUAL_WINDOW_DAYS = 5  # Measure actual return over 5 trading days forward

def update_actuals(predictions: list[dict]) -> tuple[list[dict], bool]:
    """
    Fill actual_pct for predictions whose measurement window has closed.
    For predictions with prediction_window_days=5 (new format): use price_at_prediction
    as anchor and find the close 5 trading days later.
    For legacy predictions (no prediction_window_days): use same-day return for backward compat.
    """
    today = str(date.today())
    updated = False
    for pred in predictions:
        if pred.get("actual_pct") is not None:
            continue
        pred_date = pred.get("date", "")
        if pred_date > today:
            continue

        window = pred.get("prediction_window_days", 1)  # legacy = 1-day
        try:
            # Fetch enough history to cover the window
            hist = yf.Ticker(pred["ticker"]).history(period="20d")
            if hist is None or hist.empty:
                continue
            dates = [str(d.date()) for d in hist.index]

            if window == 1:
                # Legacy: prev-close → same-day close
                anchor = pred_date
                if anchor not in dates:
                    prior = [d for d in dates if d <= anchor]
                    if not prior:
                        continue
                    anchor = prior[-1]
                idx = dates.index(anchor)
                if idx > 0:
                    p0 = float(hist["Close"].iloc[idx - 1])
                    p1 = float(hist["Close"].iloc[idx])
                    if p0 and math.isfinite(p0) and math.isfinite(p1):
                        pred["actual_pct"] = round(((p1 - p0) / p0) * 100, 2)
                        updated = True
            else:
                # New format: anchor on prediction date close, measure N trading days forward
                # Find the anchor date (prediction date or nearest prior trading day)
                anchor = pred_date
                if anchor not in dates:
                    prior = [d for d in dates if d <= anchor]
                    if not prior:
                        continue
                    anchor = prior[-1]
                anchor_idx = dates.index(anchor)
                forward_idx = anchor_idx + window
                if forward_idx >= len(dates):
                    continue  # Window not closed yet — wait
                p0 = pred.get("price_at_prediction") or float(hist["Close"].iloc[anchor_idx])
                p1 = float(hist["Close"].iloc[forward_idx])
                if p0 and math.isfinite(float(p0)) and math.isfinite(p1):
                    pred["actual_pct"] = round(((p1 - float(p0)) / float(p0)) * 100, 2)
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
                root = ET.fromstring(r.text)
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


def compute_calibration(predictions: list[dict]) -> dict:
    """
    Per-stock calibration from completed predictions.
    Returns mean_bias (actual - predicted), directional accuracy, inversion flag,
    and suppressed flag (>=10 samples with <40% accuracy — signal is noise).
    Requires >= 3 samples to include a stock; inversion requires >= 5.
    """
    # Prefer new 5-day window predictions for calibration; fall back to legacy (1-day)
    # during the transition period until enough 5-day data accumulates (min 50 entries)
    new_format = [
        p for p in predictions
        if p.get("actual_pct") is not None
        and p.get("predicted_pct") is not None
        and p.get("prediction_window_days", 1) == ACTUAL_WINDOW_DAYS
    ]
    legacy = [
        p for p in predictions
        if p.get("actual_pct") is not None
        and p.get("predicted_pct") is not None
        and p.get("prediction_window_days", 1) == 1
    ]
    completed = new_format if len(new_format) >= 50 else legacy
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
        # Suppress tickers where we have enough data (>=10) and accuracy is still below 40%
        # — the model has no reliable signal here; predicting is worse than not predicting
        suppressed = len(preds) >= 10 and acc < 0.40
        cal[ticker] = {
            "count":        len(preds),
            "mean_bias":    mean_bias,       # positive = we under-predict; negative = over-predict
            "accuracy_pct": round(acc * 100, 1),
            "inverted":     acc < 0.45 and len(preds) >= 5,
            "suppressed":   suppressed,
        }
    return cal


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


@app.get("/api/predictions")
def get_predictions():
    today = str(date.today())
    mtime_ns = PREDICTIONS_FILE.stat().st_mtime_ns if PREDICTIONS_FILE.exists() else None
    if (
        _predictions_cache["data"] is not None
        and _predictions_cache["date"] == today
        and _predictions_cache["mtime_ns"] == mtime_ns
    ):
        return _predictions_cache["data"]

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
        derived_horizons = prediction_horizon_returns(
            p.get("predicted_pct"),
            p.get("direction"),
            p.get("score"),
            p.get("confidence", "medium"),
        )
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
                **prediction_horizon_returns(None),
            })
    response = sanitize_jsonable(sorted_preds)
    _predictions_cache["date"] = today
    _predictions_cache["mtime_ns"] = PREDICTIONS_FILE.stat().st_mtime_ns if PREDICTIONS_FILE.exists() else None
    _predictions_cache["data"] = response
    return response


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
    calibration_early = compute_calibration(predictions)
    suppressed_tickers = {t for t, c in calibration_early.items() if c.get("suppressed")}
    if suppressed_tickers:
        logger.info("SUPPRESSED tickers (>=10 predictions, <40%% accuracy): %s", sorted(suppressed_tickers))

    # Watchlist stocks always get a prediction; held portfolio positions always get one too
    # so recommendations and P&L have signal. UNIVERSE fills remaining slots.
    # Suppressed tickers are excluded from universe fill — no reliable signal.
    watchlist_missing = [t for t in watchlist_tickers if t not in already_predicted]

    paper_positions = compute_positions(load_paper_portfolio())
    real_positions  = compute_positions(load_portfolio())
    held_tickers = list(dict.fromkeys(
        [t for t, p in paper_positions.items() if p["shares"] > 0] +
        [t for t, p in real_positions.items()  if p["shares"] > 0]
    ))
    held_missing = [
        t for t in held_tickers
        if t not in already_predicted and t not in watchlist_tickers
    ]

    covered = set(watchlist_tickers) | set(held_tickers)
    universe_fill = [
        t for t in UNIVERSE[:PREDICTIONS_UNIVERSE_FILL_LIMIT]
        if t not in already_predicted and t not in covered and t not in suppressed_tickers
    ]
    to_analyze = list(dict.fromkeys(watchlist_missing + held_missing + universe_fill))[
        :max(len(watchlist_missing) + len(held_missing) + PREDICTIONS_UNIVERSE_FILL_LIMIT, 1)
    ]

    if not to_analyze:
        if updated:
            save_predictions(predictions)
        return {
            "message": "Predictions already generated for today.",
            "predictions": [p for p in predictions if p["date"] == today],
        }

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
        for attempt in range(2):
            try:
                def _blocking():
                    t = yf.Ticker(ticker)
                    info = t.info
                    hist = t.history(period="60d")   # extended for RSI, SMA50, drawdown
                    news = t.news[:3] if hasattr(t, "news") else []
                    return info, hist, news
                info, hist, news = await asyncio.to_thread(_blocking)
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

                # Earnings date — flag if within 7 days
                earnings_date_str = None
                days_to_earnings = None
                try:
                    raw_ed = info.get("earningsDate") or info.get("earningsTimestamp")
                    if raw_ed:
                        if isinstance(raw_ed, (int, float)):
                            from datetime import datetime as _dt
                            ed = _dt.utcfromtimestamp(raw_ed).date()
                        else:
                            ed = date.fromisoformat(str(raw_ed)[:10])
                        days_to_earnings = (ed - date.today()).days
                        earnings_date_str = str(ed)
                except Exception:
                    pass

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
                    # Earnings calendar
                    "earnings_date":     earnings_date_str,
                    "days_to_earnings":  days_to_earnings,
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

    completed = [p for p in predictions if p.get("actual_pct") is not None]
    accuracy_summary = "\n=== HISTORICAL CALIBRATION — apply these corrections to your predictions ===\n"
    if calibration:
        for ticker, c in calibration.items():
            if c.get("suppressed"):
                accuracy_summary += (
                    f"  {ticker}: {c['count']} predictions, {c['accuracy_pct']}% directional accuracy — "
                    f"*** SUPPRESSED: model has no reliable signal here. If you must predict, use confidence=low and note the suppression. ***\n"
                )
                continue
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

    # ── Regime detection ──────────────────────────────────────────────────────
    vix_now = macro.get("VIX (Fear Index)", {}).get("price", 20)
    if vix_now >= 30:
        regime_block = (
            f"=== ⚠️  MARKET REGIME: EXTREME FEAR (VIX {vix_now:.1f}) ===\n"
            "VIX is above 30 — this is a high-volatility, risk-off regime. "
            "In regimes like this, stock-picking signal collapses and correlations spike toward 1. "
            "RULES FOR THIS REGIME:\n"
            "- Default all predictions to NEUTRAL unless there is an extremely strong, specific catalyst.\n"
            "- Set confidence=low for ALL predictions.\n"
            "- Do NOT generate bullish predictions based solely on fundamentals — they are not predictive at this time horizon during a fear spike.\n"
            "- Only flag bearish if a stock has clear downside catalysts beyond the macro.\n"
        )
    elif vix_now >= 25:
        regime_block = (
            f"=== ⚠️  MARKET REGIME: ELEVATED FEAR (VIX {vix_now:.1f}) ===\n"
            "VIX is elevated — market is in a risk-off phase. "
            "Stock-specific signals are weakened by macro uncertainty. "
            "RULES FOR THIS REGIME:\n"
            "- Cap confidence at 'medium' for all bullish predictions.\n"
            "- Lean toward NEUTRAL for stocks without strong near-term catalysts.\n"
            "- Bearish predictions for high-beta or speculative stocks are valid.\n"
        )
    else:
        regime_block = f"=== MARKET REGIME: NORMAL (VIX {vix_now:.1f}) ==="

    # ── Earnings calendar ─────────────────────────────────────────────────────
    earnings_flags = []
    for s in stocks_data:
        dte = s.get("days_to_earnings")
        ed  = s.get("earnings_date", "")
        if dte is not None and 0 <= dte <= 7:
            earnings_flags.append(
                f"  {s['ticker']}: earnings in {dte} day{'s' if dte != 1 else ''} ({ed}) — "
                "BINARY EVENT, predictions near earnings are highly uncertain. "
                "Use confidence=low and note the upcoming earnings in reasoning."
            )
        elif dte is not None and -2 <= dte < 0:
            earnings_flags.append(
                f"  {s['ticker']}: earnings just reported {abs(dte)} day{'s' if abs(dte) != 1 else ''} ago ({ed}) — "
                "post-earnings volatility may persist. Factor in reaction."
            )
    earnings_block = (
        "=== EARNINGS CALENDAR ===\n" + "\n".join(earnings_flags)
        if earnings_flags
        else "=== EARNINGS CALENDAR ===\n  No earnings events within 7 days for stocks in this batch."
    )

    prompt = f"""You are a quantitative stock analyst. Your goal: predict each stock's return over the next 5 trading days.

Today: {today}
Prediction window: 5 trading days forward (not 1 day, not 12 months — specifically 5 trading days).

{regime_block}

=== MACROECONOMIC CONDITIONS ===
{json.dumps(macro, indent=2)}

{earnings_block}

=== MARKET & FINANCIAL NEWS ===
{chr(10).join(headlines[:20]) if headlines else "No headlines fetched."}

=== WATCHLIST STOCK RESEARCH INPUTS ===
Use these outputs from the stock research agent for each watchlist ticker only if they are present, and incorporate them into your model reasoning and forecast adjustments.
{watchlist_research_context}

=== STOCKS TO ANALYZE ===
Each stock includes a pre-calculated `sentiment_score` (%) built from:
  - VIX fear adjustment
  - S&P 500 5-day momentum adjustment
  - Beta multiplier (amplifies market moves for high/low beta stocks)
  - Headline sentiment (keyword scan: +0.3% per positive signal, -0.3% per negative)

{json.dumps(stocks_data, indent=2)}
{accuracy_summary}
=== SCORING RULES (5-DAY FORWARD RETURN) ===
You are predicting the 5-day forward return. At this horizon:
- MOMENTUM signals (RSI, 52-week position, vs SMA50, recent price trend) are the PRIMARY driver — they have genuine 1-2 week predictive power.
- FUNDAMENTALS (P/E, FCF, ROE) are SECONDARY context only — they don't move prices in 5 days unless tied to a near-term catalyst like earnings.
- SENTIMENT and NEWS are HIGH WEIGHT — a fresh catalyst in the headlines can move a stock in days.

1. START with `sentiment_score` as your baseline signal strength for each stock.
2. Each stock now includes pre-computed `factor_scores` (0-100 each). WEIGHT THEM AS FOLLOWS for the 5-day horizon:
   - MOMENTUM score: HIGH weight — most predictive at this time horizon
   - QUALITY + GROWTH scores: MEDIUM weight — confirm the thesis but don't drive it
   - VALUE score: LOW weight — cheap stocks don't re-rate in 5 days without a catalyst

   VALUE     — How cheap vs fundamentals (P/E, P/B, FCF yield, EV/EBITDA, PEG)
   MOMENTUM  — Price trend strength (RSI, 52-week position, vs 50-SMA, 5d return, short interest)
   QUALITY   — Balance sheet health (ROE, gross/net margins, debt ratio, current ratio)
   GROWTH    — Earnings & revenue expansion (rev growth, EPS growth, forward PE improvement)
   COMPOSITE — Equal-weight average of all four

   Also included per-stock (when available):
   - `dcf`: intrinsic_per_share, margin_of_safety_pct (positive = undervalued), wacc_pct
   - `annualised_vol_pct`: 60-day realised volatility
   - `max_drawdown_pct`: worst peak-to-trough over 60 days

FACTOR GUIDANCE — let these scores shape confidence and direction:
- composite ≥ 70 + positive sentiment → strongly support bullish, high confidence eligible
- composite ≤ 35 + negative sentiment → strongly support bearish, high confidence eligible
- Strong VALUE + QUALITY but weak MOMENTUM → patient BUY thesis, cap confidence at "medium"
- Strong MOMENTUM but weak VALUE (composite driven by momentum only) → growth play, note valuation risk
- DCF margin_of_safety_pct > 15% → supports bullish confidence level up by one tier
- DCF margin_of_safety_pct < -25% → cap confidence at "medium" even if sentiment positive, note overvaluation
- annualised_vol_pct > 45% → high risk stock, cap confidence at "medium", mention volatility in reasoning
- max_drawdown_pct < -25% → significant recent drawdown, factor this into near-term thesis

VALUATION THRESHOLDS (for reference when factor_scores not available):
- P/E < 15 = cheap, 15-25 = fair, >25 = expensive
- PEG < 1 = undervalued vs growth, >2 = expensive
- P/B < 1 = below asset value, >3 = expensive
- EV/EBITDA < 8 = cheap, 8-15 = fair, >15 = expensive
- FCF Yield > 8% = excellent, 4-8% = good, <4% = poor

QUALITY:
- High profit margins + growing revenue = quality business
- Low debt-to-equity = financial safety
- High EPS and revenue growth = momentum

IMPORTANT: You MUST return a prediction for EVERY stock in the watchlist: {must_predict}.
For any remaining stocks {also_consider}, only include the 2-3 with the strongest outlook.

CONFIDENCE RULES — must directly reflect the reasoning, no contradictions:
- "high": strong MOMENTUM signal + positive sentiment + no earnings within 7 days + normal VIX regime
- "medium": mixed signals, or weak momentum, or elevated VIX, or earnings within 14 days
- "low": negative sentiment_score, OR bearish momentum (RSI <40, below SMA50), OR earnings within 7 days, OR suppressed ticker, OR high-fear VIX regime
If your reasoning mentions a sell-off, downtrend, overvaluation, or risk — confidence MUST be "low" or "medium", never "high".
If there is an upcoming earnings event within 7 days — confidence MUST be "low" regardless of other signals.

Return ONLY a valid JSON array, no explanation, no markdown:
[
  {{
    "ticker": "AAPL",
    "direction": "bullish",
    "score": 74,
    "confidence": "high",
    "reasoning": "Sentiment score is modestly positive, PEG is attractive, and cash generation is strong. Signals align on the bullish side."
  }}
]
Rules:
- `direction` must be one of: "bullish", "neutral", "bearish"
- `score` must be an integer from 0 to 100
- 0-39 = bearish, 40-60 = neutral, 61-100 = bullish
- `confidence` must be "low" | "medium" | "high"
- reasoning should explain the signal, not claim precise percentage upside."""

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
        cal     = calibration.get(ticker, {})
        stock_data = stock_map.get(ticker, {})

        base_direction, base_pct = refine_prediction_signal(
            raw_pct,
            cp_direction,
            cp_score,
            cp.get("confidence", "medium"),
            stock_data,
        )

        # 1. Bias correction — shift prediction by historical mean error
        bias         = cal.get("mean_bias", 0.0)
        bias_applied = abs(bias) >= 0.1   # only correct if systematic (>=0.1%)
        corrected    = round(base_pct + bias, 2) if bias_applied else base_pct

        # 2. Signal inversion — flip direction if model has been consistently wrong
        inverted  = cal.get("inverted", False)
        final_pct = round(-corrected, 2) if inverted else corrected

        # 3. Suppression flag — not enough signal quality for this ticker
        is_suppressed = cal.get("suppressed", False)

        # Append calibration notes to reasoning
        cal_note = ""
        if bias_applied:
            cal_note += f" [Bias corrected {bias:+.2f}%: raw={raw_pct:+.2f}%]"
        if inverted:
            cal_note += f" [Direction INVERTED — historical accuracy {cal['accuracy_pct']}%, signal flipped]"
        if is_suppressed:
            cal_note += f" [SUPPRESSED — {cal['accuracy_pct']}% accuracy over {cal['count']} predictions: treat with very low confidence]"

        entry = {
            "date":               today,
            "ticker":             ticker,
            "name":               name_map.get(ticker, ""),
            "predicted_pct":      final_pct,
            "raw_predicted_pct":  raw_pct,
            "model_predicted_pct": base_pct,
            "direction":          prediction_direction(final_pct),
            "score":              cp_score if cp_score is not None else prediction_score(final_pct, cp.get("confidence", "medium")),
            "bias_correction":    round(bias, 3) if bias_applied else 0.0,
            "inverted":           inverted,
            "suppressed":         is_suppressed,
            "confidence":         "low" if is_suppressed else cp.get("confidence", "medium"),
            "reasoning":          cp.get("reasoning", "") + cal_note,
            "actual_pct":              None,
            "prediction_window_days":  ACTUAL_WINDOW_DAYS,
            "price_at_prediction":     price_map.get(ticker),
            "generated_at":            datetime.utcnow().isoformat(),
            # Quant data
            "factor_scores":      stock_data.get("factor_scores"),
            "dcf":                stock_data.get("dcf"),
            "annualised_vol_pct": stock_data.get("annualised_vol_pct"),
            "max_drawdown_pct":   stock_data.get("max_drawdown_pct"),
        }
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
    return {"predictions": new_preds}


@app.post("/api/predictions/generate")
@limiter.limit("8/hour")
async def generate_predictions(request: Request):
    return await _generate_predictions_impl()


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
    }


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
def get_alerts():
    return load_alerts()


@app.post("/api/alerts/log")
async def log_alert_entry(request: Request):
    """Internal endpoint for external processes (e.g. sentiment agent) to log an alert entry."""
    entry = await request.json()
    if not isinstance(entry, dict) or not entry.get("message"):
        raise HTTPException(status_code=400, detail="Invalid alert entry")
    entry.setdefault("id", str(uuid.uuid4()))
    entry.setdefault("timestamp", datetime.now(timezone.utc).isoformat())
    append_alert(entry)
    return {"ok": True}


@app.get("/api/alerts/status")
def get_monitor_status():
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


@app.get("/api/alerts/debug-snapshot")
async def get_alert_debug_snapshot(current_user: str = Depends(get_current_user)):
    _s = load_settings()
    buy_limit = max(1, int(_s.get("alert_top_buys") or os.getenv("ALERT_TOP_BUYS", "3")))
    sell_limit = max(1, int(_s.get("alert_top_sells") or os.getenv("ALERT_TOP_SELLS", "3")))
    snapshot = await _build_recommendation_alert_snapshot(buy_limit=buy_limit, sell_limit=sell_limit)
    return {
        "user": current_user,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "buy_count": len(snapshot.get("buys", [])),
        "sell_count": len(snapshot.get("sells", [])),
        "buys": snapshot.get("buys", []),
        "sells": snapshot.get("sells", []),
        "whatsapp_for_recommendation_alerts": True,
        "whatsapp_note": "Recommendation monitor will attempt WhatsApp delivery when Twilio is configured.",
    }


@app.post("/api/alerts/test")
def test_alert():
    emailed = send_email(
        "StockLens — Test Alert",
        "This is a test alert from your StockLens app.\nNotifications are working correctly."
    )
    texted = send_sms("[TEST] StockPicker — notifications working! This is a TEST message.")
    return {"email_sent": emailed, "sms_sent": texted}


@app.post("/api/alerts/test-whatsapp")
def test_whatsapp_alert():
    result = send_whatsapp_message("[TEST] StockPicker — WhatsApp notifications working. This is a dedicated WhatsApp test message.")
    return {
        "sms_sent": bool(result.get("ok")),
        "sid": result.get("sid"),
        "status": result.get("status"),
        "error": result.get("error"),
        "to": result.get("to"),
    }


@app.post("/api/alerts/test-preview")
def test_alert_preview():
    """Send a formatted recommendation alert email using current alert history as sample data."""
    alerts = load_alerts()
    if not alerts:
        return {"email_sent": False, "sms_sent": False, "error": "No alert data to preview"}

    ET = zoneinfo.ZoneInfo("America/New_York")
    time_str = datetime.now(timezone.utc).astimezone(ET).strftime("%d %b %Y, %H:%M ET")
    buy_alerts  = [a for a in alerts if a.get("action") == "BUY"]
    sell_alerts = [a for a in alerts if a.get("action") == "SELL"]
    subject, body, sms_body = _build_alert_email(buy_alerts, sell_alerts, time_str, preview=True)
    emailed = send_email(subject, body)
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
        return {"buys": [], "sells": [], "summary": {}}
    latest_date  = max(p["date"] for p in dated)
    latest_preds = [normalize_prediction(p) for p in dated if p["date"] == latest_date]

    # Compute paper portfolio positions (used for buy filtering + sell signals)
    stage_started = datetime.now(timezone.utc)
    _tick("load_portfolio", "Loading paper portfolio and cash position…", 1, 1)
    paper_txs        = load_paper_portfolio()
    paper_positions  = compute_positions(paper_txs)
    paper_held       = {t for t, p in paper_positions.items() if p["shares"] > 0}

    # Paper cash remaining
    paper_cash = initial_float
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
        if not isinstance(info, Exception):
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
    for pred in latest_preds:
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

        # Skip if recently sold (7-day cooldown) to prevent immediate re-buy churn
        last_sell_ts = last_sell_timestamp.get(ticker)
        if last_sell_ts:
            try:
                if (datetime.utcnow() - datetime.fromisoformat(last_sell_ts)).days < 7:
                    continue
            except ValueError:
                pass

        allow_low_conf_buy = confidence == "low" and signal_score >= 68
        if direction != "bullish" or signal_score < 60 or (confidence == "low" and not allow_low_conf_buy) or remaining_cash < 500:
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

        alloc_pct = 0.15 if confidence == "high" else 0.08
        if confidence == "low":
            alloc_pct = 0.05
        accuracy_bonus = max(0, (accuracy - 0.5) * 0.4)
        alloc_pct = min(alloc_pct * (1 + accuracy_bonus), 0.20)

        # Risk-adjust position size: high-volatility stocks get smaller allocations
        vol_adj = 30.0 / max(30.0, float(vol_pct))
        alloc_pct = alloc_pct * vol_adj

        position_value = min(remaining_cash * alloc_pct, initial_float * 0.15)
        qty = int(position_value / current_price)
        if qty < 1:
            continue

        estimated_cost = round(qty * current_price, 2)
        if estimated_cost > remaining_cash:
            qty = int(remaining_cash / current_price)
            if qty < 1:
                continue
            estimated_cost = round(qty * current_price, 2)

        # Factor-boosted score: composite 70 → +35%, composite 30 → -35%
        factor_boost = 1 + (composite - 50) / 200
        score = signal_score * accuracy * (1.15 if confidence == "high" else 1.0) * factor_boost

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
        })
        remaining_cash = max(0.0, remaining_cash - estimated_cost)
        build_done += 1
        _tick("build_recommendations", f"Ranking buy and sell opportunities… ({build_done}/{build_total})", build_done, build_total)

    if not latest_preds:
        _tick("build_recommendations", "Ranking buy and sell opportunities…", build_done, build_total)

    buys.sort(key=lambda x: x["score"], reverse=True)

    if not buys and remaining_cash >= 500:
        fallback_candidates = []
        for pred in latest_preds:
            if pred["ticker"] in paper_held:
                continue
            last_sell_ts = last_sell_timestamp.get(pred["ticker"])
            if last_sell_ts:
                try:
                    if (datetime.utcnow() - datetime.fromisoformat(last_sell_ts)).days < 7:
                        continue
                except ValueError:
                    pass
            if pred.get("direction") != "bullish":
                continue
            signal_score = pred.get("score") or 50
            if signal_score < 57:
                continue
            current_price = (
                price_map.get(pred["ticker"], 0)
                or pred.get("price_at_prediction")
                or _price_from_info(info_map.get(pred["ticker"]))
                or 0
            )
            if not current_price:
                continue
            fallback_candidates.append((signal_score, pred, current_price))

        for _, pred, current_price in sorted(
            fallback_candidates,
            key=lambda item: (item[0], item[1].get("predicted_pct") or 0),
            reverse=True,
        )[:5]:
            qty = int(min(remaining_cash * 0.04, initial_float * 0.08) / current_price)
            if qty < 1:
                continue
            estimated_cost = round(qty * current_price, 2)
            if estimated_cost > remaining_cash or estimated_cost <= 0:
                continue
            buys.append({
                "ticker":         pred["ticker"],
                "name":           pred.get("name", pred["ticker"]),
                "action":         "BUY",
                "trigger":        "TOP PICK",
                "current_price":  round(current_price, 2),
                "qty":            qty,
                "estimated_cost": estimated_cost,
                "direction":      pred.get("direction"),
                "score_value":    pred.get("score"),
                "predicted_pct":  pred.get("predicted_pct"),
                "confidence":     pred.get("confidence", "low"),
                "accuracy_pct":   calibration.get(pred["ticker"], {}).get("accuracy_pct"),
                "reasoning":      pred.get("reasoning") or "Best available bullish candidate from the latest model snapshot.",
                "score":          round((pred.get("score") or 50) * 0.85, 4),
            })
            remaining_cash = max(0.0, remaining_cash - estimated_cost)
            if remaining_cash < 500:
                break
        buys.sort(key=lambda x: x["score"], reverse=True)

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
        if unrealised_pct <= -5.0:
            trigger   = "STOP LOSS"
            reasoning = f"Position down {unrealised_pct:.1f}% — stop loss triggered to protect capital."
        elif unrealised_pct >= 8.0:
            trigger   = "TAKE PROFIT"
            reasoning = f"Position up {unrealised_pct:.1f}% — take profit target reached."
        elif pred and (pred.get("direction") or prediction_direction(pred.get("predicted_pct"))) == "bearish" and (pred.get("score") or prediction_score(pred.get("predicted_pct"), pred.get("confidence", "medium")) or 50) <= 45 and pred.get("confidence") in ("high", "medium"):
            trigger   = "PREDICTION"
            reasoning = pred.get("reasoning", "")

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
    result = {
        "buys":            buys,
        "sells":           sells,
        "prediction_date": latest_date,
        "summary": {
            "initial_float":         initial_float,
            "target":                target,
            "target_months":         target_months,
            "available_cash":        round(float(portfolio_summary.get("available_cash") or paper_cash), 2),
            "total_invested":        round(float(portfolio_summary.get("total_invested") or 0.0), 2),
            "total_current_value":   round(float(portfolio_summary.get("total_current_value") or 0.0), 2),
            "cash_after_buys":       round(remaining_cash, 2),
            "total_portfolio_value": round(float(portfolio_summary.get("total_portfolio_value") or paper_total_value), 2),
            "total_pnl":             round(float(portfolio_summary.get("total_pnl") or (paper_total_value - initial_float)), 2),
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
def get_settings():
    return load_settings()


@app.post("/api/settings")
def update_settings(s: dict, current_user: str = Depends(get_current_user)):
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
async def get_portfolio():
    transactions = load_portfolio()
    positions = compute_positions(transactions)

    # Fetch current prices for all held tickers concurrently (bust info cache for fresh P&L)
    held = [t for t, p in positions.items() if p["shares"] > 0]
    for t in held:
        _info_cache.pop(t, None)
    if held:
        infos = await asyncio.gather(*[get_info_with_timeout(t, RECOMMENDATION_INFO_TIMEOUT_SEC) for t in held], return_exceptions=True)
        price_map = {}
        for ticker, info in zip(held, infos):
            if not isinstance(info, Exception):
                price_map[ticker] = _price_from_info_for_alerts(info)
    else:
        price_map = {}

    for ticker in held:
        if not price_map.get(ticker):
            fallback_price = positions.get(ticker, {}).get("avg_cost", 0) or 0
            if fallback_price:
                price_map[ticker] = fallback_price

    result = []
    total_invested = 0.0
    total_current_value = 0.0
    total_unrealised_pnl = 0.0
    total_realised_pnl = 0.0

    for ticker, pos in positions.items():
        if pos["shares"] <= 0:
            continue
        current_price = price_map.get(ticker, 0) or pos["avg_cost"]
        cost_basis = pos["shares"] * pos["avg_cost"]
        current_value = pos["shares"] * current_price
        unrealised_pnl = current_value - cost_basis
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
            "realised_pnl": round(pos["realised_pnl"], 2),
        })

    summary = {
        "total_invested": round(total_invested, 2),
        "total_current_value": round(total_current_value, 2),
        "total_unrealised_pnl": round(total_unrealised_pnl, 2),
        "total_realised_pnl": round(total_realised_pnl, 2),
        "total_pnl": round(total_unrealised_pnl + total_realised_pnl, 2),
    }

    return {
        "positions": sorted(result, key=lambda x: x["ticker"]),
        "summary": summary,
    }


@app.post("/api/portfolio/buy")
async def portfolio_buy(req: TradeRequest):
    transactions = load_portfolio()
    ticker = req.ticker.upper()
    try:
        info = await get_info_with_timeout(ticker, 2.5)
    except Exception:
        info = {}
    name = info.get("shortName", ticker) if isinstance(info, dict) else ticker
    transactions.append({
        "id": str(uuid.uuid4()),
        "type": "buy",
        "ticker": ticker,
        "name": name,
        "qty": req.qty,
        "price": req.price,
        "date": req.date or str(date.today()),
        "timestamp": datetime.utcnow().isoformat(),
    })
    save_portfolio(transactions)
    return {"ok": True}


@app.post("/api/portfolio/sell")
async def portfolio_sell(req: TradeRequest):
    transactions = load_portfolio()
    ticker = req.ticker.upper()
    positions = compute_positions(transactions)
    held = positions.get(ticker, {}).get("shares", 0)
    if req.qty > held:
        raise HTTPException(status_code=400, detail=f"Cannot sell {req.qty} shares — only {held} held")
    try:
        info = await get_info_with_timeout(ticker, 2.5)
    except Exception:
        info = {}
    name = info.get("shortName", ticker) if isinstance(info, dict) else ticker
    transactions.append({
        "id": str(uuid.uuid4()),
        "type": "sell",
        "ticker": ticker,
        "name": name,
        "qty": req.qty,
        "price": req.price,
        "date": req.date or str(date.today()),
        "timestamp": datetime.utcnow().isoformat(),
    })
    save_portfolio(transactions)
    return {"ok": True}


@app.get("/api/portfolio/transactions")
def get_transactions():
    return load_portfolio()


@app.delete("/api/portfolio/transaction/{tx_id}")
def delete_transaction(tx_id: str, current_user: str = Depends(get_current_user)):
    transactions = load_portfolio()
    transactions = [t for t in transactions if t["id"] != tx_id]
    save_portfolio(transactions)
    logger.info("TRANSACTION_DELETED user=%s tx_id=%s", current_user, tx_id)
    return {"ok": True}


_MAX_CSV_BYTES = 5 * 1024 * 1024   # 5 MB
_MAX_PDF_BYTES = 20 * 1024 * 1024  # 20 MB

@app.post("/api/portfolio/import")
@limiter.limit("10/hour")
async def import_portfolio(request: Request, file: UploadFile = File(...)):
    """
    Import transactions from a CSV file.
    Required columns: type, ticker, qty, price, date
    type must be 'buy' or 'sell'
    date format: YYYY-MM-DD
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

    transactions = load_portfolio()
    imported, skipped = 0, []

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
        save_portfolio(transactions)

    return {
        "imported": imported,
        "skipped":  len(skipped),
        "errors":   skipped,
    }


@app.get("/api/portfolio/template")
def portfolio_template():
    """Return a CSV template for portfolio import."""
    from fastapi.responses import Response
    template = "type,ticker,qty,price,date\nbuy,AAPL,10,175.50,2025-01-15\nbuy,MSFT,5,380.00,2025-02-01\nsell,AAPL,5,182.00,2025-03-10\n"
    return Response(content=template, media_type="text/csv",
                    headers={"Content-Disposition": "attachment; filename=portfolio_template.csv"})


@app.post("/api/portfolio/import-pdf")
@limiter.limit("5/hour")
async def import_portfolio_pdf(request: Request, file: UploadFile = File(...)):
    """
    Import transactions from a Saxo Bank PDF statement.
    Extracts text from the PDF and uses Claude AI to parse transactions.
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
    msg = client.messages.create(
        model=ANTHROPIC_MODEL,
        max_tokens=2048,
        messages=[{"role": "user", "content": prompt}],
    )
    raw = msg.content[0].text.strip()
    if "```" in raw:
        raw = raw.split("```")[1]
        if raw.startswith("json"):
            raw = raw[4:]
        raw = raw.strip().rstrip("```")

    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as e:
        logger.error("PDF import JSON parse error: %s", e)
        raise HTTPException(status_code=500, detail="Could not parse transactions from this PDF. Ensure it contains readable text.")

    if not parsed:
        return {"imported": 0, "skipped": 0, "errors": [], "preview": [], "message": "No transactions found in this PDF."}

    # Validate and import
    transactions = load_portfolio()
    positions    = compute_positions(transactions)
    imported, skipped = 0, []
    preview = []

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
        save_portfolio(transactions)

    return {"imported": imported, "skipped": len(skipped), "errors": skipped, "preview": preview}


# ── Paper Portfolio endpoints ─────────────────────────────────────────────────

@app.get("/api/paper-portfolio")
async def get_paper_portfolio():
    initial_float = get_paper_initial_float()
    transactions = load_paper_portfolio()
    positions    = compute_positions(transactions)
    held         = [t for t, p in positions.items() if p["shares"] > 0]

    for t in held:
        _info_cache.pop(t, None)
    if held:
        infos = await asyncio.gather(*[get_info_with_timeout(t, RECOMMENDATION_INFO_TIMEOUT_SEC) for t in held], return_exceptions=True)
        price_map = {t: _price_from_info_for_alerts(i)
                     for t, i in zip(held, infos) if not isinstance(i, Exception)}
    else:
        price_map = {}

    for ticker in held:
        if not price_map.get(ticker):
            fallback_price = positions.get(ticker, {}).get("avg_cost", 0) or 0
            if fallback_price:
                price_map[ticker] = fallback_price

    # Cash tracking: start at £100k, subtract buys, add sells
    cash = initial_float
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
    total_pnl       = total_value - initial_float
    total_pnl_pct   = round(total_pnl / initial_float * 100, 2) if initial_float else 0.0
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
        "transactions": list(reversed(transactions[-50:])),
        "summary": {
            "initial_float":   initial_float,
            "cash":            round(cash, 2),
            "total_invested":  round(total_invested, 2),
            "total_value":     round(total_value, 2),
            "total_pnl":       round(total_pnl, 2),
            "total_pnl_pct":   total_pnl_pct,
            "realised_pnl":    round(realised_pnl, 2),
        },
    }


@app.get("/api/portfolio/prices")
async def get_portfolio_prices():
    """Lightweight live-price endpoint for near-realtime polling."""
    transactions = load_portfolio()
    positions = compute_positions(transactions)
    held = [t for t, p in positions.items() if p["shares"] > 0]
    for t in held:
        _info_cache.pop(t, None)
    if held:
        infos = await asyncio.gather(*[get_info_with_timeout(t, RECOMMENDATION_INFO_TIMEOUT_SEC) for t in held], return_exceptions=True)
        price_map = {t: _price_from_info_for_alerts(i) for t, i in zip(held, infos) if not isinstance(i, Exception)}
    else:
        return {"prices": {}, "totals": {}}
    for t in held:
        if not price_map.get(t):
            price_map[t] = positions[t].get("avg_cost", 0) or 0
    prices = {}
    total_current_value = 0.0
    total_cost_basis = 0.0
    total_unrealised_pnl = 0.0
    total_realised_pnl = sum(p["realised_pnl"] for p in positions.values())
    for ticker, pos in positions.items():
        if pos["shares"] <= 0:
            continue
        current_price = price_map.get(ticker) or pos["avg_cost"]
        cost_basis = pos["shares"] * pos["avg_cost"]
        current_value = pos["shares"] * current_price
        unrealised_pnl = current_value - cost_basis
        total_current_value += current_value
        total_cost_basis += cost_basis
        total_unrealised_pnl += unrealised_pnl
        prices[ticker] = {
            "current_price":  round(current_price, 2),
            "current_value":  round(current_value, 2),
            "unrealised_pnl": round(unrealised_pnl, 2),
            "unrealised_pct": round((unrealised_pnl / cost_basis * 100) if cost_basis else 0, 2),
        }
    return {
        "prices": prices,
        "totals": {
            "total_current_value":  round(total_current_value, 2),
            "total_unrealised_pnl": round(total_unrealised_pnl, 2),
            "total_pnl":            round(total_unrealised_pnl + total_realised_pnl, 2),
        },
    }


@app.get("/api/paper-portfolio/prices")
async def get_paper_portfolio_prices():
    """Lightweight live-price endpoint for near-realtime polling."""
    initial_float = get_paper_initial_float()
    transactions = load_paper_portfolio()
    positions = compute_positions(transactions)
    held = [t for t, p in positions.items() if p["shares"] > 0]
    for t in held:
        _info_cache.pop(t, None)
    if held:
        infos = await asyncio.gather(*[get_info_with_timeout(t, RECOMMENDATION_INFO_TIMEOUT_SEC) for t in held], return_exceptions=True)
        price_map = {t: _price_from_info_for_alerts(i) for t, i in zip(held, infos) if not isinstance(i, Exception)}
    else:
        return {"prices": {}, "totals": {}}
    for t in held:
        if not price_map.get(t):
            price_map[t] = positions[t].get("avg_cost", 0) or 0
    cash = initial_float
    for tx in transactions:
        qty = float(tx.get("qty", 0))
        price = float(tx.get("price", 0))
        if tx["type"] == "buy":
            cash -= qty * price
        elif tx["type"] == "sell":
            cash += qty * price
    prices = {}
    total_current = 0.0
    total_invested = 0.0
    for ticker, pos in positions.items():
        if pos["shares"] <= 0:
            continue
        current_price = price_map.get(ticker) or pos["avg_cost"]
        cost_basis = pos["shares"] * pos["avg_cost"]
        current_value = pos["shares"] * current_price
        unrealised_pnl = current_value - cost_basis
        total_current += current_value
        total_invested += cost_basis
        prices[ticker] = {
            "current_price":  round(current_price, 2),
            "current_value":  round(current_value, 2),
            "unrealised_pnl": round(unrealised_pnl, 2),
            "unrealised_pct": round((unrealised_pnl / cost_basis * 100) if cost_basis else 0, 2),
        }
    total_value = cash + total_current
    total_pnl = total_value - initial_float
    unrealised = total_current - total_invested
    return {
        "prices": prices,
        "totals": {
            "total_value":   round(total_value, 2),
            "total_pnl":     round(total_pnl, 2),
            "total_pnl_pct": round(total_pnl / initial_float * 100, 2) if initial_float else 0.0,
            "unrealised":    round(unrealised, 2),
        },
    }


@app.post("/api/paper-portfolio/buy")
async def paper_buy(req: TradeRequest):
    initial_float = get_paper_initial_float()
    transactions = load_paper_portfolio()
    cash = initial_float
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
    transactions.append({
        "id":        str(uuid.uuid4()),
        "type":      "buy",
        "ticker":    req.ticker.upper(),
        "name":      name,
        "qty":       req.qty,
        "price":     req.price,
        "date":      req.date or str(date.today()),
        "timestamp": datetime.utcnow().isoformat(),
        "source":    "recommendation",
    })
    save_paper_portfolio(transactions)
    return {"ok": True}


@app.post("/api/paper-portfolio/sell")
async def paper_sell(req: TradeRequest):
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
    transactions.append({
        "id":        str(uuid.uuid4()),
        "type":      "sell",
        "ticker":    req.ticker.upper(),
        "name":      name,
        "qty":       req.qty,
        "price":     req.price,
        "date":      req.date or str(date.today()),
        "timestamp": datetime.utcnow().isoformat(),
        "source":    "recommendation",
    })
    save_paper_portfolio(transactions)
    return {"ok": True}


@app.delete("/api/paper-portfolio/reset")
def reset_paper_portfolio(current_user: str = Depends(get_current_user)):
    save_paper_portfolio([])
    logger.info("PAPER_PORTFOLIO_RESET user=%s", current_user)
    return {"ok": True}


@app.delete("/api/alerts")
def clear_alerts(current_user: str = Depends(get_current_user)):
    save_alerts([])
    logger.info("ALERTS_CLEARED user=%s", current_user)
    return {"message": "Alert history cleared."}
