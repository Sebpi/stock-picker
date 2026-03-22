import asyncio
import json
import os
import secrets
import smtplib
import statistics
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
from fastapi import BackgroundTasks, Depends, FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.staticfiles import StaticFiles
import bcrypt as _bcrypt
from jose import JWTError, jwt
from pydantic import BaseModel

load_dotenv(dotenv_path=Path(__file__).parent / ".env")

# ── Auth setup ─────────────────────────────────────────────────────────────────
_SECRET_KEY = os.getenv("SECRET_KEY") or secrets.token_hex(32)
_ALGORITHM  = "HS256"
_TOKEN_HOURS = 24

def _hash_pw(password: str) -> str:
    return _bcrypt.hashpw(password.encode(), _bcrypt.gensalt()).decode()

def _verify_pw(password: str, hashed: str) -> bool:
    return _bcrypt.checkpw(password.encode(), hashed.encode())

http_bearer = HTTPBearer(auto_error=False)
USERS_FILE  = Path(__file__).parent / "users.json"

# In-memory password reset tokens: token -> (username, expiry)
_reset_tokens: dict[str, tuple[str, datetime]] = {}

# Auth public routes — no JWT required
_AUTH_PUBLIC = {"/api/auth/login", "/api/auth/forgot-password", "/api/auth/reset-password"}

def load_users() -> dict:
    if USERS_FILE.exists():
        return json.loads(USERS_FILE.read_text())
    return {}

def save_users(users: dict):
    USERS_FILE.write_text(json.dumps(users, indent=2))

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

app = FastAPI(title="Stock Picker API")
scheduler = AsyncIOScheduler()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

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
    return FileResponse(FRONTEND_DIR / "index.html")

app.mount("/static", StaticFiles(directory=FRONTEND_DIR), name="static")

WATCHLIST_FILE    = Path(__file__).parent / "watchlist.json"
PREDICTIONS_FILE  = Path(__file__).parent / "predictions.json"
ALERTS_FILE       = Path(__file__).parent / "alerts.json"
PORTFOLIO_FILE    = Path(__file__).parent / "portfolio.json"
SETTINGS_FILE     = Path(__file__).parent / "settings.json"

def load_settings() -> dict:
    defaults = {"initial_float": 100000.0, "target": 200000.0, "target_months": 12}
    if SETTINGS_FILE.exists():
        return {**defaults, **json.loads(SETTINGS_FILE.read_text())}
    return defaults

def save_settings(s: dict):
    SETTINGS_FILE.write_text(json.dumps(s, indent=2))

UNIVERSE = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "TSLA", "BRK-B", "JPM", "JNJ",
    "V", "PG", "UNH", "HD", "MA", "MRK", "ABBV", "CVX", "LLY", "PEP",
    "KO", "AVGO", "COST", "MCD", "TMO", "ACN", "WMT", "DHR", "BAC", "ADBE",
    "CRM", "NEE", "TXN", "PM", "ORCL", "LIN", "RTX", "QCOM", "AMD", "HON",
    "AMGN", "IBM", "CAT", "SBUX", "GS", "SPGI", "BLK", "AXP", "ISRG", "GILD",
]

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
    "ISRG": "Intuitive Surgical Inc.", "GILD": "Gilead Sciences Inc.",
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
alert_cooldown: dict[str, datetime] = {} # ticker -> last alert time
monitor_status = {"last_check": None, "active": False, "checks_run": 0}

# yfinance info cache — 5 minute TTL to avoid redundant network calls
_info_cache: dict[str, tuple[dict, datetime]] = {}
_INFO_TTL = 300  # seconds

async def get_info(ticker: str) -> dict:
    now = datetime.now(timezone.utc)
    if ticker in _info_cache:
        cached, ts = _info_cache[ticker]
        if (now - ts).total_seconds() < _INFO_TTL:
            return cached
    info = await asyncio.to_thread(lambda: yf.Ticker(ticker).info)
    _info_cache[ticker] = (info, now)
    return info


# ── File helpers ──────────────────────────────────────────────────────────────

def load_watchlist() -> list[str]:
    if WATCHLIST_FILE.exists():
        return json.loads(WATCHLIST_FILE.read_text())
    return []

def save_watchlist(tickers: list[str]):
    WATCHLIST_FILE.write_text(json.dumps(tickers))

def load_predictions() -> list[dict]:
    if PREDICTIONS_FILE.exists():
        return json.loads(PREDICTIONS_FILE.read_text())
    return []

def save_predictions(predictions: list[dict]):
    PREDICTIONS_FILE.write_text(json.dumps(predictions[:1000], indent=2))

def calc_fcf_yield(fcf, market_cap) -> Optional[float]:
    return round((fcf / market_cap) * 100, 2) if fcf and market_cap else None

def load_alerts() -> list[dict]:
    if ALERTS_FILE.exists():
        return json.loads(ALERTS_FILE.read_text())
    return []

def save_alerts(alerts: list[dict]):
    ALERTS_FILE.write_text(json.dumps(alerts, indent=2))

def append_alert(entry: dict):
    alerts = load_alerts()
    alerts.insert(0, entry)
    save_alerts(alerts[:500])  # keep latest 500


def load_portfolio() -> list[dict]:
    if PORTFOLIO_FILE.exists():
        return json.loads(PORTFOLIO_FILE.read_text())
    return []

def save_portfolio(transactions: list[dict]):
    PORTFOLIO_FILE.write_text(json.dumps(transactions, indent=2))

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


# ── Notifications ─────────────────────────────────────────────────────────────

def send_email(subject: str, body: str) -> bool:
    smtp_host = os.getenv("SMTP_HOST", "smtp.gmail.com")
    smtp_port = int(os.getenv("SMTP_PORT", "587"))
    smtp_user = os.getenv("SMTP_USER", "")
    smtp_pass = os.getenv("SMTP_PASS", "")
    alert_email = os.getenv("ALERT_EMAIL", "")

    if not all([smtp_user, smtp_pass, alert_email]):
        return False

    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = smtp_user
        msg["To"] = alert_email
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


# ── Stock monitoring (runs every 5 min) ───────────────────────────────────────

async def monitor_stocks():
    if not is_market_open():
        return
    watchlist = load_watchlist()
    if not watchlist:
        return

    swing_threshold  = float(os.getenv("SWING_THRESHOLD_PCT", "3.0"))
    momentum_threshold = float(os.getenv("MOMENTUM_THRESHOLD_PCT", "1.5"))
    volume_multiplier = float(os.getenv("VOLUME_SURGE_MULTIPLIER", "3.0"))
    cooldown_minutes  = int(os.getenv("ALERT_COOLDOWN_MINUTES", "30"))

    now = datetime.now(timezone.utc)
    monitor_status["last_check"] = now.isoformat()
    monitor_status["checks_run"] += 1

    async def fetch_info(ticker):
        return ticker, await asyncio.to_thread(lambda: yf.Ticker(ticker).info)

    results = await asyncio.gather(*[fetch_info(t) for t in watchlist], return_exceptions=True)

    pending_alerts = []  # collect all alerts before sending

    for result in results:
        if isinstance(result, Exception):
            continue
        ticker, info = result
        try:
            current_price = info.get("currentPrice") or info.get("regularMarketPrice")
            prev_close    = info.get("previousClose")
            avg_volume    = info.get("averageVolume") or 1
            current_volume = info.get("volume") or 0
            name = info.get("shortName", ticker)

            if not current_price:
                continue

            triggered = []

            # 1. Daily swing from previous close
            if prev_close and prev_close > 0:
                daily_chg = ((current_price - prev_close) / prev_close) * 100
                if abs(daily_chg) >= swing_threshold:
                    direction = "UP" if daily_chg > 0 else "DOWN"
                    triggered.append({
                        "type": "daily_swing",
                        "signal": f"{direction} {daily_chg:+.2f}% from prev close",
                        "change_pct": round(daily_chg, 2),
                    })

            # 2. Rapid 5-min momentum
            if ticker in price_cache:
                last_price = price_cache[ticker]
                if last_price > 0:
                    momentum = ((current_price - last_price) / last_price) * 100
                    if abs(momentum) >= momentum_threshold:
                        direction = "surging" if momentum > 0 else "dropping"
                        triggered.append({
                            "type": "momentum",
                            "signal": f"Rapid move {momentum:+.2f}% in last 5 mins ({direction})",
                            "change_pct": round(momentum, 2),
                        })

            # 3. Volume surge
            if avg_volume > 0 and current_volume >= avg_volume * volume_multiplier:
                ratio = current_volume / avg_volume
                triggered.append({
                    "type": "volume_surge",
                    "signal": f"Volume surge {ratio:.1f}x normal ({current_volume:,} vs avg {avg_volume:,})",
                    "change_pct": None,
                })

            price_cache[ticker] = current_price

            if not triggered:
                continue

            # Cooldown check — don't spam alerts
            last_alerted = alert_cooldown.get(ticker)
            if last_alerted:
                elapsed = (now - last_alerted).total_seconds()
                if elapsed < cooldown_minutes * 60:
                    continue

            alert_cooldown[ticker] = now

            pending_alerts.append({
                "ticker": ticker,
                "name": name,
                "price": current_price,
                "triggered": triggered,
            })
            print(f"[Monitor] Alert fired for {ticker}: {[t['signal'] for t in triggered]}")

        except Exception as e:
            print(f"[Monitor] Error checking {ticker}: {e}")

    # Send one batched email for all alerts in this check cycle
    if pending_alerts:
        time_str = now.astimezone(ET).strftime('%Y-%m-%d %H:%M ET')
        subject = f"Stock Alerts ({len(pending_alerts)} ticker{'s' if len(pending_alerts) > 1 else ''}) — {time_str}"

        body_lines = [
            f"Stock Picker Alerts — {time_str}",
            "=" * 40,
            "",
        ]
        sms_lines = [f"StockPicker Alerts {time_str}"]

        for alert in pending_alerts:
            signals_text = "\n".join(f"    • {t['signal']}" for t in alert["triggered"])
            body_lines += [
                f"Ticker:  {alert['ticker']} ({alert['name']})",
                f"Price:   ${alert['price']:.2f}",
                f"Signals:\n{signals_text}",
                "",
            ]
            sms_lines.append(f"{alert['ticker']} ${alert['price']:.2f}: {alert['triggered'][0]['signal']}")

        body_lines.append("⚠️  This is not financial advice. Always do your own research.")
        body = "\n".join(body_lines)
        sms_body = "\n".join(sms_lines)

        emailed = send_email(subject, body)
        texted  = send_sms(sms_body[:1600])

        for alert in pending_alerts:
            record = {
                "id": str(uuid.uuid4()),
                "timestamp": now.isoformat(),
                "ticker": alert["ticker"],
                "name": alert["name"],
                "price": alert["price"],
                "signals": alert["triggered"],
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
        await generate_predictions()
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
    # Create default admin account on first run
    users = load_users()
    if not users:
        default_pass = "stockpicker123"
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

    scheduler.add_job(monitor_stocks, "interval", minutes=5,  id="monitor")
    scheduler.add_job(auto_predict,   "interval", minutes=15, id="predictions")
    scheduler.start()
    print("[Monitor] Stock monitor started — checking every 5 minutes during market hours.")
    print("[Predictions] Auto-prediction scheduled every 15 minutes during market hours.")

@app.on_event("shutdown")
async def shutdown():
    scheduler.shutdown()


# ── Auth endpoints ────────────────────────────────────────────────────────────

@app.post("/api/auth/login")
async def login(req: LoginRequest):
    users = load_users()
    user = users.get(req.username)
    if not user or not _verify_pw(req.password, user["hashed_password"]):
        raise HTTPException(status_code=401, detail="Invalid username or password")
    token = create_access_token(req.username)
    return {"access_token": token, "token_type": "bearer"}

@app.post("/api/auth/forgot-password")
async def forgot_password(req: ForgotPasswordRequest):
    users = load_users()
    user = users.get(req.username)
    if user:
        token = secrets.token_urlsafe(32)
        _reset_tokens[token] = (req.username, datetime.now(timezone.utc) + timedelta(minutes=15))
        reset_link = f"http://192.168.1.19:8000/?reset_token={token}"
        result = send_email(
            "Stock Picker - Password Reset",
            f"Click the link below to reset your password (valid 15 minutes):\n\n{reset_link}\n\nIf you did not request this, ignore this email.",
        )
        print(f"[Auth] Password reset email sent for '{req.username}': {result}")
    # Always return ok to avoid revealing valid usernames
    return {"ok": True, "message": "If that username exists, a reset link has been sent to the registered email."}

@app.post("/api/auth/reset-password")
async def reset_password(req: ResetPasswordRequest):
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
    return {"ok": True}

@app.post("/api/auth/change-password")
async def change_password(req: ChangePasswordRequest, current_user: str = Depends(get_current_user)):
    users = load_users()
    user = users[current_user]
    if not _verify_pw(req.current_password, user["hashed_password"]):
        raise HTTPException(status_code=400, detail="Current password is incorrect")
    users[current_user]["hashed_password"] = _hash_pw(req.new_password)
    save_users(users)
    return {"ok": True}

@app.get("/api/auth/me")
async def me(current_user: str = Depends(get_current_user)):
    return {"username": current_user}

# ── Existing endpoints ────────────────────────────────────────────────────────

@app.get("/api/screen")
async def screen_stocks(
    sector: Optional[str] = None,
    min_market_cap: Optional[float] = None,
    max_pe: Optional[float] = None,
    max_peg: Optional[float] = None,
    max_pb: Optional[float] = None,
    max_ev_ebitda: Optional[float] = None,
    min_fcf_yield: Optional[float] = None,
    min_volume: Optional[float] = None,
):
    infos = await asyncio.gather(*[get_info(t) for t in UNIVERSE], return_exceptions=True)

    results = []
    for ticker, info in zip(UNIVERSE, infos):
        if isinstance(info, Exception):
            continue
        try:
            stock_sector = info.get("sector", "")
            market_cap   = info.get("marketCap")
            pe           = info.get("trailingPE")
            peg          = info.get("pegRatio")
            pb           = info.get("priceToBook")
            ev_ebitda    = info.get("enterpriseToEbitda")
            fcf          = info.get("freeCashflow")
            volume       = info.get("averageVolume")
            price        = info.get("currentPrice") or info.get("regularMarketPrice")
            name         = info.get("shortName", ticker)
            fcf_yield    = calc_fcf_yield(fcf, market_cap)

            if sector and sector.lower() not in stock_sector.lower():
                continue
            if min_market_cap and (not market_cap or market_cap < min_market_cap):
                continue
            if max_pe and pe and pe > max_pe:
                continue
            if max_peg and peg and peg > max_peg:
                continue
            if max_pb and pb and pb > max_pb:
                continue
            if max_ev_ebitda and ev_ebitda and ev_ebitda > max_ev_ebitda:
                continue
            if min_fcf_yield and (not fcf_yield or fcf_yield < min_fcf_yield):
                continue
            if min_volume and (not volume or volume < min_volume):
                continue

            results.append({
                "ticker": ticker, "name": name, "sector": stock_sector,
                "price": round(price, 2) if price else None,
                "pe": round(pe, 2) if pe else None,
                "peg": round(peg, 2) if peg else None,
                "pb": round(pb, 2) if pb else None,
                "ev_ebitda": round(ev_ebitda, 2) if ev_ebitda else None,
                "fcf_yield": fcf_yield,
                "market_cap": market_cap, "volume": volume,
            })
        except Exception:
            continue
    return results


@app.get("/api/stock/{ticker}")
def get_stock(ticker: str):
    try:
        t = yf.Ticker(ticker.upper())
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
    except Exception as e:
        raise HTTPException(status_code=404, detail=str(e))


@app.get("/api/stock/{ticker}/peers")
async def get_peer_valuation(ticker: str):
    ticker = ticker.upper()
    try:
        info = await get_info(ticker)
    except Exception as e:
        raise HTTPException(status_code=404, detail=str(e))

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
    peer_infos = await asyncio.gather(*[get_info(t) for t in peer_candidates], return_exceptions=True)
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


async def _run_predictions_bg():
    try:
        print("[Predictions] Background generation triggered by watchlist add...")
        await generate_predictions()
        print("[Predictions] Background generation completed.")
    except Exception as e:
        print(f"[Predictions] Background generation failed: {e}")


@app.post("/api/watchlist/{ticker}")
async def add_to_watchlist(ticker: str, background_tasks: BackgroundTasks):
    tickers = load_watchlist()
    ticker = ticker.upper()
    if ticker not in tickers:
        tickers.append(ticker)
        save_watchlist(tickers)
        background_tasks.add_task(_run_predictions_bg)
    return {"watchlist": tickers}


@app.delete("/api/watchlist/{ticker}")
def remove_from_watchlist(ticker: str):
    tickers = load_watchlist()
    ticker = ticker.upper()
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
    message = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=1024,
        messages=[{"role": "user", "content": (
            "You are a helpful stock market analyst. The user is looking for stock recommendations. "
            "Provide thoughtful analysis and suggest specific tickers with reasoning. "
            "Always include a disclaimer that this is not financial advice.\n\n"
            f"User request: {req.query}"
        )}],
    )
    return {"response": message.content[0].text}


# ── Predictions endpoints ─────────────────────────────────────────────────────

def update_actuals(predictions: list[dict]) -> tuple[list[dict], bool]:
    today = str(date.today())
    updated = False
    for pred in predictions:
        if pred.get("actual_pct") is not None:
            continue
        if pred["date"] >= today:
            continue
        try:
            hist = yf.Ticker(pred["ticker"]).history(period="10d")
            dates = [str(d.date()) for d in hist.index]
            if pred["date"] in dates:
                idx = dates.index(pred["date"])
                if idx + 1 < len(dates):
                    p0 = float(hist["Close"].iloc[idx])
                    p1 = float(hist["Close"].iloc[idx + 1])
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
                root = ET.fromstring(r.text)
                for item in root.findall(".//item")[:limit_per_feed]:
                    title = item.find("title")
                    if title is not None and title.text:
                        headlines.append(f"[{source}] {title.text.strip()}")
            except Exception:
                continue
    return headlines


def fetch_macro_data() -> dict:
    macro = {}
    for sym, label in MACRO_SYMBOLS.items():
        try:
            hist = yf.Ticker(sym).history(period="5d")
            if len(hist) >= 2:
                latest = float(hist["Close"].iloc[-1])
                prev = float(hist["Close"].iloc[-2])
                chg = ((latest - prev) / prev) * 100
                macro[label] = {"price": round(latest, 2), "change_pct": round(chg, 2)}
        except Exception:
            continue
    return macro


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
    predictions = load_predictions()
    predictions, updated = update_actuals(predictions)
    # Backfill missing names from lookup table
    for p in predictions:
        if not p.get("name"):
            p["name"] = TICKER_NAMES.get(p["ticker"], p["ticker"])
            updated = True
    if updated:
        save_predictions(predictions)
    sorted_preds = sorted(predictions, key=lambda p: p["date"], reverse=True)

    # Always show watchlist stocks — add stub rows for any never analysed
    predicted_tickers = {p["ticker"] for p in predictions}
    for ticker in load_watchlist():
        if ticker not in predicted_tickers:
            sorted_preds.append({
                "date": "",
                "ticker": ticker,
                "name": TICKER_NAMES.get(ticker, ticker),
                "predicted_pct": None,
                "confidence": "pending",
                "reasoning": "Not yet analysed. Click Generate Predictions to include this stock.",
                "actual_pct": None,
                "price_at_prediction": None,
            })
    return sorted_preds


@app.post("/api/predictions/generate")
async def generate_predictions():
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
    universe_fill = [t for t in UNIVERSE[:20] if t not in already_predicted and t not in watchlist_tickers]
    to_analyze = list(dict.fromkeys(watchlist_missing + universe_fill))[:20]

    if not to_analyze:
        if updated:
            save_predictions(predictions)
        return {
            "message": "Predictions already generated for today.",
            "predictions": [p for p in predictions if p["date"] == today],
        }

    macro = fetch_macro_data()
    headlines = await fetch_rss_headlines(RSS_FEEDS)

    stocks_data = []
    for ticker in to_analyze:
        try:
            t = yf.Ticker(ticker)
            info = t.info
            hist = t.history(period="5d")
            news = t.news[:3] if hasattr(t, "news") else []
            recent_chg = 0.0
            if len(hist) >= 2:
                recent_chg = ((float(hist["Close"].iloc[-1]) - float(hist["Close"].iloc[-2]))
                              / float(hist["Close"].iloc[-2])) * 100
            mc = info.get("marketCap")
            fcf_yield = calc_fcf_yield(info.get("freeCashflow"), mc)

            stocks_data.append({
                "ticker": ticker,
                "name": info.get("shortName", ticker),
                "sector": info.get("sector", ""),
                "price": info.get("currentPrice") or info.get("regularMarketPrice"),
                # Valuation
                "pe": info.get("trailingPE"),
                "peg": info.get("pegRatio"),
                "pb": info.get("priceToBook"),
                "ev_ebitda": info.get("enterpriseToEbitda"),
                "fcf_yield_pct": fcf_yield,
                # Quality
                "eps_growth_yoy": info.get("earningsGrowth"),
                "revenue_growth_yoy": info.get("revenueGrowth"),
                "profit_margin": info.get("profitMargins"),
                "debt_to_equity": info.get("debtToEquity"),
                "beta": info.get("beta"),
                "short_float": info.get("shortPercentOfFloat"),
                # Momentum
                "recent_5d_change_pct": round(recent_chg, 2),
                "recent_news": [n.get("title", "") for n in news if n.get("title")],
            })
        except Exception:
            continue

    calibration = compute_calibration(predictions)

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

    prompt = f"""You are a quantitative stock analyst with one goal: identify stocks with potential for >10% monthly returns (~0.5%+ per day).

Today: {today}

=== MACROECONOMIC CONDITIONS ===
{json.dumps(macro, indent=2)}

=== MARKET & FINANCIAL NEWS ===
{chr(10).join(headlines[:20]) if headlines else "No headlines fetched."}

=== STOCKS TO ANALYZE ===
Each stock includes a pre-calculated `sentiment_score` (%) built from:
  - VIX fear adjustment
  - S&P 500 5-day momentum adjustment
  - Beta multiplier (amplifies market moves for high/low beta stocks)
  - Headline sentiment (keyword scan: +0.3% per positive signal, -0.3% per negative)

{json.dumps(stocks_data, indent=2)}
{accuracy_summary}
=== SCORING RULES ===
1. START with `sentiment_score` as your baseline predicted_pct for each stock.
2. ADJUST up or down based on fundamentals (valuation + quality):
   - Strong fundamentals (PEG<1, FCF yield>6%, high margins, low debt): add up to +1.0%
   - Weak fundamentals (P/E>30, negative FCF, declining revenue): subtract up to -1.0%
   - Neutral fundamentals: no adjustment

VALUATION THRESHOLDS:
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
- "high": sentiment_score positive AND strong fundamentals support it
- "medium": mixed signals or at least one notable risk factor
- "low": negative sentiment_score OR meaningful bearish fundamentals present
If your reasoning mentions a sell-off, downtrend, overvaluation, or risk — confidence MUST be "low" or "medium", never "high".

Return ONLY a valid JSON array, no explanation, no markdown:
[
  {{
    "ticker": "AAPL",
    "predicted_pct": 1.2,
    "confidence": "high",
    "reasoning": "Sentiment score +0.4% (VIX calm, low beta). PEG 0.8 undervalued. FCF yield 6.2% strong. Adjusted +0.8% above baseline for quality fundamentals."
  }}
]
confidence: "low" | "medium" | "high"."""

    client = anthropic.Anthropic(api_key=api_key)
    msg = client.messages.create(
        model="claude-sonnet-4-6", max_tokens=8192,
        messages=[{"role": "user", "content": prompt}],
    )
    raw = msg.content[0].text.strip()
    if "```" in raw:
        raw = raw.split("```")[1]
        if raw.startswith("json"):
            raw = raw[4:]
        raw = raw.strip().rstrip("```")

    try:
        claude_preds = json.loads(raw)
    except json.JSONDecodeError as e:
        print(f"[Predictions] JSON parse error: {e}\nRaw response (first 500 chars): {raw[:500]}")
        raise HTTPException(status_code=500, detail=f"Claude returned invalid JSON: {e}")
    price_map = {s["ticker"]: s["price"] for s in stocks_data}
    name_map  = {s["ticker"]: s["name"]  for s in stocks_data}
    new_preds = []
    seen_today = {p["ticker"] for p in predictions if p["date"] == today}
    for cp in claude_preds:
        ticker = cp["ticker"].upper()
        if ticker in seen_today:
            continue  # skip duplicates
        seen_today.add(ticker)

        raw_pct = cp["predicted_pct"]
        cal     = calibration.get(ticker, {})

        # 1. Bias correction — shift prediction by historical mean error
        bias         = cal.get("mean_bias", 0.0)
        bias_applied = abs(bias) >= 0.1   # only correct if systematic (>=0.1%)
        corrected    = round(raw_pct + bias, 2) if bias_applied else raw_pct

        # 2. Signal inversion — flip direction if model has been consistently wrong
        inverted  = cal.get("inverted", False)
        final_pct = round(-corrected, 2) if inverted else corrected

        # Append calibration notes to reasoning
        cal_note = ""
        if bias_applied:
            cal_note += f" [Bias corrected {bias:+.2f}%: raw={raw_pct:+.2f}%]"
        if inverted:
            cal_note += f" [Direction INVERTED — historical accuracy {cal['accuracy_pct']}%, signal flipped]"

        entry = {
            "date":               today,
            "ticker":             ticker,
            "name":               name_map.get(ticker, ""),
            "predicted_pct":      final_pct,
            "raw_predicted_pct":  raw_pct,
            "bias_correction":    round(bias, 3) if bias_applied else 0.0,
            "inverted":           inverted,
            "confidence":         cp.get("confidence", "medium"),
            "reasoning":          cp.get("reasoning", "") + cal_note,
            "actual_pct":         None,
            "price_at_prediction": price_map.get(ticker),
            "generated_at":       datetime.utcnow().isoformat(),
        }
        predictions.append(entry)
        new_preds.append(entry)

    save_predictions(predictions)
    return {"predictions": new_preds}


@app.get("/api/predictions/backtest")
async def backtest_predictions():
    """Replay 4 weeks of historical data through the sentiment scoring model and compare against actual returns."""
    watchlist = load_watchlist()
    end_date   = date.today()
    start_date = end_date - timedelta(weeks=6)  # 6 weeks buffer to get clean 4-week window

    # Fetch historical VIX and S&P 500
    vix_hist = yf.Ticker("^VIX").history(start=str(start_date), end=str(end_date))
    sp_hist  = yf.Ticker("^GSPC").history(start=str(start_date), end=str(end_date))
    vix_dates  = [d.date() for d in vix_hist.index]
    sp_dates   = [d.date() for d in sp_hist.index]
    vix_closes = list(vix_hist["Close"])
    sp_closes  = list(sp_hist["Close"])

    results = []

    for ticker in watchlist:
        try:
            t    = yf.Ticker(ticker)
            info = t.info
            hist = t.history(start=str(start_date), end=str(end_date))
            if len(hist) < 3:
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

                # VIX on this day
                vix_val = vix_closes[vix_dates.index(trade_date)] if trade_date in vix_dates else 20.0
                if vix_val < 12:   vix_adj = 0.4
                elif vix_val < 15: vix_adj = 0.2
                elif vix_val < 20: vix_adj = 0.0
                elif vix_val < 25: vix_adj = -0.3
                elif vix_val < 30: vix_adj = -0.6
                else:              vix_adj = -1.0

                # S&P 500 5-day momentum on this day
                sp_5d_chg = 0.0
                if trade_date in sp_dates:
                    si = sp_dates.index(trade_date)
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
        return {"results": [], "summary": {}, "by_ticker": {}}

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
    }


@app.delete("/api/predictions")
def clear_predictions():
    save_predictions([])
    return {"message": "All predictions cleared."}


# ── Alerts endpoints ──────────────────────────────────────────────────────────

@app.get("/api/alerts")
def get_alerts():
    return load_alerts()


@app.get("/api/alerts/status")
def get_monitor_status():
    watchlist = load_watchlist()
    swing  = os.getenv("SWING_THRESHOLD_PCT", "3.0")
    moment = os.getenv("MOMENTUM_THRESHOLD_PCT", "1.5")
    volume = os.getenv("VOLUME_SURGE_MULTIPLIER", "3.0")
    email_configured = bool(os.getenv("SMTP_USER") and os.getenv("SMTP_PASS") and os.getenv("ALERT_EMAIL"))
    sms_configured   = bool(os.getenv("TWILIO_ACCOUNT_SID") and os.getenv("TWILIO_AUTH_TOKEN"))
    return {
        "active": monitor_status["active"],
        "last_check": monitor_status["last_check"],
        "checks_run": monitor_status["checks_run"],
        "watching": len(watchlist),
        "tickers": watchlist,
        "thresholds": {
            "daily_swing_pct": float(swing),
            "momentum_5min_pct": float(moment),
            "volume_surge_multiplier": float(volume),
        },
        "notifications": {
            "email": email_configured,
            "sms": sms_configured,
        },
    }


@app.post("/api/alerts/test")
def test_alert():
    emailed = send_email(
        "Stock Picker — Test Alert",
        "This is a test alert from your Stock Picker app.\nNotifications are working correctly."
    )
    texted = send_sms("StockPicker test alert — notifications working!")
    return {"email_sent": emailed, "sms_sent": texted}


# ── Recommendations endpoint ──────────────────────────────────────────────────

@app.get("/api/recommendations")
async def get_recommendations():
    settings       = load_settings()
    initial_float  = settings["initial_float"]
    target         = settings["target"]
    target_months  = settings.get("target_months", 12)

    predictions    = load_predictions()
    calibration    = compute_calibration(predictions)

    # Use today's predictions; fall back to most recent date
    dated = [p for p in predictions if p.get("predicted_pct") is not None]
    if not dated:
        return {"buys": [], "sells": [], "summary": {}}
    latest_date  = max(p["date"] for p in dated)
    latest_preds = [p for p in dated if p["date"] == latest_date]

    # Compute live portfolio positions
    transactions = load_portfolio()
    positions    = compute_positions(transactions)
    held         = [t for t, p in positions.items() if p["shares"] > 0]

    all_tickers = list(set(held + [p["ticker"] for p in latest_preds]))
    infos = await asyncio.gather(*[get_info(t) for t in all_tickers], return_exceptions=True)
    price_map = {}
    for ticker, info in zip(all_tickers, infos):
        if not isinstance(info, Exception):
            price_map[ticker] = info.get("currentPrice") or info.get("regularMarketPrice") or 0

    total_invested  = sum(positions[t]["shares"] * positions[t]["avg_cost"] for t in held)
    total_current   = sum(positions[t]["shares"] * price_map.get(t, 0) for t in held)
    total_realised  = sum(positions[t]["realised_pnl"] for t in positions)
    available_cash  = max(0.0, initial_float - total_invested)
    total_value     = available_cash + total_current
    total_pnl       = (total_current - total_invested) + total_realised
    progress_pct    = round(total_value / target * 100, 1)

    # ── BUY recommendations ─────────────────────────────────────────────────
    buys = []
    for pred in latest_preds:
        ticker      = pred["ticker"]
        predicted   = pred.get("predicted_pct") or 0
        confidence  = pred.get("confidence", "medium")
        cal         = calibration.get(ticker, {})
        accuracy    = cal.get("accuracy_pct", 50) / 100

        if predicted <= 0.3 or confidence == "low" or available_cash < 500:
            continue

        current_price = price_map.get(ticker, 0)
        if not current_price:
            continue

        alloc_pct = 0.15 if confidence == "high" else 0.08
        accuracy_bonus = max(0, (accuracy - 0.5) * 0.4)
        alloc_pct = min(alloc_pct * (1 + accuracy_bonus), 0.20)

        position_value = min(available_cash * alloc_pct, initial_float * 0.15)
        qty = int(position_value / current_price)
        if qty < 1:
            continue

        estimated_cost = round(qty * current_price, 2)
        score = predicted * accuracy * (1.5 if confidence == "high" else 1.0)

        buys.append({
            "ticker":         ticker,
            "name":           pred.get("name", ticker),
            "action":         "BUY",
            "trigger":        "PREDICTION",
            "current_price":  round(current_price, 2),
            "qty":            qty,
            "estimated_cost": estimated_cost,
            "predicted_pct":  predicted,
            "confidence":     confidence,
            "accuracy_pct":   cal.get("accuracy_pct"),
            "reasoning":      pred.get("reasoning", ""),
            "score":          round(score, 4),
        })

    buys.sort(key=lambda x: x["score"], reverse=True)

    # ── SELL recommendations ────────────────────────────────────────────────
    sells = []
    for ticker in held:
        pos           = positions[ticker]
        current_price = price_map.get(ticker, 0)
        if not current_price:
            continue

        cost_basis      = pos["shares"] * pos["avg_cost"]
        current_value   = pos["shares"] * current_price
        unrealised_pnl  = current_value - cost_basis
        unrealised_pct  = (unrealised_pnl / cost_basis * 100) if cost_basis else 0

        pred    = next((p for p in latest_preds if p["ticker"] == ticker), None)
        trigger = None
        reasoning = ""

        if pred and (pred.get("predicted_pct") or 0) < -0.3 and pred.get("confidence") in ("high", "medium"):
            trigger   = "PREDICTION"
            reasoning = pred.get("reasoning", "")
        elif unrealised_pct >= 8.0:
            trigger   = "TAKE PROFIT"
            reasoning = f"Position up {unrealised_pct:.1f}% — take profit target reached."
        elif unrealised_pct <= -5.0:
            trigger   = "STOP LOSS"
            reasoning = f"Position down {unrealised_pct:.1f}% — stop loss triggered."

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
                "predicted_pct":       pred.get("predicted_pct") if pred else None,
                "confidence":          pred.get("confidence") if pred else None,
                "reasoning":           reasoning,
            })

    return {
        "buys":            buys,
        "sells":           sells,
        "prediction_date": latest_date,
        "summary": {
            "initial_float":        initial_float,
            "target":               target,
            "target_months":        target_months,
            "available_cash":       round(available_cash, 2),
            "total_invested":       round(total_invested, 2),
            "total_current_value":  round(total_current, 2),
            "total_portfolio_value": round(total_value, 2),
            "total_unrealised_pnl": round(total_current - total_invested, 2),
            "total_realised_pnl":   round(total_realised, 2),
            "total_pnl":            round(total_pnl, 2),
            "progress_pct":         progress_pct,
            "remaining_to_target":  round(target - total_value, 2),
        },
    }


@app.get("/api/settings")
def get_settings():
    return load_settings()


@app.post("/api/settings")
def update_settings(s: dict):
    save_settings(s)
    return load_settings()


# ── Portfolio endpoints ───────────────────────────────────────────────────────

@app.get("/api/portfolio")
async def get_portfolio():
    transactions = load_portfolio()
    positions = compute_positions(transactions)

    # Fetch current prices for all held tickers concurrently
    held = [t for t, p in positions.items() if p["shares"] > 0]
    if held:
        infos = await asyncio.gather(*[get_info(t) for t in held], return_exceptions=True)
        price_map = {}
        for ticker, info in zip(held, infos):
            if not isinstance(info, Exception):
                price_map[ticker] = info.get("currentPrice") or info.get("regularMarketPrice") or 0
    else:
        price_map = {}

    result = []
    total_invested = 0.0
    total_current = 0.0
    total_realised = 0.0

    for ticker, pos in positions.items():
        current_price = price_map.get(ticker, 0)
        cost_basis = pos["shares"] * pos["avg_cost"]
        current_value = pos["shares"] * current_price
        unrealised_pnl = current_value - cost_basis

        total_invested += cost_basis
        total_current += current_value
        total_realised += pos["realised_pnl"]

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

    return {
        "positions": sorted(result, key=lambda x: x["ticker"]),
        "summary": {
            "total_invested": round(total_invested, 2),
            "total_current_value": round(total_current, 2),
            "total_unrealised_pnl": round(total_current - total_invested, 2),
            "total_realised_pnl": round(total_realised, 2),
            "total_pnl": round((total_current - total_invested) + total_realised, 2),
        },
    }


@app.post("/api/portfolio/buy")
async def portfolio_buy(req: TradeRequest):
    transactions = load_portfolio()
    ticker = req.ticker.upper()
    info = await get_info(ticker)
    name = info.get("shortName", ticker) if not isinstance(info, Exception) else ticker
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
    info = await get_info(ticker)
    name = info.get("shortName", ticker) if not isinstance(info, Exception) else ticker
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
def delete_transaction(tx_id: str):
    transactions = load_portfolio()
    transactions = [t for t in transactions if t["id"] != tx_id]
    save_portfolio(transactions)
    return {"ok": True}


@app.delete("/api/alerts")
def clear_alerts():
    save_alerts([])
    return {"message": "Alert history cleared."}
