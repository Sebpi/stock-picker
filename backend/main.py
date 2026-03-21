import asyncio
import json
import os
import smtplib
import uuid
import xml.etree.ElementTree as ET
from datetime import date, datetime, time, timezone
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
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

load_dotenv(dotenv_path=Path(__file__).parent / ".env")

app = FastAPI(title="Stock Picker API")
scheduler = AsyncIOScheduler()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

FRONTEND_DIR = Path(__file__).parent.parent / "frontend"

@app.get("/", include_in_schema=False)
def serve_index():
    return FileResponse(FRONTEND_DIR / "index.html")

app.mount("/static", StaticFiles(directory=FRONTEND_DIR), name="static")

WATCHLIST_FILE    = Path(__file__).parent / "watchlist.json"
PREDICTIONS_FILE  = Path(__file__).parent / "predictions.json"
ALERTS_FILE       = Path(__file__).parent / "alerts.json"

UNIVERSE = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "TSLA", "BRK-B", "JPM", "JNJ",
    "V", "PG", "UNH", "HD", "MA", "MRK", "ABBV", "CVX", "LLY", "PEP",
    "KO", "AVGO", "COST", "MCD", "TMO", "ACN", "WMT", "DHR", "BAC", "ADBE",
    "CRM", "NEE", "TXN", "PM", "ORCL", "LIN", "RTX", "QCOM", "AMD", "HON",
    "AMGN", "IBM", "CAT", "SBUX", "GS", "SPGI", "BLK", "AXP", "ISRG", "GILD",
]

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

            # Build alert record
            signals_text = "\n".join(f"  • {t['signal']}" for t in triggered)
            subject = f"Stock Alert: {ticker} — {triggered[0]['signal']} [{now.astimezone(ET).strftime('%H:%M ET')}]"
            body = (
                f"Stock Picker Alert\n"
                f"{'='*40}\n"
                f"Ticker:  {ticker} ({name})\n"
                f"Price:   ${current_price:.2f}\n"
                f"Time:    {now.strftime('%Y-%m-%d %H:%M UTC')}\n\n"
                f"Signals detected:\n{signals_text}\n\n"
                f"⚠️  This is not financial advice. Always do your own research.\n"
            )

            emailed = send_email(subject, body)
            texted  = send_sms(f"StockPicker 📈 {ticker} ${current_price:.2f}\n{signals_text}")

            record = {
                "id": str(uuid.uuid4()),
                "timestamp": now.isoformat(),
                "ticker": ticker,
                "name": name,
                "price": current_price,
                "signals": triggered,
                "notified_email": emailed,
                "notified_sms": texted,
            }
            append_alert(record)
            print(f"[Monitor] Alert fired for {ticker}: {[t['signal'] for t in triggered]}")

        except Exception as e:
            print(f"[Monitor] Error checking {ticker}: {e}")

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


@app.on_event("startup")
async def startup():
    scheduler.add_job(monitor_stocks, "interval", minutes=5,  id="monitor")
    scheduler.add_job(auto_predict,   "interval", minutes=15, id="predictions")
    scheduler.start()
    print("[Monitor] Stock monitor started — checking every 5 minutes during market hours.")
    print("[Predictions] Auto-prediction scheduled every 15 minutes during market hours.")

@app.on_event("shutdown")
async def shutdown():
    scheduler.shutdown()


# ── Existing endpoints ────────────────────────────────────────────────────────

@app.get("/api/screen")
def screen_stocks(
    sector: Optional[str] = None,
    min_market_cap: Optional[float] = None,
    max_pe: Optional[float] = None,
    max_peg: Optional[float] = None,
    max_pb: Optional[float] = None,
    max_ev_ebitda: Optional[float] = None,
    min_fcf_yield: Optional[float] = None,
    min_volume: Optional[float] = None,
):
    results = []
    for ticker in UNIVERSE:
        try:
            info = yf.Ticker(ticker).info
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

            fcf_yield = calc_fcf_yield(fcf, market_cap)

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


@app.get("/api/watchlist")
def get_watchlist():
    tickers = load_watchlist()
    results = []
    for ticker in tickers:
        try:
            info = yf.Ticker(ticker).info
            price = info.get("currentPrice") or info.get("regularMarketPrice")
            change_pct = info.get("regularMarketChangePercent")
            results.append({
                "ticker": ticker,
                "name": info.get("shortName", ticker),
                "price": round(price, 2) if price else None,
                "change_pct": round(change_pct, 2) if change_pct else None,
            })
        except Exception:
            results.append({"ticker": ticker, "name": ticker, "price": None, "change_pct": None})
    return results


@app.post("/api/watchlist/{ticker}")
def add_to_watchlist(ticker: str):
    tickers = load_watchlist()
    ticker = ticker.upper()
    if ticker not in tickers:
        tickers.append(ticker)
        save_watchlist(tickers)
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


@app.get("/api/predictions")
def get_predictions():
    predictions = load_predictions()
    predictions, updated = update_actuals(predictions)
    if updated:
        save_predictions(predictions)
    return sorted(predictions, key=lambda p: p["date"], reverse=True)


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
    candidates = list(dict.fromkeys(watchlist_tickers + UNIVERSE[:20]))
    to_analyze = [t for t in candidates if t not in already_predicted][:15]

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

    completed = [p for p in predictions if p.get("actual_pct") is not None]
    accuracy_summary = ""
    if completed:
        correct_dir = sum(1 for p in completed if (p["predicted_pct"] > 0) == (p["actual_pct"] > 0))
        pct_acc = correct_dir / len(completed) * 100
        accuracy_summary = (
            f"\nYour historical directional accuracy: {pct_acc:.0f}% over {len(completed)} predictions.\n"
            "Most recent prediction outcomes (learn from these):\n"
        )
        for p in completed[-10:]:
            direction = "CORRECT" if (p["predicted_pct"] > 0) == (p["actual_pct"] > 0) else "WRONG"
            accuracy_summary += (
                f"  {p['ticker']} {p['date']}: predicted {p['predicted_pct']:+.2f}%, "
                f"actual {p['actual_pct']:+.2f}% [{direction}]\n"
            )

    prompt = f"""You are a quantitative stock analyst with one goal: identify stocks with potential for >10% monthly returns (~0.5%+ per day).

Today: {today}

=== MACROECONOMIC CONDITIONS ===
{json.dumps(macro, indent=2)}

=== MARKET & FINANCIAL NEWS ===
{chr(10).join(headlines[:20]) if headlines else "No headlines fetched."}

=== STOCKS TO ANALYZE ===
{json.dumps(stocks_data, indent=2)}
{accuracy_summary}
=== VALUATION FRAMEWORK TO APPLY ===
Use ALL of the following criteria to assess each stock:

VALUATION (is it cheap?):
- P/E < 15 = cheap, 15-25 = fair, >25 = expensive
- PEG < 1 = undervalued relative to growth, >2 = expensive
- P/B < 1 = trading below asset value, >3 = expensive
- EV/EBITDA < 8 = cheap, 8-15 = fair, >15 = expensive
- FCF Yield > 8% = excellent, 4-8% = good, <4% = poor

QUALITY (is it a good business?):
- High profit margins and growing revenue = quality
- Low debt-to-equity = financial safety
- High EPS and revenue growth = momentum

CATALYSTS (why would it move NOW?):
- Macro tailwinds/headwinds from news
- Sector rotation signals
- Short squeeze potential (high short float)
- Beta (high beta = amplified moves)
- Recent 5-day momentum

Flag the 2-3 stocks with the strongest combination of cheap valuation + quality + near-term catalyst.

Return ONLY a valid JSON array, no explanation, no markdown:
[
  {{
    "ticker": "AAPL",
    "predicted_pct": 1.2,
    "confidence": "high",
    "reasoning": "PEG 0.8 signals undervaluation vs growth. Strong FCF yield 6.2%. Positive macro tailwind from Fed pause. Recent momentum +2.1% over 5 days."
  }}
]
confidence: "low" | "medium" | "high"."""

    client = anthropic.Anthropic(api_key=api_key)
    msg = client.messages.create(
        model="claude-sonnet-4-6", max_tokens=2048,
        messages=[{"role": "user", "content": prompt}],
    )
    raw = msg.content[0].text.strip()
    if "```" in raw:
        raw = raw.split("```")[1]
        if raw.startswith("json"):
            raw = raw[4:]
        raw = raw.strip().rstrip("```")

    claude_preds = json.loads(raw)
    price_map = {s["ticker"]: s["price"] for s in stocks_data}
    new_preds = []
    for cp in claude_preds:
        entry = {
            "date": today,
            "ticker": cp["ticker"],
            "predicted_pct": cp["predicted_pct"],
            "confidence": cp.get("confidence", "medium"),
            "reasoning": cp.get("reasoning", ""),
            "actual_pct": None,
            "price_at_prediction": price_map.get(cp["ticker"]),
            "generated_at": datetime.utcnow().isoformat(),
        }
        predictions.append(entry)
        new_preds.append(entry)

    save_predictions(predictions)
    return {"predictions": new_preds}


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


@app.delete("/api/alerts")
def clear_alerts():
    save_alerts([])
    return {"message": "Alert history cleared."}
