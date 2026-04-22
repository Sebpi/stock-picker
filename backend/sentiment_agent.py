"""
Sentiment Analysis Agent — AI & Technology Sector
==================================================
Scans news every hour for the Magnificent 7 + broader AI sector.
Covers macro disruptions: geopolitics, Trump statements, AI regulation/breakthroughs.
Sends a WhatsApp alert (via Twilio WhatsApp Sandbox) only when genuinely disruptive events are detected.
Uses Claude to judge materiality — suppresses noise and repeated alerts.

Usage:
    python sentiment_agent.py              # run once
    python sentiment_agent.py --loop       # run every hour continuously
    python sentiment_agent.py --test-sms   # send a test WhatsApp message

WhatsApp Sandbox setup (one-time, free):
    1. Open WhatsApp and message +14155238886
    2. Send: join <your-sandbox-keyword>  (shown at console.twilio.com/console/sms/whatsapp/sandbox)
    3. Set TWILIO_FROM_NUMBER=whatsapp:+14155238886 in .env
    4. Set TWILIO_TO_NUMBER=whatsapp:+44XXXXXXXXX  in .env
"""

import argparse
import json
import logging
import os
import re
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import anthropic
import yfinance as yf
from dotenv import load_dotenv

# ── Config ─────────────────────────────────────────────────────────────────────

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(BASE_DIR / "sentiment_agent.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
# Fix Windows console encoding for Unicode characters
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
log = logging.getLogger(__name__)

# State file — tracks seen headlines and last alert timestamps to avoid spam
STATE_FILE = BASE_DIR / "sentiment_agent_state.json"
PORTFOLIO_FILE = BASE_DIR / "portfolio.json"
WATCHLIST_FILE = BASE_DIR / "watchlist.json"

# Magnificent 7
MAG7 = ["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "TSLA"]

# Broader AI/tech universe to watch for sector signals
AI_TECH_UNIVERSE = [
    "NVDA", "AMD", "INTC", "AVGO", "QCOM",          # Chips
    "MSFT", "GOOGL", "AMZN", "META", "AAPL", "TSLA", # Mag7
    "ORCL", "CRM", "SNOW", "PLTR", "AI", "PATH",     # Enterprise AI
    "SMCI", "DELL", "HPE",                            # AI infrastructure
    "ARM", "MRVL", "ANET",                            # Networking / silicon
]

# Alert cooldown: don't re-alert on the same theme within this many hours
ALERT_COOLDOWN_HOURS = 6

# How many news items to fetch per ticker
MAX_NEWS_PER_TICKER = 15

# Claude model — haiku for fast/cheap scanning; upgrade to sonnet for depth
CLAUDE_MODEL = os.getenv("ANTHROPIC_MODEL", "claude-haiku-4-5-20251001")

# Macro topics — agent always searches for these regardless of ticker
MACRO_TOPICS = [
    "Iran war Middle East oil supply disruption",
    "Trump tariff technology semiconductor China",
    "Trump executive order AI regulation",
    "Federal Reserve interest rate tech valuations",
    "AI regulation EU US China policy",
    "OpenAI Anthropic Google DeepMind breakthrough",
    "chip export controls NVIDIA AMD China",
    "recession inflation consumer spending tech",
]

# ── State management ───────────────────────────────────────────────────────────

def load_state() -> dict:
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {"seen_headlines": {}, "last_alerts": {}, "last_run": None}


def save_state(state: dict):
    STATE_FILE.write_text(json.dumps(state, indent=2, default=str), encoding="utf-8")


def load_portfolio_tickers() -> list[str]:
    """Return tickers currently held in the portfolio (non-zero positions)."""
    if not PORTFOLIO_FILE.exists():
        return []
    try:
        transactions = json.loads(PORTFOLIO_FILE.read_text(encoding="utf-8"))
        positions: dict[str, float] = {}
        for tx in sorted(transactions, key=lambda x: x.get("timestamp", "")):
            t = tx.get("ticker", "").upper()
            if not t:
                continue
            qty = float(tx.get("qty", 0))
            if tx.get("type") == "buy":
                positions[t] = positions.get(t, 0) + qty
            elif tx.get("type") == "sell":
                positions[t] = max(0, positions.get(t, 0) - qty)
        return [t for t, qty in positions.items() if qty > 0]
    except Exception as e:
        log.warning("Could not load portfolio: %s", e)
        return []


def load_watchlist_tickers() -> list[str]:
    if not WATCHLIST_FILE.exists():
        return []
    try:
        data = json.loads(WATCHLIST_FILE.read_text(encoding="utf-8"))
        return [str(t).upper() for t in data if str(t).strip()]
    except Exception:
        return []

# ── News fetching ──────────────────────────────────────────────────────────────

def fetch_ticker_news(ticker: str, max_items: int = MAX_NEWS_PER_TICKER) -> list[dict]:
    """Fetch recent news headlines + URLs for a ticker via yfinance."""
    try:
        stock = yf.Ticker(ticker)
        raw = stock.news or []
        items = []
        for item in raw[:max_items]:
            content = item.get("content") or {}
            if content:
                title = content.get("title") or ""
                provider = (content.get("provider") or {})
                publisher = provider.get("displayName") or ""
                pub_str = content.get("pubDate") or content.get("displayTime") or ""
                # Prefer clickThroughUrl, fallback to canonicalUrl
                url = (
                    (content.get("clickThroughUrl") or {}).get("url") or
                    (content.get("canonicalUrl") or {}).get("url") or ""
                )
            else:
                # Legacy flat structure
                title = item.get("title") or ""
                publisher = item.get("publisher") or ""
                pub_time = item.get("providerPublishTime") or 0
                pub_str = datetime.fromtimestamp(pub_time, tz=timezone.utc).isoformat() if pub_time else ""
                url = item.get("link") or ""

            if title:
                items.append({
                    "ticker": ticker,
                    "title": title,
                    "publisher": publisher,
                    "published_at": pub_str,
                    "url": url,
                })
        return items
    except Exception as e:
        log.warning("News fetch failed for %s: %s", ticker, e)
        return []


def fetch_macro_news() -> list[dict]:
    """Fetch news for macro proxy tickers that reflect geopolitical/economic events."""
    macro_proxies = [
        "SPY",   # S&P 500 — general market
        "QQQ",   # Nasdaq — tech sentiment
        "USO",   # Oil — Iran/Middle East proxy
        "TLT",   # Long bonds — rate/recession proxy
        "VIX",   # Volatility (use ^VIX)
        "GLD",   # Gold — flight-to-safety
        "DXY",   # Dollar — no yf ticker, skip
    ]
    all_news = []
    for proxy in macro_proxies:
        try:
            items = fetch_ticker_news(proxy, max_items=8)
            for item in items:
                item["ticker"] = f"MACRO:{proxy}"
            all_news.extend(items)
        except Exception:
            pass
    return all_news


def get_price_data(tickers: list[str]) -> dict[str, dict]:
    """Fetch current price + change for a list of tickers."""
    prices = {}
    for ticker in tickers:
        try:
            stock = yf.Ticker(ticker)
            fi = stock.fast_info
            price = getattr(fi, "last_price", None)
            prev_close = getattr(fi, "previous_close", None)
            change_pct = None
            if price and prev_close and prev_close > 0:
                change_pct = round((price - prev_close) / prev_close * 100, 2)
            prices[ticker] = {
                "price": round(float(price), 2) if price else None,
                "change_pct": change_pct,
            }
        except Exception:
            prices[ticker] = {"price": None, "change_pct": None}
    return prices

# ── Headline deduplication ─────────────────────────────────────────────────────

def filter_new_headlines(all_items: list[dict], state: dict) -> list[dict]:
    """Return only headlines not seen in a previous run (within 48 hours)."""
    seen = state.get("seen_headlines", {})
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=48)).isoformat()
    # Purge old seen headlines
    seen = {k: v for k, v in seen.items() if v >= cutoff}
    state["seen_headlines"] = seen

    new_items = []
    for item in all_items:
        key = re.sub(r"\s+", " ", item["title"].lower().strip())[:200]
        if key not in seen:
            seen[key] = datetime.now(timezone.utc).isoformat()
            new_items.append(item)
    return new_items

# ── Claude analysis ────────────────────────────────────────────────────────────

def build_analysis_prompt(
    new_headlines: list[dict],
    prices: dict[str, dict],
    portfolio_tickers: list[str],
    watchlist_tickers: list[str],
) -> str:
    # Format price snapshot
    price_lines = []
    for ticker in MAG7 + [t for t in portfolio_tickers if t not in MAG7]:
        pd = prices.get(ticker, {})
        chg = pd.get("change_pct")
        chg_str = f"{chg:+.2f}%" if chg is not None else "—"
        price_lines.append(f"  {ticker}: ${pd.get('price', '—')} ({chg_str})")
    price_block = "\n".join(price_lines) or "  (no data)"

    # Format headlines with URLs so Claude can include links in top_headlines
    headline_lines = []
    for item in new_headlines[:80]:  # cap to keep prompt size manageable
        pub = f" [{item['publisher']}]" if item.get("publisher") else ""
        url = f" | {item['url']}" if item.get("url") else ""
        headline_lines.append(f"  [{item['ticker']}]{pub} {item['title']}{url}")
    headline_block = "\n".join(headline_lines) or "  (no new headlines)"

    portfolio_str = ", ".join(portfolio_tickers) if portfolio_tickers else "none logged"
    watchlist_str = ", ".join(watchlist_tickers) if watchlist_tickers else "none"

    return f"""You are a professional equity analyst and risk manager specialising in AI and technology stocks.

## Current Market Snapshot (today)
{price_block}

## User's Portfolio Holdings
{portfolio_str}

## User's Watchlist
{watchlist_str}

## New Headlines Since Last Check
{headline_block}

## Your Task

Analyse the headlines above in the context of:
1. The Magnificent 7 (AAPL, MSFT, GOOGL, AMZN, NVDA, META, TSLA)
2. The broader AI/semiconductor sector
3. Macro disruptions: geopolitics (Iran/Middle East war, Ukraine), Trump executive actions or tariffs, AI regulation, Fed policy
4. Any breaking developments in AI models or infrastructure that shift competitive dynamics

You are a HIGH-BAR alert system. The user only wants to be contacted for genuinely urgent, market-moving events. They do NOT want noise.

## Severity definitions — be strict:

*CRITICAL* — Send immediately. Rare. Examples:
- Emergency government action: surprise chip export ban, executive order freezing AI development, sanctions
- Geopolitical shock with direct supply chain impact: war escalation blocking Taiwan Strait or Strait of Hormuz
- A Mag7 stock drops or gaps >8% pre/post market on breaking news
- Systemic financial event: major bank failure, circuit breakers triggered, flash crash
- Unexpected Fed emergency rate move outside scheduled meetings

*HIGH* — Send only if the expected price move is >5% within 48 hours AND is not yet priced in. Examples:
- Confirmed earnings catastrophe (not a miss — a guidance collapse or profit warning)
- Major regulatory action (antitrust breakup order, product ban) confirmed, not rumoured
- A competitor breakthrough that materially threatens a Mag7 revenue stream (e.g. China releases GPT-5 equivalent at 1/10th cost)
- Sudden CEO departure at a Mag7 company

*Do NOT alert for:*
- Earnings beats or misses within normal range (±10% of estimates)
- Analyst upgrades or downgrades
- Routine product launches or incremental AI model updates
- Geopolitical tension that has been ongoing for weeks without escalation
- Macro data releases (CPI, jobs) that came in close to expectations
- Any headline that uses words like "could", "may", "analysts say", "report suggests" without hard confirmation
- Stock moves already happening (already priced in)
- Anything medium severity or below — these are NOT worth interrupting the user

The default answer should be: no alert. Only override this if the event is genuinely exceptional.

## Response Format

Respond with valid JSON only — no markdown fences, no commentary outside the JSON:

{{
  "disruption_detected": true | false,
  "severity": "critical" | "high" | "medium" | "none",
  "alert_worthy": true | false,
  "expected_move_pct": <estimated % move for most affected ticker, or null>,
  "direction": "down" | "up" | null,
  "summary": "<2-3 sentence plain-English summary of what is happening and why it matters>",
  "affected_tickers": ["TICKER", ...],
  "portfolio_impact": "<1 sentence on how this affects the user's specific holdings, or 'No direct impact'>",
  "macro_factor": "<the macro theme driving this, or null>",
  "recommended_action": "<brief suggested action or 'Monitor only'>",
  "top_headlines": [
    {{"title": "<headline>", "url": "<direct article URL or empty string if unavailable>"}},
    {{"title": "<headline>", "url": "<direct article URL or empty string if unavailable>"}},
    {{"title": "<headline>", "url": "<direct article URL or empty string if unavailable>"}}
  ]
}}

Only set alert_worthy to true if severity is 'critical' or 'high' AND expected_move_pct >= 5.
For top_headlines, only include the URLs that were provided in the headlines list above — do not invent URLs.
If in doubt, do not alert. Return disruption_detected: false and alert_worthy: false."""


def analyse_with_claude(prompt: str) -> Optional[dict]:
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        log.error("ANTHROPIC_API_KEY not set — cannot analyse.")
        return None

    client = anthropic.Anthropic(api_key=api_key)
    try:
        response = client.messages.create(
            model=CLAUDE_MODEL,
            max_tokens=1024,
            system="You are a professional equity analyst. Respond with valid JSON only.",
            messages=[{"role": "user", "content": prompt}],
        )
        raw = response.content[0].text.strip()
        # Strip markdown fences if Claude wraps the JSON anyway
        raw = re.sub(r"^```json\s*", "", raw)
        raw = re.sub(r"\s*```$", "", raw)
        return json.loads(raw)
    except json.JSONDecodeError as e:
        log.error("Claude returned invalid JSON: %s", e)
        log.debug("Raw response: %s", raw if "raw" in dir() else "N/A")
        return None
    except anthropic.RateLimitError:
        log.warning("Claude rate limit hit — skipping this cycle.")
        return None
    except Exception as e:
        log.error("Claude API error: %s", e)
        return None

# ── SMS ────────────────────────────────────────────────────────────────────────

def send_whatsapp(message: str) -> bool:
    """Send a WhatsApp message via Twilio.

    Requires the Twilio WhatsApp Sandbox (free) or a production WhatsApp sender.
    FROM/TO numbers must be prefixed with 'whatsapp:' in .env, e.g.:
        TWILIO_FROM_NUMBER=whatsapp:+14155238886
        TWILIO_TO_NUMBER=whatsapp:+44XXXXXXXXX
    """
    try:
        from twilio.rest import Client
        account_sid = os.getenv("TWILIO_ACCOUNT_SID", "")
        auth_token  = os.getenv("TWILIO_AUTH_TOKEN", "")
        from_number = os.getenv("TWILIO_FROM_NUMBER", "")
        to_number   = os.getenv("TWILIO_TO_NUMBER", "")

        if not all([account_sid, auth_token, from_number, to_number]):
            log.warning("Twilio env vars not fully set — WhatsApp message not sent.")
            log.warning("Set TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_FROM_NUMBER, TWILIO_TO_NUMBER in .env")
            return False

        # Auto-prefix with whatsapp: if not already present
        if not from_number.startswith("whatsapp:"):
            from_number = f"whatsapp:{from_number}"
        if not to_number.startswith("whatsapp:"):
            to_number = f"whatsapp:{to_number}"

        client = Client(account_sid, auth_token)
        msg = client.messages.create(body=message[:4096], from_=from_number, to=to_number)
        log.info("WhatsApp message sent — SID: %s", msg.sid)
        return True
    except Exception as e:
        log.error("WhatsApp send failed: %s", e)
        return False


# Keep send_sms as an alias so existing code and tests that reference it still work
send_sms = send_whatsapp


def build_sms_message(analysis: dict, prices: dict[str, dict]) -> str:
    severity = analysis.get("severity", "high").upper()
    summary = analysis.get("summary", "")
    affected = ", ".join(analysis.get("affected_tickers", []))
    portfolio_impact = analysis.get("portfolio_impact", "")
    macro = analysis.get("macro_factor") or ""
    action = analysis.get("recommended_action", "Monitor")
    headlines = analysis.get("top_headlines", [])
    expected_move = analysis.get("expected_move_pct")
    direction = analysis.get("direction") or ""

    # Price changes for affected tickers
    price_lines = []
    for ticker in analysis.get("affected_tickers", [])[:5]:
        pd = prices.get(ticker, {})
        chg = pd.get("change_pct")
        if chg is not None:
            price_lines.append(f"{ticker} {chg:+.2f}%")

    price_str = " | ".join(price_lines)
    now_str = datetime.now().strftime("%d %b %Y %H:%M")

    # Expected move line
    move_str = ""
    if expected_move is not None:
        arrow = "DOWN" if direction == "down" else "UP" if direction == "up" else ""
        move_str = f"Expected move: {arrow} {abs(expected_move):.0f}%+ within 48h"

    parts = [
        f"*STOCKPICKER URGENT ALERT [{severity}]*",
        f"_{now_str}_",
        "",
        summary,
        "",
    ]
    if move_str:
        parts.append(move_str)
    if affected:
        parts.append(f"Affected: {affected}")
    if price_str:
        parts.append(f"Live prices: {price_str}")
    if macro:
        parts.append(f"Macro driver: {macro}")
    if portfolio_impact and portfolio_impact.lower() != "no direct impact":
        parts.append(f"Your portfolio: {portfolio_impact}")
    parts.append(f"Action: {action}")
    if headlines:
        parts.append("")
        parts.append("*Key headlines:*")
        for h in headlines[:3]:
            # Support both old string format and new {title, url} format
            if isinstance(h, dict):
                title = h.get("title", "")[:120]
                url = h.get("url", "")
                parts.append(f"- {title}\n  {url}" if url else f"- {title}")
            else:
                parts.append(f"- {str(h)[:120]}")

    return "\n".join(parts)

# ── Alert cooldown ─────────────────────────────────────────────────────────────

def is_on_cooldown(analysis: dict, state: dict) -> bool:
    """Suppress alert if we recently sent one for the same set of affected tickers."""
    last_alerts = state.get("last_alerts", {})
    cutoff = datetime.now(timezone.utc) - timedelta(hours=ALERT_COOLDOWN_HOURS)
    affected = frozenset(analysis.get("affected_tickers", []))
    macro = analysis.get("macro_factor") or ""

    # Check ticker-based cooldown
    for ticker in affected:
        key = f"ticker:{ticker}"
        last = last_alerts.get(key)
        if last:
            last_dt = datetime.fromisoformat(last)
            if last_dt > cutoff:
                log.info("Cooldown active for %s (last alert %s)", ticker, last)
                return True

    # Check macro-based cooldown (same macro theme within cooldown window)
    if macro:
        macro_key = re.sub(r"\s+", "_", macro.lower().strip())[:50]
        key = f"macro:{macro_key}"
        last = last_alerts.get(key)
        if last:
            last_dt = datetime.fromisoformat(last)
            if last_dt > cutoff:
                log.info("Cooldown active for macro theme '%s'", macro)
                return True

    return False


def record_alert_sent(analysis: dict, state: dict):
    last_alerts = state.setdefault("last_alerts", {})
    now_str = datetime.now(timezone.utc).isoformat()
    for ticker in analysis.get("affected_tickers", []):
        last_alerts[f"ticker:{ticker}"] = now_str
    macro = analysis.get("macro_factor") or ""
    if macro:
        macro_key = re.sub(r"\s+", "_", macro.lower().strip())[:50]
        last_alerts[f"macro:{macro_key}"] = now_str

# ── Main scan loop ─────────────────────────────────────────────────────────────

def run_scan(dry_run: bool = False) -> dict:
    log.info("=== Sentiment scan starting ===")
    state = load_state()

    # Determine the full universe to watch
    portfolio_tickers = load_portfolio_tickers()
    watchlist_tickers = load_watchlist_tickers()
    scan_tickers = list(dict.fromkeys(
        AI_TECH_UNIVERSE + portfolio_tickers + watchlist_tickers
    ))

    log.info("Scanning %d tickers | Portfolio: %s | Watchlist: %s",
             len(scan_tickers), portfolio_tickers or "none", watchlist_tickers or "none")

    # Fetch all news
    all_headlines = []
    for ticker in scan_tickers:
        items = fetch_ticker_news(ticker)
        all_headlines.extend(items)
        time.sleep(0.3)  # gentle rate limiting

    macro_headlines = fetch_macro_news()
    all_headlines.extend(macro_headlines)

    log.info("Fetched %d total headlines", len(all_headlines))

    # Filter to only new headlines since last run
    new_headlines = filter_new_headlines(all_headlines, state)
    log.info("%d new headlines to analyse", len(new_headlines))

    if not new_headlines:
        log.info("No new headlines — nothing to do.")
        state["last_run"] = datetime.now(timezone.utc).isoformat()
        save_state(state)
        return {"status": "no_new_headlines", "alert_sent": False}

    # Fetch current prices for Mag7 + portfolio
    price_tickers = list(dict.fromkeys(MAG7 + portfolio_tickers))
    log.info("Fetching prices for %d tickers…", len(price_tickers))
    prices = get_price_data(price_tickers)

    # Ask Claude to evaluate
    prompt = build_analysis_prompt(new_headlines, prices, portfolio_tickers, watchlist_tickers)
    log.info("Sending %d headlines to Claude (%s)…", len(new_headlines), CLAUDE_MODEL)
    analysis = analyse_with_claude(prompt)

    if not analysis:
        log.error("Analysis failed — aborting this cycle.")
        state["last_run"] = datetime.now(timezone.utc).isoformat()
        save_state(state)
        return {"status": "analysis_failed", "alert_sent": False}

    log.info(
        "Analysis: disruption=%s severity=%s alert_worthy=%s",
        analysis.get("disruption_detected"),
        analysis.get("severity"),
        analysis.get("alert_worthy"),
    )
    log.info("Summary: %s", analysis.get("summary", ""))

    # Decide whether to send alert — strict gate
    alert_sent = False
    severity = analysis.get("severity", "none")
    alert_worthy = analysis.get("alert_worthy", False)
    expected_move = analysis.get("expected_move_pct") or 0
    disruption = analysis.get("disruption_detected", False)

    # Hard rules: critical always qualifies; high only if expected move >= 5%
    qualifies = (
        disruption and alert_worthy and (
            severity == "critical" or
            (severity == "high" and abs(expected_move) >= 5)
        )
    )

    if qualifies:
        if is_on_cooldown(analysis, state):
            log.info("Alert suppressed — cooldown active.")
        else:
            message = build_sms_message(analysis, prices)
            log.info("WhatsApp message:\n%s", message)
            if dry_run:
                log.info("[DRY RUN] Would have sent WhatsApp alert.")
                alert_sent = False
            else:
                alert_sent = send_whatsapp(message)
                if alert_sent:
                    record_alert_sent(analysis, state)
    else:
        log.info(
            "No alert — severity=%s expected_move=%.1f%% alert_worthy=%s (threshold: critical always, high needs >=5%% move)",
            severity, abs(expected_move), alert_worthy
        )

    # Persist state
    state["last_run"] = datetime.now(timezone.utc).isoformat()
    save_state(state)

    return {
        "status": "ok",
        "new_headlines": len(new_headlines),
        "disruption_detected": analysis.get("disruption_detected"),
        "severity": analysis.get("severity"),
        "alert_worthy": analysis.get("alert_worthy"),
        "alert_sent": alert_sent,
        "summary": analysis.get("summary"),
        "affected_tickers": analysis.get("affected_tickers"),
        "portfolio_impact": analysis.get("portfolio_impact"),
    }


def run_loop(interval_minutes: int = 60, dry_run: bool = False):
    log.info("Starting hourly sentiment loop (interval=%d min, dry_run=%s)", interval_minutes, dry_run)
    while True:
        try:
            result = run_scan(dry_run=dry_run)
            log.info("Scan result: %s", json.dumps(result, default=str))
        except Exception as e:
            log.error("Unhandled error in scan loop: %s", e, exc_info=True)

        next_run = datetime.now() + timedelta(minutes=interval_minutes)
        log.info("Next scan at %s", next_run.strftime("%H:%M:%S"))
        time.sleep(interval_minutes * 60)


# ── CLI ────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="StockPicker Sentiment Agent")
    parser.add_argument("--loop", action="store_true", help="Run every hour continuously")
    parser.add_argument("--interval", type=int, default=60, help="Loop interval in minutes (default 60)")
    parser.add_argument("--dry-run", action="store_true", help="Analyse but don't send SMS")
    parser.add_argument("--test-sms", action="store_true", help="Send a test SMS and exit")
    parser.add_argument("--reset-state", action="store_true", help="Clear seen-headline cache and alert cooldowns")
    args = parser.parse_args()

    if args.reset_state:
        STATE_FILE.unlink(missing_ok=True)
        log.info("State reset — all seen headlines and cooldowns cleared.")
        sys.exit(0)

    if args.test_sms:
        ok = send_whatsapp("StockPicker Sentiment Agent - WhatsApp is configured correctly. You will receive alerts here when material market disruptions are detected.")
        sys.exit(0 if ok else 1)

    if args.loop:
        run_loop(interval_minutes=args.interval, dry_run=args.dry_run)
    else:
        result = run_scan(dry_run=args.dry_run)
        print(json.dumps(result, indent=2, default=str))
