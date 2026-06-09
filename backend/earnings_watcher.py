"""
Earnings intelligence — monitors SEC EDGAR 8-K Item 2.02 filings for watchlist tickers
and generates structured LLM analysis of each earnings release.

Flow per ticker:
  1. Look up CIK from EDGAR company_tickers.json (in-memory cache)
  2. Scan submissions.json for 8-K filings with Item 2.02 in the last N days
  3. Fetch Exhibit 99.1 (earnings press release HTML) from the filing index
  4. Strip HTML, get plain text
  5. Get EPS/revenue estimates from yfinance
  6. Call Opus 4.8 for structured JSON analysis
  7. Persist to earnings_events SQLite table
  8. Send immediate email notification
"""
from __future__ import annotations

import json
import logging
import os
import re
import smtplib
import time
import uuid
from datetime import datetime, timedelta, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Any

import requests

logger = logging.getLogger(__name__)

EDGAR_UA = os.getenv("EDGAR_USER_AGENT", "StockPicker research@example.com")
_EDGAR_RATE = 0.12  # seconds between requests — under SEC's 10 req/s cap
_last_req_time: float = 0.0
_cik_map: dict[str, str] = {}  # ticker -> zero-padded 10-digit CIK
_cik_map_loaded: bool = False


# ---------------------------------------------------------------------------
# EDGAR helpers
# ---------------------------------------------------------------------------

def _sec_get(url: str, timeout: int = 15) -> requests.Response | None:
    """Rate-limited GET for SEC EDGAR; returns None on any failure."""
    global _last_req_time
    elapsed = time.monotonic() - _last_req_time
    if elapsed < _EDGAR_RATE:
        time.sleep(_EDGAR_RATE - elapsed)
    _last_req_time = time.monotonic()
    headers = {
        "User-Agent": EDGAR_UA,
        "Accept": "application/json, text/html, */*",
    }
    try:
        resp = requests.get(url, headers=headers, timeout=timeout)
        resp.raise_for_status()
        return resp
    except Exception as exc:
        logger.warning("[earnings_watcher] GET %s: %s", url, exc)
        return None


def _load_cik_map() -> None:
    global _cik_map, _cik_map_loaded
    resp = _sec_get("https://www.sec.gov/files/company_tickers.json")
    if not resp:
        return
    try:
        data = resp.json()
        new_map: dict[str, str] = {}
        for entry in data.values():
            t = str(entry.get("ticker", "")).upper().strip()
            cik = str(entry.get("cik_str", "")).strip()
            if t and cik:
                new_map[t] = cik.zfill(10)
        _cik_map = new_map
        _cik_map_loaded = True
        logger.info("[earnings_watcher] CIK map loaded: %d tickers", len(_cik_map))
    except Exception as exc:
        logger.warning("[earnings_watcher] CIK map parse failed: %s", exc)


def get_cik(ticker: str) -> str | None:
    if not _cik_map_loaded:
        _load_cik_map()
    return _cik_map.get(ticker.upper())


def get_recent_earnings_filings(ticker: str, since_days: int = 3) -> list[dict]:
    """
    Return 8-K filings with Item 2.02 filed in the last since_days days.
    Each dict: {accession, accession_dashed, filing_date, cik, ticker}
    """
    cik = get_cik(ticker)
    if not cik:
        return []

    resp = _sec_get(f"https://data.sec.gov/submissions/CIK{cik}.json")
    if not resp:
        return []
    try:
        data = resp.json()
    except Exception:
        return []

    recent = data.get("filings", {}).get("recent", {})
    forms = recent.get("form", [])
    items_list = recent.get("items", [])
    accessions = recent.get("accessionNumber", [])
    dates = recent.get("filingDate", [])

    cutoff = (datetime.now(timezone.utc) - timedelta(days=since_days)).date().isoformat()
    results = []
    for form, items, acc, date in zip(forms, items_list, accessions, dates):
        if date < cutoff:
            # submissions are newest-first; once too old, everything after is older
            break
        if form != "8-K":
            continue
        if "2.02" not in str(items or ""):
            continue
        results.append({
            "accession": acc.replace("-", ""),
            "accession_dashed": acc,
            "filing_date": date,
            "cik": cik,
            "ticker": ticker.upper(),
        })
    return results


def _get_exhibit_99_1_url(cik: str, accession_nodash: str) -> str | None:
    """Return the direct URL to Exhibit 99.1 from the 8-K filing index JSON."""
    cik_int = int(cik)
    index_url = (
        f"https://data.sec.gov/Archives/edgar/data/{cik_int}"
        f"/{accession_nodash}/{accession_nodash}-index.json"
    )
    resp = _sec_get(index_url)
    if not resp:
        return None
    try:
        data = resp.json()
    except Exception:
        return None

    base = f"https://www.sec.gov/Archives/edgar/data/{cik_int}/{accession_nodash}/"
    docs = data.get("documents", [])
    # Prefer Exhibit 99.1
    for doc in docs:
        doc_type = str(doc.get("type", "")).strip()
        desc = str(doc.get("description", "")).lower()
        if doc_type in ("EX-99.1", "EX-99.01") or "press release" in desc:
            return base + doc["filename"]
    # Fallback: first .htm document that isn't the index
    for doc in docs:
        fn = str(doc.get("filename", ""))
        if (fn.endswith(".htm") or fn.endswith(".html")) and "index" not in fn.lower():
            return base + fn
    return None


def fetch_press_release(cik: str, accession_nodash: str, max_chars: int = 14000) -> str | None:
    """Fetch and return plain text of the earnings press release, trimmed to max_chars."""
    url = _get_exhibit_99_1_url(cik, accession_nodash)
    if not url:
        logger.warning("[earnings_watcher] No exhibit URL for %s/%s", cik, accession_nodash)
        return None

    resp = _sec_get(url)
    if not resp:
        return None

    try:
        from bs4 import BeautifulSoup  # bundled with yfinance dependency tree
        soup = BeautifulSoup(resp.text, "html.parser")
        for tag in soup(["script", "style", "head"]):
            tag.decompose()
        text = soup.get_text(separator="\n", strip=True)
        text = re.sub(r"\n{3,}", "\n\n", text)
        text = re.sub(r" {2,}", " ", text)
        return text[:max_chars]
    except Exception as exc:
        logger.warning("[earnings_watcher] Press release parse failed for %s: %s", url, exc)
        return None


# ---------------------------------------------------------------------------
# Market data helpers
# ---------------------------------------------------------------------------

def get_estimates(ticker: str) -> dict[str, Any]:
    """Return EPS estimate, revenue estimate, company name, sector from yfinance."""
    try:
        import yfinance as yf
        info = yf.Ticker(ticker).info
        return {
            "eps_estimate": info.get("epsCurrentYear") or info.get("forwardEps"),
            "revenue_estimate": info.get("revenueEstimate"),
            "company_name": info.get("shortName") or info.get("longName") or ticker,
            "sector": info.get("sector") or "",
        }
    except Exception as exc:
        logger.warning("[earnings_watcher] Estimates fetch failed for %s: %s", ticker, exc)
        return {
            "eps_estimate": None, "revenue_estimate": None,
            "company_name": ticker, "sector": "",
        }


def get_upcoming_earnings(watchlist: list[str], days_ahead: int = 7) -> list[dict]:
    """Return upcoming earnings dates for watchlist tickers within days_ahead days."""
    today = datetime.now(timezone.utc).date()
    cutoff = today + timedelta(days=days_ahead)
    upcoming: list[dict] = []

    for ticker in watchlist:
        try:
            import yfinance as yf
            cal = yf.Ticker(ticker).calendar
            if cal is None:
                continue
            # yfinance returns a dict-of-lists style calendar
            if isinstance(cal, dict):
                dates_raw = cal.get("Earnings Date") or []
            elif hasattr(cal, "to_dict"):
                cal_dict = cal.to_dict()
                dates_raw = list((cal_dict.get("Earnings Date") or {}).values())
            else:
                continue

            for d in dates_raw:
                if hasattr(d, "date"):
                    d = d.date()
                elif isinstance(d, str):
                    d = datetime.fromisoformat(d[:10]).date()
                if today <= d <= cutoff:
                    info = yf.Ticker(ticker).info
                    upcoming.append({
                        "ticker": ticker,
                        "company_name": info.get("shortName") or info.get("longName") or ticker,
                        "earnings_date": d.isoformat(),
                        "eps_estimate": info.get("epsCurrentYear") or info.get("forwardEps"),
                        "revenue_estimate": info.get("revenueEstimate"),
                        "sector": info.get("sector") or "",
                    })
                    break
        except Exception as exc:
            logger.debug("[earnings_watcher] Calendar fetch failed for %s: %s", ticker, exc)

    upcoming.sort(key=lambda x: x["earnings_date"])
    return upcoming


# ---------------------------------------------------------------------------
# LLM analysis
# ---------------------------------------------------------------------------

def analyse_earnings(
    ticker: str,
    company_name: str,
    sector: str,
    press_release_text: str,
    estimates: dict[str, Any],
    watchlist: list[str],
) -> dict[str, Any] | None:
    """Call Opus 4.8 with the press release and return a structured earnings analysis dict."""
    try:
        import anthropic
        model = os.getenv("EARNINGS_MODEL", "claude-opus-4-8")
        client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY", ""))

        eps_est = estimates.get("eps_estimate")
        rev_est = estimates.get("revenue_estimate")
        peers = ", ".join(w for w in watchlist if w != ticker)[:400]

        prompt = (
            f"You are an equity research analyst. Analyse the earnings press release for "
            f"{ticker} ({company_name}, {sector or 'unknown sector'}) "
            f"and return ONLY a JSON object — no markdown, no explanation.\n\n"
            f"Analyst estimates (if available):\n"
            f"  EPS estimate: {f'${eps_est:.2f}' if eps_est else 'not available'}\n"
            f"  Revenue estimate: {f'${rev_est/1e9:.1f}B' if rev_est else 'not available'}\n\n"
            f"Watchlist peers for cross-sector impact: {peers or 'none'}\n\n"
            f"Press release (truncated):\n{press_release_text[:11000]}\n\n"
            f"Return ONLY this JSON:\n"
            f'{{\n'
            f'  "beat_miss": "BEAT|MISS|IN_LINE|UNKNOWN",\n'
            f'  "eps_actual": <number or null>,\n'
            f'  "eps_estimate": <number or null>,\n'
            f'  "eps_surprise_pct": <number or null>,\n'
            f'  "revenue_actual_billions": <number or null>,\n'
            f'  "revenue_estimate_billions": <number or null>,\n'
            f'  "rev_beat_miss": "BEAT|MISS|IN_LINE|UNKNOWN",\n'
            f'  "guidance": "RAISED|MAINTAINED|LOWERED|WITHDRAWN|UNKNOWN",\n'
            f'  "guidance_details": "<1-sentence summary>",\n'
            f'  "thesis_impact": "POSITIVE|NEGATIVE|NEUTRAL",\n'
            f'  "thesis_reasoning": "<2-3 sentence investment thesis impact>",\n'
            f'  "pre_market_headline": "<15-word headline>",\n'
            f'  "key_highlights": ["<bullet 1>", "<bullet 2>", "<bullet 3>"],\n'
            f'  "cross_sector_impacts": [{{"ticker": "TICKER", "impact": "POSITIVE|NEGATIVE|NEUTRAL", "reason": "<short>"}}],\n'
            f'}}'
        )

        resp = client.messages.create(
            model=model,
            max_tokens=1600,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = resp.content[0].text.strip()
        raw = re.sub(r"^```(?:json)?\s*", "", raw)
        raw = re.sub(r"\s*```$", "", raw.strip())
        return json.loads(raw)

    except Exception as exc:
        logger.error("[earnings_watcher] LLM analysis failed for %s: %s", ticker, exc)
        return None


# ---------------------------------------------------------------------------
# Post-earnings sentiment snapshot
# ---------------------------------------------------------------------------

def get_post_earnings_sentiment(ticker: str, company_name: str) -> dict[str, Any]:
    """
    Run a quick post-earnings sentiment snapshot using recent news + price action.
    Returns {score, direction, price_change_pct, summary} or empty dict on failure.
    """
    try:
        import anthropic
        import yfinance as yf

        # Recent price action (last 2 days)
        hist = yf.Ticker(ticker).history(period="5d")
        price_change_pct: float | None = None
        if len(hist) >= 2:
            prev_close = float(hist["Close"].iloc[-2])
            last_close = float(hist["Close"].iloc[-1])
            if prev_close > 0:
                price_change_pct = round((last_close - prev_close) / prev_close * 100, 2)

        # Recent news headlines
        try:
            news_items = yf.Ticker(ticker).news or []
            headlines = "; ".join(
                n.get("content", {}).get("title", "") or n.get("title", "")
                for n in news_items[:8]
                if (n.get("content", {}).get("title") or n.get("title", ""))
            )
        except Exception:
            headlines = ""

        price_line = (
            f"Price change since last close: {price_change_pct:+.1f}%"
            if price_change_pct is not None
            else "Price data unavailable"
        )

        model = os.getenv("THESIS_MODEL", "claude-sonnet-4-6")
        client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY", ""))

        prompt = (
            f"You are a quantitative equity analyst. Provide a brief post-earnings sentiment "
            f"snapshot for {ticker} ({company_name}). Return ONLY a JSON object.\n\n"
            f"{price_line}\n"
            f"Recent headlines: {headlines[:2000] or 'none available'}\n\n"
            f"Return ONLY:\n"
            f'{{\n'
            f'  "score": <0-100 where 50=neutral, >60=bullish, <40=bearish>,\n'
            f'  "direction": "Bullish|Bearish|Neutral",\n'
            f'  "price_change_pct": <number or null>,\n'
            f'  "summary": "<2-sentence market reaction summary>"\n'
            f'}}'
        )

        resp = client.messages.create(
            model=model,
            max_tokens=300,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = resp.content[0].text.strip()
        raw = re.sub(r"^```(?:json)?\s*", "", raw)
        raw = re.sub(r"\s*```$", "", raw.strip())
        result = json.loads(raw)
        if price_change_pct is not None and result.get("price_change_pct") is None:
            result["price_change_pct"] = price_change_pct
        return result

    except Exception as exc:
        logger.warning("[earnings_watcher] Post-earnings sentiment failed for %s: %s", ticker, exc)
        return {}


# ---------------------------------------------------------------------------
# Notification helpers (inline to avoid circular import from main.py)
# ---------------------------------------------------------------------------

def _send_email(subject: str, body: str) -> bool:
    smtp_host = os.getenv("SMTP_HOST", "smtp.gmail.com")
    smtp_port = int(os.getenv("SMTP_PORT", "587"))
    smtp_user = os.getenv("SMTP_USER", "")
    smtp_pass = os.getenv("SMTP_PASS", "")
    target = os.getenv("ALERT_EMAIL", "").strip()
    if not all([smtp_user, smtp_pass, target]):
        return False
    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = smtp_user
        msg["To"] = target
        msg.attach(MIMEText(body, "plain"))
        with smtplib.SMTP(smtp_host, smtp_port) as server:
            server.ehlo()
            server.starttls()
            server.login(smtp_user, smtp_pass)
            server.send_message(msg)
        return True
    except Exception as exc:
        logger.warning("[earnings_watcher] Email send failed: %s", exc)
        return False


def _notify_earnings_result(
    event: dict,
    analysis: dict | None,
    sentiment: dict | None = None,
) -> None:
    """Send email immediately after earnings analysis completes."""
    ticker = event["ticker"]
    company = event.get("company_name", ticker)
    beat_miss = event.get("beat_miss", "UNKNOWN")
    guidance = event.get("guidance", "UNKNOWN")
    thesis_impact = event.get("thesis_impact", "NEUTRAL")
    signal_emoji = {"POSITIVE": "✅", "NEGATIVE": "❌", "NEUTRAL": "➡️"}.get(thesis_impact, "")

    eps_line = ""
    if event.get("eps_actual") is not None and event.get("eps_estimate") is not None:
        eps_line = f"\nEPS: ${event['eps_actual']:.2f} vs ${event['eps_estimate']:.2f} est"
        if event.get("eps_surprise_pct") is not None:
            eps_line += f" ({event['eps_surprise_pct']:+.1f}%)"

    summary = (
        f"🔔 [{ticker}] Earnings: {beat_miss}{eps_line}\n"
        f"Guidance: {guidance} | Signal: {thesis_impact} {signal_emoji}"
    ).strip()

    reasoning = (analysis or {}).get("thesis_reasoning", "")
    highlights = (analysis or {}).get("key_highlights", [])
    cross = (analysis or {}).get("cross_sector_impacts", [])
    headline = (analysis or {}).get("pre_market_headline", f"{ticker} earnings")

    body = f"{headline}\n\n{company} ({ticker})\n{summary}\n"
    if reasoning:
        body += f"\nThesis impact:\n{reasoning}\n"
    if highlights:
        body += "\nKey points:\n" + "".join(f"  • {h}\n" for h in highlights)
    if cross:
        body += "\nCross-sector:\n"
        for ci in cross[:5]:
            body += f"  {ci.get('ticker','')} ({ci.get('impact','')}) — {ci.get('reason','')}\n"
    if event.get("press_release_url"):
        body += f"\nSEC filing: {event['press_release_url']}"

    _send_email(
        subject=f"[StockLens] {ticker} Earnings: {beat_miss} — {thesis_impact} {signal_emoji}",
        body=body.strip(),
    )


# ---------------------------------------------------------------------------
# Core pipeline
# ---------------------------------------------------------------------------

def check_and_analyse_ticker(
    ticker: str,
    watchlist: list[str],
    since_days: int = 3,
) -> list[dict]:
    """
    Full pipeline for one ticker. Detects new 8-K earnings filings, analyses them,
    stores in DB, and sends notification. Returns list of new events processed.
    """
    import db as _db

    filings = get_recent_earnings_filings(ticker, since_days=since_days)
    new_events: list[dict] = []

    for filing in filings:
        accession = filing["accession"]
        if _db.get_earnings_event_by_accession(accession):
            continue  # already processed

        logger.info("[earnings_watcher] New 8-K filing for %s: %s (%s)",
                    ticker, accession, filing["filing_date"])

        pr_text = fetch_press_release(filing["cik"], accession) or ""
        estimates = get_estimates(ticker)
        company_name = estimates.get("company_name", ticker)
        sector = estimates.get("sector", "")

        analysis: dict | None = None
        if pr_text:
            analysis = analyse_earnings(ticker, company_name, sector, pr_text, estimates, watchlist)

        press_release_url = _get_exhibit_99_1_url(filing["cik"], accession)

        event: dict[str, Any] = {
            "event_id": str(uuid.uuid4()),
            "ticker": ticker,
            "company_name": company_name,
            "report_date": filing["filing_date"],
            "accession": accession,
            "press_release_url": press_release_url,
            "beat_miss": (analysis or {}).get("beat_miss", "UNKNOWN"),
            "eps_actual": (analysis or {}).get("eps_actual"),
            "eps_estimate": (analysis or {}).get("eps_estimate") or estimates.get("eps_estimate"),
            "eps_surprise_pct": (analysis or {}).get("eps_surprise_pct"),
            "revenue_actual": (analysis or {}).get("revenue_actual_billions"),
            "revenue_estimate": (analysis or {}).get("revenue_estimate_billions"),
            "guidance": (analysis or {}).get("guidance", "UNKNOWN"),
            "thesis_impact": (analysis or {}).get("thesis_impact", "NEUTRAL"),
            "analysis_json": json.dumps(analysis) if analysis else None,
            "detected_at": datetime.now(timezone.utc).isoformat(),
            "analysed_at": datetime.now(timezone.utc).isoformat() if analysis else None,
        }

        _db.upsert_earnings_event(event)
        new_events.append(event)

        # Get post-earnings sentiment snapshot (best-effort; non-blocking)
        sentiment: dict | None = None
        try:
            sentiment = get_post_earnings_sentiment(ticker, company_name) or None
        except Exception as exc:
            logger.warning("[earnings_watcher] Sentiment skipped for %s: %s", ticker, exc)

        _notify_earnings_result(event, analysis, sentiment)

    return new_events


def check_all_watchlist(watchlist: list[str], since_days: int = 3) -> list[dict]:
    """Check all watchlist tickers for new earnings. Returns all new events."""
    all_events: list[dict] = []
    for ticker in watchlist:
        try:
            events = check_and_analyse_ticker(ticker, watchlist, since_days=since_days)
            all_events.extend(events)
        except Exception as exc:
            logger.error("[earnings_watcher] Error processing %s: %s", ticker, exc)
    return all_events


def send_morning_reminders(watchlist: list[str]) -> int:
    """
    Send email reminders for tickers reporting today or tomorrow.
    Returns count sent.
    """
    today = datetime.now(timezone.utc).date()
    upcoming = get_upcoming_earnings(watchlist, days_ahead=1)
    sent = 0

    for item in upcoming:
        ticker = item["ticker"]
        company = item.get("company_name", ticker)
        earnings_date = item["earnings_date"]
        eps_est = item.get("eps_estimate")
        is_today = earnings_date == today.isoformat()

        day_label = "Today" if is_today else "Tomorrow"
        date_fmt = datetime.fromisoformat(earnings_date).strftime("%a %d %b %Y")
        eps_str = f"Est. EPS: ${eps_est:.2f}" if eps_est else ""

        msg = (
            f"📅 [StockLens] Earnings {day_label}\n"
            f"{ticker} — {company}\n"
            f"Date: {date_fmt}\n"
            + (f"{eps_str}\n" if eps_str else "")
            + "Watch for results — analysis will follow automatically."
        ).strip()

        try:
            _send_email(
                subject=f"[StockLens] Earnings {day_label}: {ticker} — {company}",
                body=msg,
            )
            sent += 1
            logger.info("[earnings_watcher] Morning reminder sent for %s (%s)", ticker, earnings_date)
        except Exception as exc:
            logger.warning("[earnings_watcher] Reminder failed for %s: %s", ticker, exc)

    return sent


def build_pre_market_digest(events: list[dict]) -> tuple[str, str, str]:
    """
    Format a pre-market digest from recent unsent events.
    Returns (subject, email_body, sms_body). Empty strings if no events.
    """
    if not events:
        return "", "", ""

    date_str = datetime.now(timezone.utc).strftime("%a %d %b %Y")
    subject = f"[StockLens] Earnings Digest — {date_str}"
    lines = [f"Earnings Intelligence Digest — {date_str}", ""]
    sms_parts = [f"StockLens Earnings {date_str}:"]

    for ev in events:
        ticker = ev["ticker"]
        company = ev.get("company_name", ticker)
        beat_miss = ev.get("beat_miss", "UNKNOWN")
        guidance = ev.get("guidance", "UNKNOWN")
        thesis_impact = ev.get("thesis_impact", "NEUTRAL")
        signal_emoji = {"POSITIVE": "✅", "NEGATIVE": "❌", "NEUTRAL": "➡️"}.get(thesis_impact, "")

        analysis: dict = {}
        try:
            analysis = json.loads(ev.get("analysis_json") or "{}")
        except Exception:
            pass

        headline = analysis.get("pre_market_headline", f"{ticker}: {beat_miss}")
        reasoning = analysis.get("thesis_reasoning", "")
        highlights = analysis.get("key_highlights", [])

        lines += [
            "=" * 52,
            f"{ticker} — {company}",
            f"Signal: {thesis_impact} {signal_emoji}  |  {beat_miss}  |  Guidance: {guidance}",
            headline,
        ]
        if reasoning:
            lines += ["", reasoning]

        eps_actual = ev.get("eps_actual")
        eps_est = ev.get("eps_estimate")
        eps_surp = ev.get("eps_surprise_pct")
        if eps_actual is not None and eps_est is not None:
            eps_line = f"EPS ${eps_actual:.2f} vs ${eps_est:.2f} est"
            if eps_surp is not None:
                eps_line += f" ({eps_surp:+.1f}%)"
            lines.append(eps_line)

        if highlights:
            lines.append("Key points:")
            lines += [f"  • {h}" for h in highlights[:3]]
        lines.append("")

        sms_parts.append(f"{ticker}: {beat_miss} {signal_emoji} | Guidance {guidance}")

    return subject, "\n".join(lines), "\n".join(sms_parts)
