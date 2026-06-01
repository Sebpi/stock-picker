"""
Finnhub real-time price feed.

A daemon thread keeps a WebSocket open to wss://ws.finnhub.io.
A separate seeding thread pre-fetches price + prev_close via the REST
quote endpoint so change% is available from the first WS trade.

Only plain US tickers (no dot in symbol) are subscribed via WebSocket —
international tickers like SPX.L are not available on the free tier and
fall back to the yfinance cache in the main app.

Usage (called from main.py startup):
    import finnhub_prices
    finnhub_prices.start(load_watchlist())

    # When watchlist changes:
    finnhub_prices.subscribe(["AAPL"])
    finnhub_prices.unsubscribe("TSLA")
"""
from __future__ import annotations

import json
import logging
import os
import threading
import time
from datetime import datetime, timezone
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

_KEY: str = os.getenv("FINNHUB_API_KEY", "").strip()
_prices: dict[str, dict] = {}       # ticker -> price record
_subscribed: set[str] = set()
_ws_app = None                       # current WebSocketApp instance
_lock = threading.Lock()
_started = False


# ── Public API ────────────────────────────────────────────────────────────────

def get_prices() -> dict[str, dict]:
    """Snapshot of all current live prices."""
    with _lock:
        return dict(_prices)


def get_price(ticker: str) -> Optional[dict]:
    with _lock:
        return _prices.get(ticker)


def subscribe(tickers: list[str]) -> None:
    """Add tickers to the live feed. Idempotent."""
    with _lock:
        new = [t for t in tickers if t not in _subscribed]
        _subscribed.update(tickers)
    if not new:
        return
    # Seed REST data in background so change% is ready before first WS trade
    threading.Thread(target=_seed_rest, args=(new,), daemon=True).start()
    if _ws_app:
        for t in _ws_tickers(new):
            try:
                _ws_app.send(json.dumps({"type": "subscribe", "symbol": t}))
            except Exception:
                pass


def unsubscribe(ticker: str) -> None:
    with _lock:
        _subscribed.discard(ticker)
        _prices.pop(ticker, None)
    if _ws_app and _is_ws_ticker(ticker):
        try:
            _ws_app.send(json.dumps({"type": "unsubscribe", "symbol": ticker}))
        except Exception:
            pass


def start(tickers: list[str]) -> None:
    """Start the background WebSocket thread. Safe to call multiple times."""
    global _started
    if _started:
        return
    _started = True
    if not _KEY:
        logger.info("[finnhub_prices] FINNHUB_API_KEY not set — real-time prices disabled")
        return
    with _lock:
        _subscribed.update(tickers)
    # Seed initial prices via REST (non-blocking)
    threading.Thread(target=_seed_rest, args=(list(tickers),), daemon=True).start()
    # Start persistent WebSocket thread
    threading.Thread(target=_run_ws, daemon=True, name="finnhub-ws").start()
    logger.info("[finnhub_prices] started with %d tickers", len(tickers))


# ── Internals ─────────────────────────────────────────────────────────────────

def _is_ws_ticker(ticker: str) -> bool:
    """Only plain US tickers are available on Finnhub free WS tier."""
    return "." not in ticker and ":" not in ticker


def _ws_tickers(tickers: list[str]) -> list[str]:
    return [t for t in tickers if _is_ws_ticker(t)]


def _seed_rest(tickers: list[str]) -> None:
    """Fetch initial price + prev_close via Finnhub REST quote. Rate: ~5 req/s."""
    for ticker in tickers:
        if not _KEY:
            return
        try:
            r = httpx.get(
                "https://finnhub.io/api/v1/quote",
                params={"symbol": ticker, "token": _KEY},
                timeout=6,
            )
            if r.status_code == 200:
                d = r.json()
                c = d.get("c")       # current price
                pc = d.get("pc")     # previous close
                change = d.get("d") or 0.0
                change_pct = d.get("dp") or 0.0
                if c and float(c) > 0:
                    with _lock:
                        _prices[ticker] = {
                            "price": round(float(c), 2),
                            "change": round(float(change), 2),
                            "change_pct": round(float(change_pct), 2),
                            "prev_close": round(float(pc), 2) if pc else round(float(c), 2),
                            "ts": datetime.now(timezone.utc).isoformat(),
                            "source": "finnhub",
                        }
        except Exception as exc:
            logger.debug("[finnhub_prices] REST seed error %s: %s", ticker, exc)
        time.sleep(0.2)  # ~5 req/s — well within 60/min free limit


def _on_message(ws, message: str) -> None:
    try:
        data = json.loads(message)
        if data.get("type") != "trade":
            return
        for trade in data.get("data", []):
            sym: str = trade.get("s", "")
            price = trade.get("p")
            if not sym or not price or float(price) <= 0:
                continue
            price = float(price)
            with _lock:
                prev = _prices.get(sym, {})
                prev_close = prev.get("prev_close") or price
                change = price - prev_close
                change_pct = (change / prev_close * 100) if prev_close else 0.0
                _prices[sym] = {
                    "price": round(price, 2),
                    "change": round(change, 2),
                    "change_pct": round(change_pct, 2),
                    "prev_close": prev_close,
                    "ts": datetime.now(timezone.utc).isoformat(),
                    "source": "live",
                }
    except Exception as exc:
        logger.debug("[finnhub_prices] on_message error: %s", exc)


def _on_open(ws) -> None:
    global _ws_app
    _ws_app = ws
    logger.info("[finnhub_prices] WebSocket connected")
    with _lock:
        tickers = list(_subscribed)
    for t in _ws_tickers(tickers):
        ws.send(json.dumps({"type": "subscribe", "symbol": t}))


def _on_error(ws, error) -> None:
    logger.warning("[finnhub_prices] WebSocket error: %s", error)


def _on_close(ws, code, msg) -> None:
    global _ws_app
    _ws_app = None
    logger.info("[finnhub_prices] WebSocket closed (%s)", code)


def _run_ws() -> None:
    """Persistent reconnect loop."""
    try:
        import websocket as _ws_lib
    except ImportError:
        logger.error("[finnhub_prices] websocket-client not installed — real-time prices unavailable")
        return

    backoff = 5
    while True:
        if not _KEY:
            time.sleep(60)
            continue
        try:
            ws = _ws_lib.WebSocketApp(
                f"wss://ws.finnhub.io?token={_KEY}",
                on_open=_on_open,
                on_message=_on_message,
                on_error=_on_error,
                on_close=_on_close,
            )
            ws.run_forever(ping_interval=30, ping_timeout=10)
            backoff = 5  # reset on clean disconnect
        except Exception as exc:
            logger.warning("[finnhub_prices] run error: %s", exc)
        finally:
            global _ws_app
            _ws_app = None
        logger.info("[finnhub_prices] reconnecting in %ds", backoff)
        time.sleep(backoff)
        backoff = min(backoff * 2, 60)
