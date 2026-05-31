import argparse
import datetime
import json
import os
import time
from pathlib import Path

import httpx
import yfinance as yf

_FINNHUB_KEY = os.getenv("FINNHUB_API_KEY", "").strip()


def _fetch_finnhub_headlines(ticker: str, days: int = 7) -> list[dict]:
    """Return headline objects {title, url} from Finnhub. Empty list if key unset or request fails."""
    if not _FINNHUB_KEY:
        return []
    to_date = datetime.date.today().isoformat()
    from_date = (datetime.date.today() - datetime.timedelta(days=days)).isoformat()
    try:
        r = httpx.get(
            "https://finnhub.io/api/v1/company-news",
            params={"symbol": ticker, "from": from_date, "to": to_date, "token": _FINNHUB_KEY},
            timeout=10,
        )
        r.raise_for_status()
        items = r.json() or []
        return [
            {"title": item["headline"], "url": item.get("url", "")}
            for item in items[:8]
            if item.get("headline")
        ]
    except Exception:
        return []


BASE_DIR = Path(__file__).resolve().parent
WATCHLIST_FILE = BASE_DIR / "watchlist.json"

POSITIVE_KEYWORDS = {
    "beat", "beats", "growth", "record", "surge", "gain", "up", "upgrade",
    "bullish", "partnership", "contract", "launch", "profit", "strong",
    "buyback", "expansion", "breakthrough", "outperform", "raises", "raised",
    "rebound", "momentum", "demand", "guidance",
}

NEGATIVE_KEYWORDS = {
    "miss", "misses", "drop", "down", "downgrade", "bearish", "lawsuit",
    "delay", "cut", "weak", "fall", "warning", "risk", "decline",
    "investigation", "volatile", "loss", "slump", "cuts", "lowered",
    "recession", "pressure", "shortfall", "concern",
}


def load_watchlist():
    if not WATCHLIST_FILE.exists():
        return []
    try:
        data = json.loads(WATCHLIST_FILE.read_text(encoding="utf-8"))
        return [str(item).upper() for item in data if str(item).strip()]
    except Exception:
        return []


def headline_score(headline) -> int:
    text = str(headline.get("title", "") if isinstance(headline, dict) else headline).lower()
    score = 0
    for word in POSITIVE_KEYWORDS:
        if word in text:
            score += 1
    for word in NEGATIVE_KEYWORDS:
        if word in text:
            score -= 1
    return score


def classify_sentiment(score):
    if score >= 2:
        return "bullish"
    if score <= -2:
        return "bearish"
    return "neutral"


def _summarize_with_claude(ticker: str, headlines: list[dict]) -> str:
    """Call Claude Haiku to produce a 2-3 bullet summary of relevant news for ticker."""
    api_key = os.getenv("ANTHROPIC_API_KEY", "").strip()
    if not api_key or not headlines:
        return ""
    try:
        import anthropic
        titles = "\n".join(f"- {h['title']}" for h in headlines[:8])
        client = anthropic.Anthropic(api_key=api_key)
        resp = client.messages.create(
            model=os.getenv("ANTHROPIC_MODEL", "claude-haiku-4-5-20251001"),
            max_tokens=200,
            messages=[{
                "role": "user",
                "content": (
                    f"You are analysing recent news for {ticker} stock. Headlines:\n\n{titles}\n\n"
                    f"In 2-3 short bullet points (each under 20 words), summarise ONLY what is directly "
                    f"relevant to {ticker}'s business or stock price. Skip macro or sector news unless it "
                    f"directly names {ticker}. Be specific. If nothing is relevant, write exactly: "
                    f"'No material company-specific news in this period.'"
                ),
            }],
        )
        return resp.content[0].text.strip()
    except Exception:
        return ""


def analyze_ticker(ticker: str, with_summary: bool = False) -> dict:
    stock = yf.Ticker(ticker)
    info = stock.info or {}

    # Finnhub first (works from cloud IPs); yfinance fallback for local runs
    headlines: list[dict] = _fetch_finnhub_headlines(ticker)
    if not headlines:
        try:
            for item in (stock.news or [])[:8]:
                content = item.get("content") if isinstance(item.get("content"), dict) else {}
                title = content.get("title") or item.get("title") or ""
                url = (
                    (content.get("clickThroughUrl") or {}).get("url")
                    or (content.get("canonicalUrl") or {}).get("url")
                    or item.get("link", "")
                )
                if title:
                    headlines.append({"title": title, "url": url or ""})
        except Exception:
            pass

    news_score = sum(headline_score(h) for h in headlines)

    price = info.get("currentPrice") or info.get("regularMarketPrice")
    change_pct = info.get("regularMarketChangePercent") or 0
    target_mean = info.get("targetMeanPrice")
    recommendation = info.get("recommendationKey")

    composite = news_score
    if change_pct > 1:
        composite += 1
    elif change_pct < -1:
        composite -= 1

    if recommendation == "strong_buy":
        composite += 2
    elif recommendation == "buy":
        composite += 1
    elif recommendation == "hold":
        composite += 0
    elif recommendation == "sell":
        composite -= 1
    elif recommendation == "strong_sell":
        composite -= 2

    return {
        "ticker": ticker,
        "name": info.get("shortName", ticker),
        "price": round(price, 2) if isinstance(price, (int, float)) else None,
        "change_pct": round(change_pct, 2) if isinstance(change_pct, (int, float)) else None,
        "target_mean_price": round(target_mean, 2) if isinstance(target_mean, (int, float)) else None,
        "recommendation": recommendation or "n/a",
        "headline_count": len(headlines),
        "sentiment_score": composite,
        "sentiment": classify_sentiment(composite),
        "headlines": headlines[:5],
        "news_summary": _summarize_with_claude(ticker, headlines) if with_summary else "",
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--ticker")
    parser.add_argument("--list", action="store_true")
    args = parser.parse_args()

    watchlist = load_watchlist()

    if args.list:
        print(json.dumps({"watchlist": watchlist}, indent=2))
        return

    if args.ticker:
        # Single-ticker scan: include Claude summary
        result = analyze_ticker(args.ticker.upper(), with_summary=True)
        print(json.dumps(result, indent=2))
        return

    if not watchlist:
        print(json.dumps({"watchlist": [], "results": []}, indent=2))
        return

    results = []
    for i, ticker in enumerate(watchlist):
        if i > 0:
            time.sleep(0.8)  # stay well under Yahoo Finance's rate limit
        try:
            results.append(analyze_ticker(ticker, with_summary=False))
        except Exception as exc:
            err_str = str(exc)
            if "429" in err_str or "Too Many" in err_str.lower():
                # Rate-limited mid-scan: wait and retry once before giving up
                time.sleep(15)
                try:
                    results.append(analyze_ticker(ticker, with_summary=False))
                    continue
                except Exception:
                    pass
            results.append({
                "ticker": ticker,
                "name": ticker,
                "sentiment": "error",
                "sentiment_score": 0,
                "error": err_str,
            })

    print(json.dumps({"watchlist": watchlist, "results": results}, indent=2))


if __name__ == "__main__":
    main()
