import argparse
import json
from pathlib import Path

import yfinance as yf


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


def headline_score(headline):
    text = str(headline or "").lower()
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


def analyze_ticker(ticker):
    stock = yf.Ticker(ticker)
    info = stock.info or {}
    headlines = []
    news_items = []

    try:
        news_items = stock.news or []
    except Exception:
        news_items = []

    for item in news_items[:8]:
        title = item.get("title") or ""
        if title:
            headlines.append(title)

    news_score = sum(headline_score(title) for title in headlines)

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
        result = analyze_ticker(args.ticker.upper())
        print(json.dumps(result, indent=2))
        return

    if not watchlist:
        print(json.dumps({"watchlist": [], "results": []}, indent=2))
        return

    results = []
    for ticker in watchlist:
        try:
            results.append(analyze_ticker(ticker))
        except Exception as exc:
            results.append({
                "ticker": ticker,
                "name": ticker,
                "sentiment": "error",
                "sentiment_score": 0,
                "error": str(exc),
            })

    print(json.dumps({"watchlist": watchlist, "results": results}, indent=2))


if __name__ == "__main__":
    main()
