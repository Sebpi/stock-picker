import argparse
import datetime
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import httpx
import yfinance as yf

_FINNHUB_KEY = os.getenv("FINNHUB_API_KEY", "").strip()
MAX_SCAN_WORKERS = int(os.getenv("SENTIMENT_SCAN_WORKERS", "8"))


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


def _string_filter(ticker: str, company_name: str, headlines: list[dict]) -> list[dict]:
    """Fast pre-filter: keep headlines that at least name the ticker or company."""
    name_clean = (company_name or ticker).lower()
    for suffix in (" inc", " corp", " corporation", " ltd", " plc", " co", ".", ","):
        name_clean = name_clean.replace(suffix, "")
    name_clean = name_clean.strip()
    ticker_lower = ticker.lower()
    return [
        h for h in headlines
        if ticker_lower in h["title"].lower()
        or (len(name_clean) > 3 and name_clean in h["title"].lower())
    ]


def _classify_and_summarize(ticker: str, company_name: str, candidates: list[dict]) -> tuple[list[dict], str]:
    """
    Use Claude to decide which candidates are *primarily about* ticker (not just mentioned),
    then summarise the relevant ones. Returns (relevant_headlines, summary).
    """
    api_key = os.getenv("ANTHROPIC_API_KEY", "").strip()
    if not api_key:
        return candidates, ""
    if not candidates:
        return [], "No company-specific headlines found for this period."

    try:
        import anthropic
        numbered = "\n".join(f"{i + 1}. {h['title']}" for i, h in enumerate(candidates[:10]))
        client = anthropic.Anthropic(api_key=api_key)
        resp = client.messages.create(
            model=os.getenv("ANTHROPIC_MODEL", "claude-haiku-4-5-20251001"),
            max_tokens=300,
            messages=[{
                "role": "user",
                "content": (
                    f"You are reviewing headlines for relevance to {ticker} ({company_name}).\n\n"
                    f"Headlines:\n{numbered}\n\n"
                    f"Step 1 — on ONE line, list the numbers of headlines where {ticker} is the "
                    f"PRIMARY subject (the article is fundamentally about this company, not just "
                    f"mentioning it in a list or passing comparison). Format exactly:\n"
                    f"RELEVANT: 1,3 (or RELEVANT: none)\n\n"
                    f"Step 2 — for the relevant headlines only, write 2-3 bullet points "
                    f"(each ≤20 words) covering key developments: earnings, guidance, analyst "
                    f"moves, leadership, products. If RELEVANT was none, write: "
                    f"No company-specific news this period."
                ),
            }],
        )
        text = resp.content[0].text.strip()

        relevant_indices: list[int] = []
        summary_lines: list[str] = []
        for line in text.splitlines():
            stripped = line.strip()
            if stripped.upper().startswith("RELEVANT:"):
                nums = stripped.split(":", 1)[1].strip()
                if nums.lower() != "none":
                    for n in nums.split(","):
                        try:
                            idx = int(n.strip()) - 1
                            if 0 <= idx < len(candidates):
                                relevant_indices.append(idx)
                        except ValueError:
                            pass
            elif stripped:
                summary_lines.append(stripped)

        relevant = [candidates[i] for i in sorted(set(relevant_indices))]
        summary = "\n".join(summary_lines).strip()
        if not relevant:
            summary = "No company-specific news this period."
        return relevant, summary

    except Exception:
        return candidates, ""  # fallback: show string-matched on error


def analyze_ticker(ticker: str, with_summary: bool = False) -> dict:
    stock = yf.Ticker(ticker)

    # Fetch sequentially — yfinance Ticker is not thread-safe for concurrent
    # property access on the same instance. Outer pool handles parallelism.
    headlines: list[dict] = _fetch_finnhub_headlines(ticker)
    info = stock.info or {}
    yf_news = []
    if not headlines:
        try:
            yf_news = stock.news or []
        except Exception:
            pass

    if not headlines:
        try:
            for item in yf_news[:8]:
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

    company_name = info.get("shortName", ticker)
    # String-match pre-filter runs for all scans (fast, no API cost)
    candidates = _string_filter(ticker, company_name, headlines)
    if with_summary:
        # Deep filter: Claude decides primary-subject vs passing-mention, and summarises
        display_headlines, news_summary = _classify_and_summarize(ticker, company_name, candidates)
    else:
        display_headlines, news_summary = candidates, ""

    return {
        "ticker": ticker,
        "name": company_name,
        "price": round(price, 2) if isinstance(price, (int, float)) else None,
        "change_pct": round(change_pct, 2) if isinstance(change_pct, (int, float)) else None,
        "target_mean_price": round(target_mean, 2) if isinstance(target_mean, (int, float)) else None,
        "recommendation": recommendation or "n/a",
        "headline_count": len(display_headlines),
        "sentiment_score": composite,
        "sentiment": classify_sentiment(composite),
        "headlines": display_headlines[:5],
        "news_summary": news_summary,
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

    def _scan_one(t: str) -> dict:
        try:
            return analyze_ticker(t, with_summary=True)
        except Exception as exc:
            err_str = str(exc)
            if "429" in err_str or "too many" in err_str.lower():
                time.sleep(10)
                try:
                    return analyze_ticker(t, with_summary=True)
                except Exception as exc2:
                    err_str = str(exc2)
            return {"ticker": t, "name": t, "sentiment": "error", "sentiment_score": 0, "error": err_str}

    result_map: dict[str, dict] = {}
    with ThreadPoolExecutor(max_workers=MAX_SCAN_WORKERS) as pool:
        futures = {pool.submit(_scan_one, t): t for t in watchlist}
        for future in as_completed(futures):
            result_map[futures[future]] = future.result()

    # Preserve watchlist order in output
    results = [result_map[t] for t in watchlist if t in result_map]
    print(json.dumps({"watchlist": watchlist, "results": results}, indent=2))


if __name__ == "__main__":
    main()
