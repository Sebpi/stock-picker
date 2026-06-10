"""
agent.sentiment_news — Canonical wrapper around the existing sentiment_agent logic.
Wraps news scan results into a proper AgentSignal with Evidence objects.
"""
from __future__ import annotations

import hashlib
import logging
import os
import re
from datetime import datetime, timedelta, timezone
from typing import Any

from agents import BaseAgent
from schemas import (
    Confidence,
    Direction,
    Evidence,
    Materiality,
    NewsSentimentPayload,
    QualityFlag,
)

logger = logging.getLogger(__name__)

# Positive / negative keyword lists (reused from existing sentiment_scanner.py logic)
POSITIVE_KEYWORDS = [
    "beat", "beats", "exceeds", "record", "growth", "surge", "upgrade",
    "raised", "bullish", "partnership", "contract", "approval", "launch",
    "strong", "outperform", "buy", "strong buy", "positive",
]
NEGATIVE_KEYWORDS = [
    "miss", "misses", "below", "warning", "downgrade", "cut", "bearish",
    "loss", "recall", "investigation", "lawsuit", "decline", "weak",
    "underperform", "sell", "negative", "concern", "layoffs", "fraud",
]

CRITICAL_KEYWORDS = [
    "bankruptcy", "fraud", "sec investigation", "delisted", "ceo resign",
    "cfo resign", "accounting irregularities", "restatement", "sanctions",
    "export ban", "chip ban", "acquisition failed", "deal collapsed",
]


def _sentiment_score(text: str) -> float:
    text_lower = text.lower()
    pos = sum(1 for k in POSITIVE_KEYWORDS if k in text_lower)
    neg = sum(1 for k in NEGATIVE_KEYWORDS if k in text_lower)
    critical = any(k in text_lower for k in CRITICAL_KEYWORDS)
    if critical:
        return -1.0
    raw = (pos - neg) * 0.3
    return max(-1.0, min(1.0, raw))


def _headline_hash(title: str) -> str:
    return hashlib.md5(title.lower().strip().encode(), usedforsecurity=False).hexdigest()[:12]


def _materiality_from_score(score: float) -> float:
    if score <= -0.8:
        return 1.0
    if score <= -0.4 or score >= 0.7:
        return 0.7
    if abs(score) >= 0.2:
        return 0.4
    return 0.1


class SentimentNewsAgent(BaseAgent):
    agent_id = "agent.sentiment_news"
    signal_type = "news_sentiment"
    default_horizons = ["3m", "6m"]

    def _run(self, ticker: str, run_id: str, as_of: datetime):
        news_items = self._fetch_existing_agent_news(ticker)

        if not news_items:
            return self._emit(
                ticker=ticker, run_id=run_id, as_of=as_of,
                score=50.0,
                confidence=Confidence.LOW,
                direction=Direction.NEUTRAL,
                materiality=Materiality.LOW,
                payload=NewsSentimentPayload().model_dump(),
                quality_flags=[QualityFlag.MISSING_FIELD],
            )

        claude_signal = self._try_existing_claude_analysis(ticker, run_id, as_of, news_items)
        if claude_signal:
            return claude_signal

        # ---- Process headlines ----
        seen_hashes: set[str] = set()
        events: list[dict[str, Any]] = []
        scores_24h: list[float] = []
        scores_all: list[float] = []
        cutoff_24h = as_of - timedelta(hours=24)

        for item in news_items:
            title = item.get("title", "") or ""
            pub_ts = item.get("providerPublishTime") or item.get("publishedAt") or item.get("published_at")
            link = item.get("link", "") or item.get("url", "")

            if not title:
                continue

            h = _headline_hash(title)
            if h in seen_hashes:
                continue
            seen_hashes.add(h)

            score = _sentiment_score(title)
            scores_all.append(score)

            published_dt: datetime | None = None
            if pub_ts:
                try:
                    if isinstance(pub_ts, (int, float)) or str(pub_ts).isdigit():
                        published_dt = datetime.fromtimestamp(int(pub_ts), tz=timezone.utc)
                    else:
                        published_dt = datetime.fromisoformat(str(pub_ts).replace("Z", "+00:00"))
                except Exception:
                    pass

            if published_dt and published_dt > cutoff_24h:
                scores_24h.append(score)

            mat_score = _materiality_from_score(score)
            event_type = self._classify_event(title)

            # Novelty: fraction of unique hashes out of total headlines
            novelty = 1.0 - (len(seen_hashes) - 1) / max(1, len(news_items))

            events.append({
                "event_id": h,
                "event_type": event_type,
                "headline_cluster": [title],
                "sentiment": "positive" if score > 0.1 else ("negative" if score < -0.1 else "neutral"),
                "materiality_score": round(mat_score, 2),
                "novelty_score": round(novelty, 2),
                "requires_alert": mat_score >= 0.7,
                "published_at": published_dt.isoformat() if published_dt else None,
                "url": link,
            })

        # ---- Aggregate scores ----
        sentiment_24h = round(sum(scores_24h) / len(scores_24h), 3) if scores_24h else 0.0
        sentiment_7d = round(sum(scores_all) / len(scores_all), 3) if scores_all else 0.0

        narrative_shift = "none"
        if abs(sentiment_24h - sentiment_7d) > 0.2:
            narrative_shift = "improving" if sentiment_24h > sentiment_7d else "deteriorating"
        elif sentiment_24h < -0.3 and sentiment_7d > 0.1:
            narrative_shift = "polarised"

        # ---- Composite agent score (0-100) ----
        # Map average sentiment (-1 to +1) to 0-100
        avg_score = (sentiment_7d + 1) / 2 * 100

        # Boost/penalise for material events
        material_events = [e for e in events if e["materiality_score"] >= 0.7]
        if material_events:
            negative_material = [e for e in material_events if e["sentiment"] == "negative"]
            positive_material = [e for e in material_events if e["sentiment"] == "positive"]
            avg_score = max(0, min(100, avg_score - len(negative_material) * 8 + len(positive_material) * 5))

        # ---- Direction ----
        if avg_score >= 60:
            direction = Direction.POSITIVE
        elif avg_score <= 38:
            direction = Direction.NEGATIVE
        else:
            direction = Direction.NEUTRAL

        # ---- Materiality ----
        if any(e["materiality_score"] >= 1.0 for e in events):
            materiality = Materiality.CRITICAL
        elif material_events:
            materiality = Materiality.HIGH
        else:
            materiality = Materiality.MEDIUM

        # ---- Confidence ----
        if len(events) >= 8:
            confidence = Confidence.HIGH
        elif len(events) >= 4:
            confidence = Confidence.MEDIUM
        else:
            confidence = Confidence.LOW

        flags: list[QualityFlag] = []
        if not events:
            flags.append(QualityFlag.LOW_COVERAGE)

        evidence = [
            Evidence(
                source_type="financial_press",
                source_name="Yahoo Finance news",
                url_or_ref=f"yfinance://news/{ticker}",
                credibility_weight=0.60,
                freshness_score=min(1.0, len(scores_24h) / max(1, len(events))),
                extracted_facts=[
                    f"Headlines analysed: {len(events)}",
                    f"24h sentiment: {sentiment_24h:+.2f}",
                    f"7d sentiment: {sentiment_7d:+.2f}",
                    f"Material events: {len(material_events)}",
                    f"Narrative shift: {narrative_shift}",
                ],
            )
        ]

        payload = NewsSentimentPayload(
            events=events[:10],  # store top 10
            sentiment_score_24h=sentiment_24h,
            sentiment_score_7d=sentiment_7d,
            narrative_shift=narrative_shift,
        ).model_dump()

        return self._emit(
            ticker=ticker,
            run_id=run_id,
            as_of=as_of,
            score=round(avg_score, 2),
            confidence=confidence,
            direction=direction,
            materiality=materiality,
            payload=payload,
            evidence=evidence,
            quality_flags=flags,
        )

    # ------------------------------------------------------------------
    # Existing sentiment-agent integration
    # ------------------------------------------------------------------

    @staticmethod
    def _fetch_existing_agent_news(ticker: str) -> list[dict[str, Any]]:
        """Fetch news via sentiment agent → yfinance → Yahoo RSS → Finviz RSS fallback chain."""
        # 1. Existing sentiment agent
        try:
            from sentiment_agent import fetch_ticker_news
            items = fetch_ticker_news(ticker)
            if items:
                return items
        except Exception as exc:
            logger.debug("Existing sentiment news fetch failed for %s: %s", ticker, exc)

        # 2. yfinance .news
        try:
            import yfinance as yf
            t = yf.Ticker(ticker)
            raw_news = SentimentNewsAgent._timed_fetch(lambda: t.news, f"{ticker}/news") or []
            items = []
            for item in raw_news[:15]:
                if not hasattr(item, "get"):
                    continue
                content = item.get("content") or {}
                title = content.get("title") if content else item.get("title")
                url = (
                    (content.get("clickThroughUrl") or {}).get("url")
                    if content else item.get("link")
                ) or (
                    (content.get("canonicalUrl") or {}).get("url")
                    if content else item.get("url", "")
                )
                published = content.get("pubDate") if content else item.get("providerPublishTime")
                if title:
                    items.append({
                        "ticker": ticker, "title": title,
                        "publisher": item.get("publisher", ""),
                        "published_at": published, "url": url or "",
                    })
            if items:
                return items
        except Exception:
            pass

        # URL-encode the ticker before interpolating into feed URLs (defense in
        # depth — the orchestrator validates tickers, but encode at the boundary).
        from urllib.parse import quote
        q_ticker = quote(ticker, safe="")

        # 3. Yahoo Finance RSS (no key required)
        rss_items = SentimentNewsAgent._fetch_rss(
            f"https://feeds.finance.yahoo.com/rss/2.0/headline?s={q_ticker}&region=US&lang=en-US",
            ticker, "Yahoo RSS",
        )
        if rss_items:
            return rss_items

        # 4. Finviz RSS
        finviz_items = SentimentNewsAgent._fetch_rss(
            f"https://finviz.com/rss.ashx?t={q_ticker}",
            ticker, "Finviz RSS",
        )
        return finviz_items

    @staticmethod
    def _fetch_rss(url: str, ticker: str, source: str) -> list[dict[str, Any]]:
        """Generic RSS fetcher — returns normalised news dicts."""
        try:
            import httpx
            import xml.etree.ElementTree as ET
            r = httpx.get(url, timeout=8, headers={"User-Agent": "StockPicker/1.0"})
            if r.status_code != 200:
                return []
            root = ET.fromstring(r.text)  # nosec B314 — RSS text only, no entity expansion
            ns = {"atom": "http://www.w3.org/2005/Atom"}
            items: list[dict[str, Any]] = []
            # RSS 2.0
            for item in root.findall(".//item")[:15]:
                title = item.findtext("title", "").strip()
                link  = item.findtext("link", "").strip()
                pubDate = item.findtext("pubDate", "")
                if title:
                    items.append({"ticker": ticker, "title": title, "url": link,
                                  "publisher": source, "published_at": pubDate})
            # Atom
            for entry in root.findall(".//atom:entry", ns)[:15]:
                title = (entry.findtext("atom:title", "", ns) or "").strip()
                link  = (entry.find("atom:link", ns) or {}).get("href", "")
                pub   = entry.findtext("atom:published", "", ns)
                if title:
                    items.append({"ticker": ticker, "title": title, "url": link,
                                  "publisher": source, "published_at": pub})
            return items
        except Exception as exc:
            logger.debug("RSS fetch failed [%s] %s: %s", source, ticker, exc)
            return []

    def _try_existing_claude_analysis(
        self,
        ticker: str,
        run_id: str,
        as_of: datetime,
        news_items: list[dict[str, Any]],
    ):
        """Canonicalise the established Claude sentiment analyzer into AgentSignal.

        This path does not send alerts or mutate the sentiment agent state; it only
        reuses its prompt and analysis rubric. If Claude is unavailable, callers fall
        back to the deterministic keyword scorer below.
        """
        if not os.getenv("ANTHROPIC_API_KEY"):
            return None
        try:
            from sentiment_agent import (
                analyse_with_claude,
                build_analysis_prompt,
                get_price_data,
                load_portfolio_tickers,
                load_watchlist_tickers,
            )
            portfolio_tickers = load_portfolio_tickers()
            watchlist_tickers = load_watchlist_tickers()
            prices = get_price_data([ticker])
            prompt = build_analysis_prompt(news_items, prices, portfolio_tickers, watchlist_tickers)
            analysis = analyse_with_claude(prompt)
        except Exception as exc:
            logger.debug("Existing Claude sentiment analysis failed for %s: %s", ticker, exc)
            return None

        if not analysis:
            return None

        severity = str(analysis.get("severity") or "none").lower()
        expected_move = analysis.get("expected_move_pct")
        try:
            expected_move_abs = abs(float(expected_move or 0.0))
        except (TypeError, ValueError):
            expected_move_abs = 0.0

        score = 50.0
        direction_raw = analysis.get("direction")
        if direction_raw == "up":
            score += min(35.0, expected_move_abs * 4)
            direction = Direction.POSITIVE
        elif direction_raw == "down":
            score -= min(35.0, expected_move_abs * 4)
            direction = Direction.NEGATIVE
        elif analysis.get("disruption_detected"):
            score -= 10.0
            direction = Direction.MIXED
        else:
            direction = Direction.NEUTRAL

        if severity == "critical":
            materiality = Materiality.CRITICAL
            confidence = Confidence.HIGH
        elif severity == "high":
            materiality = Materiality.HIGH
            confidence = Confidence.HIGH if analysis.get("alert_worthy") else Confidence.MEDIUM
        elif severity == "medium":
            materiality = Materiality.MEDIUM
            confidence = Confidence.MEDIUM
        else:
            materiality = Materiality.LOW
            confidence = Confidence.MEDIUM

        top_headlines = analysis.get("top_headlines") or []
        events = []
        for item in top_headlines[:10]:
            if isinstance(item, dict):
                title = item.get("title", "")
                url = item.get("url", "")
            else:
                title = str(item)
                url = ""
            if not title:
                continue
            events.append({
                "event_id": _headline_hash(title),
                "event_type": self._classify_event(title),
                "headline_cluster": [title],
                "sentiment": "positive" if direction_raw == "up" else ("negative" if direction_raw == "down" else "neutral"),
                "materiality_score": 1.0 if severity == "critical" else (0.75 if severity == "high" else 0.4),
                "novelty_score": 1.0,
                "requires_alert": bool(analysis.get("alert_worthy")),
                "published_at": None,
                "url": url,
            })

        if not events:
            for item in news_items[:5]:
                title = item.get("title", "")
                if title:
                    events.append({
                        "event_id": _headline_hash(title),
                        "event_type": self._classify_event(title),
                        "headline_cluster": [title],
                        "sentiment": "neutral",
                        "materiality_score": 0.1,
                        "novelty_score": 1.0,
                        "requires_alert": False,
                        "published_at": item.get("published_at"),
                        "url": item.get("url", ""),
                    })

        payload = NewsSentimentPayload(
            events=events,
            sentiment_score_24h=round((score - 50) / 50, 3),
            sentiment_score_7d=round((score - 50) / 50, 3),
            narrative_shift="deteriorating" if direction == Direction.NEGATIVE else ("improving" if direction == Direction.POSITIVE else "none"),
        ).model_dump()

        evidence = [
            Evidence(
                source_type="financial_press",
                source_name="StockPicker Claude sentiment agent",
                url_or_ref=f"sentiment_agent://{ticker}",
                credibility_weight=0.67,
                freshness_score=1.0,
                extracted_facts=[
                    f"Severity: {severity}",
                    f"Alert worthy: {bool(analysis.get('alert_worthy'))}",
                    f"Expected move: {expected_move}",
                    f"Summary: {analysis.get('summary', '')}",
                ],
            )
        ]

        return self._emit(
            ticker=ticker,
            run_id=run_id,
            as_of=as_of,
            score=round(max(0.0, min(100.0, score)), 2),
            confidence=confidence,
            direction=direction,
            materiality=materiality,
            payload=payload,
            evidence=evidence,
            quality_flags=[QualityFlag.LLM_UNVERIFIED],
        )

    @staticmethod
    def _classify_event(title: str) -> str:
        t = title.lower()
        if any(k in t for k in ["earnings", "revenue", "eps", "quarterly", "results"]):
            return "earnings"
        if any(k in t for k in ["guidance", "outlook", "forecast", "target"]):
            return "guidance"
        if any(k in t for k in ["fda", "approval", "regulatory", "sec", "doj", "antitrust"]):
            return "regulatory"
        if any(k in t for k in ["upgrade", "downgrade", "price target", "analyst", "buy", "sell", "hold"]):
            return "analyst"
        if any(k in t for k in ["launch", "product", "release", "announce"]):
            return "product"
        if any(k in t for k in ["lawsuit", "litigation", "sued", "settlement"]):
            return "litigation"
        if any(k in t for k in ["merger", "acquisition", "takeover", "buyout", "deal"]):
            return "m_and_a"
        if any(k in t for k in ["ceo", "cfo", "executive", "resign", "appoint"]):
            return "management"
        return "other"
