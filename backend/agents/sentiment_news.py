"""
agent.sentiment_news — Canonical wrapper around the existing sentiment_agent logic.
Wraps news scan results into a proper AgentSignal with Evidence objects.
"""
from __future__ import annotations

import hashlib
import logging
import re
from datetime import datetime, timedelta, timezone
from typing import Any

import yfinance as yf

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
    return hashlib.md5(title.lower().strip().encode()).hexdigest()[:12]


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
        t = yf.Ticker(ticker)
        news_items = []
        try:
            raw_news = t.news or []
            news_items = raw_news[:15]
        except Exception:
            pass

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

        # ---- Process headlines ----
        seen_hashes: set[str] = set()
        events: list[dict[str, Any]] = []
        scores_24h: list[float] = []
        scores_all: list[float] = []
        cutoff_24h = as_of - timedelta(hours=24)

        for item in news_items:
            # yfinance news item is a dict or object
            if hasattr(item, "get"):
                title = item.get("title", "") or ""
                pub_ts = item.get("providerPublishTime") or item.get("publishedAt")
                link = item.get("link", "") or item.get("url", "")
            else:
                title = getattr(item, "title", "") or ""
                pub_ts = getattr(item, "providerPublishTime", None)
                link = getattr(item, "link", "") or getattr(item, "url", "")

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
                    published_dt = datetime.fromtimestamp(int(pub_ts), tz=timezone.utc)
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
