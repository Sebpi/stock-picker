"""
agent.industry_competition — Competitive position, peer read-across and thematic exposure.
Scores 0-100; higher = stronger industry position.
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

import yfinance as yf

import db
from agents import BaseAgent
from schemas import (
    Confidence,
    Direction,
    Evidence,
    IndustryPayload,
    Materiality,
    QualityFlag,
)

logger = logging.getLogger(__name__)

# Controlled vocabulary for theme exposures
THEME_MAP: dict[str, list[str]] = {
    "Semiconductors": ["AI", "semiconductor", "data_center"],
    "Technology": ["cloud", "AI", "software"],
    "Software—Application": ["SaaS", "cloud", "AI"],
    "Software—Infrastructure": ["cloud", "cybersecurity", "AI"],
    "Communication Services": ["digital_media", "cloud"],
    "Biotechnology": ["biotech"],
    "Pharmaceutical": ["biotech"],
    "Electrical Equipment": ["energy_tech"],
    "Solar": ["energy_tech", "cleantech"],
    "Financials": ["fintech"],
    "Banks": ["fintech"],
    "Internet Content & Information": ["AI", "digital_media", "cloud"],
    "Electronic Components": ["semiconductor"],
}

COMPETITIVE_THRESHOLDS = {
    "leader": (0.75, 0.75),
    "challenger": (0.50, 0.0),
    "niche": (0.0, 0.0),
}


class IndustryCompetitionAgent(BaseAgent):
    agent_id = "agent.industry_competition"
    signal_type = "industry_position"
    default_horizons = ["6m", "12m"]  # structural factors matter more over longer horizons

    # ------------------------------------------------------------------
    # Scoring rubric
    # ------------------------------------------------------------------

    @staticmethod
    def _score_growth_rank(rank: float | None) -> float:
        if rank is None:
            return 20.0
        return rank * 40.0

    @staticmethod
    def _score_margin_rank(rank: float | None) -> float:
        if rank is None:
            return 17.5
        return rank * 35.0

    @staticmethod
    def _position_bonus(position: str) -> float:
        return {"leader": 25.0, "challenger": 18.0, "niche": 12.0,
                "deteriorating": 4.0, "unknown": 10.0}.get(position, 10.0)

    # ------------------------------------------------------------------
    # Core run
    # ------------------------------------------------------------------

    def _run(self, ticker: str, run_id: str, as_of: datetime):
        t = yf.Ticker(ticker)
        info = self._timed_fetch(lambda: t.info, f"{ticker}/info") or {}

        sector = self._safe_get(info, "sector", "")
        industry = self._safe_get(info, "industry", "")
        rev_growth = self._safe_get(info, "revenueGrowth")
        gross_margin = self._safe_get(info, "grossMargins")

        db.upsert_ticker(ticker, info)

        # ---- Peer group ----
        peer_group = db.get_peer_group(ticker)
        if not peer_group:
            peer_group = self._derive_peer_group(info, ticker)
            if peer_group:
                db.set_peer_group(ticker, peer_group)

        # ---- Peer metrics ----
        peer_growths: list[float] = []
        peer_margins: list[float] = []
        read_across_events: list[dict[str, Any]] = []

        for peer in peer_group[:8]:
            p_ticker = yf.Ticker(peer)
            p_info = self._timed_fetch(lambda pt=p_ticker: pt.info, f"{peer}/info") or {}
            pg = self._safe_get(p_info, "revenueGrowth")
            pm = self._safe_get(p_info, "grossMargins")
            if pg is not None:
                peer_growths.append(pg)
            if pm is not None:
                peer_margins.append(pm)

            # Read-across from sentiment agent signals
            peer_signal = db.get_signal_for_agent(peer, "agent.sentiment_news")
            if peer_signal and peer_signal.materiality.value in ("high", "critical"):
                read_across_events.append({
                    "source_ticker": peer,
                    "affected_ticker": ticker,
                    "materiality": peer_signal.materiality.value,
                    "sentiment": peer_signal.direction.value,
                    "reason": f"Material event at peer {peer} may affect {sector} sector",
                })

        # ---- Rank calculations ----
        growth_rank: float | None = None
        if rev_growth is not None and peer_growths:
            all_g = peer_growths + [rev_growth]
            all_g_sorted = sorted(all_g)
            growth_rank = round(all_g_sorted.index(rev_growth) / (len(all_g_sorted) - 1), 3) if len(all_g_sorted) > 1 else 0.5

        margin_rank: float | None = None
        if gross_margin is not None and peer_margins:
            all_m = peer_margins + [gross_margin]
            all_m_sorted = sorted(all_m)
            margin_rank = round(all_m_sorted.index(gross_margin) / (len(all_m_sorted) - 1), 3) if len(all_m_sorted) > 1 else 0.5

        # ---- Competitive position ----
        competitive_position = self._classify_position(growth_rank, margin_rank)

        # ---- Theme exposures ----
        themes = self._map_themes(industry, sector, info)

        # ---- Industry risks ----
        industry_risks = self._derive_risks(sector, info)

        # ---- Score ----
        score = (
            self._score_growth_rank(growth_rank)
            + self._score_margin_rank(margin_rank)
            + self._position_bonus(competitive_position)
        )
        score = float(min(100, max(0, score)))

        # ---- Flags ----
        flags: list[QualityFlag] = []
        if not peer_group:
            flags.append(QualityFlag.LOW_COVERAGE)
        if growth_rank is None and margin_rank is None:
            flags.append(QualityFlag.MISSING_FIELD)

        # ---- Direction ----
        if competitive_position == "leader":
            direction = Direction.POSITIVE
        elif competitive_position in ("deteriorating",):
            direction = Direction.NEGATIVE
        else:
            direction = Direction.NEUTRAL if score >= 40 else Direction.NEGATIVE

        confidence = Confidence.HIGH if (growth_rank and margin_rank and len(peer_group) >= 4) else Confidence.MEDIUM

        evidence = [
            Evidence.from_market_data(ticker, [
                f"Sector: {sector}",
                f"Industry: {industry}",
                f"Peers analysed: {len(peer_group)}",
                f"Growth rank vs peers: {growth_rank:.2f}" if growth_rank else "Growth rank: N/A",
                f"Margin rank vs peers: {margin_rank:.2f}" if margin_rank else "Margin rank: N/A",
                f"Competitive position: {competitive_position}",
            ]),
        ]

        payload = IndustryPayload(
            sector=sector,
            industry_group=industry,
            peer_group=peer_group,
            relative_growth_rank=growth_rank,
            relative_margin_rank=margin_rank,
            competitive_position=competitive_position,
            theme_exposures=themes,
            read_across_events=read_across_events[:5],
            industry_risks=industry_risks,
        ).model_dump()

        return self._emit(
            ticker=ticker,
            run_id=run_id,
            as_of=as_of,
            score=score,
            confidence=confidence,
            direction=direction,
            materiality=Materiality.HIGH if read_across_events else Materiality.MEDIUM,
            payload=payload,
            evidence=evidence,
            quality_flags=flags,
        )

    @staticmethod
    def _classify_position(growth_rank: float | None, margin_rank: float | None) -> str:
        if growth_rank is None and margin_rank is None:
            return "unknown"
        gr = growth_rank or 0.0
        mr = margin_rank or 0.0
        # Deteriorating: both below median vs previous run would need history; use low scores
        if gr < 0.25 and mr < 0.25:
            return "deteriorating"
        if gr >= 0.75 and mr >= 0.75:
            return "leader"
        if gr >= 0.50:
            return "challenger"
        return "niche"

    @staticmethod
    def _derive_peer_group(info: dict[str, Any], ticker: str) -> list[str]:
        """Auto-derive peers from yfinance recommendations or known sector peers."""
        sector = info.get("sector", "")
        industry = info.get("industry", "")

        # Well-known peer maps for common sectors
        KNOWN_PEERS: dict[str, list[str]] = {
            "Semiconductors": ["NVDA", "AMD", "INTC", "QCOM", "AVGO", "MRVL", "TXN"],
            "Software—Application": ["MSFT", "CRM", "NOW", "WDAY", "ADBE", "ORCL"],
            "Software—Infrastructure": ["MSFT", "AMZN", "GOOGL", "SNOW", "MDB", "DDOG"],
            "Internet Content & Information": ["GOOGL", "META", "AMZN", "PINS", "SNAP"],
            "Biotechnology": ["AMGN", "GILD", "BIIB", "REGN", "VRTX", "MRNA"],
        }
        peers = KNOWN_PEERS.get(industry, KNOWN_PEERS.get(sector, []))
        return [p for p in peers if p != ticker][:6]

    @staticmethod
    def _map_themes(industry: str, sector: str, info: dict[str, Any]) -> list[str]:
        themes: set[str] = set()
        for key in [industry, sector]:
            for pattern, theme_list in THEME_MAP.items():
                if pattern.lower() in key.lower():
                    themes.update(theme_list)
        # Description keyword check
        desc = (info.get("longBusinessSummary") or "").lower()
        if "artificial intelligence" in desc or " ai " in desc:
            themes.add("AI")
        if "cloud" in desc:
            themes.add("cloud")
        if "cyber" in desc or "security" in desc:
            themes.add("cybersecurity")
        if "data center" in desc or "datacenter" in desc:
            themes.add("data_center")
        return sorted(themes)[:6]

    @staticmethod
    def _derive_risks(sector: str, info: dict[str, Any]) -> list[str]:
        risks: list[str] = []
        sector_l = sector.lower()
        if "tech" in sector_l or "semi" in sector_l:
            risks.append("Rapid technological change and product cycle risk")
            risks.append("Export control and geopolitical semiconductor restrictions")
        if "bio" in sector_l or "pharma" in sector_l:
            risks.append("Clinical trial failure and FDA approval uncertainty")
        if "financ" in sector_l:
            risks.append("Interest rate sensitivity and credit cycle exposure")
        customers = info.get("topInstitutionalHolders")
        if info.get("revenueGrowth") is not None and info.get("revenueGrowth", 1) < 0:
            risks.append("Declining revenue trend")
        return risks[:4]
