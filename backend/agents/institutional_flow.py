"""
agent.institutional_flow — Institutional ownership level and composition from 13F data.

Tracks the level and composition of institutional ownership, which reflects
the collective conviction of professional money managers. Healthy institutional
interest (40–70% of float) is a bullish signal; dangerously crowded ownership
(>85%) raises exit-risk; under-ownership (<20%) suggests neglect or avoidance.
Insider ownership alignment and breadth of holders further qualify the signal.

─── Data sources ────────────────────────────────────────────────────────────────
Uses yfinance data derived from quarterly 13F filings (45-day lag):
  institutionsPercentHeld / heldPercentInstitutions — % shares held by all
    reporting institutions
  institutionsCount          — number of distinct institutional holders (breadth)
  insidersPercentHeld / heldPercentInsiders — % held by company insiders
  institutional_holders DataFrame — top holders, shares, % out, date reported

─── Scoring ─────────────────────────────────────────────────────────────────────
Base from institutional ownership %:
  > 85%  → 38  (dangerously crowded — large unwinding risk)
  70–85% → 55  (well-owned, modestly crowded)
  40–70% → 65  (healthy institutional interest)
  20–40% → 55  (moderate; may be smaller-cap or undiscovered)
  < 20%  → 42  (under-owned — low smart-money conviction)

Insider ownership adjustment:
  > 20%  → +8   (strong insider alignment)
  10–20% → +4
  5–10%  → +1
  < 1%   → −5   (minimal insider stake)

Breadth adjustment (institution count):
  > 1000 → +3
  500–1000 → +1
  < 50   → −4

Top-3 holder concentration:
  > 30% of float in top 3 → −3  (single-holder exit risk)

─── Horizon relevance ───────────────────────────────────────────────────────────
13F filings are quarterly with a 45-day lag; relevant across 3–12m.
Weight: 3m 0.06 / 6m 0.07 / 12m 0.06
"""
from __future__ import annotations

import logging
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from agents import BaseAgent
from schemas import Confidence, Direction, Evidence, Materiality, QualityFlag

import yfinance as yf

logger = logging.getLogger(__name__)


class InstitutionalFlowAgent(BaseAgent):
    agent_id = "agent.institutional_flow"
    signal_type = "institutional_flow"
    default_horizons = ["3m", "6m", "12m"]

    def _run(self, ticker: str, run_id: str, as_of: datetime):
        ticker = ticker.upper()
        t = yf.Ticker(ticker)

        info = self._timed_fetch(lambda: t.info, f"{ticker}/info") or {}
        inst_holders = self._timed_fetch(lambda: t.institutional_holders, f"{ticker}/institutional_holders")

        # ── Extract ownership metrics ─────────────────────────────────
        inst_pct: float | None = None
        for key in ("institutionsPercentHeld", "heldPercentInstitutions", "institutionsFloatPercentHeld"):
            val = self._safe_get(info, key)
            if val is not None:
                inst_pct = float(val)
                break

        insider_pct: float | None = None
        for key in ("insidersPercentHeld", "heldPercentInsiders"):
            val = self._safe_get(info, key)
            if val is not None:
                insider_pct = float(val)
                break

        inst_count: int | None = None
        val = self._safe_get(info, "institutionsCount")
        if val is not None:
            try:
                inst_count = int(val)
            except (TypeError, ValueError):
                pass

        # Top-3 holder concentration
        top3_pct: float | None = None
        if inst_holders is not None and not inst_holders.empty:
            try:
                pct_col = next(
                    (c for c in inst_holders.columns if "%" in str(c) or "out" in str(c).lower()),
                    None,
                )
                if pct_col is not None:
                    top3_pct = float(inst_holders[pct_col].head(3).sum())
            except Exception as exc:
                logger.debug("[institutional_flow] %s: top3 parse: %s", ticker, exc)

        if inst_pct is None:
            return self._emit(
                ticker=ticker, run_id=run_id, as_of=as_of,
                score=50.0,
                confidence=Confidence.LOW,
                direction=Direction.NEUTRAL,
                materiality=Materiality.LOW,
                payload={"narrative": "Institutional ownership data unavailable."},
                quality_flags=[QualityFlag.LOW_COVERAGE],
            )

        # ── Base score from institutional ownership % ─────────────────
        notes: list[str] = []
        if inst_pct > 0.85:
            base_score = 38.0
            notes.append(f"Institutional ownership {inst_pct:.1%} — crowded, high unwinding risk")
        elif inst_pct > 0.70:
            base_score = 55.0
            notes.append(f"Institutional ownership {inst_pct:.1%} — well-owned")
        elif inst_pct > 0.40:
            base_score = 65.0
            notes.append(f"Institutional ownership {inst_pct:.1%} — healthy institutional interest")
        elif inst_pct > 0.20:
            base_score = 55.0
            notes.append(f"Institutional ownership {inst_pct:.1%} — moderate")
        else:
            base_score = 42.0
            notes.append(f"Institutional ownership {inst_pct:.1%} — under-owned by institutions")

        adj = 0.0

        # ── Insider ownership adjustment ──────────────────────────────
        if insider_pct is not None:
            if insider_pct > 0.20:
                adj += 8.0
                notes.append(f"Insider ownership {insider_pct:.1%} — strong alignment")
            elif insider_pct > 0.10:
                adj += 4.0
                notes.append(f"Insider ownership {insider_pct:.1%} — good alignment")
            elif insider_pct > 0.05:
                adj += 1.0
                notes.append(f"Insider ownership {insider_pct:.1%}")
            elif insider_pct < 0.01:
                adj -= 5.0
                notes.append(f"Insider ownership {insider_pct:.1%} — minimal skin in the game")

        # ── Breadth adjustment ────────────────────────────────────────
        if inst_count is not None:
            if inst_count > 1000:
                adj += 3.0
                notes.append(f"{inst_count:,} institutional holders — broad ownership base")
            elif inst_count >= 500:
                adj += 1.0
                notes.append(f"{inst_count:,} institutional holders")
            elif inst_count < 50:
                adj -= 4.0
                notes.append(f"Only {inst_count} institutional holders — thin coverage")

        # ── Top-holder concentration risk ─────────────────────────────
        if top3_pct is not None and top3_pct > 0.30:
            adj -= 3.0
            notes.append(f"Top 3 holders control {top3_pct:.1%} — concentration risk")

        score = max(10.0, min(90.0, base_score + adj))

        # ── Direction / materiality / confidence ──────────────────────
        if score >= 62:
            direction = Direction.POSITIVE
        elif score >= 45:
            direction = Direction.NEUTRAL
        else:
            direction = Direction.NEGATIVE

        if abs(adj) >= 8 or inst_pct > 0.85 or inst_pct < 0.20:
            materiality = Materiality.HIGH
        elif abs(adj) >= 4:
            materiality = Materiality.MEDIUM
        else:
            materiality = Materiality.LOW

        if inst_count is not None and insider_pct is not None:
            confidence = Confidence.HIGH
        elif inst_count is not None or insider_pct is not None:
            confidence = Confidence.MEDIUM
        else:
            confidence = Confidence.LOW

        narrative = ". ".join(notes) + "."

        return self._emit(
            ticker=ticker, run_id=run_id, as_of=as_of,
            score=score,
            confidence=confidence,
            direction=direction,
            materiality=materiality,
            payload={
                "institutions_pct":  round(inst_pct, 4),
                "insiders_pct":      round(insider_pct, 4) if insider_pct is not None else None,
                "institution_count": inst_count,
                "top3_holder_pct":   round(top3_pct, 4) if top3_pct is not None else None,
                "base_score":        round(base_score, 2),
                "adjustment":        round(adj, 2),
                "narrative":         narrative,
            },
            evidence=[
                Evidence(
                    source_type="sec_filing",
                    source_name="yfinance (13F-derived institutional holders)",
                    url_or_ref=f"yfinance://institutional_holders/{ticker}",
                    credibility_weight=0.75,
                    extracted_facts=notes,
                )
            ],
            quality_flags=[],
        )
