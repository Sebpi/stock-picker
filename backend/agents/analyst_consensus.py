"""
agent.analyst_consensus — Analyst recommendation distribution and price target upside.

The consensus of professional sell-side analysts reflects collective fundamental
views. While analysts lag inflection points, the aggregate rating and the gap
between target price and current price provides a calibrated directional signal —
especially when combined with revision direction (covered by earnings_surprise).

─── Signals ─────────────────────────────────────────────────────────────────────
1. Consensus recommendation key:
   strong_buy → buy → hold → sell → strong_sell
   (averaged from all covering analysts' recommendations)

2. Target price upside (%) = (mean target − current price) / current price
   Positive upside = analysts collectively see room to run.

3. Target price conviction: spread between high and low targets (tight = high
   conviction; wide = analysts disagree).

4. Analyst count: more analysts = more reliable consensus signal.

─── Scoring ─────────────────────────────────────────────────────────────────────
Base from recommendation key:
  strong_buy  → 75
  buy         → 63
  hold        → 50
  sell        → 37
  strong_sell → 24
  unknown     → 52

Target upside adjustment:
  > +30%  → +8
  +15–30% → +5
  +5–15%  → +2
  −5–+5%  →  0  (priced in)
  −5–−15% → -4
  < −15%  → -8  (consensus sees downside)

Conviction adjustment (target spread / current price):
  Spread < 20%   → +2  (tight consensus — high conviction)
  Spread > 60%   → -2  (widely dispersed views)

Analyst count:
  ≥ 20 analysts  → +2  (broad, reliable consensus)
  ≤ 3 analysts   → -3  (thin coverage — noisier signal)

─── Horizon relevance ───────────────────────────────────────────────────────────
Analyst targets are typically set on 12-month horizon; most actionable at 3–6m.
Weight: 3m 0.07 / 6m 0.06 / 12m 0.04
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

_REC_SCORES: dict[str, float] = {
    "strong_buy":  75.0,
    "buy":         63.0,
    "hold":        50.0,
    "underperform": 40.0,
    "sell":        37.0,
    "strong_sell": 24.0,
}


class AnalystConsensusAgent(BaseAgent):
    agent_id = "agent.analyst_consensus"
    signal_type = "analyst_consensus"
    default_horizons = ["3m", "6m", "12m"]

    def _run(self, ticker: str, run_id: str, as_of: datetime):
        ticker = ticker.upper()
        t = yf.Ticker(ticker)

        info = self._timed_fetch(lambda: t.info, f"{ticker}/info") or {}

        rec_key     = self._safe_get(info, "recommendationKey")
        n_analysts  = self._safe_get(info, "numberOfAnalystOpinions")
        target_mean = self._safe_get(info, "targetMeanPrice")
        target_high = self._safe_get(info, "targetHighPrice")
        target_low  = self._safe_get(info, "targetLowPrice")
        current_px  = (self._safe_get(info, "currentPrice")
                       or self._safe_get(info, "regularMarketPrice"))

        if rec_key is None and target_mean is None:
            return self._emit(
                ticker=ticker, run_id=run_id, as_of=as_of,
                score=50.0,
                confidence=Confidence.LOW,
                direction=Direction.NEUTRAL,
                materiality=Materiality.LOW,
                payload={"narrative": "Analyst consensus data unavailable."},
                quality_flags=[QualityFlag.LOW_COVERAGE],
            )

        notes: list[str] = []

        # ── Base from recommendation ──────────────────────────────────
        rec_lower = (rec_key or "").lower().replace(" ", "_").replace("-", "_")
        base_score = _REC_SCORES.get(rec_lower, 52.0)
        if rec_key:
            notes.append(f"Analyst consensus: {rec_key.replace('_', ' ').title()}")
            if n_analysts is not None:
                notes[-1] += f" ({int(n_analysts)} analysts)"
        else:
            notes.append("Analyst recommendation unavailable")

        adj = 0.0

        # ── Target upside ─────────────────────────────────────────────
        upside: float | None = None
        if target_mean is not None and current_px is not None and current_px > 0:
            upside = (float(target_mean) - float(current_px)) / float(current_px)
            if upside > 0.30:
                adj += 8.0
                notes.append(f"Target price ${float(target_mean):.2f} — {upside:+.1%} upside")
            elif upside > 0.15:
                adj += 5.0
                notes.append(f"Target price ${float(target_mean):.2f} — {upside:+.1%} upside")
            elif upside > 0.05:
                adj += 2.0
                notes.append(f"Target price ${float(target_mean):.2f} — {upside:+.1%} upside")
            elif upside > -0.05:
                notes.append(f"Target price ${float(target_mean):.2f} — roughly priced in ({upside:+.1%})")
            elif upside > -0.15:
                adj -= 4.0
                notes.append(f"Target price ${float(target_mean):.2f} — {upside:+.1%} (consensus sees downside)")
            else:
                adj -= 8.0
                notes.append(f"Target price ${float(target_mean):.2f} — {upside:+.1%} (significant downside)")

        # ── Target spread (conviction) ────────────────────────────────
        if (target_high is not None and target_low is not None
                and current_px is not None and current_px > 0):
            spread_pct = (float(target_high) - float(target_low)) / float(current_px)
            if spread_pct < 0.20:
                adj += 2.0
                notes.append(f"Tight target range ${float(target_low):.0f}–${float(target_high):.0f} — high conviction")
            elif spread_pct > 0.60:
                adj -= 2.0
                notes.append(f"Wide target range ${float(target_low):.0f}–${float(target_high):.0f} — dispersed views")

        # ── Analyst count ─────────────────────────────────────────────
        if n_analysts is not None:
            n = int(n_analysts)
            if n >= 20:
                adj += 2.0
            elif n <= 3:
                adj -= 3.0
                notes.append(f"Only {n} analysts covering — limited consensus reliability")

        score = max(10.0, min(90.0, base_score + adj))

        if score >= 62:
            direction = Direction.POSITIVE
        elif score >= 45:
            direction = Direction.NEUTRAL
        else:
            direction = Direction.NEGATIVE

        if upside is not None and abs(upside) > 0.20:
            materiality = Materiality.HIGH
        elif upside is not None and abs(upside) > 0.10:
            materiality = Materiality.MEDIUM
        else:
            materiality = Materiality.LOW

        has_target  = upside is not None
        has_count   = n_analysts is not None
        if rec_key and has_target and has_count:
            confidence = Confidence.HIGH
        elif rec_key and (has_target or has_count):
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
                "recommendation":   rec_key,
                "n_analysts":       int(n_analysts) if n_analysts is not None else None,
                "target_mean":      round(float(target_mean), 2) if target_mean is not None else None,
                "target_high":      round(float(target_high), 2) if target_high is not None else None,
                "target_low":       round(float(target_low), 2) if target_low is not None else None,
                "current_price":    round(float(current_px), 2) if current_px is not None else None,
                "target_upside_pct": round(upside, 4) if upside is not None else None,
                "base_score":       round(base_score, 2),
                "adjustment":       round(adj, 2),
                "narrative":        narrative,
            },
            evidence=[
                Evidence(
                    source_type="analyst_revision",
                    source_name="yfinance (analyst consensus)",
                    url_or_ref=f"yfinance://info/{ticker}",
                    credibility_weight=0.75,
                    extracted_facts=notes,
                )
            ],
            quality_flags=[],
        )
