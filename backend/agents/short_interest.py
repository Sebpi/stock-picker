"""
agent.short_interest — Short interest positioning and squeeze risk.

Reads four yfinance fields from ticker.info:
  shortPercentOfFloat   — % of float currently sold short
  shortRatio            — days-to-cover (shares short / avg daily vol)
  sharesShort           — current shares short (latest FINRA settlement)
  sharesShortPriorMonth — shares short in prior reporting period (~4 weeks ago)

Three composite signals:

  1. SHORT % LEVEL — absolute short interest as % of float
     < 2%   → almost no negative sentiment (bullish baseline)
     2–5%   → normal range, slight caution
     5–10%  → elevated; meaningful bearish positioning
     10–15% → high conviction short; either well-shorted or squeeze risk
     > 15%  → heavily shorted; strong negative signal OR squeeze setup

  2. MONTH-ON-MONTH CHANGE — direction of short interest
     Falling sharply (−20%+) → shorts covering / capitulating → bullish
     Falling moderately      → covering in progress → mildly bullish
     Flat                    → no conviction shift
     Rising moderately       → new shorts being established → mildly bearish
     Rising sharply (+20%+)  → new high-conviction short → bearish

  3. SQUEEZE RISK BONUS — when high days-to-cover coincides with
     falling short interest the forced-covering dynamic amplifies
     upside (high short % + > 8 days-to-cover + MoM falling).

Scoring ladder:
  70–75  Very low short % (<2%) OR heavy covering of high short base
  60–65  Low-moderate short, covering or flat
  50–55  Neutral — moderate short % + flat change
  40–45  Elevated short % OR rising shorts
  30–38  High short % + flat or rising
  22–29  Very heavy shorting (>15%) with short % still rising

Direction:
  POSITIVE  if score ≥ 60  (low short or strong covering)
  NEUTRAL   if score 45–59
  NEGATIVE  if score < 45

Confidence:
  HIGH    — both current and prior-month data present, float > 10 M shares
  MEDIUM  — current data only (no prior-month for MoM delta)
  LOW     — only partial data or very small float

Coverage:
  Missing shortPercentOfFloat AND shortRatio → LOW_COVERAGE neutral 50.
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

# Squeeze threshold: days-to-cover above this + high short % = squeeze risk
SQUEEZE_DTC_THRESHOLD = 8.0
SQUEEZE_SHORT_PCT_MIN = 10.0


class ShortInterestAgent(BaseAgent):
    agent_id = "agent.short_interest"
    signal_type = "short_interest"
    default_horizons = ["3m", "6m", "12m"]

    def _run(self, ticker: str, run_id: str, as_of: datetime):
        ticker = ticker.upper()

        info = self._timed_fetch(
            lambda: yf.Ticker(ticker).info,
            f"{ticker}/info_short",
        ) or {}

        short_pct_raw   = self._safe_get(info, "shortPercentOfFloat")
        days_to_cover   = self._safe_get(info, "shortRatio")
        shares_short    = self._safe_get(info, "sharesShort")
        shares_short_pm = self._safe_get(info, "sharesShortPriorMonth")
        float_shares    = self._safe_get(info, "floatShares")

        # No usable data at all.
        if short_pct_raw is None and days_to_cover is None:
            return self._emit(
                ticker=ticker, run_id=run_id, as_of=as_of,
                score=50.0,
                confidence=Confidence.LOW,
                direction=Direction.NEUTRAL,
                materiality=Materiality.LOW,
                payload={"narrative": "Short interest data unavailable."},
                quality_flags=[QualityFlag.LOW_COVERAGE],
            )

        # yfinance returns shortPercentOfFloat as a fraction (e.g. 0.023 = 2.3%).
        short_pct = float(short_pct_raw) * 100 if short_pct_raw is not None else None
        dtc       = float(days_to_cover) if days_to_cover is not None else None

        # Month-on-month change in shares short.
        mom_change_pct: float | None = None
        if shares_short and shares_short_pm and shares_short_pm > 0:
            mom_change_pct = (shares_short - shares_short_pm) / shares_short_pm * 100

        # ── Base score from short % of float ─────────────────────────
        if short_pct is None:
            base_score = 50.0
        elif short_pct < 2.0:
            base_score = 66.0
        elif short_pct < 5.0:
            base_score = 56.0
        elif short_pct < 10.0:
            base_score = 46.0
        elif short_pct < 15.0:
            base_score = 38.0
        elif short_pct < 20.0:
            base_score = 33.0
        else:
            base_score = 27.0

        # ── MoM adjustment ───────────────────────────────────────────
        adj = 0.0
        mom_label = "no prior-month data"
        if mom_change_pct is not None:
            if mom_change_pct <= -20:
                adj, mom_label = +14.0, f"large covering −{abs(mom_change_pct):.1f}%"
            elif mom_change_pct <= -10:
                adj, mom_label = +8.0, f"moderate covering −{abs(mom_change_pct):.1f}%"
            elif mom_change_pct <= -5:
                adj, mom_label = +4.0, f"slight covering −{abs(mom_change_pct):.1f}%"
            elif mom_change_pct < 5:
                adj, mom_label = 0.0, "flat MoM"
            elif mom_change_pct < 10:
                adj, mom_label = -4.0, f"rising shorts +{mom_change_pct:.1f}%"
            elif mom_change_pct < 20:
                adj, mom_label = -8.0, f"rising shorts +{mom_change_pct:.1f}%"
            else:
                adj, mom_label = -14.0, f"large short buildup +{mom_change_pct:.1f}%"

        score = base_score + adj

        # ── Squeeze risk bonus ────────────────────────────────────────
        squeeze_flag = False
        if (short_pct is not None and short_pct >= SQUEEZE_SHORT_PCT_MIN
                and dtc is not None and dtc >= SQUEEZE_DTC_THRESHOLD
                and mom_change_pct is not None and mom_change_pct < 0):
            score = min(score + 10.0, 78.0)
            squeeze_flag = True

        score = max(10.0, min(90.0, score))

        # ── Direction ────────────────────────────────────────────────
        if score >= 60:
            direction = Direction.POSITIVE
        elif score >= 45:
            direction = Direction.NEUTRAL
        else:
            direction = Direction.NEGATIVE

        # ── Materiality ──────────────────────────────────────────────
        if short_pct is not None and (short_pct >= 10 or (mom_change_pct is not None and abs(mom_change_pct) >= 15)):
            materiality = Materiality.HIGH
        elif short_pct is not None and (short_pct >= 5 or (mom_change_pct is not None and abs(mom_change_pct) >= 8)):
            materiality = Materiality.MEDIUM
        else:
            materiality = Materiality.LOW

        # ── Confidence ───────────────────────────────────────────────
        float_large = float_shares is not None and float_shares > 10_000_000
        if mom_change_pct is not None and float_large:
            confidence = Confidence.HIGH
        elif short_pct is not None:
            confidence = Confidence.MEDIUM
        else:
            confidence = Confidence.LOW

        # ── Narrative ────────────────────────────────────────────────
        parts = []
        if short_pct is not None:
            parts.append(f"Short % of float: {short_pct:.1f}%")
        if dtc is not None:
            parts.append(f"Days-to-cover: {dtc:.1f}")
        parts.append(mom_label.capitalize())
        if squeeze_flag:
            parts.append("Squeeze risk elevated — high short base with active covering.")
        narrative = ". ".join(parts) + "."

        flags: list[QualityFlag] = []
        if mom_change_pct is None:
            flags.append(QualityFlag.MISSING_FIELD)

        evidence = [
            Evidence(
                source_type="market_data",
                source_name="yfinance (FINRA short interest via Yahoo Finance)",
                url_or_ref=f"yfinance://shortinterest/{ticker}",
                credibility_weight=0.60,
                extracted_facts=[p for p in parts],
            )
        ]

        return self._emit(
            ticker=ticker, run_id=run_id, as_of=as_of,
            score=score,
            confidence=confidence,
            direction=direction,
            materiality=materiality,
            payload={
                "short_pct_float":      round(short_pct, 2) if short_pct is not None else None,
                "days_to_cover":        round(dtc, 1) if dtc is not None else None,
                "shares_short":         shares_short,
                "shares_short_prior":   shares_short_pm,
                "mom_change_pct":       round(mom_change_pct, 1) if mom_change_pct is not None else None,
                "float_shares":         float_shares,
                "squeeze_risk":         squeeze_flag,
                "narrative":            narrative,
            },
            evidence=evidence,
            quality_flags=flags,
        )
