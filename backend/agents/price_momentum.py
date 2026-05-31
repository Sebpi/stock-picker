"""
agent.price_momentum — Cross-sectional price momentum (12-1 month factor).

The momentum effect (Jegadeesh & Titman 1993) is one of the most robust
documented anomalies in equity markets: stocks that have outperformed over
the prior 6–12 months tend to continue outperforming over the next 3–6 months.
The final month is skipped to avoid short-term reversal (bid-ask bounce).

─── Signals ─────────────────────────────────────────────────────────────────────
1. 12-1 month absolute momentum — total return from 12 months ago to 1 month ago
2. 3-month absolute momentum   — recent trend confirmation
3. Relative momentum vs SPY    — stock momentum minus market momentum
   (removes macro tail-wind/head-wind; isolates stock-specific momentum)
4. 1-month return (most recent) — short-term reversal check

─── Scoring ─────────────────────────────────────────────────────────────────────
Base from 12-1 month momentum:
  > +30%   → 78  (strong momentum — top decile historically)
  +15–30%  → 68
  +5–15%   → 60
  −5–+5%   → 50  (no signal)
  −5–−15%  → 40
  −15–−30% → 32
  < −30%   → 22  (deep momentum hole)

Relative vs SPY adjustment (12-1 month):
  Outperforming SPY by > +15%  → +6
  Outperforming SPY by +5–15%  → +3
  Underperforming SPY by −5–15% → −3
  Underperforming SPY by > −15% → −6

3-month confirmation adjustment:
  3m > +5%  → +3  (recent strength confirms 12-1 signal)
  3m < −5%  → −3  (recent weakness — momentum may be fading)

Short-term reversal check (1m):
  1m > +10% → −2  (overbought — reversion risk)
  1m < −10% → +2  (oversold bounce potential, but negative overall)

─── Horizon relevance ───────────────────────────────────────────────────────────
Momentum is strongest at 3–6 months; decays and can reverse at 12m.
Weight: 3m 0.09 / 6m 0.06 / 12m 0.02
"""
from __future__ import annotations

import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from agents import BaseAgent
from schemas import Confidence, Direction, Evidence, Materiality, QualityFlag

import yfinance as yf

logger = logging.getLogger(__name__)

_SPY = "SPY"


def _period_return(ticker: str, start: datetime, end: datetime) -> float | None:
    try:
        hist = yf.download(
            ticker,
            start=start.strftime("%Y-%m-%d"),
            end=end.strftime("%Y-%m-%d"),
            progress=False,
            auto_adjust=True,
        )
        if hist is None or hist.empty or len(hist) < 5:
            return None
        close = hist["Close"].dropna()
        if len(close) < 5:
            return None
        return float((close.iloc[-1] - close.iloc[0]) / close.iloc[0])
    except Exception as exc:
        logger.debug("_period_return %s: %s", ticker, exc)
        return None


class PriceMomentumAgent(BaseAgent):
    agent_id = "agent.price_momentum"
    signal_type = "price_momentum"
    default_horizons = ["3m", "6m", "12m"]

    def _run(self, ticker: str, run_id: str, as_of: datetime):
        ticker = ticker.upper()

        now = as_of.replace(tzinfo=None) if as_of.tzinfo else as_of
        one_month_ago  = now - timedelta(days=30)
        three_month_ago = now - timedelta(days=91)
        twelve_month_ago = now - timedelta(days=365)

        # 12-1 month momentum (core signal)
        mom_12_1 = self._timed_fetch(
            lambda: _period_return(ticker, twelve_month_ago, one_month_ago),
            f"{ticker}/mom_12_1",
        )
        # 3-month momentum (recent trend)
        mom_3m = self._timed_fetch(
            lambda: _period_return(ticker, three_month_ago, now),
            f"{ticker}/mom_3m",
        )
        # 1-month (reversal check)
        mom_1m = self._timed_fetch(
            lambda: _period_return(ticker, one_month_ago, now),
            f"{ticker}/mom_1m",
        )
        # SPY 12-1 for relative momentum
        spy_12_1 = self._timed_fetch(
            lambda: _period_return(_SPY, twelve_month_ago, one_month_ago),
            "SPY/mom_12_1",
        )

        if mom_12_1 is None and mom_3m is None:
            return self._emit(
                ticker=ticker, run_id=run_id, as_of=as_of,
                score=50.0,
                confidence=Confidence.LOW,
                direction=Direction.NEUTRAL,
                materiality=Materiality.LOW,
                payload={"narrative": "Price history unavailable."},
                quality_flags=[QualityFlag.LOW_COVERAGE],
            )

        notes: list[str] = []

        # ── Base score from 12-1 month momentum ──────────────────────
        if mom_12_1 is not None:
            if mom_12_1 > 0.30:
                base_score = 78.0
                notes.append(f"12-1m momentum +{mom_12_1:.1%} — strong (top decile)")
            elif mom_12_1 > 0.15:
                base_score = 68.0
                notes.append(f"12-1m momentum +{mom_12_1:.1%} — solid")
            elif mom_12_1 > 0.05:
                base_score = 60.0
                notes.append(f"12-1m momentum +{mom_12_1:.1%} — mild positive")
            elif mom_12_1 > -0.05:
                base_score = 50.0
                notes.append(f"12-1m momentum {mom_12_1:.1%} — neutral")
            elif mom_12_1 > -0.15:
                base_score = 40.0
                notes.append(f"12-1m momentum {mom_12_1:.1%} — mild negative")
            elif mom_12_1 > -0.30:
                base_score = 32.0
                notes.append(f"12-1m momentum {mom_12_1:.1%} — weak")
            else:
                base_score = 22.0
                notes.append(f"12-1m momentum {mom_12_1:.1%} — deep momentum hole")
        else:
            base_score = 50.0
            mom_12_1 = None

        adj = 0.0

        # ── Relative vs SPY ───────────────────────────────────────────
        rel_mom: float | None = None
        if mom_12_1 is not None and spy_12_1 is not None:
            rel_mom = mom_12_1 - spy_12_1
            if rel_mom > 0.15:
                adj += 6.0
                notes.append(f"Outperforming SPY by +{rel_mom:.1%} (12-1m)")
            elif rel_mom > 0.05:
                adj += 3.0
                notes.append(f"Outperforming SPY by +{rel_mom:.1%} (12-1m)")
            elif rel_mom < -0.15:
                adj -= 6.0
                notes.append(f"Underperforming SPY by {rel_mom:.1%} (12-1m)")
            elif rel_mom < -0.05:
                adj -= 3.0
                notes.append(f"Underperforming SPY by {rel_mom:.1%} (12-1m)")

        # ── 3-month confirmation ──────────────────────────────────────
        if mom_3m is not None:
            if mom_3m > 0.05:
                adj += 3.0
                notes.append(f"3m momentum +{mom_3m:.1%} — confirms trend")
            elif mom_3m < -0.05:
                adj -= 3.0
                notes.append(f"3m momentum {mom_3m:.1%} — trend weakening")

        # ── Short-term reversal check ─────────────────────────────────
        if mom_1m is not None:
            if mom_1m > 0.10:
                adj -= 2.0
                notes.append(f"1m return +{mom_1m:.1%} — overbought near-term")
            elif mom_1m < -0.10:
                adj += 2.0
                notes.append(f"1m return {mom_1m:.1%} — near-term oversold")

        score = max(10.0, min(90.0, base_score + adj))

        # ── Direction / materiality / confidence ──────────────────────
        if score >= 62:
            direction = Direction.POSITIVE
        elif score >= 45:
            direction = Direction.NEUTRAL
        else:
            direction = Direction.NEGATIVE

        if mom_12_1 is not None and abs(mom_12_1) > 0.20:
            materiality = Materiality.HIGH
        elif mom_12_1 is not None and abs(mom_12_1) > 0.10:
            materiality = Materiality.MEDIUM
        else:
            materiality = Materiality.LOW

        has_rel = rel_mom is not None
        has_3m  = mom_3m is not None
        if has_rel and has_3m:
            confidence = Confidence.HIGH
        elif mom_12_1 is not None:
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
                "mom_12_1_pct":   round(mom_12_1, 4) if mom_12_1 is not None else None,
                "mom_3m_pct":     round(mom_3m, 4)   if mom_3m   is not None else None,
                "mom_1m_pct":     round(mom_1m, 4)   if mom_1m   is not None else None,
                "spy_12_1_pct":   round(spy_12_1, 4) if spy_12_1 is not None else None,
                "relative_mom":   round(rel_mom, 4)  if rel_mom  is not None else None,
                "base_score":     round(base_score, 2),
                "adjustment":     round(adj, 2),
                "narrative":      narrative,
            },
            evidence=[
                Evidence(
                    source_type="market_data",
                    source_name="yfinance (price history)",
                    url_or_ref=f"yfinance://{ticker}",
                    credibility_weight=0.70,
                    extracted_facts=notes,
                )
            ],
            quality_flags=[],
        )
