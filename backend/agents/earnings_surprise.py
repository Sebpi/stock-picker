"""
agent.earnings_surprise — Historical EPS beat rate and estimate revision trend.

Exploits analyst anchoring bias: companies that consistently beat expectations
continue to do so because analysts are slow to raise estimates even when
management guides higher. A strong trailing beat rate combined with upward
near-term estimate revisions is one of the most reliable short-horizon signals.

─── Signals ─────────────────────────────────────────────────────────────────────
1. Historical beat rate (last 4 quarters, yfinance earnings_history):
   % of quarters where reported EPS ≥ analyst consensus estimate.

2. Average surprise magnitude (%):
   Large consistent beats signal structural positive analyst bias.

3. Estimate revision trend (yfinance eps_revisions, current quarter "0q"):
   Net upward revisions in the last 7 days predict beats — analysts slowly
   walk up estimates as management signals outperformance.

─── Scoring ─────────────────────────────────────────────────────────────────────
Base from beat rate (last ≤4 quarters):
  4/4 beats → 72   (perfect record)
  3/4       → 63
  2/4       → 52   (coin flip)
  1/4       → 40
  0/4       → 30   (serial misser)

Surprise magnitude adjustment:
  Avg surprise > +5%  → +6
  Avg surprise +2–5%  → +3
  Avg surprise −2–+2% →  0  (normal)
  Avg surprise < −2%  → −5
  Avg surprise < −5%  → −9

Revision adjustment (upRevisions7d − downRevisions7d for current quarter):
  Net > +2  → +5  (analysts raising bar ahead of print)
  Net +1–2  → +2
  Net 0     →  0
  Net −1–−2 → −3
  Net < −2  → −6  (cuts ahead of print — negative pre-announcement signal)

─── Horizon relevance ───────────────────────────────────────────────────────────
Earnings surprises are most actionable 1–3 months around the print.
Signal decays beyond 6m where fundamental trends dominate.
Weight: 3m 0.08 / 6m 0.07 / 12m 0.04
"""
from __future__ import annotations

import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).parent.parent))
from agents import BaseAgent
from schemas import Confidence, Direction, Evidence, Materiality, QualityFlag

import db
import yfinance as yf

logger = logging.getLogger(__name__)


def _safe_float(val: Any) -> float | None:
    try:
        f = float(val)
        return f if f == f else None  # NaN check
    except (TypeError, ValueError):
        return None


def _safe_int(val: Any) -> int | None:
    f = _safe_float(val)
    return int(f) if f is not None else None


def _get_col(row: Any, *names: str) -> Any:
    for name in names:
        try:
            v = row[name] if hasattr(row, "__getitem__") else getattr(row, name, None)
            if v is not None:
                return v
        except (KeyError, AttributeError):
            pass
    return None


class EarningsSurpriseAgent(BaseAgent):
    agent_id = "agent.earnings_surprise"
    signal_type = "earnings_surprise"
    default_horizons = ["3m", "6m", "12m"]

    def _run(self, ticker: str, run_id: str, as_of: datetime):
        ticker = ticker.upper()

        # Look up upcoming earnings date from the earnings_events DB cache.
        # If earnings is within 5 days, signal is high materiality (pre-print tension).
        # If earnings just passed (0–3 days ago), confidence is temporarily lower (surprise not priced in yet).
        days_to_earnings: int | None = None
        try:
            upcoming = db.get_recent_earnings_events(ticker, days=45)
            if upcoming:
                next_event = upcoming[0]
                report_date_str = next_event.get("report_date")
                if report_date_str:
                    from datetime import date
                    rd = date.fromisoformat(str(report_date_str)[:10])
                    days_to_earnings = (rd - as_of.date()).days
        except Exception as exc:
            logger.debug("[earnings_surprise] %s: earnings date lookup failed: %s", ticker, exc)

        t = yf.Ticker(ticker)

        earnings_hist = self._timed_fetch(lambda: t.earnings_history, f"{ticker}/earnings_history")
        eps_revisions  = self._timed_fetch(lambda: t.eps_revisions,   f"{ticker}/eps_revisions")

        # ── Historical beat rate (last 4 reported quarters) ──────────
        beats: list[bool] = []
        surprises_pct: list[float] = []

        if earnings_hist is not None and not earnings_hist.empty:
            try:
                df = earnings_hist.sort_index(ascending=False).head(4)
                for _, row in df.iterrows():
                    est   = _safe_float(_get_col(row, "epsEstimate", "EPS Estimate"))
                    act   = _safe_float(_get_col(row, "epsActual",   "EPS Actual"))
                    surp  = _safe_float(_get_col(row, "surprisePercent", "Surprise(%)"))
                    if est is not None and act is not None:
                        beats.append(act >= est)
                    if surp is not None:
                        surprises_pct.append(surp)
            except Exception as exc:
                logger.debug("[earnings_surprise] %s: history parse: %s", ticker, exc)

        # ── Estimate revisions (current quarter "0q") ─────────────────
        net_revisions_7d: int | None = None
        if eps_revisions is not None and not eps_revisions.empty:
            try:
                row = None
                if "0q" in eps_revisions.index:
                    row = eps_revisions.loc["0q"]
                elif not eps_revisions.empty:
                    row = eps_revisions.iloc[0]
                if row is not None:
                    up7   = _safe_int(_get_col(row, "upLast7days",   "Up Last 7 Days"))
                    down7 = _safe_int(_get_col(row, "downLast7days", "Down Last 7 Days"))
                    if up7 is not None and down7 is not None:
                        net_revisions_7d = up7 - down7
            except Exception as exc:
                logger.debug("[earnings_surprise] %s: revisions parse: %s", ticker, exc)

        if not beats and net_revisions_7d is None:
            return self._emit(
                ticker=ticker, run_id=run_id, as_of=as_of,
                score=50.0,
                confidence=Confidence.LOW,
                direction=Direction.NEUTRAL,
                materiality=Materiality.LOW,
                payload={"narrative": "Earnings history and revision data unavailable."},
                quality_flags=[QualityFlag.LOW_COVERAGE],
            )

        notes: list[str] = []

        # ── Base score from beat rate ─────────────────────────────────
        beat_count = sum(beats)
        n_quarters = len(beats)
        beat_rate: float | None = None

        if n_quarters >= 1:
            beat_rate = beat_count / n_quarters
            if beat_count == 4:
                base_score = 72.0
                notes.append(f"Beat EPS estimate {beat_count}/{n_quarters} quarters — strong track record")
            elif beat_count == 3:
                base_score = 63.0
                notes.append(f"Beat EPS estimate {beat_count}/{n_quarters} quarters")
            elif beat_count == 2:
                base_score = 52.0
                notes.append(f"Beat EPS estimate {beat_count}/{n_quarters} quarters — mixed")
            elif beat_count == 1:
                base_score = 40.0
                notes.append(f"Beat EPS estimate {beat_count}/{n_quarters} quarters — weak")
            else:
                base_score = 30.0
                notes.append(f"Missed EPS estimate all {n_quarters} quarters — serial misser")
        else:
            base_score = 52.0
            beat_count = 0
            notes.append("Beat rate unavailable")

        # ── Surprise magnitude adjustment ─────────────────────────────
        mag_adj = 0.0
        avg_surprise: float | None = None
        if surprises_pct:
            avg_surprise = sum(surprises_pct) / len(surprises_pct)
            if avg_surprise > 5.0:
                mag_adj = 6.0
                notes.append(f"Avg EPS surprise +{avg_surprise:.1f}% — large systematic beats")
            elif avg_surprise > 2.0:
                mag_adj = 3.0
                notes.append(f"Avg EPS surprise +{avg_surprise:.1f}%")
            elif avg_surprise < -5.0:
                mag_adj = -9.0
                notes.append(f"Avg EPS surprise {avg_surprise:.1f}% — serial disappointments")
            elif avg_surprise < -2.0:
                mag_adj = -5.0
                notes.append(f"Avg EPS surprise {avg_surprise:.1f}% — tendency to miss")

        # ── Revision trend adjustment ─────────────────────────────────
        rev_adj = 0.0
        if net_revisions_7d is not None:
            if net_revisions_7d > 2:
                rev_adj = 5.0
                notes.append(f"Net +{net_revisions_7d} estimate upgrades (7d) — analysts raising bar")
            elif net_revisions_7d > 0:
                rev_adj = 2.0
                notes.append(f"Net +{net_revisions_7d} estimate upgrades (7d)")
            elif net_revisions_7d < -2:
                rev_adj = -6.0
                notes.append(f"Net {net_revisions_7d} estimate cuts (7d) — reducing expectations")
            elif net_revisions_7d < 0:
                rev_adj = -3.0
                notes.append(f"Net {net_revisions_7d} estimate cuts (7d)")

        score = max(10.0, min(90.0, base_score + mag_adj + rev_adj))

        # ── Direction / materiality / confidence ──────────────────────
        if score >= 62:
            direction = Direction.POSITIVE
        elif score >= 45:
            direction = Direction.NEUTRAL
        else:
            direction = Direction.NEGATIVE

        total_adj = abs(mag_adj) + abs(rev_adj)
        if total_adj >= 10 or (n_quarters >= 3 and beat_count == 0):
            materiality = Materiality.HIGH
        elif total_adj >= 5 or n_quarters >= 3:
            materiality = Materiality.MEDIUM
        else:
            materiality = Materiality.LOW

        if n_quarters >= 3 and net_revisions_7d is not None:
            confidence = Confidence.HIGH
        elif n_quarters >= 1 or net_revisions_7d is not None:
            confidence = Confidence.MEDIUM
        else:
            confidence = Confidence.LOW

        # Adjust for proximity to earnings date.
        earnings_proximity_note: str | None = None
        if days_to_earnings is not None:
            if 0 < days_to_earnings <= 5:
                # Pre-print window: materiality is higher, signal is most actionable
                if materiality != Materiality.HIGH:
                    materiality = Materiality.HIGH
                earnings_proximity_note = f"Earnings in {days_to_earnings}d — signal at peak relevance"
                notes.append(earnings_proximity_note)
            elif -3 <= days_to_earnings <= 0:
                # Just reported: surprise not yet reflected; confidence temporarily lower
                confidence = Confidence.LOW
                earnings_proximity_note = f"Earnings just reported ({abs(days_to_earnings)}d ago) — surprise impact still settling"
                notes.append(earnings_proximity_note)
            elif 6 <= days_to_earnings <= 21:
                earnings_proximity_note = f"Earnings in {days_to_earnings}d"
                notes.append(earnings_proximity_note)

        flags: list[QualityFlag] = []
        if n_quarters < 2:
            flags.append(QualityFlag.LOW_COVERAGE)

        narrative = ". ".join(notes) + "."

        return self._emit(
            ticker=ticker, run_id=run_id, as_of=as_of,
            score=score,
            confidence=confidence,
            direction=direction,
            materiality=materiality,
            payload={
                "quarters_analyzed":  n_quarters,
                "beat_count":         beat_count,
                "beat_rate":          round(beat_rate, 3) if beat_rate is not None else None,
                "avg_surprise_pct":   round(avg_surprise, 2) if avg_surprise is not None else None,
                "net_revisions_7d":   net_revisions_7d,
                "base_score":         round(base_score, 2),
                "magnitude_adj":      round(mag_adj, 2),
                "revision_adj":       round(rev_adj, 2),
                "days_to_earnings":   days_to_earnings,
                "narrative":          narrative,
            },
            evidence=[
                Evidence(
                    source_type="earnings_release",
                    source_name="yfinance (earnings history, EPS revisions)",
                    url_or_ref=f"yfinance://earnings/{ticker}",
                    credibility_weight=0.80,
                    extracted_facts=notes,
                )
            ],
            quality_flags=flags,
        )
