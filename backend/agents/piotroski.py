"""
agent.piotroski — Piotroski F-Score quality composite (9 binary tests).

The Piotroski F-Score (Piotroski 2000) is a 0–9 composite of nine binary
accounting-based signals across three dimensions: profitability, financial
leverage/liquidity, and operating efficiency. It was specifically designed
to separate winners from losers within the universe of high book-to-market
(value) stocks, but works as a general quality signal.

─── Nine binary signals (each contributes 0 or 1) ──────────────────────────────
Profitability (F_ROA, F_ΔROA, F_CFO, F_ACCRUAL):
  F1 ROA > 0              — profitable this year (NI / avg assets)
  F2 ΔROA > 0             — profitability improving YoY
  F3 CFO > 0              — positive operating cash flow
  F4 CFO > NI (accruals)  — cash earnings exceed reported earnings (quality)

Leverage / Liquidity / Source of Funds (F_ΔLEV, F_ΔLIQ, F_EQ):
  F5 Leverage decreasing  — LTD/TA ratio fell YoY
  F6 Current ratio up     — short-term liquidity improved
  F7 No share dilution    — shares outstanding didn't increase

Operating Efficiency (F_ΔMARGIN, F_ΔTURN):
  F8 Gross margin up      — pricing power / cost management improving
  F9 Asset turnover up    — operational efficiency improving

─── Scoring ─────────────────────────────────────────────────────────────────────
F-Score base:
  8–9 → 75  (very high quality — Piotroski "strong")
  6–7 → 63
  4–5 → 52  (average)
  2–3 → 40
  0–1 → 28  (distressed quality — Piotroski "weak")

Partial score applied if fewer than 7 of 9 signals are computable.

─── Horizon relevance ───────────────────────────────────────────────────────────
Accounting quality signals are annual and play out over 6–18 months.
Weight: 3m 0.03 / 6m 0.06 / 12m 0.08
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

import yfinance as yf

logger = logging.getLogger(__name__)


def _row(df: Any, *names: str) -> float | None:
    for name in names:
        if name in df.index:
            val = df.loc[name, df.columns[0]]
            if val is not None and val == val:
                try:
                    return float(val)
                except (TypeError, ValueError):
                    pass
    return None


def _row2(df: Any, *names: str) -> float | None:
    if df.shape[1] < 2:
        return None
    for name in names:
        if name in df.index:
            val = df.loc[name, df.columns[1]]
            if val is not None and val == val:
                try:
                    return float(val)
                except (TypeError, ValueError):
                    pass
    return None


def _b(condition: bool | None) -> int | None:
    if condition is None:
        return None
    return 1 if condition else 0


class PiotroskiAgent(BaseAgent):
    agent_id = "agent.piotroski"
    signal_type = "piotroski"
    default_horizons = ["3m", "6m", "12m"]

    def _run(self, ticker: str, run_id: str, as_of: datetime):
        ticker = ticker.upper()
        t = yf.Ticker(ticker)

        fin = self._timed_fetch(lambda: t.financials, f"{ticker}/financials")
        bs  = self._timed_fetch(lambda: t.balance_sheet, f"{ticker}/balance_sheet")
        cf  = self._timed_fetch(lambda: t.cashflow, f"{ticker}/cashflow")

        if fin is None or bs is None or cf is None or fin.empty or bs.empty or cf.empty:
            return self._emit(
                ticker=ticker, run_id=run_id, as_of=as_of,
                score=50.0,
                confidence=Confidence.LOW,
                direction=Direction.NEUTRAL,
                materiality=Materiality.LOW,
                payload={"narrative": "Financial statements unavailable for Piotroski F-Score."},
                quality_flags=[QualityFlag.LOW_COVERAGE],
            )

        # ── Extract current and prior year ────────────────────────────
        ni_t    = _row(fin,  "Net Income", "Net Income Common Stockholders")
        ni_tm1  = _row2(fin, "Net Income", "Net Income Common Stockholders")
        rev_t   = _row(fin,  "Total Revenue")
        rev_tm1 = _row2(fin, "Total Revenue")
        gp_t    = _row(fin,  "Gross Profit")
        gp_tm1  = _row2(fin, "Gross Profit")

        ta_t    = _row(bs,   "Total Assets")
        ta_tm1  = _row2(bs,  "Total Assets")
        ca_t    = _row(bs,   "Current Assets", "Total Current Assets")
        ca_tm1  = _row2(bs,  "Current Assets", "Total Current Assets")
        cl_t    = _row(bs,   "Current Liabilities", "Total Current Liabilities")
        cl_tm1  = _row2(bs,  "Current Liabilities", "Total Current Liabilities")
        ltd_t   = _row(bs,   "Long Term Debt", "Long Term Debt And Capital Lease Obligation")
        ltd_tm1 = _row2(bs,  "Long Term Debt", "Long Term Debt And Capital Lease Obligation")
        shares_t   = _row(bs,  "Ordinary Shares Number", "Share Issued")
        shares_tm1 = _row2(bs, "Ordinary Shares Number", "Share Issued")

        cfo_t = _row(cf,  "Operating Cash Flow", "Cash Flow From Continuing Operating Activities",
                          "Total Cash From Operating Activities")

        # ── Compute signals ───────────────────────────────────────────
        signals: dict[str, int | None] = {}

        # Profitability
        avg_ta = ((ta_t + ta_tm1) / 2) if ta_t and ta_tm1 else ta_t
        roa_t   = (ni_t   / avg_ta)    if ni_t   is not None and avg_ta else None
        roa_tm1 = (ni_tm1 / ta_tm1)    if ni_tm1 is not None and ta_tm1 else None

        signals["F1_ROA_pos"]    = _b(roa_t   is not None and roa_t > 0)
        signals["F2_ΔROA_pos"]   = _b(roa_t   is not None and roa_tm1 is not None and roa_t > roa_tm1)
        signals["F3_CFO_pos"]    = _b(cfo_t   is not None and cfo_t > 0)
        signals["F4_accrual"]    = _b(cfo_t   is not None and ni_t  is not None and cfo_t > ni_t)

        # Leverage / Liquidity
        lev_t   = (ltd_t   / ta_t)   if ltd_t   is not None and ta_t   else None
        lev_tm1 = (ltd_tm1 / ta_tm1) if ltd_tm1 is not None and ta_tm1 else None
        cr_t    = (ca_t    / cl_t)   if ca_t    is not None and cl_t   and cl_t  > 0 else None
        cr_tm1  = (ca_tm1  / cl_tm1) if ca_tm1  is not None and cl_tm1 and cl_tm1 > 0 else None

        signals["F5_Δlev_neg"]   = _b(lev_t is not None and lev_tm1 is not None and lev_t < lev_tm1)
        signals["F6_Δcr_pos"]    = _b(cr_t  is not None and cr_tm1  is not None and cr_t  > cr_tm1)
        signals["F7_no_dilution"] = _b(shares_t is not None and shares_tm1 is not None
                                       and shares_t <= shares_tm1 * 1.005)

        # Operating Efficiency
        gm_t   = (gp_t   / rev_t)   if gp_t   is not None and rev_t   and rev_t   > 0 else None
        gm_tm1 = (gp_tm1 / rev_tm1) if gp_tm1 is not None and rev_tm1 and rev_tm1 > 0 else None
        at_t   = (rev_t   / ta_t)   if rev_t   is not None and ta_t   and ta_t   > 0 else None
        at_tm1 = (rev_tm1 / ta_tm1) if rev_tm1 is not None and ta_tm1 and ta_tm1 > 0 else None

        signals["F8_Δmargin_pos"] = _b(gm_t  is not None and gm_tm1  is not None and gm_t  > gm_tm1)
        signals["F9_Δturn_pos"]   = _b(at_t  is not None and at_tm1  is not None and at_t  > at_tm1)

        computed = {k: v for k, v in signals.items() if v is not None}
        f_score  = sum(v for v in computed.values() if v is not None)
        n_avail  = len(computed)

        if n_avail < 5:
            return self._emit(
                ticker=ticker, run_id=run_id, as_of=as_of,
                score=50.0,
                confidence=Confidence.LOW,
                direction=Direction.NEUTRAL,
                materiality=Materiality.LOW,
                payload={"f_score": f_score, "n_computed": n_avail,
                         "narrative": f"Only {n_avail}/9 Piotroski signals computable."},
                quality_flags=[QualityFlag.LOW_COVERAGE],
            )

        # Normalise if partial
        if n_avail < 9:
            f_equiv = round(f_score * 9 / n_avail)
        else:
            f_equiv = f_score

        notes: list[str] = []
        if f_equiv >= 8:
            base_score = 75.0
            notes.append(f"Piotroski F-Score {f_score}/{n_avail} — strong quality ({f_equiv}/9 equiv.)")
        elif f_equiv >= 6:
            base_score = 63.0
            notes.append(f"Piotroski F-Score {f_score}/{n_avail} — above-average quality")
        elif f_equiv >= 4:
            base_score = 52.0
            notes.append(f"Piotroski F-Score {f_score}/{n_avail} — average quality")
        elif f_equiv >= 2:
            base_score = 40.0
            notes.append(f"Piotroski F-Score {f_score}/{n_avail} — below-average quality")
        else:
            base_score = 28.0
            notes.append(f"Piotroski F-Score {f_score}/{n_avail} — weak quality signals")

        # Surface key passing/failing tests
        fails = [k for k, v in computed.items() if v == 0]
        passes = [k for k, v in computed.items() if v == 1]
        if fails:
            notes.append(f"Failing: {', '.join(fails[:3])}")

        score = max(10.0, min(90.0, base_score))

        if score >= 62:
            direction = Direction.POSITIVE
        elif score >= 45:
            direction = Direction.NEUTRAL
        else:
            direction = Direction.NEGATIVE

        if f_equiv >= 8 or f_equiv <= 1:
            materiality = Materiality.HIGH
        elif f_equiv >= 6 or f_equiv <= 3:
            materiality = Materiality.MEDIUM
        else:
            materiality = Materiality.LOW

        if n_avail >= 8:
            confidence = Confidence.HIGH
        elif n_avail >= 6:
            confidence = Confidence.MEDIUM
        else:
            confidence = Confidence.LOW

        flags: list[QualityFlag] = []
        if n_avail < 9:
            flags.append(QualityFlag.MISSING_FIELD)

        narrative = ". ".join(notes) + "."

        period_label = str(fin.columns[0].date()) if hasattr(fin.columns[0], "date") else str(fin.columns[0])[:10]

        return self._emit(
            ticker=ticker, run_id=run_id, as_of=as_of,
            score=score,
            confidence=confidence,
            direction=direction,
            materiality=materiality,
            payload={
                "f_score":          f_score,
                "n_computed":       n_avail,
                "f_equiv_of_9":     f_equiv,
                "signals":          {k: int(v) for k, v in computed.items()},
                "passing_signals":  passes,
                "failing_signals":  fails,
                "period":           period_label,
                "narrative":        narrative,
            },
            evidence=[
                Evidence.from_filing(ticker, period_label, notes),
            ],
            quality_flags=flags,
        )
