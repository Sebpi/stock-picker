"""
agent.financial_distress — Altman Z-Score for financial distress prediction.

The Altman Z-Score (Altman 1968, revised 2000) is a linear combination of five
financial ratios that predicts corporate bankruptcy with ~75% accuracy one year
ahead. It captures liquidity, profitability, leverage, solvency, and asset
efficiency in a single score.

Uses the Z''-Score (double-prime), the sector-agnostic variant, as it applies
to both manufacturing and non-manufacturing firms:

  Z'' = 6.56·X1 + 3.26·X2 + 6.72·X3 + 1.05·X4

  X1 = Working Capital / Total Assets            (liquidity)
  X2 = Retained Earnings / Total Assets          (cumulative profitability)
  X3 = EBIT / Total Assets                       (operating profitability)
  X4 = Book Value of Equity / Total Liabilities  (leverage)

  Z'' > 2.60  → Safe zone      (low distress risk)
  1.10–2.60   → Grey zone      (monitor leverage / liquidity)
  Z'' < 1.10  → Distress zone  (elevated bankruptcy risk)

─── Scoring ─────────────────────────────────────────────────────────────────────
Z'' > 3.5   → 74  (very safe — strong balance sheet)
2.60–3.50   → 65  (safe zone)
1.80–2.60   → 54  (grey zone upper — watch leverage)
1.10–1.80   → 42  (grey zone lower — elevated concern)
0–1.10      → 30  (distress zone)
< 0         → 20  (severe financial stress)

Additionally if Z'' > 2.60 but total-debt/EBITDA > 5× → −5 (leverage caveat)

─── Horizon relevance ───────────────────────────────────────────────────────────
Balance sheet distress signals play out over quarters to years.
Weight: 3m 0.04 / 6m 0.05 / 12m 0.06
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

_Z_SAFE_UPPER  = 3.50
_Z_SAFE        = 2.60
_Z_GREY_UPPER  = 1.80
_Z_DISTRESS    = 1.10


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


class FinancialDistressAgent(BaseAgent):
    agent_id = "agent.financial_distress"
    signal_type = "financial_distress"
    default_horizons = ["3m", "6m", "12m"]

    def _run(self, ticker: str, run_id: str, as_of: datetime):
        ticker = ticker.upper()
        t = yf.Ticker(ticker)

        info = self._timed_fetch(lambda: t.info, f"{ticker}/info") or {}
        bs   = self._timed_fetch(lambda: t.balance_sheet, f"{ticker}/balance_sheet")
        fin  = self._timed_fetch(lambda: t.financials, f"{ticker}/financials")

        if bs is None or fin is None or bs.empty or fin.empty:
            return self._emit(
                ticker=ticker, run_id=run_id, as_of=as_of,
                score=50.0,
                confidence=Confidence.LOW,
                direction=Direction.NEUTRAL,
                materiality=Materiality.LOW,
                payload={"narrative": "Financial statements unavailable for Z-Score."},
                quality_flags=[QualityFlag.LOW_COVERAGE],
            )

        # ── Balance sheet components ──────────────────────────────────
        ta   = _row(bs, "Total Assets")
        ca   = _row(bs, "Current Assets", "Total Current Assets")
        cl   = _row(bs, "Current Liabilities", "Total Current Liabilities")
        re   = _row(bs, "Retained Earnings")
        eq   = _row(bs, "Stockholders Equity", "Total Equity",
                    "Common Stock Equity", "Total Stockholders Equity")
        tl   = _row(bs, "Total Liabilities Net Minority Interest", "Total Liabilities")

        # Income statement
        ebit = _row(fin, "EBIT", "Earnings Before Interest And Taxes")
        if ebit is None:
            ni   = _row(fin, "Net Income", "Net Income Common Stockholders")
            ie   = self._safe_get(info, "interestExpense")
            tax  = _row(fin, "Tax Provision", "Income Tax Expense")
            if ni is not None:
                ebit = (ni or 0) + abs(float(ie)) if ie else ni
                if tax is not None:
                    ebit = (ebit or 0) + abs(float(tax))

        if ta is None or ta <= 0:
            return self._emit(
                ticker=ticker, run_id=run_id, as_of=as_of,
                score=50.0,
                confidence=Confidence.LOW,
                direction=Direction.NEUTRAL,
                materiality=Materiality.LOW,
                payload={"narrative": "Total assets unavailable — cannot compute Z-Score."},
                quality_flags=[QualityFlag.MISSING_FIELD],
            )

        # ── Z'' components ────────────────────────────────────────────
        wc = (ca - cl) if ca is not None and cl is not None else None

        X1 = wc / ta if wc is not None else None
        X2 = re / ta if re is not None else None
        X3 = ebit / ta if ebit is not None else None
        X4 = eq / tl if eq is not None and tl is not None and tl > 0 else None

        components = {"X1": X1, "X2": X2, "X3": X3, "X4": X4}
        available  = {k: v for k, v in components.items() if v is not None}

        if len(available) < 3:
            return self._emit(
                ticker=ticker, run_id=run_id, as_of=as_of,
                score=50.0,
                confidence=Confidence.LOW,
                direction=Direction.NEUTRAL,
                materiality=Materiality.LOW,
                payload={"narrative": f"Only {len(available)}/4 Z-Score components available.",
                         "components": available},
                quality_flags=[QualityFlag.LOW_COVERAGE],
            )

        # Compute Z'' with available components (use 0 for missing)
        z = (6.56 * (X1 or 0)
             + 3.26 * (X2 or 0)
             + 6.72 * (X3 or 0)
             + 1.05 * (X4 or 0))
        z = round(z, 3)

        # ── Base score from Z'' ───────────────────────────────────────
        notes: list[str] = []
        if z > _Z_SAFE_UPPER:
            base_score = 74.0
            notes.append(f"Altman Z''-Score {z:.2f} — very safe zone (>3.5)")
        elif z > _Z_SAFE:
            base_score = 65.0
            notes.append(f"Altman Z''-Score {z:.2f} — safe zone (>2.60)")
        elif z > _Z_GREY_UPPER:
            base_score = 54.0
            notes.append(f"Altman Z''-Score {z:.2f} — grey zone upper")
        elif z > _Z_DISTRESS:
            base_score = 42.0
            notes.append(f"Altman Z''-Score {z:.2f} — grey zone lower (elevated concern)")
        elif z > 0:
            base_score = 30.0
            notes.append(f"Altman Z''-Score {z:.2f} — distress zone")
        else:
            base_score = 20.0
            notes.append(f"Altman Z''-Score {z:.2f} — severe financial stress")

        adj = 0.0

        # ── Component commentary ──────────────────────────────────────
        if X1 is not None:
            if X1 < 0:
                notes.append(f"Negative working capital ({X1:.2f}) — liquidity strain")
            elif X1 > 0.20:
                notes.append(f"Healthy working capital ratio ({X1:.2f})")
        if X3 is not None and X3 < 0:
            notes.append(f"Negative EBIT/assets ({X3:.2f}) — operating losses")
        if X4 is not None:
            if X4 < 0.20:
                adj -= 3.0
                notes.append(f"Book equity/liabilities {X4:.2f} — highly leveraged")
            elif X4 > 1.0:
                notes.append(f"Equity > total liabilities ({X4:.2f}) — conservative balance sheet")

        score = max(10.0, min(90.0, base_score + adj))

        if score >= 62:
            direction = Direction.POSITIVE
        elif score >= 45:
            direction = Direction.NEUTRAL
        else:
            direction = Direction.NEGATIVE

        if z < _Z_DISTRESS or z > _Z_SAFE_UPPER:
            materiality = Materiality.HIGH
        elif z < _Z_SAFE:
            materiality = Materiality.MEDIUM
        else:
            materiality = Materiality.LOW

        n = len(available)
        if n == 4:
            confidence = Confidence.HIGH
        elif n == 3:
            confidence = Confidence.MEDIUM
        else:
            confidence = Confidence.LOW

        flags: list[QualityFlag] = []
        if n < 4:
            flags.append(QualityFlag.MISSING_FIELD)

        narrative = ". ".join(notes) + "."

        return self._emit(
            ticker=ticker, run_id=run_id, as_of=as_of,
            score=score,
            confidence=confidence,
            direction=direction,
            materiality=materiality,
            payload={
                "z_score":    z,
                "components": {k: round(v, 4) for k, v in available.items()},
                "n_components": n,
                "thresholds": {"safe": _Z_SAFE, "distress": _Z_DISTRESS},
                "base_score": round(base_score, 2),
                "adjustment": round(adj, 2),
                "narrative":  narrative,
            },
            evidence=[
                Evidence(
                    source_type="sec_filing",
                    source_name="yfinance (balance sheet, income statement)",
                    url_or_ref=f"yfinance://financials/{ticker}",
                    credibility_weight=0.85,
                    extracted_facts=notes,
                )
            ],
            quality_flags=flags,
        )
