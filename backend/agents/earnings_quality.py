"""
agent.earnings_quality — Earnings manipulation risk via Beneish M-score and
Sloan accruals ratio.

Answers the question the Fundamentals agent doesn't: *are these earnings real?*
Both metrics are computed entirely from financial statements already fetched via
yfinance — no new API key required.

─── Beneish M-score (Beneish 1999) ────────────────────────────────────────────
Eight financial-ratio indices compared year-on-year. Firms with M > −1.78 are
flagged as likely manipulators; M < −2.22 is the "safe" zone.

  DSRI  Days Sales Receivable Index     — rising AR relative to revenue
  GMI   Gross Margin Index              — deteriorating gross margin
  AQI   Asset Quality Index             — growing intangible/off-B/S assets
  SGI   Sales Growth Index              — high growth tempts manipulation
  DEPI  Depreciation Index              — slowing depreciation rate
  SGAI  SG&A Index                      — rising overhead vs revenue
  LVGI  Leverage Index                  — rising debt burden
  TATA  Total Accruals to Total Assets  — earnings not backed by cash flow

  M = -4.84 + 0.920·DSRI + 0.528·GMI + 0.404·AQI + 0.892·SGI
            + 0.115·DEPI - 0.172·SGAI + 4.679·TATA - 0.327·LVGI

  M > -1.78  → likely manipulator  (original paper: ~76% accuracy)
  M < -2.22  → unlikely manipulator

─── Sloan Accruals Ratio (Sloan 1996) ─────────────────────────────────────────
  Accruals Ratio = (Net Income − Operating Cash Flow) / Average Total Assets

  High positive accruals (> 0.10) indicate earnings are running well above
  cash flow — a reliable predictor of future earnings *reversals*. The market
  tends to overweight accruals-driven earnings, creating a predictable
  mean-reversion effect over 6–18 months.

  > 0.10  → earnings significantly outpacing cash flow (bearish)
  0–0.10  → mild / normal
  < 0     → cash flow exceeds earnings (high quality, bullish)

─── Score ──────────────────────────────────────────────────────────────────────
  Base score from Beneish M (if computable):
    M < -2.22         → 68  (clean)
    -2.22 ≤ M < -1.78 → 54  (grey zone)
    -1.78 ≤ M < -1.00 → 40  (elevated risk)
    M ≥ -1.00         → 28  (strong manipulation signal)

  Sloan accruals adjustment (applied on top):
    < -0.05           → +7   (earnings quality premium)
    -0.05 to 0.05     →  0   (normal)
    0.05 to 0.10      → -6   (elevated accruals)
    > 0.10            → -12  (accruals-driven earnings, expect reversion)

  If only Sloan is available (Beneish incomplete): base = 55, adj applied.

─── Horizon relevance ──────────────────────────────────────────────────────────
  Accruals revert over 6–18 months, so the signal is strongest at 6m and 12m.
  Weight: 3m 0.04 / 6m 0.09 / 12m 0.10  (see schemas.HORIZON_WEIGHTS)
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

# Beneish M-score thresholds
M_MANIPULATOR   = -1.78
M_SAFE          = -2.22

# Sloan thresholds
ACCRUALS_HIGH   = 0.10
ACCRUALS_LOW    = -0.05

# Beneish coefficient vector
_B = {
    "intercept": -4.84,
    "DSRI": 0.920,
    "GMI":  0.528,
    "AQI":  0.404,
    "SGI":  0.892,
    "DEPI": 0.115,
    "SGAI": -0.172,
    "TATA": 4.679,
    "LVGI": -0.327,
}


def _row(df: Any, *names: str) -> float | None:
    """Return the first available row value from a DataFrame, col 0 (latest)."""
    for name in names:
        if name in df.index:
            val = df.loc[name, df.columns[0]]
            if val is not None and val == val:  # NaN check
                try:
                    return float(val)
                except (TypeError, ValueError):
                    pass
    return None


def _row2(df: Any, *names: str) -> float | None:
    """Same but column 1 (prior year)."""
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


class EarningsQualityAgent(BaseAgent):
    agent_id = "agent.earnings_quality"
    signal_type = "earnings_quality"
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
                payload={"narrative": "Financial statements unavailable."},
                quality_flags=[QualityFlag.LOW_COVERAGE],
            )

        # ── Extract current-year (t) and prior-year (t-1) values ─────

        # Income statement
        rev_t    = _row(fin,  "Total Revenue")
        rev_tm1  = _row2(fin, "Total Revenue")
        gp_t     = _row(fin,  "Gross Profit")
        gp_tm1   = _row2(fin, "Gross Profit")
        sga_t    = _row(fin,  "Selling General Administrative", "Selling General And Administration",
                              "Selling And Marketing Expense")
        sga_tm1  = _row2(fin, "Selling General Administrative", "Selling General And Administration",
                               "Selling And Marketing Expense")
        dep_t    = _row(fin,  "Reconciled Depreciation", "Depreciation", "Depreciation And Amortization")
        dep_tm1  = _row2(fin, "Reconciled Depreciation", "Depreciation", "Depreciation And Amortization")
        ni_t     = _row(fin,  "Net Income", "Net Income Common Stockholders")

        # Balance sheet
        ar_t     = _row(bs,   "Net Receivables", "Receivables", "Accounts Receivable")
        ar_tm1   = _row2(bs,  "Net Receivables", "Receivables", "Accounts Receivable")
        ca_t     = _row(bs,   "Current Assets", "Total Current Assets")
        ca_tm1   = _row2(bs,  "Current Assets", "Total Current Assets")
        ppe_t    = _row(bs,   "Net PPE", "Property Plant Equipment", "Net Property Plant And Equipment")
        ppe_tm1  = _row2(bs,  "Net PPE", "Property Plant Equipment", "Net Property Plant And Equipment")
        ta_t     = _row(bs,   "Total Assets")
        ta_tm1   = _row2(bs,  "Total Assets")
        ltd_t    = _row(bs,   "Long Term Debt", "Long Term Debt And Capital Lease Obligation")
        ltd_tm1  = _row2(bs,  "Long Term Debt", "Long Term Debt And Capital Lease Obligation")
        cl_t     = _row(bs,   "Current Liabilities", "Total Current Liabilities")
        cl_tm1   = _row2(bs,  "Current Liabilities", "Total Current Liabilities")

        # Cash flow
        cfo_t    = _row(cf,   "Operating Cash Flow", "Cash Flow From Continuing Operating Activities",
                              "Total Cash From Operating Activities")

        # ── Sloan accruals ratio ─────────────────────────────────────
        sloan: float | None = None
        if ni_t is not None and cfo_t is not None and ta_t is not None and ta_t > 0:
            avg_ta = ((ta_t + (ta_tm1 or ta_t)) / 2) if ta_tm1 else ta_t
            sloan = (ni_t - cfo_t) / avg_ta

        # ── Beneish components ────────────────────────────────────────
        beneish_components: dict[str, float] = {}
        m_score: float | None = None

        def _safe_ratio(num, denom):
            if num is None or denom is None or denom == 0:
                return None
            return num / denom

        try:
            # DSRI
            dsri = _safe_ratio(
                _safe_ratio(ar_t, rev_t),
                _safe_ratio(ar_tm1, rev_tm1),
            )
            # GMI
            gm_t   = _safe_ratio(gp_t,   rev_t)
            gm_tm1 = _safe_ratio(gp_tm1, rev_tm1)
            gmi = _safe_ratio(gm_tm1, gm_t)
            # AQI
            nca_t   = 1 - _safe_ratio((ca_t or 0) + (ppe_t or 0), ta_t)   if ta_t   else None
            nca_tm1 = 1 - _safe_ratio((ca_tm1 or 0) + (ppe_tm1 or 0), ta_tm1) if ta_tm1 else None
            aqi = _safe_ratio(nca_t, nca_tm1) if nca_tm1 and nca_tm1 != 0 else None
            # SGI
            sgi = _safe_ratio(rev_t, rev_tm1)
            # DEPI
            dep_rate_t   = _safe_ratio(dep_t,   (ppe_t   or 0) + (dep_t   or 0)) if dep_t   else None
            dep_rate_tm1 = _safe_ratio(dep_tm1, (ppe_tm1 or 0) + (dep_tm1 or 0)) if dep_tm1 else None
            depi = _safe_ratio(dep_rate_tm1, dep_rate_t)
            # SGAI
            sgai = _safe_ratio(
                _safe_ratio(sga_t, rev_t),
                _safe_ratio(sga_tm1, rev_tm1),
            )
            # LVGI
            lev_t   = _safe_ratio((ltd_t   or 0) + (cl_t   or 0), ta_t)
            lev_tm1 = _safe_ratio((ltd_tm1 or 0) + (cl_tm1 or 0), ta_tm1)
            lvgi = _safe_ratio(lev_t, lev_tm1) if lev_tm1 and lev_tm1 != 0 else None
            # TATA (same as Sloan but without avg assets)
            tata = _safe_ratio((ni_t or 0) - (cfo_t or 0), ta_t) if ta_t else None

            components = {
                "DSRI": dsri, "GMI": gmi, "AQI": aqi, "SGI": sgi,
                "DEPI": depi, "SGAI": sgai, "TATA": tata, "LVGI": lvgi,
            }
            beneish_components = {k: round(v, 4) for k, v in components.items() if v is not None}

            # Need at least 5 of 8 components for a reliable M-score.
            if len(beneish_components) >= 5:
                m_score = _B["intercept"]
                for k, coef in _B.items():
                    if k == "intercept":
                        continue
                    if k in beneish_components:
                        m_score += coef * beneish_components[k]
                m_score = round(m_score, 3)

        except Exception as exc:
            logger.debug("[earnings_quality] %s: Beneish calc error: %s", ticker, exc)

        # ── Base score ───────────────────────────────────────────────
        if m_score is not None:
            if m_score < M_SAFE:
                base_score = 68.0
                risk_label = "low manipulation risk"
            elif m_score < M_MANIPULATOR:
                base_score = 54.0
                risk_label = "grey zone"
            elif m_score < -1.00:
                base_score = 40.0
                risk_label = "elevated manipulation risk"
            else:
                base_score = 28.0
                risk_label = "strong manipulation signal"
        else:
            base_score = 55.0
            risk_label = "insufficient data for M-score"

        # ── Sloan adjustment ─────────────────────────────────────────
        sloan_adj = 0.0
        sloan_label = "accruals: n/a"
        if sloan is not None:
            if sloan < ACCRUALS_LOW:
                sloan_adj, sloan_label = +7.0, f"accruals {sloan:+.3f} — cash flow exceeds earnings (quality premium)"
            elif sloan < 0.05:
                sloan_adj, sloan_label = 0.0, f"accruals {sloan:+.3f} — normal"
            elif sloan < ACCRUALS_HIGH:
                sloan_adj, sloan_label = -6.0, f"accruals {sloan:+.3f} — elevated, watch for earnings reversion"
            else:
                sloan_adj, sloan_label = -12.0, f"accruals {sloan:+.3f} — earnings significantly outpacing cash flow"

        score = max(10.0, min(90.0, base_score + sloan_adj))

        # ── Direction, materiality, confidence ───────────────────────
        if score >= 62:
            direction = Direction.POSITIVE
        elif score >= 48:
            direction = Direction.NEUTRAL
        else:
            direction = Direction.NEGATIVE

        if m_score is not None and (m_score >= M_MANIPULATOR or (sloan is not None and sloan > ACCRUALS_HIGH)):
            materiality = Materiality.HIGH
        elif m_score is not None and m_score >= M_SAFE:
            materiality = Materiality.MEDIUM
        else:
            materiality = Materiality.LOW

        n_components = len(beneish_components)
        if m_score is not None and sloan is not None and n_components >= 6:
            confidence = Confidence.HIGH
        elif m_score is not None or sloan is not None:
            confidence = Confidence.MEDIUM
        else:
            confidence = Confidence.LOW

        # ── Narrative ────────────────────────────────────────────────
        parts = []
        if m_score is not None:
            parts.append(f"Beneish M-score: {m_score:.2f} ({risk_label}; threshold −1.78)")
        parts.append(sloan_label.capitalize())
        if m_score is not None and m_score >= M_MANIPULATOR:
            parts.append("Earnings quality concern — consider verifying reported figures against cash flow.")
        narrative = ". ".join(parts) + "."

        flags: list[QualityFlag] = []
        if m_score is None:
            flags.append(QualityFlag.MISSING_FIELD)
        if sloan is None:
            flags.append(QualityFlag.MISSING_FIELD)

        period_label = str(fin.columns[0].date()) if hasattr(fin.columns[0], "date") else str(fin.columns[0])[:10]

        evidence = [
            Evidence.from_filing(ticker, period_label, [p for p in parts]),
        ]

        return self._emit(
            ticker=ticker, run_id=run_id, as_of=as_of,
            score=score,
            confidence=confidence,
            direction=direction,
            materiality=materiality,
            payload={
                "m_score":              m_score,
                "m_score_threshold":    M_MANIPULATOR,
                "beneish_components":   beneish_components,
                "sloan_accruals_ratio": round(sloan, 4) if sloan is not None else None,
                "components_available": n_components,
                "narrative":            narrative,
            },
            evidence=evidence,
            quality_flags=flags,
        )
