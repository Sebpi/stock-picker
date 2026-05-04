"""
agent.fundamentals — Financial quality and business health assessment.
Scores 0-100 based on revenue growth, margins, FCF, debt and share dilution.
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
    FundamentalPayload,
    Materiality,
    QualityFlag,
)

logger = logging.getLogger(__name__)


class FundamentalsAgent(BaseAgent):
    agent_id = "agent.fundamentals"
    signal_type = "fundamental_quality"

    # ------------------------------------------------------------------
    # Scoring rubric (deterministic, unit-testable)
    # ------------------------------------------------------------------

    @staticmethod
    def _score_revenue_growth(yoy: float | None) -> int:
        if yoy is None:
            return 0
        if yoy > 0.20:
            return 25
        if yoy > 0.10:
            return 20
        if yoy > 0.05:
            return 15
        if yoy >= 0:
            return 10
        return 0

    @staticmethod
    def _score_gross_margin(gm: float | None) -> int:
        if gm is None:
            return 0
        if gm > 0.50:
            return 20
        if gm > 0.35:
            return 15
        if gm > 0.20:
            return 10
        return 5

    @staticmethod
    def _score_fcf_conversion(conv: float | None) -> int:
        if conv is None:
            return 0
        if conv > 0.80:
            return 20
        if conv > 0.50:
            return 15
        if conv > 0.20:
            return 10
        return 0

    @staticmethod
    def _score_debt(dte: float | None, net_cash: float | None) -> int:
        if net_cash is not None and net_cash > 0:
            return 20
        if dte is None:
            return 10
        if dte < 0.5:
            return 15
        if dte < 1.0:
            return 10
        if dte < 2.0:
            return 5
        return 0

    @staticmethod
    def _score_eps_growth(yoy: float | None) -> int:
        if yoy is None:
            return 0
        if yoy > 0.20:
            return 15
        if yoy > 0.10:
            return 12
        if yoy > 0:
            return 8
        return 0

    # ------------------------------------------------------------------
    # Red flag detection
    # ------------------------------------------------------------------

    @staticmethod
    def _detect_red_flags(
        info: dict[str, Any],
        gross_margin: float | None,
        prev_gross_margin: float | None,
        dte: float | None,
        fcf: float | None,
        share_change: float | None,
    ) -> list[str]:
        flags: list[str] = []
        if (gross_margin is not None and prev_gross_margin is not None
                and (gross_margin - prev_gross_margin) < -0.02):
            flags.append(f"Margin compression: gross margin fell {(prev_gross_margin - gross_margin)*100:.1f}pp")
        if dte is not None and dte > 1.5:
            flags.append(f"High leverage: D/E ratio {dte:.2f}")
        if fcf is not None and fcf < 0:
            flags.append("Negative free cash flow")
        if share_change is not None and share_change > 0.03:
            flags.append(f"Share dilution: count up {share_change*100:.1f}% YoY")
        if info.get("returnOnEquity") is not None and info["returnOnEquity"] < 0:
            flags.append("Negative return on equity")
        return flags

    # ------------------------------------------------------------------
    # Core run
    # ------------------------------------------------------------------

    def _run(self, ticker: str, run_id: str, as_of: datetime):
        t = yf.Ticker(ticker)
        info = t.info or {}

        db.upsert_ticker(ticker, info)

        # ---- Extract metrics from yfinance info ----
        rev_growth = self._safe_get(info, "revenueGrowth")
        eps_growth = self._safe_get(info, "earningsGrowth")
        gross_margin = self._safe_get(info, "grossMargins")
        op_margin = self._safe_get(info, "operatingMargins")
        fcf = self._safe_get(info, "freeCashflow")
        net_income = self._safe_get(info, "netIncomeToCommon")
        market_cap = self._safe_get(info, "marketCap")
        total_debt = self._safe_get(info, "totalDebt", 0.0)
        total_cash = self._safe_get(info, "totalCash", 0.0)
        dte = self._safe_get(info, "debtToEquity")
        shares = self._safe_get(info, "sharesOutstanding")

        # FCF margin and conversion
        revenue = self._safe_get(info, "totalRevenue")
        fcf_margin: float | None = None
        fcf_conversion: float | None = None
        if fcf is not None and revenue and revenue > 0:
            fcf_margin = fcf / revenue
        if fcf is not None and net_income and net_income != 0:
            fcf_conversion = fcf / net_income

        # Net cash/debt
        net_cash: float | None = None
        if total_cash is not None and total_debt is not None:
            net_cash = total_cash - total_debt

        # Previous gross margin from financials DataFrame
        prev_gross_margin: float | None = None
        share_change: float | None = None
        period_label = ""
        try:
            fin = t.financials  # annual, columns are dates newest first
            if fin is not None and not fin.empty and fin.shape[1] >= 2:
                rev0 = fin.loc["Total Revenue", fin.columns[0]] if "Total Revenue" in fin.index else None
                rev1 = fin.loc["Total Revenue", fin.columns[1]] if "Total Revenue" in fin.index else None
                gp0 = fin.loc["Gross Profit", fin.columns[0]] if "Gross Profit" in fin.index else None
                gp1 = fin.loc["Gross Profit", fin.columns[1]] if "Gross Profit" in fin.index else None
                # Override rev_growth with exact YoY if available
                if rev0 and rev1 and rev1 != 0:
                    rev_growth = (rev0 - rev1) / abs(rev1)
                if gp0 and gp1 and rev0 and rev1 and rev0 != 0 and rev1 != 0:
                    prev_gross_margin = gp1 / rev1
                period_label = str(fin.columns[0].date()) if hasattr(fin.columns[0], 'date') else str(fin.columns[0])[:10]
        except Exception as exc:
            logger.debug("[%s] Could not fetch financials DataFrame: %s", ticker, exc)

        try:
            shares_hist = t.shares_outstanding_history
            if shares_hist is not None and not shares_hist.empty and len(shares_hist) >= 2:
                s_new = shares_hist.iloc[-1]
                s_old = shares_hist.iloc[-min(4, len(shares_hist) - 1)]  # ~1yr back
                if s_old and s_old != 0:
                    share_change = (s_new - s_old) / abs(s_old)
        except Exception:
            pass

        # ---- Score ----
        score = (
            self._score_revenue_growth(rev_growth)
            + self._score_gross_margin(gross_margin)
            + self._score_fcf_conversion(fcf_conversion)
            + self._score_debt(dte if dte is None else dte / 100, net_cash)
            + self._score_eps_growth(eps_growth)
        )
        score = float(min(100, max(0, score)))

        # ---- Quality flags ----
        flags: list[QualityFlag] = []
        if rev_growth is None or gross_margin is None:
            flags.append(QualityFlag.MISSING_FIELD)
        if score > 90:
            flags.append(QualityFlag.OUTLIER_VALUE)

        # ---- Red flags ----
        red_flags = self._detect_red_flags(
            info, gross_margin, prev_gross_margin, dte, fcf, share_change
        )

        # ---- Direction ----
        if score >= 65:
            direction = Direction.POSITIVE
        elif score <= 35:
            direction = Direction.NEGATIVE
        else:
            direction = Direction.NEUTRAL

        # ---- Confidence ----
        fields_present = sum(v is not None for v in [rev_growth, gross_margin, fcf_margin, dte, eps_growth])
        if fields_present >= 4:
            confidence = Confidence.HIGH
        elif fields_present >= 2:
            confidence = Confidence.MEDIUM
        else:
            confidence = Confidence.LOW

        # ---- Evidence ----
        evidence = [
            Evidence.from_filing(ticker, period_label, [
                f"Revenue growth YoY: {rev_growth*100:.1f}%" if rev_growth is not None else "Revenue growth: N/A",
                f"Gross margin: {gross_margin*100:.1f}%" if gross_margin is not None else "Gross margin: N/A",
                f"Operating margin: {op_margin*100:.1f}%" if op_margin is not None else "Operating margin: N/A",
                f"FCF conversion: {fcf_conversion*100:.1f}%" if fcf_conversion is not None else "FCF conversion: N/A",
                f"Debt/Equity: {dte:.2f}" if dte is not None else "D/E: N/A",
                f"EPS growth YoY: {eps_growth*100:.1f}%" if eps_growth is not None else "EPS growth: N/A",
            ]),
        ]

        payload = FundamentalPayload(
            period=period_label,
            revenue_growth_yoy=rev_growth,
            eps_growth_yoy=eps_growth,
            gross_margin=gross_margin,
            operating_margin=op_margin,
            fcf_margin=fcf_margin,
            fcf_conversion=fcf_conversion,
            debt_to_equity=dte,
            net_cash_or_debt=net_cash,
            share_count_change_yoy=share_change,
            red_flags=red_flags,
        ).model_dump()

        return self._emit(
            ticker=ticker,
            run_id=run_id,
            as_of=as_of,
            score=score,
            confidence=confidence,
            direction=direction,
            materiality=Materiality.HIGH if red_flags else Materiality.MEDIUM,
            payload=payload,
            evidence=evidence,
            quality_flags=flags,
        )
