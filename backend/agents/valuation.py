"""
agent.valuation — Valuation attractiveness relative to history, peers and intrinsic value.
Scores 0-100; higher = more attractively valued.
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
    Materiality,
    QualityFlag,
    ValuationPayload,
)

logger = logging.getLogger(__name__)

# Default WACC assumption when no beta available
DEFAULT_WACC = 0.09
RISK_FREE_RATE = 0.045
MARKET_PREMIUM = 0.055


class ValuationAgent(BaseAgent):
    agent_id = "agent.valuation"
    signal_type = "valuation_attractiveness"

    # ------------------------------------------------------------------
    # Scoring rubric
    # ------------------------------------------------------------------

    @staticmethod
    def _score_historical_percentile(pct: float | None) -> int:
        if pct is None:
            return 10  # neutral when no history
        if pct < 20:
            return 30
        if pct < 40:
            return 22
        if pct < 60:
            return 15
        if pct < 80:
            return 8
        return 2

    @staticmethod
    def _score_dcf_mos(mos_pct: float | None) -> int:
        if mos_pct is None:
            return 10
        if mos_pct > 20:
            return 30
        if mos_pct > 10:
            return 22
        if mos_pct > 0:
            return 15
        if mos_pct > -10:
            return 8
        return 2

    @staticmethod
    def _score_peg(peg: float | None) -> int:
        if peg is None or peg <= 0:
            return 10
        if peg < 1.0:
            return 20
        if peg < 1.5:
            return 15
        if peg < 2.0:
            return 10
        if peg < 3.0:
            return 5
        return 0

    @staticmethod
    def _score_peer_percentile(pct: float | None) -> int:
        if pct is None:
            return 10
        if pct < 30:
            return 20
        if pct < 50:
            return 15
        if pct < 70:
            return 10
        return 3

    # ------------------------------------------------------------------
    # DCF intrinsic value
    # ------------------------------------------------------------------

    @staticmethod
    def _dcf(fcf: float, shares: float, growth_rate: float,
              wacc: float, terminal_growth: float = 0.03,
              years: int = 5) -> float:
        """Simple two-stage DCF. Returns intrinsic value per share."""
        if shares <= 0 or fcf <= 0:
            return 0.0
        pv = 0.0
        cf = fcf
        for yr in range(1, years + 1):
            cf *= (1 + growth_rate)
            pv += cf / ((1 + wacc) ** yr)
        terminal = cf * (1 + terminal_growth) / (wacc - terminal_growth)
        pv += terminal / ((1 + wacc) ** years)
        return pv / shares

    def _dcf_sensitivity(self, fcf: float, shares: float,
                          base_growth: float, base_wacc: float) -> dict[str, float]:
        bear = self._dcf(fcf, shares, max(0, base_growth - 0.03), base_wacc + 0.02)
        base = self._dcf(fcf, shares, base_growth, base_wacc)
        bull = self._dcf(fcf, shares, base_growth + 0.03, max(0.05, base_wacc - 0.01))
        return {"bear": round(bear, 2), "base": round(base, 2), "bull": round(bull, 2)}

    # ------------------------------------------------------------------
    # Core run
    # ------------------------------------------------------------------

    def _run(self, ticker: str, run_id: str, as_of: datetime):
        t = yf.Ticker(ticker)
        info = self._timed_fetch(lambda: t.info, f"{ticker}/info") or {}

        price = self._safe_get(info, "currentPrice") or self._safe_get(info, "regularMarketPrice")
        market_cap = self._safe_get(info, "marketCap")
        ev = self._safe_get(info, "enterpriseValue")
        pe_ttm = self._safe_get(info, "trailingPE")
        fwd_pe = self._safe_get(info, "forwardPE")
        ps_ttm = self._safe_get(info, "priceToSalesTrailing12Months")
        ev_ebitda = self._safe_get(info, "enterpriseToEbitda")
        peg = self._safe_get(info, "trailingPegRatio") or self._safe_get(info, "pegRatio")
        fcf = self._safe_get(info, "freeCashflow")
        shares = self._safe_get(info, "sharesOutstanding")
        beta = self._safe_get(info, "beta", 1.0)
        rev_growth = self._safe_get(info, "revenueGrowth", 0.10)

        # P/FCF
        p_fcf: float | None = None
        if price and fcf and shares and shares > 0:
            fcf_per_share = fcf / shares
            if fcf_per_share > 0:
                p_fcf = price / fcf_per_share

        # WACC approximation
        wacc = RISK_FREE_RATE + max(0.5, float(beta or 1.0)) * MARKET_PREMIUM

        # DCF
        dcf_sensitivity: dict[str, float] = {}
        dcf_base: float | None = None
        mos_pct: float | None = None
        if fcf and shares and shares > 0 and rev_growth is not None:
            dcf_sensitivity = self._dcf_sensitivity(fcf, shares, rev_growth, wacc)
            dcf_base = dcf_sensitivity.get("base", 0.0)
            if price and dcf_base and dcf_base > 0:
                mos_pct = (dcf_base - price) / dcf_base * 100

        # Snapshot valuation history for percentile calculation next run
        db.snapshot_valuation(ticker, pe_ttm, fwd_pe, ev_ebitda, ps_ttm)

        # Historical percentile (P/E as primary metric)
        hist_pct: float | None = None
        if pe_ttm is not None:
            hist_pct = db.get_valuation_percentile(ticker, "pe_ttm", pe_ttm)

        # Peer percentile
        peer_pct: float | None = None
        peer_group = db.get_peer_group(ticker)
        if peer_group and pe_ttm is not None:
            peer_pes: list[float] = []
            for peer in peer_group[:8]:
                p_ticker = yf.Ticker(peer)
                p_info = self._timed_fetch(lambda pt=p_ticker: pt.info, f"{peer}/info") or {}
                p_pe = self._safe_get(p_info, "trailingPE")
                if p_pe and p_pe > 0:
                    peer_pes.append(p_pe)
            if peer_pes:
                below = sum(1 for v in peer_pes if v >= pe_ttm)
                peer_pct = round(below / len(peer_pes) * 100, 1)

        # ---- Score ----
        score = (
            self._score_historical_percentile(hist_pct)
            + self._score_dcf_mos(mos_pct)
            + self._score_peg(peg)
            + self._score_peer_percentile(peer_pct)
        )
        score = float(min(100, max(0, score)))

        # ---- Flags ----
        flags: list[QualityFlag] = []
        if price is None or (pe_ttm is None and fwd_pe is None):
            flags.append(QualityFlag.MISSING_FIELD)
        if hist_pct is None:
            flags.append(QualityFlag.STALE_SOURCE)

        # ---- Direction ----
        if score >= 60:
            direction = Direction.POSITIVE
        elif score <= 35:
            direction = Direction.NEGATIVE
        else:
            direction = Direction.NEUTRAL

        # ---- Confidence ----
        fields = sum(v is not None for v in [pe_ttm, peg, dcf_base, hist_pct])
        confidence = Confidence.HIGH if fields >= 3 else (Confidence.MEDIUM if fields >= 2 else Confidence.LOW)

        # ---- Commentary ----
        commentary: list[str] = []
        if mos_pct is not None:
            commentary.append(f"DCF margin of safety: {mos_pct:.1f}%")
        if hist_pct is not None:
            commentary.append(f"P/E at {hist_pct:.0f}th historical percentile")
        if peer_pct is not None:
            commentary.append(f"P/E at {peer_pct:.0f}th percentile vs peers")
        if peg and peg > 3:
            commentary.append(f"PEG ratio {peg:.1f} indicates elevated growth premium")

        evidence = [
            Evidence.from_market_data(ticker, [
                f"Price: {price}" if price else "Price: N/A",
                f"P/E TTM: {pe_ttm:.1f}" if pe_ttm else "P/E: N/A",
                f"Forward P/E: {fwd_pe:.1f}" if fwd_pe else "Fwd P/E: N/A",
                f"EV/EBITDA: {ev_ebitda:.1f}" if ev_ebitda else "EV/EBITDA: N/A",
                f"PEG: {peg:.2f}" if peg else "PEG: N/A",
            ]),
        ]

        payload = ValuationPayload(
            price=price,
            market_cap=market_cap,
            enterprise_value=ev,
            pe_ttm=pe_ttm,
            forward_pe=fwd_pe,
            ps_ttm=ps_ttm,
            ev_ebitda=ev_ebitda,
            peg=peg,
            p_fcf=p_fcf,
            peer_percentile=peer_pct,
            historical_percentile=hist_pct,
            dcf_sensitivity=dcf_sensitivity,
            valuation_commentary=commentary,
        ).model_dump()

        return self._emit(
            ticker=ticker,
            run_id=run_id,
            as_of=as_of,
            score=score,
            confidence=confidence,
            direction=direction,
            materiality=Materiality.HIGH,
            payload=payload,
            evidence=evidence,
            quality_flags=flags,
        )
