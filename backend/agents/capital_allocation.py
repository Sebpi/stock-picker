"""
agent.capital_allocation — Return on capital and capital deployment quality.

A company that earns returns above its cost of capital compounds value;
one that earns below it destroys it regardless of reported growth rates.
This agent scores on ROIC quality, FCF conversion, and acquisition discipline.

─── Signals ─────────────────────────────────────────────────────────────────────
1. Return on Equity (ROE) / Return on Assets (ROA) from yfinance info — direct
   proxies for capital efficiency, widely available.

2. FCF conversion — free cash flow / net income.
   FCF > NI means cash generation exceeds reported earnings (quality premium).
   FCF << NI signals heavy working-capital consumption or accruals-driven earnings.

3. Goodwill growth — goodwill + intangibles as % of total assets, vs prior year.
   Fast-growing goodwill signals acquisition-driven growth that often destroys
   value; stable or shrinking goodwill suggests organic growth.

4. Asset turnover trend — revenue / total assets, current vs prior year.
   Improving asset turns signal increasing operational efficiency.

─── Scoring ─────────────────────────────────────────────────────────────────────
Base from ROE:
  ROE > 25%  → 72  (compounding machine — top-tier capital allocation)
  ROE 15–25% → 63
  ROE 8–15%  → 53  (average)
  ROE 0–8%   → 43
  ROE < 0    → 32  (loss-making — capital destruction)
  Unavailable → 52 (neutral)

FCF conversion adjustment:
  FCF/NI > 1.5  → +7   (cash generation well exceeds earnings)
  FCF/NI 1.0–1.5 → +3
  FCF/NI 0.5–1.0 →  0
  FCF/NI 0–0.5  → -5   (heavy cash consumption; accruals-driven)
  FCF/NI < 0 or NI<0 → -4  (negative FCF or net loss)

Goodwill / intangibles growth:
  Goodwill % of assets declining → +3  (capital discipline)
  Goodwill % rising > 5pp YoY   → -5  (aggressive acquisition splurge)

Asset turnover improvement:
  Turns improving > 10%  → +2
  Turns deteriorating > 10% → -2

─── Horizon relevance ───────────────────────────────────────────────────────────
Capital allocation quality is a long-duration signal — high ROIC compounds
over years, not months. Most meaningful at 12m; light at 3m.
Weight: 3m 0.03 / 6m 0.05 / 12m 0.08
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


class CapitalAllocationAgent(BaseAgent):
    agent_id = "agent.capital_allocation"
    signal_type = "capital_allocation"
    default_horizons = ["3m", "6m", "12m"]

    def _run(self, ticker: str, run_id: str, as_of: datetime):
        ticker = ticker.upper()
        t = yf.Ticker(ticker)

        info = self._timed_fetch(lambda: t.info, f"{ticker}/info") or {}
        fin  = self._timed_fetch(lambda: t.financials, f"{ticker}/financials")
        bs   = self._timed_fetch(lambda: t.balance_sheet, f"{ticker}/balance_sheet")
        cf   = self._timed_fetch(lambda: t.cashflow, f"{ticker}/cashflow")

        # ── ROE / ROA from info (most reliable source) ────────────────
        roe = self._safe_get(info, "returnOnEquity")
        roa = self._safe_get(info, "returnOnAssets")
        if roe is not None:
            roe = float(roe)
        if roa is not None:
            roa = float(roa)

        # ── FCF conversion ────────────────────────────────────────────
        fcf_conversion: float | None = None
        ni_t: float | None = None
        fcf_t: float | None = None

        if fin is not None and not fin.empty:
            ni_t = _row(fin, "Net Income", "Net Income Common Stockholders")
        if cf is not None and not cf.empty:
            fcf_t = _row(cf, "Free Cash Flow")
            if fcf_t is None:
                cfo = _row(cf, "Operating Cash Flow", "Cash Flow From Continuing Operating Activities")
                capex = _row(cf, "Capital Expenditure", "Purchase Of Property Plant And Equipment")
                if cfo is not None and capex is not None:
                    fcf_t = cfo + capex  # capex is usually negative in yfinance

        if ni_t is not None and fcf_t is not None and ni_t != 0:
            fcf_conversion = fcf_t / ni_t

        # ── Goodwill / intangibles as % of total assets ───────────────
        gw_pct_t: float | None = None
        gw_pct_tm1: float | None = None
        if bs is not None and not bs.empty:
            ta_t   = _row(bs,  "Total Assets")
            ta_tm1 = _row2(bs, "Total Assets")
            gw_t   = _row(bs,  "Goodwill And Other Intangible Assets", "Goodwill",
                          "Other Intangible Assets")
            gw_tm1 = _row2(bs, "Goodwill And Other Intangible Assets", "Goodwill",
                           "Other Intangible Assets")
            if gw_t is not None and ta_t and ta_t > 0:
                gw_pct_t = gw_t / ta_t
            if gw_tm1 is not None and ta_tm1 and ta_tm1 > 0:
                gw_pct_tm1 = gw_tm1 / ta_tm1

        # ── Asset turnover ────────────────────────────────────────────
        asset_turn_t: float | None = None
        asset_turn_tm1: float | None = None
        if fin is not None and not fin.empty and bs is not None and not bs.empty:
            rev_t   = _row(fin,  "Total Revenue")
            rev_tm1 = _row2(fin, "Total Revenue")
            ta_t   = _row(bs,  "Total Assets")
            ta_tm1 = _row2(bs, "Total Assets")
            if rev_t and ta_t and ta_t > 0:
                asset_turn_t = rev_t / ta_t
            if rev_tm1 and ta_tm1 and ta_tm1 > 0:
                asset_turn_tm1 = rev_tm1 / ta_tm1

        # ── Base score from ROE ───────────────────────────────────────
        notes: list[str] = []
        if roe is not None:
            if roe > 0.25:
                base_score = 72.0
                notes.append(f"ROE {roe:.1%} — top-tier capital efficiency")
            elif roe > 0.15:
                base_score = 63.0
                notes.append(f"ROE {roe:.1%} — above-average returns")
            elif roe > 0.08:
                base_score = 53.0
                notes.append(f"ROE {roe:.1%} — average returns on capital")
            elif roe > 0:
                base_score = 43.0
                notes.append(f"ROE {roe:.1%} — below-average capital returns")
            else:
                base_score = 32.0
                notes.append(f"ROE {roe:.1%} — loss-making, capital destruction")
        else:
            base_score = 52.0
            notes.append("ROE unavailable")

        if roa is not None:
            notes.append(f"ROA {roa:.1%}")

        adj = 0.0

        # ── FCF conversion adjustment ─────────────────────────────────
        if fcf_conversion is not None:
            if ni_t is not None and ni_t < 0:
                adj -= 4.0
                notes.append("Net loss — FCF conversion not meaningful")
            elif fcf_conversion > 1.5:
                adj += 7.0
                notes.append(f"FCF conversion {fcf_conversion:.1f}× — cash generation well exceeds earnings")
            elif fcf_conversion > 1.0:
                adj += 3.0
                notes.append(f"FCF conversion {fcf_conversion:.1f}× — strong cash backing")
            elif fcf_conversion > 0.5:
                notes.append(f"FCF conversion {fcf_conversion:.1f}× — normal")
            elif fcf_conversion >= 0:
                adj -= 5.0
                notes.append(f"FCF conversion {fcf_conversion:.1f}× — earnings not converting to cash")
            else:
                adj -= 4.0
                notes.append(f"FCF conversion {fcf_conversion:.1f}× — negative FCF")

        # ── Goodwill / acquisition discipline ────────────────────────
        if gw_pct_t is not None and gw_pct_tm1 is not None:
            delta_gw = gw_pct_t - gw_pct_tm1
            if delta_gw < -0.01:
                adj += 3.0
                notes.append(f"Goodwill/assets {gw_pct_t:.1%} (↓ {delta_gw:.1%}) — organic growth")
            elif delta_gw > 0.05:
                adj -= 5.0
                notes.append(f"Goodwill/assets {gw_pct_t:.1%} (↑ {delta_gw:.1%}) — acquisition-driven, discipline check needed")
        elif gw_pct_t is not None:
            notes.append(f"Goodwill/assets {gw_pct_t:.1%}")

        # ── Asset turnover trend ──────────────────────────────────────
        if asset_turn_t is not None and asset_turn_tm1 is not None and asset_turn_tm1 > 0:
            turn_chg = (asset_turn_t - asset_turn_tm1) / asset_turn_tm1
            if turn_chg > 0.10:
                adj += 2.0
                notes.append(f"Asset turns improving {turn_chg:+.1%}")
            elif turn_chg < -0.10:
                adj -= 2.0
                notes.append(f"Asset turns declining {turn_chg:+.1%}")

        score = max(10.0, min(90.0, base_score + adj))

        # ── Direction / materiality / confidence ──────────────────────
        if score >= 62:
            direction = Direction.POSITIVE
        elif score >= 45:
            direction = Direction.NEUTRAL
        else:
            direction = Direction.NEGATIVE

        if (roe is not None and (roe > 0.25 or roe < 0)) or abs(adj) >= 8:
            materiality = Materiality.HIGH
        elif abs(adj) >= 4 or roe is not None:
            materiality = Materiality.MEDIUM
        else:
            materiality = Materiality.LOW

        has_fcf = fcf_conversion is not None
        has_gw  = gw_pct_t is not None
        if roe is not None and has_fcf and has_gw:
            confidence = Confidence.HIGH
        elif roe is not None and (has_fcf or has_gw):
            confidence = Confidence.MEDIUM
        elif roe is not None:
            confidence = Confidence.LOW
        else:
            confidence = Confidence.LOW

        flags: list[QualityFlag] = []
        if roe is None:
            flags.append(QualityFlag.MISSING_FIELD)

        narrative = ". ".join(notes) + "."

        return self._emit(
            ticker=ticker, run_id=run_id, as_of=as_of,
            score=score,
            confidence=confidence,
            direction=direction,
            materiality=materiality,
            payload={
                "roe":               round(roe, 4) if roe is not None else None,
                "roa":               round(roa, 4) if roa is not None else None,
                "fcf_conversion":    round(fcf_conversion, 3) if fcf_conversion is not None else None,
                "goodwill_pct":      round(gw_pct_t, 4) if gw_pct_t is not None else None,
                "goodwill_pct_prior": round(gw_pct_tm1, 4) if gw_pct_tm1 is not None else None,
                "asset_turns":       round(asset_turn_t, 3) if asset_turn_t is not None else None,
                "base_score":        round(base_score, 2),
                "adjustment":        round(adj, 2),
                "narrative":         narrative,
            },
            evidence=[
                Evidence(
                    source_type="sec_filing",
                    source_name="yfinance (financials, balance sheet, cash flow)",
                    url_or_ref=f"yfinance://financials/{ticker}",
                    credibility_weight=0.85,
                    extracted_facts=notes,
                )
            ],
            quality_flags=flags,
        )
