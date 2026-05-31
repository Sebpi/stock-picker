"""
agent.dividend_quality — Dividend sustainability and total shareholder yield.

Evaluates dividend health and capital return quality — not just yield, but
whether the dividend is well-covered, growing, and whether buybacks add to
total shareholder yield. A high-yield unsustainable dividend is a value trap;
a modest but growing dividend backed by strong FCF is a quality signal.

─── Signals ─────────────────────────────────────────────────────────────────────
1. Dividend yield — trailing annual dividend / price
2. Payout sustainability — payout ratio (dividends / earnings); also FCF coverage
3. Dividend consistency — 5-year average yield vs current (has it been maintained?)
4. Buyback yield — estimated net share repurchase rate (shares shrinkage / year)
5. Total shareholder yield — dividend yield + buyback yield

─── Scoring ─────────────────────────────────────────────────────────────────────
Non-payers receive a neutral base score (52) — absence of dividend is neither
good nor bad; buyback yield may still produce positive adjustment.

Dividend yield tier:
  0 (no dividend)      → 52 base
  0–1%                 → 54
  1–2%                 → 60
  2–4%                 → 65  (sweet spot: meaningful yield, still growing)
  4–6%                 → 58  (high yield — check sustainability)
  > 6%                 → 44  (danger zone — likely unsustainable or sector distress)

Payout ratio adjustment:
  < 30%  → +6  (very well-covered; room to grow)
  30–50% → +3
  50–75% → 0   (normal)
  75–90% → -5  (stretched)
  > 90%  → -10 (unsustainable — dividend at risk)
  Negative payout (loss-making) → -8

Buyback yield adjustment:
  Buyback yield > 3%  → +5  (significant capital return)
  Buyback yield 1–3%  → +3
  Shares being issued → -3  (dilution, opposite of buyback)

FCF coverage of dividend:
  FCF > 2× dividend payments → +3  (very well-covered by cash)
  FCF < 0                    → -5  (dividend funded by debt/equity issuance)

─── Horizon relevance ───────────────────────────────────────────────────────────
Dividend quality is a long-duration quality signal.
Weight: 3m 0.03 / 6m 0.05 / 12m 0.07
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


class DividendQualityAgent(BaseAgent):
    agent_id = "agent.dividend_quality"
    signal_type = "dividend_quality"
    default_horizons = ["3m", "6m", "12m"]

    def _run(self, ticker: str, run_id: str, as_of: datetime):
        ticker = ticker.upper()
        t = yf.Ticker(ticker)

        info = self._timed_fetch(lambda: t.info, f"{ticker}/info") or {}
        bs   = self._timed_fetch(lambda: t.balance_sheet, f"{ticker}/balance_sheet")
        cf   = self._timed_fetch(lambda: t.cashflow, f"{ticker}/cashflow")

        # ── Dividend metrics ──────────────────────────────────────────
        div_yield = self._safe_get(info, "dividendYield") or self._safe_get(info, "trailingAnnualDividendYield")
        payout_ratio = self._safe_get(info, "payoutRatio")
        div_rate     = self._safe_get(info, "dividendRate")
        five_yr_yield = self._safe_get(info, "fiveYearAvgDividendYield")

        if div_yield is not None:
            div_yield = float(div_yield)
            # yfinance sometimes returns yield as decimal (0.025) or percent (2.5) — normalise
            if div_yield > 1.0:
                div_yield /= 100.0

        # ── Shares outstanding (for buyback yield) ────────────────────
        shares_t: float | None = None
        shares_tm1: float | None = None
        if bs is not None and not bs.empty:
            shares_t   = _row(bs,  "Ordinary Shares Number", "Share Issued")
            shares_tm1 = _row2(bs, "Ordinary Shares Number", "Share Issued")

        buyback_yield: float | None = None
        if shares_t is not None and shares_tm1 is not None and shares_tm1 > 0:
            buyback_yield = (shares_tm1 - shares_t) / shares_tm1  # positive = buyback

        # ── FCF coverage of dividends ─────────────────────────────────
        fcf: float | None = None
        div_paid: float | None = None
        if cf is not None and not cf.empty:
            fcf = _row(cf, "Free Cash Flow", "Operating Cash Flow")
            div_paid = _row(cf, "Cash Dividends Paid", "Payment Of Dividends",
                            "Common Stock Dividend Paid")
            if div_paid is not None:
                div_paid = abs(float(div_paid))

        fcf_coverage: float | None = None
        if fcf is not None and div_paid is not None and div_paid > 0:
            fcf_coverage = fcf / div_paid
        elif fcf is not None and (div_yield is None or div_yield == 0):
            fcf_coverage = None  # no dividend to cover

        # ── Base score from yield ─────────────────────────────────────
        notes: list[str] = []
        is_payer = div_yield is not None and div_yield > 0.001

        if not is_payer:
            base_score = 52.0
            notes.append("Non-dividend paying stock")
        elif div_yield > 0.06:
            base_score = 44.0
            notes.append(f"Dividend yield {div_yield:.1%} — elevated, check sustainability")
        elif div_yield > 0.04:
            base_score = 58.0
            notes.append(f"Dividend yield {div_yield:.1%} — high yield")
        elif div_yield > 0.02:
            base_score = 65.0
            notes.append(f"Dividend yield {div_yield:.1%} — healthy yield")
        elif div_yield > 0.01:
            base_score = 60.0
            notes.append(f"Dividend yield {div_yield:.1%}")
        else:
            base_score = 54.0
            notes.append(f"Dividend yield {div_yield:.1%} — token dividend")

        adj = 0.0

        # ── Payout ratio ──────────────────────────────────────────────
        if payout_ratio is not None:
            pr = float(payout_ratio)
            if pr < 0:
                adj -= 8.0
                notes.append(f"Negative payout ratio — dividend paid from losses")
            elif pr < 0.30:
                adj += 6.0
                notes.append(f"Payout ratio {pr:.0%} — very well covered, room to grow")
            elif pr < 0.50:
                adj += 3.0
                notes.append(f"Payout ratio {pr:.0%} — healthy coverage")
            elif pr < 0.75:
                notes.append(f"Payout ratio {pr:.0%} — normal range")
            elif pr < 0.90:
                adj -= 5.0
                notes.append(f"Payout ratio {pr:.0%} — stretched, limited growth capacity")
            else:
                adj -= 10.0
                notes.append(f"Payout ratio {pr:.0%} — unsustainable, dividend at risk")

        # ── FCF coverage ──────────────────────────────────────────────
        if fcf is not None and is_payer:
            if fcf < 0:
                adj -= 5.0
                notes.append("Negative FCF — dividend funded by debt or equity")
            elif fcf_coverage is not None and fcf_coverage > 2.0:
                adj += 3.0
                notes.append(f"FCF covers dividend {fcf_coverage:.1f}× — strong cash backing")

        # ── Buyback yield ─────────────────────────────────────────────
        if buyback_yield is not None:
            if buyback_yield > 0.03:
                adj += 5.0
                notes.append(f"Buyback yield {buyback_yield:.1%} — significant capital return")
            elif buyback_yield > 0.01:
                adj += 3.0
                notes.append(f"Buyback yield {buyback_yield:.1%}")
            elif buyback_yield < -0.02:
                adj -= 3.0
                notes.append(f"Share dilution {buyback_yield:.1%} — value transfer to insiders")

        # Total shareholder yield
        total_yield: float | None = None
        if div_yield is not None and buyback_yield is not None:
            total_yield = div_yield + buyback_yield
            if total_yield > 0.05:
                notes.append(f"Total shareholder yield {total_yield:.1%} (dividend + buyback)")

        score = max(10.0, min(90.0, base_score + adj))

        # ── Direction / materiality / confidence ──────────────────────
        if score >= 62:
            direction = Direction.POSITIVE
        elif score >= 45:
            direction = Direction.NEUTRAL
        else:
            direction = Direction.NEGATIVE

        if (payout_ratio is not None and payout_ratio > 0.90) or (fcf is not None and fcf < 0 and is_payer):
            materiality = Materiality.HIGH
        elif abs(adj) >= 6:
            materiality = Materiality.MEDIUM
        else:
            materiality = Materiality.LOW

        has_coverage = payout_ratio is not None or fcf_coverage is not None
        if is_payer and has_coverage and buyback_yield is not None:
            confidence = Confidence.HIGH
        elif is_payer and has_coverage:
            confidence = Confidence.MEDIUM
        elif is_payer:
            confidence = Confidence.LOW
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
                "dividend_yield":    round(div_yield, 4)      if div_yield      is not None else None,
                "payout_ratio":      round(float(payout_ratio), 3) if payout_ratio is not None else None,
                "fcf_coverage":      round(fcf_coverage, 2)   if fcf_coverage   is not None else None,
                "buyback_yield":     round(buyback_yield, 4)  if buyback_yield  is not None else None,
                "total_yield":       round(total_yield, 4)    if total_yield    is not None else None,
                "five_yr_avg_yield": float(five_yr_yield)     if five_yr_yield  is not None else None,
                "base_score":        round(base_score, 2),
                "adjustment":        round(adj, 2),
                "narrative":         narrative,
            },
            evidence=[
                Evidence(
                    source_type="market_data",
                    source_name="yfinance (dividend metrics, balance sheet)",
                    url_or_ref=f"yfinance://info/{ticker}",
                    credibility_weight=0.75,
                    extracted_facts=notes,
                )
            ],
            quality_flags=[],
        )
