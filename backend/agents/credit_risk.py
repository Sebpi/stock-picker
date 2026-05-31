"""
agent.credit_risk — Corporate credit environment + company leverage profile.

Two orthogonal signals combined into one score:

─── 1. Credit environment (macro credit spread conditions) ─────────────────────
Uses HYG (iShares HY Corp Bond ETF) and LQD (iShares IG Corp Bond ETF) 30-day
price momentum as spread proxies. When credit spreads tighten (ETFs rise), the
financing environment is supportive for equities. When spreads widen (ETFs fall),
credit stress precedes equity stress by 2–6 weeks.

If FRED_API_KEY is set, actual OAS spread values are used instead (more precise):
  BAMLH0A0HYM2  — ICE BofA US HY Option-Adjusted Spread (%)
  BAMLC0A0CM    — ICE BofA US IG Corporate Bond OAS (%)

Normal ranges:
  HY OAS < 4%  → tight / benign  (bullish)
  HY OAS 4–7%  → normal range
  HY OAS > 8%  → stress / recessionary fear (bearish)
  IG OAS < 1%  → tight
  IG OAS > 2%  → elevated

─── 2. Company leverage profile ────────────────────────────────────────────────
A highly-leveraged company (HY-like) is far more exposed to credit spread
widening than a strong IG company. Classification uses:
  - Debt/Equity ratio        (D/E ≥ 2.0 → HY-like)
  - Interest coverage proxy  (EBIT / interest expense; < 3× → HY-like)
  - Current ratio            (< 1.0 → liquidity concern)
  - Free cash flow           (negative FCF amplifies credit risk)

─── Scoring ────────────────────────────────────────────────────────────────────
Base score from credit environment (60–70 good, 40–50 neutral, 20–35 stressed):

  FRED HY OAS < 3.5%   → 70  (historically tight — very supportive)
  FRED HY OAS 3.5–5%   → 62
  FRED HY OAS 5–7%     → 52
  FRED HY OAS 7–9%     → 40
  FRED HY OAS > 9%     → 28  (crisis-level spreads)

  Without FRED (ETF proxy):
  HYG 30d > +1%, LQD 30d ≥ 0%    → 65  (both solid — full tightening)
  HYG 30d > 0%,  LQD 30d ≥ 0%    → 58
  Mixed signal (one up, one down) → 50
  HYG 30d < 0%,  LQD 30d < 0%    → 40
  HYG 30d < -2%, LQD 30d < -1%   → 30  (clear spread widening)

Leverage adjustment (applied after environment base):
  IG-like company (D/E < 0.5, coverage > 10×)    → +5
  Moderate leverage (D/E 0.5–1.5)                → 0
  Elevated leverage (D/E 1.5–3.0 or cov 3–6×)   → −8
  High leverage (D/E > 3.0 or cov < 3×)         → −15 (credit stress amplified)
  Negative FCF on top of high leverage           → additional −5

─── Horizon relevance ──────────────────────────────────────────────────────────
Credit spreads lead equity by 2–8 weeks; most actionable at 3m.
Weight: 3m 0.08 / 6m 0.06 / 12m 0.04
"""
from __future__ import annotations

import logging
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from agents import BaseAgent
from schemas import Confidence, Direction, Evidence, Materiality, QualityFlag

import yfinance as yf

logger = logging.getLogger(__name__)

# Proxy ETF tickers
_HYG = "HYG"
_LQD = "LQD"

# FRED series for actual OAS spreads (optional — needs FRED_API_KEY)
_FRED_HY = "BAMLH0A0HYM2"
_FRED_IG = "BAMLC0A0CM"


def _pct_return(ticker: str, days: int = 30) -> float | None:
    try:
        end = datetime.utcnow()
        start = end - timedelta(days=days + 10)
        hist = yf.download(ticker, start=start.strftime("%Y-%m-%d"),
                           end=end.strftime("%Y-%m-%d"), progress=False, auto_adjust=True)
        if hist is None or hist.empty or len(hist) < 5:
            return None
        close = hist["Close"].dropna()
        if len(close) < 5:
            return None
        return float((close.iloc[-1] - close.iloc[0]) / close.iloc[0])
    except Exception as exc:
        logger.debug("_pct_return %s: %s", ticker, exc)
        return None


def _fred_oas(series_id: str, api_key: str) -> float | None:
    """Fetch the most recent OAS value from FRED."""
    try:
        import urllib.request, json
        url = (
            f"https://api.stlouisfed.org/fred/series/observations"
            f"?series_id={series_id}&api_key={api_key}"
            f"&sort_order=desc&limit=5&file_type=json"
        )
        with urllib.request.urlopen(url, timeout=8) as r:  # nosec B310 — URL is constructed from hardcoded FRED base, not user input
            data = json.loads(r.read())
        for obs in data.get("observations", []):
            val = obs.get("value", ".")
            if val != ".":
                return float(val)
    except Exception as exc:
        logger.debug("_fred_oas %s: %s", series_id, exc)
    return None


class CreditRiskAgent(BaseAgent):
    agent_id = "agent.credit_risk"
    signal_type = "credit_risk"
    default_horizons = ["3m", "6m", "12m"]

    def _run(self, ticker: str, run_id: str, as_of: datetime):
        ticker = ticker.upper()

        # ── 1. Credit environment ─────────────────────────────────────
        fred_key = os.getenv("FRED_API_KEY")
        hy_oas: float | None = None
        ig_oas: float | None = None
        hyg_30d: float | None = None
        lqd_30d: float | None = None
        env_source = "etf_proxy"

        if fred_key:
            hy_oas = self._timed_fetch(lambda: _fred_oas(_FRED_HY, fred_key), "FRED/HY_OAS", timeout=10.0)
            ig_oas = self._timed_fetch(lambda: _fred_oas(_FRED_IG, fred_key), "FRED/IG_OAS", timeout=10.0)
            if hy_oas is not None:
                env_source = "FRED_OAS"

        if hy_oas is None:
            hyg_30d = self._timed_fetch(lambda: _pct_return(_HYG), "HYG/price")
            lqd_30d = self._timed_fetch(lambda: _pct_return(_LQD), "LQD/price")

        # ── 2. Company leverage ───────────────────────────────────────
        info = self._timed_fetch(lambda: yf.Ticker(ticker).info, f"{ticker}/info") or {}
        dte       = self._safe_get(info, "debtToEquity")        # ratio × 100 in yfinance
        curr_r    = self._safe_get(info, "currentRatio")
        fcf       = self._safe_get(info, "freeCashflow")
        ebit      = self._safe_get(info, "ebit")
        int_exp   = self._safe_get(info, "interestExpense")     # usually positive in yfinance

        # Normalise D/E (yfinance returns × 100 in some versions)
        dte_norm: float | None = None
        if dte is not None:
            dte_norm = abs(float(dte)) / 100 if abs(float(dte)) > 20 else abs(float(dte))

        # Interest coverage = EBIT / |interest expense|
        coverage: float | None = None
        if ebit is not None and int_exp is not None and int_exp != 0:
            coverage = float(ebit) / abs(float(int_exp))

        # ── Environment base score ────────────────────────────────────
        env_notes: list[str] = []
        if env_source == "FRED_OAS" and hy_oas is not None:
            if hy_oas < 3.5:
                env_base, env_label = 70.0, f"HY OAS {hy_oas:.2f}% — historically tight"
            elif hy_oas < 5.0:
                env_base, env_label = 62.0, f"HY OAS {hy_oas:.2f}% — normal"
            elif hy_oas < 7.0:
                env_base, env_label = 52.0, f"HY OAS {hy_oas:.2f}% — mildly elevated"
            elif hy_oas < 9.0:
                env_base, env_label = 40.0, f"HY OAS {hy_oas:.2f}% — elevated stress"
            else:
                env_base, env_label = 28.0, f"HY OAS {hy_oas:.2f}% — crisis-level spreads"
            if ig_oas is not None:
                env_notes.append(f"IG OAS {ig_oas:.2f}%")
        elif hyg_30d is not None and lqd_30d is not None:
            if hyg_30d > 0.01 and lqd_30d >= 0:
                env_base, env_label = 65.0, f"HYG +{hyg_30d:.1%} / LQD +{lqd_30d:.1%} — spreads tightening"
            elif hyg_30d > 0 and lqd_30d >= -0.005:
                env_base, env_label = 58.0, f"HYG +{hyg_30d:.1%} / LQD {lqd_30d:.1%} — mildly supportive"
            elif (hyg_30d > 0) != (lqd_30d > 0):
                env_base, env_label = 50.0, f"HYG {hyg_30d:.1%} / LQD {lqd_30d:.1%} — mixed signal"
            elif hyg_30d < -0.02 and lqd_30d < -0.01:
                env_base, env_label = 30.0, f"HYG {hyg_30d:.1%} / LQD {lqd_30d:.1%} — spreads widening"
            else:
                env_base, env_label = 40.0, f"HYG {hyg_30d:.1%} / LQD {lqd_30d:.1%} — mild stress"
        elif hyg_30d is not None:
            env_base = 55.0 + hyg_30d * 200  # rough linear mapping
            env_base = max(30.0, min(70.0, env_base))
            env_label = f"HYG {hyg_30d:.1%} (LQD unavailable)"
        else:
            env_base = 52.0
            env_label = "Credit environment data unavailable — assuming neutral"

        # ── Leverage adjustment ───────────────────────────────────────
        lev_adj = 0.0
        lev_notes: list[str] = []

        if dte_norm is not None:
            if dte_norm < 0.5:
                lev_adj += 5.0
                lev_notes.append(f"Low leverage D/E {dte_norm:.2f}")
            elif dte_norm < 1.5:
                lev_notes.append(f"Moderate leverage D/E {dte_norm:.2f}")
            elif dte_norm < 3.0:
                lev_adj -= 8.0
                lev_notes.append(f"Elevated leverage D/E {dte_norm:.2f}")
            else:
                lev_adj -= 15.0
                lev_notes.append(f"High leverage D/E {dte_norm:.2f} — credit-sensitive")

        if coverage is not None:
            if coverage < 3.0:
                lev_adj -= 10.0
                lev_notes.append(f"Weak interest coverage {coverage:.1f}×")
            elif coverage < 6.0:
                lev_adj -= 4.0
                lev_notes.append(f"Moderate interest coverage {coverage:.1f}×")
            elif coverage > 10.0:
                lev_adj += 3.0
                lev_notes.append(f"Strong interest coverage {coverage:.1f}×")

        if curr_r is not None and curr_r < 1.0:
            lev_adj -= 5.0
            lev_notes.append(f"Current ratio {curr_r:.2f} — liquidity concern")

        if fcf is not None and fcf < 0 and dte_norm is not None and dte_norm > 1.5:
            lev_adj -= 5.0
            lev_notes.append("Negative FCF combined with high leverage")

        score = max(10.0, min(90.0, env_base + lev_adj))

        # ── Direction / materiality / confidence ─────────────────────
        if score >= 62:
            direction = Direction.POSITIVE
        elif score >= 48:
            direction = Direction.NEUTRAL
        else:
            direction = Direction.NEGATIVE

        has_spread_data = hy_oas is not None or hyg_30d is not None
        has_leverage    = dte_norm is not None or coverage is not None

        if not has_spread_data:
            materiality = Materiality.LOW
        elif abs(lev_adj) >= 10 or (hy_oas is not None and hy_oas > 7):
            materiality = Materiality.HIGH
        elif abs(lev_adj) >= 5 or (hy_oas is not None and hy_oas > 5):
            materiality = Materiality.MEDIUM
        else:
            materiality = Materiality.LOW

        if env_source == "FRED_OAS" and has_leverage:
            confidence = Confidence.HIGH
        elif has_spread_data and has_leverage:
            confidence = Confidence.MEDIUM
        elif has_spread_data or has_leverage:
            confidence = Confidence.LOW
        else:
            confidence = Confidence.LOW

        flags: list[QualityFlag] = []
        if not has_spread_data:
            flags.append(QualityFlag.LOW_COVERAGE)

        narrative_parts = [env_label] + env_notes + lev_notes
        narrative = ". ".join(narrative_parts) + "."

        evidence = [
            Evidence(
                source_type="market_data",
                source_name="FRED OAS spreads" if env_source == "FRED_OAS" else "yfinance (HYG/LQD ETF proxies)",
                url_or_ref="https://fred.stlouisfed.org" if env_source == "FRED_OAS" else "yfinance://HYG,LQD",
                credibility_weight=0.85 if env_source == "FRED_OAS" else 0.60,
                extracted_facts=[env_label] + env_notes,
            ),
        ]
        if lev_notes:
            evidence.append(
                Evidence(
                    source_type="sec_filing",
                    source_name="yfinance (company leverage ratios)",
                    url_or_ref=f"yfinance://info/{ticker}",
                    credibility_weight=0.80,
                    extracted_facts=lev_notes,
                )
            )

        return self._emit(
            ticker=ticker, run_id=run_id, as_of=as_of,
            score=score,
            confidence=confidence,
            direction=direction,
            materiality=materiality,
            payload={
                "env_source":       env_source,
                "hy_oas":           round(hy_oas, 3) if hy_oas is not None else None,
                "ig_oas":           round(ig_oas, 3) if ig_oas is not None else None,
                "hyg_30d_pct":      round(hyg_30d, 4) if hyg_30d is not None else None,
                "lqd_30d_pct":      round(lqd_30d, 4) if lqd_30d is not None else None,
                "debt_to_equity":   round(dte_norm, 3) if dte_norm is not None else None,
                "interest_coverage": round(coverage, 2) if coverage is not None else None,
                "current_ratio":    round(curr_r, 2) if curr_r is not None else None,
                "env_base_score":   round(env_base, 2),
                "leverage_adj":     round(lev_adj, 2),
                "narrative":        narrative,
            },
            evidence=evidence,
            quality_flags=flags,
        )
