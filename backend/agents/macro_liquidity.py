"""
agent.macro_liquidity — Macro regime, rates, inflation and liquidity assessment.
Uses yfinance proxy tickers (no external API key required).
Optional FRED_API_KEY in .env for higher-quality data.
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any

import yfinance as yf

from agents import BaseAgent
from schemas import (
    Confidence,
    Direction,
    Evidence,
    MacroPayload,
    MacroRegime,
    Materiality,
    QualityFlag,
)

logger = logging.getLogger(__name__)

# Sector sensitivity to macro factors
SECTOR_MACRO_SENSITIVITY: dict[str, dict[str, str]] = {
    "Technology": {"rate_sensitivity": "high", "usd_sensitivity": "medium", "recession_sensitivity": "high"},
    "Communication Services": {"rate_sensitivity": "high", "usd_sensitivity": "medium", "recession_sensitivity": "medium"},
    "Consumer Discretionary": {"rate_sensitivity": "high", "usd_sensitivity": "low", "recession_sensitivity": "high"},
    "Consumer Staples": {"rate_sensitivity": "medium", "usd_sensitivity": "low", "recession_sensitivity": "low"},
    "Energy": {"rate_sensitivity": "low", "usd_sensitivity": "high", "recession_sensitivity": "medium"},
    "Financials": {"rate_sensitivity": "high", "usd_sensitivity": "medium", "recession_sensitivity": "high"},
    "Health Care": {"rate_sensitivity": "low", "usd_sensitivity": "medium", "recession_sensitivity": "low"},
    "Industrials": {"rate_sensitivity": "medium", "usd_sensitivity": "high", "recession_sensitivity": "medium"},
    "Materials": {"rate_sensitivity": "medium", "usd_sensitivity": "high", "recession_sensitivity": "medium"},
    "Real Estate": {"rate_sensitivity": "high", "usd_sensitivity": "low", "recession_sensitivity": "medium"},
    "Utilities": {"rate_sensitivity": "high", "usd_sensitivity": "low", "recession_sensitivity": "low"},
    "Semiconductors": {"rate_sensitivity": "high", "usd_sensitivity": "high", "recession_sensitivity": "high"},
}

# Known upcoming macro events (rolling 90-day window — update periodically)
# Format: {"event": str, "date": "YYYY-MM-DD", "materiality": "high|medium"}
MACRO_CALENDAR: list[dict[str, str]] = [
    {"event": "FOMC Meeting", "date": "2026-06-11", "materiality": "high"},
    {"event": "FOMC Meeting", "date": "2026-07-30", "materiality": "high"},
    {"event": "FOMC Meeting", "date": "2026-09-17", "materiality": "high"},
    {"event": "US CPI Release", "date": "2026-05-13", "materiality": "high"},
    {"event": "US CPI Release", "date": "2026-06-10", "materiality": "high"},
    {"event": "US CPI Release", "date": "2026-07-15", "materiality": "high"},
    {"event": "US Jobs Report", "date": "2026-05-08", "materiality": "high"},
    {"event": "US Jobs Report", "date": "2026-06-05", "materiality": "high"},
    {"event": "US GDP (advance)", "date": "2026-04-30", "materiality": "medium"},
    {"event": "US GDP (advance)", "date": "2026-07-30", "materiality": "medium"},
]


def _fetch_yf_price(ticker_sym: str) -> float | None:
    try:
        info = yf.Ticker(ticker_sym).fast_info
        v = getattr(info, "last_price", None)
        if v and v == v:  # not NaN
            return float(v)
    except Exception:
        pass
    return None


def _fetch_yf_pct_change(ticker_sym: str, period: str = "5d") -> float | None:
    try:
        hist = yf.Ticker(ticker_sym).history(period=period)
        if hist.empty or len(hist) < 2:
            return None
        first = float(hist["Close"].iloc[0])
        last = float(hist["Close"].iloc[-1])
        if first > 0:
            return (last - first) / first
    except Exception:
        pass
    return None


class MacroLiquidityAgent(BaseAgent):
    agent_id = "agent.macro_liquidity"
    signal_type = "macro_regime"

    # ------------------------------------------------------------------
    # Regime detection (deterministic)
    # ------------------------------------------------------------------

    @staticmethod
    def _detect_regime(
        yield_curve: float | None,
        ten_year: float | None,
        vix: float | None,
        sp500_5d: float | None,
        unemp_rising: bool = False,
    ) -> str:
        if yield_curve is not None and yield_curve < -0.5 and unemp_rising:
            return MacroRegime.RECESSION_RISK.value
        if ten_year is not None and ten_year > 4.5 and yield_curve is not None and yield_curve > 0:
            return MacroRegime.RATE_PRESSURE.value
        if vix is not None and (vix > 25 or (sp500_5d is not None and sp500_5d < -0.02)):
            return MacroRegime.RISK_OFF.value
        if vix is not None and vix < 15 and (sp500_5d is not None and sp500_5d > 0.01):
            return MacroRegime.RISK_ON.value
        if ten_year is not None and ten_year < 3.5 and (vix is None or vix < 20):
            return MacroRegime.LIQUIDITY_SUPPORTIVE.value
        return MacroRegime.NEUTRAL.value

    @staticmethod
    def _regime_score(regime: str, yield_curve: float | None, vix: float | None) -> float:
        base = {
            MacroRegime.RISK_ON.value: 75.0,
            MacroRegime.LIQUIDITY_SUPPORTIVE.value: 68.0,
            MacroRegime.NEUTRAL.value: 50.0,
            MacroRegime.RATE_PRESSURE.value: 35.0,
            MacroRegime.RISK_OFF.value: 25.0,
            MacroRegime.RECESSION_RISK.value: 15.0,
        }.get(regime, 50.0)

        # Yield curve adjustment
        if yield_curve is not None:
            if yield_curve > 1.0:
                base += 8
            elif yield_curve > 0:
                base += 4
            elif yield_curve < -1.0:
                base -= 10
            elif yield_curve < 0:
                base -= 5

        # VIX adjustment
        if vix is not None:
            if vix < 12:
                base += 5
            elif vix > 30:
                base -= 5

        return round(max(0.0, min(100.0, base)), 2)

    @staticmethod
    def _usd_pressure(usd_5d: float | None) -> str:
        if usd_5d is None:
            return "medium"
        if usd_5d > 0.01:
            return "high"
        if usd_5d < -0.01:
            return "low"
        return "medium"

    # ------------------------------------------------------------------
    # Upcoming events filter
    # ------------------------------------------------------------------

    @staticmethod
    def _upcoming_events(days: int = 21) -> list[dict[str, str]]:
        today = datetime.now(timezone.utc).date()
        cutoff = today + timedelta(days=days)
        return [
            e for e in MACRO_CALENDAR
            if today <= datetime.strptime(e["date"], "%Y-%m-%d").date() <= cutoff
        ]

    # ------------------------------------------------------------------
    # Core run  (ticker param used only for sector sensitivity lookup)
    # ------------------------------------------------------------------

    def _run(self, ticker: str, run_id: str, as_of: datetime):
        import db
        ticker_info = db.get_ticker_info(ticker) or {}
        sector = ticker_info.get("sector", "")

        # ---- Fetch macro proxies via yfinance ----
        ten_year = _fetch_yf_price("^TNX")       # 10-year yield (%)
        two_year = _fetch_yf_price("^IRX")        # 13w T-bill ~= short rate
        vix = _fetch_yf_price("^VIX")
        sp500_5d = _fetch_yf_pct_change("^GSPC", "5d")
        usd_5d = _fetch_yf_pct_change("DX-Y.NYB", "5d")

        # Yield curve 2s10s approximation
        yield_curve: float | None = None
        if ten_year is not None and two_year is not None:
            # ^IRX is the discount rate (annualised). Approximate 2yr equivalent.
            yield_curve = round(ten_year - two_year, 3)

        # ---- FRED optional enrichment ----
        fred_data: dict[str, Any] = {}
        fred_key = os.getenv("FRED_API_KEY")
        if fred_key:
            try:
                fred_data = self._fetch_fred(fred_key)
            except Exception as exc:
                logger.debug("FRED fetch failed: %s", exc)

        fed_rate = fred_data.get("FEDFUNDS") or two_year
        inflation_yoy = fred_data.get("CPIAUCSL_YOY")
        unemployment = fred_data.get("UNRATE")
        pmi = fred_data.get("ISM_PMI")

        regime = self._detect_regime(
            yield_curve=yield_curve,
            ten_year=ten_year,
            vix=vix,
            sp500_5d=sp500_5d,
            unemp_rising=False,  # conservative default without FRED trend data
        )
        score = self._regime_score(regime, yield_curve, vix)
        usd_pressure = self._usd_pressure(usd_5d)
        sector_sensitivity = SECTOR_MACRO_SENSITIVITY.get(sector, {})
        upcoming = self._upcoming_events()

        # ---- Confidence ----
        fields = sum(v is not None for v in [ten_year, vix, sp500_5d, yield_curve])
        confidence = Confidence.HIGH if fields >= 3 else (Confidence.MEDIUM if fields >= 2 else Confidence.LOW)

        # ---- Flags ----
        flags: list[QualityFlag] = []
        if ten_year is None and vix is None:
            flags.append(QualityFlag.MISSING_FIELD)

        # ---- Direction ----
        if regime in (MacroRegime.RISK_ON.value, MacroRegime.LIQUIDITY_SUPPORTIVE.value):
            direction = Direction.POSITIVE
        elif regime in (MacroRegime.RISK_OFF.value, MacroRegime.RECESSION_RISK.value):
            direction = Direction.NEGATIVE
        elif regime == MacroRegime.RATE_PRESSURE.value:
            direction = Direction.NEGATIVE if sector_sensitivity.get("rate_sensitivity") == "high" else Direction.NEUTRAL
        else:
            direction = Direction.NEUTRAL

        # ---- Materiality ----
        if regime in (MacroRegime.RECESSION_RISK.value, MacroRegime.RISK_OFF.value):
            materiality = Materiality.CRITICAL
        elif regime == MacroRegime.RATE_PRESSURE.value:
            materiality = Materiality.HIGH
        else:
            materiality = Materiality.MEDIUM

        evidence = [
            Evidence.from_macro("yfinance macro proxies", [
                f"10Y yield: {ten_year:.2f}%" if ten_year else "10Y yield: N/A",
                f"VIX: {vix:.1f}" if vix else "VIX: N/A",
                f"Yield curve 2s10s: {yield_curve:.2f}%" if yield_curve else "Yield curve: N/A",
                f"S&P 500 5d return: {sp500_5d*100:.2f}%" if sp500_5d else "SPX 5d: N/A",
                f"USD 5d change: {usd_5d*100:.2f}%" if usd_5d else "USD 5d: N/A",
                f"Regime: {regime}",
            ]),
        ]

        payload = MacroPayload(
            macro_regime=regime,
            fed_rate=fed_rate,
            ten_year_yield=ten_year,
            yield_curve_2s10s=yield_curve,
            inflation_yoy=inflation_yoy,
            unemployment_rate=unemployment,
            pmi=pmi,
            vix=vix,
            usd_pressure=usd_pressure,
            sector_sensitivity=sector_sensitivity,
            upcoming_events=upcoming,
        ).model_dump()

        return self._emit(
            ticker=ticker,
            run_id=run_id,
            as_of=as_of,
            score=score,
            confidence=confidence,
            direction=direction,
            materiality=materiality,
            payload=payload,
            evidence=evidence,
            quality_flags=flags,
        )

    @staticmethod
    def _fetch_fred(api_key: str) -> dict[str, Any]:
        """Fetch key FRED series. Returns dict of series_id -> latest value."""
        import httpx
        BASE = "https://api.stlouisfed.org/fred/series/observations"
        series = {
            "FEDFUNDS": "FEDFUNDS",
            "CPIAUCSL": "CPIAUCSL",
            "UNRATE": "UNRATE",
        }
        result: dict[str, Any] = {}
        for name, sid in series.items():
            try:
                r = httpx.get(BASE, params={
                    "series_id": sid,
                    "api_key": api_key,
                    "file_type": "json",
                    "sort_order": "desc",
                    "limit": 13,  # enough for YoY calc
                }, timeout=8)
                obs = r.json().get("observations", [])
                if obs:
                    val = obs[0].get("value", ".")
                    if val != ".":
                        result[name] = float(val)
                    # CPI YoY
                    if sid == "CPIAUCSL" and len(obs) >= 13:
                        curr = float(obs[0]["value"]) if obs[0]["value"] != "." else None
                        prev = float(obs[12]["value"]) if obs[12]["value"] != "." else None
                        if curr and prev and prev != 0:
                            result["CPIAUCSL_YOY"] = round((curr - prev) / prev * 100, 2)
            except Exception:
                pass
        return result
