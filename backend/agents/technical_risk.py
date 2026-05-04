"""
agent.technical_risk — Price action, momentum and downside risk assessment.
Scores 0-100; higher = stronger technical setup.
"""
from __future__ import annotations

import logging
import math
from datetime import datetime
from typing import Any

import numpy as np
import yfinance as yf

from agents import BaseAgent
from schemas import (
    Confidence,
    Direction,
    Evidence,
    Materiality,
    QualityFlag,
    TechnicalPayload,
)

logger = logging.getLogger(__name__)


def _safe_float(val: Any) -> float | None:
    try:
        v = float(val)
        return None if math.isnan(v) or math.isinf(v) else v
    except (TypeError, ValueError):
        return None


class TechnicalRiskAgent(BaseAgent):
    agent_id = "agent.technical_risk"
    signal_type = "technical_risk_setup"
    default_horizons = ["3m", "6m"]  # technicals matter less at 12m

    # ------------------------------------------------------------------
    # Indicator calculations (all deterministic, unit-testable)
    # ------------------------------------------------------------------

    @staticmethod
    def _rsi(closes: list[float], period: int = 14) -> float | None:
        if len(closes) < period + 1:
            return None
        gains, losses = [], []
        for i in range(1, period + 1):
            delta = closes[-(period + 1 - i)] - closes[-(period + 2 - i)]
            (gains if delta >= 0 else losses).append(abs(delta))
        avg_gain = sum(gains) / period if gains else 0.0
        avg_loss = sum(losses) / period if losses else 0.0
        if avg_loss == 0:
            return 100.0
        rs = avg_gain / avg_loss
        return round(100 - (100 / (1 + rs)), 2)

    @staticmethod
    def _sma(closes: list[float], period: int) -> float | None:
        if len(closes) < period:
            return None
        return round(sum(closes[-period:]) / period, 4)

    @staticmethod
    def _atr(highs: list[float], lows: list[float],
              closes: list[float], period: int = 14) -> float | None:
        if len(closes) < period + 1:
            return None
        trs: list[float] = []
        for i in range(1, period + 1):
            idx = -(period + 1 - i)
            h, l, pc = highs[idx], lows[idx], closes[idx - 1]
            trs.append(max(h - l, abs(h - pc), abs(l - pc)))
        return round(sum(trs) / period, 4)

    @staticmethod
    def _volatility_30d(closes: list[float]) -> float | None:
        if len(closes) < 31:
            return None
        log_returns = [math.log(closes[i] / closes[i - 1]) for i in range(-30, 0)]
        mean = sum(log_returns) / len(log_returns)
        variance = sum((r - mean) ** 2 for r in log_returns) / (len(log_returns) - 1)
        return round(math.sqrt(variance) * math.sqrt(252) * 100, 2)

    @staticmethod
    def _max_drawdown(closes: list[float], window: int = 60) -> float | None:
        if len(closes) < 2:
            return None
        subset = closes[-window:]
        peak = subset[0]
        max_dd = 0.0
        for c in subset:
            peak = max(peak, c)
            dd = (c - peak) / peak * 100
            max_dd = min(max_dd, dd)
        return round(max_dd, 2)

    @staticmethod
    def _support_resistance(closes: list[float], window: int = 60,
                             n: int = 3) -> tuple[list[float], list[float]]:
        """Return local minima (support) and maxima (resistance) from last window closes."""
        subset = closes[-window:]
        supports, resistances = [], []
        for i in range(1, len(subset) - 1):
            if subset[i] < subset[i - 1] and subset[i] < subset[i + 1]:
                supports.append(round(subset[i], 2))
            elif subset[i] > subset[i - 1] and subset[i] > subset[i + 1]:
                resistances.append(round(subset[i], 2))
        # Return the n most recent, deduplicated within 1%
        def dedup(levels: list[float]) -> list[float]:
            result: list[float] = []
            for lvl in reversed(levels):
                if not any(abs(lvl - r) / r < 0.01 for r in result):
                    result.append(lvl)
                if len(result) >= n:
                    break
            return result
        return dedup(supports), dedup(resistances)

    @staticmethod
    def _trend_label(price: float, ma20: float | None, ma50: float | None,
                     ma200: float | None, rsi: float | None,
                     vol_ratio: float | None) -> str:
        if None in (ma50, ma200, rsi):
            return "unknown"
        assert ma50 is not None and ma200 is not None and rsi is not None
        if price > ma200 and price > ma50 and rsi > 50:
            if rsi > 72 and price > ma50 * 1.15:
                return "extended"
            return "uptrend"
        if price < ma200 and price < ma50 and rsi < 50:
            return "downtrend"
        if ma50 and price > ma50 * 1.02 and (vol_ratio or 1.0) > 1.3:
            return "breakout"
        if ma50 and price < ma50 * 0.98:
            return "breakdown"
        if ma20 and abs(price - ma20) / ma20 < 0.02:
            return "range"
        return "range"

    # ------------------------------------------------------------------
    # Scoring rubric
    # ------------------------------------------------------------------

    @staticmethod
    def _score_trend(trend: str) -> int:
        return {"uptrend": 30, "breakout": 25, "range": 15,
                "extended": 10, "breakdown": 5, "downtrend": 0}.get(trend, 10)

    @staticmethod
    def _score_rsi(rsi: float | None) -> int:
        if rsi is None:
            return 10
        if 45 <= rsi <= 65:
            return 25
        if 35 <= rsi < 45 or 65 < rsi <= 75:
            return 15
        if 25 <= rsi < 35 or 75 < rsi <= 80:
            return 8
        return 3

    @staticmethod
    def _score_ma_stack(price: float, ma50: float | None, ma200: float | None) -> int:
        if ma50 is None or ma200 is None:
            return 10
        if price > ma50 > ma200:
            return 25
        if price < ma50 < ma200:
            return 0
        return 15

    @staticmethod
    def _score_drawdown(dd: float | None) -> int:
        if dd is None:
            return 10
        dd = abs(dd)
        if dd < 5:
            return 20
        if dd < 15:
            return 15
        if dd < 25:
            return 8
        return 2

    # ------------------------------------------------------------------
    # Core run
    # ------------------------------------------------------------------

    def _run(self, ticker: str, run_id: str, as_of: datetime):
        hist = yf.Ticker(ticker).history(period="1y", interval="1d")
        if hist.empty or len(hist) < 20:
            return self._emit(
                ticker=ticker, run_id=run_id, as_of=as_of,
                score=50.0,
                confidence=Confidence.LOW,
                direction=Direction.NEUTRAL,
                materiality=Materiality.LOW,
                payload=TechnicalPayload().model_dump(),
                quality_flags=[QualityFlag.MISSING_FIELD],
            )

        closes = [_safe_float(c) for c in hist["Close"].tolist()]
        closes = [c for c in closes if c is not None]
        highs = [_safe_float(h) or 0.0 for h in hist["High"].tolist()]
        lows = [_safe_float(l) or 0.0 for l in hist["Low"].tolist()]
        volumes = [_safe_float(v) or 0.0 for v in hist["Volume"].tolist()]

        price = closes[-1] if closes else None
        if not price:
            return self._emit(
                ticker=ticker, run_id=run_id, as_of=as_of,
                score=50.0, confidence=Confidence.LOW,
                direction=Direction.NEUTRAL, materiality=Materiality.LOW,
                payload=TechnicalPayload().model_dump(),
                quality_flags=[QualityFlag.MISSING_FIELD],
            )

        rsi = self._rsi(closes)
        ma20 = self._sma(closes, 20)
        ma50 = self._sma(closes, 50)
        ma200 = self._sma(closes, 200)
        atr = self._atr(highs, lows, closes)
        vol_30d = self._volatility_30d(closes)
        dd = self._max_drawdown(closes)
        supports, resistances = self._support_resistance(closes)

        # Volume ratio (current vs 20d avg)
        vol_ratio: float | None = None
        if len(volumes) >= 21:
            avg_vol = sum(volumes[-21:-1]) / 20
            if avg_vol > 0:
                vol_ratio = volumes[-1] / avg_vol

        trend = self._trend_label(price, ma20, ma50, ma200, rsi, vol_ratio)

        score = (
            self._score_trend(trend)
            + self._score_rsi(rsi)
            + self._score_ma_stack(price, ma50, ma200)
            + self._score_drawdown(dd)
        )
        score = float(min(100, max(0, score)))

        # ---- Direction ----
        if trend in ("uptrend", "breakout") and (rsi or 50) < 75:
            direction = Direction.POSITIVE
        elif trend in ("downtrend", "breakdown"):
            direction = Direction.NEGATIVE
        else:
            direction = Direction.NEUTRAL

        # ---- Confidence ----
        has_ma200 = ma200 is not None
        confidence = Confidence.HIGH if (has_ma200 and rsi is not None) else Confidence.MEDIUM

        # ---- Risk zones ----
        risk_zones: list[str] = []
        if dd is not None and abs(dd) > 20:
            risk_zones.append(f"In significant drawdown: {dd:.1f}% from 52w high")
        if rsi is not None and rsi > 78:
            risk_zones.append(f"Overbought: RSI {rsi:.1f}")
        if vol_30d is not None and vol_30d > 60:
            risk_zones.append(f"High volatility: {vol_30d:.1f}% annualised")

        evidence = [
            Evidence.from_market_data(ticker, [
                f"Price: {price:.2f}",
                f"RSI-14: {rsi:.1f}" if rsi else "RSI-14: N/A",
                f"MA50: {ma50:.2f}" if ma50 else "MA50: N/A",
                f"MA200: {ma200:.2f}" if ma200 else "MA200: N/A",
                f"30d vol: {vol_30d:.1f}%" if vol_30d else "Volatility: N/A",
                f"Drawdown from high: {dd:.1f}%" if dd else "Drawdown: N/A",
                f"Trend: {trend}",
            ])
        ]

        payload = TechnicalPayload(
            price=price,
            trend_label=trend,
            ma_20=ma20,
            ma_50=ma50,
            ma_200=ma200,
            rsi_14=rsi,
            atr_14=atr,
            volatility_30d=vol_30d,
            drawdown_from_high=dd,
            support_levels=supports,
            resistance_levels=resistances,
            risk_zones=risk_zones,
        ).model_dump()

        return self._emit(
            ticker=ticker,
            run_id=run_id,
            as_of=as_of,
            score=score,
            confidence=confidence,
            direction=direction,
            materiality=Materiality.HIGH if risk_zones else Materiality.MEDIUM,
            payload=payload,
            evidence=evidence,
        )
