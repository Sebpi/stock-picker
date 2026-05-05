"""
BaseAgent abstract class and agent registry.
All agents inherit from BaseAgent and must implement run().
"""
from __future__ import annotations

import logging
import time as _time
import uuid
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
from datetime import datetime, timezone
from typing import Any, Callable, TypeVar

import tenacity

import db
from schemas import (
    AgentSignal,
    Confidence,
    Direction,
    Evidence,
    Materiality,
    QualityFlag,
)

logger = logging.getLogger(__name__)

# Maximum seconds to wait for any single data-fetch call (yfinance / HTTP).
FETCH_TIMEOUT_SECS = 15

_T = TypeVar("_T")

_retry_transient = tenacity.retry(
    retry=tenacity.retry_if_exception_type((ConnectionError, OSError, TimeoutError)),
    wait=tenacity.wait_exponential(multiplier=1, min=1, max=8),
    stop=tenacity.stop_after_attempt(3),
    reraise=True,
    before_sleep=tenacity.before_sleep_log(logger, logging.DEBUG),
)


class BaseAgent(ABC):
    agent_id: str = "agent.base"
    signal_type: str = "base"
    default_horizons: list[str] = ["3m", "6m", "12m"]

    def run(self, ticker: str, run_id: str | None = None,
            as_of: datetime | None = None) -> AgentSignal:
        if run_id is None:
            run_id = str(uuid.uuid4())
        if as_of is None:
            as_of = datetime.now(timezone.utc)

        import observability
        db_run_id = db.start_run(self.agent_id, ticker)
        signal: AgentSignal | None = None
        t0 = _time.monotonic()
        try:
            signal = self._run(ticker, run_id, as_of)
            db.upsert_signal(signal)
            db.complete_run(db_run_id, signal_id=signal.signal_id)
            duration = _time.monotonic() - t0
            logger.info("[%s] %s score=%.1f flags=%s dur=%.1fs",
                        self.agent_id, ticker, signal.score,
                        [f.value for f in signal.quality_flags], duration)
            observability.log_metric("agent_run_duration_secs", duration,
                                     {"agent": self.agent_id, "ticker": ticker, "status": "ok"})
            observability.log_metric("agent_score", signal.score,
                                     {"agent": self.agent_id, "ticker": ticker})
        except Exception as exc:
            duration = _time.monotonic() - t0
            logger.exception("[%s] %s failed: %s", self.agent_id, ticker, exc)
            db.complete_run(db_run_id, error_code=type(exc).__name__)
            observability.log_metric("agent_run_duration_secs", duration,
                                     {"agent": self.agent_id, "ticker": ticker, "status": "error"})
            signal = self._error_signal(ticker, run_id, as_of, str(exc))
        return signal

    @abstractmethod
    def _run(self, ticker: str, run_id: str, as_of: datetime) -> AgentSignal:
        """Implemented by each concrete agent. Must return an AgentSignal."""

    # ------------------------------------------------------------------
    # Helper factories — use these inside _run()
    # ------------------------------------------------------------------

    def _emit(
        self,
        ticker: str,
        run_id: str,
        as_of: datetime,
        score: float,
        confidence: Confidence,
        direction: Direction,
        materiality: Materiality,
        payload: dict[str, Any],
        evidence: list[Evidence] | None = None,
        quality_flags: list[QualityFlag] | None = None,
    ) -> AgentSignal:
        return AgentSignal(
            run_id=run_id,
            agent_id=self.agent_id,
            ticker=ticker,
            as_of=as_of,
            horizon_relevance=self.default_horizons,
            signal_type=self.signal_type,
            score=max(0.0, min(100.0, score)),
            confidence=confidence,
            direction=direction,
            materiality=materiality,
            payload=payload,
            evidence=evidence or [],
            quality_flags=quality_flags or [],
        )

    def _error_signal(self, ticker: str, run_id: str,
                      as_of: datetime, error_msg: str) -> AgentSignal:
        return AgentSignal(
            run_id=run_id,
            agent_id=self.agent_id,
            ticker=ticker,
            as_of=as_of,
            horizon_relevance=self.default_horizons,
            signal_type=self.signal_type,
            score=50.0,
            confidence=Confidence.LOW,
            direction=Direction.NEUTRAL,
            materiality=Materiality.LOW,
            payload={},
            quality_flags=[QualityFlag.MISSING_FIELD],
            errors=[error_msg],
        )

    @staticmethod
    def _timed_fetch(fn: Callable[[], _T], label: str = "",
                     timeout: float = FETCH_TIMEOUT_SECS) -> _T | None:
        """
        Run *fn* in a thread with a hard timeout.  Returns None on timeout or
        any exception, logging at DEBUG so the agent can set quality flags.
        Retries on transient network errors via tenacity.
        """
        @_retry_transient
        def _with_retry() -> _T:
            return fn()

        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(_with_retry)
            try:
                return future.result(timeout=timeout)
            except FuturesTimeoutError:
                logger.debug("_timed_fetch timeout after %.0fs: %s", timeout, label)
                future.cancel()
                return None
            except Exception as exc:
                logger.debug("_timed_fetch error [%s]: %s", label, exc)
                return None

    @staticmethod
    def _safe_get(info: dict[str, Any], key: str,
                  default: Any = None) -> Any:
        val = info.get(key)
        if val is None or val != val:  # catches NaN
            return default
        return val


def run_agent(agent: BaseAgent, ticker: str,
              run_id: str | None = None) -> AgentSignal:
    """Convenience wrapper — run a single agent for a ticker."""
    return agent.run(ticker, run_id=run_id)


def run_all_agents(ticker: str, run_id: str | None = None) -> dict[str, AgentSignal]:
    """
    Import and run all registered agents for a ticker.
    Returns dict keyed by agent_id.
    Agents are imported lazily to avoid circular imports.
    """
    from agents.fundamentals import FundamentalsAgent
    from agents.valuation import ValuationAgent
    from agents.technical_risk import TechnicalRiskAgent
    from agents.macro_liquidity import MacroLiquidityAgent
    from agents.growth_revisions import GrowthRevisionsAgent
    from agents.sentiment_news import SentimentNewsAgent
    from agents.industry_competition import IndustryCompetitionAgent
    from agents.portfolio_risk import PortfolioRiskAgent

    agents: list[BaseAgent] = [
        FundamentalsAgent(),
        ValuationAgent(),
        TechnicalRiskAgent(),
        MacroLiquidityAgent(),
        GrowthRevisionsAgent(),
        SentimentNewsAgent(),
        IndustryCompetitionAgent(),
        PortfolioRiskAgent(),
    ]
    if run_id is None:
        run_id = str(uuid.uuid4())

    results: dict[str, AgentSignal] = {}
    for agent in agents:
        sig = agent.run(ticker, run_id=run_id)
        results[agent.agent_id] = sig
    return results
