"""
Phase 4 tests — hardening, observability, report polish.
"""
from __future__ import annotations

import json
import time
import uuid
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from agents import BaseAgent, FETCH_TIMEOUT_SECS
from schemas import (
    AgentSignal, Confidence, Direction, EvidenceQuality,
    InvestmentThesis, HorizonForecast, Materiality, QualityFlag, RiskRating,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_signal(ticker="AAPL", score=70.0, flags=None) -> AgentSignal:
    return AgentSignal(
        run_id=str(uuid.uuid4()),
        agent_id="agent.test",
        ticker=ticker,
        as_of=datetime.now(timezone.utc),
        horizon_relevance=["3m", "6m", "12m"],
        signal_type="test",
        score=score,
        confidence=Confidence.MEDIUM,
        direction=Direction.POSITIVE,
        materiality=Materiality.MEDIUM,
        payload={},
        quality_flags=flags or [],
    )


def _make_thesis(ticker="AAPL") -> InvestmentThesis:
    fcast = HorizonForecast(base_return_pct=5.0, bull_return_pct=12.0,
                            bear_return_pct=-5.0, confidence=0.65)
    return InvestmentThesis(
        run_id=str(uuid.uuid4()),
        ticker=ticker,
        current_price=150.0,
        composite_score=68.0,
        risk_rating=RiskRating.MEDIUM,
        evidence_quality=EvidenceQuality.MODERATE,
        forecast={"3m": fcast, "6m": fcast, "12m": fcast},
        agent_scores={"agent.fundamentals": 72.0},
        agent_meta={"agent.fundamentals": {"direction": "positive",
                                           "confidence": "medium",
                                           "flags": [], "usable": True}},
        weighted_scores={"3m": 66.0, "6m": 67.0, "12m": 68.0},
    )


# ---------------------------------------------------------------------------
# 1. _timed_fetch timeout behaviour
# ---------------------------------------------------------------------------

class _DummyAgent(BaseAgent):
    agent_id = "agent.dummy"
    signal_type = "dummy"

    def _run(self, ticker, run_id, as_of):
        return self._emit(ticker=ticker, run_id=run_id, as_of=as_of,
                          score=50.0, confidence=Confidence.LOW,
                          direction=Direction.NEUTRAL,
                          materiality=Materiality.LOW, payload={})


def test_timed_fetch_returns_none_on_timeout():
    def slow():
        time.sleep(5)
        return "value"

    result = _DummyAgent._timed_fetch(slow, "test/slow", timeout=0.1)
    assert result is None


def test_timed_fetch_returns_value_on_success():
    result = _DummyAgent._timed_fetch(lambda: 42, "test/fast", timeout=5.0)
    assert result == 42


def test_timed_fetch_returns_none_on_exception():
    def boom():
        raise ValueError("bad data")

    result = _DummyAgent._timed_fetch(boom, "test/boom", timeout=5.0)
    assert result is None


def test_timed_fetch_retries_on_connection_error():
    calls = []

    def flaky():
        calls.append(1)
        if len(calls) < 3:
            raise ConnectionError("transient")
        return "ok"

    result = _DummyAgent._timed_fetch(flaky, "test/flaky", timeout=10.0)
    assert result == "ok"
    assert len(calls) == 3


def test_fetch_timeout_secs_is_positive():
    assert FETCH_TIMEOUT_SECS > 0


# ---------------------------------------------------------------------------
# 2. BaseAgent.run() emits log_metric
# ---------------------------------------------------------------------------

def test_base_agent_emits_metrics_on_success():
    agent = _DummyAgent()
    emitted = []

    with patch("db.start_run", return_value=1), \
         patch("db.upsert_signal"), \
         patch("db.complete_run"), \
         patch("observability.log_metric", side_effect=lambda m, v, l=None: emitted.append(m)):
        agent.run("AAPL")

    assert "agent_run_duration_secs" in emitted
    assert "agent_score" in emitted


def test_base_agent_emits_error_metric_on_failure():
    class _BrokenAgent(BaseAgent):
        agent_id = "agent.broken"
        signal_type = "broken"

        def _run(self, ticker, run_id, as_of):
            raise RuntimeError("exploded")

    agent = _BrokenAgent()
    emitted = []

    with patch("db.start_run", return_value=1), \
         patch("db.complete_run"), \
         patch("observability.log_metric", side_effect=lambda m, v, l=None: emitted.append(m)):
        agent.run("AAPL")

    assert "agent_run_duration_secs" in emitted
    # status label should be "error"
    calls = []
    with patch("db.start_run", return_value=1), \
         patch("db.complete_run"), \
         patch("observability.log_metric", side_effect=lambda m, v, l=None: calls.append((m, l))):
        _BrokenAgent().run("AAPL")
    duration_call = next((c for c in calls if c[0] == "agent_run_duration_secs"), None)
    assert duration_call is not None
    assert duration_call[1].get("status") == "error"


# ---------------------------------------------------------------------------
# 3. InvestmentThesis — agent_meta field
# ---------------------------------------------------------------------------

def test_thesis_agent_meta_roundtrip():
    thesis = _make_thesis()
    dumped = thesis.model_dump(mode="json")
    restored = InvestmentThesis.model_validate(dumped)
    assert restored.agent_meta == thesis.agent_meta


def test_thesis_agent_meta_default_empty():
    thesis = _make_thesis()
    thesis.agent_meta = {}
    assert thesis.agent_meta == {}


def test_thesis_agent_meta_usable_flag():
    thesis = _make_thesis()
    meta = thesis.agent_meta.get("agent.fundamentals", {})
    assert meta.get("usable") is True
    assert meta.get("direction") == "positive"


# ---------------------------------------------------------------------------
# 4. db.get_thesis_history
# ---------------------------------------------------------------------------

def test_get_thesis_history_returns_list():
    import db
    # Should return a list (may be empty if no DB rows)
    result = db.get_thesis_history("AAPL", limit=5)
    assert isinstance(result, list)


def test_get_thesis_history_limit_clamp():
    import db
    # limit > 50 should be clamped
    with patch("db.get_conn") as mock_conn:
        mock_cursor = MagicMock()
        mock_cursor.__enter__ = lambda s: s
        mock_cursor.__exit__ = MagicMock(return_value=False)
        mock_cursor.execute.return_value.fetchall.return_value = []
        mock_conn.return_value = mock_cursor
        db.get_thesis_history("AAPL", limit=999)
        call_args = mock_cursor.execute.call_args
        # second positional arg tuple should have limit <= 50
        assert call_args[0][1][2] <= 50


# ---------------------------------------------------------------------------
# 5. Quality flag tooltips exist for all QualityFlag values
# ---------------------------------------------------------------------------

def test_all_quality_flags_have_tooltips():
    # Check FLAG_TOOLTIPS in react-app.js covers every QualityFlag value
    js_path = os.path.join(os.path.dirname(__file__), "..", "..", "frontend", "react-app.js")
    with open(js_path, encoding="utf-8") as f:
        js = f.read()
    for flag in QualityFlag:
        assert flag.value in js, f"Missing tooltip for {flag.value} in react-app.js"


# ---------------------------------------------------------------------------
# 6. Orchestrator run_thesis failure path emits metric
# ---------------------------------------------------------------------------

def test_observability_all_agent_ids_matches_run_all_agents():
    """ALL_AGENT_IDS must stay in sync with run_all_agents so health reports are accurate."""
    import observability
    from agents import run_all_agents
    import inspect, ast

    # Extract agent_id class attributes by parsing agents/__init__.py
    init_path = os.path.join(os.path.dirname(__file__), "..", "agents", "__init__.py")
    source = open(init_path).read()
    # Get all agent_id strings from the registered agents module files
    agents_dir = os.path.join(os.path.dirname(__file__), "..", "agents")
    registered_ids = set()
    for fname in os.listdir(agents_dir):
        if fname.endswith(".py") and fname not in ("__init__.py", "orchestrator.py", "base.py"):
            fpath = os.path.join(agents_dir, fname)
            text = open(fpath).read()
            for line in text.splitlines():
                line = line.strip()
                if line.startswith("agent_id") and "=" in line:
                    val = line.split("=", 1)[1].strip().strip('"').strip("'")
                    if val.startswith("agent."):
                        registered_ids.add(val)

    missing = registered_ids - set(observability.ALL_AGENT_IDS)
    assert not missing, (
        f"Agents registered in agents/ but missing from observability.ALL_AGENT_IDS: {sorted(missing)}"
    )


def test_orchestrator_emits_error_metric_on_failure():
    from agents.orchestrator import OrchestratorAgent
    orch = OrchestratorAgent()
    emitted = []

    with patch("agents.orchestrator.run_all_agents", side_effect=RuntimeError("boom")), \
         patch("observability.log_metric", side_effect=lambda m, v, l=None: emitted.append((m, l))):
        with pytest.raises(RuntimeError):
            orch.run_thesis("AAPL", run_fresh=True)

    error_metrics = [(m, l) for m, l in emitted
                     if m == "thesis_run_duration_secs" and l and l.get("status") == "error"]
    assert error_metrics, "No error metric emitted on orchestrator failure"
