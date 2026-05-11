from __future__ import annotations

import json
import asyncio
import sys
import unittest
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
BACKEND = ROOT / "backend"
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

import db
import evaluation
from fastapi.testclient import TestClient
from fastapi.security import HTTPAuthorizationCredentials
from schemas import (
    AgentSignal,
    Confidence,
    Direction,
    EvidenceQuality,
    HorizonForecast,
    InvestmentThesis,
    Materiality,
    RiskRating,
)


class MultiAgentPhase1Tests(unittest.TestCase):
    def setUp(self):
        self.ticker = f"T{uuid.uuid4().hex[:5]}".upper()
        db.init_db()
        self._cleanup_rows()

    def tearDown(self):
        self._cleanup_rows()

    def _cleanup_rows(self):
        with db.get_conn() as conn:
            conn.execute("DELETE FROM agent_signal WHERE ticker = ?", (getattr(self, "ticker", ""),))
            conn.execute("DELETE FROM forecast_outcome WHERE ticker = ?", (getattr(self, "ticker", ""),))
            conn.execute("DELETE FROM investment_thesis WHERE ticker = ?", (getattr(self, "ticker", ""),))
            conn.execute("DELETE FROM thesis_run WHERE requested_by = 'unit-test'")
            conn.execute("DELETE FROM agent_run WHERE ticker = ?", (getattr(self, "ticker", ""),))

    def _signal(self, agent_id: str, score: float, as_of: datetime | None = None) -> AgentSignal:
        return AgentSignal(
            run_id=str(uuid.uuid4()),
            agent_id=agent_id,
            ticker=self.ticker,
            as_of=as_of or datetime.now(timezone.utc),
            signal_type="test",
            score=score,
            confidence=Confidence.HIGH,
            direction=Direction.POSITIVE,
            materiality=Materiality.MEDIUM,
            payload={"score": score},
        )

    def _thesis(self) -> InvestmentThesis:
        return InvestmentThesis(
            thesis_id=f"test-thesis-{uuid.uuid4().hex}",
            run_id=f"test-run-{uuid.uuid4().hex}",
            ticker=self.ticker,
            current_price=100.0,
            composite_score=62.0,
            risk_rating=RiskRating.MEDIUM,
            evidence_quality=EvidenceQuality.MODERATE,
            forecast={
                "3m": HorizonForecast(base_return_pct=5.0, bull_return_pct=8.0, bear_return_pct=-3.0, confidence=0.72),
                "6m": HorizonForecast(base_return_pct=9.0, bull_return_pct=14.0, bear_return_pct=-6.0, confidence=0.64),
                "12m": HorizonForecast(base_return_pct=14.0, bull_return_pct=25.0, bear_return_pct=-10.0, confidence=0.58),
            },
            drivers=["test driver"],
            risks=["test risk"],
            agent_scores={"agent.fundamentals": 70.0},
            weighted_scores={"3m": 60.0, "6m": 62.0, "12m": 64.0},
            narrative={"base": "test"},
        )

    def test_latest_signals_returns_newest_fresh_signal_per_agent(self):
        old = self._signal("agent.fundamentals", 40.0, datetime.now(timezone.utc) - timedelta(hours=3))
        new = self._signal("agent.fundamentals", 75.0, datetime.now(timezone.utc))
        stale = self._signal("agent.valuation", 80.0, datetime.now(timezone.utc) - timedelta(hours=40))
        db.upsert_signal(old)
        db.upsert_signal(new)
        db.upsert_signal(stale)

        signals = db.get_latest_signals(self.ticker, max_age_hours=26)

        self.assertEqual(set(signals), {"agent.fundamentals"})
        self.assertEqual(signals["agent.fundamentals"].score, 75.0)

    def test_forecast_outcome_error_and_calibration_are_persisted(self):
        thesis = self._thesis()
        db.store_thesis(thesis)
        with db.get_conn() as conn:
            row = conn.execute(
                "SELECT outcome_id FROM forecast_outcome WHERE thesis_id = ? AND horizon = '3m'",
                (thesis.thesis_id,),
            ).fetchone()
        self.assertIsNotNone(row)

        db.update_outcome(row["outcome_id"], realised=8.5, benchmark=2.0, sector_relative=6.5, direction_match=True)

        with db.get_conn() as conn:
            updated = conn.execute(
                "SELECT realised_return_pct, forecast_error, direction_match FROM forecast_outcome WHERE outcome_id = ?",
                (row["outcome_id"],),
            ).fetchone()
        self.assertEqual(updated["realised_return_pct"], 8.5)
        self.assertEqual(updated["forecast_error"], 3.5)
        self.assertEqual(updated["direction_match"], 1)

        calibration = evaluation.confidence_calibration(self.ticker)
        self.assertEqual(calibration["3m_high"]["hit_rate"], 1.0)
        self.assertEqual(calibration["3m_high"]["mae"], 3.5)

    def test_v1_runs_accepts_json_body_and_persists_status(self):
        import main

        db.init_db()
        original_get_orchestrator = main._get_orchestrator
        main.app.dependency_overrides[main.get_current_user] = lambda: "unit-test"

        class FakeOrchestrator:
            def run_thesis(self, ticker: str, run_fresh: bool = False):
                return None

        try:
            main._get_orchestrator = lambda: FakeOrchestrator()

            client = TestClient(main.app, headers={"host": "localhost"})
            response = client.post("/v1/runs", json={"tickers": [self.ticker.lower()], "run_fresh": False})

            self.assertEqual(response.status_code, 200, response.text)
            body = response.json()
            self.assertEqual(body["tickers"], [self.ticker])

            main._v1_runs.clear()
            persisted = client.get(f"/v1/runs/{body['run_id']}")
            self.assertEqual(persisted.status_code, 200, persisted.text)
            persisted_body = persisted.json()
            self.assertEqual(persisted_body["status"], "completed")
            self.assertEqual(persisted_body["completed"], [self.ticker])
        finally:
            main._get_orchestrator = original_get_orchestrator
            main.app.dependency_overrides.clear()

    def test_service_key_can_authenticate_v1_integration_calls(self):
        import main

        old_service_key = main._SERVICE_KEY
        main._SERVICE_KEY = "svc-test-key"
        try:
            creds = HTTPAuthorizationCredentials(scheme="Bearer", credentials="svc-test-key")
            user = asyncio.run(main.get_current_user(creds))
            self.assertEqual(user, "service:pick-shovels")
        finally:
            main._SERVICE_KEY = old_service_key


if __name__ == "__main__":
    unittest.main()
