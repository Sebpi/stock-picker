from __future__ import annotations

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
from schemas import EvidenceQuality, HorizonForecast, InvestmentThesis, RiskRating


class MultiAgentPhase3Tests(unittest.TestCase):
    def setUp(self):
        self.ticker = f"P{uuid.uuid4().hex[:5]}".upper()
        self.run_id = f"phase3-run-{uuid.uuid4().hex}"
        db.init_db()
        self._cleanup_rows()

    def tearDown(self):
        self._cleanup_rows()

    def _cleanup_rows(self):
        with db.get_conn() as conn:
            conn.execute("DELETE FROM forecast_outcome WHERE ticker = ?", (getattr(self, "ticker", ""),))
            conn.execute("DELETE FROM investment_thesis WHERE ticker = ?", (getattr(self, "ticker", ""),))
            conn.execute("DELETE FROM thesis_run WHERE run_id = ?", (getattr(self, "run_id", ""),))

    def _thesis(self) -> InvestmentThesis:
        return InvestmentThesis(
            thesis_id=f"phase3-thesis-{uuid.uuid4().hex}",
            run_id=self.run_id,
            ticker=self.ticker,
            current_price=100.0,
            composite_score=66.0,
            risk_rating=RiskRating.MEDIUM_LOW,
            evidence_quality=EvidenceQuality.MODERATE,
            forecast={
                "3m": HorizonForecast(base_return_pct=4.0, bull_return_pct=9.0, bear_return_pct=-4.0, confidence=0.61),
                "6m": HorizonForecast(base_return_pct=7.0, bull_return_pct=15.0, bear_return_pct=-7.0, confidence=0.59),
                "12m": HorizonForecast(base_return_pct=12.0, bull_return_pct=24.0, bear_return_pct=-11.0, confidence=0.55),
            },
            drivers=["phase3 driver"],
            risks=["phase3 risk"],
            agent_scores={"agent.fundamentals": 70.0},
            weighted_scores={"3m": 64.0, "6m": 65.0, "12m": 66.0},
            narrative={"base": "phase3"},
        )

    def test_forecast_outcome_status_marks_matured_pending_rows(self):
        thesis = self._thesis()
        db.store_thesis(thesis)
        old_generated_at = (datetime.now(timezone.utc) - timedelta(days=100)).isoformat()
        with db.get_conn() as conn:
            conn.execute(
                """
                UPDATE forecast_outcome
                SET thesis_generated_at = ?
                WHERE thesis_id = ? AND horizon = '3m'
                """,
                (old_generated_at, thesis.thesis_id),
            )

        status = db.get_forecast_outcome_status(self.ticker)

        self.assertEqual(status["total"], 4)  # 1m fast-track + 3m/6m/12m
        self.assertEqual(status["pending"], 4)
        self.assertEqual(status["matured_pending"], 1)
        self.assertEqual(status["by_horizon"]["3m"]["matured_pending"], 1)

    def test_operations_and_evaluation_status_endpoints(self):
        import main

        thesis = self._thesis()
        db.store_thesis(thesis)
        db.create_thesis_run(self.run_id, [self.ticker], run_fresh=False, requested_by="unit-test")
        db.update_thesis_run(self.run_id, status="partial", completed=[], failed=[self.ticker])

        original_service_key = main._SERVICE_KEY
        main._SERVICE_KEY = "svc-test-key"
        main.app.dependency_overrides[main.get_current_user] = lambda: "unit-test"
        try:
            # auth_middleware now backstops /v1/* — supply a transport credential.
            client = TestClient(main.app, headers={"host": "localhost", "Authorization": "Bearer svc-test-key"})

            runs = client.get("/v1/runs?limit=5")
            self.assertEqual(runs.status_code, 200, runs.text)
            self.assertTrue(any(r["run_id"] == self.run_id for r in runs.json()["runs"]))

            eval_status = client.get(f"/v1/evaluate/status?ticker={self.ticker}")
            self.assertEqual(eval_status.status_code, 200, eval_status.text)
            self.assertEqual(eval_status.json()["outcomes"]["ticker"], self.ticker)

            ops = client.get("/v1/operations/status")
            self.assertEqual(ops.status_code, 200, ops.text)
            body = ops.json()
            self.assertIn("forecast_outcomes", body)
            self.assertTrue(any(r["run_id"] == self.run_id for r in body["recent_failures"]))
        finally:
            main._SERVICE_KEY = original_service_key
            main.app.dependency_overrides.clear()

    def test_sync_evaluate_endpoint_reports_evaluated_count(self):
        import main

        original = evaluation.evaluate_pending_outcomes
        original_service_key = main._SERVICE_KEY
        main._SERVICE_KEY = "svc-test-key"
        main.app.dependency_overrides[main.get_current_user] = lambda: "unit-test"
        try:
            evaluation.evaluate_pending_outcomes = lambda: 7
            # auth_middleware now backstops /v1/* — supply a transport credential.
            client = TestClient(main.app, headers={"host": "localhost", "Authorization": "Bearer svc-test-key"})
            response = client.post("/v1/evaluate?sync=true")

            self.assertEqual(response.status_code, 200, response.text)
            body = response.json()
            self.assertTrue(body["ok"])
            self.assertEqual(body["evaluated"], 7)
            self.assertEqual(body["scheduler"]["last_evaluated_count"], 7)
        finally:
            evaluation.evaluate_pending_outcomes = original
            main._SERVICE_KEY = original_service_key
            main.app.dependency_overrides.clear()


if __name__ == "__main__":
    unittest.main()
