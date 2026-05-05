"""
Tests for _build_recommendation_alert_snapshot with the 9-agent pipeline.

Mocks out OrchestratorAgent.run_thesis and the portfolio/watchlist loaders
so no Claude calls or yfinance calls are made.
"""
from __future__ import annotations

import asyncio
import sys
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

ROOT = Path(__file__).resolve().parents[2]
BACKEND = ROOT / "backend"
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

from schemas import (
    Confidence,
    Direction,
    EvidenceQuality,
    HorizonForecast,
    InvestmentThesis,
    RiskRating,
)


def _make_thesis(ticker: str, composite: float, base_12m: float, base_6m: float = 0.0,
                 price: float = 100.0, narrative: dict | None = None) -> InvestmentThesis:
    """Helper — build a minimal InvestmentThesis for testing."""
    import uuid
    from datetime import datetime, timezone
    horizon_conf = 0.65  # "medium-high" confidence
    return InvestmentThesis(
        run_id=str(uuid.uuid4()),
        ticker=ticker,
        generated_at=datetime.now(timezone.utc),
        current_price=price,
        composite_score=composite,
        risk_rating=RiskRating.MEDIUM,
        evidence_quality=EvidenceQuality.STRONG,
        forecast={
            "3m":  HorizonForecast(base_return_pct=base_12m * 0.25, bull_return_pct=base_12m * 0.5,
                                   bear_return_pct=-5.0, confidence=horizon_conf),
            "6m":  HorizonForecast(base_return_pct=base_6m or base_12m * 0.5, bull_return_pct=base_12m,
                                   bear_return_pct=-8.0, confidence=horizon_conf),
            "12m": HorizonForecast(base_return_pct=base_12m, bull_return_pct=base_12m * 1.5,
                                   bear_return_pct=-12.0, confidence=horizon_conf),
        },
        drivers=["Strong revenue growth", "Expanding margins"],
        risks=["Macro headwinds", "Competition"],
        agent_scores={},
        agent_meta={},
        weighted_scores={"3m": composite, "6m": composite, "12m": composite},
        narrative=narrative or {"summary": f"{ticker} looks good.", "bear": f"{ticker} has risks."},
        quality_flags=[],
        decision_log=[],
    )


class TestAgentAlertSnapshotBuys(unittest.IsolatedAsyncioTestCase):
    """Buy signal tests."""

    async def _run(self, watchlist, portfolio_txs, thesis_map, settings=None):
        """Patch dependencies and run _build_recommendation_alert_snapshot."""
        import main as m

        _settings = {
            "initial_float": 10_000.0,
            "alert_buy_min_score": 72,
            "alert_sell_max_score": 42,
        }
        if settings:
            _settings.update(settings)

        def _fake_run_thesis(ticker, run_fresh=False):
            t = thesis_map.get(ticker)
            if t is None:
                raise ValueError(f"No thesis for {ticker}")
            return t

        with (
            patch.object(m, "load_watchlist", return_value=watchlist),
            patch.object(m, "load_portfolio", return_value=portfolio_txs),
            patch.object(m, "load_settings", return_value=_settings),
            patch("agents.orchestrator.OrchestratorAgent.run_thesis", side_effect=_fake_run_thesis),
        ):
            return await m._build_recommendation_alert_snapshot(buy_limit=3, sell_limit=3)

    async def test_high_score_ticker_triggers_buy(self):
        """A watchlist ticker with composite >= buy_min_score and positive 12m should appear as BUY."""
        thesis_map = {"AAPL": _make_thesis("AAPL", composite=80.0, base_12m=18.0)}
        result = await self._run(watchlist=["AAPL"], portfolio_txs=[], thesis_map=thesis_map)

        self.assertEqual(len(result["buys"]), 1)
        buy = result["buys"][0]
        self.assertEqual(buy["ticker"], "AAPL")
        self.assertEqual(buy["action"], "BUY")
        self.assertEqual(buy["score_value"], 80)
        self.assertGreater(buy["projected_12m_pct"], 0)

    async def test_low_score_ticker_suppressed(self):
        """A ticker below buy_min_score should NOT appear as a buy."""
        thesis_map = {"WEAK": _make_thesis("WEAK", composite=55.0, base_12m=10.0)}
        result = await self._run(watchlist=["WEAK"], portfolio_txs=[], thesis_map=thesis_map)
        self.assertEqual(result["buys"], [])

    async def test_thin_upside_suppressed(self):
        """High composite but tiny projected return should be filtered out."""
        thesis_map = {"FLAT": _make_thesis("FLAT", composite=80.0, base_12m=3.0, base_6m=2.0)}
        result = await self._run(watchlist=["FLAT"], portfolio_txs=[], thesis_map=thesis_map)
        self.assertEqual(result["buys"], [])

    async def test_already_owned_excluded_from_buys(self):
        """Watchlist ticker already in portfolio should not appear as a BUY."""
        thesis_map = {"AAPL": _make_thesis("AAPL", composite=85.0, base_12m=20.0)}
        portfolio_txs = [{"ticker": "AAPL", "type": "buy", "qty": 10, "price": 100.0,
                          "timestamp": "2026-01-01T00:00:00Z", "name": "Apple"}]
        result = await self._run(watchlist=["AAPL"], portfolio_txs=portfolio_txs, thesis_map=thesis_map)
        self.assertEqual(result["buys"], [])

    async def test_position_sizing_uses_initial_float(self):
        """est_cost should be between 5% and 15% of initial_float."""
        thesis_map = {"NVDA": _make_thesis("NVDA", composite=90.0, base_12m=25.0)}
        result = await self._run(
            watchlist=["NVDA"], portfolio_txs=[], thesis_map=thesis_map,
            settings={"initial_float": 10_000.0, "alert_buy_min_score": 72, "alert_sell_max_score": 42}
        )
        self.assertEqual(len(result["buys"]), 1)
        est_cost = result["buys"][0]["est_cost"]
        self.assertGreaterEqual(est_cost, 10_000.0 * 0.05)
        self.assertLessEqual(est_cost, 10_000.0 * 0.15)

    async def test_buy_limit_respected(self):
        """Result should honour the buy_limit parameter."""
        thesis_map = {
            f"T{i}": _make_thesis(f"T{i}", composite=75.0 + i, base_12m=15.0 + i)
            for i in range(5)
        }
        result = await self._run(
            watchlist=list(thesis_map.keys()), portfolio_txs=[], thesis_map=thesis_map
        )
        # buy_limit=3 (default in _run helper)
        self.assertLessEqual(len(result["buys"]), 3)

    async def test_buys_sorted_by_score_descending(self):
        """Buys should be ordered highest composite score first."""
        thesis_map = {
            "LOW":  _make_thesis("LOW",  composite=73.0, base_12m=12.0),
            "HIGH": _make_thesis("HIGH", composite=88.0, base_12m=20.0),
            "MID":  _make_thesis("MID",  composite=80.0, base_12m=15.0),
        }
        result = await self._run(
            watchlist=["LOW", "HIGH", "MID"], portfolio_txs=[], thesis_map=thesis_map
        )
        scores = [b["score_value"] for b in result["buys"]]
        self.assertEqual(scores, sorted(scores, reverse=True))

    async def test_scored_by_field_present(self):
        """Result must declare scored_by='multi-agent'."""
        result = await self._run(watchlist=[], portfolio_txs=[], thesis_map={})
        self.assertEqual(result.get("scored_by"), "multi-agent")


class TestAgentAlertSnapshotSells(unittest.IsolatedAsyncioTestCase):
    """Sell signal tests."""

    def _portfolio_with(self, ticker: str, shares: int = 10, avg_cost: float = 100.0):
        return [{"ticker": ticker, "type": "buy", "qty": shares, "price": avg_cost,
                 "timestamp": "2026-01-01T00:00:00Z", "name": ticker}]

    async def _run(self, portfolio_txs, thesis_map, settings=None):
        import main as m
        _settings = {
            "initial_float": 10_000.0,
            "alert_buy_min_score": 72,
            "alert_sell_max_score": 42,
        }
        if settings:
            _settings.update(settings)

        def _fake_run_thesis(ticker, run_fresh=False):
            t = thesis_map.get(ticker)
            if t is None:
                raise ValueError(f"No thesis for {ticker}")
            return t

        with (
            patch.object(m, "load_watchlist", return_value=[]),
            patch.object(m, "load_portfolio", return_value=portfolio_txs),
            patch.object(m, "load_settings", return_value=_settings),
            patch("agents.orchestrator.OrchestratorAgent.run_thesis", side_effect=_fake_run_thesis),
        ):
            return await m._build_recommendation_alert_snapshot(buy_limit=3, sell_limit=3)

    async def test_low_composite_triggers_agent_sell(self):
        """Owned ticker with composite <= sell_max_score and negative 12m should trigger SELL."""
        thesis_map = {"IBM": _make_thesis("IBM", composite=35.0, base_12m=-12.0)}
        result = await self._run(self._portfolio_with("IBM"), thesis_map)

        self.assertEqual(len(result["sells"]), 1)
        sell = result["sells"][0]
        self.assertEqual(sell["ticker"], "IBM")
        self.assertEqual(sell["action"], "SELL")
        self.assertIn("AGENT SELL", sell["trigger"])

    async def test_bullish_owned_position_not_sold(self):
        """Owned ticker with high composite and positive 12m should NOT trigger a sell."""
        thesis_map = {"MSFT": _make_thesis("MSFT", composite=82.0, base_12m=15.0)}
        result = await self._run(self._portfolio_with("MSFT"), thesis_map)
        self.assertEqual(result["sells"], [])

    async def test_agent_bearish_secondary_trigger(self):
        """composite < 50 with projected loss > 8% should trigger AGENT BEARISH."""
        thesis_map = {"WEAK": _make_thesis("WEAK", composite=45.0, base_12m=-10.0)}
        result = await self._run(self._portfolio_with("WEAK"), thesis_map)
        self.assertEqual(len(result["sells"]), 1)
        self.assertIn("BEARISH", result["sells"][0]["trigger"])

    async def test_sell_includes_unrealised_pct(self):
        """Sell signal must include unrealised_pct for context."""
        thesis_map = {"XOM": _make_thesis("XOM", composite=30.0, base_12m=-15.0, price=80.0)}
        result = await self._run(self._portfolio_with("XOM", avg_cost=100.0), thesis_map)
        self.assertIn("unrealised_pct", result["sells"][0])

    async def test_sell_limit_respected(self):
        """Result should honour the sell_limit parameter."""
        portfolio_txs = [
            {"ticker": f"S{i}", "type": "buy", "qty": 5, "price": 100.0,
             "timestamp": "2026-01-01T00:00:00Z", "name": f"S{i}"}
            for i in range(5)
        ]
        thesis_map = {
            f"S{i}": _make_thesis(f"S{i}", composite=30.0, base_12m=-15.0)
            for i in range(5)
        }
        result = await self._run(portfolio_txs, thesis_map)
        self.assertLessEqual(len(result["sells"]), 3)

    async def test_failed_thesis_ticker_skipped_gracefully(self):
        """If a thesis run raises, that ticker is skipped rather than crashing the snapshot."""
        import main as m

        def _failing_thesis(ticker, run_fresh=False):
            raise RuntimeError("Claude unavailable")

        portfolio_txs = self._portfolio_with("FAIL")
        with (
            patch.object(m, "load_watchlist", return_value=[]),
            patch.object(m, "load_portfolio", return_value=portfolio_txs),
            patch.object(m, "load_settings", return_value={
                "initial_float": 10_000.0, "alert_buy_min_score": 72, "alert_sell_max_score": 42
            }),
            patch("agents.orchestrator.OrchestratorAgent.run_thesis", side_effect=_failing_thesis),
        ):
            result = await m._build_recommendation_alert_snapshot()

        # Should return cleanly with empty lists, not raise
        self.assertIn("buys", result)
        self.assertIn("sells", result)
        self.assertEqual(result["buys"], [])
        self.assertEqual(result["sells"], [])


class TestAgentAlertSnapshotFloatRespected(unittest.IsolatedAsyncioTestCase):
    """Verify the initial_float from settings drives position sizing, not a hardcoded value."""

    async def test_different_floats_produce_different_est_cost(self):
        import main as m

        thesis_map = {"TSLA": _make_thesis("TSLA", composite=80.0, base_12m=20.0)}

        def _fake_run_thesis(ticker, run_fresh=False):
            return thesis_map[ticker]

        async def _snapshot(float_val):
            with (
                patch.object(m, "load_watchlist", return_value=["TSLA"]),
                patch.object(m, "load_portfolio", return_value=[]),
                patch.object(m, "load_settings", return_value={
                    "initial_float": float_val,
                    "alert_buy_min_score": 72,
                    "alert_sell_max_score": 42,
                }),
                patch("agents.orchestrator.OrchestratorAgent.run_thesis", side_effect=_fake_run_thesis),
            ):
                return await m._build_recommendation_alert_snapshot(buy_limit=1)

        small = await _snapshot(5_000.0)
        large = await _snapshot(50_000.0)

        cost_small = small["buys"][0]["est_cost"]
        cost_large = large["buys"][0]["est_cost"]

        # est_cost should scale proportionally with float
        self.assertLess(cost_small, cost_large)
        self.assertAlmostEqual(cost_large / cost_small, 10.0, delta=0.5)


if __name__ == "__main__":
    unittest.main()
