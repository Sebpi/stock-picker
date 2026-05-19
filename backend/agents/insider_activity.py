"""
agent.insider_activity — SEC Form 4 insider transactions as a 10th signal.

Reads the rolling 60-day insider summary (purchases / sales / unique
insiders / cluster-buying flag) from insider_transactions.summarize_ticker
and emits a 0-100 score:

   90  cluster-buying (≥3 insiders made open-market purchases in window)
   75  net-buying flow with ≥2 distinct insiders
   60  mixed P/S but net positive
   50  no high-signal P/S transactions (the common "all-quiet" case)
   45  pure selling, low dollar volume — usually scheduled / vesting
   38  pure selling, moderate $ from multiple insiders
   30  heavy multi-insider selling (>$50M net) — bearish

Direction:
   POSITIVE on net-buy or cluster
   NEUTRAL on zero P/S transactions
   NEGATIVE on net-sell (modulated by materiality — most exec selling
   is non-directional vesting, so we never go below ~30)

Confidence is driven by transaction count and unique-insider count —
3 insiders × 5 trades > 1 insider × 15 trades for this signal.

If the SQLite cache is empty for the ticker we trigger one synchronous
refresh from SEC EDGAR (~5-10s). If the cache stays empty we emit a
neutral 50 with LOW_COVERAGE.
"""
from __future__ import annotations

import logging
import sys
from datetime import datetime
from pathlib import Path

# Backend module imports — the agents/ subdir runs under the same sys.path
# as main.py (both are added in __init__.py callers).
sys.path.insert(0, str(Path(__file__).parent.parent))
from agents import BaseAgent
from schemas import (
    Confidence,
    Direction,
    Evidence,
    Materiality,
    QualityFlag,
)
from insider_transactions import (
    refresh_ticker,
    summarize_ticker,
)

logger = logging.getLogger(__name__)

# Window for the agent's rolling view. 60 days is the sweet spot for
# Form 4: long enough to catch quarterly clusters around earnings,
# short enough that the signal is still actionable.
WINDOW_DAYS = 60

# Dollar bands for materiality classification (USD, signed).
HEAVY_SELL_THRESHOLD   = -50_000_000   # below this = heavy bearish
MODERATE_SELL_THRESHOLD = -5_000_000
HEAVY_BUY_THRESHOLD    = 5_000_000     # above this = meaningful buy


class InsiderActivityAgent(BaseAgent):
    agent_id = "agent.insider_activity"
    signal_type = "insider_flow"
    default_horizons = ["3m", "6m", "12m"]

    def _run(self, ticker: str, run_id: str, as_of: datetime):
        ticker = ticker.upper()

        # 1. Try cached summary first; refresh from SEC if empty.
        summary = summarize_ticker(ticker, days=WINDOW_DAYS)
        if summary.get("transaction_count", 0) == 0:
            # No data in cache — synchronous refresh (timed-fetched so we
            # don't block the orchestrator for >15s if SEC is slow).
            refresh_info = self._timed_fetch(
                lambda: refresh_ticker(ticker, limit=20),
                f"{ticker}/insider_refresh",
            )
            if refresh_info:
                summary = summarize_ticker(ticker, days=WINDOW_DAYS)

        n_trades   = int(summary.get("transaction_count") or 0)
        n_buys     = int(summary.get("purchase_count") or 0)
        n_sales    = int(summary.get("sale_count") or 0)
        net_usd    = float(summary.get("net_value_usd") or 0.0)
        n_insiders = int(summary.get("unique_insiders") or 0)
        cluster    = bool(summary.get("cluster_buying"))

        # 2. Scoring ladder. Tunable; should be backtested against
        # realised 60-day returns once we have enough trade history.
        if cluster:
            score = 88.0
            direction = Direction.POSITIVE
            materiality = Materiality.HIGH
            note = f"Cluster buying: {n_insiders} insiders made open-market purchases."
        elif n_buys > 0 and net_usd > HEAVY_BUY_THRESHOLD:
            score = 72.0
            direction = Direction.POSITIVE
            materiality = Materiality.HIGH
            note = f"Net insider buying ${net_usd/1e6:.1f}M across {n_buys} purchases / {n_insiders} insiders."
        elif n_buys > 0 and net_usd > 0:
            score = 60.0
            direction = Direction.POSITIVE
            materiality = Materiality.MEDIUM
            note = f"Modest net buying ${net_usd/1e6:.1f}M ({n_buys} purchases, {n_sales} sales)."
        elif n_trades == 0:
            score = 50.0
            direction = Direction.NEUTRAL
            materiality = Materiality.LOW
            note = "No open-market insider transactions in the last 60 days."
        elif net_usd < HEAVY_SELL_THRESHOLD and n_insiders >= 3:
            score = 32.0
            direction = Direction.NEGATIVE
            materiality = Materiality.HIGH
            note = f"Heavy multi-insider selling ${abs(net_usd)/1e6:.1f}M from {n_insiders} insiders."
        elif net_usd < MODERATE_SELL_THRESHOLD:
            score = 40.0
            direction = Direction.NEGATIVE
            materiality = Materiality.MEDIUM
            note = f"Moderate net selling ${abs(net_usd)/1e6:.1f}M — could be scheduled/vesting."
        elif net_usd < 0:
            # Light selling — usually just diversification, not bearish
            score = 47.0
            direction = Direction.NEUTRAL
            materiality = Materiality.LOW
            note = f"Light insider selling ${abs(net_usd)/1e6:.1f}M — likely vesting/diversification."
        else:
            score = 50.0
            direction = Direction.NEUTRAL
            materiality = Materiality.LOW
            note = "Mixed insider activity, no clear directional signal."

        # 3. Confidence — driven by trade count + insider diversity.
        if cluster or n_trades >= 10:
            confidence = Confidence.HIGH
        elif n_trades >= 5 and n_insiders >= 2:
            confidence = Confidence.MEDIUM
        elif n_trades >= 1:
            confidence = Confidence.LOW
        else:
            confidence = Confidence.LOW

        # 4. Quality flags
        flags: list[QualityFlag] = []
        if n_trades == 0:
            flags.append(QualityFlag.LOW_COVERAGE)

        evidence = [
            Evidence(
                source_type="sec_filing",
                source_name="SEC EDGAR Form 4",
                url_or_ref="https://www.sec.gov/cgi-bin/browse-edgar",
                credibility_weight=0.95,
                extracted_facts=[note],
            )
        ] if n_trades > 0 else []

        return self._emit(
            ticker=ticker,
            run_id=run_id,
            as_of=as_of,
            score=score,
            confidence=confidence,
            direction=direction,
            materiality=materiality,
            payload={
                "window_days":            WINDOW_DAYS,
                "transaction_count":      n_trades,
                "purchase_count":         n_buys,
                "sale_count":             n_sales,
                "net_value_usd":          round(net_usd, 2),
                "unique_insiders":        n_insiders,
                "cluster_buying":         cluster,
                "director_transactions":  int(summary.get("director_transactions") or 0),
                "officer_transactions":   int(summary.get("officer_transactions") or 0),
                "narrative":              note,
            },
            evidence=evidence,
            quality_flags=flags,
        )
