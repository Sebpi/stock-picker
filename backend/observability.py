"""
observability.py — Agent health metrics, run tracking and quality flag summaries.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any

import db

logger = logging.getLogger(__name__)

# Expected agents — used to flag missing agents in health report
ALL_AGENT_IDS = [
    "agent.fundamentals",
    "agent.valuation",
    "agent.technical_risk",
    "agent.macro_liquidity",
    "agent.growth_revisions",
    "agent.sentiment_news",
    "agent.industry_competition",
    "agent.portfolio_risk",
]


def agent_health_report() -> dict[str, Any]:
    """Return per-agent health stats for the last 7 days."""
    rows = db.get_agent_health()
    health: dict[str, Any] = {}

    active_ids = {r["agent_id"] for r in rows}

    for row in rows:
        aid = row["agent_id"]
        total = row["total_runs"] or 1
        health[aid] = {
            "agent_id": aid,
            "last_run": row["last_run"],
            "last_status": row["last_status"],
            "avg_duration_secs": round(row["avg_duration_secs"] or 0, 2),
            "success_rate": round(1 - (row["failures"] or 0) / total, 3),
            "total_runs_7d": total,
            "stale": _is_stale(row["last_run"]),
        }

    # Flag agents that have never run or not seen recently
    for aid in ALL_AGENT_IDS:
        if aid not in active_ids:
            health[aid] = {
                "agent_id": aid,
                "last_run": None,
                "last_status": "never_run",
                "avg_duration_secs": None,
                "success_rate": None,
                "total_runs_7d": 0,
                "stale": True,
            }

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "agents": health,
        "summary": {
            "total_agents": len(ALL_AGENT_IDS),
            "healthy": sum(1 for a in health.values() if not a["stale"] and a.get("success_rate", 0) is not None and a["success_rate"] >= 0.8),
            "stale": sum(1 for a in health.values() if a["stale"]),
            "never_run": sum(1 for a in health.values() if a["last_run"] is None),
        },
    }


def _is_stale(last_run_str: str | None, max_hours: int = 26) -> bool:
    if not last_run_str:
        return True
    try:
        last = datetime.fromisoformat(last_run_str.replace("Z", "+00:00"))
        if last.tzinfo is None:
            last = last.replace(tzinfo=timezone.utc)
        age_hours = (datetime.now(timezone.utc) - last).total_seconds() / 3600
        return age_hours > max_hours
    except Exception:
        return True


def thesis_quality_summary(ticker: str) -> dict[str, Any]:
    """Return quality flag counts from the latest thesis for a ticker."""
    thesis = db.get_latest_thesis(ticker)
    if not thesis:
        return {"error": "No thesis found", "ticker": ticker}

    flag_counts: dict[str, int] = {}
    for flag in thesis.quality_flags:
        flag_counts[flag.value] = flag_counts.get(flag.value, 0) + 1

    # Also aggregate from agent signals
    signals = db.get_latest_signals(ticker)
    agent_flags: dict[str, list[str]] = {}
    for aid, sig in signals.items():
        if sig.quality_flags:
            agent_flags[aid] = [f.value for f in sig.quality_flags]

    return {
        "ticker": ticker,
        "thesis_id": thesis.thesis_id,
        "generated_at": thesis.generated_at.isoformat(),
        "composite_score": thesis.composite_score,
        "evidence_quality": thesis.evidence_quality.value,
        "thesis_flags": flag_counts,
        "agent_flags": agent_flags,
        "usable_agents": len([s for s in signals.values() if s.is_usable]),
        "total_agents": len(signals),
    }


def log_metric(metric: str, value: float, labels: dict[str, str] | None = None) -> None:
    """Emit a structured metric line to stdout (can be piped to any log aggregator)."""
    entry = {
        "metric": metric,
        "value": value,
        "labels": labels or {},
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    print(json.dumps(entry), flush=True)
