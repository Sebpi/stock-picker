"""
agent_accuracy.py — Per-agent accuracy tracking and weight recalibration.

Phase 1: Compute and persist accuracy stats by joining realized forecast
         outcomes back to the individual agent signals that contributed.

Phase 2: Auto-apply bounded weight adjustments weekly using a blend factor.
         HORIZON_WEIGHTS is mutated in-place; changes take effect on the next
         thesis generation without restart.

The key join is:
  forecast_outcome  (realized returns, per thesis/horizon)
    → investment_thesis  (via thesis_id  — gives run_id)
    → agent_signal       (via run_id + ticker — gives per-agent score/direction)
"""
from __future__ import annotations

import copy
import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any

import db
from schemas import HORIZON_WEIGHTS

logger = logging.getLogger(__name__)

BASELINE_HIT_RATE = 0.55   # expected floor for a useful agent
MIN_SAMPLES = 5             # min evaluated outcomes before reporting
MAX_WEIGHT_ADJ = 0.10       # cap ±10% per recalibration cycle
WINDOWS = [30, 60, 90]      # lookback windows to compute (days)
BLEND = 0.20                 # fraction of suggested adj applied per cycle
WEIGHT_FLOOR_RATIO = 0.50    # min weight = 50% of default
WEIGHT_CEIL_RATIO  = 2.00    # max weight = 200% of default

# Snapshot defaults at import time so resets are always available
_DEFAULT_WEIGHTS: dict[str, dict[str, float]] = copy.deepcopy(HORIZON_WEIGHTS)


def _spearman(xs: list[float], ys: list[float]) -> float | None:
    n = len(xs)
    if n < 3:
        return None

    def _rank(vals: list[float]) -> list[float]:
        order = sorted(range(n), key=lambda i: vals[i])
        ranks = [0.0] * n
        for r, i in enumerate(order):
            ranks[i] = r + 1.0
        return ranks

    rx, ry = _rank(xs), _rank(ys)
    d2 = sum((rx[i] - ry[i]) ** 2 for i in range(n))
    return 1.0 - 6.0 * d2 / (n * (n * n - 1))


def _compute_for_window(window_days: int) -> dict[str, Any]:
    """
    Join the three tables for the given lookback window and compute
    per-agent accuracy stats. Returns a summary dict.
    """
    with db.get_conn() as conn:
        rows = conn.execute(
            """
            SELECT
                fo.ticker,
                fo.horizon,
                fo.realised_return_pct,
                fo.direction_match,
                fo.forecast_error,
                s.agent_id,
                s.score     AS agent_score,
                s.direction AS agent_direction
            FROM forecast_outcome fo
            JOIN investment_thesis it ON fo.thesis_id = it.thesis_id
            JOIN agent_signal s ON s.run_id = it.run_id AND s.ticker = it.ticker
            WHERE fo.realised_return_pct IS NOT NULL
              AND fo.thesis_generated_at >= datetime('now', :neg_days)
            """,
            {"neg_days": f"-{window_days} days"},
        ).fetchall()

    if not rows:
        return {"window_days": window_days, "n_outcomes": 0, "agents": {}}

    # Group by (agent_id, horizon)
    groups: dict[tuple[str, str], list[dict]] = {}
    for row in rows:
        r = dict(row)
        groups.setdefault((r["agent_id"], r["horizon"]), []).append(r)

    now_str = datetime.now(timezone.utc).isoformat()
    results: dict[str, Any] = {}

    for (agent_id, horizon), items in groups.items():
        n = len(items)
        scores = [i["agent_score"] for i in items if i["agent_score"] is not None]
        returns = [i["realised_return_pct"] for i in items if i["realised_return_pct"] is not None]

        # Direction accuracy — only for directional signals (positive/negative)
        directional = [i for i in items if i["agent_direction"] in ("positive", "negative")]
        n_dir = len(directional)
        n_correct = sum(
            1 for i in directional
            if (i["agent_direction"] == "positive" and (i["realised_return_pct"] or 0) > 0)
            or (i["agent_direction"] == "negative" and (i["realised_return_pct"] or 0) < 0)
        )
        hit_rate = (n_correct / n_dir) if n_dir >= MIN_SAMPLES else None

        # Score partitioned by outcome correctness
        correct_items = [
            i for i in directional
            if (i["agent_direction"] == "positive" and (i["realised_return_pct"] or 0) > 0)
            or (i["agent_direction"] == "negative" and (i["realised_return_pct"] or 0) < 0)
        ]
        wrong_items = [i for i in directional if i not in correct_items]
        avg_score_correct = (
            sum(i["agent_score"] for i in correct_items) / len(correct_items)
            if correct_items else None
        )
        avg_score_wrong = (
            sum(i["agent_score"] for i in wrong_items) / len(wrong_items)
            if wrong_items else None
        )

        # Spearman correlation between agent score and realised return
        corr = _spearman(scores, returns) if len(scores) >= MIN_SAMPLES else None

        # Avg composite forecast error (shared across all agents in same thesis)
        errors = [i["forecast_error"] for i in items if i["forecast_error"] is not None]
        avg_error = sum(errors) / len(errors) if errors else None

        # Bounded weight adjustment suggestion
        adj = 0.0
        if hit_rate is not None and n_dir >= MIN_SAMPLES:
            raw = (hit_rate - BASELINE_HIT_RATE) / 0.20 * MAX_WEIGHT_ADJ
            adj = max(-MAX_WEIGHT_ADJ, min(MAX_WEIGHT_ADJ, raw))

        stat: dict[str, Any] = {
            "agent_id": agent_id,
            "horizon": horizon,
            "window_days": window_days,
            "n_evaluated": n,
            "n_direction_correct": n_correct,
            "direction_hit_rate": round(hit_rate, 3) if hit_rate is not None else None,
            "avg_score": round(sum(scores) / len(scores), 1) if scores else None,
            "avg_score_correct": round(avg_score_correct, 1) if avg_score_correct is not None else None,
            "avg_score_wrong": round(avg_score_wrong, 1) if avg_score_wrong is not None else None,
            "avg_forecast_error": round(avg_error, 2) if avg_error is not None else None,
            "score_return_corr": round(corr, 3) if corr is not None else None,
            "suggested_weight_adj": round(adj, 3),
            "computed_at": now_str,
        }

        results[f"{agent_id}|{horizon}"] = stat
        db.upsert_agent_accuracy(stat)

    logger.info("[learning] Accuracy computed: window=%dd agents=%d", window_days, len(results))
    return {"window_days": window_days, "n_outcomes": len(rows), "agents": results}


def _compute_score_buckets(window_days: int) -> list[dict]:
    """Bucket composite scores and record what each bucket actually returned."""
    with db.get_conn() as conn:
        rows = conn.execute(
            """
            SELECT fo.horizon,
                   fo.realised_return_pct,
                   fo.forecast_return_pct,
                   fo.direction_match,
                   it.composite_score
            FROM forecast_outcome fo
            JOIN investment_thesis it ON fo.thesis_id = it.thesis_id
            WHERE fo.realised_return_pct IS NOT NULL
              AND fo.thesis_generated_at >= datetime('now', :neg_days)
            """,
            {"neg_days": f"-{window_days} days"},
        ).fetchall()

    if not rows:
        return []

    buckets = [
        ("0-50",   0,    50),
        ("50-60",  50,   60),
        ("60-70",  60,   70),
        ("70-80",  70,   80),
        ("80-90",  80,   90),
        ("90-100", 90, 101),
    ]
    now_str = datetime.now(timezone.utc).isoformat()
    results = []

    for horizon in ("1m", "3m", "6m", "12m"):
        h_rows = [dict(r) for r in rows if r["horizon"] == horizon]
        for label, lo, hi in buckets:
            b = [r for r in h_rows if lo <= (r["composite_score"] or 0) < hi]
            if not b:
                continue
            n = len(b)
            stat: dict[str, Any] = {
                "bucket_label": label,
                "score_min": lo,
                "score_max": hi,
                "horizon": horizon,
                "window_days": window_days,
                "n_evaluated": n,
                "avg_realised_return": round(sum(r["realised_return_pct"] for r in b) / n, 2),
                "avg_forecast_return": round(sum(r["forecast_return_pct"] or 0 for r in b) / n, 2),
                "direction_hit_rate": round(sum(1 for r in b if r["direction_match"]) / n, 3),
                "computed_at": now_str,
            }
            results.append(stat)
            db.upsert_score_bucket(stat)

    return results


def rebuild_all(windows: list[int] | None = None, apply_adjustments: bool = False) -> dict[str, Any]:
    """Rebuild accuracy stats for all windows. Called after each evaluation cycle."""
    windows = windows or WINDOWS
    summary: dict[str, Any] = {"windows": {}}
    for w in windows:
        agent_result = _compute_for_window(w)
        bucket_result = _compute_score_buckets(w)
        summary["windows"][str(w)] = {
            "n_outcomes": agent_result.get("n_outcomes", 0),
            "n_agent_horizon_pairs": len(agent_result.get("agents", {})),
            "n_score_buckets": len(bucket_result),
        }
    summary["computed_at"] = datetime.now(timezone.utc).isoformat()
    if apply_adjustments:
        summary["recalibration"] = apply_weight_adjustments()
    return summary


def get_learning_summary(window_days: int = 90) -> dict[str, Any]:
    """Return full learning summary for the API — reads from DB, never recomputes."""
    agent_stats = db.get_agent_accuracy_all(window_days)
    bucket_stats = db.get_score_buckets_all(window_days)

    hit_rates = [
        s["direction_hit_rate"] for s in agent_stats
        if s["direction_hit_rate"] is not None and s["n_evaluated"] >= MIN_SAMPLES
    ]
    system_hit_rate = round(sum(hit_rates) / len(hit_rates), 3) if hit_rates else None

    strong, weak, insufficient = set(), set(), set()
    for s in agent_stats:
        aid = s["agent_id"]
        if s["n_evaluated"] < MIN_SAMPLES or s["direction_hit_rate"] is None:
            insufficient.add(aid)
        elif s["direction_hit_rate"] >= 0.60:
            strong.add(aid)
        elif s["direction_hit_rate"] < 0.50:
            weak.add(aid)

    return {
        "window_days": window_days,
        "system_avg_hit_rate": system_hit_rate,
        "strong_agents": sorted(strong),
        "weak_agents": sorted(weak),
        "insufficient_data_agents": sorted(insufficient),
        "agent_stats": agent_stats,
        "score_buckets": bucket_stats,
        "computed_at": datetime.now(timezone.utc).isoformat(),
    }


# ---------------------------------------------------------------------------
# Phase 2 — Weight recalibration
# ---------------------------------------------------------------------------

def apply_weight_adjustments(window_days: int = 90) -> dict[str, Any]:
    """
    Blend suggested_weight_adj from agent_accuracy into HORIZON_WEIGHTS.

    new_weight = current * (1 + BLEND * suggested_adj)
    Hard bounds: [50%, 200%] of the default weight snapshot.
    Returns a summary and persists the calibration to DB.
    """
    stats = db.get_agent_accuracy_all(window_days)
    if not stats:
        return {"applied": False, "reason": "no_accuracy_data", "n_adjusted": 0}

    deltas: dict[str, Any] = {}
    n_adjusted = 0

    for stat in stats:
        agent_id = stat["agent_id"]
        horizon = stat["horizon"]
        adj = stat.get("suggested_weight_adj") or 0.0

        if abs(adj) < 0.001:
            continue
        if (stat.get("n_evaluated") or 0) < MIN_SAMPLES:
            continue

        current_w = HORIZON_WEIGHTS.get(horizon, {}).get(agent_id)
        default_w = _DEFAULT_WEIGHTS.get(horizon, {}).get(agent_id)

        if current_w is None or default_w is None or default_w <= 0:
            continue

        raw_new = current_w * (1.0 + BLEND * adj)
        new_w = round(max(default_w * WEIGHT_FLOOR_RATIO, min(default_w * WEIGHT_CEIL_RATIO, raw_new)), 6)

        if abs(new_w - current_w) < 1e-8:
            continue

        HORIZON_WEIGHTS[horizon][agent_id] = new_w
        n_adjusted += 1
        deltas[f"{agent_id}|{horizon}"] = {
            "agent_id": agent_id,
            "horizon": horizon,
            "default": round(default_w, 6),
            "before": round(current_w, 6),
            "after": round(new_w, 6),
            "delta_pct": round((new_w - current_w) / current_w * 100, 1),
        }

    if n_adjusted == 0:
        return {"applied": False, "reason": "no_material_adjustments", "n_adjusted": 0}

    calibration_id = f"cal_{uuid.uuid4().hex}"
    db.store_calibrated_weights({
        "calibration_id": calibration_id,
        "applied_at": datetime.now(timezone.utc).isoformat(),
        "window_days": window_days,
        "n_agents_adj": n_adjusted,
        "weights_json": json.dumps(HORIZON_WEIGHTS),
        "deltas_json": json.dumps(deltas),
    })

    logger.info("[learning] Weights recalibrated: %d agent/horizon pairs adjusted", n_adjusted)
    return {
        "applied": True,
        "calibration_id": calibration_id,
        "n_adjusted": n_adjusted,
        "deltas": deltas,
    }


def load_calibrated_weights() -> bool:
    """Load the latest calibrated weights from DB into HORIZON_WEIGHTS. Called at startup."""
    row = db.get_latest_calibrated_weights()
    if not row:
        return False
    try:
        weights = json.loads(row["weights_json"])
        for horizon, agent_weights in weights.items():
            if horizon in HORIZON_WEIGHTS:
                for agent_id, w in agent_weights.items():
                    if agent_id in HORIZON_WEIGHTS[horizon]:
                        HORIZON_WEIGHTS[horizon][agent_id] = float(w)
        logger.info("[learning] Loaded calibrated weights from %s", row["applied_at"])
        return True
    except Exception as exc:
        logger.warning("[learning] Failed to load calibrated weights: %s", exc)
        return False


def reset_to_default_weights() -> None:
    """Restore HORIZON_WEIGHTS to the defaults snapshotted at module load."""
    for horizon, agents in _DEFAULT_WEIGHTS.items():
        for agent_id, w in agents.items():
            HORIZON_WEIGHTS[horizon][agent_id] = w
    logger.info("[learning] Weights reset to defaults")


def get_weight_status() -> dict[str, Any]:
    """Return current vs default weights with deltas. Used by /v1/learning/weights."""
    calibration = db.get_latest_calibrated_weights()
    history = db.list_calibration_history(10)
    rows = []
    for horizon in sorted(HORIZON_WEIGHTS):
        for agent_id in sorted(HORIZON_WEIGHTS[horizon]):
            current = HORIZON_WEIGHTS[horizon][agent_id]
            default = _DEFAULT_WEIGHTS.get(horizon, {}).get(agent_id, current)
            rows.append({
                "agent_id": agent_id,
                "horizon": horizon,
                "default_weight": round(default, 6),
                "current_weight": round(current, 6),
                "delta_pct": round((current - default) / default * 100, 1) if default else 0.0,
                "is_adjusted": abs(current - default) > 1e-8,
            })
    return {
        "weights": rows,
        "last_calibrated_at": calibration["applied_at"] if calibration else None,
        "calibration_id": calibration["calibration_id"] if calibration else None,
        "n_adjusted": sum(1 for r in rows if r["is_adjusted"]),
        "calibration_history": history,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }
