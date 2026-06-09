"""
evaluation.py — Forecast outcome tracking and backtesting metrics.
Runs as a daily job; fills in realised returns for matured thesis forecasts.
"""
from __future__ import annotations

import logging
import json
from datetime import datetime, timedelta, timezone
from typing import Any

import yfinance as yf

import db

logger = logging.getLogger(__name__)

HORIZON_DAYS = {"1m": 30, "3m": 91, "6m": 182, "12m": 365}
BENCHMARK_TICKER = "^NDX"  # NASDAQ-100 as benchmark


def _price_at(ticker: str, target_date: datetime) -> float | None:
    """Return close price on or after target_date (up to 5 trading days tolerance)."""
    try:
        start = target_date.strftime("%Y-%m-%d")
        end = (target_date + timedelta(days=7)).strftime("%Y-%m-%d")
        hist = yf.Ticker(ticker).history(start=start, end=end)
        if not hist.empty:
            return float(hist["Close"].iloc[0])
    except Exception as exc:
        logger.debug("Price fetch failed for %s on %s: %s", ticker, target_date, exc)
    return None


def _price_before(ticker: str, target_date: datetime) -> float | None:
    """Return close price on the nearest day at or before target_date."""
    try:
        start = (target_date - timedelta(days=5)).strftime("%Y-%m-%d")
        end = (target_date + timedelta(days=1)).strftime("%Y-%m-%d")
        hist = yf.Ticker(ticker).history(start=start, end=end)
        if not hist.empty:
            return float(hist["Close"].iloc[-1])
    except Exception as exc:
        logger.debug("Pre-price fetch failed for %s on %s: %s", ticker, target_date, exc)
    return None


def evaluate_pending_outcomes() -> int:
    """
    For each pending forecast_outcome row whose horizon has elapsed,
    fetch realised price and benchmark return, then write back to DB.
    Returns number of outcomes evaluated.
    """
    pending = db.get_pending_outcomes()
    now = datetime.now(timezone.utc)
    evaluated = 0

    for row in pending:
        thesis_date_str = row.get("thesis_generated_at")
        if not thesis_date_str:
            continue
        try:
            thesis_date = datetime.fromisoformat(thesis_date_str.replace("Z", "+00:00"))
        except Exception:
            continue

        horizon = row["horizon"]
        days = HORIZON_DAYS.get(horizon)
        if not days:
            continue

        maturity_date = thesis_date + timedelta(days=days)
        if maturity_date > now:
            continue  # not yet matured

        ticker = row["ticker"]
        start_price = _price_before(ticker, thesis_date)
        end_price = _price_at(ticker, maturity_date)

        if not start_price or not end_price:
            logger.debug("Cannot evaluate %s %s %s: missing price data", ticker, horizon, row["outcome_id"])
            continue

        realised = (end_price - start_price) / start_price * 100

        # Benchmark return for same period
        bench_start = _price_before(BENCHMARK_TICKER, thesis_date)
        bench_end = _price_at(BENCHMARK_TICKER, maturity_date)
        benchmark_return = 0.0
        sector_relative = realised
        if bench_start and bench_end:
            benchmark_return = (bench_end - bench_start) / bench_start * 100
            sector_relative = realised - benchmark_return

        forecast_return = row.get("forecast_return_pct") or 0.0
        direction_match = (realised >= 0) == (forecast_return >= 0)

        db.update_outcome(
            outcome_id=row["outcome_id"],
            realised=round(realised, 3),
            benchmark=round(benchmark_return, 3),
            sector_relative=round(sector_relative, 3),
            direction_match=direction_match,
        )
        logger.info("Evaluated %s %s %s: realised=%.1f%% forecast=%.1f%% match=%s",
                    ticker, horizon, row["outcome_id"], realised, forecast_return, direction_match)
        evaluated += 1

    return evaluated


def backtest_summary(ticker: str) -> dict[str, Any]:
    """Return accuracy and calibration metrics for a ticker."""
    return db.get_backtest_summary(ticker)


def confidence_calibration(ticker: str | None = None) -> dict[str, Any]:
    """Return accuracy broken down by confidence level across all/one ticker."""
    where = "AND fo.ticker = ?" if ticker else ""
    params = (ticker,) if ticker else ()

    with db.get_conn() as conn:
        rows = conn.execute(
            f"""
            SELECT fo.horizon,
                   fo.direction_match,
                   fo.forecast_error,
                   it.full_json
            FROM forecast_outcome fo
            JOIN investment_thesis it
              ON fo.thesis_id = it.thesis_id
            WHERE fo.realised_return_pct IS NOT NULL
            {where}
            """,  # nosec B608 — where is "" or "AND fo.ticker = ?", values parameterised
            params,
        ).fetchall()

    buckets: dict[str, dict[str, Any]] = {}
    for row in rows:
        r = dict(row)
        try:
            thesis = json.loads(r["full_json"])
            confidence = float(thesis.get("forecast", {}).get(r["horizon"], {}).get("confidence", 0.0))
        except Exception:
            confidence = 0.0
        if confidence >= 0.67:
            bucket = "high"
        elif confidence >= 0.50:
            bucket = "medium"
        else:
            bucket = "low"

        key = f"{r['horizon']}_{bucket}"
        item = buckets.setdefault(key, {
            "horizon": r["horizon"],
            "confidence_bucket": bucket,
            "total": 0,
            "correct": 0,
            "abs_error_sum": 0.0,
            "confidence_sum": 0.0,
        })
        item["total"] += 1
        item["correct"] += int(r["direction_match"] or 0)
        item["abs_error_sum"] += abs(float(r["forecast_error"] or 0.0))
        item["confidence_sum"] += confidence

    result: dict[str, Any] = {}
    for key, item in buckets.items():
        total = item["total"] or 1
        result[key] = {
            "horizon": item["horizon"],
            "confidence_bucket": item["confidence_bucket"],
            "total": item["total"],
            "hit_rate": round(item["correct"] / total, 3),
            "mae": round(item["abs_error_sum"] / total, 2),
            "avg_confidence": round(item["confidence_sum"] / total, 3),
        }
    return result
