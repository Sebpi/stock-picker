"""
SQLite database layer for the multi-agent stock forecasting system.
All tables defined here; JSON files remain for backward-compat with existing UI.
"""
from __future__ import annotations

import json
import logging
import sqlite3
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from schemas import AgentSignal, InvestmentThesis

logger = logging.getLogger(__name__)

DB_PATH = Path(__file__).parent / "stockpicker.db"


# ---------------------------------------------------------------------------
# Connection management
# ---------------------------------------------------------------------------

@contextmanager
def get_conn():
    conn = sqlite3.connect(DB_PATH, detect_types=sqlite3.PARSE_DECLTYPES)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA journal_mode=WAL")
    except sqlite3.OperationalError as exc:
        logger.warning("SQLite WAL mode unavailable for %s: %s", DB_PATH, exc)
        try:
            conn.execute("PRAGMA journal_mode=DELETE")
        except sqlite3.OperationalError:
            pass
    conn.execute("PRAGMA foreign_keys=ON")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Schema initialisation
# ---------------------------------------------------------------------------

DDL = """
CREATE TABLE IF NOT EXISTS ticker_master (
    ticker          TEXT PRIMARY KEY,
    company_name    TEXT,
    exchange        TEXT,
    sector          TEXT,
    industry_group  TEXT,
    peer_group      TEXT,   -- JSON array
    active_flag     INTEGER DEFAULT 1,
    updated_at      TEXT
);

CREATE TABLE IF NOT EXISTS agent_run (
    run_id          TEXT PRIMARY KEY,
    agent_id        TEXT NOT NULL,
    ticker          TEXT NOT NULL,
    started_at      TEXT NOT NULL,
    completed_at    TEXT,
    status          TEXT DEFAULT 'running',  -- running | completed | partial | failed
    duration_secs   REAL,
    error_code      TEXT,
    signal_id       TEXT
);

CREATE TABLE IF NOT EXISTS thesis_run (
    run_id          TEXT PRIMARY KEY,
    status          TEXT NOT NULL,
    tickers_json    TEXT NOT NULL,
    run_fresh       INTEGER DEFAULT 0,
    requested_by    TEXT,
    started_at      TEXT NOT NULL,
    completed_at    TEXT,
    completed_json  TEXT DEFAULT '[]',
    failed_json     TEXT DEFAULT '[]'
);

CREATE TABLE IF NOT EXISTS agent_signal (
    signal_id       TEXT PRIMARY KEY,
    run_id          TEXT NOT NULL,
    agent_id        TEXT NOT NULL,
    ticker          TEXT NOT NULL,
    as_of           TEXT NOT NULL,
    signal_type     TEXT,
    score           REAL,
    confidence      TEXT,
    direction       TEXT,
    materiality     TEXT,
    quality_flags   TEXT,   -- JSON array
    payload_json    TEXT,   -- full AgentSignal JSON
    created_at      TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_signal_ticker_agent
    ON agent_signal(ticker, agent_id, as_of DESC);

CREATE TABLE IF NOT EXISTS investment_thesis (
    thesis_id       TEXT PRIMARY KEY,
    run_id          TEXT NOT NULL,
    ticker          TEXT NOT NULL,
    generated_at    TEXT NOT NULL,
    current_price   REAL,
    composite_score REAL,
    risk_rating     TEXT,
    evidence_quality TEXT,
    forecast_json   TEXT,   -- {3m: {...}, 6m: {...}, 12m: {...}}
    agent_scores_json TEXT,
    narrative_json  TEXT,
    quality_flags   TEXT,
    full_json       TEXT    -- full InvestmentThesis JSON
);

CREATE INDEX IF NOT EXISTS idx_thesis_ticker
    ON investment_thesis(ticker, generated_at DESC);

CREATE TABLE IF NOT EXISTS forecast_outcome (
    outcome_id          TEXT PRIMARY KEY DEFAULT (lower(hex(randomblob(16)))),
    thesis_id           TEXT NOT NULL,
    ticker              TEXT NOT NULL,
    horizon             TEXT NOT NULL,  -- 3m | 6m | 12m
    forecast_return_pct REAL,
    realised_return_pct REAL,
    benchmark_return_pct REAL,
    sector_relative_return REAL,
    direction_match     INTEGER,        -- 1=correct, 0=wrong, NULL=pending
    forecast_error      REAL,
    evaluated_at        TEXT,
    thesis_generated_at TEXT
);

CREATE TABLE IF NOT EXISTS consensus_history (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker          TEXT NOT NULL,
    snapshot_date   TEXT NOT NULL,
    eps_consensus   REAL,
    revenue_consensus REAL,
    target_price_mean REAL,
    analyst_count   INTEGER,
    UNIQUE(ticker, snapshot_date)
);

CREATE TABLE IF NOT EXISTS valuation_history (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker          TEXT NOT NULL,
    snapshot_date   TEXT NOT NULL,
    pe_ttm          REAL,
    forward_pe      REAL,
    ev_ebitda       REAL,
    ps_ttm          REAL,
    UNIQUE(ticker, snapshot_date)
);

CREATE TABLE IF NOT EXISTS alert_log (
    alert_id        TEXT PRIMARY KEY DEFAULT (lower(hex(randomblob(16)))),
    ticker          TEXT,
    thesis_id       TEXT,
    alert_type      TEXT,
    materiality     TEXT,
    sent_at         TEXT,
    status          TEXT,
    payload_json    TEXT
);
"""


def init_db() -> None:
    with get_conn() as conn:
        conn.executescript(DDL)
    logger.info("SQLite DB initialised at %s", DB_PATH)


# ---------------------------------------------------------------------------
# Agent signal helpers
# ---------------------------------------------------------------------------

def upsert_signal(signal: AgentSignal) -> None:
    payload = signal.model_dump(mode="json")
    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO agent_signal
                (signal_id, run_id, agent_id, ticker, as_of, signal_type,
                 score, confidence, direction, materiality, quality_flags, payload_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(signal_id) DO UPDATE SET
                score=excluded.score,
                confidence=excluded.confidence,
                direction=excluded.direction,
                quality_flags=excluded.quality_flags,
                payload_json=excluded.payload_json
            """,
            (
                signal.signal_id,
                signal.run_id,
                signal.agent_id,
                signal.ticker,
                signal.as_of.isoformat(),
                signal.signal_type,
                signal.score,
                signal.confidence.value,
                signal.direction.value,
                signal.materiality.value,
                json.dumps([f.value for f in signal.quality_flags]),
                json.dumps(payload),
            ),
        )


def get_latest_signals(ticker: str, max_age_hours: int = 26) -> dict[str, AgentSignal]:
    """Return the most recent AgentSignal per agent for a ticker within max_age_hours."""
    cutoff = datetime.now(timezone.utc).timestamp() - max_age_hours * 3600
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT payload_json FROM agent_signal
            WHERE ticker = ?
            ORDER BY agent_id, as_of DESC
            """,
            (ticker,),
        ).fetchall()
    signals: dict[str, AgentSignal] = {}
    for row in rows:
        try:
            data = json.loads(row["payload_json"])
            sig = AgentSignal.model_validate(data)
            if sig.agent_id in signals:
                continue
            if sig.as_of.timestamp() < cutoff:
                continue
            signals[sig.agent_id] = sig
        except Exception as exc:
            logger.warning("Failed to deserialise signal: %s", exc)
    return signals


def get_signal_for_agent(ticker: str, agent_id: str) -> AgentSignal | None:
    with get_conn() as conn:
        row = conn.execute(
            """
            SELECT payload_json FROM agent_signal
            WHERE ticker = ? AND agent_id = ?
            ORDER BY as_of DESC LIMIT 1
            """,
            (ticker, agent_id),
        ).fetchone()
    if not row:
        return None
    try:
        return AgentSignal.model_validate(json.loads(row["payload_json"]))
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Investment thesis helpers
# ---------------------------------------------------------------------------

def store_thesis(thesis: InvestmentThesis) -> None:
    payload = thesis.model_dump(mode="json")
    with get_conn() as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO investment_thesis
                (thesis_id, run_id, ticker, generated_at, current_price,
                 composite_score, risk_rating, evidence_quality,
                 forecast_json, agent_scores_json, narrative_json,
                 quality_flags, full_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                thesis.thesis_id,
                thesis.run_id,
                thesis.ticker,
                thesis.generated_at.isoformat(),
                thesis.current_price,
                thesis.composite_score,
                thesis.risk_rating.value,
                thesis.evidence_quality.value,
                json.dumps({k: v.model_dump() for k, v in thesis.forecast.items()}),
                json.dumps(thesis.agent_scores),
                json.dumps(thesis.narrative),
                json.dumps([f.value for f in thesis.quality_flags]),
                json.dumps(payload),
            ),
        )
    # Create blank forecast_outcome rows for each horizon
    record_forecast_outcome(thesis)


def get_latest_thesis(ticker: str) -> InvestmentThesis | None:
    with get_conn() as conn:
        row = conn.execute(
            """
            SELECT full_json FROM investment_thesis
            WHERE ticker = ?
            ORDER BY generated_at DESC LIMIT 1
            """,
            (ticker,),
        ).fetchone()
    if not row:
        return None
    try:
        return InvestmentThesis.model_validate(json.loads(row["full_json"]))
    except Exception as exc:
        logger.warning("Failed to deserialise thesis for %s: %s", ticker, exc)
        return None


def get_thesis_by_id(thesis_id: str) -> InvestmentThesis | None:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT full_json FROM investment_thesis WHERE thesis_id = ?",
            (thesis_id,),
        ).fetchone()
    if not row:
        return None
    try:
        return InvestmentThesis.model_validate(json.loads(row["full_json"]))
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Forecast outcome helpers
# ---------------------------------------------------------------------------

def record_forecast_outcome(thesis: InvestmentThesis) -> None:
    with get_conn() as conn:
        for horizon, forecast in thesis.forecast.items():
            conn.execute(
                """
                INSERT OR IGNORE INTO forecast_outcome
                    (thesis_id, ticker, horizon, forecast_return_pct, thesis_generated_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    thesis.thesis_id,
                    thesis.ticker,
                    horizon,
                    forecast.base_return_pct,
                    thesis.generated_at.isoformat(),
                ),
            )


def get_pending_outcomes() -> list[dict[str, Any]]:
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT outcome_id, thesis_id, ticker, horizon,
                   forecast_return_pct, thesis_generated_at
            FROM forecast_outcome
            WHERE realised_return_pct IS NULL
              AND thesis_generated_at IS NOT NULL
            """,
        ).fetchall()
    return [dict(r) for r in rows]


def update_outcome(outcome_id: str, realised: float, benchmark: float,
                   sector_relative: float, direction_match: bool) -> None:
    row = None
    with get_conn() as conn:
        row = conn.execute(
            "SELECT forecast_return_pct FROM forecast_outcome WHERE outcome_id = ?",
            (outcome_id,),
        ).fetchone()
        if not row:
            return
        forecast_return = row["forecast_return_pct"] or 0.0
        forecast_error = realised - forecast_return
        conn.execute(
            """
            UPDATE forecast_outcome SET
                realised_return_pct = ?,
                benchmark_return_pct = ?,
                sector_relative_return = ?,
                direction_match = ?,
                forecast_error = ?,
                evaluated_at = datetime('now')
            WHERE outcome_id = ?
            """,
            (realised, benchmark, sector_relative, int(direction_match), forecast_error, outcome_id),
        )


# ---------------------------------------------------------------------------
# Agent run tracking
# ---------------------------------------------------------------------------

def start_run(agent_id: str, ticker: str) -> str:
    run_id = str(uuid.uuid4())
    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO agent_run (run_id, agent_id, ticker, started_at)
            VALUES (?, ?, ?, datetime('now'))
            """,
            (run_id, agent_id, ticker),
        )
    return run_id


def complete_run(run_id: str, signal_id: str | None = None,
                 error_code: str | None = None) -> None:
    status = "failed" if error_code else "completed"
    with get_conn() as conn:
        conn.execute(
            """
            UPDATE agent_run SET
                completed_at = datetime('now'),
                status = ?,
                duration_secs = (julianday('now') - julianday(started_at)) * 86400,
                signal_id = ?,
                error_code = ?
            WHERE run_id = ?
            """,
            (status, signal_id, error_code, run_id),
        )


def get_agent_health() -> list[dict[str, Any]]:
    with get_conn() as conn:
        rows = conn.execute(
            """
            WITH recent AS (
                SELECT *
                FROM agent_run
                WHERE started_at >= datetime('now', '-7 days')
            ),
            latest AS (
                SELECT agent_id, status AS last_status
                FROM (
                    SELECT agent_id,
                           status,
                           ROW_NUMBER() OVER (
                               PARTITION BY agent_id
                               ORDER BY COALESCE(completed_at, started_at) DESC
                           ) AS rn
                    FROM recent
                )
                WHERE rn = 1
            )
            SELECT r.agent_id,
                   MAX(r.completed_at) as last_run,
                   AVG(r.duration_secs) as avg_duration_secs,
                   SUM(CASE WHEN r.status='failed' THEN 1 ELSE 0 END) as failures,
                   COUNT(*) as total_runs,
                   l.last_status as last_status
            FROM recent r
            LEFT JOIN latest l ON l.agent_id = r.agent_id
            GROUP BY r.agent_id
            ORDER BY r.agent_id
            """,
        ).fetchall()
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# Thesis run tracking
# ---------------------------------------------------------------------------

def create_thesis_run(run_id: str, tickers: list[str], run_fresh: bool,
                      requested_by: str | None = None) -> None:
    with get_conn() as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO thesis_run
                (run_id, status, tickers_json, run_fresh, requested_by, started_at)
            VALUES (?, 'queued', ?, ?, ?, datetime('now'))
            """,
            (run_id, json.dumps(tickers), int(run_fresh), requested_by),
        )


def update_thesis_run(run_id: str, status: str | None = None,
                      completed: list[str] | None = None,
                      failed: list[str] | None = None) -> None:
    updates: list[str] = []
    params: list[Any] = []
    if status is not None:
        updates.append("status = ?")
        params.append(status)
        if status in {"completed", "partial", "failed"}:
            updates.append("completed_at = datetime('now')")
    if completed is not None:
        updates.append("completed_json = ?")
        params.append(json.dumps(completed))
    if failed is not None:
        updates.append("failed_json = ?")
        params.append(json.dumps(failed))
    if not updates:
        return
    params.append(run_id)
    with get_conn() as conn:
        conn.execute(
            f"UPDATE thesis_run SET {', '.join(updates)} WHERE run_id = ?",
            params,
        )


def get_thesis_run(run_id: str) -> dict[str, Any] | None:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM thesis_run WHERE run_id = ?",
            (run_id,),
        ).fetchone()
    if not row:
        return None
    data = dict(row)
    return {
        "run_id": data["run_id"],
        "status": data["status"],
        "tickers": json.loads(data["tickers_json"] or "[]"),
        "run_fresh": bool(data["run_fresh"]),
        "requested_by": data.get("requested_by"),
        "started_at": data["started_at"],
        "completed_at": data.get("completed_at"),
        "completed": json.loads(data.get("completed_json") or "[]"),
        "failed": json.loads(data.get("failed_json") or "[]"),
    }


def list_thesis_runs(limit: int = 20) -> list[dict[str, Any]]:
    """Return recent thesis pipeline runs, newest first."""
    safe_limit = max(1, min(int(limit or 20), 100))
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT *
            FROM thesis_run
            ORDER BY started_at DESC
            LIMIT ?
            """,
            (safe_limit,),
        ).fetchall()

    runs: list[dict[str, Any]] = []
    for row in rows:
        data = dict(row)
        runs.append({
            "run_id": data["run_id"],
            "status": data["status"],
            "tickers": json.loads(data["tickers_json"] or "[]"),
            "run_fresh": bool(data["run_fresh"]),
            "requested_by": data.get("requested_by"),
            "started_at": data["started_at"],
            "completed_at": data.get("completed_at"),
            "completed": json.loads(data.get("completed_json") or "[]"),
            "failed": json.loads(data.get("failed_json") or "[]"),
        })
    return runs


# ---------------------------------------------------------------------------
# Consensus and valuation history helpers
# ---------------------------------------------------------------------------

def snapshot_consensus(ticker: str, eps: float | None, revenue: float | None,
                        target_price: float | None, analyst_count: int | None) -> None:
    today = datetime.now(timezone.utc).date().isoformat()
    with get_conn() as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO consensus_history
                (ticker, snapshot_date, eps_consensus, revenue_consensus,
                 target_price_mean, analyst_count)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (ticker, today, eps, revenue, target_price, analyst_count),
        )


def get_consensus_n_days_ago(ticker: str, days: int) -> dict[str, Any] | None:
    with get_conn() as conn:
        row = conn.execute(
            """
            SELECT * FROM consensus_history
            WHERE ticker = ?
              AND snapshot_date <= date('now', ?)
            ORDER BY snapshot_date DESC LIMIT 1
            """,
            (ticker, f"-{days} days"),
        ).fetchone()
    return dict(row) if row else None


def snapshot_valuation(ticker: str, pe: float | None, fwd_pe: float | None,
                        ev_ebitda: float | None, ps: float | None) -> None:
    today = datetime.now(timezone.utc).date().isoformat()
    with get_conn() as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO valuation_history
                (ticker, snapshot_date, pe_ttm, forward_pe, ev_ebitda, ps_ttm)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (ticker, today, pe, fwd_pe, ev_ebitda, ps),
        )


def get_valuation_history(ticker: str, days: int = 252) -> list[dict[str, Any]]:
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT * FROM valuation_history
            WHERE ticker = ?
              AND snapshot_date >= date('now', ?)
            ORDER BY snapshot_date
            """,
            (ticker, f"-{days} days"),
        ).fetchall()
    return [dict(r) for r in rows]


def get_valuation_percentile(ticker: str, metric: str, current_value: float) -> float | None:
    history = get_valuation_history(ticker)
    values = [h[metric] for h in history if h.get(metric) is not None]
    if len(values) < 10:
        return None
    below = sum(1 for v in values if v <= current_value)
    return round(below / len(values) * 100, 1)


# ---------------------------------------------------------------------------
# Ticker master helpers
# ---------------------------------------------------------------------------

def upsert_ticker(ticker: str, info: dict[str, Any]) -> None:
    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO ticker_master
                (ticker, company_name, exchange, sector, industry_group, updated_at)
            VALUES (?, ?, ?, ?, ?, datetime('now'))
            ON CONFLICT(ticker) DO UPDATE SET
                company_name=excluded.company_name,
                sector=excluded.sector,
                industry_group=excluded.industry_group,
                updated_at=excluded.updated_at
            """,
            (
                ticker,
                info.get("shortName", info.get("longName", "")),
                info.get("exchange", ""),
                info.get("sector", ""),
                info.get("industry", ""),
            ),
        )


def get_ticker_info(ticker: str) -> dict[str, Any] | None:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM ticker_master WHERE ticker = ?", (ticker,)
        ).fetchone()
    return dict(row) if row else None


def get_peer_group(ticker: str) -> list[str]:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT peer_group FROM ticker_master WHERE ticker = ?", (ticker,)
        ).fetchone()
    if not row or not row["peer_group"]:
        return []
    try:
        return json.loads(row["peer_group"])
    except Exception:
        return []


def set_peer_group(ticker: str, peers: list[str]) -> None:
    with get_conn() as conn:
        conn.execute(
            "UPDATE ticker_master SET peer_group = ? WHERE ticker = ?",
            (json.dumps(peers), ticker),
        )


# ---------------------------------------------------------------------------
# Backtest / evaluation queries
# ---------------------------------------------------------------------------

def get_backtest_summary(ticker: str) -> dict[str, Any]:
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT horizon,
                   COUNT(*) as total,
                   SUM(direction_match) as correct,
                   AVG(ABS(forecast_error)) as mae,
                   AVG(sector_relative_return) as avg_alpha
            FROM forecast_outcome
            WHERE ticker = ?
              AND realised_return_pct IS NOT NULL
            GROUP BY horizon
            """,
            (ticker,),
        ).fetchall()
    result: dict[str, Any] = {}
    for row in rows:
        r = dict(row)
        total = r["total"] or 1
        result[r["horizon"]] = {
            "total_forecasts": total,
            "directional_hit_rate": round((r["correct"] or 0) / total, 3),
            "mean_absolute_error": round(r["mae"] or 0, 2),
            "sector_relative_alpha": round(r["avg_alpha"] or 0, 2),
        }
    return result


def get_forecast_outcome_status(ticker: str | None = None) -> dict[str, Any]:
    """Return pending/evaluated forecast outcome counts, including matured pending rows."""
    params: tuple[Any, ...] = (ticker.upper(),) if ticker else ()
    where = "WHERE ticker = ?" if ticker else ""
    with get_conn() as conn:
        rows = conn.execute(
            f"""
            SELECT outcome_id, ticker, horizon, realised_return_pct,
                   evaluated_at, thesis_generated_at
            FROM forecast_outcome
            {where}
            """,
            params,
        ).fetchall()

    horizon_days = {"3m": 91, "6m": 182, "12m": 365}
    now = datetime.now(timezone.utc)
    status: dict[str, Any] = {
        "ticker": ticker.upper() if ticker else None,
        "generated_at": now.isoformat(),
        "total": 0,
        "pending": 0,
        "evaluated": 0,
        "matured_pending": 0,
        "last_evaluated_at": None,
        "by_horizon": {},
    }

    for row in rows:
        data = dict(row)
        horizon = data["horizon"]
        item = status["by_horizon"].setdefault(horizon, {
            "total": 0,
            "pending": 0,
            "evaluated": 0,
            "matured_pending": 0,
        })
        status["total"] += 1
        item["total"] += 1

        evaluated = data["realised_return_pct"] is not None
        if evaluated:
            status["evaluated"] += 1
            item["evaluated"] += 1
            evaluated_at = data.get("evaluated_at")
            if evaluated_at and (status["last_evaluated_at"] is None or evaluated_at > status["last_evaluated_at"]):
                status["last_evaluated_at"] = evaluated_at
            continue

        status["pending"] += 1
        item["pending"] += 1
        try:
            generated = datetime.fromisoformat((data.get("thesis_generated_at") or "").replace("Z", "+00:00"))
            if generated.tzinfo is None:
                generated = generated.replace(tzinfo=timezone.utc)
            days = horizon_days.get(horizon)
            if days and generated + timedelta(days=days) <= now:
                status["matured_pending"] += 1
                item["matured_pending"] += 1
        except Exception:
            pass

    return status
