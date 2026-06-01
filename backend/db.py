"""
SQLite database layer for the multi-agent stock forecasting system.
All tables defined here; JSON files remain for backward-compat with existing UI.
"""
from __future__ import annotations

import json
import logging
import os
import shutil
import sqlite3
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from schemas import AgentSignal, InvestmentThesis

logger = logging.getLogger(__name__)

_DATA_DIR = Path(os.getenv("DATA_DIR", str(Path(__file__).parent.parent / "data")))
_DATA_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = _DATA_DIR / "stockpicker.db"
_LEGACY_DB_PATH = Path(__file__).parent / "stockpicker.db"
if _LEGACY_DB_PATH.exists() and not DB_PATH.exists():
    try:
        shutil.copy2(_LEGACY_DB_PATH, DB_PATH)
    except Exception as exc:
        logger.warning("Could not migrate legacy SQLite DB to DATA_DIR: %s", exc)
RETENTION_DAYS = 365


def _safe_json_loads(text: str, default: Any) -> Any:
    """json.loads with a safe fallback — logs on corruption instead of raising."""
    try:
        return json.loads(text)
    except Exception as exc:
        logger.warning("JSON decode error (returning default): %s", exc)
        return default


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

CREATE TABLE IF NOT EXISTS prediction_run (
    run_id              TEXT PRIMARY KEY,
    status              TEXT NOT NULL,
    tickers_json        TEXT NOT NULL,
    model_version       TEXT,
    prompt_version      TEXT,
    source              TEXT,
    started_at          TEXT NOT NULL,
    completed_at        TEXT,
    prediction_count    INTEGER DEFAULT 0,
    error               TEXT,
    meta_json           TEXT DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS idx_prediction_run_started
    ON prediction_run(started_at DESC);

CREATE TABLE IF NOT EXISTS prediction_snapshot (
    prediction_id       TEXT PRIMARY KEY,
    run_id              TEXT,
    ticker              TEXT NOT NULL,
    prediction_date     TEXT NOT NULL,
    generated_at        TEXT,
    model_version       TEXT,
    prompt_version      TEXT,
    name                TEXT,
    direction           TEXT,
    score               REAL,
    confidence          TEXT,
    predicted_1d_pct    REAL,
    predicted_1w_pct    REAL,
    predicted_1m_pct    REAL,
    predicted_3m_pct    REAL,
    predicted_6m_pct    REAL,
    predicted_12m_pct   REAL,
    raw_predicted_pct   REAL,
    bias_correction     REAL,
    inverted            INTEGER DEFAULT 0,
    price_at_prediction REAL,
    factor_scores_json  TEXT,
    dcf_json            TEXT,
    macro_json          TEXT,
    payload_json        TEXT,
    created_at          TEXT DEFAULT (datetime('now')),
    updated_at          TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_prediction_snapshot_ticker_date
    ON prediction_snapshot(ticker, prediction_date DESC);

CREATE INDEX IF NOT EXISTS idx_prediction_snapshot_model
    ON prediction_snapshot(model_version, prompt_version, prediction_date DESC);

CREATE TABLE IF NOT EXISTS prediction_outcome (
    outcome_id          TEXT PRIMARY KEY DEFAULT (lower(hex(randomblob(16)))),
    prediction_id       TEXT NOT NULL,
    ticker              TEXT NOT NULL,
    prediction_date     TEXT NOT NULL,
    horizon             TEXT NOT NULL,
    target_date         TEXT NOT NULL,
    forecast_return_pct REAL,
    realised_return_pct REAL,
    direction_match     INTEGER,
    forecast_error      REAL,
    evaluated_at        TEXT,
    status              TEXT DEFAULT 'pending',
    created_at          TEXT DEFAULT (datetime('now')),
    UNIQUE(prediction_id, horizon),
    FOREIGN KEY(prediction_id) REFERENCES prediction_snapshot(prediction_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_prediction_outcome_due
    ON prediction_outcome(status, target_date);

CREATE INDEX IF NOT EXISTS idx_prediction_outcome_ticker
    ON prediction_outcome(ticker, horizon, prediction_date DESC);

CREATE TABLE IF NOT EXISTS prediction_calibration (
    calibration_id      TEXT PRIMARY KEY,
    model_version       TEXT,
    prompt_version      TEXT,
    generated_at        TEXT NOT NULL,
    sample_count        INTEGER DEFAULT 0,
    calibration_json    TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_prediction_calibration_generated
    ON prediction_calibration(generated_at DESC);

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
    prune_agent_history()
    logger.info("SQLite DB initialised at %s", DB_PATH)


def prune_agent_history() -> dict[str, int]:
    """Delete cached agent/thesis results older than 12 months."""
    cutoff = (datetime.now(timezone.utc) - timedelta(days=RETENTION_DAYS)).isoformat()
    with get_conn() as conn:
        outcome_cur = conn.execute(
            "DELETE FROM forecast_outcome WHERE thesis_generated_at IS NOT NULL AND thesis_generated_at < ?",
            (cutoff,),
        )
        thesis_cur = conn.execute("DELETE FROM investment_thesis WHERE generated_at < ?", (cutoff,))
        signal_cur = conn.execute("DELETE FROM agent_signal WHERE as_of < ?", (cutoff,))
        run_cur = conn.execute("DELETE FROM agent_run WHERE started_at < ?", (cutoff,))
        thesis_run_cur = conn.execute("DELETE FROM thesis_run WHERE started_at < ?", (cutoff,))
        pred_outcome_cur = conn.execute("DELETE FROM prediction_outcome WHERE prediction_date < ?", (cutoff[:10],))
        pred_snapshot_cur = conn.execute("DELETE FROM prediction_snapshot WHERE prediction_date < ?", (cutoff[:10],))
        pred_run_cur = conn.execute("DELETE FROM prediction_run WHERE started_at < ?", (cutoff,))
        pred_cal_cur = conn.execute("DELETE FROM prediction_calibration WHERE generated_at < ?", (cutoff,))
    return {
        "forecast_outcomes": outcome_cur.rowcount or 0,
        "investment_theses": thesis_cur.rowcount or 0,
        "agent_signals": signal_cur.rowcount or 0,
        "agent_runs": run_cur.rowcount or 0,
        "thesis_runs": thesis_run_cur.rowcount or 0,
        "prediction_outcomes": pred_outcome_cur.rowcount or 0,
        "prediction_snapshots": pred_snapshot_cur.rowcount or 0,
        "prediction_runs": pred_run_cur.rowcount or 0,
        "prediction_calibrations": pred_cal_cur.rowcount or 0,
    }


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
    prune_agent_history()
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
    prune_agent_history()
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
    prune_agent_history()
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


def get_latest_scores(tickers: list[str]) -> dict[str, float]:
    """Return {ticker: composite_score} for the most recent thesis of each ticker."""
    if not tickers:
        return {}
    placeholders = ",".join("?" * len(tickers))
    with get_conn() as conn:
        rows = conn.execute(  # nosec B608 — placeholders is only '?,?,...'
            f"""
            SELECT ticker, composite_score
            FROM investment_thesis t1
            WHERE ticker IN ({placeholders})
              AND generated_at = (
                SELECT MAX(generated_at) FROM investment_thesis t2
                WHERE t2.ticker = t1.ticker
              )
            """,
            tickers,
        ).fetchall()
    return {row["ticker"]: row["composite_score"] for row in rows}


def get_thesis_history(ticker: str, limit: int = 10) -> list[dict]:
    """Return lightweight thesis summaries for a ticker, newest first."""
    prune_agent_history()
    cutoff = (datetime.now(timezone.utc) - timedelta(days=RETENTION_DAYS)).isoformat()
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT thesis_id, generated_at, composite_score, risk_rating,
                   evidence_quality, current_price
            FROM investment_thesis
            WHERE ticker = ? AND generated_at >= ?
            ORDER BY generated_at DESC
            LIMIT ?
            """,
            (ticker, cutoff, max(1, min(limit, 50))),
        ).fetchall()
    return [dict(r) for r in rows]


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
# Prediction memory helpers
# ---------------------------------------------------------------------------

PREDICTION_OUTCOME_HORIZONS: dict[str, int] = {
    "1d": 1,
    "1w": 7,
    "1m": 30,
    "3m": 91,
    "6m": 182,
    "12m": 365,
}


def _safe_float(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if number == number and abs(number) != float("inf") else None


def _safe_date(value: Any) -> str:
    if value:
        text = str(value).strip()
        if len(text) >= 10:
            candidate = text[:10]
            try:
                datetime.fromisoformat(candidate)
                return candidate
            except Exception:
                pass
    return datetime.now(timezone.utc).date().isoformat()


def _json_dumps(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"), default=str)


def prediction_id_for(
    prediction: dict[str, Any],
    model_version: str = "pred-v1",
    prompt_version: str = "prompt-v1",
) -> str:
    """Deterministic ID so JSON predictions can be synced into SQLite repeatedly."""
    ticker = str(prediction.get("ticker") or "").upper().strip()
    prediction_date = _safe_date(prediction.get("date") or prediction.get("prediction_date"))
    key = f"stocklens:prediction:{ticker}:{prediction_date}:{model_version}:{prompt_version}"
    return f"pred_{uuid.uuid5(uuid.NAMESPACE_URL, key).hex}"


def _target_date(prediction_date: str, horizon: str) -> str:
    try:
        base = datetime.fromisoformat(prediction_date[:10])
    except Exception:
        base = datetime.now(timezone.utc)
    return (base + timedelta(days=PREDICTION_OUTCOME_HORIZONS[horizon])).date().isoformat()


def create_prediction_run(
    run_id: str,
    tickers: list[str],
    model_version: str,
    prompt_version: str,
    source: str = "manual",
    meta: dict[str, Any] | None = None,
) -> None:
    with get_conn() as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO prediction_run
                (run_id, status, tickers_json, model_version, prompt_version,
                 source, started_at, meta_json)
            VALUES (?, 'running', ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                _json_dumps(tickers),
                model_version,
                prompt_version,
                source,
                datetime.now(timezone.utc).isoformat(),
                _json_dumps(meta or {}),
            ),
        )


def complete_prediction_run(
    run_id: str,
    status: str,
    prediction_count: int = 0,
    error: str | None = None,
) -> None:
    with get_conn() as conn:
        conn.execute(
            """
            UPDATE prediction_run SET
                status = ?,
                completed_at = ?,
                prediction_count = ?,
                error = ?
            WHERE run_id = ?
            """,
            (
                status,
                datetime.now(timezone.utc).isoformat(),
                int(prediction_count or 0),
                error,
                run_id,
            ),
        )


def store_prediction_snapshot(
    prediction: dict[str, Any],
    run_id: str | None = None,
    model_version: str = "pred-v1",
    prompt_version: str = "prompt-v1",
    macro: dict[str, Any] | None = None,
) -> str:
    """Upsert a generated prediction and its pending evaluation horizons."""
    ticker = str(prediction.get("ticker") or "").upper().strip()
    if not ticker:
        raise ValueError("prediction missing ticker")
    prediction_date = _safe_date(prediction.get("date") or prediction.get("prediction_date"))
    prediction_id = (
        str(prediction.get("prediction_id") or "").strip()
        or prediction_id_for(prediction, model_version, prompt_version)
    )
    generated_at = prediction.get("generated_at") or datetime.now(timezone.utc).isoformat()
    predicted_1d = _safe_float(prediction.get("predicted_1d_pct"))
    if predicted_1d is None:
        predicted_1d = _safe_float(prediction.get("predicted_pct"))

    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO prediction_snapshot
                (prediction_id, run_id, ticker, prediction_date, generated_at,
                 model_version, prompt_version, name, direction, score, confidence,
                 predicted_1d_pct, predicted_1w_pct, predicted_1m_pct,
                 predicted_3m_pct, predicted_6m_pct, predicted_12m_pct,
                 raw_predicted_pct, bias_correction, inverted, price_at_prediction,
                 factor_scores_json, dcf_json, macro_json, payload_json, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(prediction_id) DO UPDATE SET
                run_id=COALESCE(excluded.run_id, prediction_snapshot.run_id),
                generated_at=excluded.generated_at,
                name=excluded.name,
                direction=excluded.direction,
                score=excluded.score,
                confidence=excluded.confidence,
                predicted_1d_pct=excluded.predicted_1d_pct,
                predicted_1w_pct=excluded.predicted_1w_pct,
                predicted_1m_pct=excluded.predicted_1m_pct,
                predicted_3m_pct=excluded.predicted_3m_pct,
                predicted_6m_pct=excluded.predicted_6m_pct,
                predicted_12m_pct=excluded.predicted_12m_pct,
                raw_predicted_pct=excluded.raw_predicted_pct,
                bias_correction=excluded.bias_correction,
                inverted=excluded.inverted,
                price_at_prediction=COALESCE(excluded.price_at_prediction, prediction_snapshot.price_at_prediction),
                factor_scores_json=excluded.factor_scores_json,
                dcf_json=excluded.dcf_json,
                macro_json=COALESCE(excluded.macro_json, prediction_snapshot.macro_json),
                payload_json=excluded.payload_json,
                updated_at=excluded.updated_at
            """,
            (
                prediction_id,
                run_id,
                ticker,
                prediction_date,
                str(generated_at),
                model_version,
                prompt_version,
                prediction.get("name"),
                prediction.get("direction"),
                _safe_float(prediction.get("score")),
                prediction.get("confidence"),
                predicted_1d,
                _safe_float(prediction.get("predicted_1w_pct")),
                _safe_float(prediction.get("predicted_1m_pct")),
                _safe_float(prediction.get("predicted_3m_pct")),
                _safe_float(prediction.get("predicted_6m_pct")),
                _safe_float(prediction.get("predicted_12m_pct")),
                _safe_float(prediction.get("raw_predicted_pct")),
                _safe_float(prediction.get("bias_correction")),
                1 if prediction.get("inverted") else 0,
                _safe_float(prediction.get("price_at_prediction")),
                _json_dumps(prediction.get("factor_scores") or {}),
                _json_dumps(prediction.get("dcf") or {}),
                _json_dumps(macro) if macro is not None else None,
                _json_dumps(prediction),
                datetime.now(timezone.utc).isoformat(),
            ),
        )

        forecast_by_horizon = {
            "1d": predicted_1d,
            "1w": _safe_float(prediction.get("predicted_1w_pct")),
            "1m": _safe_float(prediction.get("predicted_1m_pct")),
            "3m": _safe_float(prediction.get("predicted_3m_pct")),
            "6m": _safe_float(prediction.get("predicted_6m_pct")),
            "12m": _safe_float(prediction.get("predicted_12m_pct")),
        }
        for horizon, forecast in forecast_by_horizon.items():
            conn.execute(
                """
                INSERT INTO prediction_outcome
                    (prediction_id, ticker, prediction_date, horizon, target_date,
                     forecast_return_pct)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(prediction_id, horizon) DO UPDATE SET
                    ticker=excluded.ticker,
                    prediction_date=excluded.prediction_date,
                    target_date=excluded.target_date,
                    forecast_return_pct=excluded.forecast_return_pct
                """,
                (
                    prediction_id,
                    ticker,
                    prediction_date,
                    horizon,
                    _target_date(prediction_date, horizon),
                    forecast,
                ),
            )
    return prediction_id


def sync_prediction_history(
    predictions: list[dict[str, Any]],
    model_version: str = "pred-v1",
    prompt_version: str = "prompt-v1",
) -> int:
    """Backfill/sync JSON predictions into the durable SQLite learning store."""
    synced = 0
    for prediction in predictions:
        if not isinstance(prediction, dict) or not prediction.get("ticker") or not prediction.get("date"):
            continue
        try:
            store_prediction_snapshot(
                prediction,
                model_version=str(prediction.get("model_version") or model_version),
                prompt_version=str(prediction.get("prompt_version") or prompt_version),
            )
            synced += 1
        except Exception as exc:
            logger.warning("Could not sync prediction %s: %s", prediction.get("ticker"), exc)
    return synced


def list_due_prediction_outcomes(limit: int = 100) -> list[dict[str, Any]]:
    safe_limit = max(1, min(int(limit or 100), 500))
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT po.outcome_id, po.prediction_id, po.ticker, po.prediction_date,
                   po.horizon, po.target_date, po.forecast_return_pct,
                   ps.price_at_prediction
            FROM prediction_outcome po
            LEFT JOIN prediction_snapshot ps ON ps.prediction_id = po.prediction_id
            WHERE po.realised_return_pct IS NULL
              AND po.status = 'pending'
              AND po.target_date <= date('now')
            ORDER BY po.target_date ASC
            LIMIT ?
            """,
            (safe_limit,),
        ).fetchall()
    return [dict(r) for r in rows]


def update_prediction_outcome(
    outcome_id: str,
    realised_return_pct: float,
    direction_match: bool | None,
    forecast_error: float | None,
) -> None:
    match_value = None if direction_match is None else int(direction_match)
    with get_conn() as conn:
        conn.execute(
            """
            UPDATE prediction_outcome SET
                realised_return_pct = ?,
                direction_match = ?,
                forecast_error = ?,
                evaluated_at = ?,
                status = 'evaluated'
            WHERE outcome_id = ?
            """,
            (
                round(float(realised_return_pct), 4),
                match_value,
                None if forecast_error is None else round(float(forecast_error), 4),
                datetime.now(timezone.utc).isoformat(),
                outcome_id,
            ),
        )


def get_prediction_learning_summary(limit_tickers: int = 20) -> dict[str, Any]:
    with get_conn() as conn:
        overall = dict(conn.execute(
            """
            SELECT COUNT(*) AS total,
                   SUM(CASE WHEN realised_return_pct IS NOT NULL THEN 1 ELSE 0 END) AS evaluated,
                   SUM(CASE WHEN realised_return_pct IS NULL THEN 1 ELSE 0 END) AS pending,
                   SUM(CASE WHEN realised_return_pct IS NULL AND target_date <= date('now') THEN 1 ELSE 0 END) AS matured_pending,
                   MAX(evaluated_at) AS last_evaluated_at
            FROM prediction_outcome
            """
        ).fetchone())
        horizon_rows = conn.execute(
            """
            SELECT horizon,
                   COUNT(*) AS total,
                   SUM(CASE WHEN realised_return_pct IS NOT NULL THEN 1 ELSE 0 END) AS evaluated,
                   SUM(CASE WHEN realised_return_pct IS NULL THEN 1 ELSE 0 END) AS pending,
                   SUM(CASE WHEN realised_return_pct IS NULL AND target_date <= date('now') THEN 1 ELSE 0 END) AS matured_pending,
                   SUM(CASE WHEN direction_match = 1 THEN 1 ELSE 0 END) AS correct,
                   SUM(CASE WHEN direction_match IS NOT NULL THEN 1 ELSE 0 END) AS scored,
                   AVG(ABS(forecast_error)) AS mae,
                   AVG(forecast_return_pct) AS avg_forecast,
                   AVG(realised_return_pct) AS avg_realised
            FROM prediction_outcome
            GROUP BY horizon
            """
        ).fetchall()
        ticker_rows = conn.execute(
            """
            SELECT ticker,
                   COUNT(*) AS evaluated,
                   SUM(CASE WHEN direction_match = 1 THEN 1 ELSE 0 END) AS correct,
                   AVG(ABS(forecast_error)) AS mae,
                   AVG(realised_return_pct) AS avg_realised
            FROM prediction_outcome
            WHERE direction_match IS NOT NULL
            GROUP BY ticker
            ORDER BY evaluated DESC, ticker ASC
            LIMIT ?
            """,
            (max(1, min(int(limit_tickers or 20), 100)),),
        ).fetchall()
        run_rows = conn.execute(
            """
            SELECT run_id, status, tickers_json, model_version, prompt_version,
                   source, started_at, completed_at, prediction_count, error
            FROM prediction_run
            ORDER BY started_at DESC
            LIMIT 5
            """
        ).fetchall()

    horizon_order = {name: idx for idx, name in enumerate(PREDICTION_OUTCOME_HORIZONS)}
    by_horizon = []
    for row in sorted([dict(r) for r in horizon_rows], key=lambda r: horizon_order.get(r["horizon"], 99)):
        scored = row.get("scored") or 0
        by_horizon.append({
            "horizon": row["horizon"],
            "total": row.get("total") or 0,
            "evaluated": row.get("evaluated") or 0,
            "pending": row.get("pending") or 0,
            "matured_pending": row.get("matured_pending") or 0,
            "directional_hit_rate_pct": round(((row.get("correct") or 0) / scored) * 100, 1) if scored else None,
            "mean_absolute_error_pct": round(row["mae"], 2) if row.get("mae") is not None else None,
            "avg_forecast_pct": round(row["avg_forecast"], 2) if row.get("avg_forecast") is not None else None,
            "avg_realised_pct": round(row["avg_realised"], 2) if row.get("avg_realised") is not None else None,
        })

    by_ticker = []
    for row in ticker_rows:
        data = dict(row)
        evaluated = data.get("evaluated") or 0
        by_ticker.append({
            "ticker": data["ticker"],
            "evaluated": evaluated,
            "directional_hit_rate_pct": round(((data.get("correct") or 0) / evaluated) * 100, 1) if evaluated else None,
            "mean_absolute_error_pct": round(data["mae"], 2) if data.get("mae") is not None else None,
            "avg_realised_pct": round(data["avg_realised"], 2) if data.get("avg_realised") is not None else None,
        })

    runs = []
    for row in run_rows:
        data = dict(row)
        tickers_json = data.pop("tickers_json", "[]")
        runs.append({
            **data,
            "tickers": _safe_json_loads(tickers_json or "[]", []),
        })

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "total_outcomes": overall.get("total") or 0,
        "evaluated_outcomes": overall.get("evaluated") or 0,
        "pending_outcomes": overall.get("pending") or 0,
        "matured_pending_outcomes": overall.get("matured_pending") or 0,
        "last_evaluated_at": overall.get("last_evaluated_at"),
        "by_horizon": by_horizon,
        "by_ticker": by_ticker,
        "recent_runs": runs,
        "retention_days": RETENTION_DAYS,
    }


def _pearson(xs: list[float], ys: list[float]) -> float | None:
    n = len(xs)
    if n < 3 or n != len(ys):
        return None
    mean_x = sum(xs) / n
    mean_y = sum(ys) / n
    dx = [x - mean_x for x in xs]
    dy = [y - mean_y for y in ys]
    denom_x = sum(x * x for x in dx)
    denom_y = sum(y * y for y in dy)
    if denom_x <= 0 or denom_y <= 0:
        return None
    return sum(x * y for x, y in zip(dx, dy)) / ((denom_x * denom_y) ** 0.5)


def build_prediction_calibration_model(min_samples: int = 3) -> dict[str, Any]:
    """Build a lightweight calibration model from evaluated prediction outcomes."""
    min_samples = max(1, int(min_samples or 3))
    with get_conn() as conn:
        stat_rows = conn.execute(
            """
            SELECT ticker, horizon,
                   COUNT(*) AS samples,
                   SUM(CASE WHEN direction_match = 1 THEN 1 ELSE 0 END) AS correct,
                   AVG(forecast_error) AS mean_error,
                   AVG(ABS(forecast_error)) AS mae,
                   AVG(forecast_return_pct) AS avg_forecast,
                   AVG(realised_return_pct) AS avg_realised
            FROM prediction_outcome
            WHERE realised_return_pct IS NOT NULL
              AND forecast_return_pct IS NOT NULL
            GROUP BY ticker, horizon
            """
        ).fetchall()
        global_rows = conn.execute(
            """
            SELECT horizon,
                   COUNT(*) AS samples,
                   SUM(CASE WHEN direction_match = 1 THEN 1 ELSE 0 END) AS correct,
                   AVG(forecast_error) AS mean_error,
                   AVG(ABS(forecast_error)) AS mae,
                   AVG(forecast_return_pct) AS avg_forecast,
                   AVG(realised_return_pct) AS avg_realised
            FROM prediction_outcome
            WHERE realised_return_pct IS NOT NULL
              AND forecast_return_pct IS NOT NULL
            GROUP BY horizon
            """
        ).fetchall()
        factor_rows = conn.execute(
            """
            SELECT po.horizon, po.realised_return_pct, po.direction_match,
                   ps.factor_scores_json, ps.dcf_json
            FROM prediction_outcome po
            JOIN prediction_snapshot ps ON ps.prediction_id = po.prediction_id
            WHERE po.realised_return_pct IS NOT NULL
              AND po.forecast_return_pct IS NOT NULL
            """
        ).fetchall()

    def _stat(row: sqlite3.Row) -> dict[str, Any]:
        samples = row["samples"] or 0
        correct = row["correct"] or 0
        hit_rate = (correct / samples) * 100 if samples else None
        return {
            "samples": samples,
            "directional_hit_rate_pct": round(hit_rate, 1) if hit_rate is not None else None,
            "mean_error_pct": round(row["mean_error"], 3) if row["mean_error"] is not None else None,
            "mean_absolute_error_pct": round(row["mae"], 3) if row["mae"] is not None else None,
            "avg_forecast_pct": round(row["avg_forecast"], 3) if row["avg_forecast"] is not None else None,
            "avg_realised_pct": round(row["avg_realised"], 3) if row["avg_realised"] is not None else None,
            "eligible": samples >= min_samples,
            "invert_signal": samples >= max(5, min_samples) and hit_rate is not None and hit_rate < 45.0,
            "downshift_confidence": samples >= min_samples and hit_rate is not None and hit_rate < 50.0,
        }

    global_stats: dict[str, Any] = {}
    for row in global_rows:
        global_stats[row["horizon"]] = _stat(row)

    by_ticker: dict[str, dict[str, Any]] = {}
    for row in stat_rows:
        by_ticker.setdefault(row["ticker"], {})[row["horizon"]] = _stat(row)

    factor_names = ("value", "momentum", "quality", "growth", "composite")
    factor_points: dict[str, dict[str, dict[str, list[float]]]] = {}
    for row in factor_rows:
        horizon = row["horizon"]
        realised = _safe_float(row["realised_return_pct"])
        if realised is None:
            continue
        try:
            factors = json.loads(row["factor_scores_json"] or "{}")
        except Exception:
            factors = {}
        try:
            dcf = json.loads(row["dcf_json"] or "{}")
        except Exception:
            dcf = {}
        values = {name: _safe_float(factors.get(name)) for name in factor_names}
        values["margin_of_safety"] = _safe_float(dcf.get("margin_of_safety_pct"))
        for factor, score in values.items():
            if score is None:
                continue
            bucket = factor_points.setdefault(horizon, {}).setdefault(factor, {"scores": [], "returns": []})
            bucket["scores"].append(float(score))
            bucket["returns"].append(realised)

    factor_learning: dict[str, dict[str, Any]] = {}
    for horizon, factors in factor_points.items():
        factor_learning[horizon] = {}
        for factor, values in factors.items():
            n = len(values["scores"])
            corr = _pearson(values["scores"], values["returns"])
            factor_learning[horizon][factor] = {
                "samples": n,
                "correlation": round(corr, 4) if corr is not None else None,
                "eligible": n >= max(8, min_samples),
                "direction": (
                    "positive" if corr is not None and corr > 0.05
                    else "negative" if corr is not None and corr < -0.05
                    else "weak"
                ),
            }

    one_day = global_stats.get("1d") or {}
    sample_count = sum((row["samples"] or 0) for row in global_rows)
    recommendations: list[str] = []
    if not sample_count:
        recommendations.append("No evaluated prediction outcomes yet; calibration will activate after the first horizons mature.")
    elif one_day.get("samples", 0) < min_samples:
        recommendations.append(f"Need at least {min_samples} evaluated 1d outcomes before applying short-horizon calibration.")
    elif one_day.get("directional_hit_rate_pct") is not None and one_day["directional_hit_rate_pct"] < 50:
        recommendations.append("1d directional hit rate is below 50%; future high-confidence calls should be downshifted until accuracy improves.")
    if one_day.get("mean_error_pct") is not None and abs(one_day["mean_error_pct"]) >= 0.25:
        recommendations.append(f"Average 1d forecast error is {one_day['mean_error_pct']:+.2f}%; apply partial bias correction.")

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "min_samples": min_samples,
        "sample_count": sample_count,
        "global": global_stats,
        "by_ticker": by_ticker,
        "factor_learning": factor_learning,
        "recommendations": recommendations,
    }


def store_prediction_calibration(
    model_version: str,
    prompt_version: str,
    calibration: dict[str, Any],
) -> str:
    calibration_id = f"cal_{uuid.uuid4().hex}"
    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO prediction_calibration
                (calibration_id, model_version, prompt_version, generated_at,
                 sample_count, calibration_json)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                calibration_id,
                model_version,
                prompt_version,
                calibration.get("generated_at") or datetime.now(timezone.utc).isoformat(),
                int(calibration.get("sample_count") or 0),
                _json_dumps(calibration),
            ),
        )
    return calibration_id


def get_latest_prediction_calibration() -> dict[str, Any] | None:
    with get_conn() as conn:
        row = conn.execute(
            """
            SELECT calibration_json
            FROM prediction_calibration
            ORDER BY generated_at DESC
            LIMIT 1
            """
        ).fetchone()
    if not row:
        return None
    return _safe_json_loads(row["calibration_json"], None)


def list_prediction_calibrations(limit: int = 10) -> list[dict[str, Any]]:
    safe_limit = max(1, min(int(limit or 10), 50))
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT calibration_id, model_version, prompt_version, generated_at,
                   sample_count, calibration_json
            FROM prediction_calibration
            ORDER BY generated_at DESC
            LIMIT ?
            """,
            (safe_limit,),
        ).fetchall()

    result: list[dict[str, Any]] = []
    for row in rows:
        data = dict(row)
        calibration = _safe_json_loads(data.pop("calibration_json") or "{}", {})
        global_1d = (calibration.get("global") or {}).get("1d") or {}
        recs = calibration.get("recommendations") or []
        result.append({
            **data,
            "one_day_samples": global_1d.get("samples", 0),
            "one_day_hit_rate_pct": global_1d.get("directional_hit_rate_pct"),
            "one_day_mae_pct": global_1d.get("mean_absolute_error_pct"),
            "recommendations": recs[:3],
        })
    return result


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
            f"UPDATE thesis_run SET {', '.join(updates)} WHERE run_id = ?",  # nosec B608 — column names are internal literals, values parameterised
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
        "tickers": _safe_json_loads(data["tickers_json"] or "[]", []),
        "run_fresh": bool(data["run_fresh"]),
        "requested_by": data.get("requested_by"),
        "started_at": data["started_at"],
        "completed_at": data.get("completed_at"),
        "completed": _safe_json_loads(data.get("completed_json") or "[]", []),
        "failed": _safe_json_loads(data.get("failed_json") or "[]", []),
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
            "tickers": _safe_json_loads(data["tickers_json"] or "[]", []),
            "run_fresh": bool(data["run_fresh"]),
            "requested_by": data.get("requested_by"),
            "started_at": data["started_at"],
            "completed_at": data.get("completed_at"),
            "completed": _safe_json_loads(data.get("completed_json") or "[]", []),
            "failed": _safe_json_loads(data.get("failed_json") or "[]", []),
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
            """,  # nosec B608 — where is "" or "WHERE ticker = ?", values parameterised
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
