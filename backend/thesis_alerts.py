"""
thesis_alerts.py — Sends WhatsApp/email alerts when a thesis score drops
significantly or a ticker crosses a risk rating boundary.

Configuration (env vars, all optional):
  THESIS_ALERT_DROP_THRESHOLD   int   Score drop needed to trigger (default 10)
  THESIS_ALERT_RISK_CROSSING    bool  Alert on risk-rating change (default true)
  THESIS_ALERT_COOLDOWN_HOURS   int   Min hours between alerts per ticker (default 4)
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)

_DROP_THRESHOLD = int(os.getenv("THESIS_ALERT_DROP_THRESHOLD", "10"))
_RISK_CROSSING  = os.getenv("THESIS_ALERT_RISK_CROSSING", "true").lower() != "false"
_COOLDOWN_HOURS = int(os.getenv("THESIS_ALERT_COOLDOWN_HOURS", "4"))

# In-process cooldown: ticker -> last alert datetime
_cooldown: dict[str, datetime] = {}

_RISK_ORDER = ["low", "medium_low", "medium", "medium_high", "high"]


def _risk_index(rating: str) -> int:
    try:
        return _RISK_ORDER.index(rating.lower())
    except ValueError:
        return -1


def _on_cooldown(ticker: str) -> bool:
    last = _cooldown.get(ticker)
    if not last:
        return False
    age_hours = (datetime.now(timezone.utc) - last).total_seconds() / 3600
    return age_hours < _COOLDOWN_HOURS


def _set_cooldown(ticker: str) -> None:
    _cooldown[ticker] = datetime.now(timezone.utc)


def check_and_alert(ticker: str, new_score: float, new_risk: str,
                    prev_score: float | None, prev_risk: str | None) -> dict[str, Any]:
    """
    Compare new thesis to previous. Send alert if thresholds breached.
    Returns a dict describing what was checked and whether an alert fired.
    """
    result: dict[str, Any] = {
        "ticker": ticker,
        "alerted": False,
        "reason": None,
        "score_drop": None,
        "risk_change": None,
    }

    if prev_score is None:
        result["reason"] = "first_thesis"
        return result

    score_drop = prev_score - new_score
    result["score_drop"] = round(score_drop, 1)

    prev_idx = _risk_index(prev_risk or "")
    new_idx  = _risk_index(new_risk or "")
    risk_worsened = new_idx > prev_idx and prev_idx >= 0
    result["risk_change"] = f"{prev_risk}→{new_risk}" if risk_worsened else None

    trigger_drop    = score_drop >= _DROP_THRESHOLD
    trigger_risk    = _RISK_CROSSING and risk_worsened
    should_alert    = trigger_drop or trigger_risk

    if not should_alert:
        return result

    if _on_cooldown(ticker):
        result["reason"] = "cooldown"
        return result

    reasons: list[str] = []
    if trigger_drop:
        reasons.append(f"score dropped {prev_score:.0f}→{new_score:.0f} ({score_drop:.0f} pts)")
    if trigger_risk:
        reasons.append(f"risk worsened {prev_risk}→{new_risk}")

    message = (
        f"[THESIS] {ticker}: {' and '.join(reasons)}. "
        f"Composite score now {new_score:.1f} | Risk: {new_risk}."
    )
    logger.info("[thesis_alerts] %s — %s", ticker, message)

    _fire(message)
    _set_cooldown(ticker)

    result["alerted"] = True
    result["reason"] = "; ".join(reasons)
    return result


def _fire(message: str) -> None:
    """Send via WhatsApp (primary) and email (secondary)."""
    try:
        from sentiment_agent import send_whatsapp
        send_whatsapp(message)
    except Exception as exc:
        logger.debug("[thesis_alerts] WhatsApp failed: %s", exc)

    try:
        from main import send_email
        send_email(subject=f"[StockPicker Thesis Alert]", body=message)
    except Exception as exc:
        logger.debug("[thesis_alerts] Email failed: %s", exc)
