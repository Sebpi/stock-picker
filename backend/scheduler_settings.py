"""
scheduler_settings.py — Persist and retrieve scheduler configuration.
Settings in settings.json override .env values and survive restarts.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

_FILE = Path(__file__).parent / "scheduler_settings.json"

_DEFAULTS: dict[str, Any] = {
    "thesis_auto_run_enabled": os.getenv("THESIS_AUTO_RUN_ENABLED", "false").lower() in {"1", "true", "yes", "on"},
    "thesis_auto_run_interval_minutes": max(15, int(os.getenv("THESIS_AUTO_RUN_INTERVAL_MINUTES", "1440"))),
    "thesis_auto_run_max_tickers": max(1, int(os.getenv("THESIS_AUTO_RUN_MAX_TICKERS", "8"))),
    "evaluation_auto_run_enabled": os.getenv("EVALUATION_AUTO_RUN_ENABLED", "true").lower() in {"1", "true", "yes", "on"},
    "evaluation_auto_run_interval_minutes": max(60, int(os.getenv("EVALUATION_AUTO_RUN_INTERVAL_MINUTES", "1440"))),
    "prediction_auto_run_enabled": os.getenv("PREDICTION_AUTO_RUN_ENABLED", "true").lower() in {"1", "true", "yes", "on"},
    "prediction_auto_run_interval_minutes": max(5, int(os.getenv("PREDICTION_AUTO_RUN_INTERVAL_MINUTES", "15"))),
    "monitor_auto_run_enabled": os.getenv("MONITOR_AUTO_RUN_ENABLED", "true").lower() in {"1", "true", "yes", "on"},
    "monitor_auto_run_interval_minutes": max(1, int(os.getenv("MONITOR_AUTO_RUN_INTERVAL_MINUTES", "5"))),
}


def load() -> dict[str, Any]:
    if _FILE.exists():
        try:
            stored = json.loads(_FILE.read_text())
            return {**_DEFAULTS, **stored}
        except Exception:
            pass
    return dict(_DEFAULTS)


def save(settings: dict[str, Any]) -> None:
    merged = {**load(), **settings}
    # Clamp values
    merged["thesis_auto_run_interval_minutes"] = max(15, int(merged["thesis_auto_run_interval_minutes"]))
    merged["thesis_auto_run_max_tickers"] = max(1, min(50, int(merged["thesis_auto_run_max_tickers"])))
    merged["evaluation_auto_run_interval_minutes"] = max(60, int(merged["evaluation_auto_run_interval_minutes"]))
    merged["prediction_auto_run_interval_minutes"] = max(5, int(merged["prediction_auto_run_interval_minutes"]))
    merged["monitor_auto_run_interval_minutes"] = max(1, int(merged["monitor_auto_run_interval_minutes"]))
    _FILE.write_text(json.dumps(merged, indent=2))
