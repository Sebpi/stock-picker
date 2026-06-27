"""
Tests for portal SSO user data isolation.

Validates that portal users (portal:<sub>) get auto-provisioned in app_users
and see their own isolated data — not the admin's global data or another
portal user's data.

All tests use an in-memory SQLite DB via monkeypatched DB_PATH.
"""
from __future__ import annotations

import json
import math
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))


@pytest.fixture(autouse=True)
def isolated_db(tmp_path, monkeypatch):
    db_file = tmp_path / "test_stockpicker.db"
    import db
    monkeypatch.setattr(db, "DB_PATH", db_file)
    db.init_db()
    yield db_file


def _provision_portal_user(username: str) -> dict:
    """Mirrors _get_db_user() auto-provisioning logic from main.py."""
    import db
    row = db.get_user_by_username(username)
    if row:
        return row
    if username.startswith("portal:"):
        portal_sub = username[len("portal:"):]
        placeholder_email = f"{portal_sub}@portal.local"
        unusable_hash = "!portal-sso-no-local-password"
        try:
            return db.create_user(
                username=username,
                email=placeholder_email,
                password_hash=unusable_hash,
                role="user",
                tier="free",
            )
        except Exception:
            return db.get_user_by_username(username)
    return None


def _sanitize_for_json(obj):
    """Mirrors _sanitize_for_json() from main.py."""
    if isinstance(obj, dict):
        return {k: _sanitize_for_json(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_sanitize_for_json(v) for v in obj]
    if isinstance(obj, float):
        return obj if math.isfinite(obj) else None
    try:
        if hasattr(obj, "item"):
            val = float(obj.item())
            return val if math.isfinite(val) else None
    except (TypeError, ValueError):
        pass
    return obj


# ── Auto-provisioning ───────────────────────────────────────────────────────


def test_portal_user_auto_provisioned():
    user = _provision_portal_user("portal:alice")
    assert user is not None
    assert user["username"] == "portal:alice"
    assert user["email"] == "alice@portal.local"
    assert user["tier"] == "free"
    assert user["role"] == "user"


def test_portal_user_idempotent():
    u1 = _provision_portal_user("portal:bob")
    u2 = _provision_portal_user("portal:bob")
    assert u1["user_id"] == u2["user_id"]


def test_legacy_admin_returns_none():
    result = _provision_portal_user("admin")
    assert result is None


def test_non_portal_unknown_user_returns_none():
    result = _provision_portal_user("random_user")
    assert result is None


# ── Watchlist isolation ──────────────────────────────────────────────────────


def test_portal_users_have_separate_watchlists():
    import db

    u1 = _provision_portal_user("portal:user_a")
    u2 = _provision_portal_user("portal:user_b")

    db.add_to_user_watchlist(u1["user_id"], "AAPL")
    db.add_to_user_watchlist(u1["user_id"], "MSFT")
    db.add_to_user_watchlist(u2["user_id"], "GOOG")
    db.add_to_user_watchlist(u2["user_id"], "TSLA")

    assert set(db.get_user_watchlist(u1["user_id"])) == {"AAPL", "MSFT"}
    assert set(db.get_user_watchlist(u2["user_id"])) == {"GOOG", "TSLA"}


def test_new_portal_user_has_empty_watchlist():
    import db

    user = _provision_portal_user("portal:fresh_user")
    assert db.get_user_watchlist(user["user_id"]) == []


def test_watchlist_add_remove_persists_per_user():
    import db

    user = _provision_portal_user("portal:wl_test")
    db.add_to_user_watchlist(user["user_id"], "NVDA")
    db.add_to_user_watchlist(user["user_id"], "AMD")
    assert set(db.get_user_watchlist(user["user_id"])) == {"NVDA", "AMD"}

    db.remove_from_user_watchlist(user["user_id"], "NVDA")
    assert db.get_user_watchlist(user["user_id"]) == ["AMD"]


# ── Portfolio isolation ──────────────────────────────────────────────────────


def test_portal_users_have_separate_portfolios():
    import db

    u1 = _provision_portal_user("portal:pf_user1")
    u2 = _provision_portal_user("portal:pf_user2")

    db.add_user_transaction(u1["user_id"], "real", {
        "id": str(uuid.uuid4()), "type": "buy", "ticker": "AAPL",
        "name": "Apple", "qty": 10, "price": 150.0,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    })
    db.add_user_transaction(u2["user_id"], "real", {
        "id": str(uuid.uuid4()), "type": "buy", "ticker": "MSFT",
        "name": "Microsoft", "qty": 5, "price": 400.0,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    })

    u1_txs = db.get_user_transactions(u1["user_id"], "real")
    u2_txs = db.get_user_transactions(u2["user_id"], "real")

    assert len(u1_txs) == 1 and u1_txs[0]["ticker"] == "AAPL"
    assert len(u2_txs) == 1 and u2_txs[0]["ticker"] == "MSFT"


def test_new_portal_user_has_empty_portfolio():
    import db

    user = _provision_portal_user("portal:empty_pf")
    assert db.get_user_transactions(user["user_id"], "real") == []
    assert db.get_user_transactions(user["user_id"], "paper") == []


def test_paper_portfolio_isolated_from_real():
    import db

    user = _provision_portal_user("portal:paper_test")
    now = datetime.now(timezone.utc).isoformat()

    db.add_user_transaction(user["user_id"], "real", {
        "id": str(uuid.uuid4()), "type": "buy", "ticker": "TSLA",
        "name": "Tesla", "qty": 3, "price": 250.0, "timestamp": now,
    })
    db.add_user_transaction(user["user_id"], "paper", {
        "id": str(uuid.uuid4()), "type": "buy", "ticker": "NVDA",
        "name": "Nvidia", "qty": 7, "price": 800.0, "timestamp": now,
    })

    real = db.get_user_transactions(user["user_id"], "real")
    paper = db.get_user_transactions(user["user_id"], "paper")
    assert len(real) == 1 and real[0]["ticker"] == "TSLA"
    assert len(paper) == 1 and paper[0]["ticker"] == "NVDA"


# ── Alerts isolation ────────────────────────────────────────────────────────


def test_portal_users_have_separate_alerts():
    import db

    u1 = _provision_portal_user("portal:alert_u1")
    u2 = _provision_portal_user("portal:alert_u2")

    db.append_user_alert(u1["user_id"], {
        "id": str(uuid.uuid4()),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "ticker": "AAPL", "action": "BUY",
    })
    db.append_user_alert(u2["user_id"], {
        "id": str(uuid.uuid4()),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "ticker": "GOOG", "action": "SELL",
    })

    u1_alerts = db.get_user_alerts(u1["user_id"])
    u2_alerts = db.get_user_alerts(u2["user_id"])

    assert len(u1_alerts) == 1 and u1_alerts[0]["ticker"] == "AAPL"
    assert len(u2_alerts) == 1 and u2_alerts[0]["ticker"] == "GOOG"


def test_new_portal_user_has_empty_alerts():
    import db

    user = _provision_portal_user("portal:no_alerts")
    assert db.get_user_alerts(user["user_id"]) == []


# ── Cross-user leak check ───────────────────────────────────────────────────


def test_no_data_leaks_between_portal_users():
    """Full isolation check: populate user_a with data across all stores,
    verify user_b sees nothing."""
    import db

    user_a = _provision_portal_user("portal:leak_a")
    user_b = _provision_portal_user("portal:leak_b")
    now = datetime.now(timezone.utc).isoformat()

    # Populate user_a
    db.add_to_user_watchlist(user_a["user_id"], "AAPL")
    db.add_to_user_watchlist(user_a["user_id"], "MSFT")
    db.add_to_user_watchlist(user_a["user_id"], "GOOG")

    db.add_user_transaction(user_a["user_id"], "real", {
        "id": str(uuid.uuid4()), "type": "buy", "ticker": "AAPL",
        "name": "Apple", "qty": 50, "price": 175.0, "timestamp": now,
    })
    db.add_user_transaction(user_a["user_id"], "paper", {
        "id": str(uuid.uuid4()), "type": "buy", "ticker": "TSLA",
        "name": "Tesla", "qty": 20, "price": 200.0, "timestamp": now,
    })

    db.append_user_alert(user_a["user_id"], {
        "id": str(uuid.uuid4()), "timestamp": now,
        "ticker": "NVDA", "action": "BUY",
    })

    # Verify user_b sees nothing
    assert db.get_user_watchlist(user_b["user_id"]) == []
    assert db.get_user_transactions(user_b["user_id"], "real") == []
    assert db.get_user_transactions(user_b["user_id"], "paper") == []
    assert db.get_user_alerts(user_b["user_id"]) == []

    # Verify user_a's data is still intact
    assert len(db.get_user_watchlist(user_a["user_id"])) == 3
    assert len(db.get_user_transactions(user_a["user_id"], "real")) == 1
    assert len(db.get_user_transactions(user_a["user_id"], "paper")) == 1
    assert len(db.get_user_alerts(user_a["user_id"])) == 1


# ── NaN sanitizer ───────────────────────────────────────────────────────────


def test_sanitize_for_json_handles_nan():
    result = _sanitize_for_json({
        "a": float("nan"),
        "b": float("inf"),
        "c": float("-inf"),
        "d": 42.5,
        "e": "hello",
        "f": None,
        "g": [float("nan"), 1.0, "x"],
        "h": {"nested": float("inf"), "ok": 10},
    })
    assert result["a"] is None
    assert result["b"] is None
    assert result["c"] is None
    assert result["d"] == 42.5
    assert result["e"] == "hello"
    assert result["f"] is None
    assert result["g"] == [None, 1.0, "x"]
    assert result["h"] == {"nested": None, "ok": 10}

    json.dumps(result)  # must not raise


def test_sanitize_for_json_handles_numpy():
    try:
        import numpy as np
    except ImportError:
        pytest.skip("numpy not installed")

    result = _sanitize_for_json({
        "nan_float": np.float64("nan"),
        "good_float": np.float64(42.5),
        "int_val": np.int64(7),
    })
    assert result["nan_float"] is None
    assert result["good_float"] == 42.5
    assert result["int_val"] == 7.0
    json.dumps(result)  # must not raise
