"""
Tests for the multi-user database layer.
All tests operate on an in-memory SQLite DB via a monkeypatched DB_PATH.
"""
from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path

import pytest

# Ensure backend/ is on the path so we can import db
sys.path.insert(0, str(Path(__file__).parent.parent))

# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture(autouse=True)
def isolated_db(tmp_path, monkeypatch):
    """Each test gets its own fresh SQLite DB."""
    db_file = tmp_path / "test_stockpicker.db"
    import db
    monkeypatch.setattr(db, "DB_PATH", db_file)
    db.init_db()
    yield db_file


# ── User CRUD ─────────────────────────────────────────────────────────────────

def test_create_and_fetch_user():
    import db
    user = db.create_user("alice", "alice@example.com", "hashed_pw_value")
    assert user["username"] == "alice"
    assert user["email"] == "alice@example.com"
    assert user["tier"] == "free"
    assert user["role"] == "user"
    assert user["email_verified"] == 0

    fetched = db.get_user_by_id(user["user_id"])
    assert fetched["username"] == "alice"

    by_name = db.get_user_by_username("alice")
    assert by_name["user_id"] == user["user_id"]

    by_email = db.get_user_by_email("alice@example.com")
    assert by_email["user_id"] == user["user_id"]


def test_duplicate_username_raises():
    import db
    from sqlite3 import IntegrityError
    db.create_user("bob", "bob@example.com", "hash1")
    with pytest.raises(IntegrityError):
        db.create_user("bob", "bob2@example.com", "hash2")


def test_duplicate_email_raises():
    import db
    from sqlite3 import IntegrityError
    db.create_user("carol", "carol@example.com", "hash1")
    with pytest.raises(IntegrityError):
        db.create_user("carol2", "carol@example.com", "hash2")


def test_update_user_tier():
    import db
    user = db.create_user("dave", "dave@example.com", "hash")
    db.update_user(user["user_id"], tier="pro")
    updated = db.get_user_by_id(user["user_id"])
    assert updated["tier"] == "pro"


def test_list_users():
    import db
    db.create_user("u1", "u1@example.com", "h1")
    db.create_user("u2", "u2@example.com", "h2")
    users = db.list_users()
    usernames = [u["username"] for u in users]
    assert "u1" in usernames and "u2" in usernames


# ── Email verification tokens ─────────────────────────────────────────────────

def test_email_verification_token_lifecycle():
    import db
    user = db.create_user("eve", "eve@example.com", "hash")
    raw = db.create_email_verification_token(user["user_id"])
    assert len(raw) > 20

    # Valid token marks user as verified
    uid = db.consume_email_verification_token(raw)
    assert uid == user["user_id"]
    verified_user = db.get_user_by_id(user["user_id"])
    assert verified_user["email_verified"] == 1

    # Token is single-use
    uid2 = db.consume_email_verification_token(raw)
    assert uid2 is None


def test_invalid_verification_token():
    import db
    result = db.consume_email_verification_token("totally_invalid_token_xyz")
    assert result is None


def test_new_token_replaces_old():
    import db
    user = db.create_user("frank", "frank@example.com", "hash")
    raw1 = db.create_email_verification_token(user["user_id"])
    raw2 = db.create_email_verification_token(user["user_id"])
    # old token should be deleted (used=0 rows removed on new create)
    result = db.consume_email_verification_token(raw1)
    assert result is None  # raw1 was deleted
    result2 = db.consume_email_verification_token(raw2)
    assert result2 == user["user_id"]


# ── Refresh token rotation ────────────────────────────────────────────────────

def test_refresh_token_rotation():
    import db
    user = db.create_user("grace", "grace@example.com", "hash")
    raw = db.create_refresh_token(user["user_id"])
    assert len(raw) > 30

    result = db.rotate_refresh_token(raw)
    assert result is not None
    new_raw, uid = result
    assert uid == user["user_id"]
    assert new_raw != raw

    # Old token should be revoked
    result2 = db.rotate_refresh_token(raw)
    assert result2 is None


def test_revoke_all_refresh_tokens():
    import db
    user = db.create_user("henry", "henry@example.com", "hash")
    r1 = db.create_refresh_token(user["user_id"])
    r2 = db.create_refresh_token(user["user_id"])
    db.revoke_all_refresh_tokens(user["user_id"])
    assert db.rotate_refresh_token(r1) is None
    assert db.rotate_refresh_token(r2) is None


# ── Watchlist isolation ───────────────────────────────────────────────────────

def test_watchlist_isolation():
    import db
    u1 = db.create_user("wl_user1", "wl1@example.com", "hash")
    u2 = db.create_user("wl_user2", "wl2@example.com", "hash")

    db.add_to_user_watchlist(u1["user_id"], "AAPL")
    db.add_to_user_watchlist(u1["user_id"], "MSFT")
    db.add_to_user_watchlist(u2["user_id"], "GOOG")

    assert set(db.get_user_watchlist(u1["user_id"])) == {"AAPL", "MSFT"}
    assert db.get_user_watchlist(u2["user_id"]) == ["GOOG"]


def test_watchlist_duplicate_add_is_idempotent():
    import db
    u = db.create_user("wl_dup", "wldup@example.com", "hash")
    db.add_to_user_watchlist(u["user_id"], "TSLA")
    ok, reason = db.add_to_user_watchlist(u["user_id"], "TSLA")
    assert ok
    assert reason == "already_exists"
    assert db.get_user_watchlist(u["user_id"]) == ["TSLA"]


def test_watchlist_free_tier_limit():
    import db
    u = db.create_user("wl_limit", "wllimit@example.com", "hash", tier="free")
    for i in range(10):
        ok, _ = db.add_to_user_watchlist(u["user_id"], f"TK{i:02d}")
        assert ok
    ok, reason = db.add_to_user_watchlist(u["user_id"], "OVER")
    assert not ok
    assert "10" in reason


def test_watchlist_remove():
    import db
    u = db.create_user("wl_rem", "wlrem@example.com", "hash")
    db.add_to_user_watchlist(u["user_id"], "NVDA")
    removed = db.remove_from_user_watchlist(u["user_id"], "NVDA")
    assert removed
    assert db.get_user_watchlist(u["user_id"]) == []


# ── Portfolio isolation ───────────────────────────────────────────────────────

def test_portfolio_isolation():
    import db
    u1 = db.create_user("pf_u1", "pf1@example.com", "hash")
    u2 = db.create_user("pf_u2", "pf2@example.com", "hash")

    db.upsert_user_portfolio_position(u1["user_id"], "AAPL", 10.0, cost_basis=150.0)
    db.upsert_user_portfolio_position(u2["user_id"], "MSFT", 5.0, cost_basis=300.0)

    p1 = db.get_user_portfolio(u1["user_id"])
    p2 = db.get_user_portfolio(u2["user_id"])
    assert len(p1) == 1 and p1[0]["ticker"] == "AAPL"
    assert len(p2) == 1 and p2[0]["ticker"] == "MSFT"


def test_paper_portfolio_separate_from_real():
    import db
    u = db.create_user("pf_paper", "pfpaper@example.com", "hash")
    db.upsert_user_portfolio_position(u["user_id"], "TSLA", 3.0, paper=False)
    db.upsert_user_portfolio_position(u["user_id"], "TSLA", 7.0, paper=True)

    real = db.get_user_portfolio(u["user_id"], paper=False)
    paper = db.get_user_portfolio(u["user_id"], paper=True)
    assert real[0]["shares"] == 3.0
    assert paper[0]["shares"] == 7.0


def test_portfolio_upsert_updates_shares():
    import db
    u = db.create_user("pf_up", "pfup@example.com", "hash")
    db.upsert_user_portfolio_position(u["user_id"], "AAPL", 5.0, cost_basis=100.0)
    db.upsert_user_portfolio_position(u["user_id"], "AAPL", 12.0)
    positions = db.get_user_portfolio(u["user_id"])
    assert positions[0]["shares"] == 12.0
    assert positions[0]["cost_basis"] == 100.0  # preserved on upsert


# ── Monthly thesis count ──────────────────────────────────────────────────────

def test_free_tier_thesis_limit():
    import db
    u = db.create_user("thesis_u", "thesis@example.com", "hash", tier="free")
    for _ in range(5):
        allowed, reason = db.check_and_increment_thesis_count(u["user_id"])
        assert allowed, reason
    allowed, reason = db.check_and_increment_thesis_count(u["user_id"])
    assert not allowed
    assert "5" in reason


def test_pro_tier_no_thesis_limit():
    import db
    u = db.create_user("thesis_pro", "thesispro@example.com", "hash", tier="pro")
    for _ in range(20):
        allowed, reason = db.check_and_increment_thesis_count(u["user_id"])
        assert allowed, reason


# ── User settings ─────────────────────────────────────────────────────────────

def test_user_settings():
    import db
    u = db.create_user("settings_u", "settings@example.com", "hash")
    db.set_user_setting(u["user_id"], "theme", "dark")
    db.set_user_setting(u["user_id"], "currency", "GBP")
    s = db.get_user_settings(u["user_id"])
    assert s["theme"] == "dark"
    assert s["currency"] == "GBP"

    # upsert
    db.set_user_setting(u["user_id"], "theme", "light")
    s2 = db.get_user_settings(u["user_id"])
    assert s2["theme"] == "light"


# ── APNs device tokens ────────────────────────────────────────────────────────

def test_device_token_register_unregister():
    import db
    u = db.create_user("apns_u", "apns@example.com", "hash", tier="pro")
    token = "a" * 64
    db.register_device_token(u["user_id"], token, "ios")
    tokens = db.get_device_tokens_for_user(u["user_id"])
    assert any(t["device_token"] == token for t in tokens)

    removed = db.unregister_device_token(u["user_id"], token)
    assert removed
    assert db.get_device_tokens_for_user(u["user_id"]) == []


def test_device_token_idempotent():
    import db
    u = db.create_user("apns_idem", "apnsidem@example.com", "hash", tier="pro")
    token = "b" * 64
    db.register_device_token(u["user_id"], token)
    db.register_device_token(u["user_id"], token)  # should not raise
    assert len(db.get_device_tokens_for_user(u["user_id"])) == 1
