"""Unit tests for the sector registry and schema."""
from __future__ import annotations
import sys
from pathlib import Path

# Ensure backend/ is on the path so we can import sectors
sys.path.insert(0, str(Path(__file__).parent.parent))

import pytest


def test_sector_discovery():
    from sectors.registry import all_sectors, get, all_sector_ids
    sectors = all_sectors()
    assert len(sectors) >= 3
    assert "ai-infrastructure" in all_sector_ids()
    assert get("ai-infrastructure") is not None
    assert get("nonexistent") is None


def test_sector_all_tickers():
    from sectors.registry import get
    ai = get("ai-infrastructure")
    assert ai is not None
    tickers = ai.all_tickers
    assert "NVDA" in tickers
    assert "TSM" in tickers
    assert "ASML" in tickers
    assert len(tickers) >= 20


def test_sector_ticker_layer():
    from sectors.registry import get
    ai = get("ai-infrastructure")
    layer = ai.ticker_layer("NVDA")
    assert layer is not None
    assert layer.role == "midstream"
    assert ai.ticker_layer("NONEXIST") is None


def test_build_context_notes():
    from sectors.registry import get
    ai = get("ai-infrastructure")
    notes = ai.build_context_notes("NVDA")
    assert "AI Infrastructure" in notes
    assert "midstream" in notes
    assert "upstream" in notes.lower() or "downstream" in notes.lower()


def test_build_context_notes_unknown_ticker():
    from sectors.registry import get
    ai = get("ai-infrastructure")
    notes = ai.build_context_notes("ZZZZZ")
    assert "AI Infrastructure" in notes


def test_sector_to_dict():
    from sectors.registry import get
    ai = get("ai-infrastructure")
    d = ai.to_dict()
    assert d["id"] == "ai-infrastructure"
    assert d["name"] == "AI Infrastructure"
    assert d["benchmark_etf"] == "SMH"
    assert d["ticker_count"] >= 20
    assert len(d["layers"]) >= 5
    for layer in d["layers"]:
        assert "name" in layer
        assert "role" in layer
        assert layer["role"] in ("upstream", "midstream", "downstream")
        assert "tickers" in layer


def test_no_duplicate_sector_ids():
    from sectors.registry import all_sectors
    ids = [s.id for s in all_sectors()]
    assert len(ids) == len(set(ids))


def test_shared_tickers_across_sectors():
    """Tickers can appear in multiple sectors (independent universes)."""
    from sectors.registry import get
    ai = get("ai-infrastructure")
    energy = get("energy-transition")
    assert ai is not None and energy is not None
    # CEG and VST appear in both AI infra (power) and energy transition
    ai_tickers = ai.all_tickers
    energy_tickers = energy.all_tickers
    overlap = ai_tickers & energy_tickers
    assert len(overlap) >= 1, "Expected at least one shared ticker (e.g. CEG, VST)"
