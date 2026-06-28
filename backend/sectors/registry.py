from __future__ import annotations
import importlib
import json
import logging
import pkgutil
from sectors.schema import SectorDefinition, SupplyChainLayer

logger = logging.getLogger(__name__)
_REGISTRY: dict[str, SectorDefinition] = {}
_BUILTIN_IDS: set[str] = set()


def _discover() -> None:
    import sectors as pkg
    for info in pkgutil.iter_modules(pkg.__path__):
        if info.name in ("schema", "registry"):
            continue
        try:
            mod = importlib.import_module(f"sectors.{info.name}")
            if hasattr(mod, "SECTOR") and isinstance(mod.SECTOR, SectorDefinition):
                _REGISTRY[mod.SECTOR.id] = mod.SECTOR
                _BUILTIN_IDS.add(mod.SECTOR.id)
        except Exception:
            logger.warning("Failed to load sector module %s", info.name, exc_info=True)


_discover()


def _load_custom_sectors() -> None:
    try:
        import db as _db
        for row in _db.list_custom_sectors():
            if row["id"] in _BUILTIN_IDS:
                continue
            layers_data = json.loads(row["layers"]) if isinstance(row["layers"], str) else row["layers"]
            layers = [
                SupplyChainLayer(name=l["name"], tickers=l["tickers"], role=l["role"])
                for l in layers_data
            ]
            _REGISTRY[row["id"]] = SectorDefinition(
                id=row["id"],
                name=row["name"],
                description=row["description"],
                layers=layers,
                benchmark_etf=row.get("benchmark_etf"),
            )
    except Exception:
        logger.warning("Failed to load custom sectors from DB", exc_info=True)


def reload_custom() -> None:
    for sid in list(_REGISTRY):
        if sid not in _BUILTIN_IDS:
            del _REGISTRY[sid]
    _load_custom_sectors()


def is_builtin(sector_id: str) -> bool:
    return sector_id in _BUILTIN_IDS


def get(sector_id: str) -> SectorDefinition | None:
    return _REGISTRY.get(sector_id)


def all_sectors() -> list[SectorDefinition]:
    return list(_REGISTRY.values())


def all_sector_ids() -> list[str]:
    return list(_REGISTRY.keys())
