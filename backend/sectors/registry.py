from __future__ import annotations
import importlib
import logging
import pkgutil
from sectors.schema import SectorDefinition

logger = logging.getLogger(__name__)
_REGISTRY: dict[str, SectorDefinition] = {}


def _discover() -> None:
    import sectors as pkg
    for info in pkgutil.iter_modules(pkg.__path__):
        if info.name in ("schema", "registry"):
            continue
        try:
            mod = importlib.import_module(f"sectors.{info.name}")
            if hasattr(mod, "SECTOR") and isinstance(mod.SECTOR, SectorDefinition):
                _REGISTRY[mod.SECTOR.id] = mod.SECTOR
        except Exception:
            logger.warning("Failed to load sector module %s", info.name, exc_info=True)


_discover()


def get(sector_id: str) -> SectorDefinition | None:
    return _REGISTRY.get(sector_id)


def all_sectors() -> list[SectorDefinition]:
    return list(_REGISTRY.values())


def all_sector_ids() -> list[str]:
    return list(_REGISTRY.keys())
