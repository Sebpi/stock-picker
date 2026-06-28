from __future__ import annotations
from dataclasses import dataclass, field


@dataclass
class SupplyChainLayer:
    name: str           # "Foundry", "GPU Design", "Hyperscaler"
    tickers: dict[str, str]  # {"TSM": "Leading-edge fab"}
    role: str           # "upstream" | "midstream" | "downstream"


@dataclass
class SectorDefinition:
    id: str             # "ai-infrastructure"
    name: str           # "AI Infrastructure"
    description: str
    layers: list[SupplyChainLayer] = field(default_factory=list)
    benchmark_etf: str | None = None  # "SMH"

    @property
    def all_tickers(self) -> set[str]:
        return {t for layer in self.layers for t in layer.tickers}

    def ticker_layer(self, ticker: str) -> SupplyChainLayer | None:
        for layer in self.layers:
            if ticker in layer.tickers:
                return layer
        return None

    def build_context_notes(self, ticker: str) -> str:
        """Build supply-chain context string for the orchestrator."""
        layer = self.ticker_layer(ticker)
        if not layer:
            return f"Sector: {self.name}."

        other_layers = {
            role: [(t, desc) for l in self.layers if l.role == role
                   for t, desc in l.tickers.items() if t != ticker]
            for role in ("upstream", "midstream", "downstream")
        }

        parts = [f"Sector: {self.name}. Ticker role: {layer.name} ({layer.role})."]
        if other_layers.get("upstream"):
            ups = ", ".join(f"{t} ({d})" for t, d in other_layers["upstream"][:6])
            parts.append(f"Key upstream suppliers: {ups}.")
        if other_layers.get("downstream"):
            downs = ", ".join(f"{t} ({d})" for t, d in other_layers["downstream"][:6])
            parts.append(f"Key downstream buyers: {downs}.")
        return " ".join(parts)

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "benchmark_etf": self.benchmark_etf,
            "ticker_count": len(self.all_tickers),
            "layers": [
                {
                    "name": l.name,
                    "role": l.role,
                    "tickers": l.tickers,
                }
                for l in self.layers
            ],
        }
