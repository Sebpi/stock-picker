from __future__ import annotations
from dataclasses import dataclass, field


@dataclass
class SectorNode:
    ticker: str
    description: str


@dataclass
class SectorEdge:
    source: str
    target: str
    label: str


@dataclass
class SupplyChainLayer:
    """Backward-compat grouping derived from graph topology."""
    name: str
    tickers: dict[str, str]
    role: str


@dataclass
class SectorDefinition:
    id: str
    name: str
    description: str
    nodes: list[SectorNode] = field(default_factory=list)
    edges: list[SectorEdge] = field(default_factory=list)
    benchmark_etf: str | None = None

    @property
    def all_tickers(self) -> set[str]:
        return {n.ticker for n in self.nodes}

    def ticker_node(self, ticker: str) -> SectorNode | None:
        for n in self.nodes:
            if n.ticker == ticker:
                return n
        return None

    def ticker_role(self, ticker: str) -> str:
        """Infer upstream/midstream/downstream from graph topology."""
        has_incoming = any(e.target == ticker for e in self.edges)
        has_outgoing = any(e.source == ticker for e in self.edges)
        if has_outgoing and not has_incoming:
            return "upstream"
        elif has_incoming and not has_outgoing:
            return "downstream"
        return "midstream"

    @property
    def layers(self) -> list[SupplyChainLayer]:
        """Group nodes by inferred role for backward compatibility."""
        groups: dict[str, dict[str, str]] = {}
        for n in self.nodes:
            role = self.ticker_role(n.ticker)
            groups.setdefault(role, {})[n.ticker] = n.description
        order = ["upstream", "midstream", "downstream"]
        return [
            SupplyChainLayer(name=role.title(), tickers=tickers, role=role)
            for role in order if (tickers := groups.get(role))
        ]

    def ticker_layer(self, ticker: str) -> SupplyChainLayer | None:
        """Backward compat — find the layer containing this ticker."""
        for layer in self.layers:
            if ticker in layer.tickers:
                return layer
        return None

    def build_context_notes(self, ticker: str) -> str:
        """Build supply-chain context string for the orchestrator using edge data."""
        node = self.ticker_node(ticker)
        if not node:
            return f"Sector: {self.name}."

        role = self.ticker_role(ticker)
        suppliers = [(e.source, e.label) for e in self.edges if e.target == ticker]
        customers = [(e.target, e.label) for e in self.edges if e.source == ticker]

        parts = [f"Sector: {self.name}. Ticker role: {role}."]
        if suppliers:
            s = ", ".join(f"{t} ({l})" for t, l in suppliers[:6])
            parts.append(f"Key suppliers: {s}.")
        if customers:
            c = ", ".join(f"{t} ({l})" for t, l in customers[:6])
            parts.append(f"Key customers: {c}.")
        return " ".join(parts)

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "benchmark_etf": self.benchmark_etf,
            "ticker_count": len(self.all_tickers),
            "nodes": [{"ticker": n.ticker, "description": n.description} for n in self.nodes],
            "edges": [{"source": e.source, "target": e.target, "label": e.label} for e in self.edges],
            "layers": [
                {"name": l.name, "role": l.role, "tickers": l.tickers}
                for l in self.layers
            ],
        }
