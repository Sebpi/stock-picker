from sectors.schema import SectorDefinition, SectorNode, SectorEdge

SECTOR = SectorDefinition(
    id="cybersecurity",
    name="Cybersecurity",
    description="Identity, network, endpoint, and cloud security infrastructure",
    benchmark_etf="CIBR",
    nodes=[
        # Identity & Access (upstream)
        SectorNode("OKTA", "Identity & access management"),
        SectorNode("CYBR", "Privileged access & secrets management"),
        # Network & Infrastructure Security (midstream)
        SectorNode("PANW", "Network security platform (firewall + SASE)"),
        SectorNode("FTNT", "Firewall & SD-WAN"),
        SectorNode("ZS", "Zero-trust cloud security (SASE)"),
        SectorNode("NET", "Edge security & CDN"),
        # Endpoint & Cloud Security (midstream)
        SectorNode("CRWD", "Endpoint detection & response"),
        SectorNode("S", "AI-powered endpoint security"),
        SectorNode("WDAY", "Enterprise cloud (security analytics)"),
        # Enterprise Buyers (downstream)
        SectorNode("MSFT", "Azure security suite (Defender/Sentinel)"),
        SectorNode("GOOGL", "Google Cloud security (Mandiant/Chronicle)"),
        SectorNode("AMZN", "AWS security services"),
    ],
    edges=[
        # Identity → Security platforms
        SectorEdge("OKTA", "PANW", "Identity feeds into network security"),
        SectorEdge("OKTA", "ZS", "Identity for zero-trust"),
        SectorEdge("OKTA", "CRWD", "Identity context for endpoint"),
        SectorEdge("CYBR", "PANW", "Privileged access integration"),
        SectorEdge("CYBR", "CRWD", "Secrets management"),
        # Security platforms → Enterprise buyers
        SectorEdge("PANW", "MSFT", "Network security for Azure"),
        SectorEdge("PANW", "GOOGL", "Network security for GCP"),
        SectorEdge("FTNT", "AMZN", "Firewall for AWS"),
        SectorEdge("ZS", "MSFT", "Zero-trust for Azure"),
        SectorEdge("CRWD", "MSFT", "Endpoint protection"),
        SectorEdge("CRWD", "GOOGL", "Endpoint for GCP"),
        SectorEdge("S", "AMZN", "Endpoint for AWS"),
        SectorEdge("NET", "MSFT", "Edge security / CDN"),
        SectorEdge("NET", "AMZN", "Edge security"),
        SectorEdge("WDAY", "MSFT", "Security analytics"),
    ],
)
