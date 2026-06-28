from sectors.schema import SectorDefinition, SupplyChainLayer

SECTOR = SectorDefinition(
    id="cybersecurity",
    name="Cybersecurity",
    description="Identity, network, endpoint, and cloud security infrastructure",
    benchmark_etf="CIBR",
    layers=[
        SupplyChainLayer(
            name="Identity & Access",
            role="upstream",
            tickers={
                "OKTA": "Identity & access management",
                "CYBR": "Privileged access & secrets management",
            },
        ),
        SupplyChainLayer(
            name="Network & Infrastructure Security",
            role="midstream",
            tickers={
                "PANW": "Network security platform (firewall + SASE)",
                "FTNT": "Firewall & SD-WAN",
                "ZS": "Zero-trust cloud security (SASE)",
                "NET": "Edge security & CDN",
            },
        ),
        SupplyChainLayer(
            name="Endpoint & Cloud Security",
            role="midstream",
            tickers={
                "CRWD": "Endpoint detection & response",
                "S": "AI-powered endpoint security",
                "WDAY": "Enterprise cloud (security analytics)",
            },
        ),
        SupplyChainLayer(
            name="Enterprise Buyers",
            role="downstream",
            tickers={
                "MSFT": "Azure security suite (Defender/Sentinel)",
                "GOOGL": "Google Cloud security (Mandiant/Chronicle)",
                "AMZN": "AWS security services",
            },
        ),
    ],
)
