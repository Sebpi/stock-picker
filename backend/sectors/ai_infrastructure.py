from sectors.schema import SectorDefinition, SupplyChainLayer

SECTOR = SectorDefinition(
    id="ai-infrastructure",
    name="AI Infrastructure",
    description="Picks & shovels for the AI capex cycle — fab equipment, foundries, compute, networking, and hyperscaler buyers",
    benchmark_etf="SMH",
    layers=[
        SupplyChainLayer(
            name="Fab Equipment & Materials",
            role="upstream",
            tickers={
                "ASML": "EUV lithography monopoly",
                "AMAT": "Deposition & etch systems",
                "LRCX": "Etch & deposition",
                "KLAC": "Process control & inspection",
                "ENTG": "Advanced materials & contamination control",
            },
        ),
        SupplyChainLayer(
            name="Foundry & Packaging",
            role="upstream",
            tickers={
                "TSM": "Leading-edge fab (3nm/2nm)",
                "ASX": "Advanced packaging (CoWoS)",
            },
        ),
        SupplyChainLayer(
            name="Compute Silicon",
            role="midstream",
            tickers={
                "NVDA": "GPU compute (data center)",
                "AMD": "GPU & CPU (data center + edge)",
                "INTC": "CPU & foundry services",
                "AVGO": "Networking ASICs & custom AI accelerators",
                "MRVL": "Custom silicon & electro-optics",
            },
        ),
        SupplyChainLayer(
            name="Networking & Interconnect",
            role="midstream",
            tickers={
                "ANET": "Data center switching",
                "CSCO": "Enterprise & DC networking",
                "COHR": "Optical transceivers (800G/1.6T)",
            },
        ),
        SupplyChainLayer(
            name="Infrastructure & Power",
            role="midstream",
            tickers={
                "VRT": "Liquid cooling & power management",
                "EQIX": "Colocation & interconnection",
                "DLR": "Data center REIT",
                "VST": "Power generation (nuclear fleet)",
                "CEG": "Nuclear clean energy for DC",
            },
        ),
        SupplyChainLayer(
            name="Hyperscaler Buyers",
            role="downstream",
            tickers={
                "MSFT": "Azure / OpenAI partnership",
                "GOOGL": "GCP / Gemini / TPU",
                "AMZN": "AWS / Trainium & Inferentia",
                "META": "LLaMA infra / open-source AI",
                "ORCL": "OCI / GPU cloud",
            },
        ),
    ],
)
