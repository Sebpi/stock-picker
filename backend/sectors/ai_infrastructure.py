from sectors.schema import SectorDefinition, SectorNode, SectorEdge

SECTOR = SectorDefinition(
    id="ai-infrastructure",
    name="AI Infrastructure",
    description="Picks & shovels for the AI capex cycle — fab equipment, foundries, compute, networking, and hyperscaler buyers",
    benchmark_etf="SMH",
    nodes=[
        # Fab Equipment & Materials (upstream)
        SectorNode("ASML", "EUV lithography monopoly"),
        SectorNode("AMAT", "Deposition & etch systems"),
        SectorNode("LRCX", "Etch & deposition"),
        SectorNode("KLAC", "Process control & inspection"),
        SectorNode("ENTG", "Advanced materials & contamination control"),
        # Foundry & Packaging
        SectorNode("TSM", "Leading-edge fab (3nm/2nm)"),
        SectorNode("ASX", "Advanced packaging (CoWoS)"),
        # Compute Silicon
        SectorNode("NVDA", "GPU compute (data center)"),
        SectorNode("AMD", "GPU & CPU (data center + edge)"),
        SectorNode("INTC", "CPU & foundry services"),
        SectorNode("AVGO", "Networking ASICs & custom AI accelerators"),
        SectorNode("MRVL", "Custom silicon & electro-optics"),
        # Networking & Interconnect
        SectorNode("ANET", "Data center switching"),
        SectorNode("CSCO", "Enterprise & DC networking"),
        SectorNode("COHR", "Optical transceivers (800G/1.6T)"),
        # Infrastructure & Power
        SectorNode("VRT", "Liquid cooling & power management"),
        SectorNode("EQIX", "Colocation & interconnection"),
        SectorNode("DLR", "Data center REIT"),
        SectorNode("VST", "Power generation (nuclear fleet)"),
        SectorNode("CEG", "Nuclear clean energy for DC"),
        # Hyperscaler Buyers (downstream)
        SectorNode("MSFT", "Azure / OpenAI partnership"),
        SectorNode("GOOGL", "GCP / Gemini / TPU"),
        SectorNode("AMZN", "AWS / Trainium & Inferentia"),
        SectorNode("META", "LLaMA infra / open-source AI"),
        SectorNode("ORCL", "OCI / GPU cloud"),
    ],
    edges=[
        # Equipment → Foundry
        SectorEdge("ASML", "TSM", "EUV lithography systems"),
        SectorEdge("AMAT", "TSM", "Deposition & etch equipment"),
        SectorEdge("LRCX", "TSM", "Etch & deposition tools"),
        SectorEdge("KLAC", "TSM", "Process inspection systems"),
        SectorEdge("ENTG", "TSM", "Specialty materials"),
        # Foundry → Silicon
        SectorEdge("TSM", "NVDA", "Leading-edge fab (3nm/2nm)"),
        SectorEdge("TSM", "AMD", "Foundry services"),
        SectorEdge("TSM", "AVGO", "Foundry services"),
        SectorEdge("TSM", "MRVL", "Foundry services"),
        SectorEdge("ASX", "NVDA", "Advanced packaging (CoWoS)"),
        SectorEdge("ASX", "AMD", "Advanced packaging"),
        # Silicon → Hyperscalers
        SectorEdge("NVDA", "MSFT", "GPU compute for Azure/OpenAI"),
        SectorEdge("NVDA", "GOOGL", "GPU compute for GCP"),
        SectorEdge("NVDA", "AMZN", "GPU compute for AWS"),
        SectorEdge("NVDA", "META", "GPU compute for AI infra"),
        SectorEdge("NVDA", "ORCL", "GPU compute for OCI"),
        SectorEdge("AMD", "MSFT", "CPU/GPU for Azure"),
        SectorEdge("AMD", "AMZN", "CPU/GPU for AWS"),
        SectorEdge("INTC", "MSFT", "Server CPUs"),
        SectorEdge("AVGO", "GOOGL", "Custom TPU / networking"),
        SectorEdge("AVGO", "META", "Custom networking ASICs"),
        SectorEdge("MRVL", "AMZN", "Custom silicon"),
        # Networking → Hyperscalers
        SectorEdge("ANET", "MSFT", "Data center switching"),
        SectorEdge("ANET", "META", "Data center switching"),
        SectorEdge("CSCO", "AMZN", "Enterprise networking"),
        SectorEdge("COHR", "ANET", "Optical transceivers"),
        # Infra → Hyperscalers (via colo)
        SectorEdge("VRT", "EQIX", "Liquid cooling systems"),
        SectorEdge("VRT", "DLR", "Cooling & power mgmt"),
        SectorEdge("EQIX", "MSFT", "Colocation services"),
        SectorEdge("EQIX", "AMZN", "Colocation services"),
        SectorEdge("DLR", "GOOGL", "Data center capacity"),
        SectorEdge("DLR", "META", "Data center capacity"),
        SectorEdge("VST", "EQIX", "Power generation"),
        SectorEdge("CEG", "DLR", "Nuclear power"),
    ],
)
