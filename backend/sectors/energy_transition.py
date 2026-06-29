from sectors.schema import SectorDefinition, SectorNode, SectorEdge

SECTOR = SectorDefinition(
    id="energy-transition",
    name="Energy Transition",
    description="Grid modernisation, storage, electrification, and clean energy enablers",
    benchmark_etf="ICLN",
    nodes=[
        # Critical Minerals & Materials (upstream)
        SectorNode("ALB", "Lithium production"),
        SectorNode("SQM", "Lithium & iodine"),
        SectorNode("MP", "Rare earth elements"),
        SectorNode("FCX", "Copper (electrification backbone)"),
        # Solar & Wind Equipment (midstream)
        SectorNode("FSLR", "Thin-film solar modules (US mfg)"),
        SectorNode("ENPH", "Micro-inverters"),
        SectorNode("GEV", "Wind turbines (onshore/offshore)"),
        SectorNode("TT", "HVAC & thermal management"),
        # Grid & Storage (midstream)
        SectorNode("PWR", "Grid services & transmission"),
        SectorNode("ETN", "Power management & grid equipment"),
        SectorNode("GNRC", "Backup power & energy storage"),
        SectorNode("STEM", "AI-driven energy storage"),
        # Utilities & Integrators (downstream)
        SectorNode("NEE", "Renewables utility (largest wind/solar)"),
        SectorNode("CEG", "Nuclear clean energy"),
        SectorNode("AES", "Solar + storage projects"),
        SectorNode("VST", "Power generation (gas-to-nuclear transition)"),
    ],
    edges=[
        # Minerals → Equipment
        SectorEdge("ALB", "FSLR", "Lithium for solar"),
        SectorEdge("SQM", "FSLR", "Lithium compounds"),
        SectorEdge("MP", "GEV", "Rare earths for wind turbines"),
        SectorEdge("FCX", "ETN", "Copper for power equipment"),
        SectorEdge("FCX", "PWR", "Copper for grid infrastructure"),
        # Equipment → Utilities
        SectorEdge("FSLR", "NEE", "Solar modules"),
        SectorEdge("FSLR", "AES", "Solar modules"),
        SectorEdge("ENPH", "NEE", "Micro-inverters"),
        SectorEdge("ENPH", "AES", "Micro-inverters"),
        SectorEdge("GEV", "NEE", "Wind turbines"),
        SectorEdge("TT", "NEE", "HVAC/thermal"),
        # Grid → Utilities
        SectorEdge("PWR", "NEE", "Grid services"),
        SectorEdge("PWR", "CEG", "Transmission services"),
        SectorEdge("ETN", "NEE", "Power management"),
        SectorEdge("ETN", "CEG", "Grid equipment"),
        SectorEdge("GNRC", "AES", "Energy storage"),
        SectorEdge("STEM", "AES", "AI-driven storage"),
    ],
)
