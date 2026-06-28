from sectors.schema import SectorDefinition, SupplyChainLayer

SECTOR = SectorDefinition(
    id="energy-transition",
    name="Energy Transition",
    description="Grid modernisation, storage, electrification, and clean energy enablers",
    benchmark_etf="ICLN",
    layers=[
        SupplyChainLayer(
            name="Critical Minerals & Materials",
            role="upstream",
            tickers={
                "ALB": "Lithium production",
                "SQM": "Lithium & iodine",
                "MP": "Rare earth elements",
                "FCX": "Copper (electrification backbone)",
            },
        ),
        SupplyChainLayer(
            name="Solar & Wind Equipment",
            role="midstream",
            tickers={
                "FSLR": "Thin-film solar modules (US mfg)",
                "ENPH": "Micro-inverters",
                "GEV": "Wind turbines (onshore/offshore)",
                "TT": "HVAC & thermal management",
            },
        ),
        SupplyChainLayer(
            name="Grid & Storage",
            role="midstream",
            tickers={
                "PWR": "Grid services & transmission",
                "ETN": "Power management & grid equipment",
                "GNRC": "Backup power & energy storage",
                "STEM": "AI-driven energy storage",
            },
        ),
        SupplyChainLayer(
            name="Utilities & Integrators",
            role="downstream",
            tickers={
                "NEE": "Renewables utility (largest wind/solar)",
                "CEG": "Nuclear clean energy",
                "AES": "Solar + storage projects",
                "VST": "Power generation (gas-to-nuclear transition)",
            },
        ),
    ],
)
