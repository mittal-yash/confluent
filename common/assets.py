"""Canonical synthetic asset fleet for a regional power grid.

This is the single source of truth shared by the simulators, the MongoDB seed,
and the stream processor so every component agrees on asset IDs and specs.
The fleet is fully deterministic for a given seed -> repeatable demos.
"""
from __future__ import annotations

import random
from dataclasses import asdict, dataclass, field

# Regions roughly modelled on a utility service territory.
REGIONS = ["North", "South", "East", "West"]

ASSET_TRANSFORMER = "transformer"
ASSET_LINE = "transmission_line"
ASSET_SUBSTATION = "substation"
ASSET_PUMP = "water_pump"


@dataclass
class Asset:
    asset_id: str
    name: str
    asset_type: str
    region: str
    substation: str
    voltage_kv: int
    criticality: str               # critical | high | medium
    install_year: int
    lat: float
    lon: float
    # numeric operating envelope (units noted)
    rated_load_mw: float = 0.0     # MW
    nominal_temp_c: float = 0.0    # C, normal top-oil/winding temp
    warning_temp_c: float = 0.0    # C
    critical_temp_c: float = 0.0   # C
    nominal_vibration_mm_s: float = 0.0  # mm/s RMS
    warning_vibration_mm_s: float = 0.0
    nominal_oil_pressure_kpa: float = 0.0  # kPa (pumps/transformers)
    # topology used for autonomous rerouting / isolation
    feeds: list[str] = field(default_factory=list)        # downstream assets
    tie_assets: list[str] = field(default_factory=list)   # alternate routes
    spec_sheet_id: str = ""        # link to MongoDB spec doc

    def to_dict(self) -> dict:
        return asdict(self)


def _coord(rng: random.Random) -> tuple[float, float]:
    # Loosely around Bengaluru so the demo map looks plausible.
    return (12.97 + rng.uniform(-0.6, 0.6), 77.59 + rng.uniform(-0.6, 0.6))


def build_fleet(seed: int = 42) -> list[Asset]:
    rng = random.Random(seed)
    assets: list[Asset] = []
    substations: list[Asset] = []

    # --- Substations (one or two per region) ---
    for region in REGIONS:
        n_subs = 2 if region in ("North", "South") else 1
        for s in range(n_subs):
            sid = f"SUB-{region[:1]}{s + 1}"
            lat, lon = _coord(rng)
            sub = Asset(
                asset_id=sid,
                name=f"{region} Substation {s + 1}",
                asset_type=ASSET_SUBSTATION,
                region=region,
                substation=sid,
                voltage_kv=220,
                criticality="critical",
                install_year=rng.randint(1995, 2018),
                lat=lat,
                lon=lon,
                rated_load_mw=rng.choice([180.0, 220.0, 250.0]),
                nominal_temp_c=42.0,
                warning_temp_c=70.0,
                critical_temp_c=85.0,
                spec_sheet_id=f"spec-{sid}",
            )
            substations.append(sub)
            assets.append(sub)

    sub_ids = [s.asset_id for s in substations]

    # --- Transformers (2 per substation) ---
    for sub in substations:
        for t in range(2):
            tid = f"TX-{sub.asset_id[4:]}-{t + 1}"
            rated = rng.choice([80.0, 100.0, 125.0, 160.0])
            assets.append(
                Asset(
                    asset_id=tid,
                    name=f"Power Transformer {tid}",
                    asset_type=ASSET_TRANSFORMER,
                    region=sub.region,
                    substation=sub.asset_id,
                    voltage_kv=rng.choice([132, 220]),
                    criticality=rng.choice(["critical", "high", "high"]),
                    install_year=rng.randint(1998, 2020),
                    lat=sub.lat + rng.uniform(-0.01, 0.01),
                    lon=sub.lon + rng.uniform(-0.01, 0.01),
                    rated_load_mw=rated,
                    nominal_temp_c=58.0,
                    warning_temp_c=85.0,
                    critical_temp_c=95.0,
                    nominal_vibration_mm_s=1.2,
                    warning_vibration_mm_s=4.5,
                    nominal_oil_pressure_kpa=140.0,
                    spec_sheet_id=f"spec-{tid}",
                )
            )

    # --- Transmission lines between substations (ring + ties) ---
    for i, sub in enumerate(substations):
        nxt = substations[(i + 1) % len(substations)]
        lid = f"LN-{sub.asset_id[4:]}-{nxt.asset_id[4:]}"
        # tie route = the line on the "other side" of the ring
        tie = substations[(i + 2) % len(substations)]
        tie_lid = f"LN-{sub.asset_id[4:]}-{tie.asset_id[4:]}"
        assets.append(
            Asset(
                asset_id=lid,
                name=f"Transmission Line {sub.asset_id}->{nxt.asset_id}",
                asset_type=ASSET_LINE,
                region=sub.region,
                substation=sub.asset_id,
                voltage_kv=220,
                criticality="critical",
                install_year=rng.randint(1990, 2015),
                lat=(sub.lat + nxt.lat) / 2,
                lon=(sub.lon + nxt.lon) / 2,
                rated_load_mw=rng.choice([150.0, 200.0, 250.0]),
                nominal_temp_c=45.0,         # conductor temp
                warning_temp_c=75.0,
                critical_temp_c=90.0,
                feeds=[nxt.asset_id],
                tie_assets=[tie_lid],
                spec_sheet_id=f"spec-{lid}",
            )
        )

    # --- A few water pumps (shows the platform generalises beyond power) ---
    for region in ("North", "South"):
        for p in range(2):
            pid = f"PMP-{region[:1]}{p + 1}"
            lat, lon = _coord(rng)
            assets.append(
                Asset(
                    asset_id=pid,
                    name=f"{region} Water Pump {p + 1}",
                    asset_type=ASSET_PUMP,
                    region=region,
                    substation=rng.choice(sub_ids),
                    voltage_kv=11,
                    criticality=rng.choice(["high", "medium"]),
                    install_year=rng.randint(2005, 2021),
                    lat=lat,
                    lon=lon,
                    rated_load_mw=2.5,
                    nominal_temp_c=50.0,
                    warning_temp_c=78.0,
                    critical_temp_c=92.0,
                    nominal_vibration_mm_s=1.8,
                    warning_vibration_mm_s=7.1,
                    nominal_oil_pressure_kpa=320.0,
                    spec_sheet_id=f"spec-{pid}",
                )
            )

    return assets


def fleet_by_id(seed: int = 42) -> dict[str, Asset]:
    return {a.asset_id: a for a in build_fleet(seed)}


if __name__ == "__main__":
    fleet = build_fleet()
    by_type: dict[str, int] = {}
    for a in fleet:
        by_type[a.asset_type] = by_type.get(a.asset_type, 0) + 1
    print(f"Fleet size: {len(fleet)}")
    for k, v in sorted(by_type.items()):
        print(f"  {k:18s} {v}")
