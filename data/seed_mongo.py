"""Seed MongoDB with the static reference data the platform joins against.

    python -m data.seed_mongo

Collections: assets, asset_specs, crew, maintenance_history.
Idempotent: each collection is replaced on every run.
"""
from __future__ import annotations

import random
import time

from common.assets import build_fleet
from common.crew import build_crews
from common.config import settings
from common.mongo import (
    COL_ASSETS,
    COL_CREW,
    COL_MAINT,
    COL_SPECS,
    get_db,
)
from .specs import build_spec_doc

_MAINT_NOTES = [
    "Routine oil sample - DGA within limits",
    "Cooling fan bank serviced",
    "Thermographic survey - no hotspots",
    "Bearing lubrication completed",
    "Protection relay calibration",
    "Conductor clearance inspection",
    "Breaker maintenance",
]


def _maintenance_for(asset_id: str, rng: random.Random) -> list[dict]:
    n = rng.randint(2, 5)
    now = time.time()
    out = []
    for i in range(n):
        days_ago = rng.randint(30, 900)
        out.append(
            {
                "asset_id": asset_id,
                "ts": int((now - days_ago * 86400) * 1000),
                "note": rng.choice(_MAINT_NOTES),
                "technician": f"CREW-{rng.randint(1, 8):02d}",
                "result": rng.choice(["ok", "ok", "ok", "follow_up"]),
            }
        )
    return sorted(out, key=lambda d: d["ts"], reverse=True)


def main() -> None:
    db = get_db()
    rng = random.Random(settings.random_seed)

    fleet = build_fleet(settings.random_seed)
    crews = build_crews(settings.random_seed)

    # assets
    db[COL_ASSETS].drop()
    db[COL_ASSETS].insert_many([a.to_dict() for a in fleet])
    db[COL_ASSETS].create_index("asset_id", unique=True)

    # specs
    db[COL_SPECS].drop()
    db[COL_SPECS].insert_many([build_spec_doc(a) for a in fleet])
    db[COL_SPECS].create_index("asset_id", unique=True)

    # crew
    db[COL_CREW].drop()
    db[COL_CREW].insert_many([c.to_dict() for c in crews])
    db[COL_CREW].create_index("crew_id", unique=True)

    # maintenance history
    db[COL_MAINT].drop()
    history: list[dict] = []
    for a in fleet:
        history.extend(_maintenance_for(a.asset_id, rng))
    db[COL_MAINT].insert_many(history)
    db[COL_MAINT].create_index("asset_id")

    print(f"Seeded MongoDB '{settings.mongodb_db}':")
    print(f"  assets               {db[COL_ASSETS].count_documents({})}")
    print(f"  asset_specs          {db[COL_SPECS].count_documents({})}")
    print(f"  crew                 {db[COL_CREW].count_documents({})}")
    print(f"  maintenance_history  {db[COL_MAINT].count_documents({})}")
    print("Next: python -m data.embed_manuals")


if __name__ == "__main__":
    main()
