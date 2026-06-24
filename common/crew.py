"""Canonical field-crew roster shared by the simulator, planner, and Mongo seed."""
from __future__ import annotations

import random
from dataclasses import asdict, dataclass, field

from .assets import REGIONS

SKILLS = ["transformer", "line", "pump", "general"]


@dataclass
class Crew:
    crew_id: str
    name: str
    region: str
    lat: float
    lon: float
    status: str
    skills: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return asdict(self)


def build_crews(seed: int = 42) -> list[Crew]:
    rng = random.Random(seed + 7)
    crews: list[Crew] = []
    n = 0
    for region in REGIONS:
        for _ in range(2):
            n += 1
            skills = rng.sample(SKILLS, k=rng.randint(2, 3))
            if "general" not in skills:
                skills.append("general")
            crews.append(
                Crew(
                    crew_id=f"CREW-{n:02d}",
                    name=f"Field Crew {n:02d}",
                    region=region,
                    lat=12.97 + rng.uniform(-0.5, 0.5),
                    lon=77.59 + rng.uniform(-0.5, 0.5),
                    status="available",
                    skills=sorted(set(skills)),
                )
            )
    return crews


def crews_by_id(seed: int = 42) -> dict[str, Crew]:
    return {c.crew_id: c for c in build_crews(seed)}
