"""Scenario definitions + a CLI to inject faults live during the demo.

A scenario is published to the `gridsentinel.scenario` control topic; the
simulator consumes it and bends the physics accordingly. This is what powers
the on-stage "trigger a heatwave and watch GridSentinel react" moment.

CLI examples:
    python -m simulators.scenario heatwave --region South
    python -m simulators.scenario transformer_overload --asset TX-S1-1
    python -m simulators.scenario bearing_failure --asset PMP-N1
    python -m simulators.scenario clear
"""
from __future__ import annotations

import argparse
import time

# Scenario types
SC_HEATWAVE = "heatwave"
SC_OVERLOAD = "transformer_overload"
SC_BEARING = "bearing_failure"
SC_LEAK = "pump_cavitation"
SC_CLEAR = "clear"

ALL_SCENARIOS = [SC_HEATWAVE, SC_OVERLOAD, SC_BEARING, SC_LEAK, SC_CLEAR]


def build_command(
    scenario: str,
    asset_id: str | None = None,
    region: str | None = None,
    intensity: float = 1.0,
    ramp_seconds: float = 25.0,
) -> dict:
    return {
        "scenario": scenario,
        "asset_id": asset_id or "",
        "region": region or "",
        "intensity": float(intensity),
        "ramp_seconds": float(ramp_seconds),
        "ts": int(time.time() * 1000),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Inject a GridSentinel scenario")
    parser.add_argument("scenario", choices=ALL_SCENARIOS)
    parser.add_argument("--asset", dest="asset_id", default=None)
    parser.add_argument("--region", dest="region", default=None)
    parser.add_argument("--intensity", type=float, default=1.0)
    parser.add_argument("--ramp", dest="ramp_seconds", type=float, default=25.0)
    args = parser.parse_args()

    from common.config import settings
    from common.kafka_io import KafkaProducer

    cmd = build_command(
        args.scenario,
        asset_id=args.asset_id,
        region=args.region,
        intensity=args.intensity,
        ramp_seconds=args.ramp_seconds,
    )
    producer = KafkaProducer()
    producer.send(settings.topics.scenario, cmd, key=args.scenario)
    producer.flush()
    print(f"Injected scenario: {cmd}")


if __name__ == "__main__":
    main()
