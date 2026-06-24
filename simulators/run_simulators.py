"""Run all synthetic producers in one process.

Threads:
  * scenario consumer  -> bends the physics on demand (live faults)
  * control consumer   -> recovers assets when agents act (closes the loop)
  * main loop          -> emits telemetry every tick, weather + crew periodically

    python -m simulators.run_simulators
"""
from __future__ import annotations

import random
import threading
import time

from common.config import settings
from common.crew import build_crews
from common.kafka_io import KafkaConsumer, KafkaProducer, ensure_topics
from .engine import SimEngine

_STOP = threading.Event()


def _scenario_listener(engine: SimEngine) -> None:
    consumer = KafkaConsumer(
        "sim-scenario", [settings.topics.scenario], auto_offset_reset="latest"
    )
    while not _STOP.is_set():
        rec = consumer.poll(1.0)
        if rec:
            _, _, cmd = rec
            engine.apply_scenario(cmd)
            print(f"[sim] scenario applied: {cmd.get('scenario')} "
                  f"asset={cmd.get('asset_id')} region={cmd.get('region')}")
    consumer.close()


def _control_listener(engine: SimEngine) -> None:
    consumer = KafkaConsumer(
        "sim-control", [settings.topics.control], auto_offset_reset="latest"
    )
    while not _STOP.is_set():
        rec = consumer.poll(1.0)
        if rec:
            _, _, action = rec
            # control is an upsert topic; deletes arrive as null-value tombstones.
            if action is None:
                continue
            engine.apply_control(action)
            print(f"[sim] control applied: {action.get('action_type')} "
                  f"-> {action.get('asset_id')} (asset will recover)")
    consumer.close()


def _move_crews(crews, rng) -> list[dict]:
    now = int(time.time() * 1000)
    out = []
    for c in crews:
        c.lat += rng.gauss(0, 0.0008)
        c.lon += rng.gauss(0, 0.0008)
        out.append(
            {
                "crew_id": c.crew_id,
                "ts": now,
                "region": c.region,
                "lat": round(c.lat, 5),
                "lon": round(c.lon, 5),
                "status": c.status,
                "skills": c.skills,
            }
        )
    return out


def main() -> None:
    try:
        ensure_topics()
    except Exception as exc:  # noqa: BLE001
        print(f"[sim] could not auto-create topics ({exc}); assuming they exist")

    engine = SimEngine(seed=settings.random_seed)
    crews = build_crews(settings.random_seed)
    rng = random.Random(settings.random_seed)
    producer = KafkaProducer()

    threading.Thread(target=_scenario_listener, args=(engine,), daemon=True).start()
    threading.Thread(target=_control_listener, args=(engine,), daemon=True).start()

    print(f"[sim] streaming {len(engine.fleet)} assets every {settings.tick_seconds}s "
          f"to {settings.bootstrap_servers}. Ctrl-C to stop.")

    tick = 0
    try:
        while True:
            engine.step()
            for rec in engine.telemetry():
                producer.send(settings.topics.telemetry, rec, key=rec["asset_id"])

            if tick % 5 == 0:  # weather + crew less frequently
                for rec in engine.weather():
                    producer.send(settings.topics.weather, rec, key=rec["region"])
                for rec in _move_crews(crews, rng):
                    producer.send(settings.topics.crew, rec, key=rec["crew_id"])

            producer.flush(5.0)
            tick += 1
            time.sleep(settings.tick_seconds)
    except KeyboardInterrupt:
        print("\n[sim] stopping...")
        _STOP.set()
        producer.flush()


if __name__ == "__main__":
    main()
