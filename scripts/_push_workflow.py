"""Throwaway: push a controlled 10-message sequence for ONE asset to exercise
the full pipeline (detect -> diagnose -> plan -> act -> control -> verify).

  3 baseline 'info'  -> establishes a normal window
  4 critical spikes  -> incident -> (Gemini) diagnosis -> plan -> work order
  <wait for the action to execute>
  3 recovery 'info'  -> recovery_candidates match -> resolved
"""
from __future__ import annotations

import math
import time

from common.assets import fleet_by_id
from common.config import settings
from common.kafka_io import KafkaProducer

ASSET_ID = "TX-S1-1"
WAIT_FOR_ACTION_S = 75

asset = fleet_by_id()[ASSET_ID]
producer = KafkaProducer()


def rec(temp_c: float, load_pct: float) -> dict:
    load_mw = asset.rated_load_mw * load_pct / 100.0
    v = float(asset.voltage_kv)
    current_a = load_mw * 1e6 / (math.sqrt(3) * v * 1e3) if v else 0.0
    return {
        "asset_id": asset.asset_id,
        "asset_type": asset.asset_type,
        "ts": int(time.time() * 1000),
        "region": asset.region,
        "substation": asset.substation,
        "voltage_kv": v,
        "current_a": round(current_a, 2),
        "load_mw": round(load_mw, 3),
        "load_pct": round(load_pct, 2),
        "temp_c": round(temp_c, 2),
        "ambient_c": 33.0,
        "vibration_mm_s": asset.nominal_vibration_mm_s or None,
        "oil_pressure_kpa": asset.nominal_oil_pressure_kpa or None,
    }


def send(label: str, r: dict) -> None:
    producer.send(settings.topics.telemetry, r, key=r["asset_id"])
    producer.flush(5.0)
    print(f"  [{label}] temp_c={r['temp_c']} load_pct={r['load_pct']} ts={r['ts']}")
    time.sleep(1.5)


print(f"Asset {ASSET_ID}: {asset.asset_type} {asset.region}/{asset.substation} "
      f"warn={asset.warning_temp_c}C crit={asset.critical_temp_c}C")

print("1) baseline (info):")
for _ in range(3):
    send("base", rec(58.0, 55.0))

print("2) critical spikes (temp>critical, high load):")
for _ in range(4):
    send("CRIT", rec(102.0, 116.0))

print(f"3) waiting {WAIT_FOR_ACTION_S}s for diagnose->plan->act->control ...")
time.sleep(WAIT_FOR_ACTION_S)

print("4) recovery (info, after the action so 034/035 can match):")
for _ in range(3):
    send("recv", rec(57.0, 50.0))

producer.flush(10.0)
print("done: pushed 10 telemetry messages.")
