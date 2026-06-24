"""Stateful physics engine for the synthetic grid.

Maintains per-asset dynamic state and eases each signal toward a target that is
a function of (baseline + active scenarios + active mitigations). Because the
engine reacts to control actions, the loop physically closes: when an agent
isolates or reroutes an asset, telemetry actually recovers -> the verifier sees
green again. That is the demo's payoff.
"""
from __future__ import annotations

import math
import random
import time

from common.assets import (
    ASSET_LINE,
    ASSET_PUMP,
    ASSET_TRANSFORMER,
    Asset,
    build_fleet,
)
from .scenario import SC_BEARING, SC_CLEAR, SC_HEATWAVE, SC_LEAK, SC_OVERLOAD

BASE_AMBIENT_C = 30.0
SQRT3 = math.sqrt(3.0)

# Easing factors per signal (fraction of gap closed each tick).
ALPHA = {
    "ambient": 0.05,
    "temp": 0.06,
    "vibration": 0.12,
    "pressure": 0.12,
    "load": 0.15,
}


def _stable_base_load(asset_id: str) -> float:
    rng = random.Random(hash(asset_id) & 0xFFFFFFFF)
    return rng.uniform(48.0, 68.0)


class SimEngine:
    def __init__(self, seed: int = 42) -> None:
        self.fleet: list[Asset] = build_fleet(seed)
        self.assets: dict[str, Asset] = {a.asset_id: a for a in self.fleet}
        self.rng = random.Random(seed)
        self.regions = sorted({a.region for a in self.fleet})

        # dynamic per-asset state
        self.state: dict[str, dict] = {}
        for a in self.fleet:
            self.state[a.asset_id] = {
                "load_pct": _stable_base_load(a.asset_id),
                "temp_c": a.nominal_temp_c or 45.0,
                "vibration": a.nominal_vibration_mm_s,
                "pressure": a.nominal_oil_pressure_kpa,
            }
        # dynamic per-region ambient
        self.ambient: dict[str, float] = {r: BASE_AMBIENT_C for r in self.regions}

        # active scenarios + mitigations
        self.scenarios: list[dict] = []
        self.mitigations: dict[str, dict] = {}

    # ---- external control -------------------------------------------------
    def apply_scenario(self, cmd: dict) -> None:
        scenario = cmd.get("scenario")
        if scenario == SC_CLEAR:
            self.scenarios.clear()
            self.mitigations.clear()
            return
        cmd = {**cmd, "started_at": time.time()}
        # replace any existing scenario of the same type+target
        self.scenarios = [
            s
            for s in self.scenarios
            if not (
                s.get("scenario") == scenario
                and s.get("asset_id") == cmd.get("asset_id")
                and s.get("region") == cmd.get("region")
            )
        ]
        self.scenarios.append(cmd)

    def apply_control(self, action: dict) -> None:
        """An agent action recovers the affected asset."""
        asset_id = action.get("asset_id")
        atype = action.get("action_type")
        if not asset_id or asset_id not in self.assets:
            return
        if atype in ("isolate", "reroute_load", "throttle_load", "dispatch_crew"):
            self.mitigations[asset_id] = {"mode": atype, "since": time.time()}
            # rerouting raises load on tie assets
            if atype == "reroute_load":
                for tie in self.assets[asset_id].tie_assets:
                    if tie in self.assets:
                        self.mitigations.setdefault(tie, {})
                        self.mitigations[tie]["mode"] = "absorb"
                        self.mitigations[tie]["since"] = time.time()

    # ---- helpers ----------------------------------------------------------
    def _scenario_for_asset(self, a: Asset) -> list[dict]:
        out = []
        for s in self.scenarios:
            if s.get("asset_id") and s["asset_id"] == a.asset_id:
                out.append(s)
            elif s.get("region") and s["region"] == a.region:
                out.append(s)
            elif not s.get("asset_id") and not s.get("region"):
                out.append(s)  # global
        return out

    @staticmethod
    def _ramp(scn: dict) -> float:
        ramp = max(1.0, float(scn.get("ramp_seconds", 25.0)))
        return min(1.0, (time.time() - scn.get("started_at", time.time())) / ramp)

    def _diurnal(self) -> float:
        # subtle +/-2C daily-ish wave so charts breathe
        return 2.0 * math.sin(time.time() / 120.0)

    # ---- tick -------------------------------------------------------------
    def step(self) -> None:
        # 1) regional ambient targets (heatwave drives these up)
        for r in self.regions:
            target = BASE_AMBIENT_C + self._diurnal()
            for s in self.scenarios:
                if s["scenario"] == SC_HEATWAVE and (
                    not s.get("region") or s["region"] == r
                ):
                    target += 12.0 * s.get("intensity", 1.0) * self._ramp(s)
            self.ambient[r] += (target - self.ambient[r]) * ALPHA["ambient"]

        # 2) per-asset dynamics
        for a in self.fleet:
            st = self.state[a.asset_id]
            amb = self.ambient[a.region]
            mit = self.mitigations.get(a.asset_id)
            scns = self._scenario_for_asset(a)

            # ---- load target ----
            load_target = _stable_base_load(a.asset_id) + 2.0 * math.sin(
                time.time() / 90.0 + hash(a.asset_id) % 7
            )
            for s in scns:
                if s["scenario"] == SC_OVERLOAD and a.asset_type in (
                    ASSET_TRANSFORMER,
                    ASSET_LINE,
                ):
                    load_target = (
                        load_target
                        + (118.0 - load_target) * self._ramp(s) * s.get("intensity", 1.0)
                    )
                if s["scenario"] == SC_HEATWAVE:
                    load_target += 12.0 * self._ramp(s)  # AC demand surge
            if mit:
                if mit["mode"] == "isolate":
                    load_target = 1.0
                elif mit["mode"] in ("reroute_load", "throttle_load"):
                    load_target = min(load_target, 52.0)
                elif mit["mode"] == "absorb":
                    load_target += 18.0  # picking up rerouted load
            st["load_pct"] += (load_target - st["load_pct"]) * ALPHA["load"]

            # ---- temperature target (rises with load and ambient) ----
            rated_rise = (a.nominal_temp_c or 45.0) - BASE_AMBIENT_C
            temp_target = amb + rated_rise * (max(st["load_pct"], 1.0) / 60.0) ** 1.6
            st["temp_c"] += (temp_target - st["temp_c"]) * ALPHA["temp"]

            # ---- vibration target ----
            vib_target = a.nominal_vibration_mm_s
            for s in scns:
                if s["scenario"] == SC_BEARING:
                    peak = max(a.warning_vibration_mm_s * 1.9, 9.0)
                    vib_target = (
                        a.nominal_vibration_mm_s
                        + (peak - a.nominal_vibration_mm_s)
                        * self._ramp(s)
                        * s.get("intensity", 1.0)
                    )
            if mit and mit["mode"] == "dispatch_crew":
                vib_target = a.nominal_vibration_mm_s  # crew repairs
            st["vibration"] += (vib_target - st["vibration"]) * ALPHA["vibration"]

            # ---- oil/pump pressure target ----
            press_target = a.nominal_oil_pressure_kpa
            for s in scns:
                if s["scenario"] == SC_LEAK and a.asset_type == ASSET_PUMP:
                    floor = a.nominal_oil_pressure_kpa * 0.55
                    press_target = (
                        a.nominal_oil_pressure_kpa
                        - (a.nominal_oil_pressure_kpa - floor)
                        * self._ramp(s)
                        * s.get("intensity", 1.0)
                    )
            if mit and mit["mode"] in ("isolate", "dispatch_crew"):
                press_target = a.nominal_oil_pressure_kpa
            st["pressure"] += (press_target - st["pressure"]) * ALPHA["pressure"]

    # ---- emit -------------------------------------------------------------
    def telemetry(self) -> list[dict]:
        now = int(time.time() * 1000)
        out = []
        for a in self.fleet:
            st = self.state[a.asset_id]
            load_pct = max(0.0, st["load_pct"] + self.rng.gauss(0, 0.6))
            load_mw = a.rated_load_mw * load_pct / 100.0
            v = a.voltage_kv * (1.0 + self.rng.gauss(0, 0.004))
            current_a = (
                load_mw * 1e6 / (SQRT3 * max(v, 1.0) * 1e3) if v else 0.0
            )
            rec = {
                "asset_id": a.asset_id,
                "asset_type": a.asset_type,
                "ts": now,
                "region": a.region,
                "substation": a.substation,
                "voltage_kv": round(v, 3),
                "current_a": round(current_a, 2),
                "load_mw": round(load_mw, 3),
                "load_pct": round(load_pct, 2),
                "temp_c": round(st["temp_c"] + self.rng.gauss(0, 0.4), 2),
                "ambient_c": round(self.ambient[a.region] + self.rng.gauss(0, 0.2), 2),
                "vibration_mm_s": (
                    round(max(0.0, st["vibration"] + self.rng.gauss(0, 0.08)), 3)
                    if a.nominal_vibration_mm_s
                    else None
                ),
                "oil_pressure_kpa": (
                    round(max(0.0, st["pressure"] + self.rng.gauss(0, 1.5)), 2)
                    if a.nominal_oil_pressure_kpa
                    else None
                ),
            }
            out.append(rec)
        return out

    def weather(self) -> list[dict]:
        now = int(time.time() * 1000)
        out = []
        for r in self.regions:
            amb = self.ambient[r]
            heat = any(
                s["scenario"] == SC_HEATWAVE and (not s.get("region") or s["region"] == r)
                for s in self.scenarios
            )
            out.append(
                {
                    "region": r,
                    "ts": now,
                    "ambient_c": round(amb + self.rng.gauss(0, 0.2), 2),
                    "humidity_pct": round(self.rng.uniform(35, 70), 1),
                    "wind_kph": round(self.rng.uniform(2, 18), 1),
                    "condition": "heatwave" if heat else "clear",
                }
            )
        return out
