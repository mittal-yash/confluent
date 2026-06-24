"""Shared dashboard state + background Kafka consumer.

Both the Streamlit UI and the console dashboard build on this. A single
consumer thread tails all the GridSentinel topics and folds them into an
in-memory view (latest per asset, recent incidents, the agent activity feed,
work orders, and headline KPIs).
"""
from __future__ import annotations

import threading
import time
import uuid
from collections import deque

from common.assets import build_fleet
from common.config import settings

# rough avoided-cost per autonomously-mitigated critical event (INR)
OUTAGE_COST_INR = 300000


class DashboardState:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        fleet = build_fleet(settings.random_seed)
        self.assets: dict[str, dict] = {
            a.asset_id: {
                "asset_id": a.asset_id,
                "asset_type": a.asset_type,
                "region": a.region,
                "criticality": a.criticality,
                "lat": a.lat,
                "lon": a.lon,
                "severity": "info",
                "temp_c": None,
                "load_pct": None,
                "fault_score": 0.0,
                "warning_temp_c": a.warning_temp_c,
                "critical_temp_c": a.critical_temp_c,
            }
            for a in fleet
        }
        self.history: dict[str, deque] = {a.asset_id: deque(maxlen=240) for a in fleet}
        self.incidents: deque = deque(maxlen=50)
        self.feed: deque = deque(maxlen=120)
        self.work_orders: dict[str, dict] = {}
        self.resolved: deque = deque(maxlen=50)
        self.counters = {
            "incidents": 0,
            "actions": 0,
            "resolved": 0,
            "outages_avoided": 0,
        }
        self._started = False

    # ---- KPIs -------------------------------------------------------------
    def severity_counts(self) -> dict[str, int]:
        out = {"info": 0, "warning": 0, "critical": 0}
        with self.lock:
            for a in self.assets.values():
                out[a.get("severity", "info")] = out.get(a.get("severity", "info"), 0) + 1
        return out

    def cost_saved_inr(self) -> int:
        return self.counters["outages_avoided"] * OUTAGE_COST_INR

    # ---- ingest -----------------------------------------------------------
    def _add_feed(self, stage: str, text: str) -> None:
        self.feed.appendleft({"ts": time.strftime("%H:%M:%S"), "stage": stage, "text": text})

    def apply(self, topic: str, value: dict) -> None:
        t = settings.topics
        with self.lock:
            if topic == t.enriched:
                aid = value.get("asset_id")
                a = self.assets.get(aid)
                if a:
                    a.update(
                        {
                            "severity": value.get("severity", "info"),
                            "temp_c": value.get("temp_c"),
                            "load_pct": value.get("load_pct"),
                            "fault_score": value.get("fault_score", 0.0),
                        }
                    )
                    self.history[aid].append(
                        {
                            "ts": value.get("ts"),
                            "temp_c": value.get("temp_c"),
                            "load_pct": value.get("load_pct"),
                            "fault_score": value.get("fault_score", 0.0),
                        }
                    )
            elif topic == t.incidents:
                self.counters["incidents"] += 1
                self.incidents.appendleft(value)
                self._add_feed("DETECT", f"{value.get('severity','').upper()} on "
                               f"{value.get('asset_id')} ({value.get('signal')}="
                               f"{value.get('value',0):.1f}, score={value.get('fault_score',0):.2f})")
            elif topic == t.diagnosed:
                cites = ", ".join(sorted({c.get("source", "") for c in (value.get("dx_citations") or [])}))
                self._add_feed("DIAGNOSE", f"{value.get('asset_id')}: "
                               f"{value.get('dx_root_cause','')[:80]} -> "
                               f"{value.get('dx_recommended_action')} "
                               + (f"[cites: {cites}]" if cites else ""))
            elif topic == t.planned:
                self._add_feed("PLAN", f"{value.get('asset_id')}: {value.get('action_type')} "
                               f"crew={value.get('crew_id')} {value.get('priority')}")
            elif topic == t.executed:
                self.counters["actions"] += 1
                self._add_feed("ACT", f"{value.get('asset_id')}: EXECUTED "
                               f"{value.get('action_type')}")
            elif topic == t.work_orders:
                self.work_orders[value.get("work_order_id")] = value
            elif topic == t.resolved:
                if value.get("status") == "resolved":
                    self.counters["resolved"] += 1
                    if value.get("action_type") in ("isolate", "reroute_load", "throttle_load"):
                        self.counters["outages_avoided"] += 1
                    self.resolved.appendleft(value)
                    self._add_feed("VERIFY", f"{value.get('asset_id')}: RESOLVED in "
                                   f"{value.get('recovery_seconds')}s")


def start_consumer(state: DashboardState) -> threading.Thread:
    from common.kafka_io import KafkaConsumer

    t = settings.topics
    topics = [t.enriched, t.incidents, t.diagnosed, t.planned, t.executed,
              t.work_orders, t.resolved]

    def _run() -> None:
        consumer = KafkaConsumer(f"dashboard-{uuid.uuid4().hex[:8]}", topics,
                                 auto_offset_reset="latest")
        for topic, _key, value in consumer.stream(1.0):
            # upsert decision topics (planned/executed/work.orders/resolved) can
            # emit null-value tombstones on delete; nothing to apply for those.
            if value is None:
                continue
            try:
                state.apply(topic, value)
            except Exception as exc:  # noqa: BLE001
                print(f"[dashboard] ingest error: {exc}")

    th = threading.Thread(target=_run, daemon=True)
    th.start()
    state._started = True
    return th
