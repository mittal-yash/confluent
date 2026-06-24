"""GridSentinel live operations dashboard (Streamlit).

    streamlit run dashboard/app.py

Shows the fleet status, live telemetry vs thresholds, the agent activity trail
(sense -> reason -> act -> verify), work orders, and the business-impact KPIs.
Includes scenario buttons to inject faults live during the demo.
"""
from __future__ import annotations

import os
import sys

# Ensure the repo root is importable no matter how streamlit launches this file.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import streamlit as st

from common.config import settings
from common.kafka_io import KafkaProducer
from dashboard.state import DashboardState, start_consumer
from simulators.scenario import (
    SC_BEARING,
    SC_HEATWAVE,
    SC_LEAK,
    SC_OVERLOAD,
    build_command,
)

st.set_page_config(page_title="GridSentinel", page_icon="grid", layout="wide")

SEV_COLOR = {"info": "#2e7d32", "warning": "#f9a825", "critical": "#c62828"}


@st.cache_resource
def get_live_state() -> DashboardState:
    state = DashboardState()
    start_consumer(state)
    return state


@st.cache_resource
def get_producer() -> KafkaProducer:
    return KafkaProducer()


# ---- header -------------------------------------------------------------
st.title("GridSentinel - Autonomous Grid & Asset Resilience")

state = get_live_state()


def fire(cmd: dict, msg: str) -> None:
    """Publish a scenario to Kafka; the cloud simulator + Flink react."""
    producer = get_producer()
    producer.send(settings.topics.scenario, cmd, key=cmd.get("scenario", "s"))
    producer.flush()
    st.success(msg)


# ---- sidebar: scenario controls -----------------------------------------
with st.sidebar:
    st.header("Inject scenario")
    asset_ids = sorted(state.assets.keys())
    regions = sorted({a["region"] for a in state.assets.values()})

    region = st.selectbox("Region (heatwave)", regions)
    if st.button("Trigger HEATWAVE", width="stretch"):
        fire(build_command(SC_HEATWAVE, region=region, ramp_seconds=20),
             f"Heatwave injected in {region}")

    tx_assets = [a for a in asset_ids if a.startswith("TX") or a.startswith("LN")]
    overload_asset = st.selectbox("Asset (overload)", tx_assets)
    if st.button("Trigger OVERLOAD", width="stretch"):
        fire(build_command(SC_OVERLOAD, asset_id=overload_asset, ramp_seconds=18),
             f"Overload injected on {overload_asset}")

    pumps = [a for a in asset_ids if a.startswith("PMP")]
    pump_asset = st.selectbox("Pump (mechanical)", pumps)
    c1, c2 = st.columns(2)
    if c1.button("Bearing", width="stretch"):
        fire(build_command(SC_BEARING, asset_id=pump_asset, ramp_seconds=15),
             f"Bearing fault on {pump_asset}")
    if c2.button("Cavitation", width="stretch"):
        fire(build_command(SC_LEAK, asset_id=pump_asset, ramp_seconds=15),
             f"Cavitation on {pump_asset}")

    if st.button("CLEAR all scenarios", type="primary", width="stretch"):
        fire(build_command("clear"), "Cleared all scenarios")

    st.divider()
    refresh_s = st.slider("Auto-refresh (s)", 1, 10, 2)
    focus = st.selectbox("Focus asset (chart)", asset_ids,
                         index=asset_ids.index(tx_assets[0]) if tx_assets else 0)

# ---- live dashboard body (auto-reruns smoothly via a fragment) ----------
@st.fragment(run_every=refresh_s)
def render_dashboard() -> None:
    counts = state.severity_counts()
    k = st.columns(6)
    k[0].metric("Assets", len(state.assets))
    k[1].metric("Healthy", counts["info"])
    k[2].metric("Warning", counts["warning"])
    k[3].metric("Critical", counts["critical"])
    k[4].metric("Autonomous actions", state.counters["actions"])
    k[5].metric("Outages avoided", state.counters["outages_avoided"],
                help=f"Approx INR {state.cost_saved_inr():,} saved")

    st.progress(
        min(1.0, state.counters["resolved"] / max(1, state.counters["incidents"])),
        text=f"Incidents resolved: {state.counters['resolved']} / {state.counters['incidents']}  "
             f"|  Estimated impact avoided: INR {state.cost_saved_inr():,}",
    )

    left, right = st.columns([1.1, 1])

    with left:
        st.subheader("Fleet status")
        with state.lock:
            rows = sorted(
                (dict(a) for a in state.assets.values()),
                key=lambda a: {"critical": 0, "warning": 1, "info": 2}[a["severity"]],
            )
        table = [
            {
                "asset": r["asset_id"],
                "type": r["asset_type"],
                "region": r["region"],
                "severity": r["severity"],
                "temp_C": round(r["temp_c"], 1) if r["temp_c"] is not None else None,
                "load_%": round(r["load_pct"], 0) if r["load_pct"] is not None else None,
                "fault": round(r["fault_score"], 2),
            }
            for r in rows
        ]
        st.dataframe(table, width="stretch", height=300, hide_index=True)

        map_pts = [
            {"lat": r["lat"], "lon": r["lon"]}
            for r in rows
            if r["severity"] in ("warning", "critical")
        ]
        if map_pts:
            st.caption("Assets needing attention")
            try:
                st.map(map_pts)
            except Exception:
                pass

    with right:
        st.subheader(f"Telemetry - {focus}")
        with state.lock:
            hist = list(state.history.get(focus, []))
            asset = dict(state.assets.get(focus, {}))
        if hist:
            try:
                import plotly.graph_objects as go

                xs = list(range(len(hist)))
                temps = [h["temp_c"] for h in hist]
                fig = go.Figure()
                fig.add_trace(go.Scatter(y=temps, x=xs, name="temp C", line=dict(color="#1565c0")))
                if asset.get("warning_temp_c"):
                    fig.add_hline(y=asset["warning_temp_c"], line_dash="dot",
                                  annotation_text="warning", line_color="#f9a825")
                if asset.get("critical_temp_c"):
                    fig.add_hline(y=asset["critical_temp_c"], line_dash="dash",
                                  annotation_text="critical", line_color="#c62828")
                fig.update_layout(height=240, margin=dict(l=0, r=0, t=10, b=0),
                                  yaxis_title="C", showlegend=False)
                st.plotly_chart(fig, width="stretch")
            except Exception:
                st.line_chart({"temp_C": [h["temp_c"] for h in hist]})
            st.line_chart({"fault_score": [h["fault_score"] for h in hist]})
        else:
            st.info("Waiting for telemetry... trigger a scenario from the sidebar.")

    st.subheader("Agent activity (sense -> reason -> act -> verify)")
    with state.lock:
        feed = list(state.feed)[:25]
    for e in feed:
        st.markdown(f"`{e['ts']}`  **{e['stage']}**  {e['text']}")

    with state.lock:
        wos = list(state.work_orders.values())
    if wos:
        st.subheader("Work orders (written back to MongoDB)")
        st.dataframe(
            [
                {"id": w["work_order_id"], "asset": w["asset_id"], "priority": w["priority"],
                 "crew": w.get("crew_id"), "SLA_h": w["sla_hours"], "status": w["status"]}
                for w in wos[-12:]
            ],
            width="stretch", hide_index=True,
        )


render_dashboard()
