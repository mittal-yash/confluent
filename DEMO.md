# GridSentinel - the 4-minute demo

## Setup (before you go on stage)

Cloud must already be up: Confluent Cloud topics + connectors live, the Flink
jobs (`flink/00`-`07`) running, and the Atlas vector index built. See
`infra/confluent_cloud_setup.md`. Then on the laptop, two terminals from the
repo root:

```bash
# T1 - data in motion (also obeys Flink's control commands)
python -m simulators.run_simulators

# T2 - dashboard
streamlit run dashboard/app.py
```

Wait ~15s until the dashboard shows all assets green and the feed is quiet.

## The script

1. **Set the scene (30s).** "This is a regional grid - substations,
   transformers, transmission lines - streaming live telemetry through Confluent
   Cloud. Every reading is validated against a governed schema and joined in
   Flink to asset specs sourced from MongoDB. All green."

2. **Trigger the fault (20s).** Sidebar -> pick a region -> **Trigger HEATWAVE**.
   (For a sharper spike, also **Trigger OVERLOAD** on a transformer in that region.)
   "A heatwave just hit. Ambient is climbing, AC demand is surging."

3. **Detection (30s).** Watch a transformer go amber then red. "Flink computed a
   rolling z-score and a logistic fault score - it just crossed threshold. Only
   this incident wakes Gemini; everything else stays deterministic and cheap."

4. **Reason + act (60s).** Point at the activity feed:
   - **DIAGNOSE**: "Flink embedded the incident with Gemini, ran a vector search
     over the equipment manuals in MongoDB Atlas, and grounded Gemini on the
     retrieved passage - and cites it. No hallucination; it reasons only over
     governed documents."
   - **PLAN / ACT**: "It chose `reroute_load` from a constrained action set,
     picked the nearest qualified crew, and executed - publishing a control
     command and filing a work order back to MongoDB."

5. **Recovery + verify (40s).** The transformer temperature falls; it returns to
   green. "Flink watched the telemetry recover and closed the incident
   autonomously. Sense -> reason -> act -> verify, with a human-optional loop."

6. **Governance + impact (40s).** Switch to Confluent Stream Lineage. "Every
   action traces back to validated, governed source data." Then the KPI row:
   "One outage avoided, ~INR 3,00,000 saved, in under a minute, with no operator."

## Reset between runs
Sidebar -> **CLEAR all scenarios**. Assets recover to green within ~20s.

## If something misbehaves
- No data on dashboard: confirm T1 is running and `.env` points at your
  Confluent Cloud bootstrap server; run `python -m scripts.doctor`.
- Nothing diagnosed: check the Flink jobs (`05_diagnosis.sql`) are running and
  the Gemini connection + Atlas vector index exist.
- Inspect any topic live: `python -m scripts.tap gridsentinel.incidents.diagnosed`.
