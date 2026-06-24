# GridSentinel - business case (the impact slide)

## The problem
Grid and critical-infrastructure operators drown in telemetry but act slowly.
A transformer thermal excursion or a developing bearing fault is often caught
only after an outage or equipment loss. Control-room staff cannot watch every
asset, every second, and cross-reference every spec sheet and SOP in time.

## The solution
GridSentinel turns continuously enriched, governed data streams into autonomous
action. It detects, diagnoses (grounded in the asset's own documentation),
acts within the operating envelope, and verifies recovery - in seconds, not the
30-90 minutes a manual response takes.

## Quantified impact (illustrative, per event)
- **Avoided unplanned outage:** a single distribution transformer failure can
  mean 4-12 MWh of unserved energy plus penalties - INR 3-10 lakh per event.
- **Asset protection:** acting before top-oil exceeds the critical limit avoids
  insulation damage that can cost INR 50 lakh+ to replace a large transformer.
- **Faster MTTR:** autonomous reroute/throttle in seconds vs tens of minutes of
  manual diagnosis and dispatch.
- **Crew efficiency:** the right crew with the right skills is dispatched
  automatically, with the work order pre-filled.

## Why now / why this stack
- **Confluent** makes the data trustworthy and in motion: schemas, data-quality
  rules, lineage - the trust foundation an operator needs to let an agent act.
- **Flink** does deterministic, low-latency detection and AI Model Inference at
  the source, so the expensive LLM only runs on real incidents (cost control).
- **MongoDB** is both the system of record (assets, specs, work orders) and the
  RAG store (vector search over O&M manuals) - grounding every decision.

## Scale story
The same pattern generalizes across Burns & McDonnell domains: power generation
and T&D, water/wastewater, oil & gas pipelines, and facilities - any fleet of
instrumented assets with operating envelopes and response SOPs. Add a sensor
stream and a manual, and GridSentinel covers a new asset class.

## Headline
**One platform that watches every asset, reasons over its own engineering
standards, and acts autonomously to keep critical infrastructure online -
turning data in motion into uptime.**
