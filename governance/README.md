# Governance: trustworthy data in motion

This is the rubric's third pillar and the part most teams skip. GridSentinel
makes data trust *visible* on stage.

## 1. Schema Registry (governed contracts)
Every topic has a registered Avro schema (`schemas/*.avsc`). Producers and Flink
both validate against it, so no malformed event can enter the pipeline.

```bash
python -m schemas.register_schemas
```

## 2. Data Quality rules
`data_quality_rules.json` defines CEL rules attached to subjects (e.g. top-oil
temperature must be -40..200 C, fault_score must be in 0..1). On Confluent Cloud
Stream Governance these run on write and route violations to a DLQ. Demo it by
producing an out-of-range reading and showing it land in the DLQ instead of
poisoning the agents.

## 3. Stream Lineage
Confluent Cloud auto-builds a lineage graph. After the pipeline runs, open
**Stream Lineage** to show the full provenance:

```
MongoDB (assets) --connector--> asset_specs --+
                                               +--> Flink (enrich -> score -> route)
SCADA telemetry -----------------------------+        |
                                                       v
                                              incidents -> agent mesh -> actions/work_orders -> MongoDB sink
```

This proves every autonomous action is traceable back to governed, validated
source data -- the core "trustworthy data in motion" claim.

## 4. Tags & RBAC (`tags.json`)
Topics and fields are classified (`critical-infrastructure`, `PII`,
`agent-action`, `ai-derived`). The agent service account gets least-privilege
access: READ telemetry/incidents, WRITE control/work_orders only; PII crew data
is restricted. Apply via Stream Catalog.

## Why it matters for autonomy
Autonomous actions are only as trustworthy as their inputs. Schema contracts +
DQ rules + lineage + classification are what let a utility safely let an agent
isolate a transformer without a human in the loop.
