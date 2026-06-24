# GridSentinel — Full Schema Reference

All Kafka topic contracts (Avro + Flink DDL) and MongoDB collection shapes.
Schema Registry subjects use pattern: `{topic}-value`.

---

## Kafka input topics — Avro (Schema Registry)

### `gridsentinel.telemetry`

```json
{
  "type": "record",
  "name": "Telemetry",
  "namespace": "com.burnsmcdonnell.gridsentinel",
  "doc": "Raw SCADA-style sensor reading from a grid asset.",
  "fields": [
    { "name": "asset_id", "type": "string" },
    { "name": "asset_type", "type": "string" },
    { "name": "ts", "type": { "type": "long", "logicalType": "timestamp-millis" } },
    { "name": "region", "type": "string" },
    { "name": "substation", "type": "string" },
    { "name": "voltage_kv", "type": "double" },
    { "name": "current_a", "type": "double" },
    { "name": "load_mw", "type": "double" },
    { "name": "load_pct", "type": "double", "doc": "load as percent of rated" },
    { "name": "temp_c", "type": "double" },
    { "name": "ambient_c", "type": "double" },
    { "name": "vibration_mm_s", "type": ["null", "double"], "default": null },
    { "name": "oil_pressure_kpa", "type": ["null", "double"], "default": null }
  ]
}
```

### `gridsentinel.crew.location`

```json
{
  "type": "record",
  "name": "CrewLocation",
  "namespace": "com.burnsmcdonnell.gridsentinel",
  "doc": "Live field-crew position and availability (PII-tagged).",
  "fields": [
    { "name": "crew_id", "type": "string" },
    { "name": "ts", "type": { "type": "long", "logicalType": "timestamp-millis" } },
    { "name": "region", "type": "string" },
    { "name": "lat", "type": "double" },
    { "name": "lon", "type": "double" },
    { "name": "status", "type": "string", "doc": "available|enroute|on_site|off_shift" },
    { "name": "skills", "type": { "type": "array", "items": "string" } }
  ]
}
```

### `gridsentinel.weather`

```json
{
  "type": "record",
  "name": "Weather",
  "namespace": "com.burnsmcdonnell.gridsentinel",
  "doc": "Per-region weather observation enriching grid stress analysis.",
  "fields": [
    { "name": "region", "type": "string" },
    { "name": "ts", "type": { "type": "long", "logicalType": "timestamp-millis" } },
    { "name": "ambient_c", "type": "double" },
    { "name": "humidity_pct", "type": "double" },
    { "name": "wind_kph", "type": "double" },
    { "name": "condition", "type": "string", "doc": "clear|cloudy|rain|storm|heatwave" }
  ]
}
```

## Kafka: `gridsentinel.scenario` (JSON, simulator control topic)

```json
{
  "scenario": "heatwave | transformer_overload | bearing_failure | pump_cavitation | clear",
  "asset_id": "string",
  "region": "string",
  "intensity": "double",
  "ramp_seconds": "double",
  "ts": "long (epoch ms)"
}
```

---

## Kafka / Flink tables — Flink SQL DDL

Flink-produced topics use `value.format = avro-registry`. Intermediate Flink tables (`asset_specs_ref`, `assets_ref`, `diagnosis_query`, `diagnosis_retrieved`) are Kafka-backed.

```sql
CREATE TABLE `gridsentinel.telemetry.enriched` (
  asset_id         STRING,
  asset_type       STRING,
  `ts`             TIMESTAMP_LTZ(3),
  region           STRING,
  substation       STRING,
  load_pct         DOUBLE,
  temp_c           DOUBLE,
  vibration_mm_s   DOUBLE,
  oil_pressure_kpa DOUBLE,
  fault_score      DOUBLE,
  severity         STRING,
  warning_temp_c   DOUBLE,
  critical_temp_c  DOUBLE
) DISTRIBUTED BY (asset_id) INTO 6 BUCKETS
WITH ('changelog.mode' = 'append', 'value.format' = 'avro-registry');

CREATE TABLE `gridsentinel.incidents` (
  asset_id         STRING,
  incident_id      STRING,
  asset_type       STRING,
  `ts`             TIMESTAMP_LTZ(3),
  region           STRING,
  substation       STRING,
  signal           STRING,
  `value`          DOUBLE,
  threshold        DOUBLE,
  zscore           DOUBLE,
  fault_score      DOUBLE,
  severity         STRING,
  warning_temp_c   DOUBLE,
  critical_temp_c  DOUBLE,
  warning_vibration_mm_s DOUBLE,
  nominal_oil_pressure_kpa DOUBLE,
  rated_load_mw    DOUBLE,
  tie_assets       STRING,
  reasoning_needed BOOLEAN
) DISTRIBUTED BY (asset_id) INTO 6 BUCKETS
WITH ('changelog.mode' = 'append', 'value.format' = 'avro-registry');

CREATE TABLE `gridsentinel.incidents.diagnosed` (
  asset_id             STRING,
  incident_id          STRING,
  asset_type           STRING,
  region               STRING,
  severity             STRING,
  signal               STRING,
  `value`              DOUBLE,
  fault_score          DOUBLE,
  zscore               DOUBLE,
  tie_assets           STRING,
  dx_root_cause        STRING,
  dx_recommended_action STRING,
  dx_rationale         STRING,
  dx_confidence        DOUBLE,
  dx_citations         ARRAY<ROW<`source` STRING, snippet STRING>>,
  dx_method            STRING,
  diagnosed_ts         TIMESTAMP_LTZ(3)
) DISTRIBUTED BY (asset_id) INTO 6 BUCKETS
WITH ('changelog.mode' = 'append', 'value.format' = 'avro-registry');

CREATE TABLE `gridsentinel.actions.planned` (
  incident_id      STRING,
  asset_id         STRING,
  plan_id          STRING,
  asset_type       STRING,
  region           STRING,
  `ts`             TIMESTAMP_LTZ(3),
  action_type      STRING,
  target_load_pct  STRING,
  tie_assets       STRING,
  need_crew        BOOLEAN,
  crew_id          STRING,
  need_work_order  BOOLEAN,
  priority         STRING,
  sla_hours        INT,
  severity         STRING,
  root_cause       STRING,
  rationale        STRING,
  confidence       DOUBLE,
  PRIMARY KEY (incident_id) NOT ENFORCED
) DISTRIBUTED BY (incident_id) INTO 6 BUCKETS
-- upsert keyed by incident_id: plan_ranked (Top-1 nearest crew) is an updating
-- stream, so an append sink would fail with "doesn't support consuming update
-- and delete changes". One current plan per incident.
WITH ('changelog.mode' = 'upsert', 'value.format' = 'avro-registry');

CREATE TABLE `gridsentinel.actions.executed` (
  incident_id  STRING,
  asset_id     STRING,
  action_id    STRING,
  `ts`         TIMESTAMP_LTZ(3),
  action_type  STRING,
  rationale    STRING,
  status       STRING,
  issued_by    STRING,
  PRIMARY KEY (incident_id) NOT ENFORCED
) DISTRIBUTED BY (incident_id) INTO 6 BUCKETS
-- upsert: reads the upsert actions.planned changelog. One current action/incident.
WITH ('changelog.mode' = 'upsert', 'value.format' = 'avro-registry');

CREATE TABLE `gridsentinel.control` (
  incident_id  STRING,
  asset_id     STRING,
  action_id    STRING,
  `ts`         TIMESTAMP_LTZ(3),
  action_type  STRING,
  PRIMARY KEY (incident_id) NOT ENFORCED
) DISTRIBUTED BY (incident_id) INTO 6 BUCKETS
-- upsert: reads the upsert actions.planned changelog. One current control/incident.
WITH ('changelog.mode' = 'upsert', 'value.format' = 'avro-registry');

CREATE TABLE `gridsentinel.work.orders` (
  incident_id   STRING,
  work_order_id STRING,
  asset_id      STRING,
  `ts`          TIMESTAMP_LTZ(3),
  priority      STRING,
  crew_id       STRING,
  sla_hours     INT,
  description   STRING,
  status        STRING,
  PRIMARY KEY (incident_id) NOT ENFORCED
) DISTRIBUTED BY (incident_id) INTO 6 BUCKETS
-- upsert keyed by incident_id (one current work order per incident). work_order_id
-- is derived deterministically from incident_id so upserts stay idempotent, and the
-- MongoDB sink connector upserts cleanly by key.
WITH ('changelog.mode' = 'upsert', 'value.format' = 'avro-registry');

CREATE TABLE `gridsentinel.incidents.resolved` (
  incident_id      STRING,
  asset_id         STRING,
  status           STRING,
  action_type      STRING,
  recovery_seconds DOUBLE,
  resolved_ts      TIMESTAMP_LTZ(3),
  PRIMARY KEY (incident_id) NOT ENFORCED
) DISTRIBUTED BY (incident_id) INTO 6 BUCKETS
-- upsert: 035 reads the upsert actions.executed changelog (via recovery_candidates)
-- and dedups Top-1 per incident. One resolution row per incident.
WITH ('changelog.mode' = 'upsert', 'value.format' = 'avro-registry');

CREATE TABLE asset_specs_ref (
  asset_id                 STRING,
  asset_type               STRING,
  warning_temp_c           DOUBLE,
  critical_temp_c          DOUBLE,
  warning_vibration_mm_s   DOUBLE,
  nominal_oil_pressure_kpa DOUBLE,
  rated_load_mw            DOUBLE,
  PRIMARY KEY (asset_id) NOT ENFORCED
) DISTRIBUTED BY (asset_id) INTO 6 BUCKETS
WITH ('changelog.mode' = 'upsert', 'value.format' = 'avro-registry');

CREATE TABLE assets_ref (
  asset_id    STRING,
  lat         DOUBLE,
  lon         DOUBLE,
  tie_assets  ARRAY<STRING>,
  PRIMARY KEY (asset_id) NOT ENFORCED
) DISTRIBUTED BY (asset_id) INTO 6 BUCKETS
WITH ('changelog.mode' = 'upsert', 'value.format' = 'avro-registry');

CREATE TABLE manual_chunks_vec (
  `text`      STRING,
  `source`    STRING,
  asset_type  STRING,
  embedding   ARRAY<FLOAT>
) WITH (
  'connector'           = 'mongodb',
  'mongodb.connection'  = 'mongodb_connection',
  'mongodb.database'    = 'gridsentinel',
  'mongodb.collection'  = 'manual_chunks',
  'mongodb.index'       = 'manuals_vector_index',
  'mongodb.numcandidates' = '100'
);

CREATE TABLE diagnosis_query (
  incident_id     STRING,
  asset_id        STRING,
  asset_type      STRING,
  region          STRING,
  severity        STRING,
  signal          STRING,
  `value`         DOUBLE,
  fault_score     DOUBLE,
  zscore          DOUBLE,
  tie_assets      STRING,
  warning_temp_c  DOUBLE,
  critical_temp_c DOUBLE,
  rated_load_mw   DOUBLE,
  qvec            ARRAY<FLOAT>,
  PRIMARY KEY (incident_id) NOT ENFORCED
) DISTRIBUTED BY (incident_id) INTO 6 BUCKETS
WITH ('changelog.mode' = 'append', 'value.format' = 'avro-registry');

CREATE TABLE diagnosis_retrieved (
  incident_id     STRING,
  asset_id        STRING,
  asset_type      STRING,
  region          STRING,
  severity        STRING,
  signal          STRING,
  `value`         DOUBLE,
  fault_score     DOUBLE,
  zscore          DOUBLE,
  tie_assets      STRING,
  warning_temp_c  DOUBLE,
  critical_temp_c DOUBLE,
  rated_load_mw   DOUBLE,
  context_text    STRING,
  context_source  STRING,
  PRIMARY KEY (incident_id) NOT ENFORCED
) DISTRIBUTED BY (incident_id) INTO 6 BUCKETS
WITH ('changelog.mode' = 'append', 'value.format' = 'avro-registry');

```

## MongoDB Atlas collections (`gridsentinel` database)

### `assets` (MongoDB Source Connector → `gridsentinel.mongo.gridsentinel.assets`)
```json
{
  "asset_id": "string",
  "name": "string",
  "asset_type": "transformer | transmission_line | substation | water_pump",
  "region": "string",
  "substation": "string",
  "voltage_kv": "int",
  "criticality": "critical | high | medium",
  "install_year": "int",
  "lat": "double",
  "lon": "double",
  "rated_load_mw": "double",
  "nominal_temp_c": "double",
  "warning_temp_c": "double",
  "critical_temp_c": "double",
  "nominal_vibration_mm_s": "double",
  "warning_vibration_mm_s": "double",
  "nominal_oil_pressure_kpa": "double",
  "feeds": ["string"],
  "tie_assets": ["string"],
  "spec_sheet_id": "string"
}
```

### `asset_specs` (Source Connector → `gridsentinel.mongo.gridsentinel.asset_specs`)
```json
{
  "spec_sheet_id": "string",
  "asset_id": "string",
  "asset_type": "string",
  "name": "string",
  "voltage_kv": "int",
  "rated_load_mw": "double",
  "nominal_temp_c": "double",
  "warning_temp_c": "double",
  "critical_temp_c": "double",
  "warning_vibration_mm_s": "double",
  "nominal_vibration_mm_s": "double",
  "nominal_oil_pressure_kpa": "double",
  "install_year": "int",
  "criticality": "string",
  "tie_assets": ["string"],
  "reference_guide": "string",
  "spec_text": "string"
}
```

### `crew` (static roster; live positions on `gridsentinel.crew.location` Kafka topic)
```json
{
  "crew_id": "string",
  "name": "string",
  "region": "string",
  "lat": "double",
  "lon": "double",
  "status": "available | enroute | on_site | off_shift",
  "skills": ["transformer", "line", "pump", "general"]
}
```

### `manual_chunks` (RAG corpus; queried by Flink `VECTOR_SEARCH_AGG`, not connector)
```json
{
  "text": "string",
  "source": "string",
  "chunk": "int",
  "asset_id": "string",
  "asset_type": "string",
  "embedding": ["float (3072 dimensions, gemini-embedding-001)"]
}
```
Vector index: `manuals_vector_index` on field `embedding`, 3072 dims, cosine.

### `work_orders` (MongoDB Sink Connector ← `gridsentinel.work.orders`)
```json
{
  "work_order_id": "string",
  "incident_id": "string",
  "asset_id": "string",
  "ts": "timestamp",
  "priority": "P1 | P2 | P3",
  "crew_id": "string",
  "sla_hours": "int",
  "description": "string",
  "status": "open | closed"
}
```

### `maintenance_history` (reference only, not in Flink pipeline)
```json
{
  "asset_id": "string",
  "ts": "long (epoch ms)",
  "note": "string",
  "technician": "string",
  "result": "ok | follow_up"
}
```

---

## Diagnosis model output (Gemini JSON in `dx_*` fields)

```json
{
  "root_cause": "string",
  "recommended_action": "monitor | throttle_load | reroute_load | dispatch_crew | isolate",
  "rationale": "string",
  "confidence": "number 0..1",
  "citation": "string"
}
```
