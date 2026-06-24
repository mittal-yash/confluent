# Flink SQL run order (Confluent Cloud)

**Confluent Flink accepts only ONE SQL statement per Run.** Pasting a whole file
like `01_enrichment.sql` fails with:

> Only a single statement is supported at a time.

Use the files in **`flink/steps/`** — each file is exactly one statement. Run them
**in numeric order** (`001` → `035`) in your Flink SQL workspace. Note `023` is
split into `023` (CREATE TABLE) + `023b` (INSERT job) — run `023` then `023b`.

| Step | File | Type | Notes |
|------|------|------|-------|
| 001–008 | `001_table_*.sql` … `008_table_*.sql` | CREATE TABLE | Output topics; run each separately |
| 009 | `009_table_asset_specs_ref.sql` | CREATE TABLE | Upsert ref table (PK asset_id) |
| 009b | `009b_job_insert_asset_specs_ref.sql` | **INSERT job** | Re-key connector topic `gridsentinel.mongo.gridsentinel.asset_specs` |
| 010 | `010_table_assets_ref.sql` | CREATE TABLE | Upsert ref table (PK asset_id) |
| 010b | `010b_job_insert_assets_ref.sql` | **INSERT job** | Re-key connector topic `gridsentinel.mongo.gridsentinel.assets` |
| 011 | `011_view_telemetry_anomaly.sql` | CREATE VIEW | passes through `$rowtime` |
| 012–014 | `012` … `014` | CREATE VIEW | Scoring pipeline views |
| 015 | `015_job_insert_enriched.sql` | **INSERT job** | Long-running — leave running |
| 016 | `016_job_insert_incidents.sql` | **INSERT job** | Long-running — leave running |
| 017 | `017_connection_gemini.sql` | CREATE CONNECTION | Vertex AI service-account JSON |
| 018 | `018_model_diagnosis.sql` | CREATE MODEL | Set **Catalog** + **Database** in workspace first |
| 019–022 | `019` … `022` | CONNECTION / MODEL / TABLE | RAG setup (embed conn, embed model, mongo conn, vector table) |
| 023 | `023_table_diagnosis_query.sql` | CREATE TABLE | Holds the per-incident query vector |
| 023b | `023b_job_insert_diagnosis_query.sql` | **INSERT job** | `AI_EMBEDDING` → query vector (gated incidents) |
| 024 | `024_table_diagnosis_retrieved.sql` | CREATE TABLE | Holds retrieved manual context |
| 024b | `024b_job_insert_diagnosis_retrieved.sql` | **INSERT job** | `VECTOR_SEARCH_AGG` over Atlas |
| 025 | `025_job_insert_diagnosed_rag.sql` | **INSERT job** | Gemini + vector RAG |
| 026 | `026_job_insert_diagnosed_gated.sql` | **INSERT job** | Deterministic monitor path |
| 027–029 | `027` … `029` | CREATE VIEW | Planning views |
| 030–033 | `030` … `033` | **INSERT jobs** | Plan, act, control, work orders |
| 034 | `034_view_recovery_candidates.sql` | CREATE VIEW | |
| 035 | `035_job_insert_resolved.sql` | **INSERT job** | Closes the loop |

Files named `*_job_*` start a **streaming Flink job** — submit and leave them
running. `CREATE VIEW` / `CREATE TABLE` / `CREATE CONNECTION` / `CREATE MODEL`
complete immediately.

The bundled `00_tables.sql` … `07_verify.sql` files are **reference copies** of
the same logic (easier to read); always execute from **`steps/`** in the UI.

## Quick fix: enriched stalls / no incidents (temporal join + topic name)

Symptoms: `gridsentinel.telemetry.enriched` stops emitting new rows (tap with
`--from latest` returns nothing) and `gridsentinel.incidents` is empty, even
though `gridsentinel.telemetry` is flowing.

Two causes, both fixed by steps 009/009b/010/010b:

1. **Wrong reference topic name.** The MongoDB Source Connector produces
   `gridsentinel.mongo.gridsentinel.asset_specs` (topic.prefix `gridsentinel.mongo`
   + database `gridsentinel` + collection). Joining `gridsentinel.mongo.asset_specs`
   reads an empty table. Confirm the real name: `SHOW TABLES LIKE '%asset_specs%';`
2. **Temporal join watermark stall.** Static reference data is idle, so its
   watermark never advances and the temporal join buffers everything. Set an
   idle-timeout **before** submitting the scoring INSERT jobs:

```sql
SET 'sql.tables.scan.idle-timeout' = '15s';
```

Then (re)run 009 → 009b → 010 → 010b → 011 → 012 → 013 → 014 → 015 → 016.

Also re-run `python -m data.seed_mongo` if substation specs show
`warning_temp_c = 0.0` (stale seed) — otherwise substations use wrong thresholds.

## Quick fix: "Internal error occurred" on CREATE VIEW with AI_EMBEDDING / VECTOR_SEARCH_AGG

A `CREATE VIEW` that wraps an ML table function (`AI_EMBEDDING`, `VECTOR_SEARCH_AGG`,
`ML_PREDICT`) can fail with a generic `Internal error occurred`. The embedding
model itself is fine — verify with:

```sql
SELECT CARDINALITY(embedding) AS dims
FROM (VALUES ('test')) AS t(txt),
  LATERAL TABLE(AI_EMBEDDING('gemini_embed', t.txt));
```

**Fix:** materialize to a TABLE + INSERT job instead of a VIEW. That's why step
`023` is now `023_table_diagnosis_query.sql` (CREATE TABLE) + `023b_job_insert_
diagnosis_query.sql` (INSERT). If **`024`** (`VECTOR_SEARCH_AGG`) hits the same
error, apply the same pattern: create a `diagnosis_retrieved` TABLE then an
`INSERT INTO diagnosis_retrieved SELECT ... VECTOR_SEARCH_AGG ...` job.

**Dimension check (causes `MongoCommandException` in `024b`):**
`gemini-embedding-001` returns **3072** dims, and Flink's `CREATE MODEL` cannot
request a reduced output dimension — so `VECTOR_SEARCH_AGG` always sends a 3072-d
query vector. The Atlas index `manuals_vector_index` and the `manual_chunks`
corpus MUST therefore be **3072** too, or Atlas rejects the query with a generic
`MongoCommandException`. To align: set `EMBEDDING_DIM=3072` in `.env`, re-run
`python -m data.embed_manuals`, then rebuild the index
(`python -m scripts.create_vector_index`) and wait for `READY`.

## Quick fix: action chain (004–008) must be UPSERT, not append

Step **029 `plan_ranked`** picks the nearest available crew with a `ROW_NUMBER()`
Top-1 over a join with the `crew_latest` dedup — that is an **updating stream**
(there is no append-only way to compute "best crew"). So step **030** writes a
changelog, and an append-only `actions.planned` sink fails with *"doesn't support
consuming update and delete changes"*. The change cascades to everything that
reads `actions.planned` (031/032/033) and the recovery chain (034/035).

**Fix:** tables **004–008** (`actions.planned`, `actions.executed`, `control`,
`work.orders`, `incidents.resolved`) are declared **`changelog.mode = 'upsert'`
with `PRIMARY KEY (incident_id)`** and `DISTRIBUTED BY (incident_id)` — one current
plan / action / control / work order / resolution per incident. The generated IDs
(`action_id`, `work_order_id`) are derived deterministically from `incident_id`
(not `UUID()`) so upserts stay idempotent.

If you already created 004–008 as append, drop and recreate them (no writers exist
until you run 030+, so this is safe):

```sql
DROP TABLE `gridsentinel.actions.planned`;
DROP TABLE `gridsentinel.actions.executed`;
DROP TABLE `gridsentinel.control`;
DROP TABLE `gridsentinel.work.orders`;
DROP TABLE `gridsentinel.incidents.resolved`;
```

Then re-run **004 → 008** (new upsert defs) before running **030 → 035**.

## Quick fix: "StreamPhysicalIntervalJoin doesn't support consuming update and delete changes" (034/035)

`034 recovery_candidates` matches each executed action with the first healthy
(`info`) telemetry within 10 minutes. The original join was an **interval join**
(`BETWEEN` two `$rowtime` time-attributes), which only accepts **append-only**
inputs — but `actions.executed` is now an **upsert** changelog (it reads the
upsert `actions.planned`). So the interval join is rejected.

**Fix (already applied in `034`):** cast `$rowtime` to a plain `TIMESTAMP(3)` on
both sides. That removes the time-attribute property, so Flink uses a **regular
join**, which consumes the changelog. The 10-minute window stays as an ordinary
`BETWEEN` predicate.

A regular join keeps both inputs in state, and the `info` telemetry side is
high-volume, so set a state TTL **before** submitting `035` to bound it:

```sql
SET 'sql.state-ttl' = '30 min';
```

## Quick fix: "A current, valid catalog has not been set" (CREATE MODEL / CONNECTION)

Confluent Flink scopes models and connections to **catalog** (your Cloud
**environment**) + **database** (your **Kafka cluster**).

**Option A (easiest):** In the Flink SQL workspace header, set the dropdowns before
running steps **017–021**:

- **Catalog** → your environment name (run `SHOW CATALOGS;` — e.g. `confluent-ai-day`)
- **Database** → your cluster id (run `SHOW DATABASES;` — often `cluster_0`)

Then re-run **017** (connection) then **018** (model).

**Option B:** Fully qualify the name (same catalog/db as your `gridsentinel.*` tables):

```sql
CREATE MODEL `confluent-ai-day`.`cluster_0`.diagnosis_model
INPUT (`input` STRING)
OUTPUT (`output` STRING)
WITH ( ... );
```

Use your actual catalog/database strings from `SHOW CATALOGS` / `SHOW DATABASES`.

## Quick fix: "doesn't support consuming update and delete changes"

This happens at step **015** when steps **009–010** used the old `ROW_NUMBER`
"latest row" views. That pattern emits UPDATE/DELETE changelog; a regular join
forwards retractions, but `telemetry.enriched` is **append-only**.

**Fix (re-run from 009):**

1. If you already created the old views, drop them (one statement each):
   `DROP VIEW IF EXISTS spec_latest;` then `DROP VIEW IF EXISTS asset_latest;`
2. Re-run **`009`** and **`010`** (now declare PRIMARY KEY on the mongo tables).
3. Re-run **`012`** (temporal join: `FOR SYSTEM_TIME AS OF a.\`$rowtime\``).
4. Re-run **`015`** (and **`016`** for incidents).

See [Confluent Flink troubleshooting FAQ](https://developer.confluent.io/faq/apache-flink/troubleshooting/) — replace regular joins with **temporal joins**.

If **`009`/`010` fail** on table name, run `SHOW TABLES LIKE '%asset%'` — the
Mongo connector may use `gridsentinel.mongo.gridsentinel.asset_specs` instead of
`gridsentinel.mongo.asset_specs`. Adjust the backtick name to match.

If **`012` fails** with `Column '$rowtime' not found in table 'a'`, re-run **`011`**
first — `telemetry_anomaly` must pass through `$rowtime` (not only `event_time`)
for temporal joins. Then `DROP VIEW telemetry_scored;` and re-run **`012`**.

## Quick fix for your current error

You were on step 01. Run these **two separate** submits:

1. Contents of `steps/009_view_spec_latest.sql`
2. Contents of `steps/010_view_asset_latest.sql`

Then continue from `011` onward.
