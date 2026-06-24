# Flink stream processing (the GridSentinel "brain")

The entire decision pipeline runs here in Confluent Cloud for Apache Flink. Your
laptop only runs the Faker simulator and the dashboard; everything below -
enrichment, scoring, **Gemini diagnosis**, planning, action, and verification -
is Flink SQL.

## Run order (in a Flink workspace attached to a compute pool)

**Important:** Confluent Flink allows **one SQL statement per Run**. Do not paste
whole `00_tables.sql` or `01_enrichment.sql` files — use **`flink/steps/`**
(`001` through `035`, in order). See `flink/steps/README.md` for the full table.

| Stage | Steps | What it does |
|---|---|---|
| tables | `001`–`008` | CREATE TABLE output topics |
| enrichment | `009`–`010` | PRIMARY KEY on mongo reference tables (temporal join) |
| anomaly | `011` | Rolling z-score per asset |
| score + route | `012`–`016` | Fault score, enriched + incidents jobs |
| AI model | `017`–`018` | Gemini connection + diagnosis model |
| diagnose (RAG) | `019`–`026` | Embed, vector search, Gemini, diagnosed jobs |
| plan + act | `027`–`033` | Crew pick, control, work orders |
| verify | `034`–`035` | Recovery → resolved |

> `INSERT INTO` steps (`015`, `016`, `025`, …) are long-running Flink jobs.
> Submit each and leave it running. Views are instant.

## Inputs (auto-inferred tables - you do NOT create these)

On Confluent Cloud every Kafka topic is already a Flink table with a `$rowtime`
column + default watermark. These must be producing data first:

- `gridsentinel.telemetry`, `gridsentinel.crew.location` - from the laptop simulator
- `gridsentinel.mongo.assets`, `gridsentinel.mongo.asset_specs` - from the MongoDB Source Connector

The RAG corpus is **not** a Kafka topic - it's the Atlas `manual_chunks`
collection, queried directly by Flink as a read-only external table (see below).

## RAG (true vector search, in Flink)

`05_diagnosis.sql` does real retrieval-augmented generation entirely server-side:
1. `AI_EMBEDDING` embeds each incident as a query (Gemini `gemini-embedding-001`).
2. `VECTOR_SEARCH_AGG` runs ANN search over the Atlas `manual_chunks` vector
   index (`manuals_vector_index`) and returns the best-matching passage.
3. That passage is injected into the Gemini prompt via `ML_PREDICT`, and the
   diagnosis cites it.

The corpus must be embedded with the **same** model (`gemini-embedding-001`,
3072-dim) via `python -m data.embed_manuals` - see `infra/mongodb_atlas_setup.md`.

## Vertex AI setup

Flink steps `017`–`020` use the **Vertex AI** provider (GCP billing/credits) with a
service-account `service-key` — not Google AI Studio API keys. See
`infra/confluent_cloud_setup.md` Phase 7. Region must match your Flink compute pool
(e.g. `asia-south1`).

## Known caveats (deploying blind on Confluent Cloud)

These statements follow Confluent's documented patterns, but the Flink dialect
evolves - validate as you go (the cloud `02_*`/`03_*` validation taps in
`infra/confluent_cloud_setup.md` exist for this):

- Output topics: let `00_tables.sql` create them; do **not** pre-create the
  output topics with `python -m common.kafka_io` in cloud mode.
- `06_planning_action.sql` (crew ranking join) is the most likely to need
  tuning; if it errors, simplify to skill+region match without the distance sort.
- `07_verify.sql` uses an interval join; the escalation/timeout branch is
  intentionally omitted (needs `MATCH_RECOGNIZE`/timers).
