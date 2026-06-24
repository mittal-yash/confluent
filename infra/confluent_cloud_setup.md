# Confluent Cloud setup - fully-cloud architecture

In this mode **your laptop runs only two things**: the Faker simulator (the
"SCADA edge") and the dashboard. Everything else - enrichment, scoring, Gemini
diagnosis, planning, action, and verification - runs in **Confluent Cloud
Flink**. Gemini does the reasoning.

Diagnosis uses **true vector RAG inside Flink**: each incident is embedded with
Gemini `gemini-embedding-001` (`AI_EMBEDDING`), the relevant manual passages are
retrieved from the **MongoDB Atlas `manual_chunks` vector index**
(`VECTOR_SEARCH_AGG`), and that grounded context is fed to Gemini `gemini-2.5-flash`
(`ML_PREDICT`). Atlas is therefore both the reference store (assets/specs via the
Source Connector) AND the RAG vector store (queried directly by Flink).

```
LAPTOP                         CONFLUENT CLOUD                         GOOGLE
simulator  --telemetry/crew-->  Kafka --> Flink: enrich, score, route, Gemini
dashboard  <--all decisions---  Kafka     DIAGNOSE (embed -> VECTOR_   (embed +
                                           SEARCH_AGG@Atlas -> Gemini),  diagnose)
                                           plan, act, verify
                                MongoDB Atlas: assets/specs (connector)
                                               manual_chunks + vector index (RAG)
```

Run every `python -m ...` command from inside `gridsentinel/`. After each phase
run `python -m scripts.doctor` and the relevant `python -m scripts.tap <topic>`.
Only move on when a check is green - that's how you avoid a broken demo.

---

## Script reference - what each command does

These are the only commands in this runbook. "Once" = run a single time during
setup; "long-running" = leave the process up.

| Command | Runs on | When | What it does |
|---|---|---|---|
| `python -m scripts.doctor` | laptop | after every phase | Health-check: connects to Kafka + Schema Registry + MongoDB and reports what's missing. Your green/red gate. |
| `python -m scripts.tap <topic>` | laptop | after every phase | Prints live messages from a Kafka topic and exits PASS/FAIL. Proves data is actually flowing. Aliases: `telemetry`, `crew`, `incidents`, `enriched`, `diagnosed`, `planned`, `control`, `work_orders`, `resolved`. |
| `python -m common.kafka_io` | laptop | Phase 3, once | Creates the source Kafka topics (`gridsentinel.*`). |
| `python -m schemas.register_schemas` | laptop | Phase 3, once | Registers the Avro contracts (incl. data-quality rules) in Schema Registry - the governance layer. |
| `python -m simulators.run_simulators` | laptop | Phase 4, **long-running** | The synthetic SCADA edge: streams telemetry/weather/crew to Kafka **and** obeys `control` commands so assets physically recover (closes the loop). Keep it running for the whole demo. |
| `python -m simulators.scenario <type>` | laptop | Phase 11, once per fault | Injects a live fault (`heatwave`, `transformer_overload`, `bearing_failure`, `pump_cavitation`, `clear`). This is the on-stage "trigger" button. |
| `python -m data.seed_mongo` | laptop | Phase 5, once | Seeds MongoDB Atlas with the static reference data: `assets`, `asset_specs`, `crew`, `maintenance_history`. |
| `python -m data.embed_manuals` | laptop | Phase 5, once | Chunks + embeds the O&M manuals + spec sheets into the Atlas `manual_chunks` collection (3072-dim Gemini vectors). **This is the RAG corpus Flink searches.** |
| `streamlit run dashboard/app.py` | laptop | Phase 10, **long-running** | The live ops dashboard. Reads all decision topics from Kafka. |

The brain itself is **not** a Python command - it's the SQL in `flink/`, run
inside a Confluent Cloud Flink SQL workspace (Phase 8). See the table in
`flink/README.md` for what each `.sql` file does.

---

## Phase 1 - Cluster + governance
```bash
confluent login
confluent environment create gridsentinel
confluent kafka cluster create gridsentinel-cluster --cloud gcp --region asia-south1 --type basic
```
Enable **Stream Governance (Essentials, free)** on the environment.
✅ Cluster status = Running; copy the bootstrap endpoint.

> Tip: putting the cluster in **GCP** (e.g. `asia-south1`) keeps it close to
> Gemini/Vertex if you later use the Vertex provider.

## Phase 2 - Keys + `.env` + connectivity
Create a **Kafka API key** and a **Schema Registry API key**, then:
```
GRIDSENTINEL_MODE=cloud
KAFKA_BOOTSTRAP_SERVERS=<bootstrap>:9092
KAFKA_SECURITY_PROTOCOL=SASL_SSL
KAFKA_SASL_MECHANISM=PLAIN
KAFKA_SASL_USERNAME=<kafka api key>
KAFKA_SASL_PASSWORD=<kafka api secret>
SCHEMA_REGISTRY_URL=https://<sr-host>
SCHEMA_REGISTRY_AUTH=<sr key>:<sr secret>
USE_AVRO=true
MONGODB_URI=<atlas srv uri>
ATLAS_VECTOR_SEARCH=true
```
> The laptop never *queries* vectors - retrieval happens inside Flink
> (`VECTOR_SEARCH_AGG`). The laptop only *writes* the embedded corpus in Phase 5,
> which needs the embedding vars added there.
```bash
python -m scripts.doctor      # Kafka must connect
```

## Phase 3 - Source topics + schemas
Create only the **source** topics (let Flink create its own output topics):
```bash
python -m common.kafka_io           # ok if it also makes outputs; harmless
python -m schemas.register_schemas  # registers the telemetry/weather/crew Avro contracts
```
✅ `python -m scripts.doctor` -> Schema Registry shows subjects registered.

## Phase 4 - Start the laptop simulator + confirm telemetry
```bash
python -m simulators.run_simulators        # Terminal A (leave running)
python -m scripts.tap telemetry --count 3  # Terminal B -> must PASS
python -m scripts.tap crew --count 2
```

## Phase 5 - MongoDB Atlas + reference data + RAG vector corpus
1. Create an Atlas M0 cluster, DB user, and **allowlist your IP** (+ Confluent egress IPs for the connector).
2. Set Vertex AI vars in `.env` (so the RAG corpus matches what Flink queries with):
```
VERTEX_PROJECT_ID=<your-gcp-project>
VERTEX_LOCATION=asia-south1
GOOGLE_APPLICATION_CREDENTIALS=/path/to/vertex-sa.json
EMBEDDING_MODEL=gemini-embedding-001
EMBEDDING_DIM=3072
```
3. Seed reference data and embed the RAG corpus:
```bash
python -m data.seed_mongo            # assets, asset_specs, crew, maintenance
python -m data.embed_manuals         # chunk + embed manuals -> manual_chunks (3072-dim)
```
4. Create the Atlas **Vector Search index** `manuals_vector_index` on
   `manual_chunks.embedding` with `numDimensions: 3072` (see `infra/mongodb_atlas_setup.md`).
✅ doctor shows non-zero Mongo collection counts (incl. `manual_chunks`).

## Phase 6 - MongoDB Source Connector (Mongo -> Kafka)
Create the source connector from `infra/connectors/mongodb_source.json`, and
**duplicate it for the `asset_specs` collection** (same config, different
`collection` + it will produce `gridsentinel.mongo.asset_specs`).
✅ Connector(s) Running; verify:
```bash
python -m scripts.tap gridsentinel.mongo.gridsentinel.asset_specs --count 2
python -m scripts.tap gridsentinel.mongo.gridsentinel.assets --count 2
```

## Phase 7 - Vertex AI service account + Flink connections
Enable Vertex AI on your GCP project (where credits apply):
```bash
gcloud services enable aiplatform.googleapis.com
gcloud iam service-accounts create gridsentinel-vertex --display-name="GridSentinel Vertex"
gcloud projects add-iam-policy-binding <YOUR_GCP_PROJECT> \
  --member="serviceAccount:gridsentinel-vertex@<YOUR_GCP_PROJECT>.iam.gserviceaccount.com" \
  --role="roles/aiplatform.user"
gcloud iam service-accounts keys create vertex-sa.json \
  --iam-account=gridsentinel-vertex@<YOUR_GCP_PROJECT>.iam.gserviceaccount.com
```

The **same service-account JSON** powers diagnosis (`017`/`018`) and query embeddings
(`019`/`020`). Replace `<YOUR_GCP_PROJECT>` and paste the JSON into
`flink/steps/017_connection_gemini.sql` and `019_connection_gemini_embed.sql`,
or pre-create via CLI:
```bash
confluent flink connection create gemini_connection \
  --cloud gcp --region asia-south1 --type vertexai \
  --endpoint 'https://asia-south1-aiplatform.googleapis.com/v1/projects/<PROJECT>/locations/asia-south1/publishers/google/models/gemini-2.5-flash' \
  --service-key "$(cat vertex-sa.json)"

confluent flink connection create gemini_embed_connection \
  --cloud gcp --region asia-south1 --type vertexai \
  --endpoint 'https://asia-south1-aiplatform.googleapis.com/v1/projects/<PROJECT>/locations/asia-south1/publishers/google/models/gemini-embedding-001:predict' \
  --service-key "$(cat vertex-sa.json)"
```
Also fill the **`mongodb_connection`** in `021_connection_mongodb.sql` (Atlas endpoint +
user/pass) so Flink's `VECTOR_SEARCH_AGG` can query `manual_chunks`.

## Phase 8 - Flink pipeline
Create a **compute pool**, open a SQL workspace, and run **`flink/steps/001` through
`035` in numeric order** — **one file per Run** (Confluent rejects multiple
statements in one submit). See `flink/steps/README.md`.
Validate stage-by-stage (trigger a fault in Phase 11 if assets are healthy):
```sql
SELECT * FROM `gridsentinel.telemetry.enriched` LIMIT 10;   -- has fault_score/severity
SELECT * FROM `gridsentinel.incidents.diagnosed` LIMIT 5;   -- has dx_* from Gemini
```
```bash
python -m scripts.tap incidents --count 2
python -m scripts.tap diagnosed --count 1 --timeout 90
python -m scripts.tap control   --count 1 --timeout 90
python -m scripts.tap work_orders --count 1 --timeout 90
```

## Phase 9 - MongoDB Sink Connector (work orders -> Mongo)
Create the sink from `infra/connectors/mongodb_sink_workorders.json`.

> **Format:** Flink writes `gridsentinel.work.orders` with `value.format = avro-registry`
> and a Flink-encoded binary key. The sink must use `input.data.format = AVRO` and
> `input.key.format = BYTES` (not JSON). If you see `JsonConverter` /
> `serialization error` on the sink task, update those two properties and restart
> the connector.

✅ A new doc appears in the Mongo `work_orders` collection after a fault.

## Phase 10 - Dashboard (laptop)
```bash
streamlit run dashboard/app.py
```
The dashboard reads the Avro decision topics directly (the codec auto-detects
Avro vs JSON); all reasoning happens in Flink, nothing runs locally.

## Phase 11 - End-to-end demo
```bash
python -m simulators.scenario heatwave --region South
python -m scripts.tap resolved --count 1 --timeout 120   # loop closed
python -m simulators.scenario clear
```

---

### What runs on the laptop
Only `simulators.run_simulators` (the SCADA edge) and `dashboard/app.py`.
Triage, diagnosis, planning, action, and verification are all Flink SQL jobs -
there is no Python agent process to run.
