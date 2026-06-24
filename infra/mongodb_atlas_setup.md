# MongoDB Atlas setup

Atlas is both the reference store (assets/specs, streamed to Kafka by the Source
Connector) and the RAG vector store (`manual_chunks`, queried directly by Flink).

## 1. Cluster + user

- Create a free **M0** cluster (region close to your Kafka cluster, e.g.
  `ap-south-1`).
- Add a database user and allow-list your IP (and Confluent Cloud egress IPs
  for the connector).

Set in `.env` (Vertex AI embeddings — must match Flink `gemini_embed`):

```
MONGODB_URI=mongodb+srv://<user>:<pass>@<cluster>.mongodb.net/?retryWrites=true&w=majority
MONGODB_DB=gridsentinel
ATLAS_VECTOR_SEARCH=true
VECTOR_INDEX_NAME=manuals_vector_index
VERTEX_PROJECT_ID=<your-gcp-project>
VERTEX_LOCATION=asia-south1
GOOGLE_APPLICATION_CREDENTIALS=/path/to/vertex-sa.json
EMBEDDING_MODEL=gemini-embedding-001
EMBEDDING_DIM=3072
```

## 2. Seed reference data + RAG corpus

```bash
python -m data.seed_mongo      # assets, specs, crew, maintenance history
python -m data.embed_manuals   # chunk + embed O&M manuals -> manual_chunks (3072-dim)
```

## 3. Vector Search index

Index name: `manuals_vector_index` on `gridsentinel.manual_chunks`, field
`embedding`, **3072** dimensions, **cosine** similarity.

### Easiest: create from your laptop (no Atlas shell)

After `embed_manuals` has run, from `gridsentinel/`:

```bash
python -m scripts.create_vector_index
```

Wait for `PASS: vector index is Active.` This works on M0 and avoids the Atlas
UI JSON editor entirely.

### Atlas UI (only if the script fails)

**Important:** pick **MongoDB Vector Search** as the Search Type — not “Atlas
Search”. Regular Search indexes require a `mappings` document and reject the
`fields` JSON with *“Please define the mappings document”*.

**Recommended in UI:** use the **Visual Editor** (not JSON):

1. Cluster **confluent** → **Atlas Search** → **Create Search Index**
2. **Search Type:** MongoDB Vector Search
3. Database `gridsentinel`, collection `manual_chunks`, name `manuals_vector_index`
4. **Visual Editor** → select field **`embedding`**, dimensions **3072**, similarity **cosine**
5. **Create Vector Search Index** → wait until **Active**

JSON editor (Vector Search type only):

```json
{
  "fields": [
    { "type": "vector", "path": "embedding", "numDimensions": 3072, "similarity": "cosine" }
  ]
}
```

### In-browser Atlas Shell (no local install)

Atlas left sidebar → **Data Explorer** → **>_** (Shell) at the bottom, then:

```javascript
use gridsentinel
db.manual_chunks.createSearchIndex(
  "manuals_vector_index",
  "vectorSearch",
  {
    fields: [
      { type: "vector", path: "embedding", numDimensions: 3072, similarity: "cosine" }
    ]
  }
)
```

`numDimensions` MUST match the embedding model: **3072** for Gemini
`gemini-embedding-001` (used by both `embed_manuals` and Flink's `gemini_embed`
model). Flink's `CREATE MODEL` cannot request a reduced output dimension, so it
always emits the native 3072-d vector — the corpus and this index must use 3072
to match. If you ever switch embedding models, rebuild both the corpus and this
index so the dimensions stay in sync.

## 4. How Flink uses this (true vector RAG)

`flink/05_diagnosis.sql` creates a read-only `mongodb` external table over
`manual_chunks` + this index and calls `VECTOR_SEARCH_AGG` to retrieve the
passage that best matches each incident, then grounds Gemini on it. So Atlas is
both the reference store (via the Source Connector for `assets`/`asset_specs`)
**and** the RAG vector store queried directly from Flink.

