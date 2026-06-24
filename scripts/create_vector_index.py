"""Create the Atlas Vector Search index for RAG (manual_chunks.embedding).

    python -m scripts.create_vector_index

On M0 free tier, Atlas may reject programmatic index creation — use the
Visual Editor in the Atlas UI instead (see infra/mongodb_atlas_setup.md).
"""
from __future__ import annotations

import sys
import time

from pymongo.operations import SearchIndexModel

from common.config import settings
from common.mongo import COL_MANUAL_CHUNKS, get_db

INDEX_NAME = settings.vector_index_name
DEFINITION = {
    "fields": [
        {
            "type": "vector",
            "path": "embedding",
            "numDimensions": settings.embedding_dim,
            "similarity": "cosine",
        },
        {"type": "filter", "path": "asset_id"},
        {"type": "filter", "path": "asset_type"},
    ]
}


def main() -> None:
    db = get_db()
    col = db[COL_MANUAL_CHUNKS]
    n = col.count_documents({})
    if n == 0:
        print(f"FAIL: '{COL_MANUAL_CHUNKS}' is empty. Run: python -m data.embed_manuals")
        sys.exit(1)

    sample = col.find_one({}, {"embedding": 1})
    dim = len(sample.get("embedding") or [])
    if dim != settings.embedding_dim:
        print(f"WARN: sample embedding dim={dim}, expected {settings.embedding_dim}")

    existing = list(col.list_search_indexes())
    for idx in existing:
        if idx.get("name") == INDEX_NAME:
            status = idx.get("status", "?")
            print(f"Index '{INDEX_NAME}' already exists (status={status}).")
            sys.exit(0 if status == "READY" else 1)

    print(f"Creating vector search index '{INDEX_NAME}' on {settings.mongodb_db}.{COL_MANUAL_CHUNKS} ...")
    try:
        col.create_search_index(
            SearchIndexModel(
                definition=DEFINITION,
                name=INDEX_NAME,
                type="vectorSearch",
            )
        )
    except Exception as exc:  # noqa: BLE001
        print(f"FAIL: {exc}")
        print(
            "\nOn Atlas M0, index creation via Python often fails.\n"
            "Use the Atlas UI Visual Editor instead:\n"
            "  Cluster -> Atlas Search -> Create Search Index\n"
            "  Search Type: MongoDB Vector Search (NOT Atlas Search)\n"
            "  DB: gridsentinel  Collection: manual_chunks\n"
            "  Visual Editor -> field 'embedding', 3072 dims, cosine\n"
            "  Or use Atlas in-browser Shell (see mongodb_atlas_setup.md)."
        )
        sys.exit(1)

    for _ in range(30):
        for idx in col.list_search_indexes():
            if idx.get("name") == INDEX_NAME:
                status = idx.get("status", "?")
                print(f"  status={status}")
                if status == "READY":
                    print("PASS: vector index is Active.")
                    sys.exit(0)
                if status == "FAILED":
                    print("FAIL: index build failed in Atlas.")
                    sys.exit(1)
        time.sleep(4)

    print("WARN: index still building — check Atlas Search tab; wait until Active.")
    sys.exit(1)


if __name__ == "__main__":
    main()
