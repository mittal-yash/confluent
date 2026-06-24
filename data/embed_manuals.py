"""Chunk + embed the O&M manuals and per-asset spec sheets into MongoDB.

    python -m data.embed_manuals

Writes the `manual_chunks` collection (text + embedding + asset filters). This
is the RAG corpus the diagnosis agent retrieves from. On Atlas, create the
vector index named by VECTOR_INDEX_NAME (see infra/mongodb_atlas_setup.md);
locally the vector store does an in-Python cosine scan over this collection.
"""
from __future__ import annotations

import glob
import os

from common.assets import build_fleet
from common.config import settings
from common.embeddings import embed, embedding_available
from common.mongo import COL_MANUAL_CHUNKS, get_db
from .specs import build_spec_doc

_MANUAL_DIR = os.path.join(os.path.dirname(__file__), "manuals")

# map manual file -> asset_type it applies to ("all" = generic)
_MANUAL_SCOPE = {
    "transformer_oil_thermal_guide.md": "transformer",
    "transmission_line_dynamic_rating.md": "transmission_line",
    "pump_vibration_maintenance.md": "water_pump",
    "grid_emergency_response_sop.md": "all",
}

_MAX_CHARS = 750


def _chunk(text: str) -> list[str]:
    blocks = [b.strip() for b in text.split("\n\n") if b.strip()]
    chunks: list[str] = []
    buf = ""
    for b in blocks:
        if len(buf) + len(b) + 2 <= _MAX_CHARS:
            buf = f"{buf}\n\n{b}" if buf else b
        else:
            if buf:
                chunks.append(buf)
            buf = b
    if buf:
        chunks.append(buf)
    return chunks


def main() -> None:
    db = get_db()
    col = db[COL_MANUAL_CHUNKS]
    col.drop()

    docs: list[dict] = []

    # 1) generic manuals
    for path in sorted(glob.glob(os.path.join(_MANUAL_DIR, "*.md"))):
        fname = os.path.basename(path)
        scope = _MANUAL_SCOPE.get(fname, "all")
        with open(path, "r", encoding="utf-8") as fh:
            text = fh.read()
        for i, chunk in enumerate(_chunk(text)):
            docs.append(
                {
                    "text": chunk,
                    "source": fname,
                    "chunk": i,
                    "asset_id": "",
                    "asset_type": scope,
                }
            )

    # 2) per-asset spec sheets (asset-specific grounding)
    for a in build_fleet(settings.random_seed):
        spec = build_spec_doc(a)
        docs.append(
            {
                "text": spec["spec_text"],
                "source": f"spec:{a.spec_sheet_id}",
                "chunk": 0,
                "asset_id": a.asset_id,
                "asset_type": a.asset_type,
            }
        )

    # 3) embed (batched) and store
    vectors = embed([d["text"] for d in docs])
    for d, v in zip(docs, vectors):
        d["embedding"] = v
    col.insert_many(docs)
    col.create_index("asset_type")
    col.create_index("asset_id")

    mode = "API embeddings" if embedding_available() else "offline hash embedder"
    print(f"Embedded {len(docs)} chunks into '{COL_MANUAL_CHUNKS}' "
          f"(dim={len(vectors[0])}, {mode}).")
    if settings.atlas_vector_search:
        print(f"Atlas mode: ensure vector index '{settings.vector_index_name}' "
              f"exists (see infra/mongodb_atlas_setup.md).")


if __name__ == "__main__":
    main()
