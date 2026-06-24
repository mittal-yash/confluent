"""Text embeddings for RAG.

Uses Vertex AI (google-genai) when VERTEX_PROJECT_ID is set, otherwise a
deterministic offline hashing embedder so the demo runs with no GCP creds.
"""
from __future__ import annotations

import hashlib
import math
import re

from .config import settings

_TOKEN = re.compile(r"[a-z0-9]+")


def embedding_available() -> bool:
    return bool(settings.vertex_project_id)


def _hash_embed(text: str, dim: int) -> list[float]:
    vec = [0.0] * dim
    tokens = _TOKEN.findall(text.lower())
    for tok in tokens:
        h = int(hashlib.md5(tok.encode("utf-8")).hexdigest(), 16)
        idx = h % dim
        sign = 1.0 if (h >> 8) % 2 == 0 else -1.0
        vec[idx] += sign
    norm = math.sqrt(sum(v * v for v in vec)) or 1.0
    return [v / norm for v in vec]


def _vertex_embed(texts: list[str]) -> list[list[float]]:
    from google import genai
    from google.genai import types

    client = genai.Client(
        vertexai=True,
        project=settings.vertex_project_id,
        location=settings.vertex_location,
    )
    config = types.EmbedContentConfig(output_dimensionality=settings.embedding_dim)
    resp = client.models.embed_content(
        model=settings.embedding_model,
        contents=texts,
        config=config,
    )
    vectors = [list(e.values) for e in resp.embeddings]
    if vectors and len(vectors[0]) != settings.embedding_dim:
        raise RuntimeError(
            f"Vertex returned dim={len(vectors[0])}, expected {settings.embedding_dim}. "
            "Check EMBEDDING_MODEL / EMBEDDING_DIM in .env."
        )
    return vectors


def embed(texts: list[str]) -> list[list[float]]:
    if not texts:
        return []
    if embedding_available():
        try:
            return _vertex_embed(texts)
        except Exception as exc:  # noqa: BLE001
            if settings.mode == "cloud":
                raise RuntimeError(
                    f"Vertex embedding failed in cloud mode ({exc}). "
                    "Check VERTEX_PROJECT_ID, VERTEX_LOCATION, and "
                    "GOOGLE_APPLICATION_CREDENTIALS (or gcloud auth application-default login)."
                ) from exc
            print(f"[embeddings] Vertex API failed ({exc}); falling back to offline embedder")
    dim = settings.embedding_dim
    return [_hash_embed(t, dim) for t in texts]


def embed_one(text: str) -> list[float]:
    return embed([text])[0]


def cosine(a: list[float], b: list[float]) -> float:
    if not a or not b or len(a) != len(b):
        return 0.0
    dot = sum(x * y for x, y in zip(a, b))
    na = math.sqrt(sum(x * x for x in a)) or 1.0
    nb = math.sqrt(sum(y * y for y in b)) or 1.0
    return dot / (na * nb)
