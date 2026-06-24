"""MongoDB access helpers."""
from __future__ import annotations

from functools import lru_cache

from pymongo import MongoClient
from pymongo.database import Database

from .config import settings

# Collection names
COL_ASSETS = "assets"
COL_SPECS = "asset_specs"
COL_CREW = "crew"
COL_MAINT = "maintenance_history"
COL_MANUAL_CHUNKS = "manual_chunks"


@lru_cache(maxsize=1)
def get_client() -> MongoClient:
    return MongoClient(settings.mongodb_uri, serverSelectionTimeoutMS=8000)


def get_db() -> Database:
    return get_client()[settings.mongodb_db]
