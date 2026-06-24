"""Central configuration loaded from environment / .env.

Cloud-only: point this at Confluent Cloud + MongoDB Atlas via .env (see
.env.example). The laptop runs just the simulator and the dashboard.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from functools import lru_cache

try:
    from dotenv import load_dotenv

    load_dotenv()
except Exception:  # python-dotenv optional at runtime
    pass


def _bool(name: str, default: bool = False) -> bool:
    return os.getenv(name, str(default)).strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class Topics:
    telemetry: str = "gridsentinel.telemetry"
    weather: str = "gridsentinel.weather"
    crew: str = "gridsentinel.crew.location"
    scenario: str = "gridsentinel.scenario"

    enriched: str = "gridsentinel.telemetry.enriched"
    incidents: str = "gridsentinel.incidents"
    triaged: str = "gridsentinel.incidents.triaged"
    diagnosed: str = "gridsentinel.incidents.diagnosed"
    planned: str = "gridsentinel.actions.planned"
    executed: str = "gridsentinel.actions.executed"
    control: str = "gridsentinel.control"
    work_orders: str = "gridsentinel.work.orders"
    resolved: str = "gridsentinel.incidents.resolved"

    def all(self) -> list[str]:
        return [getattr(self, f) for f in self.__dataclass_fields__]


@dataclass(frozen=True)
class Settings:
    mode: str = os.getenv("GRIDSENTINEL_MODE", "cloud")

    # Kafka
    bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")
    security_protocol: str = os.getenv("KAFKA_SECURITY_PROTOCOL", "")
    sasl_mechanism: str = os.getenv("KAFKA_SASL_MECHANISM", "")
    sasl_username: str = os.getenv("KAFKA_SASL_USERNAME", "")
    sasl_password: str = os.getenv("KAFKA_SASL_PASSWORD", "")

    # Schema Registry
    schema_registry_url: str = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:18081")
    schema_registry_auth: str = os.getenv("SCHEMA_REGISTRY_AUTH", "")
    use_avro: bool = _bool("USE_AVRO", False)

    # MongoDB
    mongodb_uri: str = os.getenv("MONGODB_URI", "")
    mongodb_db: str = os.getenv("MONGODB_DB", "gridsentinel")
    atlas_vector_search: bool = _bool("ATLAS_VECTOR_SEARCH", True)
    vector_index_name: str = os.getenv("VECTOR_INDEX_NAME", "manuals_vector_index")

    # Vertex AI (RAG corpus) — must match Flink gemini_embed (gemini-embedding-001, 3072-dim)
    vertex_project_id: str = os.getenv("VERTEX_PROJECT_ID", "")
    vertex_location: str = os.getenv("VERTEX_LOCATION", "asia-south1")
    embedding_model: str = os.getenv("EMBEDDING_MODEL", "gemini-embedding-001")
    embedding_dim: int = int(os.getenv("EMBEDDING_DIM", "3072"))

    # Simulation
    tick_seconds: float = float(os.getenv("SIM_TICK_SECONDS", "1.0"))
    random_seed: int = int(os.getenv("SIM_RANDOM_SEED", "42"))

    topics: Topics = field(default_factory=Topics)

    def kafka_common_config(self) -> dict:
        cfg: dict[str, object] = {"bootstrap.servers": self.bootstrap_servers}
        if self.security_protocol:
            cfg["security.protocol"] = self.security_protocol
        if self.sasl_mechanism:
            cfg["sasl.mechanism"] = self.sasl_mechanism
            cfg["sasl.username"] = self.sasl_username
            cfg["sasl.password"] = self.sasl_password
        return cfg


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
