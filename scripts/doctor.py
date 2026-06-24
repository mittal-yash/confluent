"""Connectivity health-check for every backing service.

Run this after each setup step to fail fast instead of discovering a broken
endpoint at demo time.

    python -m scripts.doctor

Checks (each independent; a failure in one does not stop the others):
  * Kafka       - connect + list topics, report which gridsentinel.* exist
  * Schema Reg. - reachable + how many gridsentinel subjects are registered
  * MongoDB     - connect + per-collection document counts
  * Vertex AI   - optional probe embed when VERTEX_PROJECT_ID is set

Exit code 0 only if all enabled checks pass.
"""
from __future__ import annotations

import sys

from common.config import settings

OK = "PASS"
BAD = "FAIL"


def _check_kafka() -> bool:
    print("\n=== Kafka ===")
    try:
        from confluent_kafka.admin import AdminClient

        admin = AdminClient(settings.kafka_common_config())
        md = admin.list_topics(timeout=10)
        existing = set(md.topics.keys())
        expected = settings.topics.all()
        present = [t for t in expected if t in existing]
        missing = [t for t in expected if t not in existing]
        print(f"  connected to {settings.bootstrap_servers}")
        print(f"  gridsentinel topics present: {len(present)}/{len(expected)}")
        if missing:
            print(f"  missing: {', '.join(missing)}")
            print("  -> run: python -m common.kafka_io")
        print(f"  {OK if not missing else 'WARN'}")
        return True
    except Exception as exc:  # noqa: BLE001
        print(f"  {BAD}: {exc}")
        return False


def _check_schema_registry() -> bool:
    print("\n=== Schema Registry ===")
    if not settings.use_avro:
        print("  skipped (USE_AVRO=false; JSON mode needs no Schema Registry)")
        return True
    try:
        from confluent_kafka.schema_registry import SchemaRegistryClient

        conf = {"url": settings.schema_registry_url}
        if settings.schema_registry_auth:
            conf["basic.auth.user.info"] = settings.schema_registry_auth
        client = SchemaRegistryClient(conf)
        subjects = client.get_subjects()
        gs = [s for s in subjects if s.startswith("gridsentinel")]
        print(f"  connected to {settings.schema_registry_url}")
        print(f"  gridsentinel subjects registered: {len(gs)}")
        if not gs:
            print("  -> run: python -m schemas.register_schemas")
        print(f"  {OK}")
        return True
    except Exception as exc:  # noqa: BLE001
        print(f"  {BAD}: {exc}")
        return False


def _check_mongo() -> bool:
    print("\n=== MongoDB ===")
    try:
        from common.mongo import (
            COL_ASSETS,
            COL_CREW,
            COL_MAINT,
            COL_MANUAL_CHUNKS,
            COL_SPECS,
            get_db,
        )

        db = get_db()
        db.command("ping")
        print(f"  connected to db '{settings.mongodb_db}'")
        for col in (COL_ASSETS, COL_SPECS, COL_CREW, COL_MAINT, COL_MANUAL_CHUNKS):
            n = db[col].count_documents({})
            flag = "" if n > 0 else "  (empty -> run seed/embed)"
            print(f"  {col:<20} {n}{flag}")
        print(f"  {OK}")
        return True
    except Exception as exc:  # noqa: BLE001
        print(f"  {BAD}: {exc}")
        print("  -> check MONGODB_URI in .env (Atlas IP allowlist / user/pass)")
        return False


def _check_vertex() -> bool:
    print("\n=== Vertex AI (embeddings) ===")
    if not settings.vertex_project_id:
        print("  skipped (VERTEX_PROJECT_ID not set; embed_manuals uses offline hash)")
        return True
    try:
        from common.embeddings import embed_one, embedding_available

        if not embedding_available():
            print(f"  {BAD}: VERTEX_PROJECT_ID set but embedding_available() is false")
            return False
        vec = embed_one("connectivity probe")
        print(f"  project={settings.vertex_project_id} location={settings.vertex_location}")
        print(f"  model={settings.embedding_model} dim={len(vec)}")
        print(f"  {OK}")
        return True
    except Exception as exc:  # noqa: BLE001
        print(f"  {BAD}: {exc}")
        print("  -> set GOOGLE_APPLICATION_CREDENTIALS or run gcloud auth application-default login")
        return False


def main() -> None:
    print(f"GridSentinel doctor (mode={settings.mode})")
    results = [
        _check_kafka(),
        _check_schema_registry(),
        _check_mongo(),
        _check_vertex(),
    ]
    print("\n" + ("All checks passed." if all(results) else "Some checks FAILED."))
    sys.exit(0 if all(results) else 1)


if __name__ == "__main__":
    main()
