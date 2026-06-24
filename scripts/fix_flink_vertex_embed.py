"""Fix Flink gemini_embed for Vertex AI and smoke-test AI_EMBEDDING."""
from __future__ import annotations

import json
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SA_PATH = ROOT / "vertex-sa.json"

POOL = "lfcp-12296x6"
ENV = "env-j72pkm"
DB = "lkc-mvvkgn1"
CLOUD = "gcp"
REGION = "asia-south1"
PROJECT = "decent-micron-465108-n6"
ENDPOINT = (
    f"https://{REGION}-aiplatform.googleapis.com/v1/projects/{PROJECT}"
    f"/locations/{REGION}/publishers/google/models/gemini-embedding-001:predict"
)
# Vertex embedding predict URL must include :predict. Without it Flink/Vertex
# returns HTTP 400 from ModelRuntime.


def run_sql(sql: str, *, wait: bool = True) -> tuple[int, str]:
    cmd = [
        "confluent",
        "flink",
        "statement",
        "create",
        "--environment",
        ENV,
        "--cloud",
        CLOUD,
        "--region",
        REGION,
        "--compute-pool",
        POOL,
        "--database",
        DB,
        "--sql",
        sql,
    ]
    if wait:
        cmd.append("--wait")
    print("\n=== SQL ===")
    print(sql[:500] + ("..." if len(sql) > 500 else ""))
    proc = subprocess.run(cmd, capture_output=True, text=True)
    out = (proc.stdout or "") + (proc.stderr or "")
    print(out[-3000:])
    return proc.returncode, out


def main() -> int:
    if not SA_PATH.is_file():
        print(f"Missing {SA_PATH}", file=sys.stderr)
        return 1

    sa_json = json.dumps(json.load(SA_PATH.open(encoding="utf-8")))
    # SQL string literal: JSON uses double quotes; escape any single quotes if present.
    sa_sql = sa_json.replace("'", "''")

    steps = [
        "DROP MODEL IF EXISTS `confluent-ai-day`.`cluster_0`.gemini_embed;",
        "DROP CONNECTION IF EXISTS `confluent-ai-day`.`cluster_0`.gemini_embed_connection;",
        f"""CREATE CONNECTION `confluent-ai-day`.`cluster_0`.gemini_embed_connection
WITH (
  'type' = 'vertexai',
  'endpoint' = '{ENDPOINT}',
  'service-key' = '{sa_sql}'
);""",
        """CREATE MODEL `confluent-ai-day`.`cluster_0`.gemini_embed
INPUT  (input STRING)
OUTPUT (embedding ARRAY<FLOAT>)
WITH (
  'provider'              = 'vertexai',
  'vertexai.connection'   = 'gemini_embed_connection',
  'vertexai.input_format' = 'VERTEX-EMBED',
  'task'                  = 'embedding'
);""",
        """SELECT CARDINALITY(embedding) AS dim
FROM (VALUES ('transformer oil_temp anomaly (severity critical)')) AS t(txt),
  LATERAL TABLE(AI_EMBEDDING('gemini_embed', txt, MAP['debug', 'true']));""",
    ]

    for sql in steps:
        rc, out = run_sql(sql)
        if rc != 0 or "FAILED" in out:
            print("Step failed.", file=sys.stderr)
            return 1
        time.sleep(2)

    print("\nAll steps completed successfully.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
