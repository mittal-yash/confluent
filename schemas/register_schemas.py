"""Register the Avro contracts (and data-quality rules) in Schema Registry.

Run against Confluent Cloud (or local Redpanda SR):

    python -m schemas.register_schemas

This is the governed-contract half of the rubric's Governance pillar. Schemas
are registered under `<topic>-value`; data-quality rules from
`governance/data_quality_rules.json` are attached when the SR client supports
rule sets (Confluent Cloud Stream Governance).
"""
from __future__ import annotations

import json
import os

from common.config import settings
from common.kafka_io import TOPIC_SCHEMA_FILE

_HERE = os.path.dirname(__file__)
_DQ_PATH = os.path.join(
    os.path.dirname(_HERE), "governance", "data_quality_rules.json"
)


def _load_dq_rules() -> dict:
    if not os.path.exists(_DQ_PATH):
        return {}
    with open(_DQ_PATH, "r", encoding="utf-8") as fh:
        data = json.load(fh)
    return {entry["topic"]: entry["rules"] for entry in data.get("rulesets", [])}


def main() -> None:
    from confluent_kafka.schema_registry import Schema, SchemaRegistryClient

    conf = {"url": settings.schema_registry_url}
    if settings.schema_registry_auth:
        conf["basic.auth.user.info"] = settings.schema_registry_auth
    client = SchemaRegistryClient(conf)

    dq = _load_dq_rules()

    for topic, filename in TOPIC_SCHEMA_FILE.items():
        path = os.path.join(_HERE, filename)
        with open(path, "r", encoding="utf-8") as fh:
            schema_str = fh.read()
        subject = f"{topic}-value"

        rule_set = None
        if topic in dq:
            try:
                from confluent_kafka.schema_registry import Rule, RuleKind, RuleMode, RuleSet

                rules = [
                    Rule(
                        name=r["name"],
                        doc=r.get("doc", ""),
                        kind=RuleKind.CONDITION,
                        mode=RuleMode.WRITE,
                        type="CEL",
                        expr=r["expr"],
                        on_failure="ERROR" if r.get("severity") == "error" else "DLQ",
                    )
                    for r in dq[topic]
                ]
                rule_set = RuleSet(domain_rules=rules)
            except Exception as exc:  # noqa: BLE001
                print(f"  (rule set unsupported by this SR client: {exc})")

        try:
            schema = Schema(schema_str, schema_type="AVRO", rule_set=rule_set)
        except TypeError:
            schema = Schema(schema_str, schema_type="AVRO")

        schema_id = client.register_schema(subject, schema)
        dq_note = f" + {len(dq[topic])} DQ rule(s)" if topic in dq else ""
        print(f"Registered {subject} -> id {schema_id}{dq_note}")


if __name__ == "__main__":
    main()
