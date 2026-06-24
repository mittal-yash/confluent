"""Thin Kafka helpers used by every component.

Serialization is pluggable:
  * JSON          -> default, zero-dependency, great for the local demo.
  * Avro + Schema Registry -> set USE_AVRO=true (Confluent Cloud governance story).

The same code therefore runs locally and against Confluent Cloud unchanged.
"""
from __future__ import annotations

import json
import os
from typing import Callable, Iterator, Optional

from confluent_kafka import Consumer, Producer

from .config import settings

_SCHEMA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "schemas")

# Source contract topics the LAPTOP produces (Avro, registered in Schema
# Registry when USE_AVRO=true). All decision topics are produced by Flink, which
# registers their schemas itself; the dashboard reads those via the generic
# (writer-schema) Avro deserializer below.
TOPIC_SCHEMA_FILE = {
    settings.topics.telemetry: "telemetry.avsc",
    settings.topics.weather: "weather.avsc",
    settings.topics.crew: "crew_location.avsc",
}


class _Codec:
    """Serialize/deserialize message values. Avro for known topics, else JSON."""

    def __init__(self) -> None:
        self._avro = settings.use_avro
        self._ser: dict[str, Callable] = {}
        self._de: dict[str, Callable] = {}
        self._sr = None
        self._generic_de = None  # schema-less Avro deserializer (reads writer schema)
        if self._avro:
            from confluent_kafka.schema_registry import SchemaRegistryClient

            conf = {"url": settings.schema_registry_url}
            if settings.schema_registry_auth:
                conf["basic.auth.user.info"] = settings.schema_registry_auth
            self._sr = SchemaRegistryClient(conf)

    def _avro_for(self, topic: str):
        from confluent_kafka.schema_registry.avro import (
            AvroDeserializer,
            AvroSerializer,
        )

        if topic not in self._ser:
            schema_path = os.path.join(_SCHEMA_DIR, TOPIC_SCHEMA_FILE[topic])
            with open(schema_path, "r", encoding="utf-8") as fh:
                schema_str = fh.read()
            self._ser[topic] = AvroSerializer(self._sr, schema_str)
            self._de[topic] = AvroDeserializer(self._sr, schema_str)
        return self._ser[topic], self._de[topic]

    def serialize(self, topic: str, value: dict) -> bytes:
        if self._avro and topic in TOPIC_SCHEMA_FILE:
            from confluent_kafka.serialization import (
                MessageField,
                SerializationContext,
            )

            ser, _ = self._avro_for(topic)
            return ser(value, SerializationContext(topic, MessageField.VALUE))
        return json.dumps(value, separators=(",", ":")).encode("utf-8")

    def _generic_avro(self):
        from confluent_kafka.schema_registry.avro import AvroDeserializer

        if self._generic_de is None:
            # schema_str omitted -> deserializes using the writer schema named by
            # the Confluent wire-format schema id, so we can read ANY Avro topic
            # (e.g. the decision topics Flink produces) with no local .avsc.
            self._generic_de = AvroDeserializer(self._sr)
        return self._generic_de

    def deserialize(self, topic: str, data: Optional[bytes]) -> Optional[dict]:
        if data is None:
            return None
        if self._avro:
            from confluent_kafka.serialization import (
                MessageField,
                SerializationContext,
            )

            ctx = SerializationContext(topic, MessageField.VALUE)
            # Confluent Avro wire format starts with a 0x00 magic byte; plain JSON
            # starts with '{' (0x7b). This lets producers mix Avro and JSON topics
            # (e.g. the laptop's JSON 'scenario' vs Flink's Avro 'control').
            if data[:1] == b"\x00":
                if topic in TOPIC_SCHEMA_FILE:
                    _, de = self._avro_for(topic)
                    return de(data, ctx)
                return self._generic_avro()(data, ctx)
        return json.loads(data.decode("utf-8"))


_codec = _Codec()


class KafkaProducer:
    def __init__(self) -> None:
        cfg = settings.kafka_common_config()
        cfg["linger.ms"] = 20
        cfg["enable.idempotence"] = False
        self._p = Producer(cfg)

    def send(self, topic: str, value: dict, key: Optional[str] = None) -> None:
        self._p.produce(
            topic,
            key=key.encode("utf-8") if key else None,
            value=_codec.serialize(topic, value),
        )
        self._p.poll(0)

    def flush(self, timeout: float = 10.0) -> None:
        self._p.flush(timeout)


class KafkaConsumer:
    def __init__(
        self,
        group_id: str,
        topics: list[str],
        auto_offset_reset: str = "latest",
    ) -> None:
        cfg = settings.kafka_common_config()
        cfg.update(
            {
                "group.id": group_id,
                "auto.offset.reset": auto_offset_reset,
                "enable.auto.commit": True,
            }
        )
        self._c = Consumer(cfg)
        self._c.subscribe(topics)

    def poll(self, timeout: float = 1.0) -> Optional[tuple[str, Optional[str], dict]]:
        msg = self._c.poll(timeout)
        if msg is None or msg.error():
            return None
        topic = msg.topic()
        raw_key = msg.key()
        if raw_key is None:
            key = None
        else:
            try:
                key = raw_key.decode("utf-8")
            except UnicodeDecodeError:
                # Flink-produced topics (DISTRIBUTED BY) use a non-UTF-8 binary
                # key; we only need the value, so don't choke on the key.
                key = raw_key.hex()
        value = _codec.deserialize(topic, msg.value())
        return topic, key, value

    def stream(self, timeout: float = 1.0) -> Iterator[tuple[str, Optional[str], dict]]:
        while True:
            rec = self.poll(timeout)
            if rec is not None:
                yield rec

    def close(self) -> None:
        self._c.close()


def ensure_topics(num_partitions: int = 1, replication: int = 3) -> None:
    """Create all GridSentinel topics if the cluster allows admin operations."""
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = AdminClient(settings.kafka_common_config())
    existing = set(admin.list_topics(timeout=10).topics.keys())
    new = [
        NewTopic(t, num_partitions=num_partitions, replication_factor=replication)
        for t in settings.topics.all()
        if t not in existing
    ]
    if not new:
        print("All topics already exist.")
        return
    for topic, fut in admin.create_topics(new).items():
        try:
            fut.result()
            print(f"Created topic {topic}")
        except Exception as exc:  # noqa: BLE001
            print(f"Topic {topic}: {exc}")


if __name__ == "__main__":
    ensure_topics()
