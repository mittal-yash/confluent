"""Peek at a Kafka topic to confirm data is actually flowing.

Uses the same .env, credentials, and (de)serialization as the rest of the
app, so if the tap can read a topic, every other component can too.

    # by topic alias (see common/config.py Topics fields)
    python -m scripts.tap telemetry
    python -m scripts.tap incidents --count 3 --from earliest

    # or by full topic name
    python -m scripts.tap gridsentinel.telemetry

Exit code 0 if at least one message was read before the timeout, else 1.
This makes it usable as a hard validation gate in a script.
"""
from __future__ import annotations

import argparse
import json
import sys
import time
import uuid

from common.config import settings
from common.kafka_io import KafkaConsumer


def _resolve_topic(name: str) -> str:
    """Allow a short alias (e.g. 'telemetry') or a full topic name."""
    topics = settings.topics
    if name in topics.__dataclass_fields__:
        return getattr(topics, name)
    return name


def _aliases() -> str:
    return ", ".join(settings.topics.__dataclass_fields__.keys())


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Tap a Kafka topic and print messages (validation tool)."
    )
    parser.add_argument(
        "topic", help=f"topic name or alias (aliases: {_aliases()})"
    )
    parser.add_argument("--count", type=int, default=5, help="messages to print")
    parser.add_argument(
        "--timeout", type=float, default=20.0, help="seconds to wait overall"
    )
    parser.add_argument(
        "--from",
        dest="offset",
        choices=["earliest", "latest"],
        default="earliest",
        help="earliest = include backlog, latest = only new messages",
    )
    args = parser.parse_args()

    topic = _resolve_topic(args.topic)
    print(f"Tapping '{topic}' on {settings.bootstrap_servers} "
          f"(mode={settings.mode}, avro={settings.use_avro}, from={args.offset})")
    print(f"Waiting up to {args.timeout:.0f}s for {args.count} message(s)...\n")

    consumer = KafkaConsumer(
        group_id=f"tap-{uuid.uuid4().hex[:8]}",
        topics=[topic],
        auto_offset_reset=args.offset,
    )

    seen = 0
    deadline = time.time() + args.timeout
    try:
        while seen < args.count and time.time() < deadline:
            rec = consumer.poll(1.0)
            if rec is None:
                continue
            _, key, value = rec
            seen += 1
            print(f"--- message {seen}  key={key} ---")
            print(json.dumps(value, indent=2, default=str))
            print()
    finally:
        consumer.close()

    if seen == 0:
        print("FAIL: no messages received. Is the producer running and the "
              "topic correct? Check .env endpoints/credentials.")
        sys.exit(1)
    print(f"PASS: received {seen} message(s) from '{topic}'.")


if __name__ == "__main__":
    main()
