"""Kafka transport.

- Manual offset commits, only on ack — at-least-once by construction.
- Producers key messages by tenant_id, so each tenant's events are ordered
  within a partition and partitions are the scaling axis.
- DLQ is a topic (events.dlq): poison messages are produced there with the
  failure reason in a header, then the source offset is committed so one
  bad message cannot block its partition.
- Replay (the reason Kafka is the production recommendation — DESIGN.md
  Decision 1) is just a consumer-group offset reset; nothing in this layer
  needs to change for it.
"""

from __future__ import annotations

from confluent_kafka import Consumer, KafkaError, Producer

from ..config import Settings
from .base import IncomingMessage, Transport


class KafkaTransport(Transport):
    def __init__(self, settings: Settings):
        self._settings = settings
        self._consumer = Consumer(
            {
                "bootstrap.servers": settings.kafka_bootstrap,
                "group.id": settings.kafka_group_id,
                "enable.auto.commit": False,
                "auto.offset.reset": "earliest",
            }
        )
        self._consumer.subscribe([settings.kafka_topic])
        self._producer = Producer({"bootstrap.servers": settings.kafka_bootstrap})

    def receive(self, timeout_s: float = 1.0) -> IncomingMessage | None:
        msg = self._consumer.poll(timeout_s)
        if msg is None:
            return None
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                return None
            raise RuntimeError(f"kafka consumer error: {msg.error()}")
        return IncomingMessage(body=msg.value(), transport=self, native=msg)

    def ack(self, msg: IncomingMessage) -> None:
        # Synchronous per-message commit: simple and obviously correct.
        # (Batching commits is the throughput upgrade if this ever matters.)
        self._consumer.commit(message=msg.native, asynchronous=False)

    def send_to_dlq(self, msg: IncomingMessage, reason: str) -> None:
        self._producer.produce(
            self._settings.kafka_dlq_topic,
            value=msg.body,
            headers={
                "dlq_reason": reason,
                "source_topic": self._settings.kafka_topic,
            },
        )
        self._producer.flush(10)
        self.ack(msg)

    def send(self, body: bytes, key: str | None = None) -> None:
        self._producer.produce(
            self._settings.kafka_topic,
            value=body,
            key=key.encode("utf-8") if key else None,
        )
        self._producer.poll(0)

    def flush(self) -> None:
        self._producer.flush(10)

    def close(self) -> None:
        self._producer.flush(5)
        self._consumer.close()
