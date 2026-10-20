"""Runtime configuration, all from environment variables.

Defaults target a developer running the processor on the host against a
local compose stack; docker-compose.yml overrides the endpoints with
container-internal service names.
"""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    transport: str = "kafka"  # kafka | sqs

    kafka_bootstrap: str = "localhost:9092"
    kafka_topic: str = "events.inbound"
    kafka_dlq_topic: str = "events.dlq"
    kafka_group_id: str = "event-processor"

    sqs_endpoint: str = "http://localhost:4566"
    sqs_queue: str = "events-inbound"
    sqs_region: str = "us-east-1"

    database_url: str = "postgresql://events:events@localhost:5432/events"

    schema_dir: str = "schemas"
    max_attempts: int = 3

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            transport=os.getenv("TRANSPORT", "kafka").lower(),
            kafka_bootstrap=os.getenv("KAFKA_BOOTSTRAP", "localhost:9092"),
            kafka_topic=os.getenv("KAFKA_TOPIC", "events.inbound"),
            kafka_dlq_topic=os.getenv("KAFKA_DLQ_TOPIC", "events.dlq"),
            kafka_group_id=os.getenv("KAFKA_GROUP_ID", "event-processor"),
            sqs_endpoint=os.getenv("SQS_ENDPOINT", "http://localhost:4566"),
            sqs_queue=os.getenv("SQS_QUEUE", "events-inbound"),
            sqs_region=os.getenv("AWS_REGION", "us-east-1"),
            database_url=os.getenv(
                "DATABASE_URL", "postgresql://events:events@localhost:5432/events"
            ),
            schema_dir=os.getenv("SCHEMA_DIR", "schemas"),
            max_attempts=int(os.getenv("MAX_ATTEMPTS", "3")),
        )
