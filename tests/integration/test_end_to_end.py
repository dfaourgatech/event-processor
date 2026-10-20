"""End-to-end tests against the real compose stack.

Skipped unless INTEGRATION=1. Bring the stack up first:

    docker compose up -d kafka kafka-init postgres localstack
    INTEGRATION=1 TRANSPORT=kafka python -m pytest tests/integration -q
    INTEGRATION=1 TRANSPORT=sqs   python -m pytest tests/integration -q

Each run uses a unique Kafka consumer group / fresh table so runs are
independent.
"""

import os
import sys
import time
import uuid
from pathlib import Path

import pytest

pytestmark = pytest.mark.skipif(
    os.getenv("INTEGRATION") != "1", reason="set INTEGRATION=1 with the stack up"
)

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

from producer.simulator import build_events  # noqa: E402

from event_processor.config import Settings  # noqa: E402
from event_processor.core.validator import SchemaRegistry  # noqa: E402
from event_processor.processor import Processor  # noqa: E402
from event_processor.store.repository import EventStore, Status  # noqa: E402
from event_processor.transports.factory import build_transport  # noqa: E402

VALID_COUNT = 20


@pytest.fixture()
def stack():
    settings = Settings.from_env()
    settings = Settings(
        **{**settings.__dict__, "kafka_group_id": f"test-{uuid.uuid4().hex[:8]}"}
    )
    store = EventStore(settings.database_url)
    store.ensure_schema()
    store._conn.execute("DELETE FROM events")
    if settings.transport == "sqs":
        _purge_sqs(settings)
    yield settings, store
    store.close()


def _purge_sqs(settings: Settings) -> None:
    import boto3

    client = boto3.client(
        "sqs",
        endpoint_url=settings.sqs_endpoint,
        region_name=settings.sqs_region,
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )
    for name in (settings.sqs_queue, "events-dlq"):
        url = client.get_queue_url(QueueName=name)["QueueUrl"]
        client.purge_queue(QueueUrl=url)
    time.sleep(2)  # purge is eventually consistent


def test_end_to_end(stack):
    settings, store = stack
    events = build_events(VALID_COUNT)

    producer = build_transport(settings)
    for body, key, _outcome in events:
        producer.send(body, key=key)
    if hasattr(producer, "flush"):
        producer.flush()
    producer.close()

    consumer = build_transport(settings)
    registry = SchemaRegistry(str(REPO_ROOT / "schemas"))
    proc = Processor(settings, consumer, store, registry)

    deadline = time.time() + 90
    while time.time() < deadline:
        msg = consumer.receive(timeout_s=2.0)
        if msg is None:
            if proc.stats["received"] >= len(events):
                break
            continue
        proc.process_one(msg)
        if proc.stats["received"] >= len(events):
            break
    consumer.close()

    assert proc.stats["received"] == len(events)
    assert store.count(Status.READY) == VALID_COUNT
    # bad card_last4, missing field, unknown type, non-JSON body
    assert store.count(Status.INVALID) == 4
    assert proc.stats["duplicates"] == 2
    assert store.count() == VALID_COUNT + 4
