import json

import pytest

from conftest import FakeStore, make_msg
from event_processor.processor import Processor, StoreUnavailable
from event_processor.store.repository import Status

VALID = {
    "event_id": "evt-valid",
    "event_type": "transaction.authorized",
    "tenant_id": "client-alpha",
    "schema_version": 1,
    "occurred_at": "2026-10-20T17:00:00Z",
    "payload": {"amount": 10.0, "currency": "USD", "card_last4": "4242",
                "merchant": "Shop"},
}


def build(settings, registry, transport, store) -> Processor:
    return Processor(settings, transport, store, registry)


def test_valid_event_persisted_ready_and_acked(settings, registry, fake_transport):
    store = FakeStore()
    proc = build(settings, registry, fake_transport, store)
    msg = make_msg(fake_transport, json.dumps(VALID).encode())

    proc.process_one(msg)

    assert store.rows["evt-valid"]["status"] is Status.READY
    assert store.rows["evt-valid"]["target"] == "delivery.transactions"
    assert fake_transport.acked == [msg]
    assert proc.stats["ready"] == 1


def test_invalid_event_persisted_with_errors_and_acked(settings, registry, fake_transport):
    store = FakeStore()
    proc = build(settings, registry, fake_transport, store)
    bad = {**VALID, "event_id": "evt-bad",
           "payload": {**VALID["payload"], "card_last4": "abcd"}}
    msg = make_msg(fake_transport, json.dumps(bad).encode())

    proc.process_one(msg)

    row = store.rows["evt-bad"]
    assert row["status"] is Status.INVALID
    assert row["errors"] and any("card_last4" in e for e in row["errors"])
    assert fake_transport.acked == [msg]  # bad data is acked, never redelivered
    assert fake_transport.dlq == []


def test_non_json_body_persisted_invalid(settings, registry, fake_transport):
    store = FakeStore()
    proc = build(settings, registry, fake_transport, store)
    msg = make_msg(fake_transport, b"this is not json{{{")

    proc.process_one(msg)

    assert len(store.rows) == 1
    row = next(iter(store.rows.values()))
    assert row["status"] is Status.INVALID
    assert fake_transport.acked == [msg]


def test_duplicate_delivery_absorbed_and_acked(settings, registry, fake_transport):
    store = FakeStore()
    proc = build(settings, registry, fake_transport, store)
    body = json.dumps(VALID).encode()

    proc.process_one(make_msg(fake_transport, body))
    proc.process_one(make_msg(fake_transport, body))

    assert len(store.rows) == 1
    assert len(fake_transport.acked) == 2
    assert proc.stats["duplicates"] == 1


def test_store_down_stops_without_ack(settings, registry, fake_transport):
    store = FakeStore(fail=True)
    proc = build(settings, registry, fake_transport, store)
    msg = make_msg(fake_transport, json.dumps(VALID).encode())

    with pytest.raises(StoreUnavailable):
        proc.process_one(msg)

    assert fake_transport.acked == []  # un-acked → the transport will redeliver
    assert fake_transport.dlq == []    # valid events are never DLQ'd for infra


def test_poison_message_goes_to_dlq_with_failed_record(
    settings, registry, fake_transport, monkeypatch
):
    store = FakeStore()
    proc = build(settings, registry, fake_transport, store)

    def explode(_record):
        raise RuntimeError("kaboom in validation")

    monkeypatch.setattr(registry, "validate", explode)
    msg = make_msg(fake_transport, json.dumps(VALID).encode())

    proc.process_one(msg)

    assert store.rows["evt-valid"]["status"] is Status.FAILED
    assert len(fake_transport.dlq) == 1
    assert "kaboom" in fake_transport.dlq[0][1]
    assert proc.stats["failed"] == 1
