import json

from event_processor.core.envelope import parse_event, synthetic_id


def test_parse_valid_event():
    body = json.dumps(
        {
            "event_id": "evt-1",
            "event_type": "user.action",
            "tenant_id": "client-alpha",
            "schema_version": 1,
            "occurred_at": "2026-10-20T17:00:00Z",
            "payload": {"user_id": "u-1", "action": "login"},
            "extra_data": {"campaign": "fall"},
        }
    )
    rec = parse_event(body)
    assert rec.parseable
    assert rec.event_id == "evt-1"
    assert rec.tenant_id == "client-alpha"
    assert rec.payload == {"user_id": "u-1", "action": "login"}
    assert rec.extra_data == {"campaign": "fall"}


def test_parse_non_json_gets_synthetic_id_and_error():
    rec = parse_event(b"this is not json{{{")
    assert not rec.parseable
    assert rec.event_id == synthetic_id("this is not json{{{")
    assert rec.parse_errors and "not valid JSON" in rec.parse_errors[0]


def test_synthetic_id_is_stable_for_redelivery():
    assert synthetic_id("garbage") == synthetic_id("garbage")
    assert synthetic_id("garbage") != synthetic_id("other garbage")


def test_parse_non_object_json():
    rec = parse_event("[1, 2, 3]")
    assert not rec.parseable
    assert "not a JSON object" in rec.parse_errors[0]


def test_missing_event_id_falls_back_to_synthetic():
    body = json.dumps({"event_type": "user.action"})
    rec = parse_event(body)
    assert rec.event_id == synthetic_id(body)
