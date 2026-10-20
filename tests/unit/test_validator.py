import json

from event_processor.core.envelope import parse_event

BASE = {
    "event_id": "evt-1",
    "event_type": "transaction.authorized",
    "tenant_id": "client-alpha",
    "schema_version": 1,
    "occurred_at": "2026-10-20T17:00:00Z",
    "payload": {
        "amount": 42.50,
        "currency": "USD",
        "card_last4": "4242",
        "merchant": "Coffee Shop",
    },
}


def validate(registry, obj) -> list[str]:
    return registry.validate(parse_event(json.dumps(obj)))


def test_valid_event_passes(registry):
    assert validate(registry, BASE) == []


def test_extra_data_is_open(registry):
    obj = {**BASE, "extra_data": {"anything": ["goes", 123, {"here": True}]}}
    assert validate(registry, obj) == []


def test_payload_is_strict(registry):
    obj = {**BASE, "payload": {**BASE["payload"], "surprise_field": 1}}
    errors = validate(registry, obj)
    assert errors and any("payload" in e for e in errors)


def test_envelope_is_strict(registry):
    obj = {**BASE, "surprise_field": 1}
    errors = validate(registry, obj)
    assert errors and any("envelope" in e for e in errors)


def test_bad_card_last4_fails(registry):
    obj = {**BASE, "payload": {**BASE["payload"], "card_last4": "abcd"}}
    errors = validate(registry, obj)
    assert errors and any("card_last4" in e for e in errors)


def test_missing_required_payload_field_fails(registry):
    payload = {k: v for k, v in BASE["payload"].items() if k != "merchant"}
    errors = validate(registry, {**BASE, "payload": payload})
    assert errors and any("merchant" in e for e in errors)


def test_unknown_event_type_has_no_contract(registry):
    obj = {**BASE, "event_type": "billing.invoice.paid", "payload": {"total": 5}}
    errors = validate(registry, obj)
    assert errors and any("no contract registered" in e for e in errors)


def test_unparseable_body_reports_parse_error(registry):
    rec = parse_event(b"not json")
    assert registry.validate(rec) == rec.parse_errors


def test_bad_enum_value_fails(registry):
    obj = {
        **BASE,
        "event_type": "monitoring.heartbeat",
        "payload": {"service": "authorizer", "status": "sideways"},
    }
    assert validate(registry, obj) != []
