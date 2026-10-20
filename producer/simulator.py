"""Producer simulator: the harness that exercises every processor behavior.

Emits a scripted mix onto the configured transport (TRANSPORT=kafka|sqs):

  - valid events for all three contracts, across three tenants
  - payloads that violate their schema (bad card_last4, missing field)
  - an event_type with no contract
  - a body that is not JSON at all
  - exact duplicates of earlier events (same event_id — must be absorbed)

Run it, then check the store:
  make produce
  make psql   →  SELECT status, count(*) FROM events GROUP BY status;

Usage: python -m producer.simulator [--count N]
"""

from __future__ import annotations

import argparse
import json
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from event_processor.config import Settings  # noqa: E402
from event_processor.transports.factory import build_transport  # noqa: E402

TENANTS = ["client-alpha", "client-beta", "client-gamma"]


def _envelope(event_type: str, tenant: str, payload: dict, extra: dict | None = None) -> dict:
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "tenant_id": tenant,
        "schema_version": 1,
        "occurred_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "payload": payload,
        "extra_data": extra or {},
    }


def build_events(count: int) -> list[tuple[bytes, str | None, str]]:
    """(body, kafka key, expected outcome) triples."""
    events: list[tuple[bytes, str | None, str]] = []

    valid_templates = [
        ("transaction.authorized", {"amount": 129.99, "currency": "USD",
                                    "card_last4": "4242", "merchant": "Coffee Shop"}),
        ("monitoring.heartbeat", {"service": "authorizer", "status": "up",
                                  "latency_ms": 42}),
        ("user.action", {"user_id": "u-1001", "action": "login"}),
    ]
    sent_valid: list[dict] = []
    for i in range(count):
        event_type, payload = valid_templates[i % len(valid_templates)]
        tenant = TENANTS[i % len(TENANTS)]
        env = _envelope(event_type, tenant, payload,
                        extra={"trace_hint": "sim"} if i == 0 else None)
        sent_valid.append(env)
        events.append((json.dumps(env).encode(), tenant, "READY"))

    # Schema violations
    bad_card = _envelope("transaction.authorized", "client-alpha",
                         {"amount": 10.0, "currency": "USD",
                          "card_last4": "abcd", "merchant": "Shop"})
    events.append((json.dumps(bad_card).encode(), "client-alpha", "INVALID"))
    missing_field = _envelope("user.action", "client-beta", {"user_id": "u-9"})
    events.append((json.dumps(missing_field).encode(), "client-beta", "INVALID"))

    # No contract registered for this type
    unknown = _envelope("billing.invoice.paid", "client-gamma", {"total": 5})
    events.append((json.dumps(unknown).encode(), "client-gamma", "INVALID"))

    # Not JSON at all
    events.append((b"this is not json{{{", None, "INVALID"))

    # Exact duplicates of the first two valid events — must be absorbed
    for env in sent_valid[:2]:
        events.append((json.dumps(env).encode(), env["tenant_id"], "DUPLICATE"))

    return events


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--count", type=int, default=20,
                        help="number of valid events to generate")
    args = parser.parse_args()

    settings = Settings.from_env()
    transport = build_transport(settings)
    events = build_events(args.count)

    expected: dict[str, int] = {}
    for body, key, outcome in events:
        transport.send(body, key=key)
        expected[outcome] = expected.get(outcome, 0) + 1
    if hasattr(transport, "flush"):
        transport.flush()
    transport.close()

    print(f"sent {len(events)} messages via {settings.transport}")
    print("expected store outcome:", json.dumps(expected, indent=2))
    print("(READY/INVALID rows in Postgres; DUPLICATEs absorbed, no extra rows)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
