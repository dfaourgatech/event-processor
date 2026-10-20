"""The event envelope: the strict outer contract every producer must meet.

Parsing never raises for bad input — a malformed message is data, not an
exception. Anything we cannot parse still gets an EventRecord with a
synthetic id (SHA-256 of the raw body), so it can be persisted, counted,
and — on redelivery — recognised as the same event.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from typing import Any


@dataclass
class EventRecord:
    event_id: str
    raw_body: str
    tenant_id: str | None = None
    event_type: str | None = None
    schema_version: int | None = None
    occurred_at: str | None = None
    payload: dict[str, Any] | None = None
    extra_data: dict[str, Any] = field(default_factory=dict)
    parse_errors: list[str] = field(default_factory=list)

    @property
    def parseable(self) -> bool:
        return not self.parse_errors


def synthetic_id(raw_body: str) -> str:
    """Stable id for a body we cannot extract a real event_id from."""
    return "sha256:" + hashlib.sha256(raw_body.encode("utf-8")).hexdigest()


def parse_event(raw: bytes | str) -> EventRecord:
    """Decode raw transport bytes into an EventRecord (never raises)."""
    raw_body = raw.decode("utf-8", errors="replace") if isinstance(raw, bytes) else raw

    try:
        data = json.loads(raw_body)
    except (json.JSONDecodeError, ValueError) as exc:
        return EventRecord(
            event_id=synthetic_id(raw_body),
            raw_body=raw_body,
            parse_errors=[f"body is not valid JSON: {exc}"],
        )

    if not isinstance(data, dict):
        return EventRecord(
            event_id=synthetic_id(raw_body),
            raw_body=raw_body,
            parse_errors=["body is not a JSON object"],
        )

    event_id = data.get("event_id")
    return EventRecord(
        event_id=event_id if isinstance(event_id, str) and event_id else synthetic_id(raw_body),
        raw_body=raw_body,
        tenant_id=data.get("tenant_id"),
        event_type=data.get("event_type"),
        schema_version=data.get("schema_version"),
        occurred_at=data.get("occurred_at"),
        payload=data.get("payload") if isinstance(data.get("payload"), dict) else None,
        extra_data=data.get("extra_data") if isinstance(data.get("extra_data"), dict) else {},
    )
