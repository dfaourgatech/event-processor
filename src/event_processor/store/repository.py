"""The idempotent event store.

One row per event_id, ever. `save` returns True when the row was inserted
and False when the event was already present — that False is the entire
duplicate-delivery story: at-least-once transport + this insert equals
effectively-once persistence (DESIGN.md, Decision 2).

Status semantics:
  READY    validated, routed, available to the downstream delivery service
  INVALID  failed the contract (or was unparseable); validation_errors says why
  FAILED   processing itself failed after bounded retries; sent to the DLQ
"""

from __future__ import annotations

import json
from enum import Enum

import psycopg
from psycopg.types.json import Jsonb

from ..core.envelope import EventRecord

DDL = """
CREATE TABLE IF NOT EXISTS events (
    event_id          TEXT PRIMARY KEY,
    tenant_id         TEXT,
    event_type        TEXT,
    schema_version    INTEGER,
    status            TEXT NOT NULL CHECK (status IN ('READY', 'INVALID', 'FAILED')),
    payload           JSONB,
    extra_data        JSONB,
    raw_body          TEXT,
    validation_errors JSONB,
    routing_target    TEXT,
    received_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    processed_at      TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_events_status_received ON events (status, received_at);
CREATE INDEX IF NOT EXISTS idx_events_tenant_received ON events (tenant_id, received_at);
"""

_INSERT = """
INSERT INTO events (
    event_id, tenant_id, event_type, schema_version, status,
    payload, extra_data, raw_body, validation_errors, routing_target, processed_at
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
ON CONFLICT (event_id) DO NOTHING
"""


class Status(str, Enum):
    READY = "READY"
    INVALID = "INVALID"
    FAILED = "FAILED"


class EventStore:
    def __init__(self, database_url: str):
        self._conn = psycopg.connect(database_url, autocommit=True)

    def ensure_schema(self) -> None:
        """Apply the DDL (idempotent). Compose also applies infra/schema.sql
        on first boot; this covers runs outside Compose and tests."""
        self._conn.execute(DDL)

    def save(
        self,
        record: EventRecord,
        status: Status,
        validation_errors: list[str] | None = None,
        routing_target: str | None = None,
    ) -> bool:
        """Persist an event. Returns True if inserted, False if it was a
        duplicate of an already-persisted event_id."""
        cur = self._conn.execute(
            _INSERT,
            (
                record.event_id,
                record.tenant_id,
                record.event_type,
                record.schema_version,
                status.value,
                Jsonb(record.payload) if record.payload is not None else None,
                Jsonb(record.extra_data) if record.extra_data else None,
                record.raw_body if record.parse_errors else None,
                Jsonb(validation_errors) if validation_errors else None,
                routing_target,
            ),
        )
        return cur.rowcount == 1

    def count(self, status: Status | None = None) -> int:
        if status is None:
            row = self._conn.execute("SELECT count(*) FROM events").fetchone()
        else:
            row = self._conn.execute(
                "SELECT count(*) FROM events WHERE status = %s", (status.value,)
            ).fetchone()
        return int(row[0])

    def get(self, event_id: str) -> dict | None:
        row = self._conn.execute(
            "SELECT event_id, tenant_id, event_type, schema_version, status, "
            "payload, extra_data, validation_errors, routing_target "
            "FROM events WHERE event_id = %s",
            (event_id,),
        ).fetchone()
        if row is None:
            return None
        return {
            "event_id": row[0],
            "tenant_id": row[1],
            "event_type": row[2],
            "schema_version": row[3],
            "status": row[4],
            "payload": row[5] if not isinstance(row[5], str) else json.loads(row[5]),
            "extra_data": row[6] if not isinstance(row[6], str) else json.loads(row[6]),
            "validation_errors": row[7]
            if not isinstance(row[7], str)
            else json.loads(row[7]),
            "routing_target": row[8],
        }

    def close(self) -> None:
        self._conn.close()
