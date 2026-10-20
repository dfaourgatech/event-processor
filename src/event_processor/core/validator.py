"""Declarative contract validation.

Contracts are JSON Schema files in the schema directory:
  envelope.schema.json                  — the strict outer envelope
  {event_type}.v{version}.schema.json   — the strict core payload

The one deliberate opening is the envelope's `extra_data` object: producers
may extend it freely (see DESIGN.md, Decision 4 — it is a staging area whose
stable fields get promoted into the core contract at the next version).

Validation returns a list of human-readable errors; an empty list is valid.
Nothing is dropped here — the caller persists invalid events with the errors
attached.
"""

from __future__ import annotations

import json
from pathlib import Path

from jsonschema import Draft202012Validator

from .envelope import EventRecord


class SchemaRegistry:
    def __init__(self, schema_dir: str | Path):
        self._dir = Path(schema_dir)
        envelope_path = self._dir / "envelope.schema.json"
        self._envelope = Draft202012Validator(
            json.loads(envelope_path.read_text(encoding="utf-8"))
        )
        self._payloads: dict[tuple[str, int], Draft202012Validator] = {}
        for path in sorted(self._dir.glob("*.v*.schema.json")):
            # transaction.authorized.v1.schema.json -> ("transaction.authorized", 1)
            stem = path.name[: -len(".schema.json")]
            event_type, _, version = stem.rpartition(".v")
            schema = json.loads(path.read_text(encoding="utf-8"))
            self._payloads[(event_type, int(version))] = Draft202012Validator(schema)

    def has_contract(self, event_type: str, schema_version: int) -> bool:
        return (event_type, schema_version) in self._payloads

    def validate(self, record: EventRecord) -> list[str]:
        if record.parse_errors:
            return list(record.parse_errors)

        errors: list[str] = []
        message = json.loads(record.raw_body)

        for err in sorted(self._envelope.iter_errors(message), key=lambda e: list(e.path)):
            errors.append(f"envelope.{'.'.join(map(str, err.path)) or '<root>'}: {err.message}")

        if record.event_type is not None and record.schema_version is not None:
            validator = self._payloads.get((record.event_type, record.schema_version))
            if validator is None:
                errors.append(
                    f"no contract registered for event_type={record.event_type!r} "
                    f"schema_version={record.schema_version}"
                )
            elif isinstance(record.payload, dict):
                for err in sorted(validator.iter_errors(record.payload), key=lambda e: list(e.path)):
                    errors.append(
                        f"payload.{'.'.join(map(str, err.path)) or '<root>'}: {err.message}"
                    )
        return errors
