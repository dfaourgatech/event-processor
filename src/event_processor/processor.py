"""The processor: consume → parse → validate → route → persist → ack.

Failure handling follows DESIGN.md exactly:

- Bad data (parse errors, schema violations, unknown contract)
  → persisted as INVALID with the reasons, then acked. Producers sending
    bad events is a normal case, not an outage — and nothing is dropped.
- Store unavailable
  → the save is retried in-process with backoff; if the store stays down,
    the processor STOPS without acking. The un-acked message is
    redelivered by the transport once the store is back. Valid events are
    never DLQ'd for an infra problem on our side.
- Unexpected processing error (a bug, not bad data)
  → the message is retried in-process; on exhaustion a FAILED record is
    persisted (best effort) and the message is parked in the DLQ with the
    reason, so one poison message cannot crash-loop the consumer or block
    its partition/queue.
- Duplicate delivery
  → the store reports it; we count it and ack. This is at-least-once
    working as designed, not an error.
"""

from __future__ import annotations

import logging
import time
from collections import Counter

from .config import Settings
from .core.envelope import parse_event
from .core.router import route
from .core.validator import SchemaRegistry
from .store.repository import EventStore, Status
from .transports.base import IncomingMessage, Transport

log = logging.getLogger(__name__)


class StoreUnavailable(Exception):
    """The store stayed down through all retries — stop, don't ack."""


class Processor:
    def __init__(
        self,
        settings: Settings,
        transport: Transport,
        store: EventStore,
        registry: SchemaRegistry,
    ):
        self._settings = settings
        self._transport = transport
        self._store = store
        self._registry = registry
        self.stats: Counter[str] = Counter()
        self._stopping = False

    def stop(self) -> None:
        self._stopping = True

    def run(self) -> None:
        log.info("processor starting (transport=%s)", self._settings.transport)
        while not self._stopping:
            msg = self._transport.receive(timeout_s=1.0)
            if msg is None:
                continue
            self.process_one(msg)
        log.info("processor stopped; stats=%s", dict(self.stats))

    def process_one(self, msg: IncomingMessage) -> None:
        """Process one message end to end, with the poison-message net."""
        self.stats["received"] += 1
        last_exc: Exception | None = None
        for attempt in range(1, self._settings.max_attempts + 1):
            try:
                self._handle(msg)
                return
            except StoreUnavailable:
                raise  # crash-without-ack is the designed response
            except Exception as exc:  # noqa: BLE001 — the poison net is the point
                last_exc = exc
                log.warning(
                    "processing failed (attempt %d/%d): %s",
                    attempt,
                    self._settings.max_attempts,
                    exc,
                )
                time.sleep(0.5 * (2 ** (attempt - 1)))
        record = parse_event(msg.body)  # never raises; for the FAILED record
        self._fail(msg, record, f"{type(last_exc).__name__}: {last_exc}")

    def _handle(self, msg: IncomingMessage) -> None:
        record = parse_event(msg.body)
        errors = self._registry.validate(record)

        if errors:
            inserted = self._save_with_retry(record, Status.INVALID, errors, None)
            self.stats["invalid"] += 1
            if not inserted:
                self.stats["duplicates"] += 1
            msg.ack()
            return

        target = route(record.event_type or "")
        if target is None:  # contract exists but no route: configuration bug
            raise RuntimeError(
                f"no route configured for event_type={record.event_type!r}"
            )

        inserted = self._save_with_retry(record, Status.READY, None, target)
        self.stats["ready" if inserted else "duplicates"] += 1
        msg.ack()

    def _save_with_retry(
        self,
        record,
        status: Status,
        errors: list[str] | None,
        target: str | None,
    ) -> bool:
        last_exc: Exception | None = None
        for attempt in range(1, self._settings.max_attempts + 1):
            try:
                return self._store.save(record, status, errors, target)
            except Exception as exc:  # store errors are the retryable kind here
                last_exc = exc
                log.warning(
                    "store save failed (attempt %d/%d): %s",
                    attempt,
                    self._settings.max_attempts,
                    exc,
                )
                time.sleep(0.5 * (2 ** (attempt - 1)))
        raise StoreUnavailable(str(last_exc))

    def _fail(self, msg: IncomingMessage, record, reason: str) -> None:
        log.error("poison message event_id=%s: %s", record.event_id, reason)
        try:
            self._store.save(record, Status.FAILED, [reason], None)
        except Exception:
            log.exception("could not persist FAILED record for %s", record.event_id)
        msg.send_to_dlq(reason)
        self.stats["failed"] += 1
