"""Transport selection: TRANSPORT=kafka|sqs (see DESIGN.md, Decision 1)."""

from __future__ import annotations

from ..config import Settings
from .base import Transport


def build_transport(settings: Settings) -> Transport:
    if settings.transport == "kafka":
        from .kafka_transport import KafkaTransport

        return KafkaTransport(settings)
    if settings.transport == "sqs":
        from .sqs_transport import SqsTransport

        return SqsTransport(settings)
    raise ValueError(f"unknown TRANSPORT={settings.transport!r} (expected kafka|sqs)")
