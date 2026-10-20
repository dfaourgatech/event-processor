"""Triage: which delivery target an event type belongs to.

Deliberately a small explicit map. Routing is a decision recorded on the
persisted event; the downstream delivery service owns actually sending.
An event type with no route here should already have failed validation
(no contract registered), so an unrouted event is a configuration bug,
not a runtime case.
"""

from __future__ import annotations

ROUTES: dict[str, str] = {
    "transaction.authorized": "delivery.transactions",
    "monitoring.heartbeat": "delivery.monitoring",
    "user.action": "delivery.user-actions",
}


def route(event_type: str) -> str | None:
    return ROUTES.get(event_type)
