"""Entry point: wire settings → store → registry → transport → processor.

Graceful shutdown (DESIGN.md, Decision 6): SIGTERM/SIGINT only set a flag.
The message currently in flight is finished — persisted, then acked —
before the loop notices the flag and exits. Anything not acked stays
redeliverable.
"""

from __future__ import annotations

import logging
import signal

from .config import Settings
from .core.validator import SchemaRegistry
from .processor import Processor, StoreUnavailable
from .store.repository import EventStore
from .transports.factory import build_transport


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    settings = Settings.from_env()

    store = EventStore(settings.database_url)
    store.ensure_schema()
    registry = SchemaRegistry(settings.schema_dir)
    transport = build_transport(settings)
    processor = Processor(settings, transport, store, registry)

    def _signal(signum, _frame):
        logging.getLogger(__name__).info("signal %s received, finishing up", signum)
        processor.stop()

    signal.signal(signal.SIGTERM, _signal)
    signal.signal(signal.SIGINT, _signal)

    try:
        processor.run()
    except StoreUnavailable as exc:
        # Exit non-zero without acking: the transport redelivers, an
        # orchestrator restarts us, and no valid event was DLQ'd for an
        # infrastructure problem. See processor.py module docstring.
        logging.getLogger(__name__).error("store unavailable, exiting: %s", exc)
        return 1
    finally:
        transport.close()
        store.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
