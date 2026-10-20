import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "src"))

import pytest  # noqa: E402

from event_processor.config import Settings  # noqa: E402
from event_processor.core.validator import SchemaRegistry  # noqa: E402
from event_processor.store.repository import Status  # noqa: E402
from event_processor.transports.base import IncomingMessage, Transport  # noqa: E402


@pytest.fixture()
def settings() -> Settings:
    return Settings(schema_dir=str(REPO_ROOT / "schemas"), max_attempts=3)


@pytest.fixture()
def registry(settings) -> SchemaRegistry:
    return SchemaRegistry(settings.schema_dir)


class FakeTransport(Transport):
    """Records acks and DLQ sends instead of touching a broker."""

    def __init__(self):
        self.acked: list[IncomingMessage] = []
        self.dlq: list[tuple[IncomingMessage, str]] = []
        self.sent: list[bytes] = []

    def receive(self, timeout_s: float = 1.0):
        return None

    def ack(self, msg):
        self.acked.append(msg)

    def send_to_dlq(self, msg, reason):
        self.dlq.append((msg, reason))

    def send(self, body, key=None):
        self.sent.append(body)

    def close(self):
        pass


class FakeStore:
    """In-memory stand-in with the EventStore.save contract."""

    def __init__(self, fail: bool = False):
        self.rows: dict[str, dict] = {}
        self.fail = fail

    def save(self, record, status: Status, validation_errors=None, routing_target=None):
        if self.fail:
            raise ConnectionError("store is down")
        if record.event_id in self.rows:
            return False
        self.rows[record.event_id] = {
            "status": status,
            "errors": validation_errors,
            "target": routing_target,
        }
        return True


@pytest.fixture()
def fake_transport() -> FakeTransport:
    return FakeTransport()


def make_msg(transport: FakeTransport, body: bytes) -> IncomingMessage:
    return IncomingMessage(body=body, transport=transport, native=None)
