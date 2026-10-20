"""The transport contract.

Everything above this layer (processor, validator, store) is written once
and runs on either transport. The contract is deliberately tiny:

  receive()        -> the next message, or None on timeout
  msg.ack()        -> "done, never redeliver"
                      Kafka: commit the offset.  SQS: delete the message.
  msg.send_to_dlq(reason)
                   -> "poison: park it in the dead-letter destination with
                      the reason attached, then treat as acked"
                      Kafka: produce to the DLQ topic + commit.
                      SQS: copy to the DLQ queue + delete the original.
                      (SQS also has native redrive as a backstop — see
                      infra/localstack-init.sh.)

There is deliberately NO nack/redeliver method. Transient failures are
retried in-process by the processor with bounded backoff; if they persist,
the processor stops without acking, and the transport's own redelivery
(Kafka: uncommitted offset; SQS: visibility timeout) does the rest.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass


@dataclass
class IncomingMessage:
    body: bytes
    transport: "Transport"
    native: object  # transport-specific handle (Kafka Message / SQS dict)

    def ack(self) -> None:
        self.transport.ack(self)

    def send_to_dlq(self, reason: str) -> None:
        self.transport.send_to_dlq(self, reason)


class Transport(ABC):
    @abstractmethod
    def receive(self, timeout_s: float = 1.0) -> IncomingMessage | None:
        """Next message, or None if none arrived within the timeout."""

    @abstractmethod
    def ack(self, msg: IncomingMessage) -> None:
        """Mark a message successfully processed; never redeliver it."""

    @abstractmethod
    def send_to_dlq(self, msg: IncomingMessage, reason: str) -> None:
        """Park a poison message in the DLQ with a reason, then ack it."""

    @abstractmethod
    def send(self, body: bytes, key: str | None = None) -> None:
        """Publish a message to the inbound destination (producer side)."""

    @abstractmethod
    def close(self) -> None:
        """Release connections. Un-acked messages stay redeliverable."""
