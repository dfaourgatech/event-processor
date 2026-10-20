"""SQS transport (via LocalStack locally, real AWS endpoints unchanged).

- Long-poll receive; a message is deleted only on ack. Anything not acked
  returns when its visibility timeout expires.
- Poison handling is explicit (copy to the DLQ queue + delete) so behavior
  matches the Kafka implementation; the queue's native redrive policy
  (maxReceiveCount=3, wired in infra/localstack-init.sh) is the backstop
  for messages whose processing never completes at all.
- Included for spec conformance — SQS has no replay; see the production
  note in DESIGN.md, Decision 1.
"""

from __future__ import annotations

import boto3

from ..config import Settings
from .base import IncomingMessage, Transport


class SqsTransport(Transport):
    def __init__(self, settings: Settings):
        self._settings = settings
        self._client = boto3.client(
            "sqs",
            endpoint_url=settings.sqs_endpoint,
            region_name=settings.sqs_region,
            aws_access_key_id="test",
            aws_secret_access_key="test",
        )
        self._queue_url = self._client.get_queue_url(QueueName=settings.sqs_queue)[
            "QueueUrl"
        ]
        self._dlq_url = self._client.get_queue_url(QueueName="events-dlq")["QueueUrl"]

    def receive(self, timeout_s: float = 1.0) -> IncomingMessage | None:
        resp = self._client.receive_message(
            QueueUrl=self._queue_url,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=min(int(timeout_s), 20),
            AttributeNames=["ApproximateReceiveCount"],
        )
        messages = resp.get("Messages", [])
        if not messages:
            return None
        return IncomingMessage(
            body=messages[0]["Body"].encode("utf-8"),
            transport=self,
            native=messages[0],
        )

    def ack(self, msg: IncomingMessage) -> None:
        self._client.delete_message(
            QueueUrl=self._queue_url,
            ReceiptHandle=msg.native["ReceiptHandle"],
        )

    def send_to_dlq(self, msg: IncomingMessage, reason: str) -> None:
        self._client.send_message(
            QueueUrl=self._dlq_url,
            MessageBody=msg.body.decode("utf-8", errors="replace"),
            MessageAttributes={
                "dlq_reason": {"DataType": "String", "StringValue": reason[:256]},
            },
        )
        self.ack(msg)

    def send(self, body: bytes, key: str | None = None) -> None:
        self._client.send_message(
            QueueUrl=self._queue_url,
            MessageBody=body.decode("utf-8"),
        )

    def close(self) -> None:
        self._client.close()
