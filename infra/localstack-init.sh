#!/bin/bash
# Runs inside LocalStack when it reports ready: create the SQS queues and
# wire the DLQ redrive policy (max 3 receives, then dead-letter).
set -e

awslocal sqs create-queue --queue-name events-dlq
DLQ_URL=$(awslocal sqs get-queue-url --queue-name events-dlq --query QueueUrl --output text)
DLQ_ARN=$(awslocal sqs get-queue-attributes --queue-url "$DLQ_URL" --attribute-names QueueArn --query 'Attributes.QueueArn' --output text)

awslocal sqs create-queue --queue-name events-inbound --attributes "{
  \"VisibilityTimeout\": \"30\",
  \"RedrivePolicy\": \"{\\\"deadLetterTargetArn\\\": \\\"$DLQ_ARN\\\", \\\"maxReceiveCount\\\": 3}\"
}"

echo "LocalStack SQS init complete: events-inbound (+ events-dlq)"
