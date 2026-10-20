# Design: Event Processor

## Problem Statement

Multiple producers (monitoring, user applications, transaction authorizers,
integrations) emit events in their own contexts. A single consumer — this
service — must receive those events, validate them against a declared
contract, triage them, and persist them so a downstream delivery service can
send them to the correct client with low latency.

Producers must not need to understand client integration details. The
platform is multi-tenant: every event belongs to exactly one client (tenant)
and must be routable to that client. No event may be lost, including events
that are malformed, invalid, duplicated, or in flight when the service stops.

## Architecture Overview

```
 producers ──► ┌─────────────────────┐
               │  Transport          │   Kafka topic  (partitioned by tenant_id)
               │  (one interface,    │   — or —
               │   two impls)        │   SQS queue    (LocalStack)
               └─────────┬───────────┘
                         ▼
               ┌─────────────────────┐      invalid / poison
               │  Event Processor    │ ───────────────────► DLQ + persisted
               │  parse → validate   │                      error record
               │  → route → persist  │
               └─────────┬───────────┘
                         ▼
               ┌─────────────────────┐
               │  Postgres           │  events table (idempotent on event_id)
               │  status, errors,    │  read by the downstream delivery
               │  routing, timestamps│  service (out of scope here)
               └─────────────────────┘
```

Components:

- **Transport** (`transports/`) — a small interface: *receive a batch, ack on
  success, leave un-acked work to be redelivered*. Two implementations:
  Kafka and SQS. Everything above this layer is transport-agnostic.
- **Validator** (`core/validator.py`) — JSON Schema registry keyed by
  `(event_type, schema_version)`.
- **Router** (`core/router.py`) — maps `event_type` → delivery target.
  Routing is a decision recorded at persistence time; actual delivery is a
  downstream service's job.
- **Store** (`store/`) — Postgres repository; the only writer of event state.
- **Producer simulator** (`producer/`) — a CLI harness that emits valid,
  invalid, duplicate, and poison events across tenants and types so every
  behavior above can be exercised end to end.

## Event Envelope

Every event carries a strict envelope plus a payload:

```json
{
  "event_id": "uuid",
  "event_type": "transaction.authorized",
  "tenant_id": "client-123",
  "schema_version": 1,
  "occurred_at": "2026-10-20T17:00:00Z",
  "payload": { "... validated against the type's schema ..." },
  "extra_data": { "... open extension object, see Decision 4 ..." }
}
```

## Key Design Decisions

### 1. Dual transport behind one interface — with a production note

Both Kafka and SQS are implemented, selected by configuration
(`TRANSPORT=kafka|sqs`).

> **Production note:** an actual production system should likely choose
> **Kafka**. SQS deletes a message once it is acknowledged, so there is **no
> replay**: reprocessing history after a bug fix, or rebuilding the store
> after corruption, is impossible — DLQ redrive only covers messages that
> already failed. SQS Standard queues also provide no ordering guarantee,
> and FIFO queues cap throughput. Kafka's durable log makes replay an offset
> reset, and partitioning by `tenant_id` gives per-tenant ordering and a
> natural scaling axis.
>
> The SQS/LocalStack implementation is included for conformance with the
> challenge spec, which points at AWS services simulated with LocalStack —
> and because building both is what proves the transport abstraction is real
> rather than hypothetical.

### 2. Delivery semantics: at-least-once + idempotency

Exactly-once end-to-end is not a real property of distributed systems, so the
design is at-least-once transport plus an idempotent store:

- Kafka: offsets are committed **manually, only after persistence**.
- SQS: messages are deleted **only after persistence**; otherwise the
  visibility timeout returns them.
- Store: `event_id` is the primary key; inserts are
  `ON CONFLICT DO NOTHING`. A redelivered duplicate is absorbed and counted,
  never duplicated.

### 3. Persistence: Postgres

The events table stores everything troubleshooting needs: envelope fields,
payload, `extra_data`, status (`RECEIVED` is transient; terminal states are
`READY`, `INVALID`, `FAILED`), validation errors, routing decision, and
received/processed timestamps. The downstream service consumes with a simple
indexed scan on `(status, received_at)` — low latency at this scale without a
second store to keep consistent. (Postgres `LISTEN/NOTIFY` is the noted
upgrade path if poll latency ever matters.)

Alternatives considered: DynamoDB (conditional writes make idempotency
elegant, but status/tenant troubleshooting queries become index gymnastics)
and Redis (fast, but the weakest durability story for a "no event lost"
requirement).

### 4. Validation: JSON Schema with an `extra_data` extension envelope

Each `(event_type, schema_version)` has a JSON Schema file in `schemas/`.
The envelope and the core payload are **strict**
(`additionalProperties: false`). One field is deliberately open:
`extra_data`, an object producers may extend freely.

Governance rule that keeps this honest: `extra_data` is a *staging area*, not
a junk drawer. New fields land there first; when a field proves stable it is
**promoted** into the core contract at the next schema version. Consumers
must ignore unknown `extra_data` keys, which is what makes old consumers
forward-compatible with new producers.

**Invalid events are persisted, never dropped** — status `INVALID` with the
full validation error list attached.

### 5. Poison messages

- Kafka has no native DLQ: the processor retries a failing message in
  process (bounded, with backoff), then publishes it to a `events.dlq` topic
  with the error and attempt count in headers, persists a `FAILED` record,
  and commits the offset so one bad message cannot block a partition.
- SQS: the queue's native redrive policy (`maxReceiveCount: 3`) moves
  repeat failures to a DLQ; a `FAILED` record is persisted where the
  envelope can be parsed.

### 6. Graceful shutdown

On SIGTERM/SIGINT the processor stops receiving, finishes the in-flight
message (persist, then ack/commit), and exits. Anything not acked is
redelivered to the next instance — combined with Decision 2, shutdown loses
nothing and duplicates nothing.

## Failure Modes

| Failure | Handling |
| --- | --- |
| Body is not JSON at all | Persisted with a synthetic `event_id` = SHA-256 of the raw body (so redelivery is still idempotent), status `INVALID`, raw body preserved. |
| Envelope field missing/invalid | Same path: `INVALID`, errors recorded. |
| Payload fails schema | `INVALID` with the validator's error list. No drop. |
| Unknown `event_type` / schema version | `INVALID` — no contract to validate against. |
| Postgres unavailable | Message is **not** acked/committed; transport redelivers. Processor backs off rather than hot-looping. |
| Processing exception (poison) | Bounded retries → DLQ + `FAILED` record (Decision 5). |
| Crash mid-batch | Uncommitted work is redelivered; idempotent store absorbs duplicates. |
| Duplicate delivery | `ON CONFLICT DO NOTHING`; duplicate counted in logs/metrics. |

## Known Limitations

- Schemas live as files in this repo — no registry service and no automated
  backward-compatibility check beyond the test suite.
- `extra_data` contents are unvalidated by design (Decision 4); a field that
  matters must be promoted into the core contract to gain validation.
- Scaling beyond one processor instance relies on the Kafka consumer group /
  SQS competing-consumers model; no extra coordination is implemented.
- The downstream delivery service is out of scope; its read contract is the
  events table described in Decision 3.
- LocalStack emulates SQS; its semantics can differ subtly from real AWS.
  This is one more reason for the production note in Decision 1.
