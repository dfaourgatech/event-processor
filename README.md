# Event Processor

A reactive event-processing service: consumes events from multiple producers,
validates them against a declarative contract, triages them, and persists them
idempotently for low-latency delivery by a downstream service.

**Start with [DESIGN.md](DESIGN.md)** — architecture, key decisions
(including why a production system should likely choose Kafka, and why SQS
is here anyway), failure modes, and known limitations.

## Stack

- Python 3.12
- **Two transports behind one interface** (`TRANSPORT=kafka|sqs`):
  Kafka (KRaft, local container) or SQS (LocalStack)
- Postgres for idempotent persistence
- JSON Schema contracts, one per `(event_type, schema_version)`

## Run it

```bash
cp .env.example .env        # optional; defaults work
make up                     # Kafka + LocalStack + Postgres + processor
make produce                # simulator: valid, invalid, duplicate, malformed events
make psql                   # inspect what landed
```

```sql
SELECT status, routing_target, count(*) FROM events GROUP BY 1, 2;
```

With the default simulator run (20 valid events) you should see 20 `READY`
rows, 4 `INVALID` rows (bad `card_last4`, a missing field, an unknown event
type, a non-JSON body) — and no extra rows for the 2 duplicate deliveries,
which the idempotent store absorbs.

To run on SQS instead of Kafka:

```bash
TRANSPORT=sqs make up
```

## Tests

```bash
make test               # unit tests — no stack required (23 tests)
make test-integration   # end-to-end against the running compose stack;
                        # run once per TRANSPORT
```

## Layout

- `DESIGN.md` — the design document
- `src/event_processor/` — the service
  - `core/` — envelope parsing, JSON Schema validation, routing
  - `transports/` — the transport interface + Kafka and SQS implementations
  - `store/` — the idempotent Postgres repository
  - `processor.py` — consume → validate → route → persist → ack
- `producer/` — event producer simulator
- `schemas/` — declarative event contracts
- `infra/` — Postgres DDL, LocalStack queue bootstrap
- `tests/` — unit + integration suites
