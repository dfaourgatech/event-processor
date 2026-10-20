# Event Processor

A reactive event-processing service: consumes events from multiple producers,
validates them against a declarative contract, triages them, and persists them
idempotently for low-latency delivery by a downstream service.

## Stack

- Python 3.12
- Docker Compose (broker / LocalStack / Postgres — see DESIGN.md)

## Layout

- `DESIGN.md` — architecture, key decisions, failure modes, limitations
- `src/event_processor/` — the service
- `producer/` — event producer simulator
- `schemas/` — declarative event contracts
- `tests/` — test suite (single command: `make test`)

## Status

Milestone 1: scaffold. Design decisions land in DESIGN.md as they're made.
