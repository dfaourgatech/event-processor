-- Event Processor store.
-- One row per event_id, ever: event_id is the idempotency key.
-- The downstream delivery service reads rows with status READY in
-- received_at order; INVALID / FAILED rows exist for troubleshooting
-- and are never delivered.

CREATE TABLE IF NOT EXISTS events (
    event_id          TEXT PRIMARY KEY,
    tenant_id         TEXT,
    event_type        TEXT,
    schema_version    INTEGER,
    status            TEXT NOT NULL CHECK (status IN ('READY', 'INVALID', 'FAILED')),
    payload           JSONB,
    extra_data        JSONB,
    raw_body          TEXT,
    validation_errors JSONB,
    routing_target    TEXT,
    received_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    processed_at      TIMESTAMPTZ
);

-- Downstream consumption scan: WHERE status = 'READY' ORDER BY received_at
CREATE INDEX IF NOT EXISTS idx_events_status_received
    ON events (status, received_at);

-- Troubleshooting: everything for one tenant, in arrival order
CREATE INDEX IF NOT EXISTS idx_events_tenant_received
    ON events (tenant_id, received_at);
