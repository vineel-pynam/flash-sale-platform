-- Source of truth for drops, units, orders, allocations and payments.
-- The constraints on allocations are the oversell backstop (docs/adr/0002).
-- Full description: docs/03-entities-apis-schema.md.

CREATE TABLE IF NOT EXISTS drops (
    event_id        TEXT        PRIMARY KEY,
    name            TEXT        NOT NULL,
    price_cents     BIGINT      NOT NULL CHECK (price_cents > 0),
    total_units     INT         NOT NULL CHECK (total_units > 0),
    starts_at       TIMESTAMPTZ NOT NULL,
    hold_seconds    INT         NOT NULL DEFAULT 180,
    home_region     TEXT        NOT NULL,
    state           TEXT        NOT NULL CHECK (state IN ('SCHEDULED', 'LIVE', 'ENDED'))
);

-- Every sellable unit. Redis is loaded from here (free units = units without an allocation).
CREATE TABLE IF NOT EXISTS units (
    event_id        TEXT        NOT NULL REFERENCES drops (event_id),
    unit_id         TEXT        NOT NULL,
    label           TEXT,                                   -- "Seat A-14", empty for plain items
    bucket          SMALLINT    NOT NULL CHECK (bucket >= 0),
    PRIMARY KEY (event_id, unit_id)
);

CREATE TABLE IF NOT EXISTS orders (
    order_id        UUID        PRIMARY KEY,
    reservation_id  TEXT        NOT NULL UNIQUE,
    event_id        TEXT        NOT NULL,
    user_id         TEXT        NOT NULL,
    unit_id         TEXT        NOT NULL,
    amount_cents    BIGINT      NOT NULL CHECK (amount_cents > 0),
    state           TEXT        NOT NULL CHECK (state IN ('PAYMENT_PENDING', 'PAID', 'FAILED', 'REFUNDED')),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS orders_pending_idx ON orders (updated_at) WHERE state = 'PAYMENT_PENDING';

CREATE TABLE IF NOT EXISTS allocations (
    event_id        TEXT        NOT NULL,
    unit_id         TEXT        NOT NULL,
    user_id         TEXT        NOT NULL,
    reservation_id  TEXT        NOT NULL UNIQUE,
    order_id        UUID        NOT NULL REFERENCES orders (order_id),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (event_id, unit_id),        -- one owner per unit: no double-sell
    UNIQUE      (event_id, user_id),        -- one unit per account
    FOREIGN KEY (event_id, unit_id) REFERENCES units (event_id, unit_id)
);

CREATE TABLE IF NOT EXISTS payments (
    reservation_id  TEXT        PRIMARY KEY,  -- also the PSP idempotency key: one charge per reservation
    order_id        UUID        NOT NULL REFERENCES orders (order_id),
    amount_cents    BIGINT      NOT NULL,
    payment_token   TEXT        NOT NULL,
    state           TEXT        NOT NULL CHECK (state IN ('PENDING', 'SUCCEEDED', 'DECLINED', 'REFUNDED')),
    psp_charge_id   TEXT,
    attempts        INT         NOT NULL DEFAULT 0,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Transactional outbox. In production a relay publishes these rows to Kafka (docs/adr/0003).
-- In this prototype the payment worker leases PaymentRequested rows directly, which gives the
-- same at-least-once semantics as a Kafka consumer group.
CREATE TABLE IF NOT EXISTS outbox (
    id              BIGSERIAL   PRIMARY KEY,
    aggregate_id    TEXT        NOT NULL,
    event_id        TEXT,                    -- the drop; each drop's worker only picks its own jobs
    event_type      TEXT        NOT NULL,
    payload         JSONB       NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    available_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    attempts        INT         NOT NULL DEFAULT 0,
    processed_at    TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS outbox_pending_idx ON outbox (event_id, event_type, id) WHERE processed_at IS NULL;

CREATE TABLE IF NOT EXISTS refunds (
    refund_id       TEXT        PRIMARY KEY,  -- PSP idempotency key for the refund
    reservation_id  TEXT        NOT NULL,
    reason          TEXT        NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    sent_at         TIMESTAMPTZ                -- set once the PSP accepted the refund
);

CREATE INDEX IF NOT EXISTS refunds_unsent_idx ON refunds (created_at) WHERE sent_at IS NULL;

-- Kafka consumers record each message they handled, so a redelivered message has no second effect.
-- The prototype has no Kafka (the payment worker leases outbox rows instead), so nothing writes here yet.
CREATE TABLE IF NOT EXISTS processed_messages (
    consumer        TEXT        NOT NULL,
    message_id      TEXT        NOT NULL,
    processed_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer, message_id)
);
