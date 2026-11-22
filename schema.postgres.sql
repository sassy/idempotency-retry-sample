-- Production equivalent of the in-memory store in src/store.rs.
-- This file is documentation: the sample itself runs without a database.
--
-- Two layers of defense against double charging:
--   1. idempotency_keys  -- one record per payment attempt; retries replay
--                           the stored response instead of processing again.
--   2. payments partial unique index -- one succeeded payment per order,
--                           no matter how many different keys are generated.

BEGIN;

-- ---------------------------------------------------------------------------
-- Layer 1: idempotency keys
-- ---------------------------------------------------------------------------
CREATE TABLE idempotency_keys (
    -- The key sent in the `Idempotency-Key` header. Scope it by account
    -- (and endpoint) in a real system; this sample scopes by order, so the
    -- primary key is (order_id, key).
    order_id        TEXT        NOT NULL,
    key             TEXT        NOT NULL,
    -- Hash (e.g. SHA-256) of the canonical request payload. Reusing a key
    -- with a different payload is a client bug -> 422.
    request_hash    TEXT        NOT NULL,
    status          TEXT        NOT NULL DEFAULT 'processing'
                    CHECK (status IN ('processing', 'completed')),
    -- The response of the first, successful processing. Retries with the
    -- same key return these values verbatim.
    response_status INTEGER,
    response_body   JSONB,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at    TIMESTAMPTZ,
    -- Keys are kept for a retention window (commonly 24h) and then deleted
    -- by a periodic job; `expires_at` supports that cleanup.
    expires_at      TIMESTAMPTZ NOT NULL DEFAULT now() + INTERVAL '24 hours',
    PRIMARY KEY (order_id, key)
);

-- Claim protocol (what the handler's lock + HashMap models):
--   INSERT INTO idempotency_keys (order_id, key, request_hash)
--   VALUES ($1, $2, $3)
--   ON CONFLICT (order_id, key) DO NOTHING
--   RETURNING key;
-- * Row returned      -> you claimed the key; process, then UPDATE the row
--                        to status='completed' with the response.
-- * No row returned   -> SELECT the existing row: hash differs -> 422;
--                        completed -> replay response; processing -> 409.

-- ---------------------------------------------------------------------------
-- Layer 2: payments, with the partial unique index as the last line of defense
-- ---------------------------------------------------------------------------
CREATE TABLE payments (
    id         TEXT        PRIMARY KEY,  -- e.g. 'pay_...' or a UUID
    order_id   TEXT        NOT NULL,
    amount     BIGINT      NOT NULL,
    currency   TEXT        NOT NULL,
    status     TEXT        NOT NULL CHECK (status IN ('succeeded', 'failed')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- At most ONE succeeded payment per order. Even if a client generates a
-- fresh Idempotency-Key (double click in another tab, a buggy frontend),
-- the second successful INSERT violates this index and is rejected,
-- which the API turns into 409 `order_already_paid`.
CREATE UNIQUE INDEX payments_one_succeeded_per_order
    ON payments (order_id)
    WHERE status = 'succeeded';

COMMIT;
