//! In-memory store that models two production database tables.
//!
//! This is a learning sample, so instead of PostgreSQL we keep everything in
//! memory behind a single lock. The mapping to the real schema is:
//!
//! | In-memory field          | Production table (see `schema.postgres.sql`) |
//! |--------------------------|----------------------------------------------|
//! | `idempotency_keys`       | `idempotency_keys`                           |
//! | `payments`               | `payments` (+ partial unique index)          |
//! | `charge_count`           | calls to the external payment gateway        |
//!
//! The single lock plays the role of the database transaction / unique
//! constraints: every check-and-insert sequence happens while holding it,
//! so two concurrent requests can never both slip past the checks.

use std::collections::HashMap;

/// Status of one idempotency key, mirroring the `status` column.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IdempotencyStatus {
    /// The first request with this key is still being processed.
    Processing,
    /// Processing finished; `response_status` / `response_body` are stored.
    Completed,
}

/// One row of the (modelled) `idempotency_keys` table.
#[derive(Debug, Clone)]
pub struct IdempotencyRecord {
    /// Hash of the request payload. The same key with a different payload
    /// is a client bug and is rejected with 422.
    pub request_hash: u64,
    pub status: IdempotencyStatus,
    /// Stored response, replayed verbatim for retries with the same key.
    pub response_status: Option<u16>,
    pub response_body: Option<String>,
}

/// One row of the (modelled) `payments` table.
#[derive(Debug, Clone)]
pub struct Payment {
    pub id: String,
    pub order_id: String,
    pub amount: i64,
    pub currency: String,
    /// Only `"succeeded"` exists in this sample.
    pub status: String,
}

#[derive(Debug, Default)]
pub struct Store {
    /// (order_id, key) -> record. Keys are scoped per order in this sample
    /// (in production you would usually scope by account + endpoint too).
    pub idempotency_keys: HashMap<(String, String), IdempotencyRecord>,
    pub payments: Vec<Payment>,
    /// How many times we actually "charged" (called the payment gateway).
    /// Tests assert on this: idempotency means this stays at 1.
    pub charge_count: u64,
    /// Sequence for payment ids (`pay_1`, `pay_2`, ...).
    pub payment_seq: u64,
}

impl Store {
    /// Equivalent of the partial unique index
    /// `... ON payments (order_id) WHERE status = 'succeeded'`:
    /// does this order already have a succeeded payment?
    pub fn order_already_paid(&self, order_id: &str) -> bool {
        self.payments
            .iter()
            .any(|p| p.order_id == order_id && p.status == "succeeded")
    }

    /// Record one charge. This is the side effect we must never do twice
    /// for the same logical payment.
    pub fn charge(&mut self, order_id: &str, amount: i64, currency: &str) -> Payment {
        self.charge_count += 1;
        self.payment_seq += 1;
        let payment = Payment {
            id: format!("pay_{}", self.payment_seq),
            order_id: order_id.to_string(),
            amount,
            currency: currency.to_string(),
            status: "succeeded".to_string(),
        };
        self.payments.push(payment.clone());
        payment
    }
}
