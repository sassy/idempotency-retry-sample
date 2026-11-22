//! Idempotency & retry learning sample (axum).
//!
//! Two layers of defense against double charging:
//! 1. An `Idempotency-Key` per payment attempt — retries of the same
//!    attempt replay the stored response instead of processing again.
//! 2. An order-level rule — one succeeded payment per order — enforced in
//!    production by a partial unique index (see `schema.postgres.sql`),
//!    which catches the case where a *different* key was generated
//!    (e.g. the user paid again from another browser tab).

pub mod retry;
pub mod store;

use axum::{
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::post,
    Json, Router,
};
use serde::Deserialize;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

use store::{IdempotencyRecord, IdempotencyStatus, Payment, Store};

/// Artificial processing time. Keeping the `processing` state observable
/// for a short window makes the concurrent-request scenario deterministic
/// in tests (a second request reliably arrives while the first is still
/// processing) — and models a real payment gateway call, which is never
/// instantaneous.
pub const PROCESSING_DELAY: Duration = Duration::from_millis(75);

pub type SharedStore = Arc<Mutex<Store>>;

#[derive(Debug, Deserialize)]
pub struct PaymentRequest {
    pub amount: i64,
    #[serde(default = "default_currency")]
    pub currency: String,
    /// Simulation knob for learning. Only `"succeed"` (the default) is
    /// implemented; the field exists so the request shape can grow
    /// (e.g. gateway failures) without changing the API.
    #[serde(default)]
    pub simulate: Option<String>,
}

fn default_currency() -> String {
    "JPY".to_string()
}

/// Fingerprint of the payload an idempotency key was first used with.
/// In production this would typically be a SHA-256 of the canonical
/// request body; a std hasher is enough to demonstrate the idea.
fn request_hash(req: &PaymentRequest) -> u64 {
    let mut hasher = DefaultHasher::new();
    req.amount.hash(&mut hasher);
    req.currency.hash(&mut hasher);
    req.simulate.hash(&mut hasher);
    hasher.finish()
}

fn payment_body(payment: &Payment) -> serde_json::Value {
    serde_json::json!({
        "payment_id": payment.id,
        "order_id": payment.order_id,
        "amount": payment.amount,
        "currency": payment.currency,
        "status": payment.status,
    })
}

fn error_response(status: StatusCode, code: &str, message: &str) -> Response {
    let body = serde_json::json!({ "code": code, "message": message });
    (status, Json(body)).into_response()
}

/// The actual side effect: wait (as a gateway call would), then charge.
/// Callers must have already claimed the right to process.
async fn process_payment(store: &SharedStore, order_id: &str, req: &PaymentRequest) -> Payment {
    tokio::time::sleep(PROCESSING_DELAY).await;
    let mut guard = store.lock().await;
    guard.charge(order_id, req.amount, &req.currency)
}

/// `POST /orders/{order_id}/payments`
///
/// Behaviour, by `Idempotency-Key` header:
/// - Header missing → **unsafe path**: process with no protection.
///   Two identical requests charge twice. This exists on purpose, as the
///   failure reproduction (scenario 1 in the tests / README).
/// - Same key + same payload, already completed → replay stored response.
/// - Same key + same payload, still processing → 409 `request_in_progress`
///   with `Retry-After: 1`; the client should retry with the *same* key.
/// - Same key + different payload → 422
///   `idempotency_key_reused_with_different_payload`.
/// - Different key, but the order already has a succeeded payment →
///   409 `order_already_paid` (the unique-constraint last line of defense).
async fn create_payment(
    State(store): State<SharedStore>,
    Path(order_id): Path<String>,
    headers: HeaderMap,
    Json(req): Json<PaymentRequest>,
) -> Response {
    let key = headers
        .get("idempotency-key")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    // ---- Unsafe path: no key, no protection. ----
    let Some(key) = key else {
        let payment = process_payment(&store, &order_id, &req).await;
        return (StatusCode::CREATED, Json(payment_body(&payment))).into_response();
    };

    let hash = request_hash(&req);
    let scoped_key = (order_id.clone(), key);

    // ---- Claim the key (check-and-insert under one lock). ----
    {
        let mut guard = store.lock().await;
        if let Some(record) = guard.idempotency_keys.get(&scoped_key) {
            if record.request_hash != hash {
                return error_response(
                    StatusCode::UNPROCESSABLE_ENTITY,
                    "idempotency_key_reused_with_different_payload",
                    "This Idempotency-Key was already used with a different request payload.",
                );
            }
            match record.status {
                IdempotencyStatus::Completed => {
                    let status =
                        StatusCode::from_u16(record.response_status.unwrap_or(201)).unwrap();
                    let body: serde_json::Value =
                        serde_json::from_str(record.response_body.as_deref().unwrap_or("{}"))
                            .unwrap_or(serde_json::json!({}));
                    // Replay: return the stored response, do NOT charge again.
                    return (status, Json(body)).into_response();
                }
                IdempotencyStatus::Processing => {
                    let mut res = error_response(
                        StatusCode::CONFLICT,
                        "request_in_progress",
                        "A request with this Idempotency-Key is still being processed. Retry with the same key.",
                    );
                    res.headers_mut()
                        .insert("retry-after", "1".parse().unwrap());
                    return res;
                }
            }
        }
        // Second layer of defense: a different key must not pay twice
        // for the same order.
        if guard.order_already_paid(&order_id) {
            return error_response(
                StatusCode::CONFLICT,
                "order_already_paid",
                "This order already has a succeeded payment.",
            );
        }
        guard.idempotency_keys.insert(
            scoped_key.clone(),
            IdempotencyRecord {
                request_hash: hash,
                status: IdempotencyStatus::Processing,
                response_status: None,
                response_body: None,
            },
        );
    } // lock released while processing, so a concurrent duplicate can
      // observe the `processing` state and get a 409 instead of charging.

    // ---- Process (exactly once for this key). ----
    let payment = process_payment(&store, &order_id, &req).await;
    let body = payment_body(&payment);
    let body_string = body.to_string();

    // ---- Store the response for future replays. ----
    {
        let mut guard = store.lock().await;
        if let Some(record) = guard.idempotency_keys.get_mut(&scoped_key) {
            record.status = IdempotencyStatus::Completed;
            record.response_status = Some(StatusCode::CREATED.as_u16());
            record.response_body = Some(body_string);
        }
    }

    (StatusCode::CREATED, Json(body)).into_response()
}

/// Build the axum app with a fresh, empty store.
pub fn app() -> Router {
    app_with_store(Arc::new(Mutex::new(Store::default())))
}

/// Build the app around a shared store, so tests can assert on
/// `charge_count` afterwards.
pub fn app_with_store(store: SharedStore) -> Router {
    Router::new()
        .route("/orders/{order_id}/payments", post(create_payment))
        .with_state(store)
}
