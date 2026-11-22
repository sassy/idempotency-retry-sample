//! Integration tests for the four scenarios in the README.
//!
//! Requests are sent in-process with tower's `oneshot`, so `cargo test`
//! needs no running server and no database.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use idempotency_retry_sample::{app_with_store, store::Store, SharedStore};
use std::sync::Arc;
use tokio::sync::Mutex;
use tower::ServiceExt;

fn fresh_app() -> (axum::Router, SharedStore) {
    let store: SharedStore = Arc::new(Mutex::new(Store::default()));
    (app_with_store(store.clone()), store)
}

fn payment_request(order_id: &str, key: Option<&str>) -> Request<Body> {
    let mut builder = Request::builder()
        .method("POST")
        .uri(format!("/orders/{order_id}/payments"))
        .header("content-type", "application/json");
    if let Some(key) = key {
        builder = builder.header("idempotency-key", key);
    }
    builder
        .body(Body::from(r#"{"amount": 1000, "currency": "JPY"}"#))
        .unwrap()
}

async fn body_json(response: axum::response::Response) -> serde_json::Value {
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

async fn charge_count(store: &SharedStore) -> u64 {
    store.lock().await.charge_count
}

/// Scenario 1 (failure reproduction): without an Idempotency-Key there is
/// no protection — the same request sent twice charges twice.
#[tokio::test]
async fn no_key_double_request_charges_twice() {
    let (app, store) = fresh_app();

    let res1 = app
        .clone()
        .oneshot(payment_request("order_1", None))
        .await
        .unwrap();
    assert_eq!(res1.status(), StatusCode::CREATED);
    let res2 = app.oneshot(payment_request("order_1", None)).await.unwrap();
    assert_eq!(res2.status(), StatusCode::CREATED);

    assert_eq!(
        charge_count(&store).await,
        2,
        "without a key, a double submit means a double charge"
    );
}

/// Scenario 2: retrying with the same key (e.g. the first response was
/// lost in the network) replays the stored response and charges once.
/// Both responses must be identical — including the payment id.
#[tokio::test]
async fn same_key_retry_charges_once_and_replays_response() {
    let (app, store) = fresh_app();

    let res1 = app
        .clone()
        .oneshot(payment_request("order_1", Some("attempt_1")))
        .await
        .unwrap();
    assert_eq!(res1.status(), StatusCode::CREATED);
    let body1 = body_json(res1).await;

    // The client "drops" body1 (response lost) and retries with the same key.
    let res2 = app
        .oneshot(payment_request("order_1", Some("attempt_1")))
        .await
        .unwrap();
    assert_eq!(res2.status(), StatusCode::CREATED);
    let body2 = body_json(res2).await;

    assert_eq!(body1, body2, "retry must replay the stored response");
    assert_eq!(charge_count(&store).await, 1);
}

/// Scenario 3: two requests with the same key in parallel. Exactly one
/// charge may happen; the other request gets the replayed response or a
/// 409 `request_in_progress`, but never a second charge.
#[tokio::test]
async fn concurrent_same_key_charges_once() {
    let (app, store) = fresh_app();

    let app2 = app.clone();
    let (res1, res2) = tokio::join!(
        app.oneshot(payment_request("order_1", Some("attempt_1"))),
        app2.oneshot(payment_request("order_1", Some("attempt_1"))),
    );
    let statuses = [res1.unwrap().status(), res2.unwrap().status()];
    for status in statuses {
        assert!(
            status == StatusCode::CREATED || status == StatusCode::CONFLICT,
            "unexpected status: {status}"
        );
    }
    assert!(
        statuses.contains(&StatusCode::CREATED),
        "one of the requests must succeed"
    );
    assert_eq!(
        charge_count(&store).await,
        1,
        "concurrent duplicates must never charge twice"
    );
}

/// Scenario 4: a *different* key for an order that is already paid
/// (e.g. the user opened another tab and a new key was generated).
/// The order-level rule rejects it: 409 `order_already_paid`, one charge.
#[tokio::test]
async fn different_key_for_paid_order_is_rejected() {
    let (app, store) = fresh_app();

    let res1 = app
        .clone()
        .oneshot(payment_request("order_1", Some("attempt_1")))
        .await
        .unwrap();
    assert_eq!(res1.status(), StatusCode::CREATED);

    let res2 = app
        .oneshot(payment_request("order_1", Some("attempt_2")))
        .await
        .unwrap();
    assert_eq!(res2.status(), StatusCode::CONFLICT);
    let body = body_json(res2).await;
    assert_eq!(body["code"], "order_already_paid");

    assert_eq!(charge_count(&store).await, 1);
}

/// Bonus check of a documented behaviour: reusing a key with a different
/// payload is a client bug and returns 422.
#[tokio::test]
async fn same_key_different_payload_is_422() {
    let (app, _store) = fresh_app();

    let res1 = app
        .clone()
        .oneshot(payment_request("order_1", Some("attempt_1")))
        .await
        .unwrap();
    assert_eq!(res1.status(), StatusCode::CREATED);

    let different = Request::builder()
        .method("POST")
        .uri("/orders/order_1/payments")
        .header("content-type", "application/json")
        .header("idempotency-key", "attempt_1")
        .body(Body::from(r#"{"amount": 2000, "currency": "JPY"}"#))
        .unwrap();
    let res2 = app.oneshot(different).await.unwrap();
    assert_eq!(res2.status(), StatusCode::UNPROCESSABLE_ENTITY);
    let body = body_json(res2).await;
    assert_eq!(
        body["code"],
        "idempotency_key_reused_with_different_payload"
    );
}
