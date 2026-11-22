use idempotency_retry_sample::app;

#[tokio::main]
async fn main() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000")
        .await
        .expect("bind 127.0.0.1:3000");
    println!("listening on http://{}", listener.local_addr().unwrap());
    println!("try: curl -X POST http://127.0.0.1:3000/orders/order_1/payments \\");
    println!("       -H 'Content-Type: application/json' \\");
    println!("       -H 'Idempotency-Key: attempt_1' \\");
    println!("       -d '{{\"amount\": 1000, \"currency\": \"JPY\"}}'");
    axum::serve(listener, app()).await.expect("serve");
}
