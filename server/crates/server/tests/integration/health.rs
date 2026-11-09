use std::sync::Arc;

use protocol::api::ErrorCode;
use reqwest::StatusCode;
use serde_json::{Value, json};

use crate::{
    app::{TestApp, expect_error},
    infra::Infra,
};

pub async fn healthz_is_always_ok(infra: Arc<Infra>) {
    let app = TestApp::spawn(&infra).await;
    let resp = app.get("/healthz").send().await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(resp.text().await.unwrap(), "ok");
}

pub async fn readyz_reports_dependencies(infra: Arc<Infra>) {
    let app = TestApp::spawn(&infra).await;
    let resp = app.get("/readyz").send().await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(
        body,
        json!({"postgres": true, "redis": true, "scylla": true, "ids": true, "shutting_down": false})
    );
}

/// Postgres connects lazily, so the server starts with a dead database URL
/// and must report itself not ready instead of failing to boot.
pub async fn readyz_fails_when_postgres_is_down(infra: Arc<Infra>) {
    let app = TestApp::spawn_with(&infra, |config| {
        // Port 1 on loopback: nothing listens there.
        config.database_url = server::config::Secret::new("postgres://chat:chat@127.0.0.1:1/chat");
    })
    .await;
    let resp = app.get("/readyz").send().await.unwrap();
    assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["postgres"], false);
    assert_eq!(body["redis"], true);
    assert_eq!(body["scylla"], true);

    // And a request that needs Postgres is a retryable `unavailable`.
    let resp = app.login("someone@example.com", "whatever123").await;
    expect_error(
        resp,
        StatusCode::SERVICE_UNAVAILABLE,
        ErrorCode::Unavailable,
    )
    .await;
}

pub async fn request_ids_are_assigned_and_propagated(infra: Arc<Infra>) {
    let app = TestApp::spawn(&infra).await;
    let resp = app.get("/healthz").send().await.unwrap();
    let assigned = resp.headers()["x-request-id"].to_str().unwrap();
    assert_eq!(assigned.len(), 36, "a UUID, got {assigned}");

    let resp = app
        .get("/healthz")
        .header("x-request-id", "from-the-proxy")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.headers()["x-request-id"], "from-the-proxy");
}

pub async fn unknown_api_routes_are_json_404(infra: Arc<Infra>) {
    let app = TestApp::spawn(&infra).await;
    let resp = app.get("/api/nope").send().await.unwrap();
    expect_error(resp, StatusCode::NOT_FOUND, ErrorCode::NotFound).await;
    // POSTs too: an unknown route is a 404, not an origin rejection.
    let resp = app.client.post(app.url("/api/nope")).send().await.unwrap();
    expect_error(resp, StatusCode::NOT_FOUND, ErrorCode::NotFound).await;
}

/// The value of an unlabelled metric line (`name value`), or 0 if absent.
fn metric_value(body: &str, name: &str) -> f64 {
    body.lines()
        .find(|l| !l.starts_with('#') && l.starts_with(name))
        .and_then(|l| l.split_whitespace().last())
        .and_then(|v| v.parse().ok())
        .unwrap_or(0.0)
}

/// `/metrics` renders Prometheus text, and the counters move with real
/// activity (docs/design.md 13). The recorder is process-wide, shared by every
/// concurrent test, so this asserts lower bounds its own activity guarantees.
pub async fn metrics_report_activity(infra: Arc<Infra>) {
    let app = TestApp::spawn(&infra).await;
    let ada = crate::rooms::user(&app, "ada").await;
    let room = app.create_room(&ada, "Metrics", "public").await;
    let mut socket = crate::ws::connect(&app, &ada.cookie).await;
    socket.send_message(room.id, "counted").await;

    let body = app
        .get("/metrics")
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(
        body.contains("ws_connections_open"),
        "the gauge is registered:\n{body}"
    );
    assert!(
        metric_value(&body, "ws_connections_open") >= 1.0,
        "this test's socket is counted open"
    );
    assert!(
        metric_value(&body, "messages_sent_total") >= 1.0,
        "this test's message is counted sent"
    );

    app.stop().await;
}
