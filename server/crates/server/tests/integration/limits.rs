//! Rate limiting (docs/design.md 11.1), end to end against real Redis.

use std::sync::Arc;

use protocol::{
    api::ErrorCode,
    frames::{FrameErrorCode, ServerFrame},
};
use reqwest::StatusCode;
use serde_json::json;
use uuid::Uuid;

use crate::{
    app::{TestApp, expect_error},
    infra::Infra,
    rooms::user,
    ws,
};

/// Sign-up is limited per client IP: after the burst, the next attempt is a
/// 429, not another account. Keyed by a fixed test IP so this test's bucket
/// is its own (every other test app is the TCP peer, 127.0.0.1).
pub async fn signup_is_rate_limited_per_ip(infra: Arc<Infra>) {
    let app = TestApp::spawn_with(&infra, |config| {
        config.client_ip_header = Some("x-test-ip".into());
        config.rate_limits.signup_ip = server::rate_limit::Bucket::per_second(1.0, 0.001);
    })
    .await;
    let ip = format!("198.51.100.{}", Uuid::now_v7().as_bytes()[15]);

    let first = app
        .post("/api/auth/signup")
        .header("x-test-ip", &ip)
        .json(&json!({
            "email": "a@example.com", "password": "correct horse battery",
            "handle": "limitone", "display_name": "One",
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(first.status(), StatusCode::CREATED, "first sign-up");

    let second = app
        .post("/api/auth/signup")
        .header("x-test-ip", &ip)
        .json(&json!({
            "email": "b@example.com", "password": "correct horse battery",
            "handle": "limittwo", "display_name": "Two",
        }))
        .send()
        .await
        .unwrap();
    expect_error(
        second,
        StatusCode::TOO_MANY_REQUESTS,
        ErrorCode::TooManyRequests,
    )
    .await;

    app.stop().await;
}

/// Sends are limited per user: after the burst the next send is refused with a
/// `rate_limited` error frame. The per-user bucket is keyed by the user ID, so
/// shrinking it doesn't disturb other tests sharing this Redis.
pub async fn sends_are_rate_limited_per_user(infra: Arc<Infra>) {
    let app = TestApp::spawn_with(&infra, |config| {
        config.rate_limits.send_user = server::rate_limit::Bucket::per_second(2.0, 0.001);
    })
    .await;
    let ada = user(&app, "ada").await;
    let room = app.create_room(&ada, "General", "public").await;
    let mut socket = ws::connect(&app, &ada.cookie).await;

    for content in ["one", "two"] {
        let reply = socket.request_send(room.id, Uuid::now_v7(), content).await;
        assert!(
            matches!(reply, ServerFrame::MessageAck { .. }),
            "{content} should be accepted, got {reply:?}"
        );
    }
    let third = socket.request_send(room.id, Uuid::now_v7(), "three").await;
    match third {
        ServerFrame::Error { code, .. } => assert_eq!(code, FrameErrorCode::RateLimited),
        other => panic!("expected a rate_limited error, got {other:?}"),
    }

    app.stop().await;
}
