//! Rate limiting (docs/design.md 11.1), end to end against real Redis.

use std::{sync::Arc, time::Duration};

use protocol::{
    api::ErrorCode,
    frames::{FrameErrorCode, ServerFrame, close},
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

/// A user may belong to at most the room cap (docs/design.md 11.2): once at
/// the cap, neither creating nor joining another room is allowed.
pub async fn room_membership_is_capped(infra: Arc<Infra>) {
    let app = TestApp::spawn_with(&infra, |config| config.max_rooms_per_user = 2).await;
    let ada = user(&app, "ada").await;
    app.create_room(&ada, "r1", "public").await;
    app.create_room(&ada, "r2", "public").await;

    // A third room ada would create or join is refused.
    let created = app
        .post_as(
            &ada,
            "/api/rooms",
            Some(json!({"name": "r3", "kind": "public"})),
        )
        .await;
    expect_error(created, StatusCode::FORBIDDEN, ErrorCode::Forbidden).await;

    let bob = user(&app, "bob").await;
    let bobs_room = app.create_room(&bob, "bobs", "public").await;
    let joined = app
        .post_as(&ada, &format!("/api/rooms/{}/join", bobs_room.id), None)
        .await;
    expect_error(joined, StatusCode::FORBIDDEN, ErrorCode::Forbidden).await;

    app.stop().await;
}

/// A user may hold only so many sockets at once (docs/design.md 11.2): the
/// one over the cap is closed with `4029`, and closing a socket frees a slot
/// for the next one. The cap is keyed by user ID, unique to this test.
pub async fn sockets_are_capped_per_user(infra: Arc<Infra>) {
    let app = TestApp::spawn_with(&infra, |config| config.max_sockets_per_user = 2).await;
    let ada = user(&app, "ada").await;

    let first = ws::connect(&app, &ada.cookie).await;
    let _second = ws::connect(&app, &ada.cookie).await;
    let refused = ws::connect_capped(&app, &ada.cookie, None).await;
    assert_eq!(refused.err(), Some(close::TOO_MANY), "third socket refused");

    // Closing one frees a slot; the server releases it when it sees the close.
    drop(first);
    let mut admitted = false;
    for _ in 0..40 {
        if ws::connect_capped(&app, &ada.cookie, None).await.is_ok() {
            admitted = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(admitted, "a freed slot admits a new socket");

    app.stop().await;
}

/// Sockets are also capped per client IP, across users. Keyed by a fixed test
/// IP (via `x-test-ip`) so the bucket is this test's own.
pub async fn sockets_are_capped_per_ip(infra: Arc<Infra>) {
    let app = TestApp::spawn_with(&infra, |config| {
        config.client_ip_header = Some("x-test-ip".into());
        config.max_sockets_per_ip = 2;
    })
    .await;
    let ip = format!("198.51.100.{}", Uuid::now_v7().as_bytes()[15]);
    let ada = user(&app, "ada").await;
    let bob = user(&app, "bob").await;

    let _a = ws::connect_capped(&app, &ada.cookie, Some(&ip))
        .await
        .expect("first socket on this IP");
    let _b = ws::connect_capped(&app, &bob.cookie, Some(&ip))
        .await
        .expect("second socket on this IP");
    // A third from the same IP, even as a different user, is refused.
    let refused = ws::connect_capped(&app, &ada.cookie, Some(&ip)).await;
    assert_eq!(refused.err(), Some(close::TOO_MANY), "third socket refused");

    app.stop().await;
}
