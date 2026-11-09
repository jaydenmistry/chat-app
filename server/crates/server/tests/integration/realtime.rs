//! The WebSocket gateway, mostly with two server instances, as in
//! production: a user on instance A and a user on instance B.

use std::{sync::Arc, time::Duration};

use protocol::{
    api::{HistoryPage, SyncResponse},
    frames::{
        ClientFrame, FrameErrorCode, LeaveReason, PresenceState, PresenceStatus, SUBPROTOCOL,
        ServerFrame, close,
    },
};
use reqwest::StatusCode;
use serde_json::json;
use uuid::Uuid;

use crate::{
    app::TestApp,
    infra::{Infra, TEST_ORIGIN},
    rooms::{User, user},
    ws::{self, Refused, connect, connect_with},
};

async fn history(app: &TestApp, who: &User, room: Uuid) -> HistoryPage {
    app.get_as(who, &format!("/api/rooms/{room}/messages"))
        .await
        .json()
        .await
        .unwrap()
}

pub async fn upgrade_requires_session_origin_and_version(infra: Arc<Infra>) {
    let app = TestApp::spawn(&infra).await;
    let ada = user(&app, "ada").await;
    let c = Some(ada.cookie.as_str());

    let refused = |r: Result<ws::Client, Refused>| r.err().map(|Refused(status)| status);
    assert_eq!(
        refused(connect_with(&app, None, Some(TEST_ORIGIN), Some(SUBPROTOCOL)).await),
        Some(StatusCode::UNAUTHORIZED)
    );
    assert_eq!(
        refused(connect_with(&app, c, Some("https://evil.example"), Some(SUBPROTOCOL)).await),
        Some(StatusCode::FORBIDDEN),
        "cross-site WebSocket hijacking"
    );
    assert_eq!(
        refused(connect_with(&app, c, None, Some(SUBPROTOCOL)).await),
        Some(StatusCode::FORBIDDEN)
    );
    assert_eq!(
        refused(connect_with(&app, c, Some(TEST_ORIGIN), None).await),
        Some(StatusCode::BAD_REQUEST)
    );
    assert_eq!(
        refused(connect_with(&app, c, Some(TEST_ORIGIN), Some("chat.v0")).await),
        Some(StatusCode::BAD_REQUEST)
    );
    let client = connect(&app, &ada.cookie).await;
    assert_eq!(client.user_id, ada.me.id);
}

/// The core requirement: a message sent to instance A reaches a client on
/// instance B. The sender gets its ack before its own copy.
pub async fn messages_cross_instances(infra: Arc<Infra>) {
    let a = TestApp::spawn(&infra).await;
    let b = a.spawn_peer(&infra, |_| {}).await;
    let ada = user(&a, "ada").await;
    let bob = user(&a, "bob").await;
    let room = a.create_room(&ada, "General", "public").await;
    b.join(&bob, &room).await;

    let mut ada_ws = connect(&a, &ada.cookie).await;
    let mut bob_ws = connect(&b, &bob.cookie).await;

    let sent = ada_ws.send_message(room.id, "hello from A").await;
    assert_eq!(sent.author_id, ada.me.id);
    let own_copy = ada_ws.expect_message("hello from A").await;
    assert_eq!(own_copy.id, sent.id);
    let received = bob_ws.expect_message("hello from A").await;
    assert_eq!(received, sent);

    let reply = bob_ws.send_message(room.id, "hi from B").await;
    assert!(reply.id > sent.id);
    ada_ws.expect_message("hi from B").await;

    // Both instances serve the same history.
    for app in [&a, &b] {
        let page = history(app, &bob, room.id).await;
        let contents: Vec<_> = page.messages.iter().map(|m| m.content.as_str()).collect();
        assert_eq!(contents, ["hi from B", "hello from A"]);
    }
}

/// Design 16.3: the client retries after a timeout, but the first attempt
/// had succeeded. Same key: same message, stored once, `duplicate: true`.
pub async fn retries_return_the_original_message(infra: Arc<Infra>) {
    let a = TestApp::spawn(&infra).await;
    let b = a.spawn_peer(&infra, |_| {}).await;
    let ada = user(&a, "ada").await;
    let room = a.create_room(&ada, "General", "public").await;
    let mut on_a = connect(&a, &ada.cookie).await;
    let mut on_b = connect(&b, &ada.cookie).await;

    let key = Uuid::now_v7();
    let (first, duplicate) = on_a.send_with_key(room.id, key, "only once").await;
    assert!(!duplicate);
    // The retry lands on the other instance, as it would after a reconnect.
    let (again, duplicate) = on_b.send_with_key(room.id, key, "only once").await;
    assert!(duplicate);
    assert_eq!(again, first);

    // Two attempts racing on two instances: still one message.
    let racing = Uuid::now_v7();
    let (x, y) = tokio::join!(
        on_a.send_with_key(room.id, racing, "raced"),
        on_b.send_with_key(room.id, racing, "raced"),
    );
    assert_eq!(x.0.id, y.0.id);
    assert!(x.1 != y.1, "exactly one of them is the duplicate");

    let page = history(&a, &ada, room.id).await;
    let contents: Vec<_> = page.messages.iter().map(|m| m.content.as_str()).collect();
    assert_eq!(contents, ["raced", "only once"], "no duplicates stored");

    // The same key for different content is refused, not merged.
    let conflict = on_a.request_send(room.id, key, "something else").await;
    assert!(
        matches!(
            conflict,
            ServerFrame::Error {
                code: FrameErrorCode::IdempotencyConflict,
                ..
            }
        ),
        "{conflict:?}"
    );
}

pub async fn bad_sends_get_error_frames(infra: Arc<Infra>) {
    let app = TestApp::spawn(&infra).await;
    let ada = user(&app, "ada").await;
    let mallory = user(&app, "mallory").await;
    let room = app.create_room(&ada, "Private", "private").await;
    let mut ws = connect(&app, &ada.cookie).await;
    let mut intruder = connect(&app, &mallory.cookie).await;

    let code = |frame: ServerFrame| match frame {
        ServerFrame::Error { code, .. } => code,
        other => panic!("expected an error, got {other:?}"),
    };
    assert_eq!(
        code(ws.request_send(room.id, Uuid::now_v7(), "   ").await),
        FrameErrorCode::InvalidContent
    );
    assert_eq!(
        code(
            ws.request_send(room.id, Uuid::now_v7(), &"x".repeat(4097))
                .await
        ),
        FrameErrorCode::InvalidContent
    );
    assert_eq!(
        code(
            intruder
                .request_send(room.id, Uuid::now_v7(), "let me in")
                .await
        ),
        FrameErrorCode::Forbidden
    );
    ws.send_raw(r#"{"type":"message.explode"}"#).await;
    let bad = ws
        .expect("bad_frame", |f| {
            matches!(
                f,
                ServerFrame::Error {
                    code: FrameErrorCode::BadFrame,
                    ..
                }
            )
        })
        .await;
    assert!(matches!(bad, ServerFrame::Error { req_id: None, .. }));
    // The connection survives all of that.
    ws.send_message(room.id, "still here").await;
}

/// Design 16.4: a user is removed while they have sockets on two instances.
pub async fn removal_stops_delivery_on_every_instance(infra: Arc<Infra>) {
    let a = TestApp::spawn(&infra).await;
    let b = a.spawn_peer(&infra, |_| {}).await;
    let ada = user(&a, "ada").await;
    let bob = user(&a, "bob").await;
    let room = a.create_room(&ada, "Team", "public").await;
    a.join(&bob, &room).await;

    let mut ada_ws = connect(&a, &ada.cookie).await;
    let mut bob_a = connect(&a, &bob.cookie).await;
    let mut bob_b = connect(&b, &bob.cookie).await;
    ada_ws.send_message(room.id, "before").await;
    bob_a.expect_message("before").await;
    bob_b.expect_message("before").await;

    let resp = b
        .delete_as(
            &ada,
            &format!("/api/rooms/{}/members/{}", room.id, bob.me.id),
        )
        .await;
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);
    for bob_ws in [&mut bob_a, &mut bob_b] {
        let left = bob_ws
            .expect("room.left", |f| matches!(f, ServerFrame::RoomLeft { .. }))
            .await;
        assert_eq!(
            left,
            ServerFrame::RoomLeft {
                room_id: room.id,
                reason: LeaveReason::Removed
            }
        );
    }

    ada_ws.send_message(room.id, "after").await;
    ada_ws.expect_message("after").await;
    for bob_ws in [&mut bob_a, &mut bob_b] {
        bob_ws
            .expect_none("a message after removal", Duration::from_millis(500), |f| {
                matches!(f, ServerFrame::MessageCreated { .. })
            })
            .await;
    }
    // And Bob can no longer post, on either instance.
    for bob_ws in [&mut bob_a, &mut bob_b] {
        let reply = bob_ws.request_send(room.id, Uuid::now_v7(), "sneaky").await;
        assert!(matches!(
            reply,
            ServerFrame::Error {
                code: FrameErrorCode::Forbidden,
                ..
            }
        ));
    }
}

/// Joining (on another instance, by REST) routes the room to the user's
/// open sockets, and `room.joined` arrives only once delivery is live.
pub async fn joining_routes_the_room_to_open_sockets(infra: Arc<Infra>) {
    let a = TestApp::spawn(&infra).await;
    let b = a.spawn_peer(&infra, |_| {}).await;
    let ada = user(&a, "ada").await;
    let bob = user(&a, "bob").await;
    let room = a.create_room(&ada, "Later", "public").await;
    let mut ada_ws = connect(&a, &ada.cookie).await;
    let mut bob_ws = connect(&b, &bob.cookie).await;

    a.join(&bob, &room).await;
    let joined = bob_ws
        .expect("room.joined", |f| {
            matches!(f, ServerFrame::RoomJoined { .. })
        })
        .await;
    assert!(matches!(joined, ServerFrame::RoomJoined { room: r } if r.id == room.id));
    ada_ws.send_message(room.id, "welcome").await;
    bob_ws.expect_message("welcome").await;

    // A DM opened by Ada appears on Bob's socket too.
    let dm: protocol::api::RoomInfo = a
        .post_as(&ada, "/api/dms", Some(json!({"handle": "bob"})))
        .await
        .json()
        .await
        .unwrap();
    bob_ws
        .expect(
            "the new DM",
            |f| matches!(f, ServerFrame::RoomJoined { room } if room.id == dm.id),
        )
        .await;
    ada_ws.send_message(dm.id, "psst").await;
    bob_ws.expect_message("psst").await;
}

/// Logging out (or revoking a session) closes that session's sockets on
/// every instance, and only that session's.
pub async fn logout_closes_sockets_everywhere(infra: Arc<Infra>) {
    let a = TestApp::spawn(&infra).await;
    let b = a.spawn_peer(&infra, |_| {}).await;
    let bob = user(&a, "bob").await;
    let other_session =
        crate::app::session_cookie(&a.login("bob@example.com", "correct horse battery").await)
            .unwrap();

    let mut on_a = connect(&a, &bob.cookie).await;
    let mut on_b = connect(&b, &bob.cookie).await;
    let mut other = connect(&b, &other_session).await;

    let resp = a
        .post("/api/auth/logout")
        .header(reqwest::header::COOKIE, &bob.cookie)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);
    assert_eq!(on_a.closed().await, Some(close::SESSION_ENDED));
    assert_eq!(on_b.closed().await, Some(close::SESSION_ENDED));
    other
        .expect_none("a close", Duration::from_millis(500), |_| false)
        .await;
    // The logged-out cookie can't reconnect.
    assert!(matches!(
        connect_with(&b, Some(&bob.cookie), Some(TEST_ORIGIN), Some(SUBPROTOCOL)).await,
        Err(Refused(StatusCode::UNAUTHORIZED))
    ));
}

/// A client that was away syncs what it missed, with an overlap it
/// deduplicates.
pub async fn sync_returns_missed_messages(infra: Arc<Infra>) {
    let app = TestApp::spawn(&infra).await;
    let ada = user(&app, "ada").await;
    let bob = user(&app, "bob").await;
    let room = app.create_room(&ada, "General", "public").await;
    app.join(&bob, &room).await;
    let mut ada_ws = connect(&app, &ada.cookie).await;
    let seen = ada_ws.send_message(room.id, "seen").await;
    for n in 0..3 {
        ada_ws.send_message(room.id, &format!("missed {n}")).await;
    }

    let sync: SyncResponse = app
        .post_as(
            &bob,
            "/api/sync",
            Some(json!({"rooms": [{"room_id": room.id, "after": seen.id}]})),
        )
        .await
        .json()
        .await
        .unwrap();
    let synced = &sync.rooms[0];
    assert!(synced.member && synced.complete);
    let contents: Vec<_> = synced.messages.iter().map(|m| m.content.as_str()).collect();
    // "seen" comes back too: the overlap window reaches back before the
    // cursor. The client drops it by ID.
    assert_eq!(contents, ["seen", "missed 0", "missed 1", "missed 2"]);
}

/// Design 16.1: instance A's clock runs 300 ms behind B's. A message sent
/// on A *after* one sent on B gets a smaller ID. A client that saw B's
/// message and then resumes from it must still get A's.
pub async fn resume_survives_clock_skew(infra: Arc<Infra>) {
    let a = TestApp::spawn(&infra).await;
    let b = a.spawn_peer(&infra, |c| c.clock_offset_ms = -300).await;
    let ada = user(&a, "ada").await;
    let bob = user(&a, "bob").await;
    let room = a.create_room(&ada, "Skew", "public").await;
    a.join(&bob, &room).await;
    let mut on_a = connect(&a, &ada.cookie).await;
    let mut behind = connect(&b, &ada.cookie).await;

    let later_id = on_a.send_message(room.id, "first, from A").await;
    let earlier_id = behind.send_message(room.id, "second, from B").await;
    assert!(
        earlier_id.id < later_id.id,
        "the test needs the skew to invert the IDs ({} vs {})",
        earlier_id.id,
        later_id.id
    );

    // Bob saw "first, from A" (the highest ID so far) and disconnected.
    let sync: SyncResponse = a
        .post_as(
            &bob,
            "/api/sync",
            Some(json!({"rooms": [{"room_id": room.id, "after": later_id.id}]})),
        )
        .await
        .json()
        .await
        .unwrap();
    let contents: Vec<_> = sync.rooms[0]
        .messages
        .iter()
        .map(|m| m.content.as_str())
        .collect();
    assert!(
        contents.contains(&"second, from B"),
        "message lost on resume: {contents:?}"
    );
}

pub async fn typing_crosses_instances(infra: Arc<Infra>) {
    let a = TestApp::spawn(&infra).await;
    let b = a.spawn_peer(&infra, |_| {}).await;
    let ada = user(&a, "ada").await;
    let bob = user(&a, "bob").await;
    let mallory = user(&a, "mallory").await;
    let room = a.create_room(&ada, "General", "public").await;
    a.join(&bob, &room).await;
    let mut ada_ws = connect(&a, &ada.cookie).await;
    let mut bob_ws = connect(&b, &bob.cookie).await;
    let mut outsider = connect(&a, &mallory.cookie).await;

    ada_ws.send(&ClientFrame::Typing { room_id: room.id }).await;
    let typing = bob_ws
        .expect("typing", |f| matches!(f, ServerFrame::Typing { .. }))
        .await;
    assert_eq!(
        typing,
        ServerFrame::Typing {
            room_id: room.id,
            user_id: ada.me.id
        }
    );
    // A second one right away is dropped (at most one per 2 s).
    ada_ws.send(&ClientFrame::Typing { room_id: room.id }).await;
    bob_ws
        .expect_none("a second typing event", Duration::from_millis(500), |f| {
            matches!(f, ServerFrame::Typing { .. })
        })
        .await;
    // A non-member's typing goes nowhere.
    outsider
        .send(&ClientFrame::Typing { room_id: room.id })
        .await;
    bob_ws
        .expect_none(
            "typing from a non-member",
            Duration::from_millis(500),
            |f| matches!(f, ServerFrame::Typing { user_id, .. } if *user_id == mallory.me.id),
        )
        .await;
}

pub async fn presence_crosses_instances(infra: Arc<Infra>) {
    let a = TestApp::spawn(&infra).await;
    let b = a.spawn_peer(&infra, |_| {}).await;
    let ada = user(&a, "ada").await;
    let bob = user(&a, "bob").await;
    let mut bob_ws = connect(&b, &bob.cookie).await;
    let status_of = |frame: ServerFrame| match frame {
        ServerFrame::Presence { status, .. } => status,
        other => panic!("expected presence, got {other:?}"),
    };
    let is_ada = |f: &ServerFrame| matches!(f, ServerFrame::Presence { user_id, .. } if *user_id == ada.me.id);

    bob_ws
        .send(&ClientFrame::PresenceWatch {
            user_ids: vec![ada.me.id],
        })
        .await;
    assert_eq!(
        status_of(bob_ws.expect("ada's status", is_ada).await),
        PresenceStatus::Offline
    );

    let mut ada_ws = connect(&a, &ada.cookie).await;
    assert_eq!(
        status_of(bob_ws.expect("online", is_ada).await),
        PresenceStatus::Online
    );

    ada_ws
        .send(&ClientFrame::PresenceSet {
            state: PresenceState::Idle,
        })
        .await;
    assert_eq!(
        status_of(bob_ws.expect("away", is_ada).await),
        PresenceStatus::Away
    );

    // A second, active tab on the other instance makes her online again;
    // closing it returns her to away; closing the last makes her offline.
    let second = connect(&b, &ada.cookie).await;
    assert_eq!(
        status_of(bob_ws.expect("online again", is_ada).await),
        PresenceStatus::Online
    );
    drop(second);
    assert_eq!(
        status_of(bob_ws.expect("away again", is_ada).await),
        PresenceStatus::Away
    );
    drop(ada_ws);
    assert_eq!(
        status_of(bob_ws.expect("offline", is_ada).await),
        PresenceStatus::Offline
    );
}

/// Design 16.6: a guest's account expires while they're connected. The
/// socket closes at that moment and their messages expire with them.
pub async fn guest_expiry_ends_the_socket_and_the_messages(infra: Arc<Infra>) {
    let app = TestApp::spawn_with(&infra, |c| c.guest_ttl = Duration::from_secs(3)).await;
    let ada = user(&app, "ada").await;
    let room = app.create_room(&ada, "Lobby", "public").await;
    let guest = crate::rooms::guest(&app).await;
    app.join(&guest, &room).await;
    let mut ws = connect(&app, &guest.cookie).await;
    ws.send_message(room.id, "here for a short time").await;
    assert_eq!(history(&app, &ada, room.id).await.messages.len(), 1);

    assert_eq!(ws.closed().await, Some(close::SESSION_ENDED));
    // The message had a TTL ending at the same instant.
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(history(&app, &ada, room.id).await.messages.is_empty());
}

/// Design 13: on graceful shutdown the instance tells each socket to reconnect
/// after a spread-out delay, then closes it with 1012, so clients move to
/// another instance without all hitting it at once.
pub async fn graceful_shutdown_tells_clients_to_reconnect(infra: Arc<Infra>) {
    let app = TestApp::spawn(&infra).await;
    let ada = user(&app, "ada").await;
    let mut socket = connect(&app, &ada.cookie).await;

    app.stop().await;

    let frame = socket
        .expect("reconnect", |f| matches!(f, ServerFrame::Reconnect { .. }))
        .await;
    let ServerFrame::Reconnect { retry_after_ms } = frame else {
        unreachable!()
    };
    assert!(
        retry_after_ms <= 3_000,
        "retry is spread over a few seconds"
    );
    assert_eq!(socket.closed().await, Some(close::RESTARTING));
}
