//! Failure tests: Redis goes away mid-conversation; a server instance
//! restarts under its clients. In both, every acknowledged message must
//! reach every client exactly once, live or through sync.

use std::{collections::BTreeMap, sync::Arc, time::Duration};

use protocol::{
    api::{Message, SyncResponse},
    frames::{FrameErrorCode, ServerFrame, close},
    ids::MessageId,
};
use serde_json::json;
use server::config::Secret;
use uuid::Uuid;

use crate::{
    app::TestApp,
    infra::Infra,
    rooms::{User, user},
    ws::{Client, connect},
};

/// What a client knows: messages by ID, from live delivery and sync. A map,
/// so seeing a message twice can't inflate the count; the test separately
/// checks the server never stored a duplicate.
#[derive(Default)]
struct Seen(BTreeMap<MessageId, Message>);

impl Seen {
    fn add(&mut self, m: Message) {
        self.0.insert(m.id, m);
    }

    fn last(&self) -> MessageId {
        *self.0.keys().next_back().expect("saw at least one message")
    }

    fn contents(&self) -> Vec<String> {
        self.0.values().map(|m| m.content.clone()).collect()
    }

    /// Runs the client's sync procedure for one room.
    async fn sync(&mut self, app: &TestApp, who: &User, room: Uuid) {
        let sync: SyncResponse = app
            .post_as(
                who,
                "/api/sync",
                Some(json!({"rooms": [{"room_id": room, "after": self.last()}]})),
            )
            .await
            .json()
            .await
            .unwrap();
        for m in sync.rooms.into_iter().next().unwrap().messages {
            self.add(m);
        }
    }

    /// Drains frames already received.
    async fn absorb(&mut self, client: &mut Client, wait: Duration) {
        let deadline = tokio::time::Instant::now() + wait;
        while tokio::time::Instant::now() < deadline {
            let left = deadline.saturating_duration_since(tokio::time::Instant::now());
            match tokio::time::timeout(left, client.next()).await {
                Ok(ServerFrame::MessageCreated { message }) => self.add(message),
                Ok(_) => {}
                Err(_) => break,
            }
        }
    }
}

/// Sends until acknowledged, retrying with the same key, as the client
/// does. Returns the acked message.
async fn send_until_acked(
    client: &mut Client,
    room: Uuid,
    content: &str,
    give_up: Duration,
) -> Message {
    let key = Uuid::now_v7();
    let deadline = tokio::time::Instant::now() + give_up;
    loop {
        match client.request_send(room, key, content).await {
            ServerFrame::MessageAck { message, .. } => return message,
            ServerFrame::Error { code, .. } if code.is_retryable() => {
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "never acknowledged: {content}"
                );
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            other => panic!("unexpected reply: {other:?}"),
        }
    }
}

async fn stored(app: &TestApp, who: &User, room: Uuid) -> Vec<String> {
    let page: protocol::api::HistoryPage = app
        .get_as(who, &format!("/api/rooms/{room}/messages?limit=100"))
        .await
        .json()
        .await
        .unwrap();
    let mut contents: Vec<String> = page.messages.into_iter().map(|m| m.content).collect();
    contents.reverse();
    contents
}

/// Design 16.2. Redis freezes (paused: connections stay open, nothing
/// answers, like a network partition) while clients on two instances talk.
pub async fn redis_outage_then_resync(infra: Arc<Infra>) {
    let (redis, redis_url) = infra.dedicated_redis().await;
    let point_at_redis =
        |c: &mut server::config::Config| c.redis_url = Secret::new(redis_url.clone());
    let a = TestApp::spawn_with(&infra, point_at_redis).await;
    let b = a.spawn_peer(&infra, point_at_redis).await;
    let ada = user(&a, "ada").await;
    let bob = user(&a, "bob").await;
    let room = a.create_room(&ada, "Outage", "public").await;
    b.join(&bob, &room).await;
    let mut ada_ws = connect(&a, &ada.cookie).await;
    let mut bob_ws = connect(&b, &bob.cookie).await;
    let mut bob_seen = Seen::default();

    let before = ada_ws.send_message(room.id, "before").await;
    bob_seen.add(bob_ws.expect_message("before").await);
    assert_eq!(bob_seen.last(), before.id);

    redis.pause().await.unwrap();
    // No dedupe without Redis, so no sends: the client keeps retrying.
    let during = ada_ws.request_send(room.id, Uuid::now_v7(), "during").await;
    assert!(
        matches!(
            during,
            ServerFrame::Error {
                code: FrameErrorCode::Unavailable,
                ..
            }
        ),
        "{during:?}"
    );
    // Long enough for both instances' pings to fail (5 s interval, 4 s
    // timeout), short of the 14 s worker-ID lease validity.
    tokio::time::sleep(Duration::from_secs(10)).await;
    redis.unpause().await.unwrap();

    // Each instance reconnects, resubscribes, and only then says `resync`.
    for client in [&mut ada_ws, &mut bob_ws] {
        client
            .expect("resync", |f| matches!(f, ServerFrame::Resync { .. }))
            .await;
    }
    let retried = send_until_acked(&mut ada_ws, room.id, "during", Duration::from_secs(20)).await;
    let after = send_until_acked(&mut ada_ws, room.id, "after", Duration::from_secs(20)).await;
    bob_seen.absorb(&mut bob_ws, Duration::from_secs(1)).await;
    bob_seen.sync(&b, &bob, room.id).await;

    assert!(retried.id < after.id);
    assert_eq!(bob_seen.contents(), ["before", "during", "after"]);
    assert_eq!(
        stored(&a, &bob, room.id).await,
        ["before", "during", "after"],
        "stored exactly once each"
    );
}

/// Instance A shuts down under its clients. They reconnect to B, retry
/// anything unacknowledged with the same key, sync, and end up with every
/// message exactly once.
pub async fn instance_restart_loses_nothing(infra: Arc<Infra>) {
    let a = TestApp::spawn(&infra).await;
    let b = a.spawn_peer(&infra, |_| {}).await;
    let ada = user(&a, "ada").await;
    let bob = user(&a, "bob").await;
    let room = a.create_room(&ada, "Restart", "public").await;
    a.join(&bob, &room).await;

    let mut ada_on_a = connect(&a, &ada.cookie).await;
    let mut bob_on_b = connect(&b, &bob.cookie).await;
    let mut ada_seen = Seen::default();

    let first = ada_on_a.send_message(room.id, "first").await;
    ada_seen.add(first.clone());
    // A send whose ack Ada will "miss": she retries it after reconnecting.
    let key = Uuid::now_v7();
    let (unacked, _) = ada_on_a.send_with_key(room.id, key, "maybe lost").await;

    a.stop().await;
    assert_eq!(ada_on_a.closed().await, Some(close::RESTARTING));

    // While Ada is reconnecting, Bob keeps talking on B.
    bob_on_b.send_message(room.id, "while you were away").await;

    // Ada's client reconnects; the load balancer sends her to B.
    let mut ada_on_b = connect(&b, &ada.cookie).await;
    let (retried, duplicate) = ada_on_b.send_with_key(room.id, key, "maybe lost").await;
    assert!(duplicate, "the retry is recognised");
    assert_eq!(retried.id, unacked.id);
    ada_seen.add(retried);
    ada_seen.sync(&b, &ada, room.id).await;
    bob_on_b.send_message(room.id, "welcome back").await;
    ada_seen.add(ada_on_b.expect_message("welcome back").await);

    assert_eq!(
        ada_seen.contents(),
        ["first", "maybe lost", "while you were away", "welcome back"]
    );
    assert_eq!(
        stored(&b, &bob, room.id).await,
        ["first", "maybe lost", "while you were away", "welcome back"]
    );
}
