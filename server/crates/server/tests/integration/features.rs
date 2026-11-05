//! Phase 3 features over the real stack: edits, deletes, reactions, the
//! change feed, read state and receipts, membership frames. Most tests use
//! two instances, as in production.

use std::{sync::Arc, time::Duration};

use protocol::{
    api::{HistoryPage, Message, Receipts, RoomInfo, SearchResults, SyncResponse},
    frames::{ClientFrame, FrameErrorCode, ServerFrame},
    ids::MessageId,
};
use serde_json::json;
use server::{
    config::{Config, Secret},
    realtime::send::{claim_value, fingerprint},
};
use uuid::Uuid;

use crate::{
    app::TestApp,
    infra::Infra,
    rooms::{User, user},
    ws::{Client, connect},
};

async fn history(app: &TestApp, who: &User, room: Uuid) -> HistoryPage {
    app.get_as(who, &format!("/api/rooms/{room}/messages"))
        .await
        .json()
        .await
        .unwrap()
}

async fn stored(app: &TestApp, who: &User, room: Uuid, id: MessageId) -> Message {
    history(app, who, room)
        .await
        .messages
        .into_iter()
        .find(|m| m.id == id)
        .expect("message in history")
}

async fn edit(ws: &mut Client, room_id: Uuid, message_id: MessageId, content: &str) -> ServerFrame {
    ws.request(|req_id| ClientFrame::MessageEdit {
        req_id,
        room_id,
        message_id,
        content: content.into(),
    })
    .await
}

async fn delete(ws: &mut Client, room_id: Uuid, message_id: MessageId) -> ServerFrame {
    ws.request(|req_id| ClientFrame::MessageDelete {
        req_id,
        room_id,
        message_id,
    })
    .await
}

async fn react(
    ws: &mut Client,
    room_id: Uuid,
    message_id: MessageId,
    emoji: &str,
    active: bool,
) -> ServerFrame {
    ws.request(|req_id| ClientFrame::ReactionSet {
        req_id,
        room_id,
        message_id,
        emoji: emoji.into(),
        active,
    })
    .await
}

fn is_ok(frame: &ServerFrame) -> bool {
    matches!(frame, ServerFrame::Ok { .. })
}

fn error_code(frame: &ServerFrame) -> Option<FrameErrorCode> {
    match frame {
        ServerFrame::Error { code, .. } => Some(*code),
        _ => None,
    }
}

async fn sync(app: &TestApp, who: &User, cursor: serde_json::Value) -> SyncResponse {
    app.post_as(who, "/api/sync", Some(json!({ "rooms": [cursor] })))
        .await
        .json()
        .await
        .unwrap()
}

/// Edit and delete reach the other instance; only the author may do either;
/// a deleted message stays deleted and shows no content anywhere.
pub async fn edits_and_deletes_reach_everyone(infra: Arc<Infra>) {
    let a = TestApp::spawn(&infra).await;
    let b = a.spawn_peer(&infra, |_| {}).await;
    let ada = user(&a, "ada").await;
    let bob = user(&a, "bob").await;
    let eve = user(&a, "eve").await;
    let room = a.create_room(&ada, "General", "public").await;
    a.join(&bob, &room).await;
    let mut ada_ws = connect(&a, &ada.cookie).await;
    let mut bob_ws = connect(&b, &bob.cookie).await;
    let mut eve_ws = connect(&a, &eve.cookie).await;

    let sent = ada_ws.send_message(room.id, "frist").await;
    bob_ws.expect_message("frist").await;

    assert!(is_ok(&edit(&mut ada_ws, room.id, sent.id, "first").await));
    let updated = bob_ws
        .expect("message.updated", |f| {
            matches!(f, ServerFrame::MessageUpdated { .. })
        })
        .await;
    let ServerFrame::MessageUpdated {
        message_id,
        content,
        version,
        ..
    } = updated
    else {
        unreachable!()
    };
    assert_eq!((message_id, content.as_str()), (sent.id, "first"));
    assert!(version > sent.version, "an edit outranks the original");
    let now = stored(&a, &bob, room.id, sent.id).await;
    assert_eq!(now.content, "first");
    assert_eq!(now.version, version);
    assert!(now.edited_at.is_some());

    // Only the author; only members; only real messages; only valid content.
    let bob_edit = edit(&mut bob_ws, room.id, sent.id, "mine now").await;
    assert_eq!(error_code(&bob_edit), Some(FrameErrorCode::Forbidden));
    let bob_delete = delete(&mut bob_ws, room.id, sent.id).await;
    assert_eq!(error_code(&bob_delete), Some(FrameErrorCode::Forbidden));
    let outsider = edit(&mut eve_ws, room.id, sent.id, "hi").await;
    assert_eq!(error_code(&outsider), Some(FrameErrorCode::Forbidden));
    let missing = edit(&mut ada_ws, room.id, MessageId::from_parts(1, 1, 1), "x").await;
    assert_eq!(error_code(&missing), Some(FrameErrorCode::NotFound));
    let empty = edit(&mut ada_ws, room.id, sent.id, "   ").await;
    assert_eq!(error_code(&empty), Some(FrameErrorCode::InvalidContent));

    assert!(is_ok(&delete(&mut ada_ws, room.id, sent.id).await));
    let deleted = bob_ws
        .expect("message.deleted", |f| {
            matches!(f, ServerFrame::MessageDeleted { .. })
        })
        .await;
    assert!(
        matches!(deleted, ServerFrame::MessageDeleted { message_id, .. } if message_id == sent.id)
    );
    let now = stored(&a, &bob, room.id, sent.id).await;
    assert!(now.deleted);
    assert_eq!(now.content, "");

    // Deleted is final: no edit, no second delete, no reactions.
    let late_edit = edit(&mut ada_ws, room.id, sent.id, "back").await;
    assert_eq!(error_code(&late_edit), Some(FrameErrorCode::NotFound));
    let late_react = react(&mut bob_ws, room.id, sent.id, "👍", true).await;
    assert_eq!(error_code(&late_react), Some(FrameErrorCode::NotFound));
    assert!(stored(&a, &bob, room.id, sent.id).await.deleted);
}

/// A retried send after an edit replays the original insert. Its write
/// timestamp is the ID's, below the edit's, so the edit survives
/// (docs/design.md 4.2).
pub async fn edit_survives_a_replayed_insert(infra: Arc<Infra>) {
    let app = TestApp::spawn(&infra).await;
    let ada = user(&app, "ada").await;
    let room = app.create_room(&ada, "General", "public").await;
    let mut ws = connect(&app, &ada.cookie).await;

    let key = Uuid::now_v7();
    let (sent, _) = ws.send_with_key(room.id, key, "original").await;
    assert!(is_ok(&edit(&mut ws, room.id, sent.id, "edited").await));
    let (again, duplicate) = ws.send_with_key(room.id, key, "original").await;
    assert!(duplicate);
    assert_eq!(again.id, sent.id);

    let now = stored(&app, &ada, room.id, sent.id).await;
    assert_eq!(now.content, "edited", "the replay overwrote the edit");

    // The same for a delete.
    assert!(is_ok(&delete(&mut ws, room.id, sent.id).await));
    ws.send_with_key(room.id, key, "original").await;
    let now = stored(&app, &ada, room.id, sent.id).await;
    assert!(now.deleted, "the replay undeleted the message");
    assert_eq!(now.content, "");
}

/// Counts are per emoji and per user, cross instances, ignore repeats, and
/// show `me` from each viewer's side. Only allowlisted emoji are accepted.
pub async fn reactions_count_once_per_user(infra: Arc<Infra>) {
    let a = TestApp::spawn(&infra).await;
    let b = a.spawn_peer(&infra, |_| {}).await;
    let ada = user(&a, "ada").await;
    let bob = user(&a, "bob").await;
    let room = a.create_room(&ada, "General", "public").await;
    a.join(&bob, &room).await;
    let mut ada_ws = connect(&a, &ada.cookie).await;
    let mut bob_ws = connect(&b, &bob.cookie).await;
    let sent = ada_ws.send_message(room.id, "react to me").await;

    let count_seen = async |ws: &mut Client, emoji: &'static str, want: u32| {
        ws.expect(&format!("{emoji} at {want}"), |f| {
            matches!(f, ServerFrame::ReactionUpdated { emoji: e, count, .. } if e == emoji && *count == want)
        })
        .await;
    };

    assert!(is_ok(
        &react(&mut ada_ws, room.id, sent.id, "👍", true).await
    ));
    count_seen(&mut bob_ws, "👍", 1).await;
    assert!(is_ok(
        &react(&mut bob_ws, room.id, sent.id, "👍", true).await
    ));
    count_seen(&mut ada_ws, "👍", 2).await;
    // A retry, or a second tab doing the same: still two.
    assert!(is_ok(
        &react(&mut bob_ws, room.id, sent.id, "👍", true).await
    ));
    count_seen(&mut ada_ws, "👍", 2).await;
    assert!(is_ok(
        &react(&mut bob_ws, room.id, sent.id, "🎉", true).await
    ));
    count_seen(&mut ada_ws, "🎉", 1).await;

    let bad = react(&mut bob_ws, room.id, sent.id, "not an emoji", true).await;
    assert_eq!(error_code(&bad), Some(FrameErrorCode::InvalidReaction));

    let as_ada = stored(&a, &ada, room.id, sent.id).await.reactions;
    let summary: Vec<_> = as_ada
        .iter()
        .map(|r| (r.emoji.as_str(), r.count, r.me))
        .collect();
    assert_eq!(summary, [("👍", 2, true), ("🎉", 1, false)]);
    let as_bob = stored(&b, &bob, room.id, sent.id).await.reactions;
    let summary: Vec<_> = as_bob
        .iter()
        .map(|r| (r.emoji.as_str(), r.count, r.me))
        .collect();
    assert_eq!(summary, [("👍", 2, true), ("🎉", 1, true)]);

    assert!(is_ok(
        &react(&mut ada_ws, room.id, sent.id, "👍", false).await
    ));
    count_seen(&mut bob_ws, "👍", 1).await;
    assert!(is_ok(
        &react(&mut bob_ws, room.id, sent.id, "🎉", false).await
    ));
    count_seen(&mut ada_ws, "🎉", 0).await;
    let left = stored(&a, &ada, room.id, sent.id).await.reactions;
    let summary: Vec<_> = left
        .iter()
        .map(|r| (r.emoji.as_str(), r.count, r.me))
        .collect();
    assert_eq!(summary, [("👍", 1, false)]);
}

/// A client that was away gets the current state of messages it already
/// had, if they changed, from the change feed in the sync.
pub async fn sync_returns_changes_to_loaded_messages(infra: Arc<Infra>) {
    let app = TestApp::spawn(&infra).await;
    let ada = user(&app, "ada").await;
    let bob = user(&app, "bob").await;
    let room = app.create_room(&ada, "General", "public").await;
    app.join(&bob, &room).await;
    let mut ws = connect(&app, &ada.cookie).await;
    let edited = ws.send_message(room.id, "typo").await;
    let removed = ws.send_message(room.id, "oops").await;
    let liked = ws.send_message(room.id, "nice").await;

    // Bob loads the room, then goes away.
    let page = history(&app, &bob, room.id).await;
    assert_eq!(page.messages.len(), 3);
    let newest = page.messages.iter().map(|m| m.id).max().unwrap();
    let cursor = page.change_cursor;

    assert!(is_ok(&edit(&mut ws, room.id, edited.id, "fixed").await));
    assert!(is_ok(&delete(&mut ws, room.id, removed.id).await));
    assert!(is_ok(&react(&mut ws, room.id, liked.id, "❤️", true).await));

    let resp = sync(
        &app,
        &bob,
        json!({"room_id": room.id, "after": newest, "changes_after": cursor}),
    )
    .await;
    let synced = &resp.rooms[0];
    assert!(synced.complete);
    let changed = |id: MessageId| synced.changed.iter().find(|m| m.id == id);
    assert_eq!(
        changed(edited.id).map(|m| m.content.as_str()),
        Some("fixed")
    );
    assert!(changed(removed.id).is_some_and(|m| m.deleted && m.content.is_empty()));
    assert_eq!(
        changed(liked.id).map(|m| m.reactions.len()),
        Some(1),
        "reaction changes are in the feed"
    );

    // With the new cursor, nothing has changed since.
    let again = sync(
        &app,
        &bob,
        json!({"room_id": room.id, "after": newest, "changes_after": synced.change_cursor}),
    )
    .await;
    // The cursor is backed off by the overlap window, so recent changes can
    // come back; what matters is that they are current, and nothing is new.
    for m in &again.rooms[0].changed {
        assert_eq!(Some(m), synced.changed.iter().find(|c| c.id == m.id));
    }
    // Without a change cursor: new messages only.
    let plain = sync(&app, &bob, json!({"room_id": room.id, "after": newest})).await;
    assert!(plain.rooms[0].changed.is_empty());
}

/// A send acked long after its ID was assigned (a retry of a write that
/// never landed) is older than a returning client's resume window. The
/// late-insert row in the change feed brings it (docs/design.md 16.1).
pub async fn late_inserts_reach_sync_through_the_change_feed(infra: Arc<Infra>) {
    let app = TestApp::spawn(&infra).await;
    let ada = user(&app, "ada").await;
    let bob = user(&app, "bob").await;
    let room = app.create_room(&ada, "General", "public").await;
    app.join(&bob, &room).await;
    let mut ws = connect(&app, &ada.cookie).await;

    let cursor = history(&app, &bob, room.id).await.change_cursor;
    // Ada's first attempt claimed its key and got an ID a minute ago, but
    // the write never happened.
    let key = Uuid::now_v7();
    let minute_ago = MessageId::from_parts(cursor.ms_since_epoch() - 60_000, 1023, 7);
    let mut redis = redis::Client::open(infra.redis_url.as_str())
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let _: () = redis::cmd("SET")
        .arg(format!("idem:{}:{key}", ada.me.id))
        .arg(claim_value(
            minute_ago,
            room.id,
            &fingerprint(room.id, "late"),
        ))
        .query_async(&mut redis)
        .await
        .unwrap();

    let newer = ws.send_message(room.id, "newer").await;
    // Bob has seen "newer" and nothing else changed yet.
    let before = sync(
        &app,
        &bob,
        json!({"room_id": room.id, "after": newer.id, "changes_after": cursor}),
    )
    .await;
    assert!(before.rooms[0].changed.is_empty());

    // The retry succeeds now, with the old ID.
    let (late, duplicate) = ws.send_with_key(room.id, key, "late").await;
    assert!(duplicate);
    assert_eq!(late.id, minute_ago);

    let after = sync(
        &app,
        &bob,
        json!({"room_id": room.id, "after": newer.id, "changes_after": cursor}),
    )
    .await;
    let synced = &after.rooms[0];
    assert!(
        !synced.messages.iter().any(|m| m.id == late.id),
        "the test needs the message outside the resume window"
    );
    assert!(
        synced
            .changed
            .iter()
            .any(|m| m.id == late.id && m.content == "late"),
        "late insert missing from sync: {:?}",
        synced.changed
    );
}

fn unread_of(rooms: &[RoomInfo], id: Uuid) -> (u32, bool) {
    let room = rooms.iter().find(|r| r.id == id).unwrap();
    (room.unread, room.unread_capped)
}

/// Unread counts others' messages after the read position; marks only move
/// forward; the caller's other tabs and (in a group) nobody else hear of it;
/// "seen by" counts readers.
pub async fn read_marks_unread_counts_and_receipts(infra: Arc<Infra>) {
    let a = TestApp::spawn(&infra).await;
    let b = a.spawn_peer(&infra, |_| {}).await;
    let ada = user(&a, "ada").await;
    let bob = user(&a, "bob").await;
    let cyd = user(&a, "cyd").await;
    let room = a.create_room(&ada, "General", "public").await;
    a.join(&bob, &room).await;
    a.join(&cyd, &room).await;
    let mut ada_ws = connect(&a, &ada.cookie).await;
    let mut bob_tab1 = connect(&a, &bob.cookie).await;
    let mut bob_tab2 = connect(&b, &bob.cookie).await;

    let m1 = ada_ws.send_message(room.id, "one").await;
    let m2 = ada_ws.send_message(room.id, "two").await;
    bob_tab1.send_message(room.id, "bob's own").await;
    let m3 = ada_ws.send_message(room.id, "three").await;
    let gone = ada_ws.send_message(room.id, "deleted").await;
    assert!(is_ok(&delete(&mut ada_ws, room.id, gone.id).await));

    assert_eq!(unread_of(&a.my_rooms(&bob).await, room.id), (3, false));
    assert_eq!(unread_of(&a.my_rooms(&ada).await, room.id), (1, false));

    bob_tab1
        .send(&ClientFrame::ReadMark {
            room_id: room.id,
            message_id: m2.id,
        })
        .await;
    bob_tab2
        .expect(
            "read.updated",
            |f| matches!(f, ServerFrame::ReadUpdated { message_id, .. } if *message_id == m2.id),
        )
        .await;
    assert_eq!(unread_of(&b.my_rooms(&bob).await, room.id), (1, false));
    // In a group, reads aren't broadcast to the room (docs/design.md 9).
    ada_ws
        .expect_none("bob's read", Duration::from_millis(300), |f| {
            matches!(f, ServerFrame::ReadUpdated { .. })
        })
        .await;

    // Backwards and far-future marks change nothing.
    for message_id in [
        m1.id,
        MessageId::from_parts(m3.id.ms_since_epoch() + 3_600_000, 0, 0),
    ] {
        bob_tab1
            .send(&ClientFrame::ReadMark {
                room_id: room.id,
                message_id,
            })
            .await;
    }
    bob_tab2
        .expect_none("a backwards read", Duration::from_millis(300), |f| {
            matches!(f, ServerFrame::ReadUpdated { .. })
        })
        .await;
    assert_eq!(unread_of(&a.my_rooms(&bob).await, room.id), (1, false));

    let receipts = async |id: MessageId| -> Receipts {
        a.get_as(
            &ada,
            &format!("/api/rooms/{}/receipts?message_id={id}", room.id),
        )
        .await
        .json()
        .await
        .unwrap()
    };
    let seen = receipts(m2.id).await;
    assert_eq!(seen.count, 1);
    assert_eq!(seen.readers[0].handle, "bob");
    assert_eq!(receipts(m3.id).await.count, 0);
    let resp = a
        .get_as(
            &cyd,
            &format!(
                "/api/rooms/{}/receipts?message_id={}",
                Uuid::now_v7(),
                m2.id
            ),
        )
        .await;
    // Non-members get 404, so rooms can't be probed.
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);

    bob_tab1
        .send(&ClientFrame::ReadMark {
            room_id: room.id,
            message_id: m3.id,
        })
        .await;
    bob_tab2
        .expect(
            "read.updated",
            |f| matches!(f, ServerFrame::ReadUpdated { message_id, .. } if *message_id == m3.id),
        )
        .await;
    assert_eq!(unread_of(&a.my_rooms(&bob).await, room.id), (0, false));
    assert_eq!(receipts(m3.id).await.count, 1);
}

/// In a DM the other person's read is a live receipt.
pub async fn dm_reads_are_live_receipts(infra: Arc<Infra>) {
    let a = TestApp::spawn(&infra).await;
    let b = a.spawn_peer(&infra, |_| {}).await;
    let ada = user(&a, "ada").await;
    let bob = user(&a, "bob").await;
    let dm: RoomInfo = a
        .post_as(&ada, "/api/dms", Some(json!({"handle": "bob"})))
        .await
        .json()
        .await
        .unwrap();
    let mut ada_ws = connect(&a, &ada.cookie).await;
    let mut bob_ws = connect(&b, &bob.cookie).await;
    let sent = ada_ws.send_message(dm.id, "you there?").await;
    bob_ws.expect_message("you there?").await;
    bob_ws
        .send(&ClientFrame::ReadMark {
            room_id: dm.id,
            message_id: sent.id,
        })
        .await;
    let read = ada_ws
        .expect("bob's read", |f| {
            matches!(f, ServerFrame::ReadUpdated { .. })
        })
        .await;
    assert_eq!(
        read,
        ServerFrame::ReadUpdated {
            room_id: dm.id,
            user_id: bob.me.id,
            message_id: sent.id,
        }
    );
}

/// Members see joins and leaves live, on every instance.
pub async fn member_frames_announce_joins_and_leaves(infra: Arc<Infra>) {
    let a = TestApp::spawn(&infra).await;
    let b = a.spawn_peer(&infra, |_| {}).await;
    let ada = user(&a, "ada").await;
    let bob = user(&a, "bob").await;
    let room = a.create_room(&ada, "General", "public").await;
    let mut ada_ws = connect(&a, &ada.cookie).await;

    b.join(&bob, &room).await;
    let joined = ada_ws
        .expect("member.joined", |f| {
            matches!(f, ServerFrame::MemberJoined { .. })
        })
        .await;
    let ServerFrame::MemberJoined { room_id, member } = joined else {
        unreachable!()
    };
    assert_eq!((room_id, member.user.id), (room.id, bob.me.id));

    let resp = b
        .post_as(&bob, &format!("/api/rooms/{}/leave", room.id), None)
        .await;
    assert!(resp.status().is_success());
    let left = ada_ws
        .expect("member.left", |f| {
            matches!(f, ServerFrame::MemberLeft { .. })
        })
        .await;
    assert_eq!(
        left,
        ServerFrame::MemberLeft {
            room_id: room.id,
            user_id: bob.me.id,
        }
    );
}

async fn search(app: &TestApp, who: &User, query: &str) -> Vec<(String, Uuid)> {
    let url = reqwest::Url::parse_with_params(&app.url("/api/search"), [("q", query)]).unwrap();
    let resp = app
        .client
        .get(url)
        .header(reqwest::header::COOKIE, &who.cookie)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let results: SearchResults = resp.json().await.unwrap();
    results
        .messages
        .into_iter()
        .map(|m| (m.content, m.room_id))
        .collect()
}

/// Polls until `$check` holds: the index is eventually consistent, a few
/// seconds behind (docs/design.md 10). A macro rather than a function
/// taking an async closure, whose future can't be shown to be `Send`.
macro_rules! eventually {
    ($what:expr, $check:expr) => {{
        let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
        while !$check {
            assert!(
                tokio::time::Instant::now() < deadline,
                "never happened: {}",
                $what
            );
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }};
}

/// Search covers only the caller's rooms, follows edits, and never shows a
/// deleted message, even before the index catches up.
pub async fn search_respects_membership_edits_and_deletes(infra: Arc<Infra>) {
    // Its own Redis: the search stream is shared by every instance on a
    // Redis, and other tests' instances have other databases.
    let (_redis, redis_url) = infra.dedicated_redis().await;
    let point_at_redis = |c: &mut Config| c.redis_url = Secret::new(redis_url.clone());
    let a = TestApp::spawn_with(&infra, point_at_redis).await;
    let b = a.spawn_peer(&infra, point_at_redis).await;
    let ada = user(&a, "ada").await;
    let bob = user(&a, "bob").await;
    let open = a.create_room(&ada, "Open", "public").await;
    let secret = a.create_room(&ada, "Secret", "private").await;
    a.join(&bob, &open).await;
    let mut ws = connect(&a, &ada.cookie).await;

    let public_fox = ws.send_message(open.id, "the quick brown fox").await;
    let private_fox = ws.send_message(secret.id, "a private fox").await;
    // Indexed by whichever instance claims the entries.
    eventually!(
        "both messages indexed",
        search(&b, &ada, "fox").await.len() == 2
    );
    assert_eq!(
        search(&a, &ada, "fox").await,
        [
            ("a private fox".to_string(), secret.id),
            ("the quick brown fox".to_string(), open.id)
        ],
        "newest first"
    );
    assert_eq!(
        search(&a, &bob, "foxes").await,
        [("the quick brown fox".to_string(), open.id)],
        "only rooms bob is in (and stemmed: foxes matches fox)"
    );

    // Deleted: gone from results at once, whatever the index says.
    assert!(is_ok(&delete(&mut ws, secret.id, private_fox.id).await));
    assert_eq!(search(&a, &ada, "private").await, []);

    // Edited: the old words stop matching once the index catches up, and
    // results always show the current text.
    assert!(is_ok(
        &edit(&mut ws, open.id, public_fox.id, "a lazy dog").await
    ));
    // Until then a stale hit for "fox" may come back, but with today's text.
    for (content, _) in search(&a, &bob, "fox").await {
        assert_eq!(content, "a lazy dog");
    }
    eventually!(
        "the edit indexed",
        search(&a, &bob, "dog").await.len() == 1 && search(&a, &bob, "fox").await.is_empty()
    );

    // Leaving: results go with the membership, immediately.
    let resp = a
        .post_as(&bob, &format!("/api/rooms/{}/leave", open.id), None)
        .await;
    assert!(resp.status().is_success());
    assert_eq!(search(&a, &bob, "dog").await, []);

    for bad in ["", "   ", &"x".repeat(201)] {
        let url = reqwest::Url::parse_with_params(&a.url("/api/search"), [("q", bad)]).unwrap();
        let resp = a
            .client
            .get(url)
            .header(reqwest::header::COOKIE, &ada.cookie)
            .send()
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            reqwest::StatusCode::UNPROCESSABLE_ENTITY,
            "{bad:?}"
        );
    }
}
