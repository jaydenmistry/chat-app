//! Edits, deletes, reactions and read marks (docs/design.md 6.5 and 9).
//!
//! Edit, delete and reaction share one path:
//! 1. Validate, and check membership.
//! 2. Read the message: it must exist, not be deleted, be within the
//!    member's history, and (edit, delete) be the caller's own.
//! 3. Allocate a `change_id`, write the `room_changes` row, then the change,
//!    `USING TIMESTAMP` above the message's current version and with its
//!    remaining TTL.
//! 4. `ok` to the requester, then publish to the room.
//!
//! All three are idempotent by nature (the same content, the same flag, the
//! same `active`), so a retry after a lost `ok` is safe without a key.

use std::time::Duration;

use protocol::{
    api::RoomKind,
    frames::{FrameErrorCode, ServerFrame},
    ids::MessageId,
    reactions, validate,
};
use storage::{
    messages::{ChangeKind, StoreError, StoredMessage},
    rooms::Membership,
};
use time::OffsetDateTime;
use uuid::Uuid;

use crate::{
    http::messages::floor,
    realtime::{
        events::{self, Event},
        send::{Outcome, Publish, Sender, WRITE_TIMEOUT, error},
    },
    search,
    state::AppState,
};

/// A read mark may be at most this far ahead of this instance's clock: the
/// clock-skew bound between instances (docs/design.md 3.3). Anything beyond
/// can't be a real message and would mark future messages as read.
const READ_MARK_SKEW_MS: u64 = 1_000;

/// Why a step refused: becomes the `error` reply.
struct Rejection(FrameErrorCode, &'static str);

const UNAVAILABLE: Rejection = Rejection(FrameErrorCode::Unavailable, "try again");

/// The message a change applies to, as loaded in step 2.
struct Target {
    message: StoredMessage,
    /// The message's remaining lifetime; every cell the change writes gets
    /// it (docs/design.md 11.3).
    ttl: Option<Duration>,
}

enum Ownership {
    /// Edit and delete: only the author.
    Author,
    /// Reactions: any member.
    Anyone,
}

async fn load(
    sender: &Sender<'_>,
    room_id: Uuid,
    message_id: MessageId,
    ownership: Ownership,
) -> Result<Target, Rejection> {
    let state = sender.state;
    let membership = member(state, room_id, sender.user_id).await?;
    let loaded = timed(state.messages.get_with_ttl(room_id, message_id))
        .await
        .map_err(|()| UNAVAILABLE)?;
    let not_found = || Rejection(FrameErrorCode::NotFound, "no such message");
    let Some((message, ttl)) = loaded else {
        return Err(not_found());
    };
    // Before the member's history floor, the message isn't theirs to see.
    if message.deleted || message.id < floor(membership.history_floor) {
        return Err(not_found());
    }
    if ttl.is_some_and(|ttl| ttl < Duration::from_secs(1)) {
        return Err(not_found());
    }
    if matches!(ownership, Ownership::Author) && message.author_id != sender.user_id {
        return Err(Rejection(
            FrameErrorCode::Forbidden,
            "only the author can change a message",
        ));
    }
    Ok(Target { message, ttl })
}

async fn member(state: &AppState, room_id: Uuid, user_id: Uuid) -> Result<Membership, Rejection> {
    match storage::rooms::membership(&state.pg, room_id, user_id).await {
        Ok(Some(membership)) => Ok(membership),
        Ok(None) => Err(Rejection(
            FrameErrorCode::Forbidden,
            "not a member of this room",
        )),
        Err(err) => {
            tracing::warn!(error = %err, "membership check failed");
            Err(UNAVAILABLE)
        }
    }
}

/// A Scylla call with the write timeout. Errors are logged here; callers
/// only need to know it failed.
async fn timed<T>(call: impl Future<Output = Result<T, StoreError>>) -> Result<T, ()> {
    match tokio::time::timeout(WRITE_TIMEOUT, call).await {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(err)) => {
            tracing::warn!(error = %err, "scylla call failed");
            Err(())
        }
        Err(_) => {
            tracing::warn!("scylla call timed out");
            Err(())
        }
    }
}

/// The write timestamp for a change: now, but always above the current
/// version, so the change beats the original insert and every earlier
/// change even if this instance's clock is behind (docs/design.md 4.2).
fn write_ts(state: &AppState, current_version: i64) -> i64 {
    let now_us = i64::try_from(state.ids.unix_ms()).unwrap_or(i64::MAX / 1000) * 1000;
    now_us.max(current_version + 1)
}

/// The `room_changes` row, written before the change itself.
async fn record(
    sender: &Sender<'_>,
    room_id: Uuid,
    message_id: MessageId,
    kind: ChangeKind,
    ttl: Option<Duration>,
) -> Result<MessageId, Rejection> {
    let state = sender.state;
    let change_id = state.ids.next_id().map_err(|err| {
        tracing::warn!(error = %err, "can't issue change IDs");
        UNAVAILABLE
    })?;
    timed(
        state
            .messages
            .record_change(room_id, change_id, message_id, kind, ttl),
    )
    .await
    .map_err(|()| UNAVAILABLE)?;
    Ok(change_id)
}

fn ok(req_id: &str, room_id: Uuid, frame: &ServerFrame) -> Outcome {
    Outcome {
        reply: ServerFrame::Ok {
            req_id: req_id.to_string(),
        },
        publish: Some(Publish {
            room_id,
            event: Event::deliver(frame),
        }),
    }
}

/// Runs the shared steps, or returns the error reply they produced.
macro_rules! attempt {
    ($req_id:expr, $e:expr) => {
        match $e {
            Ok(value) => value,
            Err(Rejection(code, message)) => return error($req_id, code, message),
        }
    };
}

pub async fn edit(
    sender: &Sender<'_>,
    req_id: &str,
    room_id: Uuid,
    message_id: MessageId,
    raw_content: &str,
) -> Outcome {
    let content = match validate::message_content(raw_content) {
        Ok(content) => content,
        Err(e) => {
            return error(
                req_id,
                FrameErrorCode::InvalidContent,
                format!("message {}", e.message()),
            );
        }
    };
    let target = attempt!(
        req_id,
        load(sender, room_id, message_id, Ownership::Author).await
    );
    let state = sender.state;
    let change_id = attempt!(
        req_id,
        record(sender, room_id, message_id, ChangeKind::Edit, target.ttl).await
    );
    let version = write_ts(state, target.message.version);
    let edited_at = OffsetDateTime::from_unix_timestamp_nanos(i128::from(version) * 1000)
        .unwrap_or(OffsetDateTime::UNIX_EPOCH);
    if timed(
        state
            .messages
            .edit(room_id, message_id, content, edited_at, version, target.ttl),
    )
    .await
    .is_err()
    {
        return error(req_id, FrameErrorCode::Unavailable, "try again");
    }
    search::notify(state, room_id, message_id);
    ok(
        req_id,
        room_id,
        &ServerFrame::MessageUpdated {
            room_id,
            message_id,
            content: content.to_string(),
            edited_at,
            version,
            change_id,
        },
    )
}

pub async fn delete(
    sender: &Sender<'_>,
    req_id: &str,
    room_id: Uuid,
    message_id: MessageId,
) -> Outcome {
    let target = attempt!(
        req_id,
        load(sender, room_id, message_id, Ownership::Author).await
    );
    let state = sender.state;
    let change_id = attempt!(
        req_id,
        record(sender, room_id, message_id, ChangeKind::Delete, target.ttl).await
    );
    let version = write_ts(state, target.message.version);
    if timed(
        state
            .messages
            .delete(room_id, message_id, version, target.ttl),
    )
    .await
    .is_err()
    {
        return error(req_id, FrameErrorCode::Unavailable, "try again");
    }
    search::notify(state, room_id, message_id);
    ok(
        req_id,
        room_id,
        &ServerFrame::MessageDeleted {
            room_id,
            message_id,
            version,
            change_id,
        },
    )
}

pub async fn react(
    sender: &Sender<'_>,
    req_id: &str,
    room_id: Uuid,
    message_id: MessageId,
    emoji: &str,
    active: bool,
) -> Outcome {
    if !reactions::is_allowed(emoji) {
        return error(
            req_id,
            FrameErrorCode::InvalidReaction,
            "that emoji isn't one of the allowed reactions",
        );
    }
    let target = attempt!(
        req_id,
        load(sender, room_id, message_id, Ownership::Anyone).await
    );
    let state = sender.state;
    // A guest's reaction ends with the guest, and every reaction ends with
    // the message.
    let own_ttl = sender
        .expires_at
        .map(|at| Duration::try_from(at - OffsetDateTime::now_utc()).unwrap_or_default());
    let ttl = [target.ttl, own_ttl].into_iter().flatten().min();
    if ttl.is_some_and(|ttl| ttl < Duration::from_secs(1)) {
        return error(
            req_id,
            FrameErrorCode::Forbidden,
            "this account has expired",
        );
    }
    let change_id = attempt!(
        req_id,
        record(sender, room_id, message_id, ChangeKind::Reaction, ttl).await
    );
    // Reactions have no version; the change ID's time orders toggles, so
    // the later of two toggles wins in storage as on clients.
    let ts = change_id.write_timestamp_us();
    let unavailable = || error(req_id, FrameErrorCode::Unavailable, "try again");
    if timed(state.messages.set_reaction(
        room_id,
        message_id,
        emoji,
        sender.user_id,
        active,
        ts,
        ttl,
    ))
    .await
    .is_err()
    {
        return unavailable();
    }
    let Ok(count) = timed(state.messages.reaction_count(room_id, message_id, emoji)).await else {
        return unavailable();
    };
    ok(
        req_id,
        room_id,
        &ServerFrame::ReactionUpdated {
            room_id,
            message_id,
            emoji: emoji.to_string(),
            count,
            user_id: sender.user_id,
            active,
            change_id,
        },
    )
}

/// Moves the caller's read position forward. No reply: a lost mark is
/// repaired by the next one. In a DM the other person sees it live ("Seen");
/// in a group only the caller's other tabs do (docs/design.md 9).
pub async fn mark_read(state: &AppState, user_id: Uuid, room_id: Uuid, message_id: MessageId) {
    if message_id.ms_since_epoch() > state.ids.now_ms() + READ_MARK_SKEW_MS {
        return;
    }
    let membership = match storage::rooms::membership(&state.pg, room_id, user_id).await {
        Ok(Some(membership)) => membership,
        Ok(None) => return,
        Err(err) => {
            tracing::warn!(error = %err, "membership check failed");
            return;
        }
    };
    match storage::rooms::mark_read(&state.pg, room_id, user_id, message_id.as_i64()).await {
        Ok(true) => {}
        // Not newer than the stored position: nothing changed, nothing to tell.
        Ok(false) => return,
        Err(err) => {
            tracing::warn!(error = %err, "marking read failed");
            return;
        }
    }
    let frame = ServerFrame::ReadUpdated {
        room_id,
        user_id,
        message_id,
    };
    // The room channel reaches the caller's own sockets too.
    let channel = if membership.kind == RoomKind::Dm {
        events::room_channel(room_id)
    } else {
        events::user_channel(user_id)
    };
    events::publish_best_effort(&mut state.redis.clone(), &channel, &Event::deliver(&frame)).await;
}
