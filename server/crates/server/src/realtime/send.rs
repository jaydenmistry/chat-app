//! Sending a message (docs/design.md 6.1).
//!
//! 1. Validate the content.
//! 2. Check membership (every send, not just at connect).
//! 3. Claim the idempotency key in Redis. A new key stores the freshly
//!    allocated ID; a key seen before returns the ID stored with it.
//! 4. Write to Scylla, within 2 s.
//! 5. Acknowledge, then publish to the room.
//!
//! Nothing is acknowledged or fanned out before step 4 succeeds. If the write
//! fails or times out, the client retries with the same key, gets the same
//! ID back in step 3, and step 4 writes the same cells again, which is
//! harmless (see `MessageStore::insert`). One message, one ID, however many
//! attempts it takes, and whichever instances they land on.

use std::{sync::LazyLock, time::Duration};

use protocol::{
    api::Message,
    frames::{FrameErrorCode, ServerFrame},
    ids::MessageId,
    validate,
};
use redis::Script;
use storage::messages::{ChangeKind, NewMessage};
use time::OffsetDateTime;
use uuid::Uuid;

use crate::{
    auth::token,
    http::messages::{RESUME_OVERLAP_MS, to_api},
    realtime::events::{self, Event},
    search,
    state::AppState,
};

/// A write that takes longer than this is reported as failed. Together with
/// the clock-skew bound it limits how late a message can commit relative to
/// its ID (docs/design.md 16.1).
pub const WRITE_TIMEOUT: Duration = Duration::from_secs(2);
const IDEMPOTENCY_TTL_SECS: u64 = 24 * 3600;
/// Retry a failed publish for about this long, in the background.
const PUBLISH_ATTEMPTS: u32 = 5;

/// Claims an idempotency key, or returns what it was claimed with, and adds
/// the message to the search stream.
///
/// KEYS[1] = idem:<user>:<key>, KEYS[2] = the search stream. ARGV = new
/// value, TTL in seconds, room ID, the value's `<room>:<fingerprint>` part,
/// the stream's length cap. Returns nil if the key was new (and is now
/// ours), else the stored value.
///
/// One script, so check-and-set can't interleave with a concurrent attempt,
/// and the search entry exists before the Scylla write: a crash after the
/// write can't leave the message unindexed (docs/design.md 10). A retry
/// adds a fresh entry, because the first attempt's may have been dropped
/// while its write was missing. A conflicting reuse of the key adds none.
/// Phase 4 adds the rate limit here too.
static CLAIM_KEY: LazyLock<Script> = LazyLock::new(|| {
    Script::new(
        r"
        local existing = redis.call('GET', KEYS[1])
        local value = ARGV[1]
        if existing then
          local sep = string.find(existing, ':', 1, true)
          if not sep or string.sub(existing, sep + 1) ~= ARGV[4] then
            return existing
          end
          value = existing
        else
          redis.call('SET', KEYS[1], ARGV[1], 'EX', ARGV[2])
        end
        local id = string.sub(value, 1, string.find(value, ':', 1, true) - 1)
        redis.call('XADD', KEYS[2], 'MAXLEN', '~', ARGV[5], '*',
                   'room_id', ARGV[3], 'message_id', id)
        return existing or false
        ",
    )
});

pub struct Sender<'a> {
    pub user_id: Uuid,
    /// When the author's account expires (guests).
    pub expires_at: Option<OffsetDateTime>,
    pub state: &'a AppState,
}

/// What the connection does next: send `reply` on this socket first, then
/// run `publish`, so the sender sees its ack before its own fan-out copy.
pub struct Outcome {
    pub reply: ServerFrame,
    pub publish: Option<Publish>,
}

pub struct Publish {
    pub room_id: Uuid,
    pub event: Event,
}

impl Publish {
    /// Publishes, retrying in the background if Redis is briefly unavailable.
    /// The message is already stored; if every attempt fails, clients get it
    /// from sync when they reconnect or resync.
    pub async fn run(self, state: &AppState) {
        let channel = events::room_channel(self.room_id);
        let mut redis = state.redis.clone();
        if events::publish(&mut redis, &channel, &self.event)
            .await
            .is_ok()
        {
            return;
        }
        tokio::spawn(async move {
            let mut delay = Duration::from_millis(200);
            for attempt in 1..PUBLISH_ATTEMPTS {
                tokio::time::sleep(delay).await;
                match events::publish(&mut redis, &channel, &self.event).await {
                    Ok(()) => return,
                    Err(err) if attempt + 1 == PUBLISH_ATTEMPTS => {
                        tracing::warn!(error = %err, channel, "giving up publishing a message");
                    }
                    Err(_) => delay *= 2,
                }
            }
        });
    }
}

pub(crate) fn error(req_id: &str, code: FrameErrorCode, message: impl Into<String>) -> Outcome {
    Outcome {
        reply: ServerFrame::Error {
            req_id: Some(req_id.to_string()),
            code,
            message: message.into(),
        },
        publish: None,
    }
}

/// `<message id>:<room id>:<fingerprint>`, stored under the idempotency key.
/// Public so tests can stage a claim whose write never happened.
pub fn claim_value(id: MessageId, room_id: Uuid, fingerprint: &str) -> String {
    format!("{id}:{room_id}:{fingerprint}")
}

/// What a reused key must match: the same room and the same content.
pub fn fingerprint(room_id: Uuid, content: &str) -> String {
    let mut input = room_id.as_bytes().to_vec();
    input.extend_from_slice(content.as_bytes());
    token::sha256(&input)[..16]
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

fn parse_claim(value: &str) -> Option<(MessageId, Uuid, &str)> {
    let mut parts = value.splitn(3, ':');
    let id = parts.next()?.parse().ok()?;
    let room = parts.next()?.parse().ok()?;
    let fingerprint = parts.next()?;
    Some((id, room, fingerprint))
}

pub async fn send(
    sender: &Sender<'_>,
    req_id: &str,
    room_id: Uuid,
    idempotency_key: Uuid,
    raw_content: &str,
) -> Outcome {
    let state = sender.state;
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

    let membership = match storage::rooms::membership(&state.pg, room_id, sender.user_id).await {
        Ok(Some(membership)) => membership,
        Ok(None) => {
            return error(
                req_id,
                FrameErrorCode::Forbidden,
                "not a member of this room",
            );
        }
        Err(err) => {
            tracing::warn!(error = %err, "membership check failed");
            return error(req_id, FrameErrorCode::Unavailable, "try again");
        }
    };

    // Guest messages, and messages in rooms that expire, get a TTL ending
    // exactly when the account or room expires (docs/design.md 11.3).
    let now = OffsetDateTime::now_utc();
    let expiry = [sender.expires_at, membership.message_expiry_at]
        .into_iter()
        .flatten()
        .min();
    let ttl = match expiry {
        Some(at) => match Duration::try_from(at - now) {
            Ok(left) if left >= Duration::from_secs(1) => Some(left),
            _ => {
                return error(
                    req_id,
                    FrameErrorCode::Forbidden,
                    "this account or room has expired",
                );
            }
        },
        None => None,
    };

    let candidate = match state.ids.next_id() {
        Ok(id) => id,
        Err(err) => {
            tracing::warn!(error = %err, "can't issue message IDs");
            return error(req_id, FrameErrorCode::Unavailable, "try again");
        }
    };
    let fingerprint = fingerprint(room_id, content);
    let claimed: Option<String> = match CLAIM_KEY
        .key(format!("idem:{}:{idempotency_key}", sender.user_id))
        .key(search::STREAM)
        .arg(claim_value(candidate, room_id, &fingerprint))
        .arg(IDEMPOTENCY_TTL_SECS)
        .arg(room_id.to_string())
        .arg(format!("{room_id}:{fingerprint}"))
        .arg(search::MAX_STREAM_LEN)
        .invoke_async(&mut state.redis.clone())
        .await
    {
        Ok(claimed) => claimed,
        // No dedupe without Redis, so no send: duplicates are never possible.
        Err(err) => {
            tracing::warn!(error = %err, "idempotency check failed");
            return error(req_id, FrameErrorCode::Unavailable, "try again");
        }
    };
    let (id, duplicate) = match claimed.as_deref().map(parse_claim) {
        None => (candidate, false),
        Some(Some((id, claimed_room, claimed_fingerprint)))
            if claimed_room == room_id && claimed_fingerprint == fingerprint =>
        {
            (id, true)
        }
        Some(Some(_)) => {
            return error(
                req_id,
                FrameErrorCode::IdempotencyConflict,
                "this idempotency key was already used for a different message",
            );
        }
        Some(None) => {
            tracing::error!("malformed idempotency record");
            return error(req_id, FrameErrorCode::Internal, "try again");
        }
    };

    // Named, not a temporary: the future returned by `insert` borrows it and
    // must not outlive it.
    let new = NewMessage {
        room_id,
        id,
        author_id: sender.user_id,
        content,
        ttl,
    };
    match tokio::time::timeout(WRITE_TIMEOUT, state.messages.insert(&new)).await {
        Ok(Ok(())) => {}
        Ok(Err(err)) => {
            tracing::warn!(error = %err, %id, "message write failed");
            return error(req_id, FrameErrorCode::Unavailable, "try again");
        }
        Err(_) => {
            tracing::warn!(%id, "message write timed out");
            return error(req_id, FrameErrorCode::Unavailable, "try again");
        }
    }

    // An ack this long after the ID was assigned (a retry of a slow first
    // attempt) may be behind the resume window of a client that was away:
    // the change feed tells it (docs/design.md 16.1). Before the ack, so a
    // failure here is retried with the rest.
    if id.ms_since_epoch() + RESUME_OVERLAP_MS / 2 < state.ids.now_ms() {
        let recorded = match state.ids.next_id() {
            Ok(change_id) => tokio::time::timeout(
                WRITE_TIMEOUT,
                state
                    .messages
                    .record_change(room_id, change_id, id, ChangeKind::LateInsert, ttl),
            )
            .await
            .is_ok_and(|r| r.is_ok()),
            Err(_) => false,
        };
        if !recorded {
            tracing::warn!(%id, "recording a late insert failed");
            return error(req_id, FrameErrorCode::Unavailable, "try again");
        }
    }

    let message: Message = to_api(
        room_id,
        storage::messages::StoredMessage {
            id,
            author_id: sender.user_id,
            content: content.to_string(),
            edited_at: None,
            deleted: false,
            version: id.write_timestamp_us(),
        },
        // A duplicate may have reactions by now; clients keep the ones they
        // have when the version is unchanged, and sync brings the rest.
        Vec::new(),
    );
    let created = ServerFrame::MessageCreated {
        message: message.clone(),
    };
    Outcome {
        reply: ServerFrame::MessageAck {
            req_id: req_id.to_string(),
            message,
            duplicate,
        },
        // A duplicate is published again too: the first publish may be the
        // thing that failed. Clients ignore copies by ID.
        publish: Some(Publish {
            room_id,
            event: Event::deliver(&created),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn claims_round_trip() {
        let id = MessageId::from_parts(123, 4, 5);
        let room = Uuid::now_v7();
        let fp = fingerprint(room, "hello");
        let value = claim_value(id, room, &fp);
        assert_eq!(parse_claim(&value), Some((id, room, fp.as_str())));
        assert_eq!(parse_claim("garbage"), None);
    }

    #[test]
    fn fingerprints_bind_room_and_content() {
        let (a, b) = (Uuid::now_v7(), Uuid::now_v7());
        assert_eq!(fingerprint(a, "x"), fingerprint(a, "x"));
        assert_ne!(fingerprint(a, "x"), fingerprint(a, "y"));
        assert_ne!(fingerprint(a, "x"), fingerprint(b, "x"));
        assert_eq!(fingerprint(a, "x").len(), 32);
    }
}
