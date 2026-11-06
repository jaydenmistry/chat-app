//! WebSocket frames. Every frame is a JSON object with a `type` field.
//!
//! The protocol version is negotiated once, with the WebSocket subprotocol
//! header ([`SUBPROTOCOL`]), instead of being repeated in every frame. A
//! breaking change becomes `chat.v2`. The full reference, with examples and
//! sequences, is docs/protocol.md.

use serde::{Deserialize, Serialize};
use ts_rs::TS;
use uuid::Uuid;

use crate::{
    api::{Member, Message, RoomInfo},
    ids::MessageId,
};

pub const SUBPROTOCOL: &str = "chat.v1";
/// The largest frame the server accepts. Message content is at most 4096
/// bytes; JSON escaping and the other fields fit easily in the rest.
pub const MAX_FRAME_BYTES: usize = 16 * 1024;
/// How often the server sends `ping`. The client answers with `pong`.
pub const HEARTBEAT_MS: u32 = 25_000;
/// Either side closes the connection after this long without any frame.
pub const IDLE_TIMEOUT_MS: u32 = 60_000;

/// Frames the client sends.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(tag = "type")]
#[ts(export)]
pub enum ClientFrame {
    /// Send a message. Retry with the same `idempotency_key` until a
    /// `message.ack` arrives; the server stores the message once.
    #[serde(rename = "message.send")]
    MessageSend {
        req_id: String,
        room_id: Uuid,
        idempotency_key: Uuid,
        content: String,
    },
    /// "I am typing in this room." Send at most every 3 seconds.
    #[serde(rename = "typing")]
    Typing { room_id: Uuid },
    /// Report whether the user is active in this tab.
    #[serde(rename = "presence.set")]
    PresenceSet { state: PresenceState },
    /// Replace the set of users whose presence this connection receives.
    #[serde(rename = "presence.watch")]
    PresenceWatch { user_ids: Vec<Uuid> },
    /// The answer to `ping`.
    #[serde(rename = "pong")]
    Pong,
    /// Replace the content of your own message. Answered with `ok`.
    #[serde(rename = "message.edit")]
    MessageEdit {
        req_id: String,
        room_id: Uuid,
        message_id: MessageId,
        content: String,
    },
    /// Delete your own message. Answered with `ok`.
    #[serde(rename = "message.delete")]
    MessageDelete {
        req_id: String,
        room_id: Uuid,
        message_id: MessageId,
    },
    /// Add (`active: true`) or remove your reaction. Answered with `ok`.
    /// Setting what is already set is a no-op, so retries are safe.
    #[serde(rename = "reaction.set")]
    ReactionSet {
        req_id: String,
        room_id: Uuid,
        message_id: MessageId,
        emoji: String,
        active: bool,
    },
    /// "I have read up to this message." Send when the room is visible and
    /// scrolled to the bottom, at most every 2 seconds per room. No reply.
    #[serde(rename = "read.mark")]
    ReadMark {
        room_id: Uuid,
        message_id: MessageId,
    },
}

/// Frames the server sends.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(tag = "type")]
#[ts(export)]
pub enum ServerFrame {
    /// The first frame on every connection, sent once the connection's
    /// subscriptions are live. Messages sent from now on arrive on this
    /// socket; anything earlier comes from history or sync.
    #[serde(rename = "hello")]
    Hello {
        user_id: Uuid,
        conn_id: Uuid,
        heartbeat_ms: u32,
    },
    #[serde(rename = "ping")]
    Ping,
    /// The message is stored. `duplicate` is true when this was a retry of a
    /// send the server had already stored; `message` is the original.
    #[serde(rename = "message.ack")]
    MessageAck {
        req_id: String,
        message: Message,
        duplicate: bool,
    },
    #[serde(rename = "error")]
    Error {
        /// The request this answers, if any.
        req_id: Option<String>,
        code: FrameErrorCode,
        message: String,
    },
    /// An edit, delete or reaction was stored.
    #[serde(rename = "ok")]
    Ok { req_id: String },
    #[serde(rename = "message.created")]
    MessageCreated { message: Message },
    /// A message's content changed. Apply it only if `version` is higher than
    /// the copy you have.
    #[serde(rename = "message.updated")]
    MessageUpdated {
        room_id: Uuid,
        message_id: MessageId,
        content: String,
        #[serde(with = "time::serde::rfc3339")]
        #[ts(type = "string")]
        edited_at: time::OffsetDateTime,
        #[ts(type = "number")]
        version: i64,
        change_id: MessageId,
    },
    /// A message was deleted. Final: no later frame brings its content back.
    #[serde(rename = "message.deleted")]
    MessageDeleted {
        room_id: Uuid,
        message_id: MessageId,
        #[ts(type = "number")]
        version: i64,
        change_id: MessageId,
    },
    /// Someone added or removed a reaction. `count` is the total for that
    /// emoji after the change. Of two of these for the same message, the
    /// higher `change_id` is newer.
    #[serde(rename = "reaction.updated")]
    ReactionUpdated {
        room_id: Uuid,
        message_id: MessageId,
        emoji: String,
        count: u32,
        user_id: Uuid,
        active: bool,
        change_id: MessageId,
    },
    /// A read position moved forward: the other person's in a DM (a read
    /// receipt), or your own from another tab (clear the unread badge).
    #[serde(rename = "read.updated")]
    ReadUpdated {
        room_id: Uuid,
        user_id: Uuid,
        message_id: MessageId,
    },
    /// Someone joined a room you are in.
    #[serde(rename = "member.joined")]
    MemberJoined { room_id: Uuid, member: Member },
    /// Someone left or was removed from a room you are in.
    #[serde(rename = "member.left")]
    MemberLeft { room_id: Uuid, user_id: Uuid },
    /// A guest expired and was deleted. Hide their messages in this room at
    /// once; the stored copies expire on their own (docs/design.md 11.3).
    #[serde(rename = "user.expired")]
    UserExpired { room_id: Uuid, user_id: Uuid },
    #[serde(rename = "typing")]
    Typing { room_id: Uuid, user_id: Uuid },
    #[serde(rename = "presence")]
    Presence {
        user_id: Uuid,
        status: PresenceStatus,
    },
    /// The user was added to a room (joined it on another tab, accepted an
    /// invite, or someone opened a DM with them).
    #[serde(rename = "room.joined")]
    RoomJoined { room: RoomInfo },
    #[serde(rename = "room.left")]
    RoomLeft { room_id: Uuid, reason: LeaveReason },
    /// Live delivery to this connection may have missed messages. Run the
    /// sync procedure (docs/protocol.md) for every loaded room.
    #[serde(rename = "resync")]
    Resync { reason: String },
}

/// Stable error codes for `error` frames.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum FrameErrorCode {
    /// Not JSON, an unknown `type`, or a missing field. Not retryable.
    BadFrame,
    /// Empty, too long, or contains disallowed characters. Not retryable.
    InvalidContent,
    /// Not a member of the room, or not your message. Not retryable.
    Forbidden,
    /// The message doesn't exist (or is already deleted). Not retryable.
    NotFound,
    /// Not one of the allowed reaction emoji. Not retryable.
    InvalidReaction,
    /// The idempotency key was already used for a different message.
    IdempotencyConflict,
    /// Too many requests of this kind too quickly (docs/design.md 11.1). Not
    /// retryable automatically: the user can try again after slowing down.
    RateLimited,
    /// A dependency is down. Retry the same frame, with the same key.
    Unavailable,
    /// A bug on our side. Retryable.
    Internal,
}

impl FrameErrorCode {
    /// Whether the client should retry the same frame.
    pub fn is_retryable(self) -> bool {
        matches!(self, Self::Unavailable | Self::Internal)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum LeaveReason {
    Left,
    Removed,
    Deleted,
}

/// What a client reports about its own tab.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum PresenceState {
    Active,
    /// No input for 5 minutes, or the tab is hidden.
    Idle,
}

/// What other users see: derived from all of a user's connections.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum PresenceStatus {
    /// At least one connection is active.
    Online,
    /// Connected, but every connection is idle.
    Away,
    Offline,
}

/// WebSocket close codes the server uses.
pub mod close {
    /// The session ended (logout, revoked or expired). Don't reconnect
    /// without signing in again.
    pub const SESSION_ENDED: u16 = 4001;
    /// The client didn't read fast enough. Reconnect and sync.
    pub const TOO_SLOW: u16 = 4008;
    /// The client broke the protocol (for example, no heartbeat).
    pub const PROTOCOL: u16 = 4400;
    /// Too many open sockets for this user or IP. Don't pile on reconnects.
    pub const TOO_MANY: u16 = 4029;
    /// The server is restarting. Reconnect.
    pub const RESTARTING: u16 = 1012;
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use time::macros::datetime;

    use super::*;
    use crate::ids::MessageId;

    fn uuid(n: u128) -> Uuid {
        Uuid::from_u128(n)
    }

    /// These are the exact shapes docs/protocol.md shows.
    #[test]
    fn client_frames() {
        let send: ClientFrame = serde_json::from_value(json!({
            "type": "message.send",
            "req_id": "r1",
            "room_id": "00000000-0000-0000-0000-000000000001",
            "idempotency_key": "00000000-0000-0000-0000-000000000002",
            "content": "hello",
        }))
        .unwrap();
        assert_eq!(
            send,
            ClientFrame::MessageSend {
                req_id: "r1".into(),
                room_id: uuid(1),
                idempotency_key: uuid(2),
                content: "hello".into(),
            }
        );
        assert_eq!(
            serde_json::from_value::<ClientFrame>(json!({"type": "pong"})).unwrap(),
            ClientFrame::Pong
        );
        assert_eq!(
            serde_json::from_value::<ClientFrame>(json!({"type": "presence.set", "state": "idle"}))
                .unwrap(),
            ClientFrame::PresenceSet {
                state: PresenceState::Idle
            }
        );
    }

    #[test]
    fn unknown_or_incomplete_frames_are_rejected() {
        for bad in [
            json!({"type": "message.explode"}),
            json!({"type": "message.send", "room_id": "00000000-0000-0000-0000-000000000001"}),
            json!({"content": "no type"}),
        ] {
            assert!(
                serde_json::from_value::<ClientFrame>(bad.clone()).is_err(),
                "{bad}"
            );
        }
    }

    #[test]
    fn server_frames() {
        let message = Message {
            id: MessageId::from_parts(1_000, 3, 0),
            room_id: uuid(1),
            author_id: uuid(9),
            content: "hello".into(),
            created_at: datetime!(2026-01-01 00:00:01 UTC),
            edited_at: None,
            deleted: false,
            version: 1_767_225_601_000_000,
            reactions: vec![],
        };
        assert_eq!(
            serde_json::to_value(ServerFrame::MessageAck {
                req_id: "r1".into(),
                message,
                duplicate: false,
            })
            .unwrap(),
            json!({
                "type": "message.ack",
                "req_id": "r1",
                "duplicate": false,
                "message": {
                    "id": "4194316288",
                    "room_id": "00000000-0000-0000-0000-000000000001",
                    "author_id": "00000000-0000-0000-0000-000000000009",
                    "content": "hello",
                    "created_at": "2026-01-01T00:00:01Z",
                    "edited_at": null,
                    "deleted": false,
                    "version": 1_767_225_601_000_000_i64,
                    "reactions": [],
                },
            })
        );
        assert_eq!(
            serde_json::to_value(ServerFrame::Error {
                req_id: None,
                code: FrameErrorCode::IdempotencyConflict,
                message: "key reused".into(),
            })
            .unwrap(),
            json!({"type": "error", "req_id": null, "code": "idempotency_conflict", "message": "key reused"})
        );
        assert_eq!(
            serde_json::to_value(ServerFrame::Ping).unwrap(),
            json!({"type": "ping"})
        );
    }

    #[test]
    fn only_dependency_and_internal_errors_are_retryable() {
        assert!(FrameErrorCode::Unavailable.is_retryable());
        assert!(FrameErrorCode::Internal.is_retryable());
        assert!(!FrameErrorCode::Forbidden.is_retryable());
        assert!(!FrameErrorCode::IdempotencyConflict.is_retryable());
    }
}
