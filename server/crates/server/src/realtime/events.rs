//! Events instances publish to each other through Redis pub/sub.
//!
//! Channels:
//! - `room:<id>`: messages, typing, and membership changes in the room.
//!   Every instance with a socket in the room subscribes. Redis delivers a
//!   channel's messages to all subscribers in one order, which the removal
//!   path relies on (docs/design.md 16.4).
//! - `user:<id>`: things that happen to one user: they joined a room, a
//!   session was revoked. Every instance with a socket of that user
//!   subscribes.
//! - `presence`: presence transitions, system-wide.
//!
//! Pub/sub is best effort: a message published while an instance is
//! disconnected from Redis is lost to it. Clients recover from Scylla
//! (design 16.2), so nothing here may be the only copy of anything.

use protocol::{api::RoomInfo, frames::LeaveReason};
use redis::aio::ConnectionManager;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub const PRESENCE_CHANNEL: &str = "presence";

pub fn room_channel(room_id: Uuid) -> String {
    format!("room:{room_id}")
}

pub fn user_channel(user_id: Uuid) -> String {
    format!("user:{user_id}")
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Event {
    /// A finished `ServerFrame`, as JSON, for every socket routed to the
    /// channel's room or user. Serialized once by the publisher, so each
    /// receiving instance forwards the same bytes to every socket.
    Deliver { frame: String },
    /// The user is now in this room (user channel). `room` is as they see it.
    Joined { room: RoomInfo },
    /// The user is no longer in this room (room channel).
    MemberLeft {
        room_id: Uuid,
        user_id: Uuid,
        reason: LeaveReason,
    },
    /// The room is gone (room channel).
    RoomDeleted { room_id: Uuid },
    /// A session ended; close its sockets (user channel).
    SessionRevoked { session_id: Uuid },
}

impl Event {
    pub fn deliver(frame: &protocol::frames::ServerFrame) -> Self {
        Self::Deliver {
            frame: serde_json::to_string(frame).expect("server frames always serialize"),
        }
    }
}

/// Publishes an event. Failures are the caller's to handle: for most events
/// the stored state is already committed and clients recover from it.
pub async fn publish(
    redis: &mut ConnectionManager,
    channel: &str,
    event: &Event,
) -> redis::RedisResult<()> {
    let payload = serde_json::to_string(event).expect("events always serialize");
    redis::cmd("PUBLISH")
        .arg(channel)
        .arg(payload)
        .query_async::<i64>(redis)
        .await
        .map(|_| ())
}

/// Publishes, logging instead of failing: for events whose effect is
/// already durable (a membership change), a lost event only delays other
/// sockets noticing, and their next sync or reconnect catches up.
pub async fn publish_best_effort(redis: &mut ConnectionManager, channel: &str, event: &Event) {
    if let Err(err) = publish(redis, channel, event).await {
        tracing::warn!(error = %err, channel, "publishing event failed");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn events_round_trip() {
        let event = Event::MemberLeft {
            room_id: Uuid::from_u128(1),
            user_id: Uuid::from_u128(2),
            reason: LeaveReason::Removed,
        };
        let json = serde_json::to_string(&event).unwrap();
        assert!(json.contains(r#""kind":"member_left""#), "{json}");
        assert_eq!(serde_json::from_str::<Event>(&json).unwrap(), event);
        assert_eq!(
            room_channel(Uuid::from_u128(1)),
            "room:00000000-0000-0000-0000-000000000001"
        );
    }
}
