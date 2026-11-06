//! REST request and response bodies.

use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use ts_rs::TS;
use uuid::Uuid;

use crate::ids::MessageId;

/// Stable error codes. Clients branch on the code, never on the message text.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum ErrorCode {
    /// The request could not be parsed.
    BadRequest,
    /// A field failed validation; `field` names it.
    InvalidInput,
    /// No valid session, or wrong credentials.
    Unauthorized,
    /// Authenticated, but not allowed (includes a rejected Origin).
    Forbidden,
    NotFound,
    /// The resource already exists (for example, the email is registered).
    Conflict,
    /// Rate limit exceeded (docs/design.md 11.1): too many requests of this
    /// kind too quickly. The HTTP status is 429.
    TooManyRequests,
    /// A dependency is down. Safe to retry.
    Unavailable,
    Internal,
}

/// The body of every non-2xx response.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct ApiError {
    pub code: ErrorCode,
    pub message: String,
    /// The request field the error refers to, for form validation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub field: Option<String>,
}

/// What the sign-in screen should offer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct AuthConfig {
    /// False when the server has no GitHub OAuth app configured.
    pub github: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct SignupRequest {
    pub email: String,
    pub password: String,
    pub handle: String,
    pub display_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct LoginRequest {
    pub email: String,
    pub password: String,
}

/// The signed-in user.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct Me {
    pub id: Uuid,
    pub handle: String,
    pub display_name: String,
    pub avatar_url: Option<String>,
    pub is_guest: bool,
    /// When a guest account and all its data expire. Null for other accounts.
    #[serde(with = "time::serde::rfc3339::option")]
    #[ts(type = "string | null")]
    pub expires_at: Option<OffsetDateTime>,
}

/// One of the signed-in user's sessions, for the "your sessions" list.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct SessionInfo {
    pub id: Uuid,
    #[serde(with = "time::serde::rfc3339")]
    #[ts(type = "string")]
    pub created_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    #[ts(type = "string")]
    pub last_seen_at: OffsetDateTime,
    pub user_agent: Option<String>,
    pub ip: Option<String>,
    /// True for the session that made this request.
    pub current: bool,
}

/// A user as other users see them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct UserProfile {
    pub id: Uuid,
    pub handle: String,
    pub display_name: String,
    pub avatar_url: Option<String>,
    pub is_guest: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum RoomKind {
    /// Anyone can see and join it.
    Public,
    /// Invite only.
    Private,
    /// A direct conversation between two users.
    Dm,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum MemberRole {
    Owner,
    Member,
}

/// A room the caller belongs to.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct RoomInfo {
    pub id: Uuid,
    pub kind: RoomKind,
    /// Null for DMs, which are named after the other person.
    pub name: Option<String>,
    pub role: MemberRole,
    /// For DMs, the other member.
    pub dm_peer: Option<UserProfile>,
    #[serde(with = "time::serde::rfc3339")]
    #[ts(type = "string")]
    pub created_at: OffsetDateTime,
    /// The newest message the caller has read ("0" for none).
    pub last_read_id: MessageId,
    /// Messages from others since `last_read_id`, counted up to a cap.
    pub unread: u32,
    /// True when there are more than `unread` (shown as "99+").
    pub unread_capped: bool,
}

/// An entry in the public room directory.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct PublicRoom {
    pub id: Uuid,
    pub name: String,
    #[ts(type = "number")]
    pub member_count: i64,
    /// Whether the caller is already a member.
    pub joined: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct CreateRoomRequest {
    pub name: String,
    /// `public` or `private`. DMs are created with [`CreateDmRequest`].
    pub kind: RoomKind,
}

/// Opens (or returns the existing) DM with the user who has this handle.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct CreateDmRequest {
    pub handle: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct Member {
    pub user: UserProfile,
    pub role: MemberRole,
    #[serde(with = "time::serde::rfc3339")]
    #[ts(type = "string")]
    pub joined_at: OffsetDateTime,
}

/// An invite link for a private room. The link is `/invite/<code>`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct Invite {
    pub code: String,
    #[serde(with = "time::serde::rfc3339")]
    #[ts(type = "string")]
    pub expires_at: OffsetDateTime,
    pub max_uses: i32,
}

/// A chat message.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct Message {
    pub id: MessageId,
    pub room_id: Uuid,
    pub author_id: Uuid,
    /// Empty when `deleted` is true.
    pub content: String,
    /// The time inside the ID.
    #[serde(with = "time::serde::rfc3339")]
    #[ts(type = "string")]
    pub created_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339::option")]
    #[ts(type = "string | null")]
    pub edited_at: Option<OffsetDateTime>,
    pub deleted: bool,
    /// The write timestamp of the latest change, in microseconds. Of two
    /// copies of the same message, the one with the higher version is newer.
    #[ts(type = "number")]
    pub version: i64,
    /// Reactions, one entry per emoji with at least one reaction.
    #[serde(default)]
    pub reactions: Vec<ReactionCount>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct ReactionCount {
    pub emoji: String,
    pub count: u32,
    /// Whether the caller is one of them.
    pub me: bool,
}

/// One page of history, newest message first.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct HistoryPage {
    pub messages: Vec<Message>,
    /// Pass as `before` to get the next older page. Null at the start of the
    /// room. A page can be shorter than requested and still have a cursor,
    /// when the server stopped early to bound its work.
    pub next_before: Option<MessageId>,
    /// Where this room's change feed stands now. Keep it, and send it as
    /// `changes_after` in the next sync to get edits, deletes and reactions
    /// to messages you already have.
    pub change_cursor: MessageId,
}

/// "I have this room up to `after`; send me what I missed."
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct SyncCursor {
    pub room_id: Uuid,
    /// The highest message ID the client has seen in the room.
    pub after: MessageId,
    /// The room's change cursor from the last history load or sync. Without
    /// it, the sync returns new messages only.
    #[serde(default)]
    #[ts(optional)]
    pub changes_after: Option<MessageId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct SyncRequest {
    pub rooms: Vec<SyncCursor>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct RoomSync {
    pub room_id: Uuid,
    /// False if the caller is no longer a member; the client drops the room.
    pub member: bool,
    /// Messages from slightly before `after` onwards, oldest first. The
    /// overlap is deliberate (docs/design.md 16.1); the client deduplicates by
    /// ID.
    pub messages: Vec<Message>,
    /// Messages already sent before `after` that were edited, deleted or
    /// reacted to since `changes_after`: their current state.
    pub changed: Vec<Message>,
    /// The cursor to send as `changes_after` next time.
    pub change_cursor: MessageId,
    /// False if there were more messages than one sync returns, or the
    /// change cursor is older than the 7-day change feed. The client then
    /// reloads the room from its latest page instead of patching.
    pub complete: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct SyncResponse {
    pub rooms: Vec<RoomSync>,
}

/// Who has read up to a message ("Seen by N").
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct Receipts {
    /// Members other than the caller whose read position is at or after the
    /// message.
    pub count: u32,
    /// The first 20 of them, most recent reader first.
    pub readers: Vec<UserProfile>,
}

/// Search results, newest first, from rooms the caller belongs to.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct SearchResults {
    pub messages: Vec<Message>,
    /// Pass as `before` for older results. Null when there are no more.
    pub next_before: Option<MessageId>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use time::macros::datetime;

    #[test]
    fn error_codes_are_snake_case_strings() {
        let body = ApiError {
            code: ErrorCode::InvalidInput,
            message: "too short".into(),
            field: Some("password".into()),
        };
        assert_eq!(
            serde_json::to_value(&body).unwrap(),
            json!({"code": "invalid_input", "message": "too short", "field": "password"})
        );
    }

    #[test]
    fn absent_field_is_omitted() {
        let body = ApiError {
            code: ErrorCode::Unauthorized,
            message: "sign in".into(),
            field: None,
        };
        assert_eq!(
            serde_json::to_value(&body).unwrap(),
            json!({"code": "unauthorized", "message": "sign in"})
        );
    }

    #[test]
    fn timestamps_are_rfc3339() {
        let me = Me {
            id: Uuid::nil(),
            handle: "guest_ab12".into(),
            display_name: "Guest".into(),
            avatar_url: None,
            is_guest: true,
            expires_at: Some(datetime!(2026-11-05 12:00:00 UTC)),
        };
        let value = serde_json::to_value(&me).unwrap();
        assert_eq!(value["expires_at"], json!("2026-11-05T12:00:00Z"));
        let back: Me = serde_json::from_value(value).unwrap();
        assert_eq!(back, me);
    }
}
